# Market Data Interface Design

Unified Python interface for retrieving stock prices in FinAlly. One abstract
contract, two implementations — a **GBM simulator** (default) and a **Massive
API poller** (when `MASSIVE_API_KEY` is set). Everything downstream (SSE
streaming, portfolio valuation, trade execution) is source-agnostic: it reads
prices from a shared in-memory cache and never touches the data source directly.

This document reflects the implementation in `backend/app/market/`.

## Design at a Glance

```
                create_market_data_source(cache)   ← factory reads MASSIVE_API_KEY
                          │
          ┌───────────────┴────────────────┐
          ▼                                ▼
 SimulatorDataSource              MassiveDataSource
 (GBM, ~500ms ticks)              (REST poll, 15s)
          │                                │
          └──────────────┬─────────────────┘
                         ▼  writes
                    PriceCache  (thread-safe, in-memory, single source of truth)
                         │  reads
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
   SSE /api/stream   Portfolio       Trade
     /prices         valuation       execution
```

**Strategy pattern.** Both sources implement the same ABC. **Cache as the single
point of truth.** Producers write, consumers read; no direct coupling. This also
makes future multi-user support a non-event — the data layer is unchanged.

## Public Imports

```python
from app.market import (
    PriceUpdate,
    PriceCache,
    MarketDataSource,
    create_market_data_source,
    create_stream_router,
)
```

## Core Data Model — `PriceUpdate`

The only data structure that leaves the market layer. Immutable and frozen;
`change`, `change_percent`, and `direction` are derived properties, so they can
never drift out of sync with the stored prices.

```python
from dataclasses import dataclass, field
import time

@dataclass(frozen=True, slots=True)
class PriceUpdate:
    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float: ...            # round(price - previous_price, 4)

    @property
    def change_percent(self) -> float: ...    # 0.0 when previous_price == 0

    @property
    def direction(self) -> str: ...           # "up" | "down" | "flat"

    def to_dict(self) -> dict: ...            # for JSON / SSE
```

`to_dict()` emits: `ticker, price, previous_price, timestamp, change,
change_percent, direction`.

## Abstract Interface — `MarketDataSource`

Implementations push updates into the cache on their own schedule. The interface
deliberately does **not** return prices — consumers read the cache.

```python
from abc import ABC, abstractmethod

class MarketDataSource(ABC):
    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin the background task. Call exactly once."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the task and release resources. Safe to call repeatedly."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add to the active set. No-op if present."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove from the active set and from the cache. No-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Current actively-tracked tickers."""
```

## Price Cache — `PriceCache`

Thread-safe store both sources write to and all readers read from. It owns the
change/direction computation: callers supply only `ticker`, `price`, and an
optional `timestamp`; the cache fills `previous_price` from what it already has.

```python
class PriceCache:
    def update(self, ticker, price, timestamp=None) -> PriceUpdate: ...
    def get(self, ticker) -> PriceUpdate | None: ...
    def get_price(self, ticker) -> float | None: ...
    def get_all(self) -> dict[str, PriceUpdate]: ...
    def remove(self, ticker) -> None: ...

    @property
    def version(self) -> int: ...   # monotonic; bumped on every update
```

Key points:
- A `threading.Lock` guards all access — the Massive poller runs callbacks from a
  worker thread (`asyncio.to_thread`), so locking is required, not optional.
- Prices are rounded to 2 decimals on write; first update for a ticker sets
  `previous_price == price` (direction `"flat"`).
- The `version` counter is the SSE change-detection mechanism (see below).

## Factory — `create_market_data_source`

Selects the implementation from the environment. Returns an **unstarted** source;
the caller awaits `start(tickers)`.

```python
import os

def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    return SimulatorDataSource(price_cache=price_cache)
```

Empty/whitespace `MASSIVE_API_KEY` counts as unset → simulator. This is the only
place the choice is made; nothing else in the app branches on the data source.

## Massive Implementation — `MassiveDataSource`

REST poller. One `get_snapshot_all` call covers the whole watchlist. The Massive
client is synchronous, so each call is offloaded with `asyncio.to_thread` to keep
the event loop free.

```python
class MassiveDataSource(MarketDataSource):
    def __init__(self, api_key, price_cache, poll_interval=15.0): ...

    async def start(self, tickers):
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)
        await self._poll_once()                 # immediate first fill
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")

    async def _poll_once(self):
        snapshots = await asyncio.to_thread(self._fetch_snapshots)
        for snap in snapshots:
            try:
                self._cache.update(
                    ticker=snap.ticker,
                    price=snap.last_trade.price,
                    timestamp=snap.last_trade.timestamp / 1000.0,  # ms -> s
                )
            except (AttributeError, TypeError) as e:
                logger.warning("Skipping %s: %s", getattr(snap, "ticker", "?"), e)
```

Behavior notes:
- **Immediate first poll** in `start()` so the cache is populated before the loop
  begins — no empty initial render.
- **Loop-level resilience:** `_poll_once` swallows exceptions and logs; the loop
  retries on the next interval. A bad poll just leaves the last good prices in
  place. (401 / 429 / network errors are expected and non-fatal.)
- **Per-snapshot isolation:** one malformed snapshot is skipped, not fatal.
- `add_ticker` / `remove_ticker` mutate `self._tickers`; added tickers appear on
  the next poll. `remove_ticker` also clears the cache entry.
- Tickers are normalized with `.upper().strip()`.
- Poll interval defaults to 15s (free tier). Paid tiers can drop to 2-5s.

See [MASSIVE_API.md](MASSIVE_API.md) for the API surface.

## Simulator Implementation — `SimulatorDataSource`

Wraps a `GBMSimulator` in an asyncio loop, stepping every ~500ms and writing each
price to the cache. Seeds the cache with starting prices in `start()` so the UI
has data on first paint. See [MARKET_SIMULATOR.md](MARKET_SIMULATOR.md) for the
math and structure.

```python
class SimulatorDataSource(MarketDataSource):
    def __init__(self, price_cache, update_interval=0.5, event_probability=0.001): ...

    async def start(self, tickers):
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        for t in tickers:                       # seed cache immediately
            price = self._sim.get_price(t)
            if price is not None:
                self._cache.update(ticker=t, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")

    async def _run_loop(self):
        while True:
            try:
                for ticker, price in self._sim.step().items():
                    self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

## SSE Integration — `create_stream_router`

The streaming endpoint reads the cache and pushes to connected `EventSource`
clients. It uses the cache's `version` counter to avoid resending unchanged data,
and detects client disconnects.

```python
router = create_stream_router(price_cache)   # FastAPI APIRouter
# GET /api/stream/prices  →  text/event-stream
```

Generator essentials (`backend/app/market/stream.py`):
- Emits `retry: 1000\n\n` first so the browser auto-reconnects after ~1s.
- Each tick: if `price_cache.version` changed, serialize `get_all()` as
  `{ticker: update.to_dict()}` and `yield f"data: {json}\n\n"`.
- Polls every 500ms; breaks on `await request.is_disconnected()`.
- Headers: `Cache-Control: no-cache`, `Connection: keep-alive`,
  `X-Accel-Buffering: no` (defeats nginx buffering when proxied).

Payload shape per event:
```json
{ "AAPL": { "ticker": "AAPL", "price": 190.50, "previous_price": 190.48,
            "timestamp": 1675190399.0, "change": 0.02,
            "change_percent": 0.0105, "direction": "up" }, "...": {} }
```

## File Structure

```
backend/app/market/
  __init__.py          # public exports
  models.py            # PriceUpdate
  interface.py         # MarketDataSource (ABC)
  cache.py             # PriceCache
  factory.py           # create_market_data_source()
  massive_client.py    # MassiveDataSource
  simulator.py         # GBMSimulator + SimulatorDataSource
  seed_prices.py       # SEED_PRICES, TICKER_PARAMS, correlation constants
  stream.py            # create_stream_router() (SSE)
```

## Lifecycle (FastAPI app)

1. **Startup** — create `PriceCache`, `create_market_data_source(cache)`, then
   `await source.start(watchlist_tickers)`. Mount `create_stream_router(cache)`.
2. **Watchlist change** — `await source.add_ticker(t)` /
   `await source.remove_ticker(t)`.
3. **SSE** — clients connect to `/api/stream/prices`; the generator reads the
   cache every 500ms.
4. **Trade / valuation** — read the live price with `cache.get_price(ticker)`.
5. **Shutdown** — `await source.stop()`.

## Why This Shape

- **Cache, not callbacks** — decouples cadence (500ms sim vs 15s poll) from
  consumers; the UI streams at a steady 500ms regardless of source.
- **Derived properties on `PriceUpdate`** — change/direction can't desync.
- **Version counter** — cheap "did anything change?" check for SSE without
  diffing dictionaries.
- **`to_thread` for Massive** — keeps a blocking SDK from stalling the loop.
