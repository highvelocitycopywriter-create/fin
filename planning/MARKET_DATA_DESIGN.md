# Market Data Backend — Implementation Design

Complete implementation guide for the FinAlly market data subsystem. Covers every
module in `backend/app/market/` with full code, rationale, and integration notes.

**Status:** Implemented, tested, reviewed. See `planning/MARKET_DATA_SUMMARY.md`
for the post-build summary and `planning/archive/MARKET_DATA_REVIEW.md` for the
code review findings (all resolved).

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [File Structure](#2-file-structure)
3. [Data Model — `models.py`](#3-data-model--modelspy)
4. [Price Cache — `cache.py`](#4-price-cache--cachepy)
5. [Abstract Interface — `interface.py`](#5-abstract-interface--interfacepy)
6. [Seed Prices & Ticker Parameters — `seed_prices.py`](#6-seed-prices--ticker-parameters--seed_pricespy)
7. [GBM Simulator — `simulator.py`](#7-gbm-simulator--simulatorpy)
8. [Massive API Client — `massive_client.py`](#8-massive-api-client--massive_clientpy)
9. [Factory — `factory.py`](#9-factory--factorypy)
10. [SSE Streaming Endpoint — `stream.py`](#10-sse-streaming-endpoint--streampy)
11. [Public Package API — `__init__.py`](#11-public-package-api--__init__py)
12. [FastAPI Lifecycle Integration](#12-fastapi-lifecycle-integration)
13. [Watchlist Coordination](#13-watchlist-coordination)
14. [Testing Strategy](#14-testing-strategy)
15. [Error Handling & Edge Cases](#15-error-handling--edge-cases)
16. [Configuration Reference](#16-configuration-reference)

---

## 1. Architecture Overview

```
                create_market_data_source(cache)   ← reads MASSIVE_API_KEY
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

**Two key design decisions define the whole system:**

**Strategy pattern** — `SimulatorDataSource` and `MassiveDataSource` both implement
`MarketDataSource` (an ABC). Every downstream consumer is completely source-agnostic;
it reads from the cache and never touches the data source directly. Switching from
simulator to real market data requires only setting an environment variable.

**Cache as single source of truth** — Data sources *push* into the cache on their
own schedule (500ms or 15s). Consumers *pull* from the cache on their own schedule.
This decouples the cadence of price production from the cadence of price consumption.
The SSE endpoint streams at a steady 500ms regardless of which source is active.

---

## 2. File Structure

```
backend/app/market/
  __init__.py          # Public API re-exports
  models.py            # PriceUpdate dataclass
  cache.py             # PriceCache (thread-safe in-memory store)
  interface.py         # MarketDataSource (ABC)
  seed_prices.py       # SEED_PRICES, TICKER_PARAMS, correlation constants
  simulator.py         # GBMSimulator + SimulatorDataSource
  massive_client.py    # MassiveDataSource (Polygon.io REST poller)
  factory.py           # create_market_data_source() factory
  stream.py            # FastAPI SSE router
```

Each module has a single responsibility. The `__init__.py` exposes the public API
so the rest of the backend imports from `app.market` without reaching into submodules.

---

## 3. Data Model — `models.py`

`PriceUpdate` is the only data structure that leaves the market layer. Every
downstream consumer — SSE, portfolio valuation, trade execution — works exclusively
with this type.

```python
# backend/app/market/models.py
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

**Design decisions:**

- `frozen=True` — Immutable value object. Safe to share across async tasks without
  copying. Eliminates a whole class of mutation bugs.
- `slots=True` — Minor memory optimization. We create ~20 of these per second
  (10 tickers × 2 ticks/sec), so attribute lookup speed and memory density matter.
- Computed properties — `change`, `change_percent`, and `direction` are derived
  from `price` and `previous_price`, so they can never be inconsistent. No risk of
  a stale `direction` field that disagrees with the actual prices.
- `to_dict()` — Single serialization point. Used by the SSE endpoint and REST
  API responses. Centralizing serialization means the JSON shape changes in exactly
  one place.

---

## 4. Price Cache — `cache.py`

The price cache is the central hub. Data sources write to it; SSE and portfolio
routes read from it. Thread-safe because the Massive poller runs in `asyncio.to_thread`
(a real OS thread), not on the async event loop.

```python
# backend/app/market/cache.py
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory store of the latest price per ticker.

    Producers: SimulatorDataSource or MassiveDataSource (one active at a time).
    Consumers: SSE endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonic counter, bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price. Returns the created PriceUpdate.

        Automatically fills previous_price from the last stored value.
        First update for a ticker sets previous_price == price (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def remove(self, ticker: str) -> None:
        """Remove a ticker (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Current version. Bumped on every update. Used for SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

**Why `threading.Lock` instead of `asyncio.Lock`?**

`asyncio.Lock` can only be acquired from the async event loop. The Massive client's
`get_snapshot_all()` is synchronous and runs in `asyncio.to_thread()`, which is a
real OS thread. From that thread, an `asyncio.Lock` acquire would deadlock. The
`threading.Lock` works correctly from both sync threads and the event loop.

**Why a version counter?**

Without it, the SSE endpoint would serialize and send all prices every 500ms even
when nothing changed — wasteful with the Massive source that updates every 15 seconds.
The version counter lets SSE skip sends when the cache hasn't changed:

```python
last_version = -1
while True:
    if price_cache.version != last_version:
        last_version = price_cache.version
        yield format_sse(price_cache.get_all())
    await asyncio.sleep(0.5)
```

---

## 5. Abstract Interface — `interface.py`

The contract both data sources must implement. Nothing downstream needs to know
which source is active.

```python
# backend/app/market/interface.py
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a PriceCache on their own schedule.
    Downstream code reads from the cache, never from the source directly.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # runtime: add/remove tickers as watchlist changes
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # shutdown:
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates. Call exactly once."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task. Safe to call multiple times."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker. Also removes it from the cache. No-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers."""
```

**Why the interface doesn't return prices:**

The push model decouples timing entirely. The simulator ticks at 500ms, Massive
polls at 15s — but consumers always read from the cache at their own cadence. No
consumer needs to know or care which source is active, or how often it updates.

---

## 6. Seed Prices & Ticker Parameters — `seed_prices.py`

Constants only. No logic, no imports. Shared by the simulator and available as
fallback prices for the Massive client (before the first poll completes).

```python
# backend/app/market/seed_prices.py
"""Seed prices and GBM parameters for the default watchlist."""

# Realistic starting prices for the 10 default tickers
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters.
# sigma: annualized volatility — higher = more price movement per tick
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},   # High volatility, lower drift
    "NVDA":  {"sigma": 0.40, "mu": 0.08},   # High volatility, strong drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},   # Low volatility (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},   # Low volatility (payments network)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

# Fallback for dynamically-added tickers not in the list above
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Sector groups for Cholesky correlation matrix construction
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Pairwise correlation coefficients
INTRA_TECH_CORR = 0.6     # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
TSLA_CORR = 0.3           # TSLA is in tech but does its own thing
CROSS_GROUP_CORR = 0.3    # Cross-sector or unknown pairs
```

**Ticker personality notes:**
- TSLA (`sigma=0.50`) jumps visibly each tick. JPM and V (`sigma~0.17-0.18`) drift
  quietly. This gives each ticker a distinct visual character.
- NVDA has the highest drift (`mu=0.08`) — it trends upward faster than others over
  a session, which is intentional flavor.
- Unknown tickers (dynamically added) start at a random price in `[50, 300]` and
  use `DEFAULT_PARAMS`.

---

## 7. GBM Simulator — `simulator.py`

Two classes in one file:
- `GBMSimulator` — pure synchronous math engine, holds state, advances prices
- `SimulatorDataSource` — async wrapper that ticks `GBMSimulator` every 500ms and
  writes results to the `PriceCache`

### 7.1 GBM Math

At each step, each price evolves as:

```
S(t+dt) = S(t) * exp( (mu - sigma²/2) * dt + sigma * sqrt(dt) * Z )
```

| Symbol | Meaning |
|--------|---------|
| `S(t)` | current price |
| `mu` | annualized drift (expected return), e.g. `0.05` |
| `sigma` | annualized volatility, e.g. `0.20` |
| `dt` | time step as fraction of a trading year |
| `Z` | correlated standard-normal draw |

**Why GBM?** It's the same process underlying Black-Scholes. Prices evolve with
random noise, can never go negative (exponential is always positive), and produce
lognormal returns matching real market behavior.

**Choosing `dt`:** A trading year is `252 days × 6.5 hours × 3600 seconds = 5,896,800s`.
A 500ms tick is `dt = 0.5 / 5,896,800 ≈ 8.48e-8`. This tiny `dt` produces
sub-cent moves per tick that accumulate into believable trends over a session.

**Correlated moves (Cholesky):** Real tech stocks move together. The simulator
draws correlated normals via Cholesky decomposition of a sector-based correlation
matrix:

```
L = cholesky(C)           # C = correlation matrix, L = lower-triangular factor
Z_correlated = L @ Z_independent
```

This guarantees the correct covariance structure for any valid (positive semi-definite)
correlation matrix.

### 7.2 Full Implementation

```python
# backend/app/market/simulator.py
from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion price simulator with correlated moves.

    Pure, synchronous price-path generator. No async, no I/O — just step().
    """

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600   # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR    # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance all tickers one time step. Returns {ticker: new_price}.

        Hot path — called every 500ms. Batches random draws through NumPy
        for efficiency: one standard_normal(n) call + one matrix multiply.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            p = self._params[ticker]
            drift = (p["mu"] - 0.5 * p["sigma"] ** 2) * self._dt
            diffusion = p["sigma"] * math.sqrt(self._dt) * z[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random shock: ~0.1% chance per tick per ticker
            # With 10 tickers at 2 ticks/sec, expect an event ~every 50 seconds
            if random.random() < self._event_prob:
                shock = random.uniform(0.02, 0.05) * random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock
                logger.debug(
                    "Random event on %s: %.1f%% %s",
                    ticker,
                    abs(shock) * 100,
                    "up" if shock > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)
        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker. Rebuilds Cholesky matrix. No-op if already present."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker. Rebuilds Cholesky matrix. No-op if not present."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add a ticker without rebuilding Cholesky (used for batch init)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky factor of the correlation matrix.

        Called whenever tickers are added or removed. O(n²), but n stays small
        (< 50 tickers in practice), so this is negligible.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None  # No correlation to apply with a single ticker
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Sector-based pairwise correlation coefficient.

        TSLA is in the tech set but is forced to 0.3 with everything —
        it moves on its own logic (earnings, Elon tweets, etc.).
        """
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR


class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by GBMSimulator.

    Runs a background asyncio task that calls GBMSimulator.step() every
    `update_interval` seconds and writes results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)

        # Seed the cache immediately so SSE has data on its very first tick.
        # Without this, the frontend would show blank prices for ~500ms.
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)

        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

---

## 8. Massive API Client — `massive_client.py`

Polls the Massive (Polygon.io) REST API for all watched tickers in a single call,
then writes results to the cache. The synchronous Massive client is offloaded to
`asyncio.to_thread` so it never blocks the event loop.

```python
# backend/app/market/massive_client.py
from __future__ import annotations

import asyncio
import logging
from typing import Any

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: can poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: Any = None

    async def start(self, tickers: list[str]) -> None:
        from massive import RESTClient

        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Immediate first poll — cache is populated before the loop begins,
        # so the frontend has real data on its very first SSE tick.
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (appears on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internals ---

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already ran in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, write to cache."""
        if not self._tickers or not self._client:
            return
        try:
            # The Massive RESTClient is synchronous — offload to a thread.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    # Massive timestamps are Unix milliseconds; cache expects seconds.
                    timestamp = snap.last_trade.timestamp / 1000.0
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                    processed += 1
                except (AttributeError, TypeError) as e:
                    # One bad snapshot doesn't abort the whole batch.
                    logger.warning("Skipping %s: %s", getattr(snap, "ticker", "???"), e)
            logger.debug("Massive poll: %d/%d tickers updated", processed, len(self._tickers))
        except Exception as e:
            # A failed poll leaves the last-known prices in the cache.
            # The loop will retry on the next interval.
            logger.error("Massive poll failed: %s", e)

    def _fetch_snapshots(self) -> list:
        """Synchronous Massive API call. Runs in asyncio.to_thread()."""
        from massive.rest.models import SnapshotMarketType

        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

**Massive snapshot payload (relevant fields):**

```json
{
  "ticker": "AAPL",
  "lastTrade": {
    "p": 190.50,
    "t": 1707580800000
  },
  "todaysChangePerc": -1.2,
  "day": { "o": 192.00, "h": 193.50, "l": 189.75, "c": 190.50, "v": 45000000 },
  "prevDay": { "c": 192.80 }
}
```

Python client attribute mapping: `snap.last_trade.price`, `snap.last_trade.timestamp`
(Unix ms — divide by 1000 for seconds), `snap.todays_change_percent`, `snap.day.open`,
`snap.prev_day.close`.

**Error handling matrix:**

| Error | Behavior |
|-------|----------|
| 401 Unauthorized | Logged. Loop retries next interval. |
| 429 Rate Limited | Logged. Loop retries after `poll_interval` seconds. |
| Network timeout | Logged. Retries automatically. |
| Malformed snapshot | Individual ticker skipped. Others still processed. |
| All tickers fail | Cache retains last-known prices. SSE streams stale data. |

---

## 9. Factory — `factory.py`

Exactly one place where the data source choice is made. Nothing else in the
codebase branches on which source is active.

```python
# backend/app/market/factory.py
from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Select and return the appropriate market data source.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real data)
    - Otherwise → SimulatorDataSource (GBM simulation, default)

    Returns an unstarted source. The caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        from .massive_client import MassiveDataSource
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)

    from .simulator import SimulatorDataSource
    logger.info("Market data source: GBM Simulator (no MASSIVE_API_KEY)")
    return SimulatorDataSource(price_cache=price_cache)
```

The `MassiveDataSource` import is inside the `if` block so the `massive` package
is only needed when `MASSIVE_API_KEY` is actually set. The simulator path has no
external dependencies beyond `numpy`.

---

## 10. SSE Streaming Endpoint — `stream.py`

A FastAPI route that holds open a long-lived HTTP connection and pushes price
updates to the browser as `text/event-stream`. The browser's native `EventSource`
API handles this transparently with auto-reconnect.

```python
# backend/app/market/stream.py
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE router with a reference to the shared price cache.

    Factory pattern allows injecting the cache without module-level globals.
    """
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint. Stream all tracked ticker prices every ~500ms.

        Client connects with:
            const es = new EventSource('/api/stream/prices');
            es.onmessage = (e) => { const prices = JSON.parse(e.data); };

        Each event is a JSON object: { "AAPL": {ticker, price, ...}, ... }
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering when proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator yielding SSE-formatted price events."""
    # Tell the browser to retry after 1 second if the connection drops.
    yield "retry: 1000\n\n"

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    payload = json.dumps({t: u.to_dict() for t, u in prices.items()})
                    yield f"data: {payload}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled: %s", client_ip)
```

**SSE wire format (one event):**

```
data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1707580800.5,"change":0.08,"change_percent":0.042,"direction":"up"},"GOOGL":{...}}

```

Note the blank line after `data:` — this is the SSE event delimiter. The browser
fires `EventSource.onmessage` once per event.

**Frontend connection:**

```typescript
const eventSource = new EventSource('/api/stream/prices');

eventSource.onmessage = (event) => {
    const prices: Record<string, PriceUpdate> = JSON.parse(event.data);
    // prices.AAPL.price, prices.AAPL.direction, etc.
};

eventSource.onerror = () => {
    // EventSource auto-reconnects after the retry delay (1000ms).
    // Update connection status indicator to "reconnecting".
};
```

**Why poll-and-push, not event-driven?**

The SSE loop polls the cache on a fixed 500ms cadence rather than being notified
by the data source. This produces regular, evenly-spaced updates regardless of
whether the source is the 500ms simulator or 15s Massive poller. The frontend
accumulates these for sparkline charts — regular spacing is important for clean
visualization and animation.

**SSE header notes:**
- `Cache-Control: no-cache` — prevents browsers or proxies from caching the stream
- `Connection: keep-alive` — explicit hint (HTTP/1.1 default, but explicit is clear)
- `X-Accel-Buffering: no` — disables nginx's default response buffering, which
  would cause events to batch up instead of arriving in real time

---

## 11. Public Package API — `__init__.py`

```python
# backend/app/market/__init__.py
"""Market data subsystem for FinAlly.

Usage:
    from app.market import PriceCache, create_market_data_source, create_stream_router

    cache = PriceCache()
    source = create_market_data_source(cache)   # reads MASSIVE_API_KEY
    await source.start(["AAPL", "GOOGL", "MSFT", ...])

    price = cache.get_price("AAPL")             # float | None
    update = cache.get("AAPL")                  # PriceUpdate | None
    all_prices = cache.get_all()                # dict[str, PriceUpdate]

    await source.add_ticker("TSLA")
    await source.remove_ticker("GOOGL")

    await source.stop()
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

---

## 12. FastAPI Lifecycle Integration

The market data system starts and stops with the FastAPI application using the
`lifespan` context manager pattern (the modern replacement for `@app.on_event`).

```python
# backend/app/main.py (relevant sections)
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends

from app.market import PriceCache, MarketDataSource, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP ---
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    source = create_market_data_source(price_cache)
    app.state.market_source = source

    # Load initial tickers from the watchlist table
    initial_tickers = await db.get_watchlist_tickers()
    await source.start(initial_tickers)

    # Mount the SSE endpoint (must happen after source.start so cache is seeded)
    app.include_router(create_stream_router(price_cache))

    yield  # App is running

    # --- SHUTDOWN ---
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


# FastAPI dependency functions — inject cache/source into route handlers
def get_price_cache() -> PriceCache:
    return app.state.price_cache


def get_market_source() -> MarketDataSource:
    return app.state.market_source
```

**Accessing prices from other routes:**

```python
# Trade execution: read live price from cache
@router.post("/api/portfolio/trade")
async def execute_trade(
    trade: TradeRequest,
    price_cache: PriceCache = Depends(get_price_cache),
):
    current_price = price_cache.get_price(trade.ticker)
    if current_price is None:
        raise HTTPException(400, f"No price available for {trade.ticker}. Try again shortly.")
    # Execute trade at current_price ...


# Portfolio valuation: iterate all positions
@router.get("/api/portfolio")
async def get_portfolio(price_cache: PriceCache = Depends(get_price_cache)):
    positions = await db.get_positions()
    return {
        "positions": [
            {
                **pos,
                "current_price": price_cache.get_price(pos["ticker"]),
                "unrealized_pnl": (
                    (price_cache.get_price(pos["ticker"]) or pos["avg_cost"]) - pos["avg_cost"]
                ) * pos["quantity"],
            }
            for pos in positions
        ]
    }
```

---

## 13. Watchlist Coordination

When the watchlist changes, the market data source must be notified to track the
right set of tickers.

**Adding a ticker:**

```python
@router.post("/api/watchlist")
async def add_to_watchlist(
    payload: WatchlistAdd,
    source: MarketDataSource = Depends(get_market_source),
):
    ticker = payload.ticker.upper().strip()

    # 1. Validate ticker format (1-5 uppercase letters)
    if not ticker.isalpha() or not (1 <= len(ticker) <= 5):
        raise HTTPException(400, "Invalid ticker format")

    # 2. Persist to database
    await db.insert_watchlist(ticker)

    # 3. Notify the data source — it starts tracking on the next cycle
    #    Simulator: seeds price immediately + rebuilds Cholesky
    #    Massive: appends to ticker list, appears on next poll
    await source.add_ticker(ticker)

    return {"ticker": ticker, "status": "added"}
```

**Removing a ticker:**

```python
@router.delete("/api/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    ticker = ticker.upper().strip()

    # 1. Remove from database
    await db.delete_watchlist(ticker)

    # 2. Keep tracking if the user still holds a position in this ticker
    #    (portfolio valuation requires current prices for all held positions)
    position = await db.get_position(ticker)
    if position is None or position["quantity"] == 0:
        await source.remove_ticker(ticker)
    else:
        logger.info("Keeping %s in data source — open position of %s shares", ticker, position["quantity"])

    return {"ticker": ticker, "status": "removed"}
```

**Full add/remove flow diagram:**

```
Add ticker:
  POST /api/watchlist {ticker: "PYPL"}
    → db.insert_watchlist("PYPL")
    → source.add_ticker("PYPL")
        Simulator: _add_ticker_internal + _rebuild_cholesky + seed cache
        Massive:   append to self._tickers (appears next poll)
    → 200 {ticker: "PYPL", status: "added"}

Remove ticker (no open position):
  DELETE /api/watchlist/PYPL
    → db.delete_watchlist("PYPL")
    → position = None → source.remove_ticker("PYPL")
        Simulator: remove from GBMSimulator + _rebuild_cholesky + cache.remove
        Massive:   filter from self._tickers + cache.remove
    → 200 {ticker: "PYPL", status: "removed"}
```

---

## 14. Testing Strategy

Tests live in `backend/tests/market/`. Run with:

```bash
cd backend
uv run --extra dev pytest tests/market/ -v
uv run --extra dev pytest tests/market/ --cov=app/market --cov-report=term-missing
```

### 14.1 Unit Tests — `PriceCache`

```python
# backend/tests/market/test_cache.py
from app.market.cache import PriceCache


class TestPriceCache:

    def test_update_and_get(self):
        cache = PriceCache()
        update = cache.update("AAPL", 190.50)
        assert update.ticker == "AAPL"
        assert update.price == 190.50
        assert cache.get("AAPL") == update

    def test_first_update_is_flat(self):
        cache = PriceCache()
        update = cache.update("AAPL", 190.50)
        assert update.direction == "flat"
        assert update.previous_price == 190.50

    def test_direction_up(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        update = cache.update("AAPL", 191.00)
        assert update.direction == "up"
        assert update.change == 1.00

    def test_direction_down(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        update = cache.update("AAPL", 189.00)
        assert update.direction == "down"

    def test_remove(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        cache.remove("AAPL")
        assert cache.get("AAPL") is None

    def test_get_all_returns_copy(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        cache.update("GOOGL", 175.00)
        all_prices = cache.get_all()
        assert set(all_prices.keys()) == {"AAPL", "GOOGL"}

    def test_version_increments_on_update(self):
        cache = PriceCache()
        v0 = cache.version
        cache.update("AAPL", 190.00)
        assert cache.version == v0 + 1
        cache.update("AAPL", 191.00)
        assert cache.version == v0 + 2

    def test_version_unchanged_after_remove(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        v = cache.version
        cache.remove("AAPL")
        # remove does not bump version (SSE change detection is fine with this)
        assert cache.version == v

    def test_get_price_convenience(self):
        cache = PriceCache()
        cache.update("AAPL", 190.50)
        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("NOPE") is None

    def test_thread_safety(self):
        """Concurrent writes from multiple threads should not corrupt state."""
        import threading
        cache = PriceCache()
        errors = []

        def writer(ticker, price):
            try:
                for _ in range(100):
                    cache.update(ticker, price)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(f"T{i}", float(i))) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(cache.get_all()) == 5
```

### 14.2 Unit Tests — `GBMSimulator`

```python
# backend/tests/market/test_simulator.py
from app.market.simulator import GBMSimulator
from app.market.seed_prices import SEED_PRICES


class TestGBMSimulator:

    def test_step_returns_all_tickers(self):
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        assert set(sim.step().keys()) == {"AAPL", "GOOGL"}

    def test_prices_are_always_positive(self):
        sim = GBMSimulator(tickers=["AAPL"])
        for _ in range(10_000):
            assert sim.step()["AAPL"] > 0

    def test_initial_prices_match_seeds(self):
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim.get_price("AAPL") == SEED_PRICES["AAPL"]

    def test_unknown_ticker_gets_random_seed(self):
        sim = GBMSimulator(tickers=["ZZZZ"])
        price = sim.get_price("ZZZZ")
        assert 50.0 <= price <= 300.0

    def test_empty_simulator(self):
        sim = GBMSimulator(tickers=[])
        assert sim.step() == {}

    def test_add_ticker(self):
        sim = GBMSimulator(tickers=["AAPL"])
        sim.add_ticker("TSLA")
        assert "TSLA" in sim.step()

    def test_add_ticker_duplicate_is_noop(self):
        sim = GBMSimulator(tickers=["AAPL"])
        sim.add_ticker("AAPL")
        assert len(sim.get_tickers()) == 1

    def test_remove_ticker(self):
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        sim.remove_ticker("GOOGL")
        result = sim.step()
        assert "GOOGL" not in result
        assert "AAPL" in result

    def test_remove_nonexistent_is_noop(self):
        sim = GBMSimulator(tickers=["AAPL"])
        sim.remove_ticker("NOPE")  # Should not raise

    def test_cholesky_none_for_single_ticker(self):
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim._cholesky is None

    def test_cholesky_built_for_multiple_tickers(self):
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        assert sim._cholesky is not None

    def test_cholesky_rebuilt_on_add(self):
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim._cholesky is None
        sim.add_ticker("GOOGL")
        assert sim._cholesky is not None

    def test_cholesky_valid_for_all_10_default_tickers(self):
        """Full default watchlist should produce a valid PSD correlation matrix."""
        tickers = list(SEED_PRICES.keys())
        sim = GBMSimulator(tickers=tickers)
        assert sim._cholesky is not None
        # If Cholesky succeeded, the matrix is valid PSD
        result = sim.step()
        assert len(result) == 10

    def test_high_vol_ticker_moves_more_than_low_vol(self):
        """TSLA (sigma=0.50) should produce larger moves than V (sigma=0.17)."""
        import numpy as np

        tsla_sim = GBMSimulator(tickers=["TSLA"])
        v_sim = GBMSimulator(tickers=["V"])

        tsla_changes = []
        v_changes = []
        for _ in range(1000):
            p0 = tsla_sim.get_price("TSLA")
            tsla_sim.step()
            p1 = tsla_sim.get_price("TSLA")
            tsla_changes.append(abs(p1 - p0))

            p0 = v_sim.get_price("V")
            v_sim.step()
            p1 = v_sim.get_price("V")
            v_changes.append(abs(p1 - p0))

        assert np.mean(tsla_changes) > np.mean(v_changes)
```

### 14.3 Integration Tests — `SimulatorDataSource`

```python
# backend/tests/market/test_simulator_source.py
import asyncio
import pytest
from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource


@pytest.mark.asyncio
class TestSimulatorDataSource:

    async def test_start_seeds_cache(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL", "GOOGL"])

        # Cache must be seeded before the loop runs (first SSE tick gets data)
        assert cache.get("AAPL") is not None
        assert cache.get("GOOGL") is not None

        await source.stop()

    async def test_prices_update_during_loop(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.05)
        await source.start(["AAPL"])

        v_before = cache.version
        await asyncio.sleep(0.25)  # ~5 update cycles
        v_after = cache.version

        assert v_after > v_before
        await source.stop()

    async def test_add_ticker_seeds_immediately(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.5)
        await source.start(["AAPL"])

        await source.add_ticker("TSLA")
        # Should be in cache immediately, not after the next loop tick
        assert cache.get("TSLA") is not None

        await source.stop()

    async def test_remove_ticker_clears_cache(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.5)
        await source.start(["AAPL", "TSLA"])

        await source.remove_ticker("TSLA")
        assert cache.get("TSLA") is None
        assert "TSLA" not in source.get_tickers()

        await source.stop()

    async def test_stop_is_idempotent(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])
        await source.stop()
        await source.stop()  # Should not raise
```

### 14.4 Unit Tests — `MassiveDataSource` (Mocked)

```python
# backend/tests/market/test_massive.py
from unittest.mock import MagicMock, patch, AsyncMock
import pytest
from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource


def make_snapshot(ticker: str, price: float, timestamp_ms: int = 1707580800000) -> MagicMock:
    snap = MagicMock()
    snap.ticker = ticker
    snap.last_trade.price = price
    snap.last_trade.timestamp = timestamp_ms
    return snap


@pytest.mark.asyncio
class TestMassiveDataSource:

    async def test_poll_updates_cache(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL", "GOOGL"]

        snapshots = [make_snapshot("AAPL", 190.50), make_snapshot("GOOGL", 175.25)]
        with patch.object(source, "_fetch_snapshots", return_value=snapshots):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("GOOGL") == 175.25

    async def test_timestamp_converted_from_ms_to_seconds(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL"]

        # Timestamp is 1707580800000 ms = 1707580800.0 s
        snapshots = [make_snapshot("AAPL", 190.50, timestamp_ms=1707580800000)]
        with patch.object(source, "_fetch_snapshots", return_value=snapshots):
            await source._poll_once()

        update = cache.get("AAPL")
        assert update.timestamp == pytest.approx(1707580800.0)

    async def test_malformed_snapshot_skipped(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL", "BAD"]

        good = make_snapshot("AAPL", 190.50)
        bad = MagicMock()
        bad.ticker = "BAD"
        bad.last_trade = None  # AttributeError when accessing .price

        with patch.object(source, "_fetch_snapshots", return_value=[good, bad]):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("BAD") is None

    async def test_api_error_does_not_crash_loop(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL"]

        with patch.object(source, "_fetch_snapshots", side_effect=Exception("network error")):
            await source._poll_once()  # Must not raise

    async def test_add_ticker_normalized(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        source._tickers = []

        await source.add_ticker("  aapl  ")
        assert "AAPL" in source.get_tickers()

    async def test_remove_ticker_clears_cache(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL"]
        cache.update("AAPL", 190.00)

        await source.remove_ticker("AAPL")
        assert "AAPL" not in source.get_tickers()
        assert cache.get("AAPL") is None

    async def test_empty_ticker_list_skips_poll(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        source._tickers = []

        with patch.object(source, "_fetch_snapshots") as mock_fetch:
            await source._poll_once()
            mock_fetch.assert_not_called()
```

### 14.5 Factory Tests

```python
# backend/tests/market/test_factory.py
import os
from unittest.mock import patch
from app.market.cache import PriceCache
from app.market.factory import create_market_data_source
from app.market.simulator import SimulatorDataSource
from app.market.massive_client import MassiveDataSource


class TestFactory:

    def test_returns_simulator_when_no_key(self):
        cache = PriceCache()
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("MASSIVE_API_KEY", None)
            source = create_market_data_source(cache)
        assert isinstance(source, SimulatorDataSource)

    def test_returns_simulator_when_key_empty(self):
        cache = PriceCache()
        with patch.dict(os.environ, {"MASSIVE_API_KEY": ""}):
            source = create_market_data_source(cache)
        assert isinstance(source, SimulatorDataSource)

    def test_returns_simulator_when_key_whitespace(self):
        cache = PriceCache()
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "   "}):
            source = create_market_data_source(cache)
        assert isinstance(source, SimulatorDataSource)

    def test_returns_massive_when_key_set(self):
        cache = PriceCache()
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "real-api-key"}):
            source = create_market_data_source(cache)
        assert isinstance(source, MassiveDataSource)
```

---

## 15. Error Handling & Edge Cases

### Empty Watchlist at Startup

If the database has no watchlist entries, `start([])` is called. Both sources
handle this gracefully — the simulator produces no prices, Massive skips the API
call. SSE sends empty events. When the user adds a ticker, the source starts
tracking it immediately.

### Price Cache Miss During Trade Execution

A user might try to trade a ticker that was just added to the watchlist before
the Massive client has polled. The simulator avoids this by seeding the cache in
`add_ticker()`, but the Massive path may have a brief gap.

```python
current_price = price_cache.get_price(trade.ticker)
if current_price is None:
    raise HTTPException(
        status_code=400,
        detail=f"Price not yet available for {trade.ticker}. Please wait a moment.",
    )
```

### Invalid Massive API Key

If `MASSIVE_API_KEY` is set but invalid, the first poll returns 401. The poller
logs the error and keeps retrying every `poll_interval` seconds. The SSE endpoint
streams empty data. Users see a connected (green) status indicator but no prices.
Fix: correct the key and restart.

### Ticker with Open Position Removed from Watchlist

If a user removes a ticker from the watchlist but still holds shares, keep tracking
it in the data source for accurate portfolio valuation. See section 13 for the
implementation pattern.

### Simulator Numerical Stability

GBM with the chosen `dt` is numerically stable:
- `exp()` is always positive → prices can never go negative
- Very small `dt` → very small per-tick moves → no risk of price explosion
- `round(price, 2)` on every step prevents floating-point accumulation drift

---

## 16. Configuration Reference

| Parameter | Where set | Default | Description |
|-----------|-----------|---------|-------------|
| `MASSIVE_API_KEY` | Environment variable | `""` | If set and non-empty, use Massive API. Otherwise use simulator. |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` s | Simulator tick rate |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` s | Massive API poll rate (free tier limit) |
| `event_probability` | `GBMSimulator.__init__` | `0.001` | Random shock chance per ticker per tick (~1 event/50s across 10 tickers) |
| `dt` | `GBMSimulator.DEFAULT_DT` | `~8.48e-8` | Time step as fraction of a trading year (500ms / 5,896,800s) |
| SSE poll interval | `_generate_events(interval=...)` | `0.5` s | Cache read + push cadence |
| SSE retry directive | hardcoded in `_generate_events` | `1000` ms | Browser EventSource reconnection delay |

**pyproject.toml requirements:**

```toml
[project]
dependencies = [
    "fastapi>=0.110",
    "uvicorn[standard]>=0.29",
    "numpy>=1.26",
    "massive>=1.0.0",   # Polygon.io REST client
]

[tool.hatch.build.targets.wheel]
packages = ["app"]      # Required for uv sync to find the app package
```

**Running tests:**

```bash
cd backend

# All market data tests
uv run --extra dev pytest tests/market/ -v

# With coverage
uv run --extra dev pytest tests/market/ --cov=app/market --cov-report=term-missing

# Run the Rich terminal demo (live price display)
uv run market_data_demo.py
```
