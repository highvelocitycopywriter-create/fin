# Massive API Reference (formerly Polygon.io)

Reference for the Massive REST API as used by FinAlly to retrieve real-time and
end-of-day US stock prices. Massive is the rebrand of Polygon.io (announced
2025-10-30); existing keys, accounts, and integrations continue to work, and the
old `api.polygon.io` host is still supported.

## Overview

| | |
|---|---|
| Base URL | `https://api.massive.com` (legacy `https://api.polygon.io` still works) |
| Python package | `massive` — `uv add massive` |
| Import | `from massive import RESTClient` |
| Auth | API key via `MASSIVE_API_KEY` env var, or `RESTClient(api_key=...)` |
| Auth transport | `Authorization: Bearer <API_KEY>` (handled by the client) |
| Coverage | All 19 US exchanges, dark pools, FINRA TRFs, OTC |
| Min Python | 3.9+ |

Sources: [massive.com](https://massive.com/), [Stocks REST overview](https://massive.com/docs/rest/stocks/overview),
[client-python](https://github.com/massive-com/client-python).

## Rate Limits

| Tier | Limit | Recommended poll interval |
|------|-------|---------------------------|
| Free | 5 requests / minute | 15s |
| Paid | High / unlimited (stay < 100 req/s) | 2-5s |

FinAlly polls on a timer rather than using WebSockets. Because the snapshot
endpoint returns **all requested tickers in a single call**, one poll covers the
entire watchlist regardless of size — this is what keeps the free tier viable.

## Client Initialization

```python
from massive import RESTClient

# Reads MASSIVE_API_KEY from the environment automatically
client = RESTClient()

# Or pass the key explicitly
client = RESTClient(api_key="your_key_here")
```

The synchronous client has built-in retries (3 by default) for transient 5xx
errors. It is blocking, so in async code it must be run in a thread (see below).

## Endpoints Used in FinAlly

### 1. Snapshot — All Tickers (primary polling endpoint)

Returns the current minute bar, day bar, previous-day bar, last trade, and last
quote for a set of tickers in **one API call**.

**REST**
```
GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT
```

Query parameters:
- `tickers` — case-sensitive comma-separated list (e.g. `AAPL,TSLA,GOOG`). Empty
  string returns *all* tickers.
- `include_otc` — include OTC securities (default `false`).

**Python client** (the form FinAlly uses)
```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient()

snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
)

for snap in snapshots:
    print(f"{snap.ticker}: ${snap.last_trade.price}")
    print(f"  Change: {snap.todays_change} ({snap.todays_change_percent}%)")
    print(f"  Day OHLC: O={snap.day.open} H={snap.day.high} "
          f"L={snap.day.low} C={snap.day.close} V={snap.day.volume}")
    print(f"  Prev close: ${snap.prev_day.close}")
```

`market_type` also accepts the plain string `"stocks"` positionally
(`client.get_snapshot_all("stocks", tickers=[...])`); the `SnapshotMarketType`
enum is the explicit, self-documenting form.

**Raw JSON response** (per ticker; client uses snake_case attribute names)
```json
{
  "ticker": "AAPL",
  "todaysChange": -4.54,
  "todaysChangePerc": -3.50,
  "updated": 1675190399000000000,
  "day":     { "o": 129.61, "h": 130.15, "l": 125.07, "c": 125.07, "v": 111237700, "vw": 127.35 },
  "min":     { "o": 125.10, "h": 125.20, "l": 125.00, "c": 125.07, "v": 12300,    "vw": 125.08 },
  "prevDay": { "o": 130.00, "h": 131.00, "l": 128.50, "c": 129.61, "v": 98000000, "vw": 129.40 },
  "lastTrade": { "p": 125.07, "s": 100, "x": 11, "t": 1675190399000, "i": "12345" },
  "lastQuote": { "P": 125.08, "S": 10, "p": 125.06, "s": 5, "t": 1675190399500 }
}
```

**JSON → Python client attribute mapping**

| JSON | Python attribute | Meaning |
|------|------------------|---------|
| `ticker` | `snap.ticker` | Symbol |
| `lastTrade.p` | `snap.last_trade.price` | Latest trade price |
| `lastTrade.t` | `snap.last_trade.timestamp` | Trade time (**Unix ms**) |
| `lastQuote.p` / `.P` | `snap.last_quote.bid_price` / `.ask_price` | NBBO bid / ask |
| `day.o/h/l/c/v/vw` | `snap.day.open/high/low/close/volume/vwap` | Today's bar |
| `prevDay.c` | `snap.prev_day.close` | Previous close (for day change) |
| `min` | `snap.min` | Most recent minute bar |
| `todaysChange` | `snap.todays_change` | Absolute change vs prev close |
| `todaysChangePerc` | `snap.todays_change_percent` | Percent change vs prev close |

**Fields FinAlly extracts:** `last_trade.price` (the live price written to the
cache) and `last_trade.timestamp` (converted from ms to seconds). Day-change can
be derived downstream, or read directly from `todays_change_percent`.

### 2. Snapshot — Single Ticker

For an on-demand detail view of one ticker.

```python
snap = client.get_snapshot_ticker(
    market_type=SnapshotMarketType.STOCKS,
    ticker="AAPL",
)
print(f"Price: ${snap.last_trade.price}")
print(f"Bid/Ask: ${snap.last_quote.bid_price} / ${snap.last_quote.ask_price}")
print(f"Day range: ${snap.day.low} - ${snap.day.high}")
```

### 3. Previous Close (end-of-day)

Previous trading day's OHLCV for one ticker — useful for seeding prices or an
end-of-day reference.

**REST:** `GET /v2/aggs/ticker/{ticker}/prev`

```python
prev = client.get_previous_close_agg(ticker="AAPL")
for agg in prev:
    print(f"Prev close: ${agg.close}  O={agg.open} H={agg.high} "
          f"L={agg.low} V={agg.volume}")
```

```json
{ "ticker": "AAPL", "results": [
  { "o": 150.0, "h": 155.0, "l": 149.0, "c": 154.5, "v": 1000000, "t": 1672531200000 }
] }
```

### 4. Aggregates / Bars (historical)

Historical OHLCV over a date range — not needed for live polling, but available
for historical charts.

**REST:** `GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}`

```python
bars = []
for a in client.list_aggs(
    ticker="AAPL",
    multiplier=1,
    timespan="day",        # second | minute | hour | day | week | month
    from_="2025-01-01",
    to="2025-01-31",
    limit=50000,
):
    bars.append(a)

for a in bars:
    print(f"{a.timestamp}: O={a.open} H={a.high} L={a.low} C={a.close} V={a.volume}")
```

`list_aggs` is a generator that transparently follows pagination.

### 5. Last Trade / Last Quote

Single most-recent trade or NBBO quote for one ticker.

```python
trade = client.get_last_trade(ticker="AAPL")
print(f"Last trade: ${trade.price} x {trade.size}")

quote = client.get_last_quote(ticker="AAPL")
print(f"Bid {quote.bid_price} x {quote.bid_size} / "
      f"Ask {quote.ask_price} x {quote.ask_size}")
```

## How FinAlly Polls

The Massive poller runs as a background task and writes into the shared price
cache. Because the client is synchronous, each network call is offloaded to a
thread so it never blocks the asyncio event loop.

```python
import asyncio
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

async def poll_loop(api_key, get_tickers, cache, interval=15.0):
    client = RESTClient(api_key=api_key)
    while True:
        tickers = get_tickers()
        if tickers:
            snapshots = await asyncio.to_thread(
                client.get_snapshot_all,
                market_type=SnapshotMarketType.STOCKS,
                tickers=tickers,
            )
            for snap in snapshots:
                cache.update(
                    ticker=snap.ticker,
                    price=snap.last_trade.price,
                    timestamp=snap.last_trade.timestamp / 1000.0,  # ms -> s
                )
        await asyncio.sleep(interval)
```

The real implementation is in `backend/app/market/massive_client.py`
(`MassiveDataSource`), which adds an immediate first poll on `start()`,
per-snapshot error isolation, and graceful loop-level error handling. See
[MARKET_INTERFACE.md](MARKET_INTERFACE.md).

## Error Handling

| Status | Cause | FinAlly behavior |
|--------|-------|------------------|
| 401 | Invalid API key | Logged; loop retries next interval |
| 403 | Plan lacks the endpoint | Logged; loop retries |
| 429 | Rate limit (free tier 5/min) | Logged; loop retries (widen interval) |
| 5xx | Server error | Client auto-retries (3x), then logged |

The poller never re-raises from its loop — a failed poll just leaves the last
good prices in the cache and the next interval tries again. Individual malformed
snapshots are skipped (caught `AttributeError`/`TypeError`) without aborting the
whole batch.

## Notes / Gotchas

- The all-tickers snapshot returns every requested ticker in **one** call —
  essential for free-tier rate limits.
- `last_trade.timestamp` is Unix **milliseconds**; the `updated` field is Unix
  **nanoseconds**. FinAlly normalizes to Unix seconds in the cache.
- When the market is closed, `last_trade.price` reflects the last traded price
  (may include after-hours).
- The `day` bar resets at market open; during pre-market its values may still be
  from the prior session — `prev_day` is the reliable previous-close reference.
- Ticker symbols are case-sensitive in the `tickers` query parameter.
