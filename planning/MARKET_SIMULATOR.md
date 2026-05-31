# Market Simulator Design

Approach and code structure for simulating realistic stock prices when no Massive
API key is configured. This is the **default** data source — FinAlly runs fully
offline with no external dependency or key.

This document reflects `backend/app/market/simulator.py` and
`backend/app/market/seed_prices.py`.

## Why GBM

The simulator uses **Geometric Brownian Motion (GBM)** — the same process that
underlies Black-Scholes. It is the natural fit because:

- Prices evolve continuously with random noise — the stream *feels alive*.
- Prices can never go negative (the move is multiplicative via `exp()`).
- Returns are lognormal, matching real market behavior.

Updates run every ~500ms, producing a steady flow of small, believable moves.

## The Math

At each step, a price evolves as:

```
S(t+dt) = S(t) * exp( (mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z )
```

| Symbol | Meaning |
|--------|---------|
| `S(t)` | current price |
| `mu` | annualized drift (expected return), e.g. `0.05` |
| `sigma` | annualized volatility, e.g. `0.20` |
| `dt` | time step as a fraction of a trading year |
| `Z` | a correlated standard-normal draw, `N(0,1)` |

**Choosing `dt`.** A trading year is `252 days * 6.5 hours * 3600 s = 5,896,800`
seconds. A 500ms tick is therefore:

```
dt = 0.5 / 5_896_800 ≈ 8.48e-8
```

This tiny `dt` yields sub-cent moves per tick that accumulate naturally — over a
simulated day the range looks right (e.g. `sigma=0.50` for TSLA produces a
realistic intraday swing).

## Correlated Moves (Cholesky)

Real stocks don't move independently — tech names rise and fall together. The
simulator draws **correlated** normals via a **Cholesky decomposition** of a
correlation matrix `C`:

```
L = cholesky(C)              # lower-triangular
Z_correlated = L @ Z_independent
```

Cholesky reproduces the target covariance structure for any **positive definite**
correlation matrix. `np.linalg.cholesky` requires positive definiteness, not mere
positive semi-definiteness — a degenerate (rank-deficient) matrix will fail at
runtime, which is the signal to revisit the correlation table.

Correlation structure (from `seed_prices.py`):

| Pair | Coefficient | Constant |
|------|-------------|----------|
| Same tech sector | 0.6 | `INTRA_TECH_CORR` |
| Same finance sector | 0.5 | `INTRA_FINANCE_CORR` |
| Anything with TSLA | 0.3 | `TSLA_CORR` |
| Cross-sector / unknown | 0.3 | `CROSS_GROUP_CORR` |

Groups: **tech** = {AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX}, **finance** =
{JPM, V}. TSLA is listed in tech but is forced to 0.3 with everything — it does
its own thing.

The matrix is rebuilt whenever a ticker is added or removed. Rebuilding fills an
`O(n^2)` matrix and then runs an `O(n^3)` Cholesky factorization, but `n` stays
small — well under 50 — so the cost is negligible.

## Random Events

Each step, each ticker has a small probability (`event_probability=0.001`) of a
sudden 2-5% shock for drama:

```python
if random.random() < self._event_prob:
    shock_magnitude = random.uniform(0.02, 0.05)
    shock_sign = random.choice([-1, 1])
    self._prices[ticker] *= 1 + shock_magnitude * shock_sign
```

At ~0.1% per tick, with 2 ticks/sec and 10 tickers, expect an event roughly every
50 seconds somewhere on the board — enough to keep it interesting.

## Seed Prices & Per-Ticker Parameters

Realistic starting prices and individualized volatility/drift give each ticker a
distinct personality. From `seed_prices.py`:

```python
SEED_PRICES = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00,
    "TSLA": 250.00, "NVDA": 800.00, "META": 500.00, "JPM": 195.00,
    "V": 280.00, "NFLX": 600.00,
}

TICKER_PARAMS = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},  # high volatility
    "NVDA":  {"sigma": 0.40, "mu": 0.08},  # high vol, strong drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},  # low vol (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},  # low vol (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

DEFAULT_PARAMS = {"sigma": 0.25, "mu": 0.05}
```

Tickers added dynamically (not in `SEED_PRICES`) start at a random price in
`[50, 300]` and use `DEFAULT_PARAMS`.

## Implementation — `GBMSimulator`

A pure, synchronous price-path generator. No async, no I/O — just `step()`. The
async wrapper (`SimulatorDataSource`) lives alongside it and is documented in
[MARKET_INTERFACE.md](MARKET_INTERFACE.md).

```python
import math, random
import numpy as np

class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600       # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR        # ~8.48e-8

    def __init__(self, tickers, dt=DEFAULT_DT, event_probability=0.001):
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None
        for t in tickers:
            self._add_ticker_internal(t)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance all tickers one tick. Returns {ticker: rounded_price}."""
        n = len(self._tickers)
        if n == 0:
            return {}

        z = np.random.standard_normal(n)
        if self._cholesky is not None:
            z = self._cholesky @ z                      # correlate the draws

        result = {}
        for i, ticker in enumerate(self._tickers):
            p = self._params[ticker]
            drift = (p["mu"] - 0.5 * p["sigma"] ** 2) * self._dt
            diffusion = p["sigma"] * math.sqrt(self._dt) * z[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            if random.random() < self._event_prob:      # random shock
                shock = random.uniform(0.02, 0.05) * random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock

            result[ticker] = round(self._prices[ticker], 2)
        return result

    def add_ticker(self, ticker): ...      # adds + rebuilds Cholesky
    def remove_ticker(self, ticker): ...   # removes + rebuilds Cholesky
    def get_price(self, ticker) -> float | None: ...
    def get_tickers(self) -> list[str]: ...
```

`step()` is the hot path — called twice a second — so it batches the random draws
through NumPy (`np.random.standard_normal(n)` then one matrix multiply) rather
than looping per ticker.

### Cholesky rebuild

```python
def _rebuild_cholesky(self):
    n = len(self._tickers)
    if n <= 1:
        self._cholesky = None      # single ticker: no correlation to apply
        return
    corr = np.eye(n)
    for i in range(n):
        for j in range(i + 1, n):
            rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
            corr[i, j] = corr[j, i] = rho
    self._cholesky = np.linalg.cholesky(corr)
```

`_pairwise_correlation` looks up the two tickers against the sector sets and
returns the coefficient from the table above (TSLA short-circuits to 0.3 first).

## File Structure

```
backend/app/market/
  simulator.py     # GBMSimulator (math) + SimulatorDataSource (async wrapper)
  seed_prices.py   # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS,
                   # CORRELATION_GROUPS, correlation constants
```

`seed_prices.py` holds only constants. `simulator.py` holds the logic. The async
`SimulatorDataSource` (the `MarketDataSource` implementation) ticks `step()` every
500ms and writes to the `PriceCache`.

## Behavior Notes

- **Never negative** — GBM is multiplicative; `exp()` is always positive.
- **Per-tick moves are sub-cent** — they accumulate into believable trends.
- **Higher `sigma` = livelier** — TSLA (`0.50`) jumps; JPM/V (`~0.17`) drift gently.
- **Correlation matrix stays valid** — Cholesky requires PSD; the sector
  coefficients keep it well-conditioned. If a degenerate matrix ever arose,
  `np.linalg.cholesky` would raise — a signal the correlation table needs review.
- **Dynamic tickers** — adding mid-session rebuilds Cholesky (`O(n^2)`, cheap for
  small `n`) and seeds a starting price.
- **Reproducibility** — seeding `random` and `np.random` before `step()` makes a
  run deterministic, which the tests rely on.

## Testing

Covered by `backend/tests/market/test_simulator.py` and
`test_simulator_source.py`:

- Prices stay positive across many steps.
- GBM moves scale with `sigma` (high-vol tickers move more than low-vol).
- Correlated draws show the expected positive correlation within a sector.
- `add_ticker` / `remove_ticker` rebuild correctly and the Cholesky matrix stays
  valid.
- The async source seeds the cache on `start()` and ticks on the interval.

Run with `cd backend && uv run --extra dev pytest tests/market/ -v` (the project
is defined in `backend/pyproject.toml`).
