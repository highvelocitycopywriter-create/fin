# Review: `planning/MARKET_SIMULATOR.md`

## Findings

1. **Medium** - The Cholesky section overstates the guarantee. `np.linalg.cholesky` requires a positive definite matrix, not merely a positive semidefinite one. The current wording says it "guarantees the resulting covariance structure for any valid (positive semi-definite) correlation matrix," which is mathematically too broad and could mislead future edits to the correlation table. Narrow the claim or note that degenerate matrices will still fail at runtime.

2. **Medium** - The complexity note under the rebuild path is understated. The doc calls the rebuild `O(n^2)`, but `_rebuild_cholesky()` also performs `np.linalg.cholesky(corr)`, which is an `O(n^3)` factorization. That is probably fine for the current small watchlist, but the document should either state the full cost or explicitly tie the design to a small, bounded ticker set.

3. **Low** - The test command is likely wrong from the repository root. The doc suggests `uv run --extra dev pytest tests/market/ -v`, but the project is defined in `backend/pyproject.toml` and the tests live under `backend/tests/market/`. As written, the command only works if the user first changes into `backend/`. Update the command to match the repo layout, e.g. `cd backend && uv run --extra dev pytest tests/market/ -v`.

## Summary

The design is coherent and matches the implementation closely. The main issues are one mathematical overclaim, one understated performance note, and one test command that will confuse anyone running it from the repo root.
