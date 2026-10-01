# Cross-Engine Enrichment — Adoption Guide

**Problem solved:** engines were isolated. Each hand-wired 3-8 data sources
with ad-hoc merge logic. No shared per-ticker "full picture."

## The two integration points

### 1. On-demand: `ticker_360` shared module (any engine)

```python
from ticker_360 import enrich, enrich_many

# Full 360° view of one ticker — 20 domains, fail-soft
view = enrich("AAPL", s3)
view["domains"]["dark-pool"]["ticker_data"]   # per-ticker slice
view["domains"]["short-interest"]["as_of"]    # freshness
view["confluence"]["coverage_count"]          # how many domains cover it
view["confluence"]["domains_covering"]        # which ones

# Batch (S3 packets read once, shared cache)
views = enrich_many(["AAPL", "NVDA", "TSLA"], s3)
```

A dead source marks its domain `available: False` — it never breaks the view.
To add a new data domain: **one entry in `ticker_360.SOURCES`**. Every engine
picks it up automatically. No per-engine edits.

### 2. Pre-computed: `data/ticker-360.json` (any consumer)

Built twice daily by `justhodl-ticker-360`. The unified index:

```json
{
  "generated_at": "...",
  "universe_size": 734,
  "tickers": {
    "AAPL": {
      "coverage_count": 5,
      "coverage_pct": 25.0,
      "domains": {
        "dark-pool":      {"as_of": "...", "data": {...}},
        "short-interest": {"as_of": "...", "data": {...}}
      }
    }
  }
}
```

Frontend pages and engines read **one key** instead of N.

## Migration pattern (existing engines)

Replace hand-wired multi-source blocks:

```python
# BEFORE (isolated — each engine reimplements this)
dp = __import__("offexchange_context").decision_view(_read("data/dark-pool.json"))
si = __import__("short_interest_context").decision_view(_read("data/short-interest.json"))
# ... manual ticker extraction, manual merge ...

# AFTER (enriched — full picture in one call)
from ticker_360 import enrich_many
views = enrich_many(my_tickers, s3)
for t, v in views.items():
    dp_data = v["domains"]["dark-pool"]["ticker_data"]
    si_data = v["domains"]["short-interest"]["ticker_data"]
    # ... both present, same shape, freshness attached ...
```

## Domain registry (`ticker_360.SOURCES`)

20 domains: short-interest, short-interest-tkr, finra-short-volume,
dark-pool, share-flows, squeeze-fuel-ftd, forensic, settlement-fails,
dollar, futures, fx, gold-rotation, macro-regime, flow-confluence,
squeeze-pretrigger, cboe-options, xbrl-fundamentals, sec-8k,
corporate-actions, etf-holdings.

Evidence-contract contexts are honored: each domain runs through its
registered `decision_view` before extraction.
