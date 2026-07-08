# Memory archive — edit #20 (migrated verbatim 2026-07-08, Khalid-approved memory diet)

Source: Claude memory edit #20. This file is the authoritative archive; the memory slot now holds a one-line pointer here. Grep this directory before building anything fleet-related.

---

Data infra hardening (April 2026): FRED cache v3.2 smart TTL auto-detects per-series publishing cadence (88% hit rate, 24x speed). Keys: data/fred-cache.json + data/fred-cache-secretary.json. Secretary v2.2 fixed FRED 429 silent failure (max_workers=2, retries w/ backoff). calc_liquidity fails loudly w/ regime=UNKNOWN. ReservedConcurrentExecutions=1 on daily-report-v3.
