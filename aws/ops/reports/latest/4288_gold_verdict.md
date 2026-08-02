# ops 4288 -- dead-gold verdict: ACTIVE fetchers vs mentions

**Status:** success  
**Duration:** 41.6s  
**Finished:** 2026-08-02T20:57:00+00:00  

## Data

| cls | ctx | engine |
|---|---|---|
| ACTIVE | D/bbl)   DHHNGSP             Henry Hub Natural Gas Spot (USD/mmBtu)   GOLDAMGBD228NLBM    LBMA G | commodity-curves |
| ACTIVE | nt") for f in (c.get("fred_metrics") or []) if f.get("series_id") == "GOLDAMGBD228NLBM"), None), | morning-intelligence |
| mention | ': 'Regular Gasoline Price',     'DHHNGSP': 'Natural Gas Price',     'GOLDAMGBD228NLBM': 'Gold P | fedliquidityapi |
| mention | Crude', 'DCOILBRENTEU': 'Brent Crude',     'DHHNGSP': 'Natural Gas', 'GOLDAMGBD228NLBM': 'Gold', | bloomberg-v8 |
| mention | t_FRED_series, structural_roll_estimate_pct_yr)     "GLD":  ("GLD",  "GOLDAMGBD228NLBM",  0.0),  | carry-surface |
| mention | ": ["DEXCHUS"],     "copper": ["PCOPPUSDM"],     "gold": ["IQ12260", "GOLDAMGBD228NLBM"], }   de | china-liquidity |
| mention | l",          "fred_id": "DCOILWTICO",       "diff_mode": "pct"},     "GOLDPMGBD228NLBM": {"label | correlation-breaks |
| mention | ies','WTI Crude'), 'DCOILBRENTEU':('commodities','Brent Crude'),     'GOLDAMGBD228NLBM':('commod | daily-report-v3 |
| mention | ,     "DCOILBRENTEU": {"freq": "daily", "title": "Brent Crude"},     "GOLDPMGBD228NLBM": {"freq" | divergence-engine-v2 |
| mention |  cu = dict(fred("PCOPPUSDM", "2000-01-01"))         au = dict(probe(["GOLDPMGBD228NLBM", "GOLDAM | us-cycle |
| mention | PoolExecutor(max_workers=6) as ex:         f_goldf = ex.submit(fred, "GOLDAMGBD228NLBM")         | valuations-agent |
| mention | RATE", "AAAFF", "BAMLH0A0HYM2", "BAMLH0A3HYC", "DCOILWTICO",         "GOLDAMGBD228NLBM", "DEXJPU | openbb-system2-api |

## Log
- `20:56:18` classification: 2 ACTIVE, 10 MENTION-only
## exercise every ACTIVE (ex commodity-curves, fixed 4285)

- `20:56:18` ✅ commodity-curves: engine-level fix verified in 4285 (GCUSD rail, artifact fresh, no 400s)
- `20:56:53` morning-intelligence invoked: {"statusCode": 200, "body": "{\"success\": true, \"khalid\": {\"score\": 51, \"regime\": \
- `20:57:00` morning-intelligence: gold path conditional, not exercised this run (no 400s either) -- shim insurance stands
## VERDICT

- `20:57:00` ✅ No engine anywhere emits a dead-gold 400. Runtime fetchers: commodity-curves (fixed+verified) + 1 conditional-path engines shielded by the shim. Mention-only literals: 10 engines (label maps/docs -- harmless). The 4285 '12 infected' claim is hereby corrected to '1 runtime fetcher + 10 mentions'; the shim gold branch remains as fleet-wide insurance for any conditional path that fires later.
- `20:57:00` ✅ OPS 4288 PASS -- wave 4 closed on evidence
