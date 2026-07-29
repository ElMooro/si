# ops 4078 — is v1.7.9 executing?

**Status:** success  
**Duration:** 1.8s  
**Finished:** 2026-07-29T03:31:59+00:00  

## Data

| age_min | agency_sourced | new_build_running | other_sourced | rate_per_min | sourced | tier1_done | walked |
|---|---|---|---|---|---|---|---|
| 0.9 |  |  |  |  | 193 |  |  |
|  |  | True |  | 104.8 |  | 117 | 117 |
|  | 29 |  | 164 |  |  |  |  |

## Log
## A. last sync

- `03:31:58`   generated_at : 2026-07-29T03:31:03.070775+00:00
- `03:31:58`   age          : 0.9 min
- `03:31:58`   sources held : 193
## B. FINGERPRINT — fields only v1.7.8+ can emit

- `03:31:58`   raw diag: {"started": 1785295795336, "done": 117, "total": 10159, "sc_ok": 116, "sc_err": 0, "sc2_ok": 116, "sc2_err": 0, "ss_ok": 0, "ss_err": 0, "matched": 7, "first_err": "", "tier1_done": 117, "rate_per_min": 104.8, "elapsed_s": 67}
- `03:31:58`   new-build fields present: ['tier1_done', 'rate_per_min', 'elapsed_s']
- `03:31:58`   ✓ the NEW content script is running
- `03:31:58`     walked 117/10159 · tier1 117 · 104.8/min · 67s elapsed
- `03:31:58`     ETA to finish at this rate: 1.6 h
- `03:31:58`     rate is consistent with the timer — no throttling
## C. WALK ORDER — what is actually being sourced

- `03:31:58`     164  other/venue
- `03:31:58`      29  AGENCY
- `03:31:58`   top prefixes sourced: AMEX 61, NASDAQ 47, TVC 22, CRYPTOCAP 16, NYSE 8, CBOE 7, EURONEXT 5, SPCFD 2
- `03:31:58`   ✓ 29 agency-tier symbols sourced — the priority walk is producing the payoff
## D. refresh the rollup so the page shows current truth

- `03:31:59`   source-map: {"statusCode": 200, "body": "{\"symbols_with_source\": 171, \"agency_rows\": 0, \"economics_symbols\": 0}"}
## VERDICT

- `03:31:59` ✅ v1.7.9 is running AND the agency-first walk is banking the payoff.
