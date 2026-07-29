# ops 4099 — full check, read-only

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-07-29T21:52:44+00:00  

## Data

| cot_live | distinct | live | map_age_h | marker | n_new | pending | rows | store_age_h | store_n | symbols_with_notes | symbols_with_source | symbols_with_tv_source | unique_symbols | vault_age_h | vault_marker | watchlists |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  | 0.1 | 232 |  |  |  |  |  |  |  |
|  | 29 |  | 9.5 | source-map engine v2.1 ops4083 | 15 |  |  |  |  |  | 197 |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | 771 |  | 197 | 10319 |  |  | 491 |
| 0 |  | 1176 |  |  |  | 0 | 1358 |  |  |  |  |  |  | 0.8 | tradingview-vault v3.13.1 ops4094 cftc-alias |  |

## Log
## A. harvest store

- `21:52:43`   DIAG: {"started": 1785360540970, "done": 148, "total": 10116, "sc_ok": 143, "sc_err": 5, "sc2_ok": 143, "sc2_err": 5, "ss_ok": 0, "ss_err": 0, "matched": 0, "first_err": "sc2:TypeError: Failed to fetch", "tier1_done": 148, "rate_per_min": 10.1, "elapsed_s": 882, "delay_ms": 21530, "wall_events": 0, "recov
## B. source-map — the delta deliverable

- `21:52:43`   KNOWN: MARKET-VENUES:165
- `21:52:43`   NEW    16  CRYPTOCAP   e.g. CRYPTOCAP:TOTAL3, CRYPTOCAP:BTC.D, CRYPTOCAP:TOTAL2
- `21:52:43`   NEW     2  US   e.g. SPCFD:SPX, SPCFD:SPF
- `21:52:43`   NEW     2  ICEUS   e.g. ICEUS:AWN1!, ICEUS:MWL1!
- `21:52:43`   NEW     1  OSE   e.g. OSE:TOPIX1!
- `21:52:43`   NEW     1  LSE   e.g. LSE:0LMQ
- `21:52:43`   NEW     1  COMEX   e.g. COMEX:HG1!
- `21:52:43`   NEW     1  GPW   e.g. GPW:WIG20
- `21:52:43`   NEW     1  NYMEX   e.g. NYMEX:CL1!
- `21:52:43`   NEW     1  FTSE   e.g. FTSE:UKX
- `21:52:43`   NEW     1  HOSE   e.g. HOSE:VN30
- `21:52:43`   NEW     1  VIE   e.g. VIE:ATX
- `21:52:43`   NEW     1  DJ   e.g. DJ:W1DOW
- `21:52:43`   NEW     1  CSELK   e.g. CSELK:ASI
- `21:52:43`   NEW     1  PSE   e.g. PSE:PSEI
- `21:52:43`   NEW     1  SPARKS   e.g. SPARKS:BANKS
## C. workbench page

## D. vault mirror + COT wound

- `21:52:44` ✅ CHECK DONE — store 232 / map 197 src 15 NEW / page 197 sourced / vault 1176 LIVE 0 pending, cot 0
