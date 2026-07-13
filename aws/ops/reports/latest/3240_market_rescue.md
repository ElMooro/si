# ops 3240 — MARKET-miss sub-census + rescue ladders

**Status:** success  
**Duration:** 54.7s  
**Finished:** 2026-07-13T07:51:06+00:00  

## Data

| active_before | active_now | ber_tried | coverage_now | curations | n_fails | n_warns | nasdaq_tried | verdict | woken |
|---|---|---|---|---|---|---|---|---|---|
|  |  | 13 |  | 9 |  |  | 25 |  |  |
|  |  |  | 74.1 |  |  |  |  |  |  |
| 131 | 131 |  |  |  |  |  |  |  | 0 |
|  |  |  |  |  | 0 | 0 |  | PASS |  |

## Log
## 1. MARKET misses by exchange

- `07:50:12`    370 × NASDAQ     e.g. NASDAQ:NQJPN ''
- `07:50:12`    136 × INDEX      e.g. INDEX:ADVN 'ADVN (INDEX)'
- `07:50:12`     43 × SSE        e.g. SSE:000057 '000057 (SSE)'
- `07:50:12`     33 × CBOE       e.g. CBOE:SHORTVOL ''
- `07:50:12`     27 × EURONEXT   e.g. EURONEXT:MTH 'MTH (EURONEXT)'
- `07:50:12`     18 × TRADEGATE  e.g. TRADEGATE:FJ2B 'FJ2B (TRADEGATE)'
- `07:50:12`     14 × SWB        e.g. SWB:FJ2B 'FJ2B (SWB)'
- `07:50:12`     13 × FX         e.g. FX:2USNOTE ''
- `07:50:12`     13 × BER        e.g. BER:P3WC 'P3WC (BER)'
- `07:50:12`     10 × SIX        e.g. SIX:LYCNB.EUR 'LYCNB.EUR (SIX)'
- `07:50:12`     10 × HKEX       e.g. HKEX:388 '388 (HKEX)'
- `07:50:12`      9 × FWB        e.g. FWB:ODEF 'ODEF (FWB)'
## 2. BER + NASDAQ-index ladders (probe-gated)

- `07:50:23`   ✓ BER:DX2Z           → DX2Z.DE      (4684)  'DX2Z (BER)'
- `07:50:28`   ✓ NASDAQ:CRSPLCG1    → ^CRSPLCG1    (3477)  ''
- `07:50:28`   ✓ NASDAQ:CRSPLCGT    → ^CRSPLCGT    (6292)  ''
- `07:50:28`   ✓ NASDAQ:CRSPLCV1    → ^CRSPLCV1    (3477)  ''
- `07:50:28`   ✓ NASDAQ:CRSPLCVT    → ^CRSPLCVT    (6292)  ''
- `07:50:28`   ✓ NASDAQ:CRSPMC1     → ^CRSPMC1     (3840)  ''
- `07:50:29`   ✓ NASDAQ:CRSPMIG1    → ^CRSPMIG1    (3477)  ''
- `07:50:29`   ✓ NASDAQ:CRSPMIGT    → ^CRSPMIGT    (6292)  ''
- `07:50:29`   ✓ NASDAQ:CRSPMT1     → ^CRSPMT1     (3630)  ''
## 3. Fleet — wakes by name

