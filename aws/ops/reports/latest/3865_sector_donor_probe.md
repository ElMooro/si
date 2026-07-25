# ops 3865 — PROBE: donor to close 8 unresolved + is RORO 0/25 correct

**Status:** success  
**Duration:** 3.5s  
**Finished:** 2026-07-25T16:41:32+00:00  

## Data

| closes | donor_pairs | feed_age_h | recommended_donor | risk_regime | risk_regime_score | sectors_seen | tickers_outside_roro_sets |
|---|---|---|---|---|---|---|---|
| 8 | 11519 |  | data/finviz-universe.json |  |  |  |  |
|  |  | 17.9 |  | NEUTRAL | 2.5 |  |  |
|  |  |  |  |  |  | 6 | 1 |

## Log
- `16:41:29`   25 ranked tickers · 8 unresolved from ops 3863
## 1. current donors — keep or drop

- `16:41:30` ✅   screener/data.json                      503 pairs · keep (15/25)
- `16:41:30` ✗   data/capital-flow-radar.json              0 pairs · DROP — contributes nothing
- `16:41:30` ✅   data/deep-value.json                     30 pairs · keep (2/25)
- `16:41:30` ✗   data/accumulation-radar.json              0 pairs · DROP — contributes nothing
- `16:41:30` ✅   data/asymmetric-scorer.json              43 pairs · keep (0/25)
## 2. candidate donors — scored on the 8 that actually matter

- `16:41:30` ✅   data/universe.json                     5320 pairs · closes 8/8 ['BE', 'UMC', 'DFTX', 'OVV', 'HCC', 'ALHC', 'IPGP', 'CENX'] · 23h old
- `16:41:31` ✅   data/finviz-universe.json             11519 pairs · closes 8/8 ['BE', 'UMC', 'DFTX', 'OVV', 'HCC', 'ALHC', 'IPGP', 'CENX'] · 3h old
- `16:41:31` ✅   data/finviz-signals.json               3683 pairs · closes 5/8 ['BE', 'DFTX', 'OVV', 'ALHC', 'CENX'] · 3h old
- `16:41:32`   data/stock-xray.json                      0 pairs · closes 0/8 [] · 18h old
- `16:41:32`   data/finviz-groups.json                   0 pairs · closes 0/8 [] · 2h old
- `16:41:32`   data/short-book.json                      7 pairs · closes 0/8 [] · 19h old
- `16:41:32`   data/best-setups.json                    24 pairs · closes 0/8 [] · 1h old
- `16:41:32`   data/fundamental-census-matrix.json       0 pairs · closes 0/8 [] · 131h old
## 3. recommended wiring

- `16:41:32` ✅   data/finviz-universe.json — closes 8/8 ['BE', 'UMC', 'DFTX', 'OVV', 'HCC', 'ALHC', 'IPGP', 'CENX']
- `16:41:32` ✅   data/universe.json — closes 8/8 ['BE', 'UMC', 'DFTX', 'OVV', 'HCC', 'ALHC', 'IPGP', 'CENX']
- `16:41:32` ✅   data/finviz-signals.json — closes 5/8 ['BE', 'DFTX', 'OVV', 'ALHC', 'CENX']
## 4. is risk_regime 0/25 correct, or broken

- `16:41:32` ✅   score 2.5 sits in the engine's neutral band (-12, 12] — 0/25 is CORRECT, not a defect. Do not manufacture a tilt.
## 5. do the resolved sectors even fall in RORO's tilt sets

- `16:41:32`   Energy                      7 tickers · HIGH_BETA
- `16:41:32`   Technology                  4 tickers · HIGH_BETA
- `16:41:32`   Financial Services          3 tickers · HIGH_BETA
- `16:41:32`   Real Estate                 1 tickers · DEFENSIVE
- `16:41:32`   Healthcare                  1 tickers · DEFENSIVE
- `16:41:32` ✗   Industrials                 1 tickers · NOT IN EITHER SET
- `16:41:32`   NOTE: RORO uses FinViz vocabulary (Consumer Cyclical / Financial Services). Any GICS-named sector lands outside both sets and is silently skipped — a vocabulary mismatch, not a data gap.
- `16:41:32` ✅ PROBE COMPLETE
