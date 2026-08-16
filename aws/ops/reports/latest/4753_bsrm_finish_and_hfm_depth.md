# ops 4753 -- BSRM workbooks banked, duplicate marked, HFM depth proven

**Status:** success  
**Duration:** 0.8s  
**Finished:** 2026-08-16T16:25:36+00:00  

## Data

| banked | bytes | check | earliest | file | kind | latest | mnemonic | n | value |
|---|---|---|---|---|---|---|---|---|---|
| True | 597243 |  |  | ofr_bsrm.xlsx | xlsx (PK magic) |  |  |  |  |
| True | 161171 |  |  | ofr_bsrm_international_scores.xlsx | xlsx (PK magic) |  |  |  |  |
|  |  | hfm_series_objects |  |  |  |  |  |  | 497 |
|  |  |  | 2020-03-23 |  |  | 2026-07-20 | FICC-SPONSORED_REPO_VOL | 1579 |  |
|  |  |  | 2024-03-31 |  |  | 2026-06-04 | FPF-STRATEGY_FUTURES_GNE_CHANGE | 3 |  |

## Log
## A. Bank the two real BSRM workbooks

- `16:25:35` ✅ ofr_bsrm.xlsx: 597243 bytes banked (xlsx (PK magic)) -> data/warm/ofr-bsrm/ofr_bsrm.xlsx
- `16:25:36` ✅ ofr_bsrm_international_scores.xlsx: 161171 bytes banked (xlsx (PK magic)) -> data/warm/ofr-bsrm/ofr_bsrm_international_scores.xlsx
## B. Mark the accidental duplicate sub-prefix

- `16:25:36` ✅ duplicate marker written
## C. HFM depth proof

- `16:25:36` ✅ FICC-SPONSORED_REPO_VOL: 1579 distinct dates, 2020-03-23 -> 2026-07-20
- `16:25:36` ✅ FPF-STRATEGY_FUTURES_GNE_CHANGE: 3 distinct dates, 2024-03-31 -> 2026-06-04
