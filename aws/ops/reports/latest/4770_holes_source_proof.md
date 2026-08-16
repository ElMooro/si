# ops 4770 -- flagged holes vs the live publisher

**Status:** success  
**Duration:** 1.9s  
**Finished:** 2026-08-16T18:55:50+00:00  

## Data

| bank_missing | banked_obs | live_obs | match | mnemonic | window |
|---|---|---|---|---|---|
| 0 | 12 | 12 | True | REPO-DVP_AR_OO-P | 2024-12-18..2025-01-06 |
| 0 | 151 | 151 | True | REPO-GCF_AR_LE30-P | 2020-03-25..2020-10-28 |
| 0 | 95 | 95 | True | REPO-GCF_OV_B27-P | 2025-12-20..2026-05-10 |

## Log
- `18:55:49`   REPO-DVP_AR_OO-P live dates in window: 2024-12-18, 2024-12-19, 2024-12-20, 2024-12-23, 2024-12-24, 2024-12-26, 2024-12-27, 2024-12-30, 2024-12-31, 2025-01-02, 2025-01-03, 2025-01-06
- `18:55:49`   REPO-GCF_AR_LE30-P live dates in window: 2020-03-25, 2020-03-26, 2020-03-27, 2020-03-30, 2020-03-31, 2020-04-01, 2020-04-02, 2020-04-03, 2020-04-06, 2020-04-07, 2020-04-08, 2020-04-09 ...
- `18:55:50`   REPO-GCF_OV_B27-P live dates in window: 2025-12-22, 2025-12-23, 2025-12-24, 2025-12-26, 2025-12-29, 2025-12-30, 2025-12-31, 2026-01-02, 2026-01-05, 2026-01-06, 2026-01-07, 2026-01-08 ...
- `18:55:50` ✅ all flagged windows: bank == publisher exactly -- the holes are the source's own record (closures / zero-trade days), not banking gaps
