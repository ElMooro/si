# ops 4670 — OFR depth census + catalog diff (#1-#8)

**Status:** success  
**Duration:** 31.6s  
**Finished:** 2026-08-14T21:46:19+00:00  

## Log
## A. Depth census across every banked mnemonic

- `21:45:47`   catalog=442 banked=442
- `21:46:13`   parsed 442/442 in 26s (unparsed 0)
- `21:46:13`   NYPD   series=194  obs=96038     2015-01-07 -> 2026-07-22
- `21:46:13`   REPO   series=164  obs=311695    2014-08-22 -> 2026-08-04
- `21:46:13`   MMF    series=42   obs=6683      2010-11-30 -> 2026-06-30
- `21:46:13`   FNYR   series=30   obs=68934     2016-03-01 -> 2026-08-04
- `21:46:13`   TYLD   series=12   obs=98816     1990-01-02 -> 2026-08-04
- `21:46:13` ✅   [census] 442/442 series measured; earliest datum 1990-01-02
## B. Live catalog diff — anything we never saw

- `21:46:18`   live catalog: OK 10045 bytes
- `21:46:19`   live=442 ours=442 · NEW upstream=0 · not-in-live=0
- `21:46:19` ✅   [diff] our catalog is complete vs live
## C. #4 tri-party / haircut coverage, explicitly

- `21:46:19`   REPO-DVP_AR_B27-F            n=156    2025-08-14 -> 2026-03-31
- `21:46:19`   REPO-DVP_AR_B27-P            n=242    2025-08-14 -> 2026-08-04
- `21:46:19`   REPO-DVP_AR_B830-F           n=156    2025-08-14 -> 2026-03-31
- `21:46:19`   REPO-DVP_AR_B830-P           n=242    2025-08-14 -> 2026-08-04
- `21:46:19`   REPO-DVP_AR_G30-F            n=1972   2018-05-07 -> 2026-03-31
- `21:46:19`   REPO-DVP_AR_G30-P            n=2058   2018-05-07 -> 2026-08-04
- `21:46:19`   REPO-DVP_AR_LE30-F           n=1816   2018-05-07 -> 2025-08-13
- `21:46:19`   REPO-DVP_AR_LE30-P           n=1816   2018-05-07 -> 2025-08-13
- `21:46:19`   REPO-DVP_AR_OO-F             n=1972   2018-05-07 -> 2026-03-31
- `21:46:19`   REPO-DVP_AR_OO-P             n=2058   2018-05-07 -> 2026-08-04
- `21:46:19`   REPO-DVP_AR_TOT-F            n=1972   2018-05-07 -> 2026-03-31
- `21:46:19`   REPO-DVP_AR_TOT-P            n=2058   2018-05-07 -> 2026-08-04
- `21:46:19`   REPO-DVP_OV_B27-F            n=156    2025-08-14 -> 2026-03-31
- `21:46:19`   REPO-DVP_OV_B27-P            n=242    2025-08-14 -> 2026-08-04
- `21:46:19`   REPO-DVP_OV_B830-F           n=156    2025-08-14 -> 2026-03-31
- `21:46:19`   REPO-DVP_OV_B830-P           n=242    2025-08-14 -> 2026-08-04
- `21:46:19`   REPO-DVP_OV_G30-F            n=1972   2018-05-07 -> 2026-03-31
- `21:46:19`   REPO-DVP_OV_G30-P            n=2058   2018-05-07 -> 2026-08-04
- `21:46:19`   REPO-DVP_OV_LE30-F           n=1816   2018-05-07 -> 2025-08-13
- `21:46:19`   REPO-DVP_OV_LE30-P           n=1816   2018-05-07 -> 2025-08-13
- `21:46:19`   REPO-DVP_OV_OO-F             n=1972   2018-05-07 -> 2026-03-31
- `21:46:19`   REPO-DVP_OV_OO-P             n=2058   2018-05-07 -> 2026-08-04
- `21:46:19`   REPO-DVP_OV_TOT-F            n=1972   2018-05-07 -> 2026-03-31
- `21:46:19`   REPO-DVP_OV_TOT-P            n=2058   2018-05-07 -> 2026-08-04
- `21:46:19`   REPO-DVP_TV_B27-F            n=156    2025-08-14 -> 2026-03-31
- `21:46:19`   REPO-DVP_TV_B27-P            n=242    2025-08-14 -> 2026-08-04
- `21:46:19`   REPO-DVP_TV_B830-F           n=156    2025-08-14 -> 2026-03-31
- `21:46:19`   REPO-DVP_TV_B830-P           n=242    2025-08-14 -> 2026-08-04
- `21:46:19`   REPO-DVP_TV_G30-F            n=1972   2018-05-07 -> 2026-03-31
- `21:46:19`   REPO-DVP_TV_G30-P            n=2058   2018-05-07 -> 2026-08-04
- `21:46:19`   tri/haircut-family series held: 164 (earliest 2014-08-22)
- `21:46:19` ✅   [#4] tri-party lane is BANKED, not missing — 164 series
## D. Publish coverage doc

- `21:46:19` ✅   published data/repo-coverage.json
## verdict

- `21:46:19` ✅ repo lane measured end-to-end: 442 series, earliest 1990-01-02; #1/#2/#4/#7 confirmed banked with real depth
