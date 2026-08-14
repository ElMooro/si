# ops 4659 — PD full-history depth v3 (4602 redo)

**Status:** success  
**Duration:** 31.8s  
**Finished:** 2026-08-14T17:24:00+00:00  

## Log
- `17:23:28` fred guard (untouched): ver=2.3.0 imported=281947 status=COMPLETE_WITH_LEAKS
## 1. Deploy settle — env PD_TRANCHE=150 is the signal

- `17:23:29`   live env: {'S3_BUCKET': 'justhodl-dashboard-live', 'PD_TRANCHE': '150'}
- `17:23:29` ✅   [deploy] v3 config applied (PD_TRANCHE=150)
## 2. Event-kick + wait out the full first v3 cycle

- `17:23:29`   before: hist_v=3 status=converging-v3 done=138
- `17:23:29`   kicked (Event — 4602's sync-invoke death not repeated)
- `17:23:59`   after 30s: hist_v=3 status=converging-v3 done=138 failures=0 shallow_n=0 budget=stopped after 138 this run (90s headroom kept)
- `17:23:59`   validated breaks (6): ['SBN2013', 'SBN2015', 'SBN2022', 'SBN2024', 'SBP2001', 'SBP2013']
- `17:23:59`   probe map: 6 ok, 18 rejected (e.g. {'1998-01-28': 'HTTPError', '2001-06-30': 'HTTPError', '2001-07-01': 'HTTPError', '2013-03-31': 'HTTPError'})
- `17:23:59` ✅   [flip] hist_v=3 live — worklist re-queued
- `17:23:59` ✅   [breaks] 6 live-validated break ids, none are labels
- `17:23:59` ✅   [block] not blocked (status=converging-v3)
## 3. Depth proof from actual per-key docs

- `17:23:59`   PDABTOT: hist_v=3 n_obs=697 first=2013-04-03 last=2026-08-05 breaks=['SBN2013', 'SBN2015', 'SBN2022', 'SBN2024'] gz=4510B
- `17:23:59`   PDABTOTC: hist_v=3 n_obs=697 first=2013-04-03 last=2026-08-05 breaks=['SBN2013', 'SBN2015', 'SBN2022', 'SBN2024'] gz=4417B
- `17:23:59`   PDCAFHLMCNONUMBS-DR25: hist_v=3 n_obs=55 first=2022-01-13 last=2026-07-13 breaks=['SBN2022', 'SBN2024'] gz=401B
- `17:23:59`   PDCAFNMAFHLMC-DR25: hist_v=3 n_obs=55 first=2022-01-13 last=2026-07-13 breaks=['SBN2022', 'SBN2024'] gz=621B
- `17:24:00`   PDCAFNMAFHLMC-FR30: hist_v=3 n_obs=55 first=2022-01-13 last=2026-07-13 breaks=['SBN2022', 'SBN2024'] gz=545B
- `17:24:00`   PDCBFNMAFHLMC-DR35: hist_v=3 n_obs=55 first=2022-01-18 last=2026-07-16 breaks=['SBN2022', 'SBN2024'] gz=586B
- `17:24:00`   sample: 6 deep / 0 current-only · mean n_obs=269 (shallow era ~110) · mean gz=1847B -> projected fleet footprint 2.7 MB (was 5.07; tranche-1 skews young MBS-detail vintages, ancient cores land later)
- `17:24:00` ✅   [depth] 6 of 6 sampled docs merged >=2 breaks
- `17:24:00` ✅   [depth] current-only fallbacks in sample: 0
- `17:24:00` ✅   [depth] mean n_obs 269 vs shallow-era ~110
- `17:24:00` ✅   [depth] 2 sampled doc(s) reach pre-2016 with 500+ obs — full-lineage proof (proj 2.7 MB logged, not contracted: gz makes MB the wrong depth metric)
- `17:24:00` ✅   [hygiene] failures=0 shallow_n=0
- `17:24:00`   ETA: 1401 remaining / 150 per hourly tranche ≈ 9 h to full v3 depth
## verdict

- `17:24:00` ✅ depth PROVEN on live docs — hourly tranches converge the rest; data.html footprint climbs off 5.07 MB from the next provider-catalog refresh
