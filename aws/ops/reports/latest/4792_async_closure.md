# ops 4792 -- async engine run + 14-block closure

**Status:** success  
**Duration:** 25.6s  
**Finished:** 2026-08-16T22:56:17+00:00  

## Data

| blk01_venue_rates_vols | blk02_tenor_rollover | blk03_pd_financing | blk04_dtcc_daily_fails | blk05_ficc_sponsored | blk06_haircuts | blk07_specialness_proxy | blk08_sofr_distribution | blk09_srf | blk10_mmf_counterparties | blk11_collateral_splits | blk12_fima_foreign_pool | blk13_europe_mmsr | blk14_derived_barometer | check | value | waited_s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | as_of_before | 2026-08-16T22:42:22+00:00 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | event_status | 202 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | engine_completed | True | 25.2 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | engine_v_note | 2026-08-16T22:48:21+00:00 |  |
| PRESENT |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | PRESENT |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  | PRESENT |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | PENDING(source recon) |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | PRESENT |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | PENDING(parser v2) |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | PRESENT(proxy) |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | PRESENT |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | PRESENT |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | PRESENT |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | PRESENT |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | PARTIAL |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | PENDING(next arc) |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | PRESENT |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | board_total | 1484 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | groups_total | 15 |  |

## Log
- `22:56:17`   #1 venue_rates_vols: TRIV1=40 DVP=36 GCF=44
- `22:56:17`   #2 tenor_rollover: OV legs=28
- `22:56:17`   #3 pd_financing: NYPD rows=106; deepest 1998-01-28
- `22:56:17`   #4 dtcc_daily_fails: no public API confirmed yet
- `22:56:17`   #5 ficc_sponsored: FICC-SPONSORED_REPO_VOL 2020-03-23->2026-07-17 n=1578; FICC-SPONSORED_REVREPO_VOL 2020-03-23->2026-07-17 n=1578
- `22:56:17`   #6 haircuts: ops 4793 parser v2 in this push | NCCBR publisher-blocked, tripwire armed
- `22:56:17`   #7 specialness_proxy: D_DVP_SOFR 2018-05-07->2026-08-04 n=2058; per-CUSIP specials = commercial only
- `22:56:17`   #8 sofr_distribution: percentile rows=12; SOFR 2018-04-03->2026-08-13 n=2089; UV 2018-04-02->2026-08-04 n=2083
- `22:56:17`   #9 srf: D_SOFR_SRF 2021-09-20->2026-02-18 n=1055; RPONTSYD 2000-01-03->2026-08-14 n=3276; SRF_TAKEUP 2021-01-04->2026-08-14 n=1402
- `22:56:17`   #10 mmf_counterparties: rows=5; wFICC 2014-03-31->2026-06-30 n=139
- `22:56:17`   #11 collateral_splits: all legs on board
- `22:56:17`   #12 fima_foreign_pool: []
- `22:56:17`   #13 europe_mmsr: ECB MMSR queued -- international arc
- `22:56:17`   #14 derived_barometer: derived=11 first-class series + red-tag user barometer
