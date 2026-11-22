# ops 6906 - the Repo desk after its four open items, real browser on the live site (read-only), 2026-11-22T15:54+00:00

**Status:** success  
**Duration:** 27.5s  
**Finished:** 2026-11-22T15:54:58+00:00  

## Data

| crisis_cells_equal | crisis_cells_recomputed | failed_checks | marked_late | tiles | tiles_equal_to_board |
|---|---|---|---|---|---|
| 98 | 98 | 0 | 1 | 74 | 18 |

## Log
## 0. The live site serves the new files

- `15:54:31` served after 1 tries: {"engine": true, "rail": true, "desk": true, "page": true, "dealer_page": true}
- `15:54:31` chart.html carries the Repo cache-bust on 2 script tags
- `15:54:33` feeds: board engine 3.1 as of 2026-11-22T06:37:16+00:00, 1803 series in 21 groups | desk as of 2026-11-19, score 17.6 CALM, policy block {"admin_rate": 3.9, "admin_rate_name": "IORB", "on_rrp_award": 3.75, "on_rrp_award_as_of": "2026-11-20", "range_top": 4.0, "range_top_as_of": "2026-11-21", "not
- `15:54:33` installing playwright
## 1. chart.html: the Repo button

- `15:54:41` button: {"btn": true, "prev": "btn-cds", "next": "btn-sov", "label": "RpRepo", "title": "Repo \u00b7 rates, venues, Fed facilities, crisis spikes", "box": [506, -193, 52, 44], "overlay": "", "src": null, "head": null}
- `15:54:46` clicked by mouse at box [506, 52, 52, 44] -> overlay on | Repo · rates, venues, Fed facilities, crisis spikes | /repo.html
## 2. The Desk view, inside the chart

- `15:54:48` as-of line: board as of 2026-11-22T06:37:16+00:00 · 1803 series across 21 groups · 7 skipped (empty at source) · overnight desk as of 2026-11-19
- `15:54:48` hero: 17.6/ 100 CALM Repo stress 17.6/100 -- CALM. Tail 7.0bps, SOFR-IORB +1.0bps, RRP buffer $0B. Hottest: RRP buffer depletion, Room to SRF ceiling, SOFR - IORB. Rates as of 2026-11-19 · NY Fed markets API
- `15:54:48` tabs: Desk | Crisis spikes | Overview1803 | ★ Favorites0 | Ref rates31 | Derived25 | Percentiles12 | SRF2 | Tri-party40 | GCF44 | DVP36 | Sponsored2 | Money funds42 | PD financing88 | PD sec lending85 | PD fails18 | DTCC fails2 | FRED repo/SOFR1028 | Haircuts81 | EU collateral4 | EU yields10 | SFTR12 | TRI legacy40 | Dollar9 | ICE BofA192
- `15:54:48` 74 tiles | without a number: [] | marked late: ['Dealer reverse repo on Treasuries (cash lent) (2026-10-28 · 25 days old)']
- `15:54:48`   Dealer repo on Treasuries (cash borrowed)      $3.40T | 2026-11-11 | engine document: 3398.38 as of 2026-11-11, published this week: True
- `15:54:48`   Dealer reverse repo on Treasuries (cash lent)  $3.00T | 2026-10-28 · 25 days old | engine document: 3002.182 as of 2026-10-28, published this week: False
- `15:54:48`   HAIRCUT-us-treasuries-excluding-strips--median_haircut     2.0% | 2026-10-09 | board 2026-10-09
- `15:54:48`   HAIRCUT-equities--median_haircut                           8.0% | 2026-10-09 | board 2026-10-09
- `15:54:48`   HAIRCUT-corporates-non-investment-grade--median_haircut    8.0% | 2026-10-09 | board 2026-10-09
- `15:54:48`   DTCC-TREASURY-FAILS                                        $16B | 2026-11-20 | board 2026-11-20
- `15:54:48` board engine 3.1, its own sources this run: {"survey_rate": {"status": "banked", "n": 4998}, "haircuts": {"status": "level", "asked": true, "source_newest": "2026-10-09", "bank_newest_before": "2026-10-09", "written": 0, "unchanged": 73, "new_series": [], "observations_added": 0, "observations_revised": 0, "revised_sample": [], "dry_run": false, "old_workbook_read": false, "series": 81, "bank_newest_after": "2026-10-09"}} | dtcc {"url": "https://cms-prod.dtcc.com/data/failsdata.csv", "live_rows": 250, "treasury": 317, "agency": 317}
- `15:54:48`   FNYR-SOFR-A                3.91%     2026-11-19  SOFR
- `15:54:48`   FNYR-TGCR-A                3.88%     2026-11-19  TGCR
- `15:54:48`   REPO-TRIV1_AR_OO-P         3.89%     2026-11-18  Tri-party, overnight
- `15:54:48`   REPO-GCF_AR_TOT-P          3.96%     2026-11-19  GCF repo
- `15:54:48`   REPO-DVP_AR_OO-P           3.92%     2026-11-19  DVP (cleared bilateral), overnight
- `15:54:48`   FNYR-EFFR-A                3.88%     2026-11-19  Fed funds effective
- `15:54:48`   FNYR-SOFR_UV-A             $3.16T    2026-11-19  Volume behind SOFR
- `15:54:48`   REPO-TRIV1_TV_TOT-P        $2.24T    2026-11-18  Tri-party volume
- `15:54:48`   REPO-GCF_TV_TOT-P          $274B     2026-11-19  GCF volume
- `15:54:48`   REPO-DVP_TV_TOT-P          $3.06T    2026-11-19  DVP volume
- `15:54:48`   FICC-SPONSORED_REPO_VOL    $1.05T    2026-10-15  FICC sponsored repo
- `15:54:48`   MMF-MMF_RP_TOT-M           $2.79T    2026-10-31  Money-fund repo, total
- `15:54:48`   MMF-MMF_RP_wFR-M           $6.1B     2026-09-30  … with the Fed (ON RRP)
- `15:54:48`   NYPD-PD_AFtD_T-A           $74B      2026-11-11  Treasury fails to deliver
- `15:54:48`   NYPD-PD_SB_T_TOT-A         $458B     2026-11-11  Securities borrowed, Treasury
- `15:54:48`   RRPONTSYAWARD              3.75%     2026-11-20  ON RRP award rate (the floor)
- `15:54:48`   IORB                       3.90%     2026-11-23  Interest on reserve balances
- `15:54:48`   DFEDTARU                   4.00%     2026-11-21  Top of the target range (the ceiling)
- `15:54:48` tiles equal to the board, value and date: 18 of 18
- `15:54:48` corridor: 3.75% · ON RRP (floor) · 3.90% · IORB · 4.00% · SRF (ceiling) · TGCR · BGCR · EFFR · OBFR 3.88% · SOFR 3.91% | the board's floor / IORB / ceiling: ['3.75%', '3.90%', '4.00%']
- `15:54:49` screenshot of the overlay: 61471 bytes
## 3. Crisis spikes

- `15:54:51`   October 2025 month-end squeeze     +34.5 bp 2025-10-31 | -3.5 bp 2025-11-05 | +32.0 bp 2025-10-31 | +23.0 bp 2025-10-30 | +36.0 bp 2025-10-31 | +9.0 bp 2025-11-04 | -13.0 bp 2025-10-27 | — | $120B 2025-10-29 | $10.3B 2025-10-29 | $50.3B 2025-10-31 | $50.4B 2025-10-31 | $7.9B 2025-10-29 | $0.1B 2025-11-05
- `15:54:51`   March 2020 dash for cash           +41.5 bp 2020-03-17 | +11.5 bp 2020-03-24 | +44.0 bp 2020-03-17 | +174 bp 2020-03-16 | +29.0 bp 2020-03-17 | +46.0 bp 2020-03-16 | 0.0 bp 2020-03-16 | +142 bp 2020-03-26 | $269B 2020-03-18 | $132B 2020-03-18 | $153B 2020-03-17 | — | $50.8B 2020-03-25 | $358B 2020-04-08
- `15:54:51`   September 2019 repo squeeze        +313 bp 2019-09-17 | +5.5 bp 2019-09-27 | +315 bp 2019-09-17 | +375 bp 2019-09-17 | +295 bp 2019-09-17 | +116 bp 2019-09-18 | +5.0 bp 2019-09-17 | +37.0 bp 2019-10-03 | $125B 2019-09-18 | $65.7B 2019-09-18 | $75.0B 2019-09-18 | — | $0.0B 2019-10-02 | $1.0B 2019-10-02
- `15:54:51`   Lehman / global financial crisis   +20.0 bp 2008-09-12 | +189 bp 2008-10-03 | — | — | — | — | +97.0 bp 2008-10-07 | +458 bp 2008-10-10 | $2,684B 2008-10-22 | $178B 2008-11-19 | $100B 2008-09-18 | — | $111B 2008-10-29 | $583B 2008-12-17
- `15:54:51`   Bear Stearns                       -10.0 bp 2008-03-28 | +191 bp 2008-03-19 | — | — | — | — | +26.0 bp 2008-03-31 | +200 bp 2008-03-19 | $1,042B 2008-03-26 | $256B 2008-03-19 | $12.0B 2008-03-17 | — | $0.6B 2008-03-26 | $0.0B 2008-03-12
- `15:54:51`   August 2007 money-market freeze    +32.0 bp 2007-09-18 | +309 bp 2007-08-21 | — | — | — | — | +17.0 bp 2007-09-18 | +242 bp 2007-08-20 | $76.3B 2007-08-22 | $79.9B 2007-08-15 | $38.0B 2007-08-10 | — | $7.2B 2007-09-12 | $0.0B 2007-08-15
- `15:54:51`   Year-end 1999 (Y2K)                -4.0 bp 2000-01-03 | +284 bp 1999-12-30 | — | — | — | — | -7.0 bp 2000-01-03 | +93.0 bp 1999-12-30 | $14.7B 1999-12-29 | $35.6B 1999-12-29 | $9.0B 2000-01-03 | — | — | —
- `15:54:51` cells of seven episodes recomputed from the history files: 98 of 98 equal
- `15:54:51`   policy rate - repo: 2008 +189 bp 2008-10-03 | August 2007 +309 bp 2007-08-21 | year-end 1999 +284 bp 1999-12-30
- `15:54:51`   Repo − policy rate 1998-02-20 → 2026-11-19                 2019-09-17 +313 bp +309 bp over September 2019 repo squeeze ; 2019-01-02 +77.5 bp +70.0 bp over Year-end 2018 ; 1998-12-15 +53.0 bp +61.5 bp over
- `15:54:51`   Policy rate − repo 1998-02-20 → 2026-11-19                 2007-08-21 +309 bp +296 bp over August 2007 money-market freeze ; 1999-12-30 +284 bp +275 bp over Year-end 1999 (Y2K) ; 2007-12-31 +274 bp +258 bp over year-end
- `15:54:51`   SOFR − IOER/IORB 2018-04-03 → 2026-11-19                   2019-09-17 +315 bp +309 bp over September 2019 repo squeeze ; 2019-01-02 +75.0 bp +75.0 bp over Year-end 2018 ; 2020-03-17 +44.0 bp +45.0 bp over March 2020 dash for cash
- `15:54:51`   SOFR tail 2018-04-03 → 2026-11-19                          2019-09-17 +375 bp +361 bp over September 2019 repo squeeze ; 2018-12-31 +325 bp +313 bp over Year-end 2018 ; 2020-03-16 +174 bp +163 bp over March 2020 dash for cash
- `15:54:51`   SOFR − fed funds 2018-04-03 → 2026-11-19                   2019-09-17 +295 bp +293 bp over September 2019 repo squeeze ; 2019-01-02 +75.0 bp +74.0 bp over Year-end 2018 ; 2019-04-30 +31.0 bp +30.5 bp over month-end
- `15:54:51`   GCF − tri-party 2018-05-07 → 2026-11-18                    2018-12-31 +120 bp +114 bp over Year-end 2018 ; 2019-09-18 +116 bp +109 bp over September 2019 repo squeeze ; 2019-12-31 +85.0 bp +80.5 bp over year-end
- `15:54:51`   Fed funds − target 1982-09-27 → 2026-11-19                 1986-12-30 +1,030 bp +1,018 bp over year-end ; 1985-12-31 +571 bp +569 bp over year-end ; 1991-01-23 +364 bp +359 bp over
- `15:54:51`   TED spread 1986-01-02 → 2022-01-21                         2008-10-10 +458 bp +340 bp over Lehman / global financial crisis ; 2008-09-17 +303 bp +190 bp over Lehman / global financial crisis ; 1987-10-20 +301 bp +180 bp over October 1987 crash
- `15:54:51`   Treasury fails 1998-01-28 → 2026-11-11                     2008-10-22 $2,684B +$2,627B over Lehman / global financial crisis ; 2003-08-20 $1,622B +$1,518B over 2003 Treasury fails episode ; 2001-09-19 $1,330B +$1,302B over September 11, 2001
- `15:54:51`   Agency MBS fails 1998-01-28 → 2026-11-11                   2010-11-17 $1,225B +$662B over Late 2010 agency MBS fails ; 2010-05-19 $1,058B +$632B over ; 2003-06-18 $886B +$619B over
- `15:54:51`   Fed repo operations 2000-01-03 → 2026-11-20                2020-03-17 $153B +$105B over March 2020 dash for cash ; 2008-09-18 $100B +$92.5B over Lehman / global financial crisis ; 2020-06-05 $106B +$85.2B over
- `15:54:51`   Standing Repo Facility 2021-01-04 → 2026-11-20             2025-12-31 $74.6B +$74.5B over Year-end 2025 ; 2025-10-31 $50.4B +$50.3B over October 2025 month-end squeeze ; 2026-02-17 $30.5B +$30.5B over
- `15:54:51`   Discount window 2002-12-18 → 2026-11-18                    2023-03-15 $153B +$148B over March 2023 bank failures ; 2008-10-29 $111B +$93.3B over Lehman / global financial crisis ; 2020-03-25 $50.8B +$50.8B over March 2020 dash for cash
- `15:54:51`   Swap lines 2002-12-18 → 2026-11-18                         2008-12-10 $583B +$518B over Lehman / global financial crisis ; 2020-05-27 $449B +$449B over ; 2008-10-08 $335B +$273B over Lehman / global financial crisis
## 4. A number opens its history; the board is still there

- `15:54:51` chart: D_REPO_TARGET — Overnight Treasury repo − fed funds target (bps): dealer-survey  | sitive = cash is scarce · 7154 obs · 1998-02-20 → 2026-11-19 | Largest spikes over its own prior median: 2019-09-17 312.5 (3.5 before) September 2019 repo squeeze 2019-01-02 77.5 (7.5 before) Year-end 2018 1998-12-15 53 (-8.5 before) 1998-11-16 52 (-4 before) 2001-04-18 44 (-8.5 before) 2001-01-03 42 (-5 before)
- `15:54:52` overview: 42 key dials, 21 group cards, board barometer block | Money market funds tab: 42 rows in ['Holdings', 'Median yield on repo', 'Repo by collateral and term', 'Repo by counterparty'] | search 'sofr': 1047 rows
## 5. A phone, 390 px

- `15:54:55` desk: 74 tiles, sideways overflow 0 px | crisis: 15 rows, overflow 0 px | reference rates tab: 31 rows, overflow 0 px | page errors []
## 6. The dealer engine's own page

- `15:54:57` dealer page: Generated 2026-11-22T04:27:27.378012+00:00 · source acquired 2026-11-22T04:27:21.984820+00:00 | links [["Original observations 1 of 5", "/data/evidence/fr2004/4eab7e009eb213f1648a40b50e71ad7f5f2f0d8e7c481b3a4e49c2b02f370e60/3a261ef210b3c8471343996c47814361550741b9b3de04794daa8888b24ca15a.bin.gz"], ["Original observations 2 of 5", "/data/evidence/fr2004/66cb9e11ad13b410e28fde5d39595dc28ae411e2399982c2148431f1fae32911/9cf0f9a99b6a042a1ecd441c510e1258144c6d49421259ef73899168cbd1f49d.bin.gz"], ["Original observations 3 o | page errors []
- `15:54:58`   first original response: 57240 bytes stored, gzip
- `15:54:58` ✅ the desk carries the four items: repo against policy from 1998, haircuts and DTCC fails current, dealers' repo at the newest week; 98 of 98 crisis cells equal the history files; no page errors
