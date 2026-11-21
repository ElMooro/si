# ops 6898 - the chart's Repo button and the desk it opens, live (read-only), 2026-11-21T21:21+00:00

**Status:** success  
**Duration:** 24.6s  
**Finished:** 2026-11-21T21:21:39+00:00  

## Data

| crisis_cells_equal | crisis_cells_recomputed | failed_checks | marked_late | tiles | tiles_equal_to_board |
|---|---|---|---|---|---|
| 48 | 48 | 0 | 8 | 74 | 18 |

## Log
## 0. The live site serves the new files

- `21:21:15` served after 1 tries: {"engine": true, "rail": true, "desk": true, "page": true}
- `21:21:15` chart.html carries the Repo cache-bust on 2 script tags
- `21:21:16` feeds: board engine 3.0 as of 2026-11-21T20:41:44+00:00, 1800 series in 21 groups | desk as of 2026-11-19, score 17.6 CALM, policy block null
- `21:21:16` installing playwright
## 1. chart.html: the Repo button

- `21:21:31` button: {"btn": true, "prev": "btn-cds", "next": "btn-usd", "label": "RpRepo", "title": "Repo \u00b7 rates, venues, Fed facilities, crisis spikes", "box": [506, -261, 52, 44], "overlay": "", "src": null, "head": null}
- `21:21:33` clicked by mouse at box [506, 52, 52, 44] -> overlay on | Repo · rates, venues, Fed facilities, crisis spikes | /repo.html
## 2. The Desk view, inside the chart

- `21:21:34` as-of line: board as of 2026-11-21T20:41:44+00:00 · 1800 series across 21 groups · 7 skipped (empty at source) · overnight desk as of 2026-11-19
- `21:21:34` hero: 17.6/ 100 CALM Repo stress 17.6/100 -- CALM. Tail 7.0bps, SOFR-IORB +1.0bps, RRP buffer $0B. Hottest: RRP buffer depletion, Room to SRF ceiling, SOFR - IORB. Rates as of 2026-11-19 · NY Fed markets API
- `21:21:34` tabs: Desk | Crisis spikes | Overview1800 | ★ Favorites0 | Ref rates30 | Derived23 | Percentiles12 | SRF2 | Tri-party40 | GCF44 | DVP36 | Sponsored2 | Money funds42 | PD financing88 | PD sec lending85 | PD fails18 | DTCC fails2 | FRED repo/SOFR1028 | Haircuts81 | EU collateral4 | EU yields10 | SFTR12 | TRI legacy40 | Dollar9 | ICE BofA192
- `21:21:34` 74 tiles | without a number: [] | marked late: ['Dealer repo on Treasuries (cash borrowed) (2026-10-28 · 24 days old)', 'Dealer reverse repo on Treasuries (cash lent) (2026-10-28 · 24 days old)', 'FICC daily Treasury fails (2026-09-08 · 74 days old)', 'Treasuries (2026-07-09 · 135 days old)', 'Agency MBS (2026-07-09 · 135 days old)', 'Corporates, investment grade (2026-07-09 · 135 days old)', 'Corporates, high yield (2026-07-09 · 135 days old)', 'Equities (2026-07-09 · 135 days old)']
- `21:21:34`   FNYR-SOFR-A                3.91%     2026-11-19  SOFR
- `21:21:34`   FNYR-TGCR-A                3.88%     2026-11-19  TGCR
- `21:21:34`   REPO-TRIV1_AR_OO-P         3.89%     2026-11-18  Tri-party, overnight
- `21:21:34`   REPO-GCF_AR_TOT-P          3.96%     2026-11-19  GCF repo
- `21:21:34`   REPO-DVP_AR_OO-P           3.92%     2026-11-19  DVP (cleared bilateral), overnight
- `21:21:34`   FNYR-EFFR-A                3.88%     2026-11-19  Fed funds effective
- `21:21:34`   FNYR-SOFR_UV-A             $3.16T    2026-11-19  Volume behind SOFR
- `21:21:34`   REPO-TRIV1_TV_TOT-P        $2.24T    2026-11-18  Tri-party volume
- `21:21:34`   REPO-GCF_TV_TOT-P          $274B     2026-11-19  GCF volume
- `21:21:34`   REPO-DVP_TV_TOT-P          $3.06T    2026-11-19  DVP volume
- `21:21:34`   FICC-SPONSORED_REPO_VOL    $1.05T    2026-10-15  FICC sponsored repo
- `21:21:34`   MMF-MMF_RP_TOT-M           $2.79T    2026-10-31  Money-fund repo, total
- `21:21:34`   MMF-MMF_RP_wFR-M           $6.1B     2026-09-30  … with the Fed (ON RRP)
- `21:21:34`   NYPD-PD_AFtD_T-A           $74B      2026-11-11  Treasury fails to deliver
- `21:21:34`   NYPD-PD_SB_T_TOT-A         $458B     2026-11-11  Securities borrowed, Treasury
- `21:21:34`   RRPONTSYAWARD              3.75%     2026-11-20  ON RRP award rate (the floor)
- `21:21:34`   IORB                       3.90%     2026-11-23  Interest on reserve balances
- `21:21:34`   DFEDTARU                   4.00%     2026-11-21  Top of the target range (the ceiling)
- `21:21:34` tiles equal to the board, value and date: 18 of 18
- `21:21:34` corridor: 3.75% · ON RRP (floor) · 3.90% · IORB · 4.00% · SRF (ceiling) · TGCR · BGCR · EFFR · OBFR 3.88% · SOFR 3.91% | the board's floor / IORB / ceiling: ['3.75%', '3.90%', '4.00%']
- `21:21:34` screenshot of the overlay: 62473 bytes
## 3. Crisis spikes

- `21:21:36`   October 2025 month-end squeeze     +32.0 bp 2025-10-31 | +23.0 bp 2025-10-30 | +36.0 bp 2025-10-31 | +9.0 bp 2025-11-04 | -13.0 bp 2025-10-27 | — | $120B 2025-10-29 | $10.3B 2025-10-29 | $50.3B 2025-10-31 | $50.4B 2025-10-31 | $7.9B 2025-10-29 | $0.1B 2025-11-05
- `21:21:36`   March 2020 dash for cash           +44.0 bp 2020-03-17 | +174 bp 2020-03-16 | +29.0 bp 2020-03-17 | +46.0 bp 2020-03-16 | 0.0 bp 2020-03-16 | +142 bp 2020-03-26 | $269B 2020-03-18 | $132B 2020-03-18 | $153B 2020-03-17 | — | $50.8B 2020-03-25 | $358B 2020-04-08
- `21:21:36`   September 2019 repo squeeze        +315 bp 2019-09-17 | +375 bp 2019-09-17 | +295 bp 2019-09-17 | +116 bp 2019-09-18 | +5.0 bp 2019-09-17 | +37.0 bp 2019-10-03 | $125B 2019-09-18 | $65.7B 2019-09-18 | $75.0B 2019-09-18 | — | $0.0B 2019-10-02 | $1.0B 2019-10-02
- `21:21:36`   Lehman / global financial crisis   — | — | — | — | +97.0 bp 2008-10-07 | +458 bp 2008-10-10 | $2,684B 2008-10-22 | $178B 2008-11-19 | $100B 2008-09-18 | — | $111B 2008-10-29 | $583B 2008-12-17
- `21:21:36` cells of four episodes recomputed from the history files: 48 of 48 equal
- `21:21:36`   SOFR − IOER/IORB 2018-04-03 → 2026-11-19                   2019-09-17 +315 bp +309 bp over September 2019 repo squeeze ; 2019-01-02 +75.0 bp +75.0 bp over Year-end 2018 ; 2020-03-17 +44.0 bp +45.0 bp over March 2020 dash for cash
- `21:21:36`   SOFR tail 2018-04-03 → 2026-11-19                          2019-09-17 +375 bp +361 bp over September 2019 repo squeeze ; 2018-12-31 +325 bp +313 bp over Year-end 2018 ; 2020-03-16 +174 bp +163 bp over March 2020 dash for cash
- `21:21:36`   SOFR − fed funds 2018-04-03 → 2026-11-19                   2019-09-17 +295 bp +293 bp over September 2019 repo squeeze ; 2019-01-02 +75.0 bp +74.0 bp over Year-end 2018 ; 2019-04-30 +31.0 bp +30.5 bp over month-end
- `21:21:36`   GCF − tri-party 2018-05-07 → 2026-11-18                    2018-12-31 +120 bp +114 bp over Year-end 2018 ; 2019-09-18 +116 bp +109 bp over September 2019 repo squeeze ; 2019-12-31 +85.0 bp +80.5 bp over year-end
- `21:21:36`   Fed funds − target 1982-09-27 → 2026-11-19                 1986-12-30 +1,030 bp +1,018 bp over year-end ; 1985-12-31 +571 bp +569 bp over year-end ; 1991-01-23 +364 bp +359 bp over
- `21:21:36`   TED spread 1986-01-02 → 2022-01-21                         2008-10-10 +458 bp +340 bp over Lehman / global financial crisis ; 2008-09-17 +303 bp +190 bp over Lehman / global financial crisis ; 1987-10-20 +301 bp +180 bp over October 1987 crash
- `21:21:36`   Treasury fails 1998-01-28 → 2026-11-11                     2008-10-22 $2,684B +$2,627B over Lehman / global financial crisis ; 2003-08-20 $1,622B +$1,518B over 2003 Treasury fails episode ; 2001-09-19 $1,330B +$1,302B over September 11, 2001
- `21:21:36`   Agency MBS fails 1998-01-28 → 2026-11-11                   2010-11-17 $1,225B +$662B over Late 2010 agency MBS fails ; 2010-05-19 $1,058B +$632B over ; 2003-06-18 $886B +$619B over
- `21:21:36`   Fed repo operations 2000-01-03 → 2026-11-20                2020-03-17 $153B +$105B over March 2020 dash for cash ; 2008-09-18 $100B +$92.5B over Lehman / global financial crisis ; 2020-06-05 $106B +$85.2B over
- `21:21:36`   Standing Repo Facility 2021-01-04 → 2026-11-20             2025-12-31 $74.6B +$74.5B over Year-end 2025 ; 2025-10-31 $50.4B +$50.3B over October 2025 month-end squeeze ; 2026-02-17 $30.5B +$30.5B over
- `21:21:36`   Discount window 2002-12-18 → 2026-11-18                    2023-03-15 $153B +$148B over March 2023 bank failures ; 2008-10-29 $111B +$93.3B over Lehman / global financial crisis ; 2020-03-25 $50.8B +$50.8B over March 2020 dash for cash
- `21:21:36`   Swap lines 2002-12-18 → 2026-11-18                         2008-12-10 $583B +$518B over Lehman / global financial crisis ; 2020-05-27 $449B +$449B over ; 2008-10-08 $335B +$273B over Lehman / global financial crisis
## 4. A number opens its history; the board is still there

- `21:21:36` chart: D_SOFR_ADMIN — SOFR − interest on reserves (bps; IOER until July 2021, IORB sinc | e banks earn at the Fed · 2156 obs · 2018-04-03 → 2026-11-19 | Largest spikes over its own prior median: 2019-09-17 315 (6 before) September 2019 repo squeeze 2019-01-02 75 (0 before) Year-end 2018 2020-03-17 44 (-1 before) March 2020 dash for cash 2019-04-30 36 (1.5 before) month-end 2025-10-31 32 (-1 before) October 202
- `21:21:37` overview: 39 key dials, 21 group cards, board barometer block | Money market funds tab: 42 rows in ['Holdings', 'Median yield on repo', 'Repo by collateral and term', 'Repo by counterparty'] | search 'sofr': 1045 rows
## 5. A phone, 390 px

- `21:21:39` desk: 74 tiles, sideways overflow 0 px | crisis: 14 rows, overflow 0 px | reference rates tab: 30 rows, overflow 0 px | page errors []
- `21:21:39` ✅ the Repo button opens the desk; every tile carries a number equal to the board's; 48 of 48 crisis cells equal the history files; no page errors; no sideways scroll at 390 px
