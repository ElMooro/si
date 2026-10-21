# ops 6596 - chart.html feed census (read-only): which engines behind the chart are fresh, stale, dead

**Status:** success  
**Duration:** 24.4s  
**Finished:** 2026-10-21T14:15:15+00:00  

## Data

| age_h | bound_h | engine | key | size | stamp | verdict |
|---|---|---|---|---|---|---|
| 0.6 | 30 | justhodl-13f-positions | data/13f-by-ticker.json | 57302 | - | FRESH |
| 759.1 | 30 | justhodl-13f-positions | data/13f-cusip-map.json | 1667330 | - | STALE |
| 0.6 | 30 | justhodl-13f-positions | data/13f-desk.json | 57356 | - | FRESH |
| 0.6 | 30 | justhodl-13f-positions | data/13f-flows-by-ticker.json | 57319 | - | FRESH |
| 0.6 | 30 | justhodl-13f-positions | data/13f-positions.json | 57370 | - | FRESH |
| 759.1 | 30 | justhodl-13f-positions | data/13f-price-anchors.json | 663539 | - | STALE |
| 759.1 | 30 | justhodl-13f-positions | data/13f-state/first-seen.json | 432683 | - | STALE |
| 0.7 | 30 | justhodl-13f-positions | data/holdings-research.json | 20982 | - | FRESH |
| 7.1 | 30 | justhodl-ai | data/ai.json | 12764 | 2026-10-21T07:07:38.05415 | FRESH |
| 7.1 | 30 | justhodl-ai | data/ai/control.json | 159 | 2026-10-21T07:07:38.05415 | FRESH |
| 0.7 | 30 | justhodl-beneish | data/beneish.json | 11604 | 2026-10-21T13:30:53.90749 | FRESH |
| 0.7 | 30 | justhodl-boom-radar | data/boom-radar.json | 4500 | 2026-10-21T13:31:06.30026 | FRESH |
| 10.5 | 80 | justhodl-bottom | data/bottom.json | 2041484 | 2026-10-21T03:47:49Z | FRESH |
| 111.9 | 30 | justhodl-brief-compiler | data/jh-internals.json | 5101 | 2026-10-16T22:20:55.05675 | STALE |
| 0.5 | 30 | justhodl-buyback-engine | data/buyback-engine.json | 2867406 | - | FRESH |
| 2.2 | 30 | justhodl-catalyst-calendar | data/catalyst-calendar.json | 38707 | 2026-10-21T12:00:25+00:00 | FRESH |
| 112.2 | 198 | justhodl-cftc-deep-view | data/cftc-deep-view.json | 12093 | 2026-10-16T22:00:41.28951 | FRESH |
| 112.2 | 198 | justhodl-cftc-deep-view | data/cot-extremes.json | 201 | 2026-10-16T22:00:41.28951 | FRESH |
| 7.0 | 30 | justhodl-ciss-stress | data/ciss-stress.json | 4615783 | - | FRESH |
| 3.4 | 30 | justhodl-cq-feed | data/cq-catalog.json | 40716 | 2026-10-21 | FRESH |
| 3.4 | 30 | justhodl-cq-feed | data/cq-feed.json | 53763 | 2026-10-21T10:53:05.77553 | FRESH |
| 0.7 | 30 | justhodl-crypto-etf-flows | data/crypto-etf-flows-history.json | 12881 | 2026-06-28 | FRESH |
| 0.7 | 30 | justhodl-crypto-etf-flows | data/crypto-etf-flows.json | 1874 | 2026-10-21T13:31:10Z | FRESH |
| 113.1 | 30 | justhodl-cryptoquant | data/config/cryptoquant-spec.json | 157292 | - | STALE |
| 113.1 | 30 | justhodl-cryptoquant | data/cryptoquant-onchain.json | 327369 | 2026-10-16T21:05:07+00:00 | STALE |
| 113.1 | 30 | justhodl-cryptoquant | data/cryptoquant-series.json | 3767789 | 2026-10-16T21:05:07+00:00 | STALE |
| 131.2 | 30 | justhodl-cryptoquant | data/history/alt-px-cm.json | 637409 | - | STALE |
| 131.2 | 30 | justhodl-cryptoquant | data/history/btc-price-cm.json | 175993 | - | STALE |
| 113.1 | 30 | justhodl-cryptoquant | data/history/cryptoquant.json | 3640009 | - | STALE |
| 131.2 | 30 | justhodl-cryptoquant | data/history/eth-price-cm.json | 121027 | - | STALE |
| 112.6 | 80 | justhodl-dark-pool | data/dark-pool.json | 1454842 | - | STALE |
| 112.2 | 80 | justhodl-dix | data/dix-history.json | 131560 | 2026-10-16T22:00:43.58955 | STALE |
| 112.2 | 80 | justhodl-dix | data/dix.json | 2714 | 2026-10-16T22:00:43.58955 | STALE |
| 138.4 | 30 | justhodl-earnings-pead | data/earnings-pead.json | 24251068 | 2026-10-15T19:48:19.59357 | STALE |
| 0.7 | 30 | justhodl-earnings-quality | data/earnings-quality.json | 5089286 | 2026-10-21T13:31:04.51539 | FRESH |
| 2.7 | 30 | justhodl-earnings-tracker | data/earnings-tracker.json | 23965 | 2026-10-21T11:30:42.10298 | FRESH |
| 266.2 | 30 | justhodl-estimate-revisions | data/estimate-revisions.json | 4668982 | 2026-10-10T12:01:02.17803 | STALE |
| 578.2 | 30 | justhodl-estimate-revisions | estimate-revisions/state.json | 196878 | 2026-09-27T12:00:35.78175 | STALE |
| 129.3 | 30 | justhodl-etf-global-desk | data/etf-derived.json | 1937 | 2026-10-16T04:56:51.79228 | STALE |
| 129.3 | 30 | justhodl-etf-global-desk | data/etf-desk-research.json | 5071409 | 2026-10-16T04:00:00.61125 | STALE |
| 723.3 | 30 | justhodl-etf-global-desk | data/etf-desk-research/migration.json | 219 | - | STALE |
| 129.3 | 30 | justhodl-etf-global-desk | data/etf-desk.json | 1932 | 2026-10-16T04:56:51.79228 | STALE |
| 129.3 | 30 | justhodl-etf-global-desk | data/etf-global-desk-meta.json | 1953 | 2026-10-16T04:56:51.79228 | STALE |
| 129.3 | 30 | justhodl-etf-global-desk | data/etf-global.json | 1932 | 2026-10-16T04:56:51.79228 | STALE |
| 129.3 | 30 | justhodl-etf-global-desk | data/etf-holdings-complete.json | 1958 | 2026-10-16T04:56:51.79228 | STALE |
| 129.3 | 30 | justhodl-etf-global-desk | data/etf-holdings-index.json | 1953 | 2026-10-16T04:56:51.79228 | STALE |
| 13.2 | 80 | justhodl-finra-short | data/finra-short.json | 533832 | Wed, 21 Oct 2026 01:00:35 | FRESH |
| 3.2 | 30 | justhodl-finviz-news | data/finviz-news.json | 77478 | 2026-10-21T11:00:24.61586 | FRESH |
| 0.7 | 30 | justhodl-flow-confluence | data/flow-confluence.json | 31198 | 2026-10-21T13:31:19.08397 | FRESH |
| 725.7 | 30 | justhodl-flow-lookthrough | data/etf-holdings-research/migration.json | 227 | - | STALE |
| 0.6 | 30 | justhodl-flow-lookthrough | data/flow-lookthrough.json | 986151 | - | FRESH |
| 735.0 | 30 | justhodl-flow-lookthrough | data/impact/etf-holdings-index.json | 7214 | 2026-09-20T23:15:23.10685 | STALE |
| 0.7 | 30 | justhodl-forensic-screen | data/forensic-screen.json | 2397831 | - | FRESH |
| 112.2 | 30 | justhodl-gf-value | data/gf-value.json | 419756 | 2026-10-16T22:00:14.98537 | STALE |
| 2.0 | 30 | justhodl-indicator-bus | data/indicator-bus.json | 2020105 | 2026-10-21T12:15:22.74842 | FRESH |
| 10.8 | 30 | justhodl-khalid | data/history/khalid.json | 163556 | 2026-10-21T03:25:50.91864 | FRESH |
| 10.8 | 30 | justhodl-khalid | data/khalid-candidates.json | 18454100 | 2026-10-21T03:25:50.91864 | FRESH |
| 10.8 | 30 | justhodl-khalid | data/khalid.json | 31958894 | 2026-10-21T03:25:50.91864 | FRESH |
| 112.2 | 198 | justhodl-liquidity-profile | data/liquidity-profile.json | 245457 | 2026-10-16T22:00:38.20392 | FRESH |
| 111.9 | 30 | justhodl-options-confluence | data/options-confluence.json | 21223 | 2026-10-16T22:20:04.82314 | STALE |
| 0.7 | 30 | justhodl-provider-catalog | data/audit/provider-join-debug.json | 471 | - | FRESH |
| 0.7 | 30 | justhodl-provider-catalog | data/audit/provider-join-lastgood.json | 71783 | - | FRESH |
| 0.6 | 30 | justhodl-provider-catalog | data/provider-catalog.json | 42704 | 2026-10-21T13:31:21+00:00 | FRESH |
| 0.6 | 30 | justhodl-provider-catalog | data/search/provider-shards.json | 21123 | 2026-10-21T13:31:21+00:00 | FRESH |
| 6.3 | 30 | justhodl-public-archive-index | data/archive-indexes/justhodl-dark-pool.json | 2354 | 2026-10-21T07:58:53.37687 | FRESH |
| 2.1 | 30 | justhodl-risk-gate | data/risk-gate.json | 403107 | - | FRESH |
| 600.6 | 30 | justhodl-share-flows | data/share-flows.json | 7595375 | - | STALE |
| 6.9 | 30 | justhodl-short-interest | data/short-interest.json | 5022896 | 2026-10-21T07:17:22.36342 | FRESH |
| 128.8 | 30 | justhodl-squeeze-pretrigger | data/squeeze-pretrigger.json | 6873 | 2026-10-16T05:26:52.75788 | STALE |
| - | 30 | justhodl-symbology-master | data/symbology/bond-cusips.json | - | ClientError | MISSING |
| 9.0 | 30 | justhodl-symbology-master | data/symbology/master.json | 20810127 | 2026-10-21T05:16:21+00:00 | FRESH |
| 161.6 | 30 | justhodl-symdir | data/symdir/fred-titles.json | 470682 | - | STALE |
| 8.5 | 30 | justhodl-symdir | data/symdir/fred-untitled.json | 68961 | - | FRESH |
| 8.5 | 30 | justhodl-symdir | data/symdir/instruments.json.gz | 613768 | - | FRESH |
| 8.5 | 30 | justhodl-symdir | data/symdir/manifest.json | 8893 | - | FRESH |
| 8.5 | 30 | justhodl-symdir | data/symdir/wb-countries.json | 24604 | - | FRESH |
| - | 30 | justhodl-symdir | data/symdir/wb-indicators.json.gz | - | ClientError | MISSING |
| 112.7 | 30 | justhodl-symdir | data/warm/treasury-par/curve.json.gz | 180750 | - | STALE |
| - | - | justhodl-tv-notes-ingest | data/tv-descriptions.json | - | ClientError | MISSING |
| 1386.6 | - | justhodl-tv-notes-ingest | data/tv-watchlists.json | 503551 | 2026-08-24T19:40:21.01052 | DEAD-WRITER |
| 1619.1 | - | justhodl-tv-notes-ingest | data/warm/archived-fred/_index.json | 2218 | 2026-08-15T03:07:41+00:00 | DEAD-WRITER |
| 1610.2 | - | justhodl-tv-notes-ingest | data/warm/tv-bars/_index.json | 74 | 2026-08-15T12:05:03+00:00 | DEAD-WRITER |
| 873.8 | - | (none - one-off ops 5576 harvest 2026-09-15) | data/fmp-ratios.json | 181618 | 2026-09-15T04:24:38.46802 | DEAD-WRITER |
| - | - | writers not run since thaw: 10 | justhodl-cftc-deep-view, justhodl-cryptoquant, justhodl-dark-pool, justhodl-dix, justhodl-etf-global-desk, justhodl-finra-short, justhodl-gf-value, justhodl-liquidity-profile, justhodl-options-confluence, justhodl-squeeze-pretrigger | - | held: none | SUMMARY |

## Log
- `14:14:51` contract: 82 outputs of 39 engines bound to chart.html; +3 extra functions, +7 unbound families; thaw 10-21 00:30; now 10-21 14:14 UTC
## 1. writers

- `14:15:02` justhodl-13f-positions               trig=cron(21 16 * * ? *);cron(40 15 * * ? *) | last inv 10-21 13:14 | 8d inv 82 err 0 | days {"10-13": 10, "10-14": 24, "10-15": 24, "10-16": 23, "10-21": 1} | timeout 600s mem 1024 rc 2 next today 15:40
- `14:15:02` justhodl-ai                          trig=cron(7 7 * * ? *);cron(45 5 * * ? *);cron(37 6 * * ? *);cron(31 9 ? *  | last inv 10-21 06:14 | 8d inv 403 err 0 | days {"10-13": 40, "10-14": 107, "10-15": 109, "10-16": 144, "10-21": 3} | timeout 900s mem 3008 rc 2 next today 05:45
- `14:15:02` justhodl-beneish                     trig=cron(29 14 * * ? *) | last inv 10-21 13:14 | 8d inv 3 err 0 | days {"10-15": 1, "10-16": 1, "10-21": 1} | timeout 600s mem 1024 rc None next today 14:29
- `14:15:02` justhodl-boom-radar                  trig=cron(25 15 * * ? *) | last inv 10-21 13:14 | 8d inv 4 err 0 | days {"10-14": 1, "10-15": 1, "10-16": 1, "10-21": 1} | timeout 120s mem 512 rc None next today 15:25
- `14:15:02` justhodl-bottom                      trig=cron(45 3 ? * TUE-SAT *) | last inv 10-21 03:14 | 8d inv 4 err 0 | days {"10-14": 1, "10-15": 1, "10-16": 1, "10-21": 1} | timeout 900s mem 8192 rc None next today 03:45
- `14:15:02` justhodl-brief-compiler              trig=cron(35 18 * * ? *);cron(20 22 * * ? *);cron(25 18 * * ? *);cron(40 1  | last inv 10-21 01:14 | 8d inv 59 err 0 | days {"10-13": 3, "10-14": 14, "10-15": 18, "10-16": 22, "10-21": 2} | timeout 120s mem 512 rc None next today 01:40
- `14:15:02` justhodl-buyback-engine              trig=cron(40 13 * * ? *) | last inv 10-21 13:14 | 8d inv 4 err 0 | days {"10-13": 1, "10-15": 1, "10-21": 2} | timeout 600s mem 512 rc None next today 13:40
- `14:15:02` justhodl-catalyst-calendar           trig=cron(0 12 * * ? *) | last inv 10-21 11:14 | 8d inv 4 err 0 | days {"10-14": 1, "10-15": 1, "10-16": 1, "10-21": 1} | timeout 120s mem 512 rc 1 next today 12:00
- `14:15:02` justhodl-cftc-deep-view              trig=cron(0 21 ? * FRI,SAT *) | last inv 10-16 21:14 (NOT since thaw) | 8d inv 2 err 0 | days {"10-16": 2} | timeout 300s mem 1024 rc None next today -
- `14:15:02` justhodl-ciss-stress                 trig=cron(10 7 * * ? *) | last inv 10-21 06:14 | 8d inv 2 err 0 | days {"10-16": 1, "10-21": 1} | timeout 600s mem 1024 rc None next today 07:10
- `14:15:02` justhodl-cq-feed                     trig=cron(50 10 * * ? *) | last inv 10-21 10:14 | 8d inv 2 err 0 | days {"10-14": 1, "10-21": 1} | timeout 900s mem 1024 rc None next today 10:50
- `14:15:02` justhodl-crypto-etf-flows            trig=cron(50 21 * * ? *) | last inv 10-21 13:14 | 8d inv 8 err 0 | days {"10-14": 2, "10-15": 2, "10-16": 3, "10-21": 1} | timeout 150s mem 256 rc None next today 21:50
- `14:15:02` justhodl-cryptoquant                 trig=cron(5 21 * * ? *) | last inv 10-16 20:14 (NOT since thaw) | 8d inv 4 err 0 | days {"10-14": 1, "10-15": 1, "10-16": 2} | timeout 900s mem 1024 rc None next today 21:05
- `14:15:02` justhodl-dark-pool                   trig=cron(40 21 ? * MON-FRI *) | last inv 10-16 21:14 (NOT since thaw) | 8d inv 3 err 0 | days {"10-15": 1, "10-16": 2} | timeout 300s mem 1024 rc None next today 21:40
- `14:15:02` justhodl-dix                         trig=cron(0 22 ? * MON-FRI *) | last inv 10-16 21:14 (NOT since thaw) | 8d inv 4 err 0 | days {"10-14": 1, "10-15": 1, "10-16": 2} | timeout 60s mem 512 rc None next today 22:00
- `14:15:02` justhodl-earnings-pead               trig=tick:daily-08utc@cron(0 8 * * ? *) | last inv 10-21 07:14 | 8d inv 5 err 3 | days {"10-14": 1, "10-15": 1, "10-21": 3} | timeout 600s mem 1024 rc None next today 08:00
- `14:15:02` justhodl-earnings-quality            trig=cron(18 14 * * ? *) | last inv 10-21 13:14 | 8d inv 3 err 0 | days {"10-14": 1, "10-16": 1, "10-21": 1} | timeout 720s mem 1536 rc None next today 14:18
- `14:15:02` justhodl-earnings-tracker            trig=cron(30 11 * * ? *) | last inv 10-21 11:14 | 8d inv 1 err 0 | days {"10-21": 1} | timeout 600s mem 512 rc None next today 11:30
- `14:15:02` justhodl-estimate-revisions          trig=cron(0 12 * * ? *) | last inv 10-21 13:14 | 8d inv 16 err 16 | days {"10-14": 4, "10-15": 2, "10-16": 4, "10-21": 6} | timeout 120s mem 512 rc None next today 12:00
- `14:15:02` justhodl-etf-global-desk             trig=cron(5 23 * * ? *) | last inv 10-16 04:14 (NOT since thaw) | 8d inv 3 err 0 | days {"10-14": 1, "10-15": 1, "10-16": 1} | timeout 900s mem 4096 rc None next today 23:05
- `14:15:02` justhodl-finra-short                 trig=cron(0 1 ? * TUE-SAT *) | last inv 10-21 00:14 (NOT since thaw) | 8d inv 1 err 0 | days {"10-21": 1} | timeout 300s mem 1024 rc None next today 01:00
- `14:15:02` justhodl-finviz-news                 trig=cron(0 11 * * ? *) | last inv 10-21 10:14 | 8d inv 3 err 0 | days {"10-15": 1, "10-16": 1, "10-21": 1} | timeout 60s mem 256 rc None next today 11:00
- `14:15:02` justhodl-flow-confluence             trig=cron(36 14 * * ? *) | last inv 10-21 13:14 | 8d inv 8 err 0 | days {"10-14": 2, "10-15": 3, "10-16": 2, "10-21": 1} | timeout 120s mem 512 rc None next today 14:36
- `14:15:02` justhodl-flow-lookthrough            trig=cron(15 23 * * ? *) | last inv 10-21 13:14 | 8d inv 4 err 0 | days {"10-14": 1, "10-15": 1, "10-16": 1, "10-21": 1} | timeout 840s mem 3072 rc None next today 23:15
- `14:15:02` justhodl-forensic-screen             trig=cron(26 17 * * ? *) | last inv 10-21 13:14 | 8d inv 4 err 0 | days {"10-13": 1, "10-15": 1, "10-16": 1, "10-21": 1} | timeout 840s mem 1024 rc None next today 17:26
- `14:15:02` justhodl-gf-value                    trig=cron(0 22 * * ? *) | last inv 10-16 21:14 (NOT since thaw) | 8d inv 3 err 0 | days {"10-15": 1, "10-16": 2} | timeout 900s mem 1536 rc None next today 22:00
- `14:15:02` justhodl-indicator-bus               trig=cron(15 12 * * ? *) | last inv 10-21 12:14 | 8d inv 3 err 0 | days {"10-13": 1, "10-16": 1, "10-21": 1} | timeout 120s mem 512 rc None next today 12:15
- `14:15:02` justhodl-khalid                      trig=cron(25 3 * * ? *) | last inv 10-21 03:14 | 8d inv 51 err 0 | days {"10-13": 4, "10-14": 12, "10-15": 16, "10-16": 18, "10-21": 1} | timeout 180s mem 1024 rc None next today 03:25
- `14:15:02` justhodl-liquidity-profile           trig=cron(0 22 ? * FRI *) | last inv 10-16 21:14 (NOT since thaw) | 8d inv 1 err 0 | days {"10-16": 1} | timeout 600s mem 1024 rc None next today -
- `14:15:02` justhodl-options-confluence          trig=cron(20 15 * * ? *) | last inv 10-16 22:14 (NOT since thaw) | 8d inv 53 err 0 | days {"10-13": 5, "10-14": 12, "10-15": 17, "10-16": 19} | timeout 120s mem 512 rc None next today 15:20
- `14:15:02` justhodl-provider-catalog            trig=cron(13 22 * * ? *) | last inv 10-21 13:14 | 8d inv 59 err 0 | days {"10-13": 7, "10-14": 15, "10-15": 16, "10-16": 20, "10-21": 1} | timeout 900s mem 1024 rc 2 next today 22:13
- `14:15:02` justhodl-public-archive-index        trig=cron(58 7 * * ? *) | last inv 10-21 07:14 | 8d inv 221 err 0 | days {"10-13": 23, "10-14": 55, "10-15": 57, "10-16": 85, "10-21": 1} | timeout 180s mem 512 rc None next today 07:58
- `14:15:02` justhodl-risk-gate                   trig=cron(6 12 * * ? *) | last inv 10-21 11:14 | 8d inv 55 err 0 | days {"10-13": 3, "10-14": 16, "10-15": 13, "10-16": 22, "10-21": 1} | timeout 600s mem 1024 rc None next today 12:06
- `14:15:02` justhodl-share-flows                 trig=cron(35 13 * * ? *) | last inv 10-21 13:14 | 8d inv 7 err 7 | days {"10-14": 1, "10-16": 3, "10-21": 3} | timeout 900s mem 4096 rc None next today 13:35
- `14:15:02` justhodl-short-interest              trig=cron(15 7 ? * * *) | last inv 10-21 07:14 | 8d inv 6 err 0 | days {"10-14": 2, "10-15": 2, "10-16": 1, "10-21": 1} | timeout 360s mem 512 rc None next today 07:15
- `14:15:02` justhodl-squeeze-pretrigger          trig=cron(30 23 * * ? *) | last inv 10-16 05:14 (NOT since thaw) | 8d inv 2 err 0 | days {"10-15": 1, "10-16": 1} | timeout 540s mem 768 rc None next today 23:30
- `14:15:02` justhodl-symbology-master            trig=cron(15 5 * * ? *) | last inv 10-21 05:14 | 8d inv 2 err 0 | days {"10-15": 1, "10-21": 1} | timeout 120s mem 1024 rc None next today 05:15
- `14:15:02` justhodl-symdir                      trig=cron(40 5 * * ? *);cron(46 13 ? * MON-FRI *);cron(44 22 * * ? *);cron( | last inv 10-21 13:14 | 8d inv 18531 err 0 | days {"10-13": 3995, "10-14": 8701, "10-15": 3222, "10-16": 1007, "10-21": 1606} | timeout 900s mem 6144 rc 2 next today 05:40
- `14:15:02` justhodl-symdir-fredfresh            trig=NONE | last inv none in 8d (NOT since thaw) | 8d inv 0 err 0 | days {} | timeout Nones mem None rc None
- `14:15:02` justhodl-tv-bars                     trig=cron(30 2 * * ? *) | last inv 10-21 02:14 | 8d inv 3745 err 0 | days {"10-13": 756, "10-14": 2773, "10-15": 175, "10-16": 36, "10-21": 5} | timeout 600s mem 1024 rc None next today 02:30
- `14:15:02` justhodl-tv-notes-ingest             trig=NONE | last inv none in 8d (NOT since thaw) | 8d inv 0 err 0 | days {} | timeout 300s mem 1024 rc 2
- `14:15:02` justhodl-wl-series-api               trig=NONE | last inv 10-21 12:14 | 8d inv 11219 err 0 | days {"10-13": 26, "10-14": 554, "10-15": 555, "10-16": 587, "10-21": 9497} | timeout 60s mem 2048 rc 2
## 2. outputs

- `14:15:14` MISSING (3): data/symbology/bond-cusips.json <- justhodl-symbology-master (0h); data/symdir/wb-indicators.json.gz <- justhodl-symdir (0h); data/tv-descriptions.json <- justhodl-tv-notes-ingest (0h)
- `14:15:14` DEAD-WRITER (4): data/tv-watchlists.json <- justhodl-tv-notes-ingest (1387h); data/warm/archived-fred/_index.json <- justhodl-tv-notes-ingest (1619h); data/warm/tv-bars/_index.json <- justhodl-tv-notes-ingest (1610h); data/fmp-ratios.json <- (none - one-off ops 5576 harvest 2026-09-15) (874h)
- `14:15:14` STALE (33): data/13f-cusip-map.json <- justhodl-13f-positions (759h); data/13f-price-anchors.json <- justhodl-13f-positions (759h); data/13f-state/first-seen.json <- justhodl-13f-positions (759h); data/jh-internals.json <- justhodl-brief-compiler (112h); data/config/cryptoquant-spec.json <- justhodl-cryptoquant (113h); data/cryptoquant-onchain.json <- justhodl-cryptoquant (113h); data/cryptoquant-series.json <- justhodl-cryptoquant (113h); data/history/alt-px-cm.json <- justhodl-cryptoquant (131h); data/history/btc-price-cm.json <- justhodl-cryptoquant (131h); data/history/cryptoquant.json <- justhodl-cryptoquant (113h); data/history/eth-price-cm.json <- justhodl-cryptoquant (131h); data/dark-pool.json <- justhodl-dark-pool (113h); data/dix-history.json <- justhodl-dix (112h); data/dix.json <- justhodl-dix (112h); data/earnings-pead.json <- justhodl-earnings-pead (138h); data/estimate-revisions.json <- justhodl-estimate-revisions (266h); estimate-revisions/state.json <- justhodl-estimate-revisions (578h); data/etf-derived.json <- justhodl-etf-global-desk (129h); data/etf-desk-research.json <- justhodl-etf-global-desk (129h); data/etf-desk-research/migration.json <- justhodl-etf-global-desk (723h); data/etf-desk.json <- justhodl-etf-global-desk (129h); data/etf-global-desk-meta.json <- justhodl-etf-global-desk (129h); data/etf-global.json <- justhodl-etf-global-desk (129h); data/etf-holdings-complete.json <- justhodl-etf-global-desk (129h); data/etf-holdings-index.json <- justhodl-etf-global-desk (129h); data/etf-holdings-research/migration.json <- justhodl-flow-lookthrough (726h); data/impact/etf-holdings-index.json <- justhodl-flow-lookthrough (735h); data/gf-value.json <- justhodl-gf-value (112h); data/options-confluence.json <- justhodl-options-confluence (112h); data/share-flows.json <- justhodl-share-flows (601h); data/squeeze-pretrigger.json <- justhodl-squeeze-pretrigger (129h); data/symdir/fred-titles.json <- justhodl-symdir (162h); data/warm/treasury-par/curve.json.gz <- justhodl-symdir (113h)
## 3. unbound families

- `14:15:14` data/watchlist-insights/stock/       writer github:watchlist-insights.yml (cron 35 23 * * *) | first page 812 keys | newest 10-18 23:42 (62.5h) | oldest 10-12
- `14:15:14` data/watchlist-insights/etf/         writer github:watchlist-insights.yml (cron 35 23 * * *) | first page 294 keys | newest 10-18 23:42 (62.5h) | oldest 10-18
- `14:15:15` data/warm/tv-bars/universe/          writer justhodl-tv-bars (02:30 UTC) | first page 650 keys | newest 10-21 02:30 (11.7h) | oldest 09-03
- `14:15:15` data/warm/tv-bars/                   writer justhodl-tv-bars (02:30 UTC) | first page 652 keys | newest 10-21 02:30 (11.7h) | oldest 08-15
- `14:15:15` data/series/                         writer justhodl-symdir series bank (opened-on-demand heal + fredfresh) | first page 0 keys | newest - (-1.0h) | oldest -
- `14:15:15` data/etf-flow-hist/                  writer ? | first page 117 keys | newest 09-20 22:20 (735.9h) | oldest 09-20
- `14:15:15` data/providers/                      writer ? | first page 1000 keys (more) | newest 10-21 13:37 (0.6h) | oldest 08-20
## 4. fmp-ratios.json detail

- `14:15:15` generated_at 2026-09-15T04:24:38.468021+00:00 schema fmp-ratios.v1 cadence EOD_TTM key_status {"status": "ok"} tickers 40: AAPL,ABBV,ABT,ACN,ADBE,AMD,AMZN,ASML,AVGO,BAC,BRK-B,COST,CRM,CSCO,CVX,GE,GOOGL,HD,JNJ,JPM,KO,LIN,LLY,MA,MCD,META,MRK,MSFT,NFLX,NVDA,ORCL,PEP,PG,TMO,TSLA,TSM,UNH,V,WMT,XOM
- `14:15:15` fields per ticker: ['accruals', 'acq_driven', 'acq_pct', 'beat_rate', 'beats_n', 'beta', 'cash_conv', 'ceo', 'cur_ratio', 'desc', 'div_yield', 'employees', 'ev_ebitda', 'exchange', 'fcfm_latest', 'financials', 'gm_latest', 'gm_trend', 'industry', 'insider_buys', 'insider_net', 'insider_sells', 'insider_sig', 'int_cov', 'mkt_cap', 'name', 'net_debt_ebitda', 'next_earnings', 'nq_eps_est', 'nq_rev_est', 'off_52w_high', 'off_52w_low', 'om_latest', 'pb', 'pe', 'pe_high', 'pe_low', 'pe_pctile', 'peg', 'price']
- `14:15:15` SUMMARY: outputs {"FRESH": 43, "STALE": 33, "MISSING": 3, "DEAD-WRITER": 4} | writers not run since the thaw (10): justhodl-cftc-deep-view, justhodl-cryptoquant, justhodl-dark-pool, justhodl-dix, justhodl-etf-global-desk, justhodl-finra-short, justhodl-gf-value, justhodl-liquidity-profile, justhodl-options-confluence, justhodl-squeeze-pretrigger | held (rc=0): none
- `14:15:15` ✅ census read
