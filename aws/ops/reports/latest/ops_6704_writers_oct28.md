# ops 6704 - who writes on Oct 28 so far vs Oct 27 (access logs, read-only)

**Status:** success  
**Duration:** 151.5s  
**Finished:** 2026-10-28T16:26:50+00:00  

## Data

| writes_2026-10-27 | writes_2026-10-28 |
|---|---|
| 270949 | 747059 |

## Log
- `16:25:43` 2026/10/27: 259323 log objects over 24.0 h, read 6000 (x43.2 to a day): ~270949 writes/day
- `16:26:50` 2026/10/28: 189786 log objects over 16.0 h, read 6000 (x47.5 to a day): ~747059 writes/day
## writers per day (scaled), biggest first by the latest day

- `16:26:50` justhodl-bank-histories                            10/27  110125 | 10/28  652614
- `16:26:50` justhodl-polygon-eod                               10/27    2117 | 10/28   28300
- `16:26:50` justhodl-bank-factors                              10/27   28309 | 10/28   21984
- `16:26:50` justhodl-polygon-bank                              10/27    5748 | 10/28   19135
- `16:26:50` justhodl-tradingview                               10/27     605 | 10/28    3323
- `16:26:50` justhodl-eia-bank                                  10/27    2204 | 10/28    3323
- `16:26:50` justhodl-repo                                      10/27    8557 | 10/28    2374
- `16:26:50` justhodl-fmp-walker                                10/27       0 | 10/28    2374
- `16:26:50` justhodl-src-mirror                                10/27       0 | 10/28    2374
- `16:26:50` justhodl-polygon-fx-regime                         10/27    7779 | 10/28    2136
- `16:26:50` justhodl-signal-board                              10/27       0 | 10/28    1044
- `16:26:50` justhodl-tail-risk                                 10/27    2420 | 10/28     902
- `16:26:50` justhodl-implied-prob                              10/27     734 | 10/28     712
- `16:26:50` justhodl-risk-regime                               10/27    2204 | 10/28     474
- `16:26:50` justhodl-daily-report-v3                           10/27    2117 | 10/28     474
- `16:26:50` justhodl-fundamental-graphs                        10/27     345 | 10/28     379
- `16:26:50` justhodl-short-interest                            10/27       0 | 10/28     379
- `16:26:50` justhodl-fiscaldata-full                           10/27       0 | 10/28     332
- `16:26:50` justhodl-finra-short                               10/27     129 | 10/28     332
- `16:26:50` justhodl-polygon-futures-curves                    10/27      86 | 10/28     284
## what grew most (latest day vs Oct 23), writer x folder

- `16:26:50` + 542488/day  justhodl-bank-histories                      data/warm/history-book/
- `16:26:50` +  26182/day  justhodl-polygon-eod                         data/warm/polygon-full/
- `16:26:50` +  13387/day  justhodl-polygon-bank                        data/warm/polygon-bank/
- `16:26:50` +   2374/day  justhodl-src-mirror                          data/warm/nyfed-research/
- `16:26:50` +   2374/day  justhodl-fmp-walker                          data/warm/fmp/
- `16:26:50` +   1890/day  justhodl-tradingview                         data/evidence/
- `16:26:50` +   1119/day  justhodl-eia-bank                            data/warm/eia-bank/
- `16:26:50` +    828/day  justhodl-tradingview                         data/fred-levels/
- `16:26:50` +    617/day  justhodl-signal-board                        audit-private/20260909-originals/signal-board-research/
- `16:26:50` +    427/day  justhodl-signal-board                        data/signal-board-research/
- `16:26:50` +    332/day  justhodl-short-interest                      audit-private/20260909-originals/short-interest-research/
- `16:26:50` +    332/day  justhodl-fiscaldata-full                     data/warm/fiscaldata-full/
- `16:26:50` +    237/day  justhodl-buyback-engine                      data/buyback-engine/
- `16:26:50` +    202/day  justhodl-finra-short                         audit-private/20260909-originals/short-volume-research/
- `16:26:50` +    198/day  justhodl-polygon-futures-curves              audit-private/20260909-originals/futures-research/
- `16:26:50` ✅ read-only; nothing changed
