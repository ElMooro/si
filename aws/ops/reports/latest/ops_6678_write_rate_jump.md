# ops 6678 - who doubled the writes on Oct 25-26? (access logs, read-only)

**Status:** success  
**Duration:** 162.4s  
**Finished:** 2026-10-26T16:21:25+00:00  

## Data

| writes_2026-10-23 | writes_2026-10-25 | writes_2026-10-26 |
|---|---|---|
| 88920 | 145592 | 1051416 |

## Log
- `16:19:36` 2026/10/23: 242839 log objects over 45.2 h, read 6000 (x40.5 to a day): ~88919 writes/day
- `16:20:19` 2026/10/25: 186258 log objects over 23.8 h, read 6000 (x31.0 to a day): ~145591 writes/day
- `16:21:25` 2026/10/26: 325258 log objects over 15.2 h, read 6000 (x85.9 to a day): ~1051416 writes/day
## writers per day (scaled), biggest first by the latest day

- `16:21:25` justhodl-bank-histories                            10/23       0 | 10/25       0 | 10/26  639059
- `16:21:25` justhodl-polygon-eod                               10/23       0 | 10/25   31198 | 10/26  212962
- `16:21:25` justhodl-bank-factors                              10/23       0 | 10/25       0 | 10/26  104248
- `16:21:25` justhodl-worldbank-full                            10/23       0 | 10/25       0 | 10/26   45082
- `16:21:25` justhodl-polygon-bank                              10/23       0 | 10/25    7512 | 10/26   26792
- `16:21:25` justhodl-corporate-actions                         10/23       0 | 10/25       0 | 10/26    4121
- `16:21:25` justhodl-global-business-cycle                     10/23     283 | 10/25      31 | 10/26    3692
- `16:21:25` justhodl-polygon-fx-regime                         10/23    9915 | 10/25      31 | 10/26    3005
- `16:21:25` justhodl-eia-bank                                  10/23       0 | 10/25    1552 | 10/26    2833
- `16:21:25` runner:github-actions-justhodl                     10/23   17605 | 10/25   24430 | 10/26    1889
- `16:21:25` justhodl-tail-risk                                 10/23    1861 | 10/25       0 | 10/26    1545
- `16:21:25` justhodl-fundamental-graphs                        10/23    6637 | 10/25    1521 | 10/26    1373
- `16:21:25` justhodl-implied-prob                              10/23     890 | 10/25       0 | 10/26     858
- `16:21:25` justhodl-imf-full                                  10/23       0 | 10/25       0 | 10/26     515
- `16:21:25` justhodl-ciss-stress                               10/23     161 | 10/25      93 | 10/26     343
- `16:21:25` justhodl-equity-research                           10/23    6070 | 10/25    2700 | 10/26     343
- `16:21:25` justhodl-vintage-fred                              10/23       0 | 10/25     310 | 10/26     343
- `16:21:25` justhodl-intraday-pulse                            10/23     364 | 10/25       0 | 10/26     257
- `16:21:25` justhodl-vol-surface                               10/23       0 | 10/25       0 | 10/26     257
- `16:21:25` justhodl-short-interest                            10/23     121 | 10/25     372 | 10/26     257
## what grew most (latest day vs Oct 23), writer x folder

- `16:21:25` + 639059/day  justhodl-bank-histories                      data/warm/history-book/
- `16:21:25` + 212962/day  justhodl-polygon-eod                         data/warm/polygon-full/
- `16:21:25` + 104248/day  justhodl-bank-factors                        data/warm/factor-book/
- `16:21:25` +  45082/day  justhodl-worldbank-full                      data/warm/worldbank-full/
- `16:21:25` +  26792/day  justhodl-polygon-bank                        data/warm/polygon-bank/
- `16:21:25` +   4121/day  justhodl-corporate-actions                   data/corporate-actions/
- `16:21:25` +   3530/day  justhodl-global-business-cycle               audit-private/20260909-originals/global-business-cycle-research/
- `16:21:25` +   2833/day  justhodl-eia-bank                            data/warm/eia-bank/
- `16:21:25` +   1889/day  runner:github-actions-justhodl               data/ops/
- `16:21:25` +    515/day  justhodl-imf-full                            data/warm/imf-full/
- `16:21:25` +    343/day  justhodl-vintage-fred                        data/vintage-research/
- `16:21:25` +    257/day  justhodl-vol-surface                         audit-private/20260909-originals/volatility-research/
- `16:21:25` +    181/day  justhodl-ciss-stress                         data/ciss-research/
- `16:21:25` +    171/day  justhodl-apex-fusion                         audit-private/20260909-originals/momentum-leaders-research/
- `16:21:25` +    171/day  justhodl-debate-engine                       data/(files)
- `16:21:25` ✅ read-only; nothing changed
