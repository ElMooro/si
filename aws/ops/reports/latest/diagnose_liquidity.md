# Why is liquidity.html broken?

**Status:** success  
**Duration:** 0.4s  
**Finished:** 2026-04-25T22:47:32+00:00  

## Data

| liquidity_public | n_public_resources |
|---|---|
| False | 24 |

## Log
## A. S3 key existence

- `22:47:32`   ✅ liquidity-data.json                      10475B  2026-04-25 12:30:39
- `22:47:32`   ❌ data/liquidity-data.json            404
- `22:47:32`   ❌ data/liquidity.json                 404
- `22:47:32`   ❌ fred-liquidity.json                 404
- `22:47:32`   ✅ data/report.json                       1754369B  2026-04-25 22:45:05
## B. Current bucket policy resources

- `22:47:32`   PublicReadDataDir: data/*
- `22:47:32`   PublicReadScreener: screener/*
- `22:47:32`   PublicReadSentiment: sentiment/*
- `22:47:32`   PublicReadRootDashboardFiles:
- `22:47:32`     - flow-data.json
- `22:47:32`     - crypto-intel.json
- `22:47:32`     - health.html
- `22:47:32`     - _health/*
- `22:47:32`     - intelligence-report.json
- `22:47:32`     - edge-data.json
- `22:47:32`     - repo-data.json
- `22:47:32`     - ai-prediction.json
- `22:47:32`     - options-flow.json
- `22:47:32`     - valuations.json
- `22:47:32`     - morning-brief.json
- `22:47:32`   PublicReadRegime: regime/*
- `22:47:32`   PublicReadDivergence: divergence/*
- `22:47:32`   PublicReadCOT: cot/*
- `22:47:32`   PublicReadRisk: risk/*
- `22:47:32`   PublicReadOpportunities: opportunities/*
- `22:47:32`   PublicReadPortfolio: portfolio/*
- `22:47:32`   PublicReadInvestorDebate: investor-debate/*
- `22:47:32`   PublicReadReports: reports/*
- `22:47:32`   PublicReadArchive: archive/*
- `22:47:32`   PublicReadLearning: learning/*
## C. Is liquidity-data.json public-readable?

- `22:47:32`   All public resources (24):
- `22:47:32`     _health/*
- `22:47:32`     ai-prediction.json
- `22:47:32`     archive/*
- `22:47:32`     cot/*
- `22:47:32`     crypto-intel.json
- `22:47:32`     data/*
- `22:47:32`     divergence/*
- `22:47:32`     edge-data.json
- `22:47:32`     flow-data.json
- `22:47:32`     health.html
- `22:47:32`     intelligence-report.json
- `22:47:32`     investor-debate/*
- `22:47:32`     learning/*
- `22:47:32`     morning-brief.json
- `22:47:32`     opportunities/*
- `22:47:32`     options-flow.json
- `22:47:32`     portfolio/*
- `22:47:32`     regime/*
- `22:47:32`     repo-data.json
- `22:47:32`     reports/*
- `22:47:32`     risk/*
- `22:47:32`     screener/*
- `22:47:32`     sentiment/*
- `22:47:32`     valuations.json
- `22:47:32` ⚠ 
  ⚠ liquidity-data.json is NOT public — this is the bug
- `22:47:32` ⚠   Need to add it to PublicReadRootDashboardFiles statement
## D. What other root JSONs might pages need?

- `22:47:32`   Root .json files: 14
- `22:47:32`     ❌ crypto-data.json                                   40110B
- `22:47:32`     ✅ crypto-intel.json                                  57280B
- `22:47:32`     ❌ data-peek.json                                     60635B
- `22:47:32`     ❌ data.json                                          60635B
- `22:47:32`     ✅ edge-data.json                                      1888B
- `22:47:32`     ✅ flow-data.json                                     31438B
- `22:47:32`     ✅ intelligence-report.json                            4369B
- `22:47:32`     ❌ liquidity-data.json                                10475B
- `22:47:32`     ❌ manifest.json                                        264B
- `22:47:32`     ❌ predictions.json                                   14351B
- `22:47:32`     ❌ pro-data.json                                     122573B
- `22:47:32`     ✅ repo-data.json                                     16418B
- `22:47:32`     ❌ stock-picks-data.json                              98291B
- `22:47:32`     ❌ valuations-data.json                                2188B
- `22:47:32` Done
