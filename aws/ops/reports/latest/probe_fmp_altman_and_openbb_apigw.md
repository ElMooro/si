# Find Altman endpoint + check OpenBB API Gateway

**Status:** success  
**Duration:** 7.9s  
**Finished:** 2026-04-26T00:03:02+00:00  

## Log
## Setup probe Lambda

- `00:02:58` ✅   probe Lambda ready
## A. FMP /stable/ candidates for Altman Z

- `00:02:59`   financial-scores                    ✅ (11 keys)
- `00:02:59` ✅      🎯 Altman keys: ['altmanZScore']
- `00:02:59`         altmanZScore = 10.557061601901797
- `00:02:59`      All keys: ['altmanZScore', 'ebit', 'marketCap', 'piotroskiScore', 'reportedCurrency', 'retainedEarnings', 'revenue', 'symbol', 'totalAssets', 'totalLiabilities', 'workingCapital']
- `00:02:59`   financial-strength                  ✗ HTTPError
- `00:02:59`   company-rating                      ✗ HTTPError
- `00:03:00`   altman-zscore                       ✗ HTTPError
- `00:03:00`   altman-z                            ✗ HTTPError
- `00:03:00`   piotroski-score                     ✗ HTTPError
- `00:03:00`   balance-sheet-statement             ✅ (61 keys)
- `00:03:00`   income-statement                    ✅ (39 keys)
- `00:03:00` ✅ 
  ✅ ALTMAN WINNER: endpoint=financial-scores, field=altmanZScore
## B. OpenBB API Gateway state (powers landing page N/A cards)

- `00:03:00`   API Gateway ID: i70jxru6md
- `00:03:01` ⚠   error: An error occurred (AccessDeniedException) when calling the GetRestApi operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: apigateway:GET on resource: arn:aws:apigateway:us-east-1::/restapis/i70jxru6md because no identity-based policy allows the apigateway:GET action
## C. Available S3 data files for rebuilding landing page

- `00:03:01`   Root JSONs available (14):
- `00:03:01`     crypto-data.json                        40110B  2026-02-28 03:21
- `00:03:01`     crypto-intel.json                       56593B  2026-04-25 23:54
- `00:03:01`     data-peek.json                          60635B  2026-02-23 08:09
- `00:03:01`     data.json                               60635B  2026-02-18 13:00
- `00:03:01`     edge-data.json                           1888B  2026-04-25 22:04
- `00:03:01`     flow-data.json                          31440B  2026-04-26 00:02
- `00:03:02`     intelligence-report.json                 4369B  2026-04-25 12:10
- `00:03:02`     liquidity-data.json                     10475B  2026-04-25 12:30
- `00:03:02`     manifest.json                             264B  2026-02-18 04:33
- `00:03:02`     predictions.json                        14351B  2026-04-23 16:55
- `00:03:02`     pro-data.json                          122573B  2026-02-26 06:24
- `00:03:02`     repo-data.json                          16418B  2026-04-25 12:00
- `00:03:02`     stock-picks-data.json                   98291B  2026-03-02 09:05
- `00:03:02`     valuations-data.json                     2188B  2026-04-01 14:00
## Cleanup

- `00:03:02` Done
