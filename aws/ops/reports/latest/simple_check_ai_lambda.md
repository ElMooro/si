# Quick state check on both Lambdas

**Status:** success  
**Duration:** 11.9s  
**Finished:** 2026-04-25T23:39:54+00:00  

## Data

| ai_company | ai_elapsed | ai_status | has_bear | has_bull | has_description | has_scenarios |
|---|---|---|---|---|---|---|
| Apple Inc. | 11.4 | 200 | True | True | True | True |

## Log
## A. AI Lambda configuration

- `23:39:42`   CodeSha256:    yEBOt1/Nc3q8iv+DolbUJ8kP...
- `23:39:42`   LastModified:  2026-04-25T23:34:39
- `23:39:42`   CodeSize:      5927 bytes
- `23:39:42`   Timeout:       90s
- `23:39:42`   ReservedConc:  default
## B. AI Lambda smoke test — ticker=AAPL

- `23:39:54` ✅   ✅ HTTP 200 in 11.4s
- `23:39:54` 
- `23:39:54`   Company: Apple Inc. (Technology)
- `23:39:54`   Price:   $271.06  P/E=33.9
- `23:39:54`   Cached:  False  Model: claude-haiku-4-5-20251001
- `23:39:54`   Lambda runtime: 11.22s
- `23:39:54` 
- `23:39:54`   AI Description: Apple designs, manufactures, and sells iPhones, Macs, iPads, wearables, and services globally. Revenue is driven by hardware sales (iPhone ~52% of revenue) and recurring services (App Store, AppleCare...
- `23:39:54` 
- `23:39:54`   Bull thesis:  Services acceleration and installed-base monetization justify premium valuation despite hardware maturity.
- `23:39:54`   Bear thesis:  Valuation premium (33.9x P/E, 9.14x P/S) leaves no margin for error; iPhone commodity risk and China exposure pose secular headwinds.
- `23:39:54` 
- `23:39:54`   horizon_1m    : bull=$285  base=$275  bear=$255
- `23:39:54`   horizon_1q    : bull=$330  base=$305  bear=$270
- `23:39:54`   horizon_1y    : bull=$390  base=$340  bear=$280
## C. Screener Lambda configuration (just state, no invoke)

- `23:39:54`   CodeSha256:    YKGCA/bIqQvYjY7uFWW4Tz5O...
- `23:39:54`   LastModified:  2026-04-25T23:34:47
- `23:39:54`   CodeSize:      4827 bytes
- `23:39:54`   Timeout:       900s
- `23:39:54` Done
