# Path C — deep triage of remaining hidden pages

**Status:** success  
**Duration:** 7.6s  
**Finished:** 2026-04-26T00:59:51+00:00  

## Log
## A. 28 hidden pages to triage

- `00:59:47`   Already linked: 21 pages
- `00:59:47`   Hidden:         28 pages
## B. Per-page triage

## C. Verdicts

- `00:59:51` 
  ── BROKEN (4 pages):
- `00:59:51`     downloads.html                        23087B  HTTP S3 website (mixed content)
- `00:59:51`     exponential-search-dashboard.html     56587B  dead OpenBB APIGW
- `00:59:51`     macroeconomic-platform.html          118991B  dead OpenBB APIGW
- `00:59:51`       ← flow-data.json
- `00:59:51`     stocks/index.html                     26099B  HTTP S3 website (mixed content)
- `00:59:51`       ← stock-picks-data.json
- `00:59:51` 
  ── ARCHIVE (6 pages):
- `00:59:51`     Reports.html                            252B  stub <500B
- `00:59:51`     euro/index.html                       65451B  missing/stale data: ['report.json']
- `00:59:51`       ← data.json
- `00:59:51`       ← report.json
- `00:59:51`     ml.html                                 288B  stub <500B
- `00:59:51`     pro.html                              58313B  missing/stale data: [('pro-data.json', '1411h')]
- `00:59:51`       ← pro-data.json
- `00:59:51`     repo.html                               451B  stub <500B
- `00:59:51`     stocks.html                             249B  stub <500B
- `00:59:51` 
  ── QUIET (17 pages):
- `00:59:51`     agent/index.html                      17322B  no S3 deps; check externally
- `00:59:51`     ai-predictions-supabase.html          38120B  no S3 deps; check externally
- `00:59:51`     ai_predictions.html                   22799B  no S3 deps; check externally
- `00:59:51`     benzinga.html                          5609B  no S3 deps; check externally
- `00:59:51`     bls.html                              49832B  no S3 deps; check externally
- `00:59:51`     bot/index.html                        27832B  no S3 deps; check externally
- `00:59:51`     census.html                           33487B  no S3 deps; check externally
- `00:59:51`     ecb.html                             117234B  no S3 deps; check externally
- `00:59:51`     eia.html                               5414B  no S3 deps; check externally
- `00:59:51`     fmp.html                               5880B  no S3 deps; check externally
- `00:59:51`     fred.html                             27454B  no S3 deps; check externally
- `00:59:51`     nasdaq-datalink.html                   4352B  no S3 deps; check externally
- `00:59:51`     ny-fed.html                           65947B  no S3 deps; check externally
- `00:59:51`     ofr.html                              61702B  no S3 deps; check externally
- `00:59:51`     openbb-realtime-dashboard.html        61769B  no S3 deps; check externally
- `00:59:51`     trading-signals.html                  10925B  week-fresh (56h)
- `00:59:51`       ← predictions.json
- `00:59:51`     treasury-auctions.html                85210B  no S3 deps; check externally
- `00:59:51` 
  ── PROMOTE (1 pages):
- `00:59:51`     khalid/index.html                     47707B  fresh data (14.0h)
- `00:59:51`       ← data/khalid-metrics.json
- `00:59:51`       ← data/khalid-config.json
- `00:59:51`       ← data/khalid-analysis.json
## D. Recommendations

- `00:59:51` 
  ARCHIVE (delete or move to /archive/): 6 pages
- `00:59:51`   BROKEN  (need fixes before any use):    4 pages
- `00:59:51`   QUIET   (link from related pages, not main launcher): 17 pages
- `00:59:51`   PROMOTE (add to launcher tier 3):       1 pages
- `00:59:51` Done
