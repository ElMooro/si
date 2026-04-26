# Final cleanup verification

**Status:** success  
**Duration:** 36.1s  
**Finished:** 2026-04-26T12:03:31+00:00  

## Log
## 🔎 news-sentiment-agent

- `12:03:05`   EventBridge rules (1):
- `12:03:05`     ⚪ news-sentiment-update                              DISABLED   rate(30 minutes)
- `12:03:06`   Event source mappings (0):
- `12:03:06`   Function policy statements (1):
- `12:03:06`     sid=FunctionURLAllowPublicAccess             principal=*
## 🔎 fmp-stock-picks-agent

- `12:03:15`   EventBridge rules (3):
- `12:03:15`     ⚪ fmp-movers-hourly                                  DISABLED   cron(0 14,16,18,20 ? * MON-FRI *)
- `12:03:15`     ⚪ fmp-stock-picks-daily                              DISABLED   cron(0 12 ? * MON-FRI *)
- `12:03:15`     ⚪ fmp-stock-picks-daily                              DISABLED   cron(0 12 ? * MON-FRI *)
- `12:03:15`   Event source mappings (0):
- `12:03:16`   Function policy statements (3):
- `12:03:16`     sid=fmp-daily                                principal=events.amazonaws.com
- `12:03:16`     sid=fmp-movers                               principal=events.amazonaws.com
- `12:03:16`     sid=fmp-daily-trigger                        principal=events.amazonaws.com
## 🔎 justhodl-daily-macro-report

- `12:03:25`   EventBridge rules (1):
- `12:03:25`     ⚪ DailyMacroReportRule                               DISABLED   cron(0 12 * * ? *)
- `12:03:25`   Event source mappings (0):
- `12:03:25`   Function policy: none
## D. GitHub Pages — confirm cleanup is live

- `12:03:29`   🟡 stub-removed       Reports.html                                            HTTP 200 256B
- `12:03:30`   🟡 stub-removed       ml.html                                                 HTTP 200 288B
- `12:03:30`   🟡 stub-removed       stocks.html                                             HTTP 200 249B
- `12:03:30`   🔴 archived           archive/pro.html                                        HTTP 404 
- `12:03:30`   🔴 archived           archive/exponential-search-dashboard.html               HTTP 404 
- `12:03:30`   🔴 archived           archive/macroeconomic-platform.html                     HTTP 404 
- `12:03:31`   🔴 archive-readme     archive/README.md                                       HTTP 404 
- `12:03:31`   ✅ real-now           repo.html                                               HTTP 200 22264B
- `12:03:31`   ✅ new                volatility.html                                         HTTP 200 17390B
- `12:03:31` Done
