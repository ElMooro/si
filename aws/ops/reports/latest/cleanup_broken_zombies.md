# Cleanup: disable broken Lambdas + archive dead pages

**Status:** success  
**Duration:** 9.6s  
**Finished:** 2026-04-26T11:59:36+00:00  

## Log
## A. EventBridge rule lookup

- `11:59:35`   Found 4 rules to disable:
- `11:59:35`     DailyMacroReportRule                                    → justhodl-daily-macro-report    (cron(0 12 * * ? *))
- `11:59:35`     fmp-movers-hourly                                       → fmp-stock-picks-agent          (cron(0 14,16,18,20 ? * MON-FRI *))
- `11:59:35`     fmp-stock-picks-daily                                   → fmp-stock-picks-agent          (cron(0 12 ? * MON-FRI *))
- `11:59:35`     fmp-stock-picks-daily                                   → fmp-stock-picks-agent          (cron(0 12 ? * MON-FRI *))
## B. Disabling rules

- `11:59:35` ✅   disabled: DailyMacroReportRule
- `11:59:36` ✅   disabled: fmp-movers-hourly
- `11:59:36` ✅   disabled: fmp-stock-picks-daily
- `11:59:36` ✅   disabled: fmp-stock-picks-daily
## C. Archive dead pages

- `11:59:36` ✅   archived: pro.html → archive/pro.html
- `11:59:36` ✅   archived: exponential-search-dashboard.html → archive/exponential-search-dashboard.html
- `11:59:36` ✅   archived: macroeconomic-platform.html → archive/macroeconomic-platform.html
## D. Remove stubs

- `11:59:36` ✅   removed: Reports.html (256B)
- `11:59:36` ✅   removed: ml.html (288B)
- `11:59:36` ✅   removed: stocks.html (249B)
## E. Archive README

- `11:59:36` ✅   wrote /home/runner/work/si/si/archive/README.md
- `11:59:36` Done
