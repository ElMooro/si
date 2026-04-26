# Probe all 14 gap Lambdas — alive or zombie?

**Status:** success  
**Duration:** 11.8s  
**Finished:** 2026-04-26T11:28:49+00:00  

## Log
## A. Build S3 key index

- `11:28:41`   19493 S3 keys total
## 🔍 justhodl-daily-macro-report

- `11:28:41`   config: runtime=python3.11 mod=2026-04-25 state=Active
- `11:28:41`   invocations_2d: 0  errors_2d: 0
- `11:28:41`   S3 outputs found:
- `11:28:41`     🟢 portfolio/pnl-daily.json                                1182B    13.5h ago
- `11:28:41`   ▸ ZOMBIE: 0 invocations in 2 days
## 🔍 justhodl-financial-secretary

- `11:28:41`   config: runtime=python3.12 mod=2026-04-25 state=Active
- `11:28:41`   invocations_2d: 12  errors_2d: 0
- `11:28:41`   ⚠ no S3 outputs found
- `11:28:41`   ▸ ALIVE-NO-OUTPUT: running but no fresh S3 output
## 🔍 justhodl-news-sentiment

- `11:28:42`   config: runtime=python3.11 mod=2026-04-25 state=Active
- `11:28:42`   invocations_2d: 2  errors_2d: 0
- `11:28:42`   ⚠ no S3 outputs found
- `11:28:42`   ▸ ALIVE-NO-OUTPUT: running but no fresh S3 output
## 🔍 justhodl-repo-monitor

- `11:28:42`   config: runtime=python3.12 mod=2026-04-25 state=Active
- `11:28:42`   invocations_2d: 24  errors_2d: 0
- `11:28:42`   S3 outputs found:
- `11:28:42`     🟢 repo-data.json                                         16418B    23.5h ago
- `11:28:42`     🔴 archive/repo/2026/02/23/1200.json                       2478B  1487.5h ago
- `11:28:42`     🔴 archive/repo/2026/02/23/0713.json                       8123B  1492.3h ago
- `11:28:42`     🔴 archive/repo/2026/02/23/0701.json                       1239B  1492.5h ago
- `11:28:42`     🔴 archive/repo/2026/02/23/0655.json                       1239B  1492.5h ago
- `11:28:42`   ▸ ALIVE-PRODUCES-DATA: fresh S3 output found
## 🔍 macro-financial-intelligence

- `11:28:42`   config: runtime=python3.11 mod=2026-04-25 state=Active
- `11:28:43`   invocations_2d: 13  errors_2d: 0
- `11:28:43`   S3 outputs found:
- `11:28:43`     🔴 macroeconomic-platform.html                           119365B  1443.2h ago
- `11:28:43`   ▸ ALIVE-NO-OUTPUT: running but no fresh S3 output
## 🔍 volatility-monitor-agent

- `11:28:43`   config: runtime=python3.9 mod=2026-04-25 state=Active
- `11:28:44`   invocations_2d: 11  errors_2d: 0
- `11:28:44`   ⚠ no S3 outputs found
- `11:28:44`   ▸ ALIVE-NO-OUTPUT: running but no fresh S3 output
## 🔍 dollar-strength-agent

- `11:28:44`   config: runtime=python3.9 mod=2026-04-25 state=Active
- `11:28:45`   invocations_2d: 11  errors_2d: 0
- `11:28:45`   ⚠ no S3 outputs found
- `11:28:45`   ▸ ALIVE-NO-OUTPUT: running but no fresh S3 output
## 🔍 fmp-stock-picks-agent

- `11:28:45`   config: runtime=python3.12 mod=2026-04-25 state=Active
- `11:28:45`   invocations_2d: 21  errors_2d: 20
- `11:28:45`   S3 outputs found:
- `11:28:45`     🔴 fmp.html                                                5907B  1371.8h ago
- `11:28:45`   ▸ ALIVE-NO-OUTPUT: running but no fresh S3 output
## 🔍 bond-indices-agent

- `11:28:45`   config: runtime=python3.9 mod=2026-04-25 state=Active
- `11:28:45`   invocations_2d: 32  errors_2d: 0
- `11:28:45`   ⚠ no S3 outputs found
- `11:28:45`   ▸ ALIVE-NO-OUTPUT: running but no fresh S3 output
## 🔍 bea-economic-agent

- `11:28:46`   config: runtime=python3.9 mod=2026-04-25 state=Active
- `11:28:46`   invocations_2d: 11  errors_2d: 0
- `11:28:46`   ⚠ no S3 outputs found
- `11:28:46`   ▸ ALIVE-NO-OUTPUT: running but no fresh S3 output
## 🔍 manufacturing-global-agent

- `11:28:46`   config: runtime=python3.9 mod=2026-04-25 state=Active
- `11:28:47`   invocations_2d: 11  errors_2d: 0
- `11:28:47`   ⚠ no S3 outputs found
- `11:28:47`   ▸ ALIVE-NO-OUTPUT: running but no fresh S3 output
## 🔍 securities-banking-agent

- `11:28:47`   config: runtime=python3.9 mod=2026-04-25 state=Active
- `11:28:48`   invocations_2d: 11  errors_2d: 0
- `11:28:48`   ⚠ no S3 outputs found
- `11:28:48`   ▸ ALIVE-NO-OUTPUT: running but no fresh S3 output
## 🔍 google-trends-agent

- `11:28:48`   config: runtime=python3.11 mod=2026-04-25 state=Active
- `11:28:48`   invocations_2d: 11  errors_2d: 0
- `11:28:48`   ⚠ no S3 outputs found
- `11:28:48`   ▸ ALIVE-NO-OUTPUT: running but no fresh S3 output
## 🔍 news-sentiment-agent

- `11:28:48`   config: runtime=python3.9 mod=2026-04-25 state=Active
- `11:28:49`   invocations_2d: 11  errors_2d: 11
- `11:28:49`   ⚠ no S3 outputs found
- `11:28:49`   ▸ BROKEN: 11 errors in 2d, no S3 output
## FINAL VERDICTS

- `11:28:49` 
  ALIVE-PRODUCES-DATA (1):
- `11:28:49`     justhodl-repo-monitor                      inv2d=24 err2d=0
- `11:28:49`       └ repo-data.json (23.5h)
- `11:28:49`       └ archive/repo/2026/02/23/0655.json (1492.5h)
- `11:28:49` 
  ALIVE-NO-OUTPUT (11):
- `11:28:49`     bea-economic-agent                         inv2d=11 err2d=0
- `11:28:49`     bond-indices-agent                         inv2d=32 err2d=0
- `11:28:49`     dollar-strength-agent                      inv2d=11 err2d=0
- `11:28:49`     fmp-stock-picks-agent                      inv2d=21 err2d=20
- `11:28:49`       └ fmp.html (1371.8h)
- `11:28:49`     google-trends-agent                        inv2d=11 err2d=0
- `11:28:49`     justhodl-financial-secretary               inv2d=12 err2d=0
- `11:28:49`     justhodl-news-sentiment                    inv2d=2 err2d=0
- `11:28:49`     macro-financial-intelligence               inv2d=13 err2d=0
- `11:28:49`       └ macroeconomic-platform.html (1443.2h)
- `11:28:49`     manufacturing-global-agent                 inv2d=11 err2d=0
- `11:28:49`     securities-banking-agent                   inv2d=11 err2d=0
- `11:28:49`     volatility-monitor-agent                   inv2d=11 err2d=0
- `11:28:49` 
  BROKEN (1):
- `11:28:49`     news-sentiment-agent                       inv2d=11 err2d=11
- `11:28:49` 
  ZOMBIE (1):
- `11:28:49`     justhodl-daily-macro-report                inv2d=0 err2d=0
- `11:28:49`       └ portfolio/pnl-daily.json (13.5h)
- `11:28:49` Done
