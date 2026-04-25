# Reconcile feature audit findings against repo + S3 reality

**Status:** success  
**Duration:** 8.2s  
**Finished:** 2026-04-25T02:14:58+00:00  

## Data

| pages_in_repo | pages_in_s3_only | pages_truly_missing |
|---|---|---|
| 15 | 1 | 1 |

## Log
## 1. Check HTML pages: repo vs S3

- `02:14:50`   index.html                repo: ✓ index.html                                       s3: ✓ (55121B)
- `02:14:50`   pro.html                  repo: ✓ pro.html                                         s3: ✓ (58557B)
- `02:14:50`   agent.html                repo: ✓ agent                                            s3: ✗
- `02:14:50`   charts.html               repo: ✓ charts.html                                      s3: ✓ (245035B)
- `02:14:50`   valuations.html           repo: ✓ valuations.html                                  s3: ✓ (25062B)
- `02:14:50`   edge.html                 repo: ✓ edge.html                                        s3: ✗
- `02:14:50`   flow.html                 repo: ✓ flow.html                                        s3: ✓ (30349B)
- `02:14:50`   intelligence.html         repo: ✓ intelligence.html                                s3: ✓ (27710B)
- `02:14:50`   risk.html                 repo: ✓ risk.html                                        s3: ✗
- `02:14:50`   stocks.html               repo: ✓ stocks.html                                      s3: ✓ (26200B)
- `02:14:50`   ath.html                  repo: ✓ ath.html                                         s3: ✓ (15998B)
- `02:14:50`   trading-signals.html      repo: ✓ trading-signals.html                             s3: ✗
- `02:14:50`   reports.html              repo: ✗                                                  s3: ✗
- `02:14:50`   ml.html                   repo: ✓ ml.html                                          s3: ✗
- `02:14:50`   dex.html                  repo: ✓ dex.html                                         s3: ✓ (49207B)
- `02:14:50`   liquidity.html            repo: ✓ liquidity.html                                   s3: ✗
- `02:14:50`   health.html               repo: ✗                                                  s3: ✓ (9996B)
## 2. DEX scanner data location

- `02:14:50`   Source paths writing: ['dex.html']
- `02:14:50`     dex.html                                 49207B  age 1148.9h
- `02:14:50` 
  S3 search for dex* files:
- `02:14:51`     bot/index.html                                        28606B  age 1147.1h
- `02:14:54`     data/dex-scanner-data.json                           144314B  age 1148.8h
- `02:14:54`     dex.html                                              49207B  age 1148.9h
- `02:14:54`     index.html                                            55121B  age 1120.9h
- `02:14:54`     khalid/index.html                                     69260B  age 1363.2h
- `02:14:54`     secretary/index.html                                  17322B  age 1296.6h
- `02:14:54`     stock/index.html                                      46644B  age 1121.3h
## 3. ATH tracker data location

- `02:14:54`   daily-report-v3 source ATH writes: ['data/ath.json']
- `02:14:54` 
  S3 search for ath* files:
- `02:14:54`     ath.html                                              15998B  age 1387.3h
- `02:14:57`     data/ath.json                                         18658B  age 0.1h
- `02:14:58` 
  data/report.json contains ATH data: True
- `02:14:58`     ath_breakouts: ['breakouts', 'near_ath', 'total_at_ath', 'total_near_ath', 'ath_coverage']
## 4. Reconciled status — what's actually missing?

- `02:14:58`   Pages summary:
- `02:14:58`     In git repo (= served on justhodl.ai): 15
- `02:14:58`     In S3 only (not on justhodl.ai): 1
- `02:14:58`     Truly missing (neither): 1
- `02:14:58` 
  Genuinely missing pages:
- `02:14:58`     reports.html
- `02:14:58` ✅   Appended reconciliation to feature_audit_2026-04-25.md
- `02:14:58` Done
