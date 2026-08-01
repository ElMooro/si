# ops 4255 v2 — silent-writer forensics (35 pairs)

**Status:** success  
**Duration:** 198.2s  
**Finished:** 2026-08-01T21:55:48+00:00  

## Data

| age_h | artifact | class | cls | count | deployed_writes | fresh_siblings | log_lines | moved_on_probe | probe_error | repo_writes | sched_inputs | section | writer |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 650.4 | data/_freshness-manifest.json | ASYNC-NO-WRITE |  |  | True | [] | [] | False | None | True |  | engine | justhodl-fleet-freshness-monitor |
| 0.6 | data/alert-history.json | WRITES-ON-PROBE |  |  | True | ["data/_fleet-monitor-alert-history.json", "data/_freshness-alert-history.json"] | [] | True | None | True | [] | engine | justhodl-alert-router |
| 0.6 | data/bis-crossborder.json | FIXED-DEPLOY-DRIFT |  |  | False | [] | [] | True | None | True |  | engine | justhodl-bis-crossborder |
| 789.5 | data/brain-history.json | ASYNC-NO-WRITE |  |  | True | [] | [] | False | None | True |  | engine | justhodl-brain-sync |
| 0.6 | data/buyback-scanner.json | WRITES-ON-PROBE |  |  | True | [] | [] | True | None | True | [] | engine | justhodl-buyback-scanner |
| 28.0 | data/compound-signals.json | SUPERSEDED? |  |  | False | ["data/compound-signals-state.json"] | [] | False | None | True |  | engine | justhodl-alpha-research |
| 1492.7 | data/congress-party-map.json | ASYNC-NO-WRITE |  |  | True | [] | [] | False | None | True |  | engine | justhodl-political-stocks |
| 0.5 | data/etf-census-matrix.json | SCHEDULE-INPUT |  |  | True | [] | [] | True | None | True | ["{}"] | engine | justhodl-etf-census |
| 0.5 | data/etf-census.json | SCHEDULE-INPUT |  |  | True | ["data/etf-census-matrix.json"] | ["[etf-census] flow records=60 vocab=['aum_b', 'avg_20d_dollar_vol_b', 'avg_5d_dollar_vol_b', 'avg_60d_dollar_vol_b', 'category', 'dvol_5d_v | True | None | True | ["{}"] | engine | justhodl-etf-census |
| 0.6 | data/eurodollar-stress.json | ASYNC-NO-WRITE |  |  | True | [] | [] | False | None | True |  | engine | justhodl-dollar-radar |
| 128.3 | data/factor-data-cache.json | ASYNC-NO-WRITE |  |  | True | [] | [] | False | None | True |  | engine | justhodl-factor-decomposition |
| 0.4 | data/factor-decomposition.json | SCHEDULE-INPUT |  |  | True | [] | ["[factor-decomposition] start v1.0.0", "[factor-decomposition] complete: n_ok=47 pos_alpha=2 neg_alpha=5"] | True | None | True | ["{}"] | engine | justhodl-factor-decomposition |
| 2145.1 | data/feedback-summary.json | CODE-PATH-SILENT |  |  | True | [] | [] | False | None | True |  | engine | justhodl-feedback |
| 0.4 | data/fi-census-matrix.json | FIXED-DEPLOY-DRIFT |  |  | False | [] | [] | True | None | True |  | engine | justhodl-fi-census |
| 0.4 | data/fi-census.json | FIXED-DEPLOY-DRIFT |  |  | False | ["data/fi-census-matrix.json"] | ["[fi-census] 0/45 BIL", "[fi-census] 12/45 GOVT"] | True | None | True |  | engine | justhodl-fi-census |
| 0.3 | data/forward-returns.json | WRITES-ON-PROBE |  |  | True | [] | ["[forward-returns] v1.0.0 starting", "[forward-returns] fetching market data ..."] | True | None | True | [] | engine | justhodl-forward-returns |
| 1304.9 | data/history-index.json | ASYNC-NO-WRITE |  |  | True | [] | [] | False | None | True |  | engine | justhodl-calibration-snapshotter |
| 0.3 | data/index-inclusion.json | FIXED-DEPLOY-DRIFT |  |  | False | [] | [] | True | None | True |  | engine | justhodl-index-inclusion |
| 0.3 | data/inventory-drawdown.json | FIXED-DEPLOY-DRIFT |  |  | False | [] | ["[inventory-drawdown] sectors_drawing=6 names_with_inv=87 boom_setups=7 4.4s"] | True | None | True |  | engine | justhodl-inventory-drawdown |
| 418.7 | data/ka-analysis.json | ASYNC-NO-WRITE |  |  | True | [] | [] | False | None | True |  | engine | justhodl-ka-metrics |
| 418.7 | data/khalid-analysis.json | ASYNC-NO-WRITE |  |  | True | [] | [] | False | None | True |  | engine | justhodl-ka-metrics |
| 23.9 | data/massive-signals.json | ASYNC-NO-WRITE |  |  | True | [] | [] | False | None | True |  | engine | justhodl-alpha-score |
| 0.2 | data/miss-calibrator-proposals.json | WRITES-ON-PROBE |  |  | True | [] | [] | True | None | True | [] | engine | justhodl-miss-calibrator |
| 23.9 | data/options-flow.json | SUPERSEDED? |  |  | True | ["data/crypto-options-history.json", "data/crypto-options-surface-history.json", "data/crypto-options-surface.json"] | ["inputs: 503 stocks \u00b7 503 sentiment \u00b7 1862 smart-money \u00b7 8 options-flow"] | False | None | True |  | engine | justhodl-alpha-score |
| 0.2 | data/playbook-rules.json | FIXED-DEPLOY-DRIFT |  |  | False | [] | [] | True | None | True |  | engine | justhodl-playbook-engine |
| 104.1 | data/polygon-related-graph.json | ASYNC-NO-WRITE |  |  | True | [] | [] | False | None | True |  | engine | justhodl-supply-chain-graph |
| 895.9 | data/quiver-congress-cache.json | ASYNC-NO-WRITE |  |  | True | [] | [] | False | None | True |  | engine | justhodl-political-stocks |
| 1229.9 | data/quiver-lobbying-cache.json | ASYNC-NO-WRITE |  |  | True | [] | [] | False | None | True |  | engine | justhodl-lobbying-intel |
| 0.6 | data/risk-regime.json | SUPERSEDED? |  |  | True | ["data/risk-regime-state.json"] | [] | False | None | True |  | engine | justhodl-carry-surface |
| 0.7 | data/sec-filings-intel.json | ASYNC-NO-WRITE |  |  | True | [] | [] | False | None | True |  | engine | justhodl-pump-mechanics |
| 135.9 | data/signal-halflife.json | ASYNC-NO-WRITE |  |  | True | [] | [] | False | None | True |  | engine | justhodl-meta-improver |
| 130.9 | data/smart-money-13f.json | WRITES-ON-PROBE |  |  | True | ["data/smart-money-clusters.json", "data/smart-money-names.json"] | [] | True | None | True | [] | engine | justhodl-smart-money-13f |
| 158.6 | data/symbol-dictionary.json | FIXED-DEPLOY-DRIFT |  |  | False | [] | [] | True | None | True |  | engine | justhodl-symbol-dictionary |
| 459.6 | data/symbol-map.json | SUPERSEDED? |  |  | False | ["data/symbol-aliases.json", "data/symbol-feed.json"] | [] | False | None | True |  | engine | justhodl-symbol-dictionary |
| 128.7 | data/whales.json | WRITES-ON-PROBE |  |  | True | [] | [] | True | None | True | [] | engine | justhodl-whales |
|  |  |  | ASYNC-NO-WRITE | 14 |  |  |  |  |  |  |  | matrix |  |
|  |  |  | FIXED-DEPLOY-DRIFT | 7 |  |  |  |  |  |  |  | matrix |  |
|  |  |  | WRITES-ON-PROBE | 6 |  |  |  |  |  |  |  | matrix |  |
|  |  |  | SUPERSEDED? | 4 |  |  |  |  |  |  |  | matrix |  |
|  |  |  | SCHEDULE-INPUT | 3 |  |  |  |  |  |  |  | matrix |  |
|  |  |  | CODE-PATH-SILENT | 1 |  |  |  |  |  |  |  | matrix |  |

## Log
- `21:52:30` resume cursor: 0 pair(s) already evidenced today
## Phase 1 — static evidence + drift repair

- `21:52:33` ⚠ DEPLOY-DRIFT bis-crossborder.json <- justhodl-bis-crossborder — redeploying from repo
- `21:52:36` ⚠ DEPLOY-DRIFT compound-signals.json <- justhodl-alpha-research — redeploying from repo
- `21:52:41` ⚠ DEPLOY-DRIFT fi-census-matrix.json <- justhodl-fi-census — redeploying from repo
- `21:52:42` ⚠ DEPLOY-DRIFT fi-census.json <- justhodl-fi-census — redeploying from repo
- `21:52:48` ⚠ DEPLOY-DRIFT index-inclusion.json <- justhodl-index-inclusion — redeploying from repo
- `21:52:50` ⚠ DEPLOY-DRIFT inventory-drawdown.json <- justhodl-inventory-drawdown — redeploying from repo
- `21:52:53` ⚠ DEPLOY-DRIFT playbook-rules.json <- justhodl-playbook-engine — redeploying from repo
- `21:52:59` ⚠ DEPLOY-DRIFT symbol-dictionary.json <- justhodl-symbol-dictionary — redeploying from repo
- `21:53:00` ⚠ DEPLOY-DRIFT symbol-map.json <- justhodl-symbol-dictionary — redeploying from repo
## Phase 2 — probe volley

- `21:53:14` async volley fired: 27 | fast RequestResponse queue: 1
- `21:53:15` single settle wait 150s for the async volley…
## Classification

- `21:55:45` ✗ _freshness-manifest.json               justhodl-fleet-freshness-mon ASYNC-NO-WRITE     
- `21:55:45` ✅ alert-history.json                     justhodl-alert-router        WRITES-ON-PROBE    data/_fleet-monitor-alert-history.json;data/_freshness-alert
- `21:55:45` ✅ bis-crossborder.json                   justhodl-bis-crossborder     FIXED-DEPLOY-DRIFT 
- `21:55:46` ✗ brain-history.json                     justhodl-brain-sync          ASYNC-NO-WRITE     
- `21:55:46` ✅ buyback-scanner.json                   justhodl-buyback-scanner     WRITES-ON-PROBE    
- `21:55:46` ⚠ compound-signals.json                  justhodl-alpha-research      SUPERSEDED?        data/compound-signals-state.json
- `21:55:46` ✗ congress-party-map.json                justhodl-political-stocks    ASYNC-NO-WRITE     
- `21:55:46` ✗ etf-census-matrix.json                 justhodl-etf-census          SCHEDULE-INPUT     
- `21:55:46` ✗ etf-census.json                        justhodl-etf-census          SCHEDULE-INPUT     [etf-census] flow records=60 vocab=['aum_b', 'avg_20d_dollar_vol_b', '
- `21:55:46` ✗ eurodollar-stress.json                 justhodl-dollar-radar        ASYNC-NO-WRITE     
- `21:55:46` ✗ factor-data-cache.json                 justhodl-factor-decompositio ASYNC-NO-WRITE     
- `21:55:46` ✗ factor-decomposition.json              justhodl-factor-decompositio SCHEDULE-INPUT     [factor-decomposition] start v1.0.0;[factor-decomposition] complete: n
- `21:55:46` ✗ feedback-summary.json                  justhodl-feedback            CODE-PATH-SILENT   
- `21:55:46` ✅ fi-census-matrix.json                  justhodl-fi-census           FIXED-DEPLOY-DRIFT 
- `21:55:46` ✅ fi-census.json                         justhodl-fi-census           FIXED-DEPLOY-DRIFT [fi-census] 0/45 BIL;[fi-census] 12/45 GOVT
- `21:55:47` ✅ forward-returns.json                   justhodl-forward-returns     WRITES-ON-PROBE    [forward-returns] v1.0.0 starting;[forward-returns] fetching market da
- `21:55:47` ✗ history-index.json                     justhodl-calibration-snapsho ASYNC-NO-WRITE     
- `21:55:47` ✅ index-inclusion.json                   justhodl-index-inclusion     FIXED-DEPLOY-DRIFT 
- `21:55:47` ✅ inventory-drawdown.json                justhodl-inventory-drawdown  FIXED-DEPLOY-DRIFT [inventory-drawdown] sectors_drawing=6 names_with_inv=87 boom_setups=7
- `21:55:47` ✗ ka-analysis.json                       justhodl-ka-metrics          ASYNC-NO-WRITE     
- `21:55:47` ✗ khalid-analysis.json                   justhodl-ka-metrics          ASYNC-NO-WRITE     
- `21:55:47` ✗ massive-signals.json                   justhodl-alpha-score         ASYNC-NO-WRITE     
- `21:55:47` ✅ miss-calibrator-proposals.json         justhodl-miss-calibrator     WRITES-ON-PROBE    
- `21:55:47` ⚠ options-flow.json                      justhodl-alpha-score         SUPERSEDED?        inputs: 503 stocks · 503 sentiment · 1862 smart-money · 8 options-flow
- `21:55:47` ✅ playbook-rules.json                    justhodl-playbook-engine     FIXED-DEPLOY-DRIFT 
- `21:55:47` ✗ polygon-related-graph.json             justhodl-supply-chain-graph  ASYNC-NO-WRITE     
- `21:55:48` ✗ quiver-congress-cache.json             justhodl-political-stocks    ASYNC-NO-WRITE     
- `21:55:48` ✗ quiver-lobbying-cache.json             justhodl-lobbying-intel      ASYNC-NO-WRITE     
- `21:55:48` ⚠ risk-regime.json                       justhodl-carry-surface       SUPERSEDED?        data/risk-regime-state.json
- `21:55:48` ✗ sec-filings-intel.json                 justhodl-pump-mechanics      ASYNC-NO-WRITE     
- `21:55:48` ✗ signal-halflife.json                   justhodl-meta-improver       ASYNC-NO-WRITE     
- `21:55:48` ✅ smart-money-13f.json                   justhodl-smart-money-13f     WRITES-ON-PROBE    data/smart-money-clusters.json;data/smart-money-names.json
- `21:55:48` ✅ symbol-dictionary.json                 justhodl-symbol-dictionary   FIXED-DEPLOY-DRIFT 
- `21:55:48` ⚠ symbol-map.json                        justhodl-symbol-dictionary   SUPERSEDED?        data/symbol-aliases.json;data/symbol-feed.json
- `21:55:48` ✅ whales.json                            justhodl-whales              WRITES-ON-PROBE    
## MATRIX

- `21:55:48`   ASYNC-NO-WRITE         14
- `21:55:48`   FIXED-DEPLOY-DRIFT     7
- `21:55:48`   WRITES-ON-PROBE        6
- `21:55:48`   SUPERSEDED?            4
- `21:55:48`   SCHEDULE-INPUT         3
- `21:55:48`   CODE-PATH-SILENT       1
- `21:55:48` auto-repaired deploy-drift, artifact verified moving: 7
## RESULT

- `21:55:48` ✅ OPS 4255 v2 PASS — matrix complete, cursor-resumable
