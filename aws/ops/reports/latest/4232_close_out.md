# ops 4232 — wave 4: double-fire fix + OpenSearch teardown

**Status:** success  
**Duration:** 66.0s  
**Finished:** 2026-08-01T14:16:53+00:00  

## Data

| expr | function | removed | rule | section |
|---|---|---|---|---|
| cron(45 13 * * ? *) | justhodl-ai-infra-stack | 1 | ai-infra-stack-daily | double_fire |
| cron(15 14 * * ? *) | justhodl-ai-rerating-radar | 1 | ai-rerating-radar-daily | double_fire |
| cron(50 */3 * * ? *) | justhodl-alpha-compass | 1 | alpha-compass-3h | double_fire |
| cron(0 6 * * ? *) | justhodl-ark-holdings | 1 | ark-holdings-daily | double_fire |
| cron(45 14 * * ? *) | justhodl-attention-signals | 1 | attention-signals-daily | double_fire |
| cron(15 14 * * ? *) | bea-economic-agent | 1 | bea-economic-agent-daily | double_fire |
| cron(0 14 * * ? *) | bls-labor-agent | 1 | bls-labor-agent-daily | double_fire |
| cron(30 22 * * ? *) | justhodl-capital-flow-radar | 1 | capital-flow-radar-daily | double_fire |
| rate(4 hours) | justhodl-carry-surface | 1 | carry-surface-4h | double_fire |
| cron(30 14 * * ? *) | census-economic-agent | 1 | census-economic-agent-daily | double_fire |
| cron(15 * * * ? *) | justhodl-crisis-composite | 1 | crisis-composite-hourly | double_fire |
| cron(0 9 * * ? *) | justhodl-engine-signal-map | 1 | engine-signal-map-daily | double_fire |
| cron(30 12 * * ? *) | justhodl-engine-trust | 1 | engine-trust-daily | double_fire |
| cron(0 12 * * ? *) | justhodl-eurodollar-plumbing | 1 | eurodollar-plumbing-daily | double_fire |
| rate(6 hours) | justhodl-event-flow-monitor | 1 | event-flow-monitor-hourly | double_fire |
| cron(0 12 * * ? *) | justhodl-fast-filings | 1 | fast-filings-daily | double_fire |
| rate(6 hours) | justhodl-fed-nlp | 1 | fed-nlp-6h | double_fire |
| cron(0 12 * * ? *) | justhodl-finnhub-signals | 1 | finnhub-signals-daily | double_fire |
| cron(35 21 * * ? *) | justhodl-fomc-reaction | 1 | fomc-reaction-daily | double_fire |
| cron(0 3 ? * SUN *) | justhodl-forward-returns | 1 | forward-returns-weekly | double_fire |
| cron(45 12 * * ? *) | justhodl-gdelt-buzz | 1 | gdelt-buzz-daily | double_fire |
| cron(0 14 ? * MON-FRI *) | justhodl-global-liquidity | 1 | global-liquidity-daily | double_fire |
| cron(15 6,18 * * ? *) | justhodl-global-sovereign | 1 | global-sovereign-12h | double_fire |
| cron(30 12 ? * SUN *) | justhodl-hiring-velocity | 1 | hiring-velocity-weekly | double_fire |
| cron(0 9 * * ? *) | justhodl-hkma-monitor | 1 | hkma-monitor-daily | double_fire |
| cron(35 21 * * ? *) | justhodl-industry-rotation | 1 | industry-rotation-daily | double_fire |
| rate(6 hours) | justhodl-stress-index | 1 | jsi-6h | double_fire |
| cron(30 9 ? * SUN *) | justhodl-jsi-calibrator | 1 | jsi-calibrator-weekly | double_fire |
| cron(50 21 ? * MON-FRI *) | justhodl-accumulation-radar | 1 | justhodl-accumulation-radar-daily | double_fire |
| cron(10 15 * * ? *) | justhodl-attention-confluence | 1 | justhodl-attention-confluence-daily | double_fire |
| cron(0 9 1 * ? *) | justhodl-brain-sync | 1 | justhodl-brain-sync-monthly | double_fire |
| cron(0 22 ? * MON-FRI *) | justhodl-breadth-thrust | 1 | justhodl-breadth-thrust-daily | double_fire |
| cron(50 * * * ? *) | justhodl-canary-warroom | 1 | justhodl-canary-warroom-hourly | double_fire |
| cron(0 13,23 * * ? *) | justhodl-crypto-emergence | 1 | justhodl-crypto-emergence-daily | double_fire |
| cron(0 12,22 * * ? *) | justhodl-crypto-liquidity | 1 | justhodl-crypto-liquidity-daily | double_fire |
| cron(30 23 * * ? *) | justhodl-cycle-clock | 1 | justhodl-cycle-clock-daily | double_fire |
| cron(0 14 ? * WED *) | justhodl-dark-pool | 1 | justhodl-dark-pool-weekly | double_fire |
| cron(7 13-21 ? * MON-FRI *) | justhodl-dealer-gex | 1 | justhodl-dealer-gex-hourly | double_fire |
| cron(30 22 ? * MON-FRI *) | justhodl-earnings-confluence | 1 | justhodl-earnings-confluence-daily | double_fire |
| cron(0 11 ? * SUN *) | justhodl-edge-discovery | 1 | justhodl-edge-discovery-weekly | double_fire |
| cron(30 0 * * ? *) | justhodl-equity-confluence | 1 | justhodl-equity-confluence-daily | double_fire |
| cron(0 15 * * ? *) | justhodl-interpretation-grader | 1 | justhodl-interp-grader-daily | double_fire |
| cron(5 * * * ? *) | justhodl-llm-cost-dashboard | 1 | justhodl-llm-cost-dashboard-hourly | double_fire |
| cron(30 21 ? * MON-FRI *) | justhodl-ma200-reclaim | 1 | justhodl-ma200-reclaim-daily | double_fire |
| cron(20 12 * * ? *) | justhodl-macro-leads | 1 | justhodl-macro-leads-daily | double_fire |
| cron(30 13 * * ? *) | justhodl-nowcast-desk | 1 | justhodl-nowcast-desk-daily | double_fire |
| cron(0 13 ? * TUE-SAT *) | justhodl-options-analytics | 1 | justhodl-options-analytics-daily | double_fire |
| cron(29 21 * * ? *) | justhodl-outcome-checker | 1 | justhodl-outcome-checker-4h | double_fire |
| cron(30 21 * * ? *) | justhodl-paper-book | 1 | justhodl-paper-book-daily | double_fire |
| cron(0 4 * * ? *) | justhodl-peru-copper | 1 | justhodl-peru-copper-daily | double_fire |
| cron(20 13 * * ? *) | justhodl-refining-stress | 1 | justhodl-refining-stress-daily | double_fire |
| cron(0 12 * * ? *) | justhodl-regime-conditional-trust | 1 | justhodl-regime-cond-trust-daily | double_fire |
| cron(0 13,21 * * ? *) | justhodl-regime-map | 1 | justhodl-regime-map-daily | double_fire |
| cron(45 22 ? * MON-FRI *) | justhodl-resilience | 1 | justhodl-resilience-daily | double_fire |
| cron(0 22 * * ? *) | justhodl-sector-emergence | 1 | justhodl-sector-emergence-daily | double_fire |
| cron(20 4 * * ? *) | justhodl-singapore-nodx | 1 | justhodl-singapore-nodx-daily | double_fire |
| cron(30 13 ? * TUE-SAT *) | justhodl-squeeze-fuel | 1 | justhodl-squeeze-fuel-daily | double_fire |
| cron(0 14 ? * MON-FRI *) | justhodl-strategist | 1 | justhodl-strategist-daily | double_fire |
| cron(20 12 ? * SUN *) | justhodl-strategy-portfolio | 1 | justhodl-strategy-portfolio-weekly | double_fire |
| cron(45 13 ? * TUE-SAT *) | justhodl-supply-chain-graph | 1 | justhodl-supply-chain-graph-daily | double_fire |
| cron(0 13 ? * TUE-SAT *) | justhodl-tail-risk | 1 | justhodl-tail-risk-daily | double_fire |
| cron(30 3 * * ? *) | justhodl-taiwan-moea | 1 | justhodl-taiwan-moea-daily | double_fire |
| cron(30 13 ? * TUE-SAT *) | justhodl-treasury-noise | 1 | justhodl-treasury-noise-daily | double_fire |
| cron(0 13 * * ? *) | justhodl-leadlag-graph | 1 | leadlag-graph-daily | double_fire |
| cron(0 16 * * ? *) | justhodl-lobbying-intel | 1 | lobbying-intel-daily | double_fire |
| cron(30 7 * * ? *) | justhodl-magnitude-distributions | 1 | magnitude-distributions-daily | double_fire |
| cron(40 22 ? * TUE-SAT *) | justhodl-market-internals | 1 | market-internals-daily | double_fire |
| cron(0 22 * * ? *) | justhodl-massive-signals | 1 | massive-signals-daily | double_fire |
| cron(15 9 ? * MON *) | justhodl-miss-calibrator | 1 | miss-calibrator-weekly | double_fire |
| cron(0 1 * * ? *) | justhodl-miss-detector | 1 | miss-detector-nightly | double_fire |
| cron(30 23 * * ? *) | justhodl-money-flow-state | 1 | money-flow-state-daily | double_fire |
| cron(58 21 * * ? *) | justhodl-near-miss-monitor | 1 | near-miss-monitor-hourly | double_fire |
| cron(10 12 * * ? *) | justhodl-notes-intel | 1 | notes-intel-daily | double_fire |
| cron(0 17 * * ? *) | justhodl-patent-velocity | 1 | patent-velocity-daily | double_fire |
| cron(0 14 * * ? *) | justhodl-political-stocks | 1 | political-stocks-daily | double_fire |
| cron(50 15 * * ? *) | justhodl-rotation-chain | 1 | rotation-chain-2x-daily | double_fire |
| cron(7 21 * * ? *) | justhodl-sec-filings-intel | 1 | sec-filings-intel-3x-daily | double_fire |
| cron(15 0/6 * * ? *) | justhodl-signal-board | 1 | signal-board-3h | double_fire |
| cron(0 6 ? * MON *) | justhodl-signal-halflife | 1 | signal-halflife-weekly | double_fire |
| cron(15 23 * * ? *) | justhodl-signal-harvester | 1 | signal-harvester-daily | double_fire |
| cron(0 11 ? * MON *) | justhodl-smart-money-13f | 1 | smart-money-13f-weekly | double_fire |
| cron(0 12 ? * * *) | justhodl-sovereign-stress | 1 | sovereign-stress-daily | double_fire |
| cron(15 12 * * ? *) | justhodl-stocktwits | 1 | stocktwits-daily | double_fire |
| cron(0 5 ? * SUN *) | justhodl-symbol-dictionary | 1 | symbol-dictionary-weekly | double_fire |
| cron(30 13 * * ? *) | justhodl-theme-rotation | 1 | theme-rotation-daily | double_fire |
| cron(0 14 * * ? *) | justhodl-theme-second-wave | 1 | theme-second-wave-daily | double_fire |
| cron(45 22 ? * TUE-SAT *) | justhodl-thesis-engine | 1 | thesis-engine-daily | double_fire |
| cron(15 21 ? * TUE-SAT *) | justhodl-tv-watchlist-tracker | 1 | tv-watchlist-tracker-daily | double_fire |
| cron(30 22 ? * TUE-SAT *) | justhodl-wl-engines | 1 | wl-engines-daily | double_fire |
| cron(50 22 ? * TUE-SAT *) | justhodl-wl-fusion | 1 | wl-fusion-daily | double_fire |

## Log
## A. Remove duplicate targets inside EventBridge rules

- `14:15:48` ✅   ai-infra-stack-daily                     justhodl-ai-infra-stack            removed 1 redundant target(s)
- `14:15:48` ✅   ai-rerating-radar-daily                  justhodl-ai-rerating-radar         removed 1 redundant target(s)
- `14:15:48` ✅   alpha-compass-3h                         justhodl-alpha-compass             removed 1 redundant target(s)
- `14:15:48` ✅   ark-holdings-daily                       justhodl-ark-holdings              removed 1 redundant target(s)
- `14:15:49` ✅   attention-signals-daily                  justhodl-attention-signals         removed 1 redundant target(s)
- `14:15:49` ✅   bea-economic-agent-daily                 bea-economic-agent                 removed 1 redundant target(s)
- `14:15:49` ✅   bls-labor-agent-daily                    bls-labor-agent                    removed 1 redundant target(s)
- `14:15:50` ✅   capital-flow-radar-daily                 justhodl-capital-flow-radar        removed 1 redundant target(s)
- `14:15:50` ✅   carry-surface-4h                         justhodl-carry-surface             removed 1 redundant target(s)
- `14:15:50` ✅   census-economic-agent-daily              census-economic-agent              removed 1 redundant target(s)
- `14:15:51` ✅   crisis-composite-hourly                  justhodl-crisis-composite          removed 1 redundant target(s)
- `14:15:52` ✅   engine-signal-map-daily                  justhodl-engine-signal-map         removed 1 redundant target(s)
- `14:15:52` ✅   engine-trust-daily                       justhodl-engine-trust              removed 1 redundant target(s)
- `14:15:52` ✅   eurodollar-plumbing-daily                justhodl-eurodollar-plumbing       removed 1 redundant target(s)
- `14:15:52` ✅   event-flow-monitor-hourly                justhodl-event-flow-monitor        removed 1 redundant target(s)
- `14:15:53` ✅   fast-filings-daily                       justhodl-fast-filings              removed 1 redundant target(s)
- `14:15:53` ✅   fed-nlp-6h                               justhodl-fed-nlp                   removed 1 redundant target(s)
- `14:15:53` ✅   finnhub-signals-daily                    justhodl-finnhub-signals           removed 1 redundant target(s)
- `14:15:53` ✅   fomc-reaction-daily                      justhodl-fomc-reaction             removed 1 redundant target(s)
- `14:15:54` ✅   forward-returns-weekly                   justhodl-forward-returns           removed 1 redundant target(s)
- `14:15:54` ✅   gdelt-buzz-daily                         justhodl-gdelt-buzz                removed 1 redundant target(s)
- `14:15:54` ✅   global-liquidity-daily                   justhodl-global-liquidity          removed 1 redundant target(s)
- `14:15:54` ✅   global-sovereign-12h                     justhodl-global-sovereign          removed 1 redundant target(s)
- `14:15:55` ✅   hiring-velocity-weekly                   justhodl-hiring-velocity           removed 1 redundant target(s)
- `14:15:55` ✅   hkma-monitor-daily                       justhodl-hkma-monitor              removed 1 redundant target(s)
- `14:15:55` ✅   industry-rotation-daily                  justhodl-industry-rotation         removed 1 redundant target(s)
- `14:15:57` ✅   jsi-6h                                   justhodl-stress-index              removed 1 redundant target(s)
- `14:15:57` ✅   jsi-calibrator-weekly                    justhodl-jsi-calibrator            removed 1 redundant target(s)
- `14:15:58` ✅   justhodl-accumulation-radar-daily        justhodl-accumulation-radar        removed 1 redundant target(s)
- `14:15:59` ✅   justhodl-attention-confluence-daily      justhodl-attention-confluence      removed 1 redundant target(s)
- `14:16:00` ✅   justhodl-brain-sync-monthly              justhodl-brain-sync                removed 1 redundant target(s)
- `14:16:00` ✅   justhodl-breadth-thrust-daily            justhodl-breadth-thrust            removed 1 redundant target(s)
- `14:16:00` ✅   justhodl-canary-warroom-hourly           justhodl-canary-warroom            removed 1 redundant target(s)
- `14:16:02` ✅   justhodl-crypto-emergence-daily          justhodl-crypto-emergence          removed 1 redundant target(s)
- `14:16:02` ✅   justhodl-crypto-liquidity-daily          justhodl-crypto-liquidity          removed 1 redundant target(s)
- `14:16:02` ✅   justhodl-cycle-clock-daily               justhodl-cycle-clock               removed 1 redundant target(s)
- `14:16:03` ✅   justhodl-dark-pool-weekly                justhodl-dark-pool                 removed 1 redundant target(s)
- `14:16:03` ✅   justhodl-dealer-gex-hourly               justhodl-dealer-gex                removed 1 redundant target(s)
- `14:16:04` ✅   justhodl-earnings-confluence-daily       justhodl-earnings-confluence       removed 1 redundant target(s)
- `14:16:04` ✅   justhodl-edge-discovery-weekly           justhodl-edge-discovery            removed 1 redundant target(s)
- `14:16:04` ✅   justhodl-equity-confluence-daily         justhodl-equity-confluence         removed 1 redundant target(s)
- `14:16:07` ✅   justhodl-interp-grader-daily             justhodl-interpretation-grader     removed 1 redundant target(s)
- `14:16:08` ✅   justhodl-llm-cost-dashboard-hourly       justhodl-llm-cost-dashboard        removed 1 redundant target(s)
- `14:16:08` ✅   justhodl-ma200-reclaim-daily             justhodl-ma200-reclaim             removed 1 redundant target(s)
- `14:16:08` ✅   justhodl-macro-leads-daily               justhodl-macro-leads               removed 1 redundant target(s)
- `14:16:09` ✅   justhodl-nowcast-desk-daily              justhodl-nowcast-desk              removed 1 redundant target(s)
- `14:16:10` ✅   justhodl-options-analytics-daily         justhodl-options-analytics         removed 1 redundant target(s)
- `14:16:10` ✅   justhodl-outcome-checker-4h              justhodl-outcome-checker           removed 1 redundant target(s)
- `14:16:10` ✅   justhodl-paper-book-daily                justhodl-paper-book                removed 1 redundant target(s)
- `14:16:10` ✅   justhodl-peru-copper-daily               justhodl-peru-copper               removed 1 redundant target(s)
- `14:16:11` ✅   justhodl-refining-stress-daily           justhodl-refining-stress           removed 1 redundant target(s)
- `14:16:11` ✅   justhodl-regime-cond-trust-daily         justhodl-regime-conditional-trust  removed 1 redundant target(s)
- `14:16:12` ✅   justhodl-regime-map-daily                justhodl-regime-map                removed 1 redundant target(s)
- `14:16:12` ✅   justhodl-resilience-daily                justhodl-resilience                removed 1 redundant target(s)
- `14:16:13` ✅   justhodl-sector-emergence-daily          justhodl-sector-emergence          removed 1 redundant target(s)
- `14:16:14` ✅   justhodl-singapore-nodx-daily            justhodl-singapore-nodx            removed 1 redundant target(s)
- `14:16:14` ✅   justhodl-squeeze-fuel-daily              justhodl-squeeze-fuel              removed 1 redundant target(s)
- `14:16:14` ✅   justhodl-strategist-daily                justhodl-strategist                removed 1 redundant target(s)
- `14:16:14` ✅   justhodl-strategy-portfolio-weekly       justhodl-strategy-portfolio        removed 1 redundant target(s)
- `14:16:14` ✅   justhodl-supply-chain-graph-daily        justhodl-supply-chain-graph        removed 1 redundant target(s)
- `14:16:15` ✅   justhodl-tail-risk-daily                 justhodl-tail-risk                 removed 1 redundant target(s)
- `14:16:15` ✅   justhodl-taiwan-moea-daily               justhodl-taiwan-moea               removed 1 redundant target(s)
- `14:16:16` ✅   justhodl-treasury-noise-daily            justhodl-treasury-noise            removed 1 redundant target(s)
- `14:16:17` ✅   leadlag-graph-daily                      justhodl-leadlag-graph             removed 1 redundant target(s)
- `14:16:17` ✅   lobbying-intel-daily                     justhodl-lobbying-intel            removed 1 redundant target(s)
- `14:16:17` ✅   magnitude-distributions-daily            justhodl-magnitude-distributions   removed 1 redundant target(s)
- `14:16:18` ✅   market-internals-daily                   justhodl-market-internals          removed 1 redundant target(s)
- `14:16:18` ✅   massive-signals-daily                    justhodl-massive-signals           removed 1 redundant target(s)
- `14:16:18` ✅   miss-calibrator-weekly                   justhodl-miss-calibrator           removed 1 redundant target(s)
- `14:16:18` ✅   miss-detector-nightly                    justhodl-miss-detector             removed 1 redundant target(s)
- `14:16:18` ✅   money-flow-state-daily                   justhodl-money-flow-state          removed 1 redundant target(s)
- `14:16:18` ✅   near-miss-monitor-hourly                 justhodl-near-miss-monitor         removed 1 redundant target(s)
- `14:16:19` ✅   notes-intel-daily                        justhodl-notes-intel               removed 1 redundant target(s)
- `14:16:19` ✅   patent-velocity-daily                    justhodl-patent-velocity           removed 1 redundant target(s)
- `14:16:19` ✅   political-stocks-daily                   justhodl-political-stocks          removed 1 redundant target(s)
- `14:16:20` ✅   rotation-chain-2x-daily                  justhodl-rotation-chain            removed 1 redundant target(s)
- `14:16:20` ✅   sec-filings-intel-3x-daily               justhodl-sec-filings-intel         removed 1 redundant target(s)
- `14:16:20` ✅   signal-board-3h                          justhodl-signal-board              removed 1 redundant target(s)
- `14:16:20` ✅   signal-halflife-weekly                   justhodl-signal-halflife           removed 1 redundant target(s)
- `14:16:21` ✅   signal-harvester-daily                   justhodl-signal-harvester          removed 1 redundant target(s)
- `14:16:21` ✅   smart-money-13f-weekly                   justhodl-smart-money-13f           removed 1 redundant target(s)
- `14:16:21` ✅   sovereign-stress-daily                   justhodl-sovereign-stress          removed 1 redundant target(s)
- `14:16:21` ✅   stocktwits-daily                         justhodl-stocktwits                removed 1 redundant target(s)
- `14:16:22` ✅   symbol-dictionary-weekly                 justhodl-symbol-dictionary         removed 1 redundant target(s)
- `14:16:22` ✅   theme-rotation-daily                     justhodl-theme-rotation            removed 1 redundant target(s)
- `14:16:22` ✅   theme-second-wave-daily                  justhodl-theme-second-wave         removed 1 redundant target(s)
- `14:16:22` ✅   thesis-engine-daily                      justhodl-thesis-engine             removed 1 redundant target(s)
- `14:16:23` ✅   tv-watchlist-tracker-daily               justhodl-tv-watchlist-tracker      removed 1 redundant target(s)
- `14:16:23` ✅   wl-engines-daily                         justhodl-wl-engines                removed 1 redundant target(s)
- `14:16:23` ✅   wl-fusion-daily                          justhodl-wl-fusion                 removed 1 redundant target(s)
- `14:16:23` 
- `14:16:23` enabled rules scanned: 414 | rules fixed: 90 | redundant targets removed: 90
- `14:16:23` => 90 engines were firing twice per tick and now fire once
## B. Strip stale OpenSearch env ref, then re-gate

- `14:16:23`   removing OPENSEARCH_ENDPOINT = search-openbb-financial-search-pjxaw2cqqeqfilppjyxkhfgwue.us-east-1.es
- `14:16:24` ✅   env updated (7 -> 6 vars); prior env backed up to S3
- `14:16:24`   re-running the reference gate…
- `14:16:31` ✅   gate clean — 0 references fleet-wide
- `14:16:32` ✅   OpenSearch openbb-financial-search DELETED (2 docs, orphaned)
## C. openbb-simple-working — backup, verify, then delete

- `14:16:32`   endpoint search-openbb-simple-working-oi5qjg5nan4a73ifgopmhfp234.us-east-1.es.amazonaws.com
- `14:16:33` ✅   access policy MERGED (existing statements preserved, 2 total)
- `14:16:33`   waiting for the domain to apply the config…
- `14:16:49` ✅   config active after 15s
- `14:16:52` ✅   dump OK — 6206 docs retrieved (index reports 6206)
- `14:16:53`   verify: s3 round-trip returned 6206 docs
- `14:16:53` ✅   backup verified: s3://justhodl-dashboard-live/backups/opensearch/openbb-simple-working-20260801.json
- `14:16:53` ✅   OpenSearch openbb-simple-working DELETED (6206 docs preserved on S3)
## D. Result

- `14:16:53`    {"a": "strip_env", "fn": "scrapeMacroData", "removed": ["OPENSEARCH_ENDPOINT"]}
- `14:16:53`    {"a": "delete_domain", "id": "openbb-financial-search"}
- `14:16:53`    {"a": "delete_domain", "id": "openbb-simple-working", "backup": "backups/opensearch/openbb-simple-working-20260801.json", "docs": 6206}
- `14:16:53` double-fire rules fixed: 90 (90 redundant targets removed)
- `14:16:53` ✅ wrote 4232_close_out.json
