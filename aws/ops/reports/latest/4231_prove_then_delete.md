# ops 4231 — prove-before-delete + fleet schedule dedupe

**Status:** success  
**Duration:** 52.1s  
**Finished:** 2026-08-01T14:09:53+00:00  

## Data

| candidate | dns_refs | env_refs | exprs | function | repo_refs | section | verdict |
|---|---|---|---|---|---|---|---|
| opensearch:openbb-financial-search | 0 | 2 |  |  | 0 | gate | REFERENCED |
| opensearch:openbb-simple-working | 0 | 0 |  |  | 0 | gate | ORPHANED |
| elbv2:openbb-prod-alb | 0 | 0 |  |  | 0 | gate | ORPHANED |
| elbv2:openbb-basic-alb | 0 | 0 |  |  | 0 | gate | ORPHANED |
|  |  |  | cron(0 11 * * ? *), cron(0 12 1 * ? *), cron(0 12 ? * SUN *), cron(0 15 * * ? *), cron(0 16 * * ? *) | justhodl-scheduler |  | different_intent |  |
|  |  |  | cron(0 12 * * ? *), cron(0 21 * * ? *), cron(0 9 * * ? *), cron(20 1,7,13,19 * * ? *), cron(30 10 *  | justhodl-ai-brief-router |  | different_intent |  |
|  |  |  | cron(0 12 * * ? *), cron(40 13 ? * MON-FRI *), cron(40 17 ? * MON-FRI *) | justhodl-estimate-revisions |  | different_intent |  |
|  |  |  | cron(35 21 ? * * *), cron(40 12 * * ? *), cron(55 21 * * ? *) | justhodl-crypto-exchange-flows |  | different_intent |  |
|  |  |  | cron(15 21 ? * MON-FRI *), cron(40 12 * * ? *), cron(40 22 ? * TUE-SAT *) | justhodl-market-internals |  | different_intent |  |
|  |  |  | cron(12 18 * * ? *), cron(38 * * * ? *), cron(5 21 ? * MON-FRI *) | justhodl-sector-rotation |  | different_intent |  |
|  |  |  | cron(0 22 * * ? *), cron(20 21 * * ? *) | justhodl-backtest-harness |  | different_intent |  |
|  |  |  | cron(0 13 ? * SUN *), cron(11 14 * * ? *) | justhodl-behavior-mirror |  | different_intent |  |
|  |  |  | cron(0 3 ? * SUN *), cron(29 14 * * ? *) | justhodl-beneish |  | different_intent |  |
|  |  |  | cron(20 22 ? * MON-FRI *), cron(30 22 * * ? *) | justhodl-capital-flow-radar |  | different_intent |  |
|  |  |  | cron(0 13 * * ? *), rate(4 hours) | justhodl-carry-surface |  | different_intent |  |
|  |  |  | cron(0 21 ? * SUN *), cron(8 14 * * ? *) | justhodl-causality-scanner |  | different_intent |  |

## Log
## 1. REFERENCE SWEEP — the gate

- `14:09:01` 1a. loading live Lambda env vars across the fleet…
- `14:09:07` ✅    765 functions, 511 carrying env vars
- `14:09:07` 1b. loading Route53 record sets…
- `14:09:08` ⚠    route53 not in scope (An error occurred (AccessDenied) when calling the ListHostedZones oper) — DNS surface unverified
- `14:09:08` ✗   opensearch:openbb-financial-search           REFERENCED — HOLD
- `14:09:08`        env: scrapeMacroData, scrapeMacroData
- `14:09:08` ✅   opensearch:openbb-simple-working             ORPHANED — 0 refs on all surfaces
- `14:09:08` ✅   elbv2:openbb-prod-alb                        ORPHANED — 0 refs on all surfaces
- `14:09:08` ✅   elbv2:openbb-basic-alb                       ORPHANED — 0 refs on all surfaces
## 2. Re-confirm zero traffic at deletion time

- `14:09:09`    ALB openbb-prod-alb        14d requests = 0
- `14:09:09`    ALB openbb-basic-alb       14d requests = 0
## 3. Deletions — only where the gate passed

- `14:09:09` ✅   ALB openbb-prod-alb DELETED (0 requests/14d, 0 references)
- `14:09:11` ⚠      tg openbb-api-tg: An error occurred (AccessDenied) when calling the DeleteTargetGroup op
- `14:09:11` ✅   ALB openbb-basic-alb DELETED (0 requests/14d, 0 references)
- `14:09:11` ⚠   OpenSearch openbb-financial-search HELD — referenced
- `14:09:27` ⚠      index dump failed: HTTP Error 403: Forbidden
- `14:09:27` ✗   OpenSearch openbb-simple-working HELD — 6206 docs and the backup dump failed. Deleting is irreversible; not risking the index.
- `14:09:28` ✅   SageMaker app default (JupyterLab) DELETED — EFS home preserved, relaunch from Studio anytime
- `14:09:28` 
- `14:09:28`   EIP 44.223.130.247   still associated (eni-00e4d6c6a852ea8c3) — KEPT
## 4. Fleet-wide EXACT-duplicate schedule dedupe

- `14:09:28` rule = same target function AND same expression AND same input payload. Anything else is different intent, untouched.
- `14:09:51` enabled Lambda-targeting schedules: 803
- `14:09:51` EXACT duplicate groups (fn+expr+payload identical): 113
- `14:09:51`   bea-economic-agent                       cron(15 14 * * ? *)        x2  keep=bea-economic-agent-daily
- `14:09:51`   bls-labor-agent                          cron(0 14 * * ? *)         x2  keep=bls-labor-agent-daily
- `14:09:51`   census-economic-agent                    cron(30 14 * * ? *)        x2  keep=census-economic-agent-dail
- `14:09:51`   justhodl-accumulation-radar              cron(50 21 ? * MON-FRI *)  x2  keep=justhodl-accumulation-rada
- `14:09:51`   justhodl-activity-nowcast                cron(30 12 * * ? *)        x2  keep=activity-nowcast-sched
- `14:09:51`      disabled events/activity-nowcast-daily
- `14:09:51`   justhodl-ai-infra-stack                  cron(45 13 * * ? *)        x2  keep=ai-infra-stack-daily
- `14:09:51`   justhodl-ai-rerating-radar               cron(15 14 * * ? *)        x2  keep=ai-rerating-radar-daily
- `14:09:51`   justhodl-alpha-compass                   cron(50 */3 * * ? *)       x2  keep=alpha-compass-3h
- `14:09:51`   justhodl-alpha-daily-brief               cron(30 11 * * ? *)        x3  keep=alpha-daily-brief-sched
- `14:09:52`      disabled events/justhodl-alpha-daily-brief
- `14:09:52`      disabled events/justhodl-alpha-daily-brief
- `14:09:52`   justhodl-ark-holdings                    cron(0 6 * * ? *)          x2  keep=ark-holdings-daily
- `14:09:52`   justhodl-attention-confluence            cron(10 15 * * ? *)        x2  keep=justhodl-attention-conflue
- `14:09:52`   justhodl-attention-signals               cron(45 14 * * ? *)        x2  keep=attention-signals-daily
- `14:09:52`   justhodl-brain-sync                      cron(0 9 1 * ? *)          x2  keep=justhodl-brain-sync-monthl
- `14:09:52`   justhodl-breadth-thrust                  cron(0 22 ? * MON-FRI *)   x2  keep=justhodl-breadth-thrust-da
- `14:09:52`   justhodl-buyback-scanner                 cron(0 12 ? * MON *)       x2  keep=justhodl-buyback-scanner-s
- `14:09:52`      disabled events/justhodl-buyback-scanner-weekly
- `14:09:52`   justhodl-canary-warroom                  cron(50 * * * ? *)         x2  keep=justhodl-canary-warroom-ho
- `14:09:52`   justhodl-capital-flow-radar              cron(30 22 * * ? *)        x2  keep=capital-flow-radar-daily
- `14:09:52`   justhodl-carry-surface                   rate(4 hours)              x2  keep=carry-surface-4h
- `14:09:52`   justhodl-chokepoint                      cron(30 15 * * ? *)        x2  keep=chokepoint-sched
- `14:09:52`      disabled events/justhodl-chokepoint-daily
- `14:09:52`   justhodl-coinbase-premium                cron(35 * * * ? *)         x2  keep=coinbase-premium-sched
- `14:09:52`      disabled events/justhodl-coinbase-premium-hourly
- `14:09:52`   justhodl-crisis-composite                cron(15 * * * ? *)         x2  keep=crisis-composite-hourly
- `14:09:52`   justhodl-crypto-confluence               cron(20 23 * * ? *)        x2  keep=crypto-confluence-sched
- `14:09:52`      disabled events/justhodl-crypto-confluence-daily
- `14:09:52`   justhodl-crypto-emergence                cron(0 13,23 * * ? *)      x2  keep=justhodl-crypto-emergence-
- `14:09:52`   justhodl-crypto-liquidity                cron(0 12,22 * * ? *)      x2  keep=justhodl-crypto-liquidity-
- `14:09:52`   justhodl-crypto-miners                   cron(30 12 * * ? *)        x2  keep=crypto-miners-sched
- `14:09:52`      disabled events/justhodl-crypto-miners-daily
- `14:09:52`   justhodl-cycle-clock                     cron(30 23 * * ? *)        x2  keep=justhodl-cycle-clock-daily
- `14:09:52`   justhodl-dark-pool                       cron(0 14 ? * WED *)       x2  keep=justhodl-dark-pool-weekly
- `14:09:52`   justhodl-dealer-gex                      cron(7 13-21 ? * MON-FRI * x2  keep=justhodl-dealer-gex-hourly
- `14:09:52`   justhodl-debate-engine                   cron(15 12 * * ? *)        x2  keep=debate-engine-sched
- `14:09:52`      disabled events/justhodl-debate-engine-daily
- `14:09:52`   justhodl-earnings-confluence             cron(30 22 ? * MON-FRI *)  x2  keep=justhodl-earnings-confluen
- `14:09:52`   justhodl-earnings-sentiment              cron(0 10 * * ? *)         x2  keep=earnings-sentiment-sched
- `14:09:52`      disabled events/justhodl-earnings-sentiment-daily
- `14:09:52`   justhodl-edge-discovery                  cron(0 11 ? * SUN *)       x2  keep=justhodl-edge-discovery-we
- `14:09:52`   justhodl-engine-signal-map               cron(0 9 * * ? *)          x2  keep=engine-signal-map-daily
- `14:09:52`   justhodl-engine-trust                    cron(30 12 * * ? *)        x2  keep=engine-trust-daily
- `14:09:52`   justhodl-equity-confluence               cron(30 0 * * ? *)         x2  keep=justhodl-equity-confluence
- `14:09:52`   justhodl-eurodollar-plumbing             cron(0 12 * * ? *)         x2  keep=eurodollar-plumbing-daily
- `14:09:52`   justhodl-event-flow-monitor              rate(6 hours)              x2  keep=event-flow-monitor-hourly
- `14:09:52`   justhodl-factor-decomposition            cron(35 13 ? * MON *)      x2  keep=justhodl-factor-decomp-wee
- `14:09:52`      disabled events/justhodl-factor-decomposition-weekly
- `14:09:52`   justhodl-fast-filings                    cron(0 12 * * ? *)         x2  keep=fast-filings-daily
- `14:09:52`   justhodl-fed-nlp                         rate(6 hours)              x2  keep=fed-nlp-6h
- `14:09:52`   justhodl-finnhub-signals                 cron(0 12 * * ? *)         x2  keep=finnhub-signals-daily
- `14:09:52`   justhodl-finviz-signals                  cron(0 14,18,21 * * ? *)   x2  keep=finviz-signals-sched
- `14:09:52`      disabled events/finviz-signals-3x
- `14:09:52`   justhodl-fomc-reaction                   cron(35 21 * * ? *)        x2  keep=fomc-reaction-daily
- `14:09:52`   justhodl-forward-returns                 cron(0 3 ? * SUN *)        x2  keep=forward-returns-weekly
- `14:09:52`   justhodl-gdelt-buzz                      cron(45 12 * * ? *)        x2  keep=gdelt-buzz-daily
- `14:09:52`   justhodl-global-liquidity                cron(0 14 ? * MON-FRI *)   x2  keep=global-liquidity-daily
- `14:09:52`   justhodl-global-sovereign                cron(15 6,18 * * ? *)      x2  keep=global-sovereign-12h
- `14:09:52`   justhodl-hiring-velocity                 cron(30 12 ? * SUN *)      x2  keep=hiring-velocity-weekly
- `14:09:52`   justhodl-hkma-monitor                    cron(0 9 * * ? *)          x2  keep=hkma-monitor-daily
- `14:09:52`   justhodl-hot-stocks-digest               cron(30 12 * * ? *)        x2  keep=hot-stocks-digest-sched
- `14:09:52`      disabled events/justhodl-hot-stocks-digest-am
- `14:09:52`   justhodl-industry-rotation               cron(35 21 * * ? *)        x2  keep=industry-rotation-daily
- `14:09:52`   justhodl-insider-buys-enriched           cron(0 10 * * ? *)         x2  keep=justhodl-insider-buys-enri
- `14:09:52`      disabled events/justhodl-insider-buys-enriched-sched
- `14:09:52`   justhodl-interpretation-grader           cron(0 15 * * ? *)         x2  keep=justhodl-interp-grader-dai
- `14:09:52`   justhodl-jsi-calibrator                  cron(30 9 ? * SUN *)       x2  keep=jsi-calibrator-weekly
- `14:09:52`   justhodl-leadlag-graph                   cron(0 13 * * ? *)         x2  keep=leadlag-graph-daily
- `14:09:52`   justhodl-llm-cost-dashboard              cron(5 * * * ? *)          x2  keep=justhodl-llm-cost-dashboar
- `14:09:52`   justhodl-lobbying-intel                  cron(0 16 * * ? *)         x2  keep=lobbying-intel-daily
- `14:09:52`   justhodl-ma200-reclaim                   cron(30 21 ? * MON-FRI *)  x2  keep=justhodl-ma200-reclaim-dai
- `14:09:52`   justhodl-macro-leads                     cron(20 12 * * ? *)        x2  keep=justhodl-macro-leads-daily
- `14:09:52`   justhodl-magnitude-distributions         cron(30 7 * * ? *)         x2  keep=magnitude-distributions-da
- `14:09:52`   justhodl-market-internals                cron(40 22 ? * TUE-SAT *)  x2  keep=market-internals-daily
- `14:09:52`   justhodl-massive-signals                 cron(0 22 * * ? *)         x2  keep=massive-signals-daily
- `14:09:52`   justhodl-meta-improver                   cron(0 22 ? * SUN *)       x2  keep=meta-improver-sched
- `14:09:52`      disabled events/meta-improver-weekly
- `14:09:52`   justhodl-miss-calibrator                 cron(15 9 ? * MON *)       x2  keep=miss-calibrator-weekly
- `14:09:52`   justhodl-miss-detector                   cron(0 1 * * ? *)          x2  keep=miss-detector-nightly
- `14:09:52`   justhodl-money-flow-state                cron(30 23 * * ? *)        x2  keep=money-flow-state-daily
- `14:09:52`   justhodl-near-miss-monitor               cron(58 21 * * ? *)        x2  keep=near-miss-monitor-hourly
- `14:09:52`   justhodl-news-wire                       cron(0 11 * * ? *)         x2  keep=news-wire-sched
- `14:09:52`      disabled events/news-wire-15m
- `14:09:52`   justhodl-notes-intel                     cron(10 12 * * ? *)        x2  keep=notes-intel-daily
- `14:09:52`   justhodl-nowcast-desk                    cron(30 13 * * ? *)        x2  keep=justhodl-nowcast-desk-dail
- `14:09:52`   justhodl-opex-calendar                   cron(0 11 * * ? *)         x2  keep=justhodl-opex-calendar-dai
- `14:09:52`      disabled events/justhodl-opex-calendar-sched
- `14:09:52`   justhodl-options-analytics               cron(0 13 ? * TUE-SAT *)   x2  keep=justhodl-options-analytics
- `14:09:52`   justhodl-options-confluence              cron(20 * * * ? *)         x3  keep=options-confluence-sched
- `14:09:52`      disabled events/justhodl-options-confluence-hourly
- `14:09:52`      disabled events/justhodl-options-confluence-hourly
- `14:09:52`   justhodl-outcome-checker                 cron(29 21 * * ? *)        x2  keep=justhodl-outcome-checker-4
- `14:09:52`   justhodl-paper-book                      cron(30 21 * * ? *)        x2  keep=justhodl-paper-book-daily
- `14:09:52`   justhodl-patent-velocity                 cron(0 17 * * ? *)         x2  keep=patent-velocity-daily
- `14:09:52`   justhodl-peru-copper                     cron(0 4 * * ? *)          x2  keep=justhodl-peru-copper-daily
- `14:09:52`   justhodl-political-stocks                cron(0 14 * * ? *)         x2  keep=political-stocks-daily
- `14:09:52`   justhodl-premortem-engine                cron(0 14 ? * MON-FRI *)   x3  keep=premortem-engine-sched
- `14:09:52`      disabled events/premortem-engine-daily
- `14:09:52`      disabled events/premortem-engine-daily
- `14:09:52`   justhodl-refining-stress                 cron(20 13 * * ? *)        x2  keep=justhodl-refining-stress-d
- `14:09:52`   justhodl-regime-conditional-trust        cron(0 12 * * ? *)         x2  keep=justhodl-regime-cond-trust
- `14:09:52`   justhodl-regime-map                      cron(0 13,21 * * ? *)      x2  keep=justhodl-regime-map-daily
- `14:09:52`   justhodl-resilience                      cron(45 22 ? * MON-FRI *)  x2  keep=justhodl-resilience-daily
- `14:09:52`   justhodl-rotation-chain                  cron(50 15 * * ? *)        x2  keep=rotation-chain-2x-daily
- `14:09:52`   justhodl-russell-recon-frontrun          cron(30 12 * * ? *)        x2  keep=justhodl-russell-recon-dai
- `14:09:52`      disabled events/justhodl-russell-recon-frontrun-sched
- `14:09:52`   justhodl-rv-iv-scanner                   cron(30 14 * * ? *)        x2  keep=justhodl-rv-iv-scanner-dai
- `14:09:52`      disabled events/justhodl-rv-iv-scanner-sched
- `14:09:52`   justhodl-sec-filings-intel               cron(7 21 * * ? *)         x2  keep=sec-filings-intel-3x-daily
- `14:09:52`   justhodl-sector-emergence                cron(0 22 * * ? *)         x2  keep=justhodl-sector-emergence-
- `14:09:52`   justhodl-signal-board                    cron(15 0/6 * * ? *)       x2  keep=signal-board-3h
- `14:09:52`   justhodl-signal-halflife                 cron(0 6 ? * MON *)        x2  keep=signal-halflife-weekly
- `14:09:52`   justhodl-signal-harvester                cron(15 23 * * ? *)        x2  keep=signal-harvester-daily
- `14:09:52`   justhodl-singapore-nodx                  cron(20 4 * * ? *)         x2  keep=justhodl-singapore-nodx-da
- `14:09:52`   justhodl-smart-money-13f                 cron(0 11 ? * MON *)       x2  keep=smart-money-13f-weekly
- `14:09:52`   justhodl-sovereign-stress                cron(0 12 ? * * *)         x2  keep=sovereign-stress-daily
- `14:09:52`   justhodl-squeeze-fuel                    cron(30 13 ? * TUE-SAT *)  x2  keep=justhodl-squeeze-fuel-dail
- `14:09:52`   justhodl-stocktwits                      cron(15 12 * * ? *)        x2  keep=stocktwits-daily
- `14:09:52`   justhodl-strategist                      cron(0 14 ? * MON-FRI *)   x2  keep=justhodl-strategist-daily
- `14:09:52`   justhodl-strategy-portfolio              cron(20 12 ? * SUN *)      x2  keep=justhodl-strategy-portfoli
- `14:09:52`   justhodl-stress-index                    rate(6 hours)              x2  keep=jsi-6h
- `14:09:52`   justhodl-supply-chain-graph              cron(45 13 ? * TUE-SAT *)  x2  keep=justhodl-supply-chain-grap
- `14:09:52`   justhodl-symbol-dictionary               cron(0 5 ? * SUN *)        x2  keep=symbol-dictionary-weekly
- `14:09:52`   justhodl-tail-risk                       cron(0 13 ? * TUE-SAT *)   x2  keep=justhodl-tail-risk-daily
- `14:09:52`   justhodl-taiwan-moea                     cron(30 3 * * ? *)         x2  keep=justhodl-taiwan-moea-daily
- `14:09:52`   justhodl-theme-rotation                  cron(30 13 * * ? *)        x2  keep=theme-rotation-daily
- `14:09:52`   justhodl-theme-second-wave               cron(0 14 * * ? *)         x2  keep=theme-second-wave-daily
- `14:09:52`   justhodl-thesis-engine                   cron(45 22 ? * TUE-SAT *)  x2  keep=thesis-engine-daily
- `14:09:52`   justhodl-trade-evaluator                 cron(0 23 * * ? *)         x2  keep=trade-evaluator-sched
- `14:09:52`      disabled events/justhodl-trade-evaluator-daily
- `14:09:52`   justhodl-treasury-noise                  cron(30 13 ? * TUE-SAT *)  x2  keep=justhodl-treasury-noise-da
- `14:09:52`   justhodl-tv-watchlist-tracker            cron(15 21 ? * TUE-SAT *)  x2  keep=tv-watchlist-tracker-daily
- `14:09:52`   justhodl-vix-backwardation-trigger       cron(15 13 * * ? *)        x2  keep=justhodl-vix-backwardation
- `14:09:53`      disabled events/justhodl-vix-backwardation-trigger-daily
- `14:09:53`   justhodl-vol-surface                     cron(30 13 * * ? *)        x2  keep=justhodl-vol-surface-daily
- `14:09:53`      disabled events/justhodl-vol-surface-sched
- `14:09:53`   justhodl-wl-engines                      cron(30 22 ? * TUE-SAT *)  x2  keep=wl-engines-daily
- `14:09:53`   justhodl-wl-fusion                       cron(50 22 ? * TUE-SAT *)  x2  keep=wl-fusion-daily
- `14:09:53` ✅ disabled 26 exact-duplicate schedules (NOT deleted — re-enable is one call each)
- `14:09:53` 
- `14:09:53` UNTOUCHED — same function, DIFFERENT cadence/payload (different intent): 68 functions
- `14:09:53`    justhodl-scheduler                       cron(0 11 * * ? *), cron(0 12 1 * ? *), cron(0 12 ? * SUN *), cron(0 15 * * ? *)
- `14:09:53`    justhodl-ai-brief-router                 cron(0 12 * * ? *), cron(0 21 * * ? *), cron(0 9 * * ? *), cron(20 1,7,13,19 * *
- `14:09:53`    justhodl-estimate-revisions              cron(0 12 * * ? *), cron(40 13 ? * MON-FRI *), cron(40 17 ? * MON-FRI *)
- `14:09:53`    justhodl-crypto-exchange-flows           cron(35 21 ? * * *), cron(40 12 * * ? *), cron(55 21 * * ? *)
- `14:09:53`    justhodl-market-internals                cron(15 21 ? * MON-FRI *), cron(40 12 * * ? *), cron(40 22 ? * TUE-SAT *)
- `14:09:53`    justhodl-sector-rotation                 cron(12 18 * * ? *), cron(38 * * * ? *), cron(5 21 ? * MON-FRI *)
- `14:09:53`    justhodl-backtest-harness                cron(0 22 * * ? *), cron(20 21 * * ? *)
- `14:09:53`    justhodl-behavior-mirror                 cron(0 13 ? * SUN *), cron(11 14 * * ? *)
- `14:09:53`    justhodl-beneish                         cron(0 3 ? * SUN *), cron(29 14 * * ? *)
- `14:09:53`    justhodl-capital-flow-radar              cron(20 22 ? * MON-FRI *), cron(30 22 * * ? *)
- `14:09:53`    justhodl-carry-surface                   cron(0 13 * * ? *), rate(4 hours)
- `14:09:53`    justhodl-causality-scanner               cron(0 21 ? * SUN *), cron(8 14 * * ? *)
## 5. Rollback ledger

- `14:09:53` ✅ rollback ledger -> s3://justhodl-dashboard-live/backups/ops-4231-rollback.json
## RESULT

- `14:09:53` deleted: 3   held: 2   schedules disabled: 26
- `14:09:53`    - elbv2 openbb-prod-alb
- `14:09:53`    - elbv2 openbb-basic-alb
- `14:09:53`    - sagemaker_app default
- `14:09:53` ⚠    HELD openbb-financial-search (referenced)
- `14:09:53` ⚠    HELD openbb-simple-working (backup_failed)
