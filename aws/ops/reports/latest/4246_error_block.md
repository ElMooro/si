# ops 4246 — the D8 error block

**Status:** failure  
**Duration:** 151.7s  
**Finished:** 2026-08-01T18:48:13+00:00  

## Error

```
SystemExit: FAILS: scorecard probe
```

## Data

| backup | before_pct | bytes | function | now_pct | ok | runs_24h | runtime | section | tagged | verdict | writes |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  | 81.3 |  | dollar-strength-agent | 25.0 |  | 4 |  | timeout_outcome |  | IMPROVED |  |
|  | 84.5 |  | fedliquidityapi | 62.5 |  | 8 |  | timeout_outcome |  | IMPROVED |  |
|  | 42.6 |  | justhodl-cb-injection | 33.3 |  | 3 |  | timeout_outcome |  | UNCHANGED |  |
|  | 33.3 |  | justhodl-construction-housing | 0.0 |  | 2 |  | timeout_outcome |  | FIXED |  |
|  | 25.7 |  | justhodl-ecb-detail | 0.0 |  | 2 |  | timeout_outcome |  | FIXED |  |
|  | 31.5 |  | justhodl-fx-intelligence | 0.0 |  | 2 |  | timeout_outcome |  | FIXED |  |
|  | 28.2 |  | justhodl-gdelt-buzz | 0.0 |  | 2 |  | timeout_outcome |  | FIXED |  |
|  | None |  | justhodl-global-liquidity | 0.0 |  | 1 |  | timeout_outcome |  | no baseline |  |
|  | 100.0 |  | justhodl-search-attention | 75.0 |  | 4 |  | timeout_outcome |  | IMPROVED |  |
|  | 70.0 |  | justhodl-signal-scorecard | 100.0 |  | 9 |  | timeout_outcome |  | WORSE |  |
|  | 32.5 |  | justhodl-yen-carry | 33.3 |  | 3 |  | timeout_outcome |  | UNCHANGED |  |
|  | 83.7 |  | manufacturing-global-agent | 50.0 |  | 4 |  | timeout_outcome |  | IMPROVED |  |
|  | 83.1 |  | securities-banking-agent | 50.0 |  | 4 |  | timeout_outcome |  | IMPROVED |  |
|  | 68.8 |  | volatility-monitor-agent | 25.0 |  | 4 |  | timeout_outcome |  | IMPROVED |  |
|  |  |  |  |  | None |  |  | ssm |  |  | null |
| s3://justhodl-dashboard-live-dr/quarantine/2026-08-01/macro-report-api.zip |  | 29862 | macro-report-api |  |  |  | python3.9 | quarantine | False |  |  |
| s3://justhodl-dashboard-live-dr/quarantine/2026-08-01/multi-agent-orchestrator.zip |  | 1505 | multi-agent-orchestrator |  |  |  | python3.11 | quarantine | False |  |  |
| s3://justhodl-dashboard-live-dr/quarantine/2026-08-01/nyfed-financial-stability-fetcher.zip |  | 2769 | nyfed-financial-stability-fetcher |  |  |  | python3.9 | quarantine | False |  |  |
| s3://justhodl-dashboard-live-dr/quarantine/2026-08-01/nyfed-primary-dealer-fetcher.zip |  | 1692 | nyfed-primary-dealer-fetcher |  |  |  | python3.9 | quarantine | False |  |  |
| s3://justhodl-dashboard-live-dr/quarantine/2026-08-01/nyfedapi-isolated.zip |  | 4426 | nyfedapi-isolated |  |  |  | python3.9 | quarantine | False |  |  |
| s3://justhodl-dashboard-live-dr/quarantine/2026-08-01/ultimate-multi-agent.zip |  | 1931 | ultimate-multi-agent |  |  |  | python3.11 | quarantine | False |  |  |

## Log
## A. Did the ops-4234 timeout raises actually work?

- `18:45:41` engines whose timeout was raised in ops 4234: 14
- `18:45:41` 
- `18:45:41` ENGINE                                     BEFORE   NOW24h    RUNS24h VERDICT
- `18:45:42` dollar-strength-agent                         81%      25%          4 IMPROVED
- `18:45:42` fedliquidityapi                               84%      62%          8 IMPROVED
- `18:45:43` justhodl-cb-injection                         43%      33%          3 UNCHANGED
- `18:45:43` justhodl-construction-housing                 33%       0%          2 FIXED
- `18:45:44` justhodl-ecb-detail                           26%       0%          2 FIXED
- `18:45:44` justhodl-fx-intelligence                      32%       0%          2 FIXED
- `18:45:45` justhodl-gdelt-buzz                           28%       0%          2 FIXED
- `18:45:45` justhodl-global-liquidity                      ?%       0%          1 no baseline
- `18:45:46` justhodl-search-attention                    100%      75%          4 IMPROVED
- `18:45:46` justhodl-signal-scorecard                     70%     100%          9 WORSE
- `18:45:47` justhodl-yen-carry                            32%      33%          3 UNCHANGED
- `18:45:48` manufacturing-global-agent                    84%      50%          4 IMPROVED
- `18:45:48` securities-banking-agent                      83%      50%          4 IMPROVED
- `18:45:49` volatility-monitor-agent                      69%      25%          4 IMPROVED
- `18:45:49` 
- `18:45:49` fixed/improved=10 unchanged=3 worse=1 not-yet-run=0
- `18:45:49` ⚠ Engines that have not re-run since the change cannot be judged yet — most of this fleet is on daily cadence, so the honest read arrives tomorrow.
## A2. Current top of the error block

- `18:45:49` D8 entries on the board: 47
- `18:45:49`    aiapi-market-analyzer                    0% error rate (0/304), 5 throttles
- `18:45:49`    alphavantage-market-agent                0% error rate (0/533), 25 throttles
- `18:45:49`    bea-economic-agent                       0% error rate (0/282), 25 throttles
- `18:45:49`    bls-labor-agent                          2% error rate (6/300), 25 throttles
- `18:45:49`    bond-indices-agent                       1% error rate (4/303), 22 throttles
- `18:45:49`    census-economic-agent                    0% error rate (0/288), 18 throttles
- `18:45:49`    chatgpt-agent-api                        0% error rate (0/258), 21 throttles
- `18:45:49`    dollar-strength-agent                    81% error rate (209/257), 22 throttles
- `18:45:49`    enhanced-repo-agent                      1% error rate (4/308), 16 throttles
- `18:45:49`    fedliquidityapi                          85% error rate (289/342), 5 throttles
- `18:45:49`    fred-ice-bofa-api                        0% error rate (0/557), 15 throttles
- `18:45:49`    google-trends-agent                      0% error rate (0/255), 24 throttles
- `18:45:49`    justhodl-13f-positions                   0% error rate (0/24), 72 throttles
- `18:45:49`    justhodl-air-cargo                       27% error rate (11/41), 0 throttles
- `18:45:49`    justhodl-backtest-harness                0% error rate (0/42), 106 throttles
- `18:45:49`    justhodl-bagger-engine                   75% error rate (6/8), 0 throttles
- `18:45:49`    justhodl-boj-detail                      29% error rate (11/38), 0 throttles
- `18:45:49`    justhodl-cb-injection                    43% error rate (20/47), 0 throttles
## B. signal-scorecard — the silent-data bug

- `18:46:00` ✅ marker verified
- `18:48:06` ✗ probe FunctionError=Unhandled {"errorType":"Sandbox.Timedout","errorMessage":"RequestId: e39c9ae7-b6da-4773-9307-99f759eb4404 Error: Task timed out after 120.00 seconds"}
- `18:48:06` ssm_writes -> null
- `18:48:06` ⚠ artifact has no ssm_writes key yet (engine may not have reached that branch on this run)
## C. Quarantine the six broken packages

- `18:48:08` ✅   macro-report-api                     backed up 29862 bytes, concurrency 0, tagged
- `18:48:09` ✅   multi-agent-orchestrator             backed up 1505 bytes, concurrency 0, tagged
- `18:48:10` ✅   nyfed-financial-stability-fetcher    backed up 2769 bytes, concurrency 0, tagged
- `18:48:11` ✅   nyfed-primary-dealer-fetcher         backed up 1692 bytes, concurrency 0, tagged
- `18:48:11` ✅   nyfedapi-isolated                    backed up 4426 bytes, concurrency 0, tagged
- `18:48:12` ✅   ultimate-multi-agent                 backed up 1931 bytes, concurrency 0, tagged
- `18:48:13` ✅ quarantine ledger -> config/quarantine-ledger.json + S3
## RESULT

- `18:48:13` ✗   scorecard probe
