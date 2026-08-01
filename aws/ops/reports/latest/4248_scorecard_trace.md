# ops 4248 — scorecard: read the logs

**Status:** success  
**Duration:** 8.6s  
**Finished:** 2026-08-01T18:56:53+00:00  

## Data

| generated_at | has_ssm_writes | memory | section | ssm_ok | timeout |
|---|---|---|---|---|---|
|  |  | 1024 | config |  | 900 |
| 2026-08-01T18:53:06.833714+00:00 | False |  | artifact | None |  |

## Log
## 1. Confirm what code is actually running

- `18:56:45` timeout=900s memory=1024MB modified=2026-08-01T18:52:32.000+0000
## 2. Most recent run — the whole stream, not one line

- `18:56:45` --- stream $LATEST]727bb3b492564d4bb185a581eac5b0c5 ---
- `18:56:45`    INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89a3cfcfffeca7bc
- `18:56:45`    START RequestId: da6b1e5b-18d8-402a-b106-57a1c4f3504d Version: $LATEST
- `18:56:45`    [signal-scorecard] starting 2026-08-01T18:52:37.841041+00:00
- `18:56:45`    [signal-scorecard] scanned 101095 outcome records
- `18:56:45`    [scorecard] SPY history points: 296
- `18:56:45`    END RequestId: da6b1e5b-18d8-402a-b106-57a1c4f3504d
- `18:56:45`    REPORT RequestId: da6b1e5b-18d8-402a-b106-57a1c4f3504d	Duration: 29390.51 ms	Billed Duration: 29964 ms	Memory Size: 1024 MB	Max Memory Used: 319 MB	Init Duration: 573.49 ms	
XRAY TraceId: 1-6a6e4075-1
- `18:56:45` --- stream $LATEST]28b641d44fb84209a39984e07dc4572c ---
- `18:56:45`    INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89a3cfcfffeca7bc
- `18:56:45`    START RequestId: e39c9ae7-b6da-4773-9307-99f759eb4404 Version: $LATEST
- `18:56:45`    [signal-scorecard] starting 2026-08-01T18:46:06.267447+00:00
- `18:56:45`    END RequestId: e39c9ae7-b6da-4773-9307-99f759eb4404
- `18:56:45`    REPORT RequestId: e39c9ae7-b6da-4773-9307-99f759eb4404	Duration: 120000.00 ms	Billed Duration: 120779 ms	Memory Size: 256 MB	Max Memory Used: 256 MB	Init Duration: 778.85 ms	Status: timeout
XRAY Trace
## 3. Explicit ERROR filter across 24h

- `18:56:46` matching lines in 24h: 0
- `18:56:46` ✅ no ERROR lines in 24h — the handler is completing; the missing ssm_writes key is a code-path question, not a crash
## 4. What the artifact says now

- `18:56:46` generated_at=2026-08-01T18:53:06.833714+00:00 keys=22
- `18:56:46` has ssm_writes: False   ssm_ok: None
- `18:56:46` top-level keys: alpha, avg_graded_wilson_lb, data_quality_flags, deprecated_signals, elapsed_s, generated_at, interpretation, method, n_deprecated, n_insufficient, n_outcomes_legacy, n_outcomes_neutral, n_outcomes_scanned, n_outcomes_scored, n_outcomes_unresolved, n_promoted, n_signals_graded, n_signals_tracked, pr
- `18:56:46` artifact age: 0.00 days
## 5. Who consumes this artifact

- `18:56:53` functions naming the scorecard in their env: 0
- `18:56:53` ⚠ The scorecard artifact was 5 DAYS STALE before ops 4247. Anything reading it — calibrator, best-setups, master-ranker — was scoring on week-old truth and had no way to know. This is precisely the failure the contract gate exists to catch, and its STALE bound should have fired: worth checking why it did not.
## RESULT

- `18:56:53` ✅ OPS 4248 — diagnostic complete, no changes made
