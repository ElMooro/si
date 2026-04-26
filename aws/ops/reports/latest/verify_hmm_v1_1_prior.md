# Phase 9.2 v1.1 — verify Dirichlet prior fixed state collapse

**Status:** success  
**Duration:** 13.1s  
**Finished:** 2026-04-26T15:17:03+00:00  

## Log
## 1. Lambda metadata

- `15:16:50`   CodeSha256:   uzgaO50EEhj3Rse+aLphO9SB0uk+mPZpj+pBJDCmMf4=
- `15:16:50`   LastModified: 2026-04-26T15:16:10.000+0000
- `15:16:50`   Runtime:      python3.12
- `15:16:50`   Timeout:      240s
- `15:16:50`   Memory:       1024MB
## 2. Manual invoke (forcing fresh fit)

- `15:17:00`   ✅ OK (9.5s)
- `15:17:00`   payload summary: {"statusCode": 200, "body": "{\"status\": \"ok\", \"elapsed_sec\": 8.53, \"ka_index_n_obs\": 200, \"hmm_state\": \"CRISIS\", \"n_anomalies\": 0, \"anomaly_score\": 0, \"s3_key\": \"data/regime-anomaly.json\"}"}
## 3. Read s3://.../data/regime-anomaly.json

- `15:17:03`   age=2.5s
## 4. State probability sanity

- `15:17:03`   current state: CRISIS
- `15:17:03`   probabilities: {'EXPANSION': 0.0, 'LATE_CYCLE': 0.0, 'CONTRACTION': 0.0, 'CRISIS': 1.0}
- `15:17:03` ⚠   ⚠ state collapse persists — one state has 1.000, others ~0
- `15:17:03` ⚠   Prior may need to be increased further or training data is genuinely uniform
## 5. Transition matrix sanity

- `15:17:03`   EXPANSION: row_sum=0.9990, diag=0.810
- `15:17:03`   LATE_CYCLE: row_sum=0.9990, diag=0.810
- `15:17:03`   CONTRACTION: row_sum=0.9990, diag=0.810
- `15:17:03`   CRISIS: row_sum=0.9990, diag=0.864
## 6. State means + std spread

- `15:17:03`   means:  {'EXPANSION': 0.0, 'LATE_CYCLE': 0.0, 'CONTRACTION': 0.0, 'CRISIS': 43.0}
- `15:17:03`   stds:   {'EXPANSION': 0.94, 'LATE_CYCLE': 0.94, 'CONTRACTION': 0.94, 'CRISIS': 0.94}
- `15:17:03`   ✅ state means show separation
## 7. Anomaly engine status

- `15:17:03`   per_signal count: 0
- `15:17:03`   n_anomalies:      0
- `15:17:03`   composite score:  0
## FINAL

- `15:17:03`   HMM v1.1 deploy: 2026-04-26T15:16:10.000+0000
- `15:17:03`   Sample size:      200
- `15:17:03`   Warming up:       False
- `15:17:03`   Result: NEEDS MORE WORK
- `15:17:03` Done
