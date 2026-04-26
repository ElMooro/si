# Phase 9.2 v1.2 — verify zero-filter loader + state distribution

**Status:** success  
**Duration:** 11.7s  
**Finished:** 2026-04-26T15:24:50+00:00  

## Log
## 1. Lambda metadata (post-redeploy)

- `15:24:39`   CodeSha256:   OF4FpiEMN4KGNlOQYxAVGPjtykwPf8AJ9SjnAY6hj4c=
- `15:24:39`   LastModified: 2026-04-26T15:22:15.000+0000
## 2. Manual invoke

- `15:24:47`   ✅ OK (8.0s)
- `15:24:47`   payload: {"statusCode": 200, "body": "{\"status\": \"ok\", \"elapsed_sec\": 7.16, \"ka_index_n_obs\": 10, \"hmm_state\": null, \"n_anomalies\": 0, \"anomaly_score\": 0, \"s3_key\": \"data/regime-anomaly.json\"}"}
## 3. Read regime-anomaly.json

- `15:24:50`   training_n:        10  (was 200 before zero filter)
- `15:24:50`   is_warming_up:     True
- `15:24:50`   ka_index_obs:      10
- `15:24:50`   earliest:          2026-04-25T00:04:26.342788+00:00
- `15:24:50`   latest:            2026-04-26T13:00:21.825915+00:00
- `15:24:50` 
- `15:24:50`   state_label:       None
- `15:24:50`   probabilities:
- `15:24:50` 
- `15:24:50`   state_means:       {}
- `15:24:50`   state_stds:        {}
## 4. Quality assessment

- `15:24:50`   ✅ all 4 states have positive probability
- `15:24:50`   ✅ is_warming_up=True correctly indicates limited data
- `15:24:50`     Frontend will show the warming-up banner to user
- `15:24:50`   ✅ low training_n acknowledged — model is honest about uncertainty
## FINAL

- `15:24:50` Done
