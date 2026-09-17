# ops 5641 -- freeze the anonymized market drills, then sit the holdout exam

**Status:** failure  
**Duration:** 320.5s  
**Finished:** 2026-09-17T19:53:38+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. Freeze

- `19:53:37` {
- `19:53:37`   "status": "written",
- `19:53:37`   "written": 0,
- `19:53:37`   "exists": 235,
- `19:53:37`   "errors": [
- `19:53:37`     "SPY:session_file_missing:2020-02-18",
- `19:53:37`     "QQQ:session_file_missing:2020-02-18",
- `19:53:37`     "IWM:session_file_missing:2020-02-18",
- `19:53:37`     "TLT:session_file_missing:2020-02-18",
- `19:53:37`     "GLD:session_file_missing:2020-02-18"
- `19:53:37`   ],
- `19:53:37`   "per_block": {
- `19:53:37`     "covid-2020": 0,
- `19:53:37`     "svb-2023": 0,
- `19:53:37`     "yen-carry-2024": 0,
- `19:53:37`     "hikes-2022": 230,
- `19:53:37`     "gilt-ldi-2022": 5,
- `19:53:37`     "tariff-2025": 0
- `19:53:37`   },
- `19:53:37`   "manifest_write": "written"
- `19:53:37` }
- `19:53:37` {"counts": {"holdout": 0, "train": 235}, "frozen_at": "2026-09-17T19:53:37.299221Z"}
- `19:53:37` ✅ market drills frozen (or already present)
## 2. Holdout exam

- `19:53:38` {"ok": false, "error": "season weights or owned inference control missing", "season": false, "control": {"adapter_generation": "base", "at": "2026-09-14T23:14:04.245525+00:00", "enabled": true, "endpoint_config": "jh-owned-coder-cfg-20260914-224817", "endpoint_name": "jh-owned-coder-async", "git_sha": "7a63f0d7840f", "image": "763104351884.dkr.ecr.us-east-1.amazonaws.com/djl-inference@sha256:f0f537adb2d8956727ba8a2d6f6c37b935f62cf05e574a292653f18d0d4b3b95", "image_tag": "0.36-lmi28.0.0-cu130-v1", "instance_type": "ml.g5.xlarge", "max_new_tokens": 700, "mode": "async-scale-to-zero", "model_id":
- `19:53:38` ✗ exam exited 2: 
