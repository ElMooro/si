# ops 3145 — Fusion Wave 1 (5 engines, additive)

**Status:** failure  
**Duration:** 1253.3s  
**Finished:** 2026-07-12T05:01:27+00:00  

## Error

```
SystemExit: 1
```

## Data

| bs_earnings_within7d | bs_rows | bs_with_earnings | bs_with_flow_quadrant | bs_with_squeeze | ir_legacy_flow_rows | ir_quadrant_rows | ir_rows | n_fails | n_warns | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | 0 | 0 | 0 |  |  |  |
| 6 | 50 | 37 | 0 | 0 |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 4 | 1 | FAIL |

## Log
## A. industry-rotation ← divergence fields

- `04:40:34`   zip: 73610 bytes
## 1. Lambda

- `04:40:34`   Lambda exists — updating
- `04:40:39` ✅   ✓ updated justhodl-industry-rotation
- `04:40:40` ✅   ✓ Function URL: https://x4cqrcvjdgjkpk2pvx4e6sjtjy0ofxna.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `04:40:40`   rule already correct: industry-rotation-daily (cron(35 21 * * ? *))
- `04:40:40` ✅   ✓ target → justhodl-industry-rotation
- `04:40:41` ✅   ✓ added invoke permission
## 3. Smoke test

- `04:40:41`   invoking justhodl-industry-rotation…
- `04:45:51` ✅ A live: e.g. None → None
## B. best-setups ← earnings + squeeze + IR flow quadrant

- `04:45:52`   zip: 72700 bytes
## 1. Lambda

- `04:45:52`   Lambda exists — updating
- `04:45:55` ✅   ✓ updated justhodl-best-setups
- `04:45:56` ✅   ✓ Function URL: https://42527xww5a3b6fnidjwuugpzpq0rbiox.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `04:45:56` ✅   ✓ updated schedule on justhodl-best-setups-hourly
- `04:45:56` ✅   ✓ target → justhodl-best-setups
- `04:45:57` ✅   ✓ added invoke permission
## 3. Smoke test

- `04:45:57`   invoking justhodl-best-setups…
- `04:46:01` ✅   ✓ smoke test passed
- `04:46:01`     ok                       True
- `04:46:01`     n_setups                 527
- `04:46:01`     strong_buy               3
- `04:46:01`     buy                      15
- `04:46:01`     weight_source            prior-only
- `04:46:02` ✅ B live on 50 setups
## C. master-ranker ← kill-theses + squeeze-fuel overlays

## D. convergence-radar ← early_signals block

- `04:53:07`   zip: 65647 bytes
## 1. Lambda

- `04:54:07`   Lambda exists — updating
- `04:54:13` ✅   ✓ updated justhodl-convergence-radar
- `04:54:13` ✅   ✓ Function URL: https://nrjt2ax6fecycews4sxnqfuhpi0uihax.lambda-url.us-east-1.on.aws/
## 3. Smoke test

- `04:54:13`   invoking justhodl-convergence-radar…
- `04:54:15` ✗   ✗ FunctionError: Unhandled
- `04:54:15`   body: {"errorMessage": "unsupported operand type(s) for +: 'dict' and 'list'", "errorType": "TypeError", "requestId": "c8a8ad89-944e-423e-808b-8a76627ee81c", "stackTrace": ["  File \"/var/task/lambda_function.py\", line 1199, in lambda_handler\n    \"universe_new\": _top((_uni.get(\"threshold_crossers\") or [])\n"]}
## E. alpha-daily-brief ← desk-sheet (smoke off)

- `05:01:22`   zip: 58273 bytes
## 1. Lambda

- `05:01:22`   Lambda exists — updating
- `05:01:25` ✅   ✓ updated justhodl-alpha-daily-brief
- `05:01:26` ✅   ✓ Function URL: https://s2wq5oajenqawpdhh572ocedta0unvdn.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `05:01:26`   rule already correct: justhodl-alpha-daily-brief (cron(30 11 * * ? *))
- `05:01:26` ✅   ✓ target → justhodl-alpha-daily-brief
- `05:01:27` ✅   ✓ added invoke permission
- `05:01:27` ✅ E deployed — desk-sheet block lands in next scheduled brief (LLM run skipped in-op by design)
- `05:01:27` ⚠ B: zero flow quadrants — IR join fields not propagating
- `05:01:27` ✗ A deploy: Read timeout on endpoint URL: "https://lambda.us-east-1.amazonaws.com/2015-03-31/functions/justhodl-industry-rotation/invocations"
- `05:01:27` ✗ C deploy: [Errno 2] No such file or directory: '/home/runner/work/si/si/aws/lambdas/justhodl-master-ranker/config.json'
- `05:01:27` ✗ C: master-ranker.json never freshened
- `05:01:27` ✗ D: convergence-radar.json never freshened
