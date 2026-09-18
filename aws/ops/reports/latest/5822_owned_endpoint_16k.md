# ops 5822 -- owned endpoint context 8k -> 16k

**Status:** failure  
**Duration:** 778.7s  
**Finished:** 2026-09-18T20:13:55+00:00  

## Error

```
SystemExit: 1
```

## Data

| approx_tokens | config | endpoint_status | image | instance | instances_now | max_model_len | model | prompt_chars | rollback_config |
|---|---|---|---|---|---|---|---|---|---|
|  | jh-owned-coder-cfg-20260914-224817 | InService | 6f6c37b935f62cf05e574a292653f18d0d4b3b95 | ml.g5.xlarge | 0 | 8192 | jh-owned-coder-20260914-224817 |  |  |
|  |  |  |  |  |  |  |  |  | jh-owned-coder-cfg-20260914-224817 |
| 27204 |  |  |  |  |  |  |  | 108817 |  |

## Log
## 1. The live recipe

## 2. New model + endpoint config (identical apart from the context)

- `20:00:57` ✅ model jh-owned-coder-16k-20260918200057, config jh-owned-coder-cfg-16k-20260918200057 (max_model_len 16384, prefill 16384, chunked)
## 3. update_endpoint (blue/green) -- previous config kept for rollback

- `20:01:28` t+ 0 min Updating
- `20:01:58` t+ 1 min Updating
- `20:02:28` t+ 1 min Updating
- `20:02:59` t+ 2 min Updating
- `20:03:29` t+ 2 min Updating
- `20:03:59` t+ 3 min Updating
- `20:04:29` t+ 3 min Updating
- `20:05:00` t+ 4 min Updating
- `20:05:30` t+ 4 min Updating
- `20:06:00` t+ 5 min Updating
- `20:06:31` t+ 5 min Updating
- `20:07:01` t+ 6 min Updating
- `20:07:31` t+ 6 min Updating
- `20:08:01` t+ 7 min Updating
- `20:08:31` t+ 7 min Updating
- `20:09:02` t+ 8 min Updating
- `20:09:32` t+ 8 min Updating
- `20:10:02` t+ 9 min Updating
- `20:10:32` t+ 9 min Updating
- `20:11:03` t+10 min Updating
- `20:11:33` t+10 min Updating
- `20:12:03` t+11 min Updating
- `20:12:33` t+11 min Updating
- `20:13:04` t+12 min Updating
- `20:13:34` t+12 min InService
- `20:13:34` ✅ InService on jh-owned-coder-cfg-16k-20260918200057
## 4. Proof: a 12k-token prompt round trip

- `20:13:55` ✗ failure object: {"error":"exception occurred during rolling batch inference","code":424}
