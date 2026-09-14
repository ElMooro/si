# ops 5563 (re-create after the guard fix) -- owned model async endpoint (scale-to-zero) from the staged weights; proven by one round trip

**Status:** success  
**Duration:** 1660.1s  
**Finished:** 2026-09-14T23:14:25+00:00  

## Data

| endpoint | head | instance |
|---|---|---|
| jh-owned-coder-async | 7a63f0d784 | ml.g5.xlarge |

## Log
- `22:46:45` ✅ weights: s3://justhodl-ai-857687956942/factory/models/base/qwen2-5-coder-7b-instruct/c03e6d358207e414f1eca0bb1891e29f1db0e242/ (apache-2.0, 15.2 GB)
- `22:46:46` ✅ hosting rights attached to group justhodl-runner-ecr for github-actions-justhodl; waiting 90 s
- `22:48:16` ✅ hosting price ml.g5.xlarge = $1.4080/h while scaled up; $0 at zero instances
- `22:48:17` ✅ serving image: djl-inference:0.36-lmi28.0.0-cu130-v1 (pushed 2026-08-06) pinned sha256:f0f537adb2d8
- `22:48:17` ✅ model jh-owned-coder-20260914-224817 created from the owned weights
- `22:48:19` ✅ endpoint jh-owned-coder-async creating (async)
- `23:14:01` ✅ endpoint InService
- `23:14:03` ✅ autoscaling 0..1 registered: backlog target + wake-from-zero alarm (scale-in cooldown 15 min)
- `23:14:03` ✅ justhodl-ai execution role: lambda-execution-role
- `23:14:04` ✅ lambda-execution-role may InvokeEndpointAsync on jh-owned-coder-async
- `23:14:05` ✅ probe submitted through factory_inference.submit: req-d3af75c003a3031e635c381f -> s3://justhodl-ai-857687956942/factory/inference/outputs/109833cb-eaf8-4776-9354-55a8ff3f8dfd.out
- `23:14:25` ✅ round trip OK (owned:qwen2-5-coder-7b-instruct@c03e6d358207): Certainly! Below is a Python function `is_palindrome(s)` that checks if a given string `s` is a palindrome, ignoring case and non-letter characters. I've also included two example calls to demonstrate its usage. ```python import re def is_palindrome(s): """ Checks if the given string s is a palindro
- `23:14:25` ✅ factory/control/inference.json written: enabled=True -> the chat routes ordinary and coding questions to the owned model
- `23:14:25` ✅ GREEN -- the owned model is reachable from the chat (async, scale-to-zero)
