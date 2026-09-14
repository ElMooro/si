# ops 5556 (re-arm of 5554) -- owned model async endpoint (scale-to-zero) from the staged weights; proven by one round trip

**Status:** success  
**Duration:** 871.0s  
**Finished:** 2026-09-14T17:37:07+00:00  

## Data

| endpoint | head | instance |
|---|---|---|
| jh-owned-coder-async | c4aff69f18 | ml.g5.xlarge |

## Log
- `17:22:36` ✅ weights: s3://justhodl-ai-857687956942/factory/models/base/qwen2-5-coder-7b-instruct/c03e6d358207e414f1eca0bb1891e29f1db0e242/ (apache-2.0, 15.2 GB)
- `17:22:37` ✅ hosting rights attached to group justhodl-runner-ecr for github-actions-justhodl; waiting 90 s
- `17:24:07` ✅ hosting price ml.g5.xlarge = $1.4080/h while scaled up; $0 at zero instances
- `17:24:07` ✅ serving image: djl-inference:0.36-lmi28.0.0-cu130-v1 (pushed 2026-08-06) pinned sha256:f0f537adb2d8
- `17:24:08` ✅ model jh-owned-coder-20260914-172407 created from the owned weights
- `17:24:10` ✅ endpoint jh-owned-coder-async creating (async)
- `17:36:44` ✅ endpoint InService
- `17:36:46` ✅ autoscaling 0..1 registered: backlog target + wake-from-zero alarm (scale-in cooldown 15 min)
- `17:36:46` ✅ justhodl-ai execution role: lambda-execution-role
- `17:36:46` ✅ lambda-execution-role may InvokeEndpointAsync on jh-owned-coder-async
- `17:36:47` ✅ probe submitted through factory_inference.submit: req-8c9c72f440c6ada8366c798d -> s3://justhodl-ai-857687956942/factory/inference/outputs/0db5dde8-b2b2-4072-9550-2f0583ef5f11.out
- `17:37:07` ✅ round trip OK (owned:qwen2-5-coder-7b-instruct@c03e6d358207): Certainly! Below is a Python function `is_palindrome(s)` that checks if a given string `s` is a palindrome, ignoring case and non-letter characters. I've also included two example calls to demonstrate its usage. ```python import re def is_palindrome(s): # Remove non-letter characters and convert to 
- `17:37:07` ✅ factory/control/inference.json written: enabled=True -> the chat routes ordinary and coding questions to the owned model
- `17:37:07` ✅ GREEN -- the owned model is reachable from the chat (async, scale-to-zero)
