# ops 5551 -- owned model async endpoint (scale-to-zero) from the staged weights; proven by one round trip

**Status:** success  
**Duration:** 1607.1s  
**Finished:** 2026-09-14T03:54:21+00:00  

## Data

| endpoint | head | instance |
|---|---|---|
| jh-owned-coder-async | 32124eb6cf | ml.g5.xlarge |

## Log
- `03:27:34` ✅ weights: s3://justhodl-ai-857687956942/factory/models/base/qwen2-5-coder-7b-instruct/c03e6d358207e414f1eca0bb1891e29f1db0e242/ (apache-2.0, 15.2 GB)
- `03:27:35` ✅ hosting rights attached to group justhodl-runner-ecr for github-actions-justhodl; waiting 90 s
- `03:29:05` ✅ hosting price ml.g5.xlarge = $1.4080/h while scaled up; $0 at zero instances
- `03:29:06` ✅ serving image: djl-inference:0.36-lmi28.0.0-cu130-v1 (pushed 2026-08-06) pinned sha256:f0f537adb2d8
- `03:29:07` ✅ model jh-owned-coder-20260914-032906 created from the owned weights
- `03:29:08` ✅ endpoint jh-owned-coder-async creating (async)
- `03:54:20` ✗ endpoint status Creating (); deleting so nothing bills
- `03:54:21` ✗ RED -- endpoint
