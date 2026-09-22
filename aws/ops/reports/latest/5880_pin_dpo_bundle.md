# ops 5880 -- pin the dpo-capable trainer; count pairs; train_mode=auto

**Status:** success  
**Duration:** 157.2s  
**Finished:** 2026-09-22T14:26:21+00:00  

## Data

| current_bundle | image | pairs_estimate | require_digest | sampled_kinds | sampled_pairs | trace_objects_private | verified_objects |
|---|---|---|---|---|---|---|---|
| ng/bundles/train-b64e3b4bc006dde1.tar.gz | be47f293d0cb62b0e4b3f4e69aab6e74558f38b45c9ff5f67bd143366e86 |  | False |  |  |  |  |
|  |  | 0 |  | {"public_benchmark_train": 692, "self_trace": 1036} | 0 |  | 5185 |
|  |  |  |  |  |  | 1 |  |

## Log
- `14:23:45`   "bundle": "written",
- `14:23:45`   "bundle_key": "factory/training/bundles/train-cdae308108d808aa.tar.gz",
- `14:23:45`   "bundle_sha256": "cdae308108d808aa3f4ef028c3d91bc0fa14afdfe733d4a9dea4dca6598fc8b0",
- `14:23:45`   "pin_key": "factory/training/current.json",
- `14:23:45`   "image": "857687956942.dkr.ecr.us-east-1.amazonaws.com/justhodl/factory-train:hf-pt2.3-tf4.46-cu121@sha256:39b1be47f293d0cb62b0e4b3f4e69aab6e74558f38b45c9ff5f67bd143366e86"
- `14:23:45` }
- `14:23:45` ✅ pinned bundle cdae308108d8 (was b64e3b4bc006)
## 2. Preference pairs available

## 3. Control

- `14:26:21` ✅ train_mode=auto, min_pairs=50
