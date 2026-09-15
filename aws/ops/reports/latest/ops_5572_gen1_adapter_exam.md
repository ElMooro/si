# ops 5572 -- generation 1: wait for training, extract the adapter, launch the frozen exam with it

**Status:** failure  
**Duration:** 663.0s  
**Finished:** 2026-09-15T02:58:38+00:00  

## Data

| head | train_job |
|---|---|
| 168fe375fe | jh-gearb-gen1-20260915-024201 |

## Log
- `02:58:38` ✅ training job jh-gearb-gen1-20260915-024201 status=Failed secondary=Failed train_s=305 billable_s=215 reason=AlgorithmError: ExecuteUserScriptError:
ExitCode 3
ErrorMessage ""
Command "/opt/conda/bin/python3.11 train_qlora.py --base_manifest_sha256 8cacbc1d7868e9114ce500d37a6613740c1e2e393fdc68701342820b2eaf
- `02:58:38` ✗ generation 1 is not complete (Failed); re-arm when it is -- nothing launched
