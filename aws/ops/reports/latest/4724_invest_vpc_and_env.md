# ops 4724 — VPC config, environment, layers on the deployed function

**Status:** success  
**Duration:** 0.4s  
**Finished:** 2026-08-15T21:28:32+00:00  

## Data

| architecture | env_var_names | ephemeral_storage | n_layers | package_type | role | runtime | security_group_ids | subnet_ids | timeout | vpc_id |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  | [] | [] |  | (none) |
|  | ['S3_BUCKET'] |  |  |  |  |  |  |  |  |  |
|  |  |  | 0 |  |  |  |  |  |  |  |
| ['x86_64'] |  | 512 |  | Zip | arn:aws:iam::857687956942:role/lambda-execution-role | python3.12 |  |  | 300 |  |

## Log
## VPC config

- `21:28:32` ✅   no VPC attached -- not the cause
## Environment variables

- `21:28:32`   S3_BUCKET = 'justhodl-dashboard-live'
## Layers

## Other config that could matter

## Verdict

