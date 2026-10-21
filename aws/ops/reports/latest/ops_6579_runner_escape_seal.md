# ops 6579 - attach the runner escape seal (group at the policy limit)

**Status:** success  
**Duration:** 20.9s  
**Finished:** 2026-10-21T00:15:50+00:00  

## Data

| group_policies | unexpected |
|---|---|
| 10 | 0 |

## Log
- `00:15:29` group policies (10): ['justhodl-deny-rogue-compute', 'justhodl-factory-train-ecr', 'justhodl-runner-dlc-pull', 'justhodl-runner-hosting', 'justhodl-runner-hosting-v2', 'justhodl-runner-hosting-v3', 'justhodl-runner-pricing', 'justhodl-runner-processing', 'justhodl-runner-processing-v2', 'justhodl-runner-training-bursts']
- `00:15:29` detached justhodl-runner-training-bursts (rights the sealed deny makes pointless)
- `00:15:30` ✅ attached justhodl-deny-runner-escape
- `00:15:50` runner: denied ['explicitDeny', 'explicitDeny', 'explicitDeny', 'explicitDeny', 'explicitDeny', 'explicitDeny']; allowed-still ['allowed', 'allowed', 'allowed', 'allowed', 'allowed', 'allowed', 'allowed', 'allowed']
- `00:15:50` ✅ runner sealed: no role assumption, no group escape, no trust edits; deploys and ops rights intact
