- `14:13:09` ✅ Restricted factory roles and frozen controls seeded; all existing Brain assets retained.
**Status:** success  
**Duration:** 2.5s  
**Finished:** 2026-09-13T14:13:09+00:00  

## Data

| created_at | endpoints_created | roles | schedule_created | schema_version | seeded_keys | student_cannot | training_jobs_started |
|---|---|---|---|---|---|---|---|
| 2026-09-13T14:13:08+00:00 | 0 | {'student': {'arn': 'arn:aws:iam::857687956942:role/justhodl-student-rsi-role', 'policy_sha256': '0ad3c47a871b09bb18e4d7d5b382c38105d11e3d34d41e3d0d3901788a2307af'}, 'grader': {'arn': 'arn:aws:iam::857687956942:role/justhodl-factory-grader-role', 'policy_sha256': 'e53f2a90cec419b1069d0e33643f344dec84a0f8c85ed8ae65fce0775b8a4db6'}} | False | factory-bootstrap.v1 | ['factory/control/policy.json', 'factory/control/invites.json', 'factory/exams/code-identity-v1.json', 'factory/control/season.json', 'factory/salon/season.json'] | ['iam:*', 'events:*', 'scheduler:*', 'sagemaker:*', 'bedrock:*', 'delete', 'read_hidden_exams', 'write_controls'] | 0 |

## Log

