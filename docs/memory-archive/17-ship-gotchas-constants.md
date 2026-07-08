# Memory archive — edit #17 (migrated verbatim 2026-07-08, Khalid-approved memory diet)

Source: Claude memory edit #17. This file is the authoritative archive; the memory slot now holds a one-line pointer here. Grep this directory before building anything fleet-related.

---

SHIP GOTCHAS+CONSTANTS. Sandbox blocks *.amazonaws.com — never run aws CLI/boto3, always ops-script. GH Actions diff=HEAD^ HEAD so split unrelated changes across commits. Use Python zipfile (not zip cmd). Always \n endings — CRLF breaks Lambda. CONSTANTS: role=arn:aws:iam::857687956942:role/lambda-execution-role, runtime=python3.12, region=us-east-1, S3=justhodl-dashboard-live, IAM=github-actions-justhodl. Bootstrap lives in the ⛔ MASTER BOOTSTRAP card (edit #2).
