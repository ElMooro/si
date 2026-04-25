# S3 public-read audit — why does repo-data.json 403?

**Status:** success  
**Duration:** 1.0s  
**Finished:** 2026-04-25T00:07:43+00:00  

## Log
## A. Bucket policy

- `00:07:42` {
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PublicReadDataDir",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::justhodl-dashboard-live/data/*"
    },
    {
      "Sid": "PublicReadScreener",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::justhodl-dashboard-live/screener/*"
    },
    {
      "Sid": "PublicReadSentiment",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::justhodl-dashboard-live/sentiment/*"
    },
    {
      "Sid": "PublicReadRootDashboardFiles",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": [
        "arn:aws:s3:::justhodl-dashboard-live/flow-data.json",
        "arn:aws:s3:::justhodl-dashboard-live/crypto-intel.json"
      ]
    }
  ]
}
## B. Public access block config

- `00:07:43`   {'BlockPublicAcls': False, 'IgnorePublicAcls': False, 'BlockPublicPolicy': False, 'RestrictPublicBuckets': False}
## C. HTTP test for each data file the Lambda needs

- `00:07:43`   ✓ data/report.json                         HTTP 200 — 1725000 bytes
- `00:07:43`   ✗ repo-data.json                           HTTP 403 
- `00:07:43`   ✗ edge-data.json                           HTTP 403 
- `00:07:43`   ✓ flow-data.json                           HTTP 200 — 31517 bytes
- `00:07:43`   ✗ predictions.json                         HTTP 403 
- `00:07:43`   ✗ intelligence-report.json                 HTTP 403 
## D. Per-object ACLs

- `00:07:43`   data/report.json                         grants=1 public_read=False
- `00:07:43`   repo-data.json                           grants=1 public_read=False
- `00:07:43`   edge-data.json                           grants=1 public_read=False
- `00:07:43` Done
