# Pull ath.html from S3 → commit directly

**Status:** success  
**Duration:** 0.2s  
**Finished:** 2026-04-26T00:39:26+00:00  

## Data

| http_subs | n_api_urls | n_http_urls | n_lambda_urls | original_size | patched_size |
|---|---|---|---|---|---|
| 1 | 0 | 1 | 0 | 15910 | 15893 |

## Log
## A. Fetch from S3

- `00:39:26`   Pulled 15910B  mod=2026-02-26 06:57:36+00:00
## B. Inspect for HTTP / dead-URL references

- `00:39:26`   HTTP (non-HTTPS) URLs: 1
- `00:39:26`     http://justhodl-dashboard-live.s3-website-us-east-1.amazonaws.com
- `00:39:26` 
  API Gateway URLs: 0
- `00:39:26` 
  Lambda Function URLs: 0
- `00:39:26` 
  S3 website endpoint refs: 1
- `00:39:26`     s3-website-us-east-1.amazonaws.com
## C. Patch problematic URLs

- `00:39:26`   Patched 1 HTTP S3 website → HTTPS REST endpoints
## D. Write to repo

- `00:39:26`   Wrote 15893B to /home/runner/work/si/si/ath.html
- `00:39:26`   GitHub Actions will auto-commit on success.
- `00:39:26`   GitHub Pages will serve the new ath.html directly from /ath.html
- `00:39:26` Done
