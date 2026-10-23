# ops 6625 - identity of the direct S3 reader on 17.166.x (read-only), 2026-10-23 18:04 UTC

**Status:** success  
**Duration:** 51.4s  
**Finished:** 2026-10-23T18:05:24+00:00  

## Data

| applebot_signals | direct_lines | distinct_ips |
|---|---|---|
| 113 | 101 | 94 |

## Log
- `18:05:23` log objects today 192521, read 3000
## 1. direct requests from 17.x: status/error, user agents, referers

- `18:05:23` status+error: [('200 -', 101)]
- `18:05:23` UA x101: 200 | Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15 (Applebot/0.1; +http://www.apple.com/go/applebot)
- `18:05:23` Referer x87: 200 | http://justhodl.ai/
- `18:05:23` Referer x14: 200 | https://justhodl.ai/
- `18:05:23` URI x1: 200 | GET /data/forensic-screen.json?_=1792767072869 HTTP/1.1
- `18:05:23` URI x1: 200 | GET /data/industry-rotation.json?_=1792771475485 HTTP/1.1
- `18:05:23` URI x1: 200 | GET /data/forward-orders.json?_=1792743798185 HTTP/1.1
- `18:05:23` URI x1: 200 | GET /data/industry-rotation.json?_=1792732011473 HTTP/1.1
- `18:05:23` URI x1: 200 | GET /data/estimate-revisions.json?_=1792729959466 HTTP/1.1
- `18:05:23` URI x1: 200 | GET /data/insider-radar.json?_=1792750585433 HTTP/1.1
- `18:05:23` URI x1: 200 | GET /data/finra-short.json?_=1792753387491 HTTP/1.1
- `18:05:23` URI x1: 200 | GET /data/insider-radar.json?_=1792765242323 HTTP/1.1
- `18:05:23` URI x1: 200 | GET /data/finra-short.json?_=1792728802190 HTTP/1.1
- `18:05:23` URI x1: 200 | GET /data/finra-short.json?_=1792757673756 HTTP/1.1
- `18:05:23` URI x1: 200 | GET /data/buyback-engine.json?_=1792763388002 HTTP/1.1
- `18:05:23` URI x1: 200 | GET /data/best-setups.json?_=1792715553672 HTTP/1.1
## 2. reverse DNS of the busiest addresses

- `18:05:23` 17.166.25.136 x2 -> 17-166-25-136.applebot.apple.com
- `18:05:23` 17.166.20.77 x2 -> 17-166-20-77.applebot.apple.com
- `18:05:23` 17.166.22.67 x2 -> 17-166-22-67.applebot.apple.com
- `18:05:23` 17.166.236.149 x2 -> 17-166-236-149.applebot.apple.com
- `18:05:23` 17.166.235.235 x2 -> 17-166-235-235.applebot.apple.com
- `18:05:23` 17.166.234.242 x2 -> 17-166-234-242.applebot.apple.com
- `18:05:23` 17.166.232.247 x2 -> 17-166-232-247.applebot.apple.com
- `18:05:23` 17.166.20.251 x1 -> 17-166-20-251.applebot.apple.com
- `18:05:23` 17.166.21.138 x1 -> 17-166-21-138.applebot.apple.com
- `18:05:23` 17.166.22.64 x1 -> 17-166-22-64.applebot.apple.com
- `18:05:23` 17.166.237.234 x1 -> 17-166-237-234.applebot.apple.com
- `18:05:23` 17.166.236.212 x1 -> 17-166-236-212.applebot.apple.com
## 3. live bucket policy statements

- `18:05:24` PublicReadMergedV1 Allow action=s3:GetObject resources=40 condition=null
- `18:05:24` JustHodlDenyDeleteBankedHistory Deny action=['s3:DeleteObject', 's3:DeleteObjectVersion'] resources=4 condition=null
- `18:05:24` Audit20260909PrivatePersonalArtifacts Deny action=['s3:GetObject', 's3:GetObjectVersion'] resources=128 condition={"StringNotEquals": {"aws:PrincipalAccount": "857687956942"}}
- `18:05:24` Audit20260909ImmutableOriginalBackups Deny action=['s3:GetObject', 's3:GetObjectVersion'] resources=1 condition={"StringNotEquals": {"aws:PrincipalAccount": "857687956942"}}
- `18:05:24` Audit20260909PrivateDerivativeHistory Deny action=s3:GetObjectVersion resources=47 condition={"StringNotEquals": {"aws:PrincipalAccount": "857687956942"}}
- `18:05:24` RequireIdentityForFactoryEvidence Deny action=s3:GetObject resources=3 condition={"Null": {"aws:PrincipalArn": "true"}}
- `18:05:24` JustHodlRetiredRetailOperational20260920 Deny action=['s3:GetObject', 's3:GetObjectVersion'] resources=12 condition={"StringNotEquals": {"aws:PrincipalAccount": "857687956942"}}
- `18:05:24` SnapshotCASv1 Deny action=s3:PutObject resources=1 condition={"Null": {"s3:if-none-match": "true", "s3:if-match": "true"}}
- `18:05:24` SnapshotHistoryCASv1 Deny action=s3:PutObject resources=1 condition={"Null": {"s3:if-none-match": "true"}}
- `18:05:24` SnapshotHistoryNoDeletev1 Deny action=['s3:DeleteObject', 's3:DeleteObjectVersion', 's3:ReplicateObject', 's3:ReplicateDelete'] resources=2 condition=null
- `18:05:24` DenyAbusiveScrapersByUserAgent Deny action=s3:GetObject resources=1 condition={"StringLike": {"aws:UserAgent": ["*Lightpanda*", "*lightpanda*"]}}
- `18:05:24` ✅ read-only; nothing changed
