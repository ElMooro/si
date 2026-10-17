# ops 6529 - gentle purge + egress attribution

**Status:** success  
**Duration:** 3303.3s  
**Finished:** 2026-10-17T02:23:01+00:00  

## Log
## Egress / access logs (read-only)

- `01:27:58` log day folders present: ['2026/09/14', '2026/09/15', '2026/09/16', '2026/09/17', '2026/09/18', '2026/09/19', '2026/09/20', '2026/09/21', '2026/09/22', '2026/09/23', '2026/09/24', '2026/09/25', '2026/09/26', '2026/09/27', '2026/09/28', '2026/09/29', '2026/09/30', '2026/10/01', '2026/10/02', '2026/10/03', '2026/10/04', '2026/10/05', '2026/10/06', '2026/10/07', '2026/10/08', '2026/10/09', '2026/10/10', '2026/10/11', '2026/10/12', '2026/10/13', '2026/10/14', '2026/10/15', '2026/10/16', '2026/10/17']
- `01:27:59` target bucket policy allows logging.s3.amazonaws.com: True; encryption {"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}, "BucketKeyEnabled": false, "BlockedEncryptionTypes": {"EncryptionType": ["SSE-C"]}; ownership {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]}
- `01:27:59` newest log day 2026/10/17: 1043 objects, last delivered 2026-10-17T01:27:59+00:00
- `01:28:04` egress sample hour 2026-10-17-00 (1043 log objects): GB by requester [('anonymous', 0.721), ('user/github-actions-justhodl', 0.0)]
- `01:28:04`   GB by client: [('-', 0.464), ('Mozilla', 0.256), ('aws-cli', 0.0), ('Lightpanda', 0.0)]
- `01:28:04`   GB by key prefix: [('data/best-setups.json', 0.147), ('data/etf-desk-research.json', 0.106), ('data/hiring-velocity.json', 0.068), ('data/share-flows.json', 0.053), ('data/earnings-quality.json', 0.04), ('data/risk-gate.json', 0.035), ('data/estimate-revisions.json', 0.033), ('data/buyback-engine.json', 0.028)]
- `01:28:04`   GB by IP /16: [('17.166.*', 0.222), ('104.22.*', 0.177), ('162.158.*', 0.067), ('172.71.*', 0.048), ('104.23.*', 0.04), ('172.68.*', 0.038)]
## Gentle purge

- `01:28:04` day-logs in scope (newest first): [('2026-10-16.jsonl', 26.0), ('2026-10-15.jsonl', 30.8), ('2026-10-14.jsonl', 30.7), ('2026-10-13.jsonl', 33.2), ('2026-10-12.jsonl', 22.0)]
- `02:23:00` 2026-10-16.jsonl: listed 349752, deleted 349394, kept_larger 357, errors 0, pages 350, complete True
- `02:23:00` 2026-10-15.jsonl: listed 653059, deleted 652951, kept_larger 107, errors 0, pages 654, complete True
- `02:23:00` 2026-10-14.jsonl: listed 3213377, deleted 3213370, kept_larger 6, errors 0, pages 3214, complete True
- `02:23:00` 2026-10-13.jsonl: listed 2811297, deleted 2811218, kept_larger 78, errors 0, pages 2812, complete True
- `02:23:00` 2026-10-12.jsonl: listed 1531000, deleted 1530558, kept_larger 441, errors 0, pages 1531, complete False
- `02:23:01` ✅ current day-logs intact: True
- `02:23:01` ✅ deleted 8557491 noncurrent versions this run (errors 0)
- `02:23:01` ✅ wrote aws/ops/reports/6529.json
