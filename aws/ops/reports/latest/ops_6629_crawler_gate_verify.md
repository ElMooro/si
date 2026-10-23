# ops 6629 - live proof of the crawler gate (Worker + bucket), read-only

**Status:** failure  
**Duration:** 133.8s  
**Finished:** 2026-10-23T18:41:12+00:00  

## Error

```
SystemExit: 1
```

## Data

| problems |
|---|
| 10 |

## Log
## A. HTTP: crawlers refused, the system's own clients served

- `18:38:59` https://justhodl.ai/data/best-setups.json: crawlers {'Googlebot-smartphone': 403, 'Applebot': 403, 'bingbot': 403, 'GPTBot': 403, 'ClaudeBot': 403, 'AhrefsBot': 403, 'Bytespider': 403, 'Lightpanda': 403} | must_pass {}
- `18:39:03` https://justhodl.ai/data/bottom.json: crawlers {'Googlebot-smartphone': 403, 'Applebot': 403, 'bingbot': 403, 'GPTBot': 403, 'ClaudeBot': 403, 'AhrefsBot': 403, 'Bytespider': 403, 'Lightpanda': 403} | must_pass {'ok': 26, '403 None': 2}
- `18:39:04` https://justhodl-data-proxy.raafouis.workers.dev/data/best-setups.json: crawlers {'Googlebot-smartphone': 403, 'Applebot': 403, 'bingbot': 403, 'GPTBot': 403, 'ClaudeBot': 403, 'AhrefsBot': 403, 'Bytespider': 403, 'Lightpanda': 403} | must_pass {}
- `18:39:07` https://justhodl-data-proxy.raafouis.workers.dev/data/bottom.json: crawlers {'Googlebot-smartphone': 403, 'Applebot': 403, 'bingbot': 403, 'GPTBot': 403, 'ClaudeBot': 403, 'AhrefsBot': 403, 'Bytespider': 403, 'Lightpanda': 403} | must_pass {'ok': 26, '403 None': 2}
- `18:39:07` workers.dev /robots.txt for Googlebot -> 200 b'User-agent: *\nDisallow: /\n'
- `18:39:07` workers.dev /health for Googlebot -> 200 gate=None
- `18:39:07` justhodl.ai /robots.txt (GitHub Pages) -> 403, 701 bytes (informational)
## B. real pages in headless Chrome

- `18:39:07` installing playwright
- `18:39:57` headless Chrome (own agent) /: {"data_responses": 135, "by_status": {"justhodl.ai 200": 77, "workers.dev 200": 54, "s3 200": 4}, "refused_by_gate": 0, "ok_200": 135, "s3_403": 0}
- `18:39:57` headless Chrome (own agent) /why.html?t=NVDA: {"data_responses": 29, "by_status": {"justhodl.ai 200": 9, "workers.dev 200": 9, "s3 200": 11}, "refused_by_gate": 0, "ok_200": 29, "s3_403": 0}
- `18:39:57` headless Chrome (own agent) /master-rank.html: {"data_responses": 11, "by_status": {"justhodl.ai 200": 6, "s3 200": 5}, "refused_by_gate": 0, "ok_200": 11, "s3_403": 0}
- `18:39:57` headless Chrome (own agent) /risk.html: {"data_responses": 7, "by_status": {"justhodl.ai 200": 2, "s3 200": 5}, "refused_by_gate": 0, "ok_200": 7, "s3_403": 0}
- `18:40:35` desktop Chrome agent /: {"data_responses": 140, "by_status": {"justhodl.ai 200": 81, "workers.dev 200": 55, "s3 200": 4}, "refused_by_gate": 0, "ok_200": 140, "s3_403": 0}
- `18:40:35` desktop Chrome agent /why.html?t=NVDA: {"data_responses": 29, "by_status": {"workers.dev 200": 9, "justhodl.ai 200": 9, "s3 200": 11}, "refused_by_gate": 0, "ok_200": 29, "s3_403": 0}
- `18:40:35` desktop Chrome agent /master-rank.html: {"data_responses": 11, "by_status": {"justhodl.ai 200": 6, "s3 200": 5}, "refused_by_gate": 0, "ok_200": 11, "s3_403": 0}
- `18:40:35` desktop Chrome agent /risk.html: {"data_responses": 7, "by_status": {"justhodl.ai 200": 2, "s3 200": 5}, "refused_by_gate": 0, "ok_200": 7, "s3_403": 0}
- `18:41:12` Googlebot agent /: {"data_responses": 107, "by_status": {"justhodl.ai 403 gate": 45, "workers.dev 403 gate": 57, "s3 403": 5}, "refused_by_gate": 102, "ok_200": 0, "s3_403": 5}
- `18:41:12` Googlebot agent /why.html?t=NVDA: {"data_responses": 50, "by_status": {"workers.dev 403 gate": 16, "justhodl.ai 403 gate": 16, "s3 403": 18}, "refused_by_gate": 32, "ok_200": 0, "s3_403": 18}
- `18:41:12` Googlebot agent /master-rank.html: {"data_responses": 13, "by_status": {"justhodl.ai 403 gate": 6, "workers.dev 403 gate": 1, "s3 403": 6}, "refused_by_gate": 7, "ok_200": 0, "s3_403": 6}
- `18:41:12` Googlebot agent /risk.html: {"data_responses": 4, "by_status": {"justhodl.ai 403 gate": 2, "s3 403": 2}, "refused_by_gate": 2, "ok_200": 0, "s3_403": 2}
- `18:41:12` ⚠ crawler GPTBot on https://justhodl.ai/data/best-setups.json -> 403 None
- `18:41:12` ⚠ crawler ClaudeBot on https://justhodl.ai/data/best-setups.json -> 403 None
- `18:41:12` ⚠ crawler Bytespider on https://justhodl.ai/data/best-setups.json -> 403 None
- `18:41:12` ⚠ crawler GPTBot on https://justhodl.ai/data/bottom.json -> 403 None
- `18:41:12` ⚠ crawler ClaudeBot on https://justhodl.ai/data/bottom.json -> 403 None
- `18:41:12` ⚠ crawler Bytespider on https://justhodl.ai/data/bottom.json -> 403 None
- `18:41:12` ⚠ must_pass 'Python-urllib/3.12' on https://justhodl.ai/data/bottom.json -> 403 None
- `18:41:12` ⚠ must_pass '' on https://justhodl.ai/data/bottom.json -> 403 None
- `18:41:12` ⚠ must_pass 'Python-urllib/3.12' on https://justhodl-data-proxy.raafouis.workers.dev/data/bottom.json -> 403 None
- `18:41:12` ⚠ must_pass '' on https://justhodl-data-proxy.raafouis.workers.dev/data/bottom.json -> 403 None
- `18:41:12` ✗ 10 problem(s)
