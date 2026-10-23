# ops 6629 - live proof of the crawler gate (Worker + bucket), read-only

**Status:** success  
**Duration:** 149.4s  
**Finished:** 2026-10-23T18:45:39+00:00  

## Data

| problems |
|---|
| 0 |

## Log
## A. HTTP: crawlers refused, the system's own clients served

- `18:43:10` https://justhodl.ai/data/best-setups.json: crawlers {'Googlebot-smartphone': '403 gate', 'Applebot': '403 gate', 'bingbot': '403 gate', 'GPTBot': '403 other', 'ClaudeBot': '403 other', 'AhrefsBot': '403 gate', 'Bytespider': '403 other', 'Lightpanda': '403 gate'} | must_pass {}
- `18:43:25` https://justhodl.ai/data/bottom.json: crawlers {'Googlebot-smartphone': '403 gate', 'Applebot': '403 gate', 'bingbot': '403 gate', 'GPTBot': '403 other', 'ClaudeBot': '403 other', 'AhrefsBot': '403 gate', 'Bytespider': '403 other', 'Lightpanda': '403 gate'} | must_pass {'ok': 26, 'edge-refused (also on /health, which the gate never touches)': 2}
- `18:43:26` https://justhodl-data-proxy.raafouis.workers.dev/data/best-setups.json: crawlers {'Googlebot-smartphone': '403 gate', 'Applebot': '403 gate', 'bingbot': '403 gate', 'GPTBot': '403 gate', 'ClaudeBot': '403 gate', 'AhrefsBot': '403 gate', 'Bytespider': '403 gate', 'Lightpanda': '403 gate'} | must_pass {}
- `18:43:42` https://justhodl-data-proxy.raafouis.workers.dev/data/bottom.json: crawlers {'Googlebot-smartphone': '403 gate', 'Applebot': '403 gate', 'bingbot': '403 gate', 'GPTBot': '403 gate', 'ClaudeBot': '403 gate', 'AhrefsBot': '403 gate', 'Bytespider': '403 gate', 'Lightpanda': '403 gate'} | must_pass {'ok': 26, 'edge-refused (also on /health, which the gate never touches)': 2}
- `18:43:42` workers.dev /robots.txt for Googlebot -> 200 b'User-agent: *\nDisallow: /\n'
- `18:43:42` workers.dev /health for Googlebot -> 200 gate=None
- `18:43:42` refused by Cloudflare's edge before the Worker (not the gate; the gate-exempt /health is refused too): '' -> 403 on https://justhodl-data-proxy.raafouis.workers.dev/data/bottom.json and 403 on /health: b'{"type":"https://developers.cloudflare.com/support/troublesh'; '' -> 403 on https://justhodl.ai/data/bottom.json and 403 on /health: b'{"type":"https://developers.cloudflare.com/support/troublesh'; 'Python-urllib/3.12' -> 403 on https://justhodl-data-proxy.raafouis.workers.dev/data/bottom.json and 403 on /health: b'{"type":"https://developers.cloudflare.com/support/troublesh'; 'Python-urllib/3.12' -> 403 on https://justhodl.ai/data/bottom.json and 403 on /health: b'{"type":"https://developers.cloudflare.com/support/troublesh'
- `18:43:43` justhodl.ai /robots.txt (GitHub Pages) -> 404, 9379 bytes (informational)
## B. real pages in headless Chrome

- `18:43:43` installing playwright
- `18:44:24` headless Chrome (own agent) /: {"data_responses": 137, "by_status": {"justhodl.ai 200": 78, "workers.dev 200": 55, "s3 200": 4}, "refused_by_gate": 0, "ok_200": 137, "s3_403": 0}
- `18:44:24` headless Chrome (own agent) /why.html?t=NVDA: {"data_responses": 29, "by_status": {"justhodl.ai 200": 9, "s3 200": 11, "workers.dev 200": 9}, "refused_by_gate": 0, "ok_200": 29, "s3_403": 0}
- `18:44:24` headless Chrome (own agent) /master-rank.html: {"data_responses": 11, "by_status": {"justhodl.ai 200": 6, "s3 200": 5}, "refused_by_gate": 0, "ok_200": 11, "s3_403": 0}
- `18:44:24` headless Chrome (own agent) /risk.html: {"data_responses": 7, "by_status": {"justhodl.ai 200": 2, "s3 200": 5}, "refused_by_gate": 0, "ok_200": 7, "s3_403": 0}
- `18:45:02` desktop Chrome agent /: {"data_responses": 141, "by_status": {"justhodl.ai 200": 82, "workers.dev 200": 55, "s3 200": 4}, "refused_by_gate": 0, "ok_200": 141, "s3_403": 0}
- `18:45:02` desktop Chrome agent /why.html?t=NVDA: {"data_responses": 29, "by_status": {"workers.dev 200": 9, "justhodl.ai 200": 9, "s3 200": 11}, "refused_by_gate": 0, "ok_200": 29, "s3_403": 0}
- `18:45:02` desktop Chrome agent /master-rank.html: {"data_responses": 11, "by_status": {"justhodl.ai 200": 6, "s3 200": 5}, "refused_by_gate": 0, "ok_200": 11, "s3_403": 0}
- `18:45:02` desktop Chrome agent /risk.html: {"data_responses": 7, "by_status": {"justhodl.ai 200": 2, "s3 200": 5}, "refused_by_gate": 0, "ok_200": 7, "s3_403": 0}
- `18:45:39` Googlebot agent /: {"data_responses": 107, "by_status": {"justhodl.ai 403 gate": 45, "workers.dev 403 gate": 57, "s3 403": 5}, "refused_by_gate": 102, "ok_200": 0, "s3_403": 5}
- `18:45:39` Googlebot agent /why.html?t=NVDA: {"data_responses": 50, "by_status": {"workers.dev 403 gate": 16, "justhodl.ai 403 gate": 16, "s3 403": 18}, "refused_by_gate": 32, "ok_200": 0, "s3_403": 18}
- `18:45:39` Googlebot agent /master-rank.html: {"data_responses": 13, "by_status": {"justhodl.ai 403 gate": 6, "s3 403": 6, "workers.dev 403 gate": 1}, "refused_by_gate": 7, "ok_200": 0, "s3_403": 6}
- `18:45:39` Googlebot agent /risk.html: {"data_responses": 4, "by_status": {"justhodl.ai 403 gate": 2, "s3 403": 2}, "refused_by_gate": 2, "ok_200": 0, "s3_403": 2}
- `18:45:39` ✅ crawlers refused on both hosts and in a real render; every own client and both browser agents get data
