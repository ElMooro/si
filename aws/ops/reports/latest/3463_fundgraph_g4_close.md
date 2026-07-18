# ops 3463 — Fundamental Graphs G4 close (CORS + gzip)

**Status:** success  
**Duration:** 10.9s  
**Finished:** 2026-07-18T16:35:16+00:00  

## Log
- `16:35:06` current Cors: {"AllowCredentials": false, "AllowHeaders": ["content-type"], "AllowMethods": ["*"], "AllowOrigins": ["*"], "MaxAge": 86400}
- `16:35:06` PASS  H1_url_cors_config — {'url': 'https://fqb6ztg7v6ax4qzylimqjiezmq0kqyyy.lambda-url.us-east-1.on.aws', 'had': ['*']}
- `16:35:06`   zip: 89383 bytes
## 1. Lambda

- `16:35:06`   Lambda exists — updating
- `16:35:12` ✅   ✓ updated justhodl-fundamental-graphs
- `16:35:16` PASS  H2_cors_star — {'acao': '*', 'enc': 'gzip', 'wire': 35896, 'plain': 167922}
- `16:35:16` PASS  H3_gzip_header_path — {'acao': '*', 'enc': 'gzip', 'wire': 35896, 'plain': 167922, 'version': '1.0.0'}
- `16:35:16` PASS  H4_gz_query_force — {'acao': '*', 'enc': 'gzip', 'wire': 31809, 'plain': 160665, 'sym': 'CHTR'}
# RESULT: ALL PASS

