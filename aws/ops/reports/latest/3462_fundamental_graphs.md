# ops 3462 — Fundamental Graphs (engine + page + sidebar)

**Status:** success  
**Duration:** 167.3s  
**Finished:** 2026-07-18T16:32:52+00:00  

## Log
- `16:30:04`   zip: 89317 bytes
## 1. Lambda

- `16:30:04`   Lambda missing — creating
- `16:30:10` ✅   ✓ created justhodl-fundamental-graphs
- `16:30:10` ✅   ✓ Function URL: https://fqb6ztg7v6ax4qzylimqjiezmq0kqyyy.lambda-url.us-east-1.on.aws/
- `16:30:10` Function URL: https://fqb6ztg7v6ax4qzylimqjiezmq0kqyyy.lambda-url.us-east-1.on.aws
- `16:30:11` published data/fundgraph/config.json
- `16:30:22` PASS  G1_deploy_url — {'url': True, 'warm': {'CHTR_quarter': {'ok': True, 'n': 44, 'keys': 136}, 'CHTR_annual': {'ok': True, 'n': 12, 'keys': 136}, 'AAPL_quarter': {'ok': True, 'n': 44, 'keys': 138}, 'AAPL_annual': {'ok': True, 'n': 12, 'keys': 138}, 'MSFT_quarter': {'ok': True, 'n': 44, 'keys': 138}, 'MSFT_annual': {'ok': True, 'n': 12, 'keys': 138}}}
- `16:30:22` PASS  G2_aapl_coverage — {'rev_pts': 44, 'first': '2015-06-27', 'keys': 138, 'price_pts': 552, 'missing': []}
- `16:30:22` PASS  G3_chtr_crosscheck — {'last_fq': ['2026-03-31', 5434000000.0], 'n': 44}
- `16:30:23` FAIL  G4_url_cors_gzip — {'status': 200, 'gzip': None, 'acao': None, 'cached': True, 'bytes': 167922}
- `16:32:52` PASS  G5_page_and_sidebar_live — {'page_ok': True, 'manifest_ok': True, 'err': 'HTTP Error 404: Not Found', 'page': 200, 'manifest': 200}
# RESULT: FAILS: ['G4_url_cors_gzip']

- `16:32:52` failed gates: G4_url_cors_gzip
