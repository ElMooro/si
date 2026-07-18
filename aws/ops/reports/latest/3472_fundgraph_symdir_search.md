# ops 3472 — symdir ranked search + memory-archive 34

**Status:** success  
**Duration:** 23.1s  
**Finished:** 2026-07-18T21:34:18+00:00  

## Log
- `21:33:55`   zip: 94614 bytes
## 1. Lambda

- `21:33:55`   Lambda exists — updating
- `21:34:01` ✅   ✓ updated justhodl-fundamental-graphs
- `21:34:14` symdir diag: [["stock-list", "raw=90758 kept=0"], ["stock/list", "ERR fetch failed: HTTP Error 404: Not Found"], ["available-traded/list", "ERR fetch failed: HTTP Error 404: Not Found"], ["company-screener?marketCapMoreThan=10000000&limit=10000", "raw=10000 kept=4705"]]
- `21:34:16` FAIL  D1_symdir_built — {'rows': 4705, 'built': 4705, 'sample': [['NVDA', 'NVIDIA Corporation', 'NASDAQ'], ['AAPL', 'Apple Inc.', 'NASDAQ'], ['GOOGL', 'Alphabet Inc.', 'NASDAQ']]}
- `21:34:17` PASS  D2_relevance_quartet — {'micro': ['MU', 'MCHP', 'MSFT', 'STRF'], 'apple': ['AAPL', 'APLE', 'MLP'], 'tesla': ['TSLA'], 'berkshire': ['BRK-A', 'BRK-B', 'OMAH'], 'src': 'symdir'}
- `21:34:18` PASS  D3_engine_regression — {'keys': 197, 'version': '1.1.1'}
# RESULT: FAILS: ['D1_symdir_built']

