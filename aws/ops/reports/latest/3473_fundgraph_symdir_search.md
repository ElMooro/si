# ops 3473 — symdir ranked search + memory-archive 34

**Status:** success  
**Duration:** 14.3s  
**Finished:** 2026-07-18T21:36:22+00:00  

## Log
- `21:36:07`   zip: 94724 bytes
## 1. Lambda

- `21:36:08`   Lambda exists — updating
- `21:36:13` ✅   ✓ updated justhodl-fundamental-graphs
- `21:36:19` symdir diag: [["company-screener?marketCapMoreThan=2000000000&limit=10000", "raw=5462 kept=2683"], ["company-screener?marketCapMoreThan=5000000&marketCapLowerThan=2000000000&limit=10000", "raw=10000 kept=4657"], ["stock-list", "raw=90758 kept=0 keys=['companyName', 'symbol']"]]
- `21:36:21` PASS  D1_symdir_built — {'rows': 7340, 'built': 7340, 'sample': [['NVDA', 'NVIDIA Corporation', 'NASDAQ'], ['AAPL', 'Apple Inc.', 'NASDAQ'], ['GOOGL', 'Alphabet Inc.', 'NASDAQ']]}
- `21:36:21` FAIL  D2_relevance_quartet — {'micro': ['MU', 'MBOT', 'MCHP', 'MLGO'], 'apple': ['AAPL', 'APLE', 'MLP'], 'tesla': ['TSLA'], 'berkshire': ['BRK-A', 'BRK-B', 'OMAH'], 'src': 'symdir'}
- `21:36:22` PASS  D3_engine_regression — {'keys': 197, 'version': '1.1.1'}
# RESULT: FAILS: ['D2_relevance_quartet']

