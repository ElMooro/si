# ops 3474 — symdir ranked search + memory-archive 34

**Status:** success  
**Duration:** 14.8s  
**Finished:** 2026-07-18T21:38:27+00:00  

## Log
- `21:38:12`   zip: 94822 bytes
## 1. Lambda

- `21:38:12`   Lambda exists — updating
- `21:38:17` ✅   ✓ updated justhodl-fundamental-graphs
- `21:38:24` symdir diag: [["company-screener?marketCapMoreThan=2000000000&limit=10000", "raw=5462 kept=2683"], ["company-screener?marketCapMoreThan=5000000&marketCapLowerThan=2000000000&limit=10000", "raw=10000 kept=4657"], ["stock-list", "raw=90758 kept=0 keys=['companyName', 'symbol']"]]
- `21:38:26` PASS  D1_symdir_built — {'rows': 7340, 'built': 7340, 'sample': [['NVDA', 'NVIDIA Corporation', 'NASDAQ', 4912261010000.0], ['AAPL', 'Apple Inc.', 'NASDAQ', 4901758191440.0], ['GOOGL', 'Alphabet Inc.', 'NASDAQ', 4194138171504.0]]}
- `21:38:26` PASS  D2_relevance_quartet — {'micro': ['MSFT', 'MU', 'MCHP', 'STRF'], 'apple': ['AAPL', 'APLE', 'MLP'], 'tesla': ['TSLA'], 'berkshire': ['BRK-B', 'BRK-A', 'OMAH'], 'src': 'symdir'}
- `21:38:27` PASS  D3_engine_regression — {'keys': 197, 'version': '1.1.1'}
# RESULT: ALL PASS

