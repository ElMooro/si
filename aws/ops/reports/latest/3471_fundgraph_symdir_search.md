# ops 3471 — symdir ranked search + memory-archive 34

**Status:** success  
**Duration:** 20.7s  
**Finished:** 2026-07-18T21:31:51+00:00  

## Log
- `21:31:31`   zip: 94264 bytes
## 1. Lambda

- `21:31:31`   Lambda exists — updating
- `21:31:36` ✅   ✓ updated justhodl-fundamental-graphs
- `21:31:44` FAIL  D1_symdir_built — An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `21:31:51` FAIL  D2_relevance_quartet — {'micro': ['PETSUSD', 'TICKUSD', 'MCRT', 'AMMUSD'], 'apple': ['AAPL', 'APPLX', 'APUSD'], 'tesla': ['TSLA', 'TXLZF', 'DTSLAUSD'], 'berkshire': ['BGRY', 'BFOCX', 'BGRYW'], 'src': None}
- `21:31:51` PASS  D3_engine_regression — {'keys': 197, 'version': '1.1.1'}
# RESULT: FAILS: ['D1_symdir_built', 'D2_relevance_quartet']

