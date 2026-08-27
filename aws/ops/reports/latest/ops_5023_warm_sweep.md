# ops 5023 -- warm sweep to 2.9.3 (30 majors)

**Status:** failure  
**Duration:** 13.2s  
**Finished:** 2026-08-27T21:11:07+00:00  

## Error

```
SystemExit: warm sweep failures after retry: INTC
```

## Data

| failed | tickers | total_s |
|---|---|---|
| 1 | 30 | 13.2 |

## Log
## P1 threaded builds

- `21:10:56` ✅ AMZN 2.0s 2.9.3
- `21:10:56` ✅ AAPL 2.0s 2.9.3
- `21:10:56` ✅ META 2.0s 2.9.3
- `21:10:56` ✅ GOOGL 2.1s 2.9.3
- `21:10:56` ✅ MSFT 2.1s 2.9.3
- `21:10:56` ✅ NVDA 2.2s 2.9.3
- `21:10:58` ✅ ORCL 2.1s 2.9.3
- `21:10:58` ✅ TSLA 2.2s 2.9.3
- `21:10:58` ✅ PEP 2.1s 2.9.3
- `21:10:58` ✅ AVGO 2.2s 2.9.3
- `21:10:58` ✅ AMD 2.4s 2.9.3
- `21:10:58` ✅ KO 2.5s 2.9.3
- `21:10:59` ✅ V 1.7s 2.9.3
- `21:10:59` ✅ UNH 1.7s 2.9.3
- `21:11:00` ✅ MA 1.9s 2.9.3
- `21:11:00` ✅ XOM 1.8s 2.9.3
- `21:11:00` ✅ JPM 2.0s 2.9.3
- `21:11:00` ✅ LLY 2.3s 2.9.3
- `21:11:01` ✅ PLTR 1.7s 2.9.3
- `21:11:02` ✗ INTC 1.1s None
- `21:11:02` ✅ CRM 1.9s 2.9.3
- `21:11:02` ✅ WMT 2.3s 2.9.3
- `21:11:02` ✅ NFLX 2.3s 2.9.3
- `21:11:02` ✅ COST 2.4s 2.9.3
- `21:11:04` ✅ MU 2.1s 2.9.3
- `21:11:04` ✅ BA 2.1s 2.9.3
- `21:11:04` ✅ AAOI 2.0s 2.9.3
- `21:11:04` ✅ TSM 2.4s 2.9.3
- `21:11:04` ✅ DIS 2.2s 2.9.3
- `21:11:04` ✅ QCOM 2.7s 2.9.3
## P2 sequential retry of failures

- `21:11:07` ✗ retry INTC 0.5s None
