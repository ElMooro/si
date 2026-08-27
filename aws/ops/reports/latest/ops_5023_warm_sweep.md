# ops 5023 -- warm sweep to 2.9.3 (30 majors)

**Status:** failure  
**Duration:** 16.8s  
**Finished:** 2026-08-27T21:16:16+00:00  

## Error

```
SystemExit: warm sweep failures after retry: INTC
```

## Data

| failed | tickers | total_s |
|---|---|---|
| 1 | 30 | 16.8 |

## Log
## P1 threaded builds

- `21:16:02` ✅ GOOGL 2.2s 2.9.3
- `21:16:02` ✅ MSFT 2.2s 2.9.3
- `21:16:02` ✅ AMZN 2.2s 2.9.3
- `21:16:02` ✅ AAPL 2.3s 2.9.3
- `21:16:02` ✅ META 2.4s 2.9.3
- `21:16:02` ✅ NVDA 2.5s 2.9.3
- `21:16:04` ✅ ORCL 2.3s 2.9.3
- `21:16:04` ✅ TSLA 2.6s 2.9.3
- `21:16:04` ✅ AMD 2.6s 2.9.3
- `21:16:04` ✅ AVGO 2.7s 2.9.3
- `21:16:04` ✅ PEP 2.5s 2.9.3
- `21:16:05` ✅ KO 3.3s 2.9.3
- `21:16:06` ✅ XOM 1.4s 2.9.3
- `21:16:06` ✅ MA 1.8s 2.9.3
- `21:16:06` ✅ V 1.9s 2.9.3
- `21:16:06` ✅ JPM 2.0s 2.9.3
- `21:16:06` ✅ UNH 2.0s 2.9.3
- `21:16:07` ✅ LLY 2.0s 2.9.3
- `21:16:07` ✅ WMT 1.5s 2.9.3
- `21:16:08` ✗ INTC 0.8s {"errorMessage": "'<' not supported between instances of 'complex' and 'int'", "errorType": "TypeError", "requestId": "0ab33a3f-9967-420f-b794-d9274d96cf48", "stackTrace": ["  File \"/var/task/lambda_function.py\", line 5837, in lambda_handler\n    health            = compute_financial_health(scores, ratios_ttm, key_ttm,\n", "  File \"/var/task/lambda_function.py\", line 1713, in compute_financial_health\n    \"score\": round(max(0, min(100, growth_score)), 0),\n"]}
- `21:16:08` ✅ CRM 1.9s 2.9.3
- `21:16:08` ✅ COST 2.1s 2.9.3
- `21:16:09` ✅ NFLX 2.6s 2.9.3
- `21:16:09` ✅ PLTR 2.5s 2.9.3
- `21:16:09` ✅ MU 2.0s 2.9.3
- `21:16:10` ✅ BA 1.6s 2.9.3
- `21:16:12` ✅ QCOM 3.8s 2.9.3
- `21:16:12` ✅ TSM 3.8s 2.9.3
- `21:16:12` ✅ DIS 3.3s 2.9.3
- `21:16:12` ✅ AAOI 3.3s 2.9.3
## P2 sequential retry of failures

- `21:16:16` ✗ retry INTC 2.1s {"errorMessage": "'<' not supported between instances of 'complex' and 'int'", "errorType": "TypeError", "requestId": "4b1af5f9-17ba-4567-9b34-dc34eb0437db", "stackTrace": ["  File \"/var/task/lambda_function.py\", line 5837, in lambda_handler\n    health            = compute_financial_health(scores, ratios_ttm, key_ttm,\n", "  File \"/var/task/lambda_function.py\", line 1713, in compute_financial_health\n    \"score\": round(max(0, min(100, growth_score)), 0),\n"]}
