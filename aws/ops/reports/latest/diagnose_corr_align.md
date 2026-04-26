# Diagnose correlation-breaks alignment failure

**Status:** success  
**Duration:** 3.0s  
**Finished:** 2026-04-26T22:06:01+00:00  

## Log
## 1. Force fresh invoke

- `22:05:58`   invoke 0.7s  payload: {"statusCode": 200, "body": "{\"status\": \"warming_up\", \"n_dates\": 0}"}
## 2. CloudWatch tail

- `22:06:01`   stream: 2026/04/26/[$LATEST]f617f76f754d4ae3b20f638c4b1ddd34
- `22:06:01`     INIT_START Runtime Version: python:3.12.mainlinev2.v7	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:e4ab553846c4e081013ff7d1d608a5358d5b956bb5b81c83c66d2a31da8f6244
- `22:06:01`     [correlation-breaks] starting at 2026-04-26T22:02:48.591493+00:00
- `22:06:01`     [fred] GOLDAMGBD228NLBM HTTP 400: HTTP Error 400: Bad Request
- `22:06:01`     [correlation-breaks] fetched 10 series in 0.5s; lengths: {'VIXCLS': 563, 'SP500': 548, 'NASDAQCOM': 548, 'DGS2': 545, 'BAMLH0A0HYM2': 573, 'DGS10': 545, 'DTWEXBGS': 543, 'DEXJPUS': 543, 'DCOILWTICO': 540, 'GOLDAMGBD228NLBM': 0}
- `22:06:01`     [correlation-breaks] aligned table: 0 dates × 10 instruments
- `22:06:01`     [correlation-breaks] Insufficient aligned data: 0 dates (need ≥312)
- `22:06:01` 
- `22:06:01` Done
