- `03:10:13`   zip: 77484 bytes
## 1. Lambda
**Status:** failure  
**Duration:** 163.3s  
**Finished:** 2026-07-14T03:12:56+00:00  

## Error

```
SystemExit: FAILS: level outside truth band: 0.461
```

## Data

| corr60 | d60 | dist_bps | episode_buckets_with_n | eps | fails | inherited_env_keys | level | real10 | tier |
|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  | 3 |  |  |  |
| -0.075 | -379.9 | 453.9 |  |  |  |  | 0.461 | 2.32 | BENIGN |
|  |  |  | 3 | {'cross_4.50': 3, 'cross_4.75': 5, 'cross_5.00': 2} |  |  |  |  |  |
|  |  |  |  |  | ['level outside truth band: 0.461'] |  |  |  |  |

## Log

- `03:10:14`   Lambda missing — creating
- `03:10:19` ✅   ✓ created justhodl-us10y-sentinel
- `03:10:19` ✅   ✓ Function URL: https://gplvjzbh5asyqbycvwnauhmvaq0lhcls.lambda-url.us-east-1.on.aws/
## 2. EventBridge Scheduler (classic cap saturated)

- `03:10:19`   created cron(20 0,6,12,16,20 * * ? *) UTC
## 3. First run + truth bands

## 4. yield-curve.html sentinel strip live

- `03:12:56`   strip markers live
