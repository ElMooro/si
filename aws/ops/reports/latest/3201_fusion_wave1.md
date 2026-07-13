# ops 3201 — 16 engines fused with his research, proven in live feeds

**Status:** failure  
**Duration:** 119.6s  
**Finished:** 2026-07-13T04:06:58+00:00  

## Error

```
SystemExit: 1
```

## Data

| deployed | divergences | n_fails | n_warns | of | of_samples | proven_total | themes | verdict | verified_feeds |
|---|---|---|---|---|---|---|---|---|---|
|  | 3 |  |  |  |  | 0 | 10 |  |  |
| 15 |  |  |  | 16 |  |  |  |  |  |
|  |  |  |  |  | 4 |  |  |  | 4 |
|  |  | 1 | 0 |  |  |  |  | FAIL |  |

## Log
## 1. Refresh wl-fusion on the 3200 index

- `04:04:59`   zip: 73208 bytes
## 1. Lambda

- `04:04:59`   Lambda exists — updating
- `04:05:05` ✅   ✓ updated justhodl-wl-fusion
## 2. EB rule + permissions

- `04:05:06`   rule already correct: wl-fusion-daily (cron(50 22 ? * TUE-SAT *))
- `04:05:06` ✅   ✓ target → justhodl-wl-fusion
- `04:05:06` ✅   ✓ added invoke permission
- `04:05:11`   BREADTH    pressure 82.1p  firing 5/7  EXTREME
- `04:05:11`   INFLATION  pressure 65.2p  firing 2/3  ELEVATED
- `04:05:11`   LIQUIDITY  pressure 64.7p  firing 6/15  ELEVATED
- `04:05:11`   DOLLAR     pressure 55.4p  firing 1/6  QUIET
- `04:05:11`   STRESS     pressure 54.8p  firing 0/9  QUIET
- `04:05:11` ✅ wl-fusion fresh on the alive index
## 2. Deploy the 16 fused engines

- `04:05:12`   zip: 79367 bytes
## 1. Lambda

- `04:05:12`   Lambda exists — updating
- `04:05:15` ✅   ✓ updated justhodl-regime-composite
- `04:05:15` ✅   ✓ Function URL: https://sdvyecytc2r4jcl4oopdzumm2y0eidmi.lambda-url.us-east-1.on.aws/
- `04:05:16`   zip: 76409 bytes
## 1. Lambda

- `04:05:16`   Lambda exists — updating
- `04:05:19` ✅   ✓ updated justhodl-crisis-composite
- `04:05:20` ✅   ✓ Function URL: https://jvnfkjubxpfrrvks7dzvdsdwsu0jmqnb.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `04:05:20`   rule already correct: crisis-composite-hourly (cron(15 * * * ? *))
- `04:05:20` ✅   ✓ target → justhodl-crisis-composite
- `04:05:20` ✅   ✓ added invoke permission
- `04:05:21`   zip: 80371 bytes
## 1. Lambda

- `04:05:21`   Lambda exists — updating
- `04:05:26` ✅   ✓ updated justhodl-risk-regime
- `04:05:27`   zip: 73556 bytes
## 1. Lambda

- `04:05:27`   Lambda exists — updating
- `04:05:33` ✅   ✓ updated justhodl-global-liquidity
- `04:05:33` ✅   ✓ Function URL: https://jm3rzq6ryqvukw77xf5nzwia3e0fauvg.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `04:05:33`   rule already correct: global-liquidity-daily (cron(0 14 ? * MON-FRI *))
- `04:05:34` ✅   ✓ target → justhodl-global-liquidity
- `04:05:34` ✅   ✓ added invoke permission
- `04:05:34`   zip: 86185 bytes
## 1. Lambda

- `04:05:34`   Lambda exists — updating
- `04:05:37` ✅   ✓ updated justhodl-dollar-radar
- `04:05:38`   zip: 87784 bytes
## 1. Lambda

- `04:05:38`   Lambda exists — updating
- `04:05:42` ✅   ✓ updated justhodl-master-ranker
- `04:05:42`   zip: 83411 bytes
## 1. Lambda

- `04:05:43`   Lambda exists — updating
- `04:05:46` ✅   ✓ updated justhodl-liquidity-credit-engine
- `04:05:46` ✅   ✓ Function URL: https://zp4zfspxympgugjmpmxz3mtgam0adsey.lambda-url.us-east-1.on.aws/
- `04:05:46`   zip: 97462 bytes
## 1. Lambda

- `04:05:47`   Lambda exists — updating
- `04:05:52` ✅   ✓ updated justhodl-cycle-clock
- `04:05:52` ✅   ✓ Function URL: https://nzvpiazto345r3xd7ol474jabm0foxjp.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `04:05:53`   rule already correct: justhodl-cycle-clock-daily (cron(30 23 * * ? *))
- `04:05:53` ✅   ✓ target → justhodl-cycle-clock
- `04:05:53` ✅   ✓ added invoke permission
- `04:05:53`   zip: 75589 bytes
## 1. Lambda

- `04:05:54`   Lambda exists — updating
- `04:05:59` ✅   ✓ updated justhodl-macro-nowcast
- `04:05:59` ✅   ✓ Function URL: https://grqrglqleo7mnatpuzifhw2yym0gyabh.lambda-url.us-east-1.on.aws/
- `04:06:00`   zip: 75700 bytes
## 1. Lambda

- `04:06:00`   Lambda exists — updating
- `04:06:05` ✅   ✓ updated justhodl-equity-confluence
## 2. EB rule + permissions

- `04:06:06`   rule already correct: justhodl-equity-confluence-daily (cron(30 0 * * ? *))
- `04:06:06` ✅   ✓ target → justhodl-equity-confluence
- `04:06:06` ✅   ✓ added invoke permission
- `04:06:07`   zip: 75324 bytes
## 1. Lambda

- `04:06:07`   Lambda exists — updating
- `04:06:12` ✅   ✓ updated justhodl-breadth-divergence
- `04:06:13` ✅   ✓ Function URL: https://74cemrbug6wwr2hudpjp6h7tqy0vespw.lambda-url.us-east-1.on.aws/
- `04:06:13`   zip: 79424 bytes
## 1. Lambda

- `04:06:13`   Lambda exists — updating
- `04:06:19` ✅   ✓ updated justhodl-breadth-thrust
- `04:06:19` ✅   ✓ Function URL: https://efhuswwcjfk3smdt5e4md3meei0cjcxs.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `04:06:20`   rule already correct: justhodl-breadth-thrust-daily (cron(0 22 ? * MON-FRI *))
- `04:06:20` ✅   ✓ target → justhodl-breadth-thrust
- `04:06:20` ✅   ✓ added invoke permission
- `04:06:20`   zip: 76671 bytes
## 1. Lambda

- `04:06:20`   Lambda exists — updating
- `04:06:26` ✅   ✓ updated justhodl-crypto-liquidity
- `04:06:26` ✅   ✓ Function URL: https://zxldrxsah6ophdakllnyxqz7da0zngef.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `04:06:26`   rule already correct: justhodl-crypto-liquidity-daily (cron(0 12,22 * * ? *))
- `04:06:27` ✅   ✓ target → justhodl-crypto-liquidity
- `04:06:27` ✅   ✓ added invoke permission
- `04:06:27`   zip: 75780 bytes
## 1. Lambda

- `04:06:27`   Lambda exists — updating
- `04:06:30` ✅   ✓ updated justhodl-crypto-emergence
- `04:06:31` ✅   ✓ Function URL: https://quuo552ekepjgu7utpppcvru4q0dquno.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `04:06:31`   rule already correct: justhodl-crypto-emergence-daily (cron(0 13,23 * * ? *))
- `04:06:31` ✅   ✓ target → justhodl-crypto-emergence
- `04:06:31` ✅   ✓ added invoke permission
- `04:06:32`   zip: 81816 bytes
## 1. Lambda

- `04:06:32`   Lambda exists — updating
- `04:06:35` ✅   ✓ updated justhodl-eurodollar-plumbing
- `04:06:35` ✅   ✓ Function URL: https://iwjky4fvp4lkkbunkc4ba5qvwa0dlkxq.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `04:06:36`   rule already correct: eurodollar-plumbing-daily (cron(0 12 * * ? *))
- `04:06:36` ✅   ✓ target → justhodl-eurodollar-plumbing
- `04:06:36` ✅   ✓ added invoke permission
## 3. Prove wl_research lands in live feeds

- `04:06:47`   ✓ regime-composite: wl_research live — DOLLAR 55.4p
- `04:06:47`   ✓ macro-nowcast: wl_research live — INFLATION 65.2p
- `04:06:58`   ✓ dollar-radar: wl_research live — LIQUIDITY 64.7p
- `04:06:58`   ✓ crypto-liquidity: wl_research live — CRYPTO 50.9p
- `04:06:58` ✗ deploy justhodl-credit-stress: 'str' object has no attribute 'get'
