# Surgical trace — does outcome-checker strip baseline_price?

**Status:** success  
**Duration:** 16.1s  
**Finished:** 2026-04-25T20:28:09+00:00  

## Data

| baseline_preserved | bp_after | bp_before | n_diffs | signal_id |
|---|---|---|---|---|
| True | 777.17 | 777.17 | 3 | b4b1bdb1-d435-4b9e-8 |

## Log
## 1. Find pending signal with bp + elapsed window

- `20:27:54`   Total pending: 2035
- `20:27:54`   Pending with bp + elapsed window: 15
- `20:27:54` 
  Chose signal: b4b1bdb1-d435-4b9e-8268-2fa974...
- `20:27:54`   type:           screener_top_pick
- `20:27:54`   baseline_price: 777.17
- `20:27:54`   status:         pending
- `20:27:54`   measure_against: LITE
- `20:27:54`   elapsed windows: ['day_30']
- `20:27:54`   logged_at:      2026-03-26T15:10:14.387815+00:00
## 2. Full BEFORE snapshot of signal record

- `20:27:54`   accuracy_scores                dict(0 keys)
- `20:27:54`   baseline_price                 = 777.17
- `20:27:54`   benchmark                      = SPY
- `20:27:54`   check_timestamps               dict(3 keys)
- `20:27:54`   check_windows                  list(3)
- `20:27:54`   confidence                     = 0.777778
- `20:27:54`   logged_at                      = 2026-03-26T15:10:14.387815+00:00
- `20:27:54`   logged_epoch                   = 1774537814.0
- `20:27:54`   measure_against                = LITE
- `20:27:54`   metadata                       dict(2 keys)
- `20:27:54`   outcomes                       dict(0 keys)
- `20:27:54`   predicted_direction            = OUTPERFORM
- `20:27:54`   signal_id                      = b4b1bdb1-d435-4b9e-8268-2fa974f8d332
- `20:27:54`   signal_type                    = screener_top_pick
- `20:27:54`   signal_value                   = TOP_10
- `20:27:54`   status                         = pending
- `20:27:54`   ttl                            = 1806073814.0
## 3. Invoke outcome-checker synchronously

- `20:28:07` ✅   Invoked in 13.5s, response: {"statusCode": 200, "body": "{\"processed\": 33, \"timestamp\": \"2026-04-25T20:28:07.561575+00:00\"}"}
## 4. Re-read signal AFTER outcome-checker ran

- `20:28:09`   baseline_price (BEFORE): 777.17
- `20:28:09`   baseline_price (AFTER):  777.17
- `20:28:09`   status (BEFORE): pending
- `20:28:09`   status (AFTER):  partial
- `20:28:09` ✅   ✅ baseline_price PRESERVED (still $777.17)
## 5. Full diff of ALL fields BEFORE vs AFTER

- `20:28:09`   3 fields changed:
- `20:28:09` 
    last_checked:
- `20:28:09`       BEFORE: <MISSING>
- `20:28:09`       AFTER:  2026-04-25T20:27:54.835714+00:00
- `20:28:09` 
    outcomes:
- `20:28:09`       BEFORE: {}
- `20:28:09`       AFTER:  {'day_30': {'benchmark_price': 713.94, 'asset_price': 881.64
- `20:28:09` 
    status:
- `20:28:09`       BEFORE: pending
- `20:28:09`       AFTER:  partial
- `20:28:09` Done
