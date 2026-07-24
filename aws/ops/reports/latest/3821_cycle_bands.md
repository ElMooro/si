# ops 3821 — Rainbow + Pi Cycle into onchain-ratios

**Status:** failure  
**Duration:** 28.0s  
**Finished:** 2026-07-24T21:16:46+00:00  

## Error

```
SystemExit: 1
```

## Data

| fair_value | mayer | n_closes | pi_distance_pct | pi_signal | price | r_squared | rainbow_band | z_sigma |
|---|---|---|---|---|---|---|---|---|
| 52637.35 | 0.897 | 5850 | -59.38 | no signal | 65094.41 | 0.9091 | FAIR / HOLD | 0.2 |

## Log
## G0. No-rebuild check

- `21:16:18` ✅   rainbow: still zero other engines — genuine gap
- `21:16:18` ✅   pi_cycle: still zero other engines — genuine gap
## 1. Inherit env + deploy

- `21:16:18`   preserving env keys: ['S3_BUCKET']
- `21:16:18`   zip: 89850 bytes
## 1. Lambda

- `21:16:19`   Lambda exists — updating
- `21:16:22` ✅   ✓ updated justhodl-onchain-ratios
## 2. Zip-settle

- `21:16:28` ✅   settled after 5s
## 3. Invoke

- `21:16:45`   {'statusCode': 200, 'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}, 'body': '{"ok": true, "btc_mvrv": 1.2309583188260411, "eth_gas": null}'}
## 4. Verify the fit is REAL

- `21:16:46` ✅   cycle_bands present reason=
- `21:16:46` ✅   history >= 800 closes = 5850 from 2010-07-18
- `21:16:46` ✅   rainbow fit available reason=
- `21:16:46` ✅   R^2 in a sane range (0.5–0.999) = 0.9091
- `21:16:46` ✅   residual sigma > 0 = 1.0678
- `21:16:46` ✅   fair value within 10x of spot fair 52637.35 vs spot 65094.41
- `21:16:46` ✅   band + z published z=0.2 band='FAIR / HOLD'
- `21:16:46` ✅   7 band prices 
- `21:16:46` ✅   pi cycle available reason=
- `21:16:46` ✅   350DMA genuinely computed 111d=70122.93 350d=86313.9
- `21:16:46` ✅   pi signal + distance no signal · -59.38% to trigger
- `21:16:46` ✅   pi ships n=3 
- `21:16:46` ✗   honesty caveats present 
- `21:16:46`     Mayer 0.897 (CHEAP (<1.0)) pctile 27.1
- `21:16:46`     200wMA 63190.37 · 3.01% above
- `21:16:46` ✗ FAILED 1: ['honesty caveats present']
