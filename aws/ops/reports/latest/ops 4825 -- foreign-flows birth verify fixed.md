# G0. LIVE-verify research-doc series ids on FRED

**Status:** failure  
**Duration:** 22.3s  
**Finished:** 2026-08-17T16:16:13+00:00  

## Error

```
SystemExit: 1
```

## Data

| env_FRED | fred_key_donor | mem | new_release | out_kb | state |
|---|---|---|---|---|---|
|  | justhodl-risk-gate |  |  |  |  |
|  |  | 512 |  |  | Active |
| HEALED |  |  |  |  |  |
|  |  |  | False | 2.5 |  |

## Log
- `16:15:52` ✅   G0 total   FORLTTOTALNET99996   n=497 first=1985-01-01 'Foreign Net Transactions of All U.S. Long-Term Securities:'
- `16:15:52` ✅   G0 treas   FORTREASNET69995     n=279 first=2003-03-01 'Foreign Net Transactions of U.S. Long-Term and Short-Term '
- `16:15:53` ✅   G0 equity  FORLTEQTYNET69995    n=497 first=1985-01-01 'Foreign Net Transactions of U.S. Equity Securities: All Co'
- `16:15:54` ✅   G0 corp    FORLTCORPNET99996    n=497 first=1985-01-01 'Foreign Net Transactions of U.S. Long-Term Corporate Bonds'
- `16:15:55` ✅   G0 agency  FORLTAGCYNET99996    n=497 first=1985-01-01 'Foreign Net Transactions of U.S. Long-Term Agency Bonds: G'
- `16:15:55` ✅   G0 tbills  FORSTTREASNET99996   n=497 first=1985-01-01 'Foreign Net Transactions of U.S. Short-Term Treasury Secur'
# 1. function + key + settle

- `16:15:59` ✅ marker settled (attempt 1)
# 2. daily schedule (21:30 UTC)

- `16:16:00` ✅ schedule created -> cron(30 21 * * ? *)
# 3. Event-invoke + poll (<=5 min)

- `16:16:12` ✅ fresh doc in 12s runtime_ms=5218
# 4. truths

- `16:16:12` ✅   LIVE v1.0.0 6/6 latest_month=2026-05-01
- `16:16:12` ✅   treas latest == independent FRED refetch (+17.0B @ 2026-05-01)
- `16:16:12` ✅   equity latest == independent FRED refetch (+134.3B @ 2026-05-01)
- `16:16:12` ✅   all six series share latest_month
- `16:16:12` ✅   risk_appetite latest == component sum (+206.0B)
- `16:16:12` ✅   total_demand latest == component sum (+223.0B)
- `16:16:12` ✅   safe_haven latest == treas-equity (-117.3B)
- `16:16:12` ✅   official_private deferred-not-guessed
- `16:16:12` ✅   all z_10y within [-4,4]
- `16:16:12` ✅   bank FORLTTOTALNET99996   n=497 first=1985-01-01 (Deny-Delete zone)
- `16:16:12` ✗   bank FORTREASNET69995 thin n=279 first=2003-03-01
- `16:16:13` ✅   bank FORLTEQTYNET69995    n=497 first=1985-01-01 (Deny-Delete zone)
- `16:16:13` ✅   bank FORLTCORPNET99996    n=497 first=1985-01-01 (Deny-Delete zone)
- `16:16:13` ✅   bank FORLTAGCYNET99996    n=497 first=1985-01-01 (Deny-Delete zone)
- `16:16:13` ✅   bank FORSTTREASNET99996   n=497 first=1985-01-01 (Deny-Delete zone)
# 5. readout

- `16:16:13`   total     +262.8B  3m   +566.1  12m  +1771.9  z=1.61  since 1985-01-01
- `16:16:13`   treas      +17.0B  3m    +46.2  12m   +334.8  z=-0.16  since 2003-03-01
- `16:16:13`   equity    +134.3B  3m   +253.9  12m   +901.7  z=1.27  since 1985-01-01
- `16:16:13`   corp       +52.5B  3m   +150.0  12m   +448.8  z=1.17  since 1985-01-01
- `16:16:13`   agency     +19.1B  3m    +41.4  12m   +128.9  z=0.67  since 1985-01-01
- `16:16:13`   tbills     -43.5B  3m    -76.1  12m    +52.6  z=-1.63  since 1985-01-01
- `16:16:13`   SIGNAL risk_appetite   +206.0B  12m   +1479.4  z=1.6
- `16:16:13`   SIGNAL safe_haven      -117.3B  12m    -566.9  z=-1.2
- `16:16:13`   SIGNAL total_demand    +223.0B  12m   +1814.2  z=1.22
- `16:16:13`   treas series correctly starts 2003-03 (L+S combined; per-series floor)
- `16:16:13`   NOTE today 4pm ET IS a TIC release (end-June data); the 21:30 UTC daily run flips new_release when FRED ingests it
# 6. verdict

- `16:16:13` ✗ HARD FAILS: ['bank_FORTREASNET69995']
