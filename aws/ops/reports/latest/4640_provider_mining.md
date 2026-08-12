# ops 4639 — schema realignment

**Status:** failure  
**Duration:** 241.7s  
**Finished:** 2026-08-12T19:18:02+00:00  

## Error

```
SystemExit: 1
```

## Data

| barometer | fn_error | resolved | reversal | rlabel | tlabel | trend |
|---|---|---|---|---|---|---|
|  | None |  |  |  |  |  |
| 15.1 |  | 686 | 17.4 | FORMING TURN TO EASE | MIXED | 13.0 |

## Log
## deploy (ops-side) + settle

- `19:17:05` ✅   [deploy] v1.4.0 live
## fleet-store shapes (BDI/CRYPTOCAP evidence)

- `19:17:06` data/freight-pulse.json: {"ok": true, "version": "2.0.0", "generated_at": "2026-08-12T11:50:17.996738+0", "engine_class": "physical_trade_slow_confirma", "composite_role": "slow_confirmation_leg", "lag_months": -2}
- `19:17:06` data/cryptoquant-series.json: {"generated_at": "2026-08-11T21:05:07+00:00", "series": {"btc_exchange_netflow": {"d": "list", "v": "list"}, "btc_exchange_inflow": {"d": "list", "v": "list"}, "btc_exchange_outflow": {"d": "list", "v": "list"}, "btc_exchange_reserve": {"d": "list", "v": "list"}, "btc_exchange_addr_in": {"d": "list"
- `19:17:07` data/coinmarketcap.json: MISS An error occurred (NoSuchKey) when calling the GetObject ope
## run + row-schema truth

- `19:18:01` CAPITALCOM:COPPER/TVC:GOLD res=True  z=1.63  trend=UP    
- `19:18:01` ECONOMICS:USM2             res=None  z=None  trend=None  
- `19:18:01` FOREXCOM:USDJPY            res=None  z=None  trend=None  
- `19:18:01` TVC:GB10Y                  res=None  z=None  trend=None  
- `19:18:01` CRYPTOCAP:TOTAL            res=None  z=None  trend=None  
- `19:18:01` INDEX:BDI                  res=None  z=None  trend=None  
- `19:18:01` ✗   [mined-routes] CONTRACT MISS — 1/3 new provider routes z-based
- `19:18:01` ✗   [resolution] CONTRACT MISS — 686/1086 resolved (budgets rescaled)
- `19:18:01` FRED:WALCL               chg=+0.15% (WoW)     z=0.79  trend=UP    rev=NONE
- `19:18:01` FRED:DGS10               chg=+1.51% (DoD)     z=1.49  trend=UP    rev=NONE
- `19:18:01` AMEX:JNK                 chg=-0.17% (DoD)     z=0.68  trend=DOWN  rev=NONE
- `19:18:01` TVC:DE10Y-TVC:IT10Y      chg=-3.60% (MoM)     z=0.27  trend=UP    rev=REVERSAL_UP
- `19:18:01` ✅   [row-schema] 4/4 sample rows carry chg+trend+reversal(alias)
- `19:18:01` ✅   [dials] TREND 13.0 (MIXED) · REV 17.4 (FORMING TURN TO EASE)
## edge page/payload

- `19:18:02` ✅   [edge] page reads evolved keys; payload rows render-ready
## verdict

- `19:18:02` ✗ schema realign: 2 red
