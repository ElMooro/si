# ops 4628 — fleet-join v1.3.1: columnar stores resolution

**Status:** failure  
**Duration:** 18.9s  
**Finished:** 2026-08-12T02:24:18+00:00  

## Error

```
SystemExit: 1
```

## Data

| alarm | amber | extremes | red | resolved | with_history |
|---|---|---|---|---|---|
| AMBER | 3 | 43 | 0 | 361 | 216 |

## Log
## pre-dump store shapes

- `02:23:59` data/_ma200/closes.json: {"dates": ["len=235", "2025-09-03"], "series": {"IBKR": ["len=235", "float"], "ELF": ["len=235", "float"], "DIA": ["len=235", "float"], "XLB": ["len=235", "float"], "DOW": ["len=235", "float"]}}
- `02:23:59` data/vix-curve-history.json: {"generated_at": "2026-08-12T02:14:36.765267+00:00", "n_days": 1764, "first_date": "2019-08-05", "last_date": "2026-08-11", "dates": ["len=1764", "2019-08-05"]}
- `02:23:59` data/dollar-radar-history.json: {"rows": ["len=37", {"date": "str", "ts": "str", "dollar_pressure": "int", "regime": "str", "risk_score": "int"}], "updated": "2026-08-12T01:15:31.982569+00:00"}
## deploy-settle

- `02:23:59` ✅   [deploy] v1.3.1 live
## run + fleet-join truth

- `02:24:12` TVC:MOVE         CALM      z=0.6   n=260  +3.96% (chg)
- `02:24:12` CBOE:VIX3M       NO_HISTORY z=None  n=1    
- `02:24:12` CBOE:VXN         AMBER     z=None  n=2    
- `02:24:12` TVC:DXY          CALM      z=None  n=2    
- `02:24:12` NASDAQ:TLT       CALM      z=1.4   n=235  -0.85% (DoD)
- `02:24:12` AMEX:HYG         CALM      z=0.65  n=235  -0.16% (DoD)
- `02:24:12` NASDAQ:SMH       CALM      z=0.88  n=235  -2.28% (DoD)
- `02:24:12` ECONOMICS:USWEI  CALM      z=0.6   n=300  +10.74% (WoW)
- `02:24:12` ✅   [fleet-z] z-basis via fleet joins: ['TVC:MOVE', 'NASDAQ:TLT', 'AMEX:HYG']
- `02:24:12` ✅   [wei] ECONOMICS:USWEI via FRED alias
- `02:24:12` ✗   [resolution] CONTRACT MISS — 361/500 resolved
- `02:24:12` ✗   [history-depth] CONTRACT MISS — 216 rows on statistical basis
- `02:24:12` ✅   [alarm-valid] alarm AMBER
## canary + edge

- `02:24:17` ✅   [canary] board parity: {"state": "AMBER", "n_red": 0, "n_amber": 3, "n_range_extreme": 43, "list": "Black Swan Event", "doctrine": "K
- `02:24:18` ✅   [edge] edge serves fleet-joined TLT z-basis
## verdict

- `02:24:18` ✗ fleet-join: 2 red (shapes above are the repair evidence)
