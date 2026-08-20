- `16:54:29` G0 producer contract OK
- `16:54:29` PRE-RUN colliding tickers: 626  sample=['LEN', 'IBIA', 'SE', 'WDC', 'ALNY', 'HPE', 'HALO', 'CPAY', 'IREN', 'UHS']
**Status:** failure  
**Duration:** 239.2s  
**Finished:** 2026-08-20T16:58:28+00:00  

## Error

```
SystemExit: 1
```

## Log
- `16:58:27` invoke rc=200 in 238s
- `16:58:28` PASS G1 one ticker claimed by exactly one cusip  residual=[]
- `16:58:28` PASS G2 CPAY/ORCL/ICLN each held by <=1 cusip  {'CPAY': [('219948106', 'CORPAY INC')], 'ORCL': [('68389X105', 'ORACLE CORP')], 'ICLN': [('464288224', 'ISHARES GLOBAL CLEAN ENERGY')]}
- `16:58:28` PASS G3a roster still balances  18/17/1
- `16:58:28` PASS G3b holders still <= roster  []
- `16:58:28` PASS G3c exits also <= roster  
- `16:58:28` FAIL G4 no position exceeds its own market cap  [{'t': 'VSSSF', 'held': 775343042.0, 'mcap': 1084971.0, 'x': 714.6}, {'t': 'IBIA', 'held': 434009062.0, 'mcap': 10153387.0, 'x': 42.7}, {'t': 'NFE', 'held': 2502861000.0, 'mcap': 92574190.0, 'x': 27.0}, {'t': 'MBAIF', 'held': 126563754.0, 'mcap': 31833442.0, 'x': 4.0}]
- `16:58:28` as_of=2026-06-30 parsed=17/18 tickers=8665 stale=[{"fund_key": "PERSHING", "period_of_report": "2026-03-31"}, {"fund_key": "GREENLIGHT", "period_of_report": "2023-12-31"}, {"fund_key": "SCION", "period_of_report": "2025-09-30"}]
- `16:58:28` ops 4937 RED: G4 no position exceeds its own market cap
