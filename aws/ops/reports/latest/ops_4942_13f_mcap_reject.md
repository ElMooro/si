- `15:03:31` G0 deployed artifact carries the rejection pass
- `15:06:53` payload refreshed after 202s
**Status:** failure  
**Duration:** 208.0s  
**Finished:** 2026-08-21T15:06:58+00:00  

## Error

```
SystemExit: 1
```

## Log
- `15:06:58` FAIL G1 zero surviving impossible market caps  [{'t': 'VSSSF', 'x': 714.6, 'name': 'STATE STR CORP'}, {'t': 'IBIA', 'x': 42.7, 'name': 'iShares Trust'}, {'t': 'MBAIF', 'x': 4.0, 'name': 'SEI INVTS CO'}]
- `15:06:58` FAIL G2 VSSSF/IBIA/NFE/MBAIF carry null cap AND a reason  {'VSSSF': {'market_cap': 1084971.0, 'cap_tier': 'MICRO', 'rejected': None}, 'IBIA': {'market_cap': 10153387.0, 'cap_tier': 'MICRO', 'rejected': None}, 'NFE': {'market_cap': None, 'cap_tier': None, 'rejected': None}, 'MBAIF': {'market_cap': 31833442.0, 'cap_tier': 'MICRO', 'rejected': None}}
- `15:06:58` FAIL G3 rejected caps published with the value thrown away  3 rows: ['VSSSF', 'IBIA', 'MBAIF']
- `15:06:58` PASS G4 real mcap coverage survives (>=400 tickers)  598
- `15:06:58` PASS G5a roster 18/18  18/18/0
- `15:06:58` PASS G5b zero label-vs-filer mismatches  
- `15:06:58` PASS G5c holders <= roster  
- `15:06:58` as_of=2026-06-30 tickers=8131 mcap_kept=598 rejected=3
- `15:06:58` ops 4942 RED: G1 zero surviving impossible market caps; G2 VSSSF/IBIA/NFE/MBAIF carry null cap AND a reason; G3 rejected caps published with the value thrown away
