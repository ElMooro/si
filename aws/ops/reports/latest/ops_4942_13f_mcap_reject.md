- `15:09:45` artifact ready: state=Active update=Successful
- `15:09:45` G0 deployed function is Active and carries the rejection pass
**Status:** failure  
**Duration:** 220.8s  
**Finished:** 2026-08-21T15:13:25+00:00  

## Error

```
SystemExit: 1
```

## Log
- `15:13:19` payload refreshed after 203s
- `15:13:25` PASS G1 zero surviving impossible market caps  []
- `15:13:25` FAIL G2 VSSSF/IBIA/NFE/MBAIF carry null cap AND a reason  {'VSSSF': {'market_cap': None, 'cap_tier': None, 'rejected': 'held_exceeds_market_cap'}, 'IBIA': {'market_cap': None, 'cap_tier': None, 'rejected': 'held_exceeds_market_cap'}, 'NFE': {'market_cap': None, 'cap_tier': None, 'rejected': None}, 'MBAIF': {'market_cap': None, 'cap_tier': None, 'rejected': 'held_exceeds_market_cap'}}
- `15:13:25` PASS G3 rejected caps published with the value thrown away  12 rows: ['VSSSF', 'IBIA', 'MBAIF', 'IWM', 'SPY', 'VOO']
- `15:13:25` PASS G4 real mcap coverage survives (>=400 tickers)  586
- `15:13:25` PASS G5a roster 18/18  18/18/0
- `15:13:25` PASS G5b zero label-vs-filer mismatches  
- `15:13:25` PASS G5c holders <= roster  
- `15:13:25` as_of=2026-06-30 tickers=8131 mcap_kept=586 rejected=12
- `15:13:25` ops 4942 RED: G2 VSSSF/IBIA/NFE/MBAIF carry null cap AND a reason
