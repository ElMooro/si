- `16:36:41` invoking justhodl-13f-positions (parser v5 -- full re-parse, no cache reuse)
- `16:40:11` invoke rc=200 in 210s
**Status:** failure  
**Duration:** 212.0s  
**Finished:** 2026-08-20T16:40:13+00:00  

## Error

```
SystemExit: 1
```

## Log
- `16:40:13` PASS G1 roster balances (total == parsed + failed)  18 == 17 + 1
- `16:40:13` PASS G2 no ticker held by more funds than exist  worst=[]
- `16:40:13` FAIL G3 zero crypto pairs in the book  ['USD']
- `16:40:13` PASS G4 one ticker -> one name (CPAY 4-way collision)  []
- `16:40:13` PASS G5 stale_funds field published  [{'fund_key': 'PERSHING', 'period_of_report': '2026-03-31'}, {'fund_key': 'GREENLIGHT', 'period_of_report': '2023-12-31'}, {'fund_key': 'SCION', 'period_of_report': '2025-09-30'}]
- `16:40:13` PASS G6 fund_errors names every gap  [{'fund_key': 'ELLIOTT', 'error': 'no_13f_hr_filing_found'}]
- `16:40:13` as_of_quarter=2026-06-30  parsed=17/18  tickers=7504
- `16:40:13` stale roster: [{"fund_key": "PERSHING", "period_of_report": "2026-03-31"}, {"fund_key": "GREENLIGHT", "period_of_report": "2023-12-31"}, {"fund_key": "SCION", "period_of_report": "2025-09-30"}]
- `16:40:13` ops 4936 RED: G3 zero crypto pairs in the book
