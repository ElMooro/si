- `16:40:38` invoking justhodl-13f-positions (parser v5 -- full re-parse, no cache reuse)
- `16:43:23` invoke rc=200 in 165s
**Status:** failure  
**Duration:** 165.4s  
**Finished:** 2026-08-20T16:43:24+00:00  

## Error

```
SystemExit: 1
```

## Log
- `16:43:24` PASS G1 roster balances (total == parsed + failed)  18 == 17 + 1
- `16:43:24` PASS G2 no ticker held by more funds than exist  worst=[]
- `16:43:24` FAIL G3 zero crypto pairs in the book  ['USD']
- `16:43:24` PASS G4 one ticker -> one name (CPAY 4-way collision)  []
- `16:43:24` PASS G5 stale_funds field published  [{'fund_key': 'PERSHING', 'period_of_report': '2026-03-31'}, {'fund_key': 'GREENLIGHT', 'period_of_report': '2023-12-31'}, {'fund_key': 'SCION', 'period_of_report': '2025-09-30'}]
- `16:43:24` PASS G6 fund_errors names every gap  [{'fund_key': 'ELLIOTT', 'error': 'no_13f_hr_filing_found'}]
- `16:43:24` as_of_quarter=2026-06-30  parsed=17/18  tickers=7504
- `16:43:24` stale roster: [{"fund_key": "PERSHING", "period_of_report": "2026-03-31"}, {"fund_key": "GREENLIGHT", "period_of_report": "2023-12-31"}, {"fund_key": "SCION", "period_of_report": "2025-09-30"}]
- `16:43:24` ops 4936 RED: G3 zero crypto pairs in the book
