# ops 4668 — repo #1-#8 endpoint discovery (build spec, not guesses)

**Status:** failure  
**Duration:** 7.7s  
**Finished:** 2026-08-14T21:32:05+00:00  

## Error

```
SystemExit: 1
```

## Log
## 0. What we ALREADY hold (extend-don't-duplicate)

- `21:31:58`   ofr-stfm: 442 mnemonics, 442 banked, 0 pending
- `21:31:58`   families: [('NYPD', 194), ('REPO', 164), ('MMF', 42), ('FNYR', 30), ('TYLD', 12)]
- `21:31:58`   banked REPO-DVP_TV-FRB: An error occurred (NoSuchKey) when calling the GetObject operation: Th
- `21:31:58`   banked REPO-SOFR_VW-FRB: An error occurred (NoSuchKey) when calling the GetObject operation: Th
- `21:31:58`   nyfed PD: done=1109 depth_keys=1109 first=2013-04-03 mean=330 breaks=['SBN2013', 'SBN2015', 'SBN2022', 'SBN2024', 'SBP2001', 'SBP2013']
## 1. Live probes from inside AWS

- `21:32:05`   (temp probe function deleted)
- `21:32:05`   #1 OFR datasets catalog -> ERR HTTP Error 403: Forbidden
- `21:32:05`   #1 OFR repo dataset series list -> ERR HTTP Error 403: Forbidden
- `21:32:05`   #1 OFR repo full history (DVP vol) -> ERR HTTP Error 400: Bad Request
- `21:32:05`   #2 OFR NYFed ref-rates dataset -> ERR HTTP Error 403: Forbidden
- `21:32:05`   #2 OFR SOFR full history -> ERR HTTP Error 400: Bad Request
- `21:32:05` ✅   #3 NYFed PD earliest break (1998) -> 30 bytes · dict:pd
- `21:32:05`       body: {"pd": { "timeseries": [ ] } }
- `21:32:05` ✅   #3 NYFed PD seriesbreaks -> 716 bytes · dict:pd
- `21:32:05`       body: {"pd": { "seriesbreaks": [ { "label": "JAN 1998 TO JUN 2001", "seriesbreak": "SBP2001", "startdate": "1998-01-28", "enddate": "2001-06-30" }, { "label": "JUL 2001 TO MAR 2013", "seriesbreak": "SBP2013", "startdate": "2001-07-01", 
- `21:32:05`   #4 NYFed tri-party latest -> ERR HTTP Error 400: Bad Request
- `21:32:05`   #4 NYFed tri-party alt path -> ERR HTTP Error 400: Bad Request
- `21:32:05`   #5 OFR sponsored repo -> ERR HTTP Error 403: Forbidden
- `21:32:05` ✅   #6 NYFed RRP full history -> 1325961 bytes · dict:repo
- `21:32:05`       body: { "repo": { "operations": [ { "operationId": "RP 081426 26", "operationDate": "2026-08-14", "operationType": "Reverse Repo", "note": "" ,"totalAmtAccepted": 250000000 }, { "operationId": "RP 081326 26", "operationDate": "2026-08-1
- `21:32:05` ✅   #6 NYFed SRF/repo ops search -> 16636 bytes · dict:repo
- `21:32:05`       body: { "repo": { "operations": [ { "operationId": "RP 081426 25", "auctionStatus": "Results", "operationDate": "2026-08-14", "settlementDate": "2026-08-14", "maturityDate": "2026-08-17", "operationType": "Repo", "operationMethod": "Ful
- `21:32:05`   #7 OFR MMF dataset -> ERR HTTP Error 403: Forbidden
- `21:32:05`   #8 OFR hedge fund monitor -> ERR HTTP Error 403: Forbidden
- `21:32:05` ✗   only 4/14 endpoints answered — spec would be guesswork
- `21:32:05`   manifest -> data/_state/repo-probe-manifest.json
## verdict

- `21:32:05` ✗ discovery incomplete: 1 red
