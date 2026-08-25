## P1 field census

**Status:** failure  
**Duration:** 9.1s  
**Finished:** 2026-08-25T22:25:37+00:00  

## Error

```
SystemExit: 1
```

## Log
- `22:25:28`   otcMarket.weeklySummary date-field=lastUpdateDate keys=['MPID', 'firmCRDNumber', 'initialPublishedDate', 'issueName', 'issueSymbolIdentifier', 'lastReportedDate', 'lastUpdateDate', 'marketParticipantName']
- `22:25:29`   otcMarket.regShoDaily date-field=tradeReportDate keys=['marketCode', 'reportingFacilityCode', 'securitiesInformationProcessorSymbolIdentifier', 'shortExemptParQuantity', 'shortParQuantity', 'totalParQuantity', 'tradeReportDate']
## P2 filter probes (historical 2015-06-29)

- `22:25:29`   GET-equality -> 5 rows (0 historical)
- `22:25:33`   POST-compare -> Expecting value: line 1 column 1 (char 0)
- `22:25:37`   POST-dateRange -> Expecting value: line 1 column 1 (char 0)
- `22:25:37`   WINNING SHAPE: None
- `22:25:37` ops 4984 RED: keyless tier refuses historical filters -- full depth rides the auth secret
