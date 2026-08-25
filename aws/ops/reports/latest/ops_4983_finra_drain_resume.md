## P1 catalog + resume census

**Status:** failure  
**Duration:** 384.3s  
**Finished:** 2026-08-25T22:00:28+00:00  

## Error

```
SystemExit: 1
```

## Log
- `21:54:04`   otcMarket metadata refused (HTTP Error 404: Not Found) -> seeds
- `21:54:19`   fixedIncomeMarket metadata refused (HTTP Error 404: Not Found) -> seeds
- `21:54:23`   universe=9 invalid=10 already-banked(4982)=8
## P2 drain (checkpointed)

- `21:54:23`   fixedIncomeMarket__treasuryWeeklyAggregates          FAIL Expecting value: line 1 column 1 (char 0)
- `21:54:24`   banked=8 rows=22212147 failed=1 remaining=0 phase=DRAIN
## P4 substance

- `21:54:26`   substance err Compressed file ended before the end-of-stream marker was reached
- `21:54:26` P4 FAIL
## P5 card

- `22:00:28` P5 PASS note=FULL Query-API warehouse (finra-full v1): 8/9 datasets · 22212147 rows since inception · 700MB · phase DRAIN · daily rediscovery
- `22:00:28` ops 4983 RED: P4
