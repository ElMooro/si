## P1 catalog + resume census

**Status:** failure  
**Duration:** 386.8s  
**Finished:** 2026-08-25T22:07:18+00:00  

## Error

```
SystemExit: 1
```

## Log
- `22:00:51`   otcMarket metadata refused (HTTP Error 404: Not Found) -> seeds
- `22:01:04`   fixedIncomeMarket metadata refused (HTTP Error 404: Not Found) -> seeds
- `22:01:08`   universe=9 invalid=10 already-banked(4982)=8
## P2 drain (checkpointed)

- `22:01:08`   fixedIncomeMarket__treasuryWeeklyAggregates          FAIL Expecting value: line 1 column 1 (char 0)
- `22:01:09`   banked=8 rows=22212147 failed=1 remaining=0 phase=DRAIN
## P4 substance

- `22:01:16`   otcMarket__weeklySummary rows=5755000 span 2022-11-07 .. 2023-12-11
- `22:01:16` P4 FAIL
## P5 card

- `22:07:18` P5 PASS note=FULL Query-API warehouse (finra-full v1): 8/9 datasets · 22212147 rows since inception · 700MB · phase DRAIN · daily rediscovery
- `22:07:18` ops 4983 RED: P4
