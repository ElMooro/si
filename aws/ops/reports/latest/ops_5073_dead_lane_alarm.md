## P0 deploy + run the sentinel

**Status:** failure  
**Duration:** 430.0s  
**Finished:** 2026-08-31T02:22:40+00:00  

## Error

```
SystemExit: 1
```

## Log
- `02:15:31`   code fresh 2026-08-31T02:02:25.000+0000
- `02:15:31`   before: overall=DEGRADED worst=census-us
- `02:15:31`   sentinel kicked
## P1 the new pipeline

- `02:22:35`   dead-lanes pipeline ABSENT -- the check did not run
- `02:22:35`   all pipelines now:
- `02:22:35`     fred             COMPLETE         COMPLETE_WITH_LEAKS
- `02:22:35`     nyfed            OK               rates 10.3h · pd 1539/? · repo 10.4h
- `02:22:35`     sdmx-eurostat    COMPLETE         8152/8152, 6 source-side failures
- `02:22:35`     sdmx-oecd        COMPLETE         1548/1546, 488 source-side failures
- `02:22:35`     sdmx-statcan     COMPLETE         8229/8229, 5 source-side failures
- `02:22:35`     sdmx-bis         COMPLETE         29/29, 1 source-side failures
- `02:22:35`     sdmx-ecb         COMPLETE         214/214, 7 source-side failures
- `02:22:35`     census-us        STALE            COMPLETE 55/56 datasets · 4604884 rows · 1 source failures logged
- `02:22:35`     provider-catalog OK               hub index freshness
## P2 has the banner cleared

- `02:22:35`   overall DEGRADED -> DEGRADED   worst census-us -> census-us
- `02:22:35`   still DEGRADED because of census-us; that is now a lane with a name rather than a mystery
- `02:22:35`   incidents retained: 5 (aged to 14 days by ops 5056)
## P3 the running imports

- `02:22:36`   gdelt backfill: recovered=0 permanent=0 remaining=0 0.00 GB
- `02:22:39`   boj: 60,725/120,394 series (50.4%) rows 294,539
- `02:22:40`   census-econ: 385/1226 entries
- `02:22:40`   -> data/ops/dead-lane-alarm.json
- `02:22:40` ops 5073 RED: P1:absent
