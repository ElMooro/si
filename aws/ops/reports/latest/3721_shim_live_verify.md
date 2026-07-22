# ops 3721 — FMP calendar shim: live verification

**Status:** failure  
**Duration:** 40.7s  
**Finished:** 2026-07-22T19:22:05+00:00  

## Error

```
SystemExit: 1
```

## Data

| detail | gate | ok |
|---|---|---|
| needs_republish=['justhodl-estimate-revisions', 'justhodl-earnings-tracker'] state={'justhodl-estimate-revisions': {'has_benzinga_module': True, 'has_shim': False, 'n_fil | G1_shim_state_known | True |
| shim missing after republish: none | G2_shim_live | True |
| artifact refreshed in 15s | G3_refreshed | True |
| upward=0 downward=0 (was 0/0) n_tracked=436 n_fmp_enriched=256 n_with_history=436 | G4_directions_populate | False |
| n_tracked=436 n_with_history=436 n_state_keys=448 (re-seed under new fiscal keys is expected — FMP supplies no fiscal_period/fiscal_year, so some names produce a directio | G5_calendar_alive | True |

## Log
## 1 — live zip inspection

- `19:21:25` G1_shim_state_known True
## 2 — republish (deploy-lambdas skipped the [skip-deploy] commit)

- `19:21:31` republished justhodl-estimate-revisions (87390 bytes)
- `19:21:37` republished justhodl-earnings-tracker (89642 bytes)
- `19:21:49` G2_shim_live True
## 3 — invoke estimate-revisions

- `19:22:05` G3_refreshed True
- `19:22:05` G4_directions_populate False
- `19:22:05` G5_calendar_alive True
- `19:22:05` VERDICT: GAPS: G4_directions_populate
