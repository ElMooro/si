## TEST ALL THREE KEYS vs BENZINGA

**Status:** failure  
**Duration:** 0.5s  
**Finished:** 2026-07-15T01:35:27+00:00  

## Error

```
SystemExit: 1
```

## Data

| Default (…X_d) | RESULT | beautiful_chandrasekhar (…ptM) | desperate_lamarr (…FPI) |
|---|---|---|---|
|  |  | {'http': 403, 'status': 'NOT_AUTHORIZED'} |  |
|  |  |  | {'http': 403, 'status': 'NOT_AUTHORIZED'} |
| {'http': 403, 'status': 'NOT_AUTHORIZED'} |  |  |  |
|  | NO_ENTITLED_KEY |  |  |

## Log
## VERDICT

- `01:35:27` ✗ None of the 3 Massive keys are Benzinga-entitled (all 403/empty). The Benzinga add-on is not attached to any key on this Massive account — needs enabling in the Massive dashboard (per-key entitlement toggle), or the add-on lives on a different Massive account.
