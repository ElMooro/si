# ops 4113 — family verify-only

**Status:** failure  
**Duration:** 1.8s  
**Finished:** 2026-07-30T04:03:30+00:00  

## Error

```
SystemExit: 1
```

## Data

| generated_at | marker |
|---|---|
| 2026-07-29T21:55:11.472980+00:00 | tradingview-vault v3.14.1 ops4099 wallclock-guard |

## Log
## marker not v3.15.0 — the vault's own story

- `04:03:30`   [03:33:18] END RequestId: d10e0460-ca39-4a63-8bcb-d9b9ba55078c
- `04:03:30`   [03:33:18] REPORT RequestId: d10e0460-ca39-4a63-8bcb-d9b9ba55078c	Duration: 900000.00 ms	Billed Duration: 900000 ms	Memory Size: 1024 MB	Max Memory Used: 134 MB	Status: timeout
XRAY
- `04:03:30`   [03:36:39] END RequestId: 6374f735-2227-4820-817c-c906319f7e94
- `04:03:30`   [03:36:39] REPORT RequestId: 6374f735-2227-4820-817c-c906319f7e94	Duration: 900000.00 ms	Billed Duration: 900000 ms	Memory Size: 1024 MB	Max Memory Used: 134 MB	Status: timeout
XRAY
- `04:03:30`   [03:38:33] INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89a3cfcfffec
- `04:03:30`   [03:38:33] START RequestId: 6374f735-2227-4820-817c-c906319f7e94 Version: $LATEST
- `04:03:30`   [03:50:34] INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89a3cfcfffec
- `04:03:30`   [03:50:35] START RequestId: e3e04958-c179-451e-ad09-2219fcde98a9 Version: $LATEST
- `04:03:30`   [03:53:33] END RequestId: 6374f735-2227-4820-817c-c906319f7e94
- `04:03:30`   [03:53:33] REPORT RequestId: 6374f735-2227-4820-817c-c906319f7e94	Duration: 900000.00 ms	Billed Duration: 900486 ms	Memory Size: 1024 MB	Max Memory Used: 134 MB	Init Duration: 485.9
- `04:03:30` ✗ ARTIFACT NOT v3.15.0 — evidence above
