# ops 3948 — runner-exec ground truth + IMF Series attrs

**Status:** success  
**Duration:** 457.2s  
**Finished:** 2026-07-27T02:16:26+00:00  

## Data

| coverage_pct | imf_working_key | n_live | statuses |
|---|---|---|---|
| 80.7 | None | 453 | {'META': 1, 'LIVE': 453, 'DISCONTINUED': 2, 'NO_FREE_SOURCE': 105} |

## Log
## A. runner-exec the engine functions (real network, tracebacks)

- `02:08:49`   mofjp_latest('2Y') -> {'value': 1.531, 'prev': None, 'chg_pct': None, 'asof': 'mof.go.jp:2026/7/24'}
- `02:08:50`   snb_latest('2 year') -> None
- `02:08:51`   snb_latest('3 year') -> None
- `02:08:51`   mof diag: ['https://www.mof.go.jp/english/policy/jgbs/reference/interest -> OK 21 lines']
## B. IMF <Series> attrs -> exact key -> verify

- `02:08:52` ✅   SERIES ATTRS: <Series COUNTRY="AGO" INDICATOR="IRFCLDT1_IRFCL32_USD" SECTOR="S1XS1311" FREQUENCY="M" SCALE="6" METHODOLOGY="IRFCL13" ACCESS_SHARING_LEVEL="PUBLIC_OPEN" SECURITY_CLASSIFICATION="PUB">
- `02:08:52`   parsed attrs: {'COUNTRY': 'AGO', 'INDICATOR': 'IRFCLDT1_IRFCL32_USD', 'SECTOR': 'S1XS1311', 'FREQUENCY': 'M', 'SCALE': '6', 'METHODOLOGY': 'IRFCL13', 'ACCESS_SHARING_LEVEL': 'PUBLIC_OPEN', 'SECURITY_CLASSIFICATION': 'PUB'}
- `02:08:52`   a US series: None
## C. settle v3.5.3 + force + gates

- `02:08:52` ✅   settled attempt 1
- `02:16:26` ✅   refreshed ~450s
- `02:16:26`   JP02Y: LIVE value=1.531 src=mof-japan asof=mof.go.jp:2026/7/24
- `02:16:26`   CH02Y: NO_FREE_SOURCE value=None src=unresolved_tv_only asof=None
- `02:16:26`   CH03Y: NO_FREE_SOURCE value=None src=unresolved_tv_only asof=None
- `02:16:26` ✅   runner-exec ran
- `02:16:26` ✅   IMF series attrs extracted
- `02:16:26` ✅   v3.5.3 settled
- `02:16:26` ✅   force run wrote
- `02:16:26` ✅   JP02Y LIVE via mof
- `02:16:26` ✅   n_live >= 453
- `02:16:26` ✅   zero bare UNRESOLVED
- `02:16:26` ✅ PASS_ALL — 453 LIVE, JP02Y 1.531 from MOF Japan; IMF key: None
