# ops 6890 - bond desk and sector fusion: first runs after the re-pin, 2026-11-21T19:42+00:00

**Status:** success  
**Duration:** 80.9s  
**Finished:** 2026-11-21T19:44:16+00:00  

## Data

| bond_desk_flows_published | sector_flow_state_published |
|---|---|
| True | True |

## Log
## 1. What is deployed

- `19:42:56` justhodl-bond-desk               updated 2026-11-21T19:39:54 | {"bond_flow_qualification.py": true}
- `19:42:56` justhodl-sector-flow-state       updated 2026-11-21T19:40:57 | {"sector_fusion_pins.py": true, "sector_issuer_native.py": true}
- `19:42:57` justhodl-sector-capital-fusion   updated 2026-11-21T19:40:26 | {"sector_fusion_pins.py": true, "sector_issuer_native.py": true}
- `19:42:57` ✅ the three packages carry this checkout's pins and parser copy
## 2. Bond desk

- `19:42:58` before: written 2026-11-20 15:56, version 3.2.1, flow_status withheld_ValueError
- `19:43:20` invoke: status 200 in 23 s | {"ok": true, "world": null, "regime": "UNAVAILABLE", "regions_live": 1, "tiles": 10, "analogs": "OK", "ai": "UNQUALIFIED_REQUIRED_INPUT"}
- `19:43:21` after: written 2026-11-21 19:43, generated_at 2026-11-21T19:43:19.936972+00:00, flow_status dated_issuer_cohorts, cohort contract bond-flow-cohorts.v1
- `19:43:21`   cohort aggregate              partial_coverage_subtotal | funds included 1 | complete total None | coverage subtotal 285516240.000000
- `19:43:21`   cohort belly_gov              partial_coverage_subtotal | funds included 3 | complete total None | coverage subtotal -4490953917.600000
- `19:43:21`   cohort em_debt                partial_coverage_subtotal | funds included 1 | complete total None | coverage subtotal -491487640.400000
- `19:43:21`   cohort equity_core            partial_coverage_subtotal | funds included 3 | complete total None | coverage subtotal -4306210225.200000
- `19:43:21`   cohort front_gov              partial_coverage_subtotal | funds included 4 | complete total None | coverage subtotal 2175129339.010000
- `19:43:21`   cohort hy_credit              partial_coverage_subtotal | funds included 4 | complete total None | coverage subtotal -1665202825.850000
- `19:43:21`   cohort ig_credit              partial_coverage_subtotal | funds included 1 | complete total None | coverage subtotal 206388402.300000
- `19:43:21`   cohort loans                  partial_coverage_subtotal | funds included 1 | complete total None | coverage subtotal -136725438.250000
- `19:43:21`   cohort long_gov               partial_coverage_subtotal | funds included 1 | complete total None | coverage subtotal 866570699.300000
- `19:43:21`   cohort muni                   partial_coverage_subtotal | funds included 1 | complete total None | coverage subtotal -724093236.000000
- `19:43:21`   cohort tips                   partial_coverage_subtotal | funds included 2 | complete total None | coverage subtotal -102122693.850000
- `19:43:21` ✅ the bond desk publishes its dated issuer cohorts again (was withheld_ValueError since 2026-11-11)
## 3. Sector fusion

- `19:43:21` before: written 2026-11-10 21:16, generated_at 2026-11-10T21:15:46.037846+00:00, contract sector-fusion-research.v1
- `19:44:15` invoke: status 200 in 54 s | {"statusCode": 200, "body": "{\"contract\": \"sector-fusion-request.v1\", \"kind\": \"flow\", \"request_id\": \"7e487f70-584a-468c-b410-cbded4ceef3d\", \"execution_id\": \"7e487f70-584a-468c-b410-cbded4ceef3d\", \"started_at\": \"2026-11-21T19:43:22.243186+00:00\", \"status\": \"complete\", \"phase\": \"complete\", \"completed_at\": \"2026-11-21T19:44:15.336529+00:00\", \"published\": true, \"generated_at\": \"2026-11-21T19:43:29.168871+00:00\", \"quality\": {\"status\": \"source_check_due\", \"configured_sectors\": 11, \"issuer_price_five_window_available\": 11, \"independent_investment_votes
- `19:44:15` after: written 2026-11-21 19:44, generated_at 2026-11-21T19:43:29.168871+00:00, contract sector-fusion-research.v1, sectors 11
- `19:44:15` ✅ the sector flow state is published again (last publication before this was 2026-11-10T21:15:46.037846+00:00)
- `19:44:16` sector-capital-fusion (not invoked): written 2026-11-20 23:38, generated_at 2026-11-20T23:37:26.769324+00:00
