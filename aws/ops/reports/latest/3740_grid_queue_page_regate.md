# ops 3740 — grid-queue page + field-coverage audit

**Status:** failure  
**Duration:** 16.4s  
**Finished:** 2026-07-22T21:40:27+00:00  

## Error

```
SystemExit: 1
```

## Data

| failed | page | queue_mw | unrendered | verdict |
|---|---|---|---|---|
| G2_field_coverage | grid-queue.html | 120495.1 | caiso_active_rows,completed_projects,eia_industrial_plants,eia_load_states,withdrawn_projects | FAIL |

## Log
## G0 — settle v1.0.3 and refresh the artifact

- `21:40:11`   v1.0.3 settled after 0s
- `21:40:11` PASS G0_settle_103 — v1.0.3 deployed
- `21:40:27`   artifact refreshed on v1.0.3
## G1 — page served

- `21:40:27` PASS G1_page_live — len=12563 missing_markers=[]
## G2 — field coverage (live S3 artifact vs page render paths)

- `21:40:27`   top-level keys: ['attribution', 'coverage', 'gaps', 'generated_at', 'hotspots', 'industrial_load', 'method', 'planned_capacity', 'queue', 'version']
- `21:40:27`   per-row keys:   ['active_mw', 'active_projects', 'by_county_mw', 'by_fuel_mw', 'caiso_active_rows', 'capacity_mw', 'completed_mw', 'completed_projects', 'completion_ratio', 'county', 'eia_industrial_plants', 'eia_load_states', 'entity', 'fuel', 'industrial_load_yoy_pct', 'industrial_plants', 'iso_queues_live', 'iso_queues_missing', 'large_projects', 'legs', 'load_3m_pct', 'mom_3m_pct', 'mw', 'n_industrial', 'online', 'period', 'planned_uprate_mw', 'plant', 'project', 'read', 'sales_gwh', 'sector', 'state', 'states', 'status', 'tech', 'upcoming_uprates', 'uprate_by_sector', 'uprate_by_state', 'withdrawal_ratio', 'withdrawn_mw', 'withdrawn_projects', 'yoy_pct']
- `21:40:27` FAIL G2_field_coverage — UNRENDERED KEYS: ['caiso_active_rows', 'completed_projects', 'eia_industrial_plants', 'eia_load_states', 'withdrawn_projects']
## G3 — nav manifest (served)

- `21:40:27` PASS G3_nav — listed under ('Macro & Liquidity', 'Grid Queue')
## G4 — declared gaps + queue sanity

- `21:40:27`   coverage: {'iso_queues_live': ['CAISO'], 'iso_queues_missing': ['ERCOT', 'PJM', 'MISO', 'ISO-NE', 'NYISO', 'SPP'], 'caiso_active_rows': 272, 'eia_industrial_plants': 5, 'eia_load_states': 62}
- `21:40:27`     gap: ERCOT GIS queue report ID unresolved (ops 3734) — excluded from v1
- `21:40:27`     gap: PJM Data Miner 2 requires a subscription key (401)
- `21:40:27`     gap: MISO public queue endpoint returns 404
- `21:40:27`     gap: LBNL 'Queued Up' blocked (403) from Lambda IPs
- `21:40:27`     gap: EPA ECHO serves compliance data, not construction permits — excluded rather than used as a false permit proxy
- `21:40:27` PASS G4_fuel_labels — fuel labels clean
- `21:40:27` PASS G4_declared_gaps — 5 gaps declared, queue 120495.1 MW / 272 projects
## VERDICT

- `21:40:27` ✗ gates failed: ['G2_field_coverage']
