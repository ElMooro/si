# ops 6889 - bond desk and sector fusion against the deployed ETF compilers (read-only dry run), 2026-11-21T19:28

**Status:** success  
**Duration:** 11.5s  
**Finished:** 2026-11-21T19:29:04+00:00  

## Data

| bond_desk_would_publish_flows | repo_digests | sector_fusion_would_restore |
|---|---|---|
| True | {'etf_issuers': '96a1ad4eed71d1bd', 'etf_native': 'c2a17295b8bfc4ec', 'etf_research': '24187530a946bd5b', 'etf_universe': '7b8da9b7d0a627fd', 'evidence_store': '3baf3781f7450aaf'} | True |

## Log
## 1. What is published today

- `19:28:53` data/bond-desk.json                written 2026-11-20 15:56 | {"generated_at": "2026-11-20T15:56:04.107827+00:00", "version": "3.2.1", "flow_status": "withheld_ValueError"}
- `19:28:53` data/sector-flow-state.json        written 2026-11-10 21:16 | {"generated_at": "2026-11-10T21:15:46.037846+00:00", "contract": "sector-fusion-research.v1", "version": "2.0.0"}
- `19:28:53` data/sector-capital-fusion.json    written 2026-11-20 23:38 | {"generated_at": "2026-11-20T23:37:26.769324+00:00", "contract": "sector-capital-research.v1", "version": "2.0.0"}
- `19:28:53` data/etf-true-flows.json           written 2026-11-21 15:52 | {"generated_at": "2026-11-21T15:49:18.750102+00:00", "contract": "etf-original-research.v1", "version": "3.1.0", "run": "defd3e05d0af8226413.json"}
## 2. Bond desk: the cohort compiler, pins as they are and as they would be

- `19:28:53`   etf_issuers     pinned None | live publication 96a1ad4eed71 | this checkout 96a1ad4eed71 | pin matches live: False | checkout matches live: True
- `19:28:53`   etf_native      pinned 67fbab1a936d | live publication c2a17295b8bf | this checkout c2a17295b8bf | pin matches live: False | checkout matches live: True
- `19:28:53`   etf_research    pinned 726c2ea25632 | live publication 24187530a946 | this checkout 24187530a946 | pin matches live: False | checkout matches live: True
- `19:28:53`   etf_universe    pinned 7b8da9b7d0a6 | live publication 7b8da9b7d0a6 | this checkout 7b8da9b7d0a6 | pin matches live: True | checkout matches live: True
- `19:28:53`   evidence_store  pinned 3baf3781f745 | live publication 3baf3781f745 | this checkout 3baf3781f745 | pin matches live: True | checkout matches live: True
- `19:28:53` with the pins as they are: refused - ValueError: Complete reviewed upstream compiler required
- `19:28:54` with the pins set to this checkout's digests: accepted | binding {"whole_packet_bound": true, "issuer_originals_replayed_here": false}
- `19:28:54` independent proof: {"contract": "bond-flow-independent-check.v1", "cohorts": 11, "windows_checked": 33, "members_checked": 138, "rational_sums": 66, "complete_windows": 0, "partial_windows": 33, "unavailable_windows": 0, "all_checks_passed": true, "source_packet_sha256": "314c4b1d29e5bc9479c82826dbe715affe3ed8e242bf1147a95e54a32938c31f", "source_original_replay_required_separately": true, "historical_point_in_time_v
- `19:28:54`   cohort front_gov              5-observation flow None USD | funds 4 of 5 | partial_coverage_subtotal
- `19:28:54`   cohort belly_gov              5-observation flow None USD | funds 3 of 4 | partial_coverage_subtotal
- `19:28:54`   cohort long_gov               5-observation flow None USD | funds 1 of 5 | partial_coverage_subtotal
- `19:28:54`   cohort tips                   5-observation flow None USD | funds 2 of 5 | partial_coverage_subtotal
- `19:28:54`   cohort ig_credit              5-observation flow None USD | funds 1 of 5 | partial_coverage_subtotal
- `19:28:54`   cohort hy_credit              5-observation flow None USD | funds 4 of 5 | partial_coverage_subtotal
- `19:28:54`   cohort loans                  5-observation flow None USD | funds 1 of 2 | partial_coverage_subtotal
- `19:28:54`   cohort em_debt                5-observation flow None USD | funds 1 of 3 | partial_coverage_subtotal
- `19:28:54`   cohort aggregate              5-observation flow None USD | funds 1 of 3 | partial_coverage_subtotal
- `19:28:54`   cohort muni                   5-observation flow None USD | funds 1 of 2 | partial_coverage_subtotal
- `19:28:54`   cohort equity_core            5-observation flow None USD | funds 3 of 7 | partial_coverage_subtotal
- `19:28:54` ✅ the bond desk's cohort flows compile and prove against the live publication once the pins name the deployed compilers
## 3. Sector fusion: the issuer restore, pins as they are and as they would be

- `19:28:54` with the pins as they are: refused - ValueError: Issuer compiler inventory differs
- `19:29:04` with the pins set to this checkout's digests: restored | {"generated_at": "2026-11-21T15:49:18.750102+00:00", "source_generated_at": "2026-11-21T15:49:18.702674+00:00", "replay": 2, "sectors": 11, "reference_dates": 6673, "reference_source": 3, "reference_calendar": 8, "scope": "Eleven State Street histories and IVV reference dates; other funds retained without selected-source replay."}
- `19:29:04` ✅ the sector fusion restores the issuer packet from the retained originals once the pins name the deployed compilers
- `19:29:04` ✅ read-only; nothing changed
