# ops 6889 - bond desk and sector fusion against the deployed ETF compilers (read-only dry run), 2026-11-21T19:27

**Status:** success  
**Duration:** 1.6s  
**Finished:** 2026-11-21T19:27:53+00:00  

## Data

| bond_desk_would_publish_flows | repo_digests | sector_fusion_would_restore |
|---|---|---|
| False | {'etf_native': 'c2a17295b8bfc4ec', 'etf_research': '24187530a946bd5b', 'etf_universe': '7b8da9b7d0a627fd', 'evidence_store': '3baf3781f7450aaf'} | False |

## Log
## 1. What is published today

- `19:27:51` data/bond-desk.json                written 2026-11-20 15:56 | {"generated_at": "2026-11-20T15:56:04.107827+00:00", "version": "3.2.1", "flow_status": "withheld_ValueError"}
- `19:27:52` data/sector-flow-state.json        written 2026-11-10 21:16 | {"generated_at": "2026-11-10T21:15:46.037846+00:00", "contract": "sector-fusion-research.v1", "version": "2.0.0"}
- `19:27:52` data/sector-capital-fusion.json    written 2026-11-20 23:38 | {"generated_at": "2026-11-20T23:37:26.769324+00:00", "contract": "sector-capital-research.v1", "version": "2.0.0"}
- `19:27:52` data/etf-true-flows.json           written 2026-11-21 15:52 | {"generated_at": "2026-11-21T15:49:18.750102+00:00", "contract": "etf-original-research.v1", "version": "3.1.0", "run": "defd3e05d0af8226413.json"}
## 2. Bond desk: the cohort compiler, pins as they are and as they would be

- `19:27:52`   etf_issuers     pinned None | live publication 96a1ad4eed71 | this checkout None | pin matches live: False | checkout matches live: False
- `19:27:52`   etf_native      pinned 67fbab1a936d | live publication c2a17295b8bf | this checkout c2a17295b8bf | pin matches live: False | checkout matches live: True
- `19:27:52`   etf_research    pinned 726c2ea25632 | live publication 24187530a946 | this checkout 24187530a946 | pin matches live: False | checkout matches live: True
- `19:27:52`   etf_universe    pinned 7b8da9b7d0a6 | live publication 7b8da9b7d0a6 | this checkout 7b8da9b7d0a6 | pin matches live: True | checkout matches live: True
- `19:27:52`   evidence_store  pinned 3baf3781f745 | live publication 3baf3781f745 | this checkout 3baf3781f745 | pin matches live: True | checkout matches live: True
- `19:27:52` with the pins as they are: refused - ValueError: Complete reviewed upstream compiler required
- `19:27:52` ⚠ the live publication was not compiled by the files in this checkout; re-pinning to them would not help
## 3. Sector fusion: the issuer restore, pins as they are and as they would be

- `19:27:52` with the pins as they are: refused - ValueError: Issuer compiler inventory differs
- `19:27:53` ⚠ sector fusion dry run: ValueError: Issuer compiler inventory differs
- `19:27:53` Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_6889_two_desks_against_the_new_etf_compilers.py", line 118, in main
    issuance = issuer.restore(fpacket, fread)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/shared/sector_fusion_issuer.py", line 36, in restore
    if set(run['compilers'])!=set(ETF_COMPILERS):raise ValueError('Issuer compiler inventory differs')
                                                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ValueError: Issuer compiler inventory differs

- `19:27:53` ✅ read-only; nothing changed
