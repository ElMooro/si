# ops 4968 -- 13F live-delta

**Status:** success  
**Duration:** 299.1s  
**Finished:** 2026-08-24T13:06:01+00:00  

## Data

| coverage_pct | detector | full_refresh | funds_parsed | industries | overrides | smallmid_new |
|---|---|---|---|---|---|---|
| 60.9 | rate(30 minutes) | rate(2 hours) | 18 | 99 | 2 | 30 |

## Log
- `13:01:02` mark 2026-08-24T13:01:02+00:00
- `13:01:02` G-1 PASS
- `13:01:02` P0 EDGAR truth sweep — FROM the runner
- `13:01:02`   BERKSHIRE    cik=0001067983 latest=0001193125-26-352200 per=2026-06-30 age=55d 
- `13:01:03`   BRIDGEWATER  cik=0001350694 latest=0001350694-26-000003 per=2026-06-30 age=55d 
- `13:01:03`   RENAISSANCE  cik=0001037389 latest=0001037389-26-000059 per=2026-06-30 age=55d 
- `13:01:03`   AQR          cik=0001167557 latest=0001167557-26-000226 per=2026-06-30 age=55d 
- `13:01:03`   TWO_SIGMA    cik=0001179392 latest=0000899140-26-000855 per=2026-06-30 age=55d 
- `13:01:04`   CITADEL      cik=0001423053 latest=0001104659-26-097200 per=2026-06-30 age=55d 
- `13:01:04`   MILLENNIUM   cik=0001273087 latest=0001273087-26-000007 per=2026-06-30 age=55d 
- `13:01:04`   PERSHING     cik=0001336528 latest=0001172661-26-002336 per=2026-03-31 age=146d 
- `13:01:04`   GREENLIGHT   cik=0001079114 latest=0001172661-24-001512 per=2023-12-31 age=967d STALE
- `13:01:05`   SOROS        cik=0001029160 latest=0000902664-26-003507 per=2026-06-30 age=55d 
- `13:01:05`   TIGER_GLOBAL cik=0001167483 latest=0000919574-26-005427 per=2026-06-30 age=55d 
- `13:01:05`   COATUE       cik=0001135730 latest=0000919574-26-005478 per=2026-06-30 age=55d 
- `13:01:05`   BAUPOST      cik=0001061768 latest=0001061768-26-000010 per=2026-06-30 age=55d 
- `13:01:05`   ELLIOTT      cik=0001791786 latest=0001013594-26-000915 per=2026-06-30 age=55d 
- `13:01:06`   SCION        cik=0001649339 latest=0001649339-25-000007 per=2025-09-30 age=328d STALE
- `13:01:06`   DURATION     cik=0001582202 latest=0001582202-26-000007 per=2026-06-30 age=55d 
- `13:01:06`   LONE_PINE    cik=0001061165 latest=0000919574-26-005485 per=2026-06-30 age=55d 
- `13:01:06`   re-hunt GREENLIGHT -> browse-edgar company='greenlight'
- `13:01:15`     GREENLIGHT: no active filer — marked deregistered (final filing stands, labeled)
- `13:01:15`   re-hunt SCION -> browse-edgar company='scion'
- `13:01:26`     SCION: no active filer — marked deregistered (final filing stands, labeled)
- `13:01:26` P0 done — 2 stale, 2 overrides live
- `13:01:27`   settle justhodl-13f-positions OK (0s)
- `13:01:27`   settle justhodl-sec-13f OK (0s)
- `13:01:27` G0 PASS
- `13:01:27`   rule justhodl-sec-13f-daily -> rate(30 minutes) [ENABLED]
- `13:01:28`   rule justhodl-13f-positions-sched -> rate(2 hours) [ENABLED]
- `13:01:28`   rule justhodl-13f-positions-6h -> DISABLED (phantom)
- `13:01:28`   rule justhodl-page-ai-commentary-daily -> ENABLED
- `13:01:28` G1 PASS
- `13:01:29` G2 PASS — index fresh, 0 mismatches
- `13:05:52`   G3 fresh            PASS
- `13:05:52`   G3 accounted        PASS
- `13:05:52`   G3 industries>=12   PASS
- `13:05:52`   G3 coverage>=50     PASS
- `13:05:52`   G3 reconcile        PASS
- `13:05:52`   G3 new_since        PASS
- `13:05:52`   G3 seed_silent      PASS
- `13:05:52`   G3 smallmid>=3      PASS
- `13:05:52`   G3 ledger           PASS
- `13:05:52`   parsed=18 failed=0 total=18
- `13:05:52`   industries=99 coverage=60.9% ind_net=57392643633 cls_net=57392643633 smallmid=30 stale_funds=['PERSHING', 'GREENLIGHT', 'SCION']
- `13:06:00` G4 PASS — ai-commentary/13f generated_at=2026-08-24T13:06:00.144868+00:00
- `13:06:00`   justhodl.ai serving new markup (try 1)
- `13:06:01`   proxy feed carries industry_flows: True
- `13:06:01` G5 PASS
- `13:06:01` ops 4968 GREEN — 13F is live-delta: EDGAR watched every 30 min with instant rebuilds, industries bought/sold on the page, red flash wired to a seed-silent ledger, AI brief regenerated, stale roster truth-resolved.
