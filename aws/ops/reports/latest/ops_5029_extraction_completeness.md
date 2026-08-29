## P0 denominator -- what sdmx-walker has downloaded

**Status:** success  
**Duration:** 4.2s  
**Finished:** 2026-08-29T13:25:29+00:00  

## Data

| coverage_bytes_pct | flows_done | flows_total | pages_held | remaining_flows |
|---|---|---|---|---|
| 0.71 | 79 | 8147 | 3961 | 8068 |

## Log
- `13:25:27`   data/warm/eurostat/data/: 8147 files, 8.86 GB
- `13:25:27`   written between 2026-08-06 19:26:29+00:00 and 2026-08-25 01:39:43+00:00
## P1 numerator -- what was actually parsed

- `13:25:27`   flows_done=79  n_pages=3466  series_count=1733000  buffer=233  updated_at=2026-08-09T02:40:20+00:00
- `13:25:27`   first 12 flows parsed: ['AACT_ALI01', 'AACT_ALI01_R', 'AACT_ALI02', 'AACT_ALI02_R', 'AACT_EAA01', 'AACT_EAA01_R', 'AACT_EAA02', 'AACT_EAA02_R', 'AACT_EAA03', 'AACT_EAA04', 'AACT_EAA05', 'AACT_EAA05_R']
- `13:25:27`   LAST flow parsed: APRO_CPNHR  (the next one, flow #80, is the poison pill that killed every run since)
- `13:25:27`   COVERAGE by flow count : 79 / 8147  = 0.97%
- `13:25:27`   COVERAGE by RAW BYTES  : 0.06 GB / 8.86 GB = 0.71%
- `13:25:27`   remaining: 8068 flows, 8.80 GB unparsed
- `13:25:27`   largest unparsed flows:
- `13:25:27`     NAIO_10_FGDE                                       42.0 MB
- `13:25:27`     PROJ_19RP3                                         42.0 MB
- `13:25:27`     NAMA_10_A64_P5                                     42.0 MB
- `13:25:27`     DEMO_R_MLIFE                                       41.9 MB
- `13:25:27`     EF_OGA_TYPE                                        41.9 MB
- `13:25:27`     EF_LF_FAM                                          41.9 MB
- `13:25:27`     NAIO_10_FGDEE                                      41.9 MB
- `13:25:27`     BOP_EUINS6_M                                       41.9 MB
## P2 what we HOLD -- and is it intact

- `13:25:28`   data/providers/eurostat/series/: 3961 current objects, 1.03 GB
- `13:25:28`   page-0000.json                 page=0 count=500 rows=500  sample_id=None
- `13:25:28`       fields: ['dims', 'engines', 'first_obs', 'flow', 'freq', 'geo', 'id', 'last_obs', 'last_value', 'name', 'raw_key', 'source_url']
- `13:25:29`   page-1980.json                 page=1980 count=500 rows=500  sample_id=None
- `13:25:29`       fields: ['dims', 'engines', 'first_obs', 'flow', 'freq', 'geo', 'id', 'last_obs', 'last_value', 'name', 'raw_key', 'source_url']
- `13:25:29`   page-3960.json                 page=3960 count=500 rows=500  sample_id=None
- `13:25:29`       fields: ['dims', 'engines', 'first_obs', 'flow', 'freq', 'geo', 'id', 'last_obs', 'last_value', 'name', 'raw_key', 'source_url']
- `13:25:29`   pages readable, rows present -> the 79 parsed flows are INTACT on disk (the purge removes noncurrent versions only; every current page above survives)
## P3 forecast -- cost of actually finishing

- `13:25:29`   observed rate: 27416.8 series per MB of raw eurostat
- `13:25:29`   remaining 8.80 GB -> ~241.3M more series, ~482544 more pages, ~125.8 GB stored (avg page 255 KB)
- `13:25:29`   ONE-TIME write cost at $0.005/1k PUT: ~$2.41
- `13:25:29`   storage at $0.023/GB-mo: ~$2.89/month once complete
- `13:25:29`   (v2 hash-skips identical pages, so reruns after this cost nothing -- the $239 anomaly was 100%% rewrite waste, not extraction)
## P4 the gdelt lane (provider-catalog, same pattern)

- `13:25:29`   data/providers/gdelt/: 804 current objects, 0.06 GB
- `13:25:29`     page-000.json                                   62 KB  08-29 12:53
- `13:25:29`     page-001.json                                   63 KB  08-29 12:53
- `13:25:29`     page-002.json                                   62 KB  08-29 12:53
- `13:25:29`     page-003.json                                   62 KB  08-29 12:53
- `13:25:29`     page-004.json                                   62 KB  08-29 12:53
- `13:25:29`     page-005.json                                   62 KB  08-29 12:53
- `13:25:29`   no gdelt extractor state (An error occurred (NoSuchKey) when calling the GetObject ope) -- provider-catalog writes that prefix on its own cadence
- `13:25:29`   -> data/ops/eurostat-extraction-coverage.json
- `13:25:29` ops 5029 GREEN -- coverage measured against the real denominator
