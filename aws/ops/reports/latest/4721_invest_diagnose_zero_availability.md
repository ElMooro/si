# ops 4721 — diagnose universal leg unavailability

**Status:** success  
**Duration:** 1.0s  
**Finished:** 2026-08-15T21:20:33+00:00  

## Data

| code_sha256 | deployed_fleet_io_bytes | deployed_fleet_io_has_bracket_search | fleet_io_in_zip | last_modified | shared_modules_bundled | zip_file_count |
|---|---|---|---|---|---|---|
| DkgyV4+YTjrghhHw |  |  |  | 2026-08-15T21:16:25.000+0000 |  |  |
|  |  |  |  |  |  | 35 |
|  |  |  | True |  |  |  |
|  | 6122 | True |  |  |  |  |
|  |  |  |  |  | ['impact_mapper.py', 'evidence_weights.py'] |  |

## Log
## 1. Zip-settle: is the deployed code actually current?

- `21:20:33` ✅   deployed fleet_io.py IS current (contains '_BRACKET_RE')
## 2. Direct read_leg_value() call against real S3

- `21:20:33`   leg.source = 'fleet:data/asia-leads.json:korea_exports.yoy_pct'
- `21:20:33`   parsed: key='data/asia-leads.json' path='korea_exports.yoy_pct'
- `21:20:33`   doc is None: False
- `21:20:33`   doc top-level keys: ['disclaimer', 'elapsed_s', 'engine', 'generated_at', 'korea_exports', 'korea_flash', 'korea_flash_tape', 'methodology', 'siblings', 'sources', 'taiwan_exports', 'taiwan_orders', 'version']
- `21:20:33`   dig(doc, path) = 47.96
- `21:20:33`   read_leg_value(source) = 47.96
- `21:20:33` ✅   resolves correctly in THIS checkout's code: 47.96
## Verdict

