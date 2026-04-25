# Add wildcard public-read for *.json at bucket root

**Status:** success  
**Duration:** 0.6s  
**Finished:** 2026-04-25T22:49:13+00:00  

## Data

| root_jsons_covered | statements_after |
|---|---|
| 14 | 15 |

## Log
- `22:49:12`   Current statements: 14
- `22:49:12`   + PublicReadAllRootJSON: arn:aws:s3:::justhodl-dashboard-live/*.json
- `22:49:12`   (matches every .json at bucket root)
- `22:49:13` ✅   Policy updated
## Verify — every root .json should be publicly readable

- `22:49:13`   Root .json files: 14
- `22:49:13`     ✅ crypto-data.json
- `22:49:13`     ✅ crypto-intel.json
- `22:49:13`     ✅ data-peek.json
- `22:49:13`     ✅ data.json
- `22:49:13`     ✅ edge-data.json
- `22:49:13`     ✅ flow-data.json
- `22:49:13`     ✅ intelligence-report.json
- `22:49:13`     ✅ liquidity-data.json
- `22:49:13`     ✅ manifest.json
- `22:49:13`     ✅ predictions.json
- `22:49:13`     ✅ pro-data.json
- `22:49:13`     ✅ repo-data.json
- `22:49:13`     ✅ stock-picks-data.json
- `22:49:13`     ✅ valuations-data.json
- `22:49:13` ✅ 
  All 14 root JSONs now publicly readable
- `22:49:13` Done — refresh liquidity.html etc. to verify (hard refresh, CORS cache)
