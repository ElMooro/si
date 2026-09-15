# ops 5581 -- provider catalog refresh (data.html): invoke after deploy, verify the new providers are listed

**Status:** success  
**Duration:** 393.5s  
**Finished:** 2026-09-15T13:34:34+00:00  

## Log
- `13:34:34` ✅ invoke status=200 error=None payload={"statusCode": 200, "body": "{\"ok\": true, \"providers\": 73, \"datasets\": 821822, \"keys\": 1932288, \"bytes\": 533796249696, \"gb\": 533.8}"}
- `13:34:34` ✅ data/provider-catalog.json: 73 providers, generated None; totals={"providers": 73, "datasets": 821822, "keys": 1932288, "bytes": 533796249696, "gb": 533.8}
- `13:34:34`   openfigi: keys=None hot=null
- `13:34:34`   cryptoquant: keys=None hot=null
- `13:34:34`   fmp: keys=None hot=null
- `13:34:34` ✅ GREEN -- data.html now lists FMP and every other provider
