# ops 6563 - retire engines: switch off 11, delete 24 (backed up)

**Status:** success  
**Duration:** 22.0s  
**Finished:** 2026-10-20T20:56:12+00:00  

## Data

| backups | delete_failed | deleted | functions_now | switched_off |
|---|---|---|---|---|
| s3://justhodl-ops-private-857687956942/backups/deleted-functions/20261020T205550Z/ | 0 | 24 | 902 | 11 |

## Log
- `20:55:50` retime: 0 items
- `20:55:51` disable: 7 items
- `20:55:51` triggers disabled: {"applied": 8}
- `20:55:52` ✅ fan-out manifest: 3 left their tick (ecb-data-daily-updater, ofrapi, ultimate-multi-agent)
- `20:55:54` hold macro-financial-intelligence: upgraded from FREEZE
- `20:55:54` hold ofrapi: upgraded from FREEZE
- `20:55:55` hold treasury-api: upgraded from FREEZE
- `20:56:02` deleted 24 function(s) (packages backed up under backups/deleted-functions/20261020T205550Z/); failed 0 []
- `20:56:02` rules deleted with their only target: none
- `20:56:12` ✅ retired; fleet is 902 functions
