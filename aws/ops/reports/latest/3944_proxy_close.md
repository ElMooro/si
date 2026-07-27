# ops 3944 — proxy verify + v3.5.1 close

**Status:** failure  
**Duration:** 201.3s  
**Finished:** 2026-07-27T01:01:38+00:00  

## Error

```
SystemExit: 1
```

## Log
## runner-verify the /gov proxy (worker must be live)

- `00:58:17` ✅   mof via proxy: attempt 1, 1923b, marker found
- `00:58:17`   imf attempt 1: HTTP Error 403: Forbidden
- `00:58:38`   imf attempt 2: 3284b, no marker; head '<?xml version=\'1.0\' encoding=\'UTF-8\'?><message:StructureSpecificData xmlns:ss="http://www.'
- `00:58:58`   imf attempt 3: 3284b, no marker; head '<?xml version=\'1.0\' encoding=\'UTF-8\'?><message:StructureSpecificData xmlns:ss="http://www.'
- `00:59:18`   imf attempt 4: 3284b, no marker; head '<?xml version=\'1.0\' encoding=\'UTF-8\'?><message:StructureSpecificData xmlns:ss="http://www.'
- `00:59:38`   imf attempt 5: 3284b, no marker; head '<?xml version=\'1.0\' encoding=\'UTF-8\'?><message:StructureSpecificData xmlns:ss="http://www.'
- `00:59:58`   imf attempt 6: 3284b, no marker; head '<?xml version=\'1.0\' encoding=\'UTF-8\'?><message:StructureSpecificData xmlns:ss="http://www.'
- `01:00:18`   imf attempt 7: 3284b, no marker; head '<?xml version=\'1.0\' encoding=\'UTF-8\'?><message:StructureSpecificData xmlns:ss="http://www.'
- `01:00:38`   imf attempt 8: 3284b, no marker; head '<?xml version=\'1.0\' encoding=\'UTF-8\'?><message:StructureSpecificData xmlns:ss="http://www.'
- `01:00:58`   imf attempt 9: 3284b, no marker; head '<?xml version=\'1.0\' encoding=\'UTF-8\'?><message:StructureSpecificData xmlns:ss="http://www.'
- `01:01:18`   imf attempt 10: 3284b, no marker; head '<?xml version=\'1.0\' encoding=\'UTF-8\'?><message:StructureSpecificData xmlns:ss="http://www.'
- `01:01:38` ✗ proxy not serving both — worker deploy issue
