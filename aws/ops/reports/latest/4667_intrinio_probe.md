# ops 4667 — Intrinio: pre-2023 ICE or not

**Status:** failure  
**Duration:** 6.3s  
**Finished:** 2026-08-14T20:59:46+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. Store key in SSM (SecureString)

- `20:59:40` ✅   stored + read back OK (len=44, value never logged)
## 2. THE PROBE — first date from 1996 request

- `20:59:40`   [economic index historical] HTTP 404 — {"error":"Indices not found","message":"An error occurred. Please contact success@intrinio.com with the details."}
- `20:59:42`   [economic index (no metric)] HTTP 404 — {"error":"Indices not found","message":"An error occurred. Please contact success@intrinio.com with the details."}
- `20:59:44`   [all economic indices search] shape=['messages', 'indices', 'next_page'] sample={"messages":[],"indices":[],"next_page":null}
## verdict

- `20:59:46` ✗   no dated payload returned — endpoint shape or entitlement unclear; raw bodies above are the evidence for a corrected retry. Failing loud so this cannot read as a clean answer.
