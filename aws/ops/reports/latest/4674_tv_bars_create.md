# ops 4674 — create tv-bars engine + prove the rail

**Status:** failure  
**Duration:** 90.8s  
**Finished:** 2026-08-14T22:30:39+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. Create or update the function from repo source

- `22:29:09` ✅   CREATED justhodl-tv-bars (deploy-lambdas skips new functions outside MISSING-mode dispatch)
- `22:29:17` ✅   [deploy] justhodl-tv-bars Active
## 2. Schedule (hourly) so convergence is autonomous

- `22:29:18` ✅   schedule justhodl-tv-bars-hourly -> rate(1 hour)
## 3. Pull 3 ICE symbols (sync)

- `22:30:39`   handler: {"statusCode": 200, "body": "{\"ok\": true, \"pulled\": 0, \"failed\": 3, \"done\": 0, \"catalog\": 192, \"status\": \"converging\", \"recent_failures\": {\"BAMLH0A0HYM2\": \"RuntimeError: handshake: b'HTTP/1.1 400 Bad Request'\", \"BAMLC0A2CAA\": \"RuntimeError: handshake: b'HTTP/1.1 400 Bad Request'\", \"BAMLH0A3HYC\": \"RuntimeError: handshake: b'HTTP/1.1 400 Bad Request'\"}}"}
## 4. What landed

- `22:30:39`   BAMLH0A0HYM2: nothing banked
- `22:30:39`   BAMLC0A2CAA: nothing banked
- `22:30:39`   BAMLH0A3HYC: nothing banked
- `22:30:39`   failures: {'BAMLH0A0HYM2': "RuntimeError: handshake: b'HTTP/1.1 400 Bad Request'", 'BAMLC0A2CAA': "RuntimeError: handshake: b'HTTP/1.1 400 Bad Request'", 'BAMLH0A3HYC': "RuntimeError: handshake: b'HTTP/1.1 400 Bad Request'"}
- `22:30:39` ✗   [depth] CONTRACT MISS — 0/3 symbols carry pre-2020 history
## verdict

- `22:30:39` ✗ tv rail: 1 red — protocol evidence above drives the next revision
