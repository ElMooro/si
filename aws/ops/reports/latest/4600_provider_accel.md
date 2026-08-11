# ops 4600 — provider acceleration

**Status:** failure  
**Duration:** 125.1s  
**Finished:** 2026-08-11T03:06:06+00:00  

## Error

```
SystemExit: 1
```

## Log
## 0. FRED untouched (guard, read-only)

- `03:04:02` ✅   [fred-guard] scope=full_catalog knob=100 ver=2.3.0 rpm=96.0 imported=68452 — nothing written
## 1. Settle walker, launch retry sweeps

- `03:04:02`   oecd before: n_failures=991 retried_ok=0
- `03:04:02`   oecd retry sweep launched
- `03:04:02`   statcan before: n_failures=291 retried_ok=2
- `03:04:02`   statcan retry sweep launched
## 2. Poll sweeps (walker budget ~700s)

- `03:04:23`   oecd progress: attempted=2 failures_now=991
- `03:04:23`   statcan progress: attempted=2 failures_now=290
## 3. Sweep results

- `03:06:05`   oecd: recovered=0 refailed=2 failures 991->991; remaining top: [('HTTPError: HTTP Error 429: Too Man', 972), ('HTTPError: HTTP Error 524: <none>', 9), ('HTTPError: HTTP Error 500: Interna', 7), ('HTTPError: HTTP Error 404: Not Fou', 3)]
- `03:06:05` ✅   [oecd] retry pass executed (2 attempted)
- `03:06:05` ✗   [oecd] CONTRACT MISS — constrained variants recovered 0 of the denied set
- `03:06:05`   statcan: recovered=1 refailed=1 failures 291->290; remaining top: [('HTTPError: HTTP Error 429: Too Man', 106), ('URLError: <urlopen error [Errno 10', 83), ('URLError: <urlopen error [Errno 11', 72), ('HTTPError: HTTP Error 502: Bad Gat', 24)]
- `03:06:05` ✅   [statcan] retry pass executed (2 attempted)
## 4. sec-bulk: schedule + kick

- `03:06:06`   schedule exists
- `03:06:06`   companyfacts.zip  08-11 02:50  1400.4 MB
- `03:06:06`   submissions.zip  08-09 04:35  1556.6 MB
- `03:06:06` ✅   kicked (multi-GB pull — freshness reads next check-in)
## verdict

- `03:06:06` ✗ provider acceleration: 1 red
