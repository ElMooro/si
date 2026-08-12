# ops 4644 — dxy canary on the board

**Status:** failure  
**Duration:** 166.1s  
**Finished:** 2026-08-12T22:35:54+00:00  

## Error

```
SystemExit: 1
```

## Data

| canary |
|---|
| {} |

## Log
## deploy (ops-side) + settle

- `22:33:09` ✅   [deploy] v2.1.5 live
## run + parity

- `22:33:13` ✗   [canary] CONTRACT MISS — board parity: None / None
- `22:33:13` ✗   [trio] CONTRACT MISS — board carries ['blackswan_strip']
## edge

- `22:35:54` ✗   [edge] CONTRACT MISS — edge board carries the dxy dial
## verdict

- `22:35:54` ✗ dxy canary: 3 red
