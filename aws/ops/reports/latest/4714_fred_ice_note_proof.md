# ops 4714 — additive ICE note on FRED card, FRED count PROVEN unchanged

**Status:** failure  
**Duration:** 241.7s  
**Finished:** 2026-08-15T19:26:59+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. BEFORE snapshot

- `19:22:57`   FRED before: n_keys=279497 total_mb=11230.24 note=None
- `19:22:57` ✅   [before] FRED entry found in current hub
## 2. Kick provider-catalog

## 3. AFTER snapshot — wait for the new note to appear

- `19:26:59`   FRED after: n_keys=279497 total_mb=11230.24 note=None
## 4. THE PROOF — count identical, ICE note present

- `19:26:59` ✅   [unchanged] FRED n_keys IDENTICAL before/after (279497 == 279497)
- `19:26:59` ✅   [unchanged] FRED total_mb IDENTICAL before/after (11230.24 == 11230.24)
- `19:26:59` ✗   [additive] CONTRACT MISS — FRED note now contains the additive ICE annotation: None
- `19:26:59` ✗   [additive] CONTRACT MISS — the ORIGINAL fred note content is still present (this was an append, not a replace)
## verdict

- `19:26:59` ✗ fred-ice-note: 2 red
