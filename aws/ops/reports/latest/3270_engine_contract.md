# ops 3270 — engine contract: audit with teeth

**Status:** failure  
**Duration:** 32.3s  
**Finished:** 2026-07-13T18:34:28+00:00  

## Error

```
SystemExit: 1
```

## Data

| active | active_after | dormant | gated_still_dormant | macro_untagged | n_fails | n_warns | notes_total | playbook_rules | ticker_tagged | tickers_indexed | true_dead | verdict | weeks_gated |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 197 |  | 10 |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | 9 |  | 1 |
|  | 197 |  | 1 |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | 0 |  |  | 3322 | 563 | 3322 | 536 |  |  |  |
|  |  |  |  |  | 1 | 0 |  |  |  |  |  | FAIL |  |

## Log
## 1. Dormant classification from the weekly cache

- `18:33:58`   GATED  20wk — Country Import Prices
- `18:33:58`   DEAD   0 data — Altcoin Dominance
- `18:33:58`   DEAD   0 data — Buying the Dip indicators
- `18:33:58`   DEAD   0 data — Crypto Projects Dominance
- `18:33:58`   DEAD   0 data — Dubai Index
## 2. Contract enforcement: gate 60→13, wake them

- `18:33:58` ✅ gate lowered in source
- `18:33:58`   zip: 84279 bytes
## 1. Lambda

- `18:33:58`   Lambda exists — updating
- `18:34:04` ✅   ✓ updated justhodl-wl-engines
## 2. EB rule + permissions

- `18:34:05`   rule already correct: wl-engines-daily (cron(30 22 ? * TUE-SAT *))
- `18:34:05` ✅   ✓ target → justhodl-wl-engines
- `18:34:05` ✅   ✓ added invoke permission
## 3. Notes coverage census

- `18:34:28` ✅ every note flows: stance→4 consumers, rules→playbook, macro→themes, all→brain (proven ops 3259–3266)
- `18:34:28` ✗ 1 gated panels still dormant post-enforcement
