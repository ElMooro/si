# Re-verify with logged_at field name (158 used wrong field)

**Status:** success  
**Duration:** 2.6s  
**Finished:** 2026-04-25T19:13:01+00:00  

## Data

| n_complete_with_bp | n_signals | scored_ok |
|---|---|---|
| 0 | 4854 | 0 |

## Log
- `19:13:01`   Total signals: 4854
## 1. logged_at distribution by status

- `19:13:01` 
  complete       (n=889, with logged_at: 889):
- `19:13:01`     Oldest: 2026-03-12T00:51:38.052144+00:00
- `19:13:01`     Newest: 2026-04-18T09:10:13.947131+00:00
- `19:13:01` 
  partial        (n=1612, with logged_at: 1612):
- `19:13:01`     Oldest: 2026-03-11T09:13:18.581768+00:00
- `19:13:01`     Newest: 2026-04-23T21:10:14.707551+00:00
- `19:13:01` 
  pending        (n=2035, with logged_at: 2035):
- `19:13:01`     Oldest: 2026-03-26T15:10:14.269417+00:00
- `19:13:01`     Newest: 2026-04-25T15:10:15.319981+00:00
- `19:13:01` 
  unscoreable    (n=318, with logged_at: 318):
- `19:13:01`     Oldest: 2026-04-11T03:10:13.682328+00:00
- `19:13:01`     Newest: 2026-04-24T21:10:14.486229+00:00
## 2. baseline_price coverage on complete + partial signals

- `19:13:01` 
  complete:
- `19:13:01`     with baseline_price>0: 0
- `19:13:01`     without:               889
- `19:13:01`     WITHOUT bp logged_at range: 2026-03-12T00:51:38.052144+00:00 → 2026-04-18T09:10:13.947131+00:00
- `19:13:01` 
  partial:
- `19:13:01`     with baseline_price>0: 940
- `19:13:01`     without:               672
- `19:13:01`     WITH bp logged_at range: 2026-03-11T09:13:18.581768+00:00 → 2026-03-26T09:10:14.376940+00:00
- `19:13:01`     WITHOUT bp logged_at range: 2026-03-12T00:51:38.598633+00:00 → 2026-04-23T21:10:14.707551+00:00
## 3. The legacy-data hypothesis

- `19:13:01`   ops/112 comment said pre-Week-1-fix signals lack baseline_price.
- `19:13:01`   If true:
- `19:13:01`     - Old signals (no logged_at) → no baseline → unscoreable forever
- `19:13:01`     - Newer signals (have logged_at + baseline) → scoreable on day_7
- `19:13:01`   Verify: are 'with bp' signals NEWER than 'without bp' signals?
## 4. Newest complete signal WITH baseline_price

- `19:13:01` ⚠   ❌ NO complete signal has both baseline_price AND logged_at
- `19:13:01` ⚠   This is the bug — completes are losing baseline somehow
## 5. Are ANY outcomes scored properly?

- `19:13:01` Done
