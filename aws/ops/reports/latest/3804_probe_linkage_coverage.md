# ops 3804 — can supply-chain-linkage widen dependency coverage?

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-07-24T16:33:57+00:00  

## Data

| age_hours | bytes | dependency_today | distinct_symbols_in_linkage | graph_overlap | last_modified | ledger | linkage_overlap | union_overlap |
|---|---|---|---|---|---|---|---|---|
| 1.5 | 25737 |  |  |  | 2026-07-24 15:01:22 |  |  |  |
|  |  |  | 49 |  |  |  |  |  |
|  |  | 156 |  | 182 |  | 1269 | 48 | 205 |

## Log
## 1. Feed presence + freshness

- `16:33:57` ✅ LINK.exists :: 25737 bytes
- `16:33:57` ✅ LINK.fresh :: 1.5h old
## 2. ACTUAL schema

- `16:33:57`   engine                       str      20
- `16:33:57`   version                      str      5
- `16:33:57`   generated_at                 str      32
- `16:33:57`   universe_size                int      49
- `16:33:57`   n_systemic_hubs              int      0
- `16:33:57`   n_with_high_severity_flags   int      44
- `16:33:57`   max_degree_in_universe       int      0
- `16:33:57`   entries                      list     49
- `16:33:57`   geographic_risk_tiers        dict     25
- `16:33:57`   methodology                  dict     7
- `16:33:57`   academic_basis               list     2
- `16:33:57`   duration_seconds             float    36.2
## 3. Symbol universe

- `16:33:57`   sample: ['AAPL', 'ABBV', 'ABT', 'ACN', 'ADBE', 'AMD', 'AMZN', 'AVGO', 'BAC', 'BRK-B', 'CAT', 'CMCSA', 'COST', 'CRM', 'CSCO', 'CVX', 'DHR', 'DIS', 'GOOGL', 'HD', 'IBM', 'INTU', 'JNJ', 'JPM', 'KO', 'LIN']
## 4. Directional relationships present?

- `16:33:57`   field 'suppliers' appears 49 times
- `16:33:57`   field 'customers' appears 49 times
- `16:33:57`   field 'n_suppliers' appears 49 times
- `16:33:57`   field 'n_customers' appears 49 times
- `16:33:57` ✅ LINK.directional :: directional fields present
## 5. What would coverage become?

- `16:33:57` ✅ COVERAGE.gain :: union would reach 205 names vs 182 from the curated graph alone
## VERDICT

- `16:33:57` ⚠ Linkage adds little (48 vs 182). Expanding coverage then means widening the PRODUCER's universe, not adding a join.
- `16:33:57` ✅ PASS_ALL — probe complete
