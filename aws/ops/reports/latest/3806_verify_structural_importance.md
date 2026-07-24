# ops 3806 — 'how crucial to industry' on both surfaces

**Status:** failure  
**Duration:** 1.3s  
**Finished:** 2026-07-24T17:00:14+00:00  

## Error

```
SystemExit: 1
```

## Data

| coverage_gain | dependency | ledger | si_max | si_median | si_min | si_p25 | si_p75 | structural_importance | unmapped_but_scored |
|---|---|---|---|---|---|---|---|---|---|
| 12x | 156 | 1847 |  |  |  |  |  | 1847 |  |
|  |  |  | 73.0 | 23.9 | 0.0 | 16.6 | 32.8 |  |  |
|  |  |  |  |  |  |  |  |  | 1847 |

## Log
## Feed

- `17:00:13` ✅ FEED.v44 :: v4.4
- `17:00:13` ✅ FEED.coverage :: 1847 names vs 156 via the supply-chain map
- `17:00:13` ✅ FEED.note :: method note shipped
## Copies must carry it (the recurring snapshot trap)

- `17:00:13` ✅ COPY.leaderboard :: 50 of 50 rows
- `17:00:13` ✗ COPY.members :: 0 of 1847 rows
## Sanity — the score must discriminate, not flatline

- `17:00:13` ✅ SANITY.spread :: range 73.0 — a real cross-section
- `17:00:13` ✅ SANITY.not_saturated :: median 23.9 (not everything is 'crucial')
## Unmapped names now scored — the whole point

- `17:00:13` ✅ FIX.unmapped_scored :: 1847 companies outside the curated map now have a crucialness score
- `17:00:13`   IBM    Information Technology Service SI= 73.0  top-8% of industry revenue; margins 21% above 
- `17:00:13`   META   Internet Content & Information SI= 71.1  margins 25% above peers; R&D 9% above peers; 5
- `17:00:13`   AMZN   Specialty Retail               SI= 69.8  top-4% of industry revenue; R&D 13% above peer
- `17:00:13`   LLY    Drug Manufacturers - General   SI= 69.1  top-16% of industry revenue; R&D 7% above peer
- `17:00:13`   CSCO   Communication Equipment        SI= 67.7  top-6% of industry revenue; margins 26% above 
- `17:00:13`   PDD    Department Stores              SI= 67.2  margins 20% above peers; R&D 8% above peers
- `17:00:13`   NVDA   Semiconductors                 SI= 66.8  top-2% of industry revenue; margins 18% above 
- `17:00:13`   BKNG   Travel Services                SI= 66.6  top-12% of industry revenue; margins 29% above
- `17:00:13`   HCA    Medical - Care Facilities      SI= 65.8  top-7% of industry revenue; margins 29% above 
- `17:00:13`   EA     Electronic Gaming & Multimedia SI= 65.6  margins 18% above peers; R&D 16% above peers
## Served pages

- `17:00:13` ✅ PAGE.stamp :: capture-gap v11 (46047 bytes)
- `17:00:13` ✅ PAGE.si_fn :: renderer present
- `17:00:13` ✅ PAGE.col :: column labelled
- `17:00:13` ✅ PAGE.gloss :: glossary entry present
- `17:00:14` ✅ WHY.tile :: tile on the research page
## Additive

- `17:00:14` ✅ ADDITIVE.capture_gap :: preserved
- `17:00:14` ✅ ADDITIVE.revenue_share_pct :: preserved
- `17:00:14` ✅ ADDITIVE.catchup_pct :: preserved
- `17:00:14` ✅ ADDITIVE.criticality :: preserved
## VERDICT

- `17:00:14` ✗ FAILED: COPY.members
