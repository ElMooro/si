# ops 3849 — FDR family scope (decision support, no changes)

**Status:** success  
**Duration:** 0.2s  
**Finished:** 2026-07-25T03:48:42+00:00  

## Data

| at_p05 | bh_fleetwide | bh_within_theme | expected_by_chance | obs_over_expected | panels | themes |
|---|---|---|---|---|---|---|
| 5 | 0 | 0 | 9.0 | 0.56 | 179 | 10 |

## Log
- `03:48:42` ✅   179 panels with usable statistics (of 207)
## A. Is the theme taxonomy a priori?

- `03:48:42`   10 themes: [('BREADTH', 14), ('CREDIT', 22), ('CRYPTO', 2), ('DOLLAR', 10), ('GROWTH', 15), ('INFLATION', 4), ('LIQUIDITY', 27), ('OTHER', 62), ('RATES', 12), ('STRESS', 11)]
- `03:48:42` ✅   Themes are a fixed engine-side taxonomy assigned per panel, not chosen after inspecting results — so a within-theme family is defensible ON PRINCIPLE, independent of what it yields.
## B. Benjamini-Hochberg at q=0.05 — both scopes

- `03:48:42`   FLEET-WIDE  m= 179  discoveries=0  crit_p=None
- `03:48:42`   BREADTH      m=  14  discoveries=0  best_p=0.2801 (t=1.08) needs p<=0.00357
- `03:48:42`   CREDIT       m=  22  discoveries=0  best_p=0.0424 (t=2.03) needs p<=0.00227
- `03:48:42`   CRYPTO       m=   2  discoveries=0  best_p=0.4593 (t=0.74) needs p<=0.02500
- `03:48:42`   DOLLAR       m=  10  discoveries=0  best_p=0.0340 (t=2.12) needs p<=0.00500
- `03:48:42`   GROWTH       m=  15  discoveries=0  best_p=0.2983 (t=1.04) needs p<=0.00333
- `03:48:42`   INFLATION    m=   4  discoveries=0  best_p=0.1336 (t=-1.50) needs p<=0.01250
- `03:48:42`   LIQUIDITY    m=  27  discoveries=0  best_p=0.1285 (t=1.52) needs p<=0.00185
- `03:48:42`   OTHER        m=  62  discoveries=0  best_p=0.0220 (t=2.29) needs p<=0.00081
- `03:48:42`   RATES        m=  12  discoveries=0  best_p=0.1902 (t=1.31) needs p<=0.00417
- `03:48:42`   STRESS       m=  11  discoveries=0  best_p=0.2891 (t=1.06) needs p<=0.00455
- `03:48:42`   WITHIN-THEME total discoveries = 0
## C. Chance baseline — is there anything to find at all?

- `03:48:42`   panels at p<0.05 : 5
- `03:48:42`   expected by CHANCE alone (5% of 179): 9.0
- `03:48:42`   observed / expected = 0.56x
## VERDICT

- `03:48:42` ⚠   NEITHER scope yields a single discovery. The family-scope
- `03:48:42` ⚠   question is MOOT — narrowing the correction would not
- `03:48:42` ⚠   change the outcome, so there is nothing to decide and no
- `03:48:42` ⚠   temptation to shop for a threshold.
- `03:48:42` ⚠   Worse: the hit count is 0.56x chance, i.e. the
- `03:48:42` ⚠   panel set is performing AT OR BELOW what random data
- `03:48:42` ⚠   would produce. The correct action is to leave the gate
- `03:48:42` ⚠   shut and treat the panels as CONTEXT, which is exactly
- `03:48:42` ⚠   what wl_fusion already does.
- `03:48:42` ✅   RECOMMENDATION: change nothing in the FDR scope.
- `03:48:42` ✅ ANALYSIS COMPLETE — no engine modified
