# ops 4934 — BIS LBS: which row is actually THE aggregate

**Status:** success  
**Duration:** 15.1s  
**Finished:** 2026-08-20T16:00:22+00:00  

## Data

| claims_bn | duration_s | liabilities_bn | net_bn | passing_ratio_test | pos_C_rows_returned | pos_C_surviving_series | pos_L_rows_returned | pos_L_surviving_series | ratio | status | tuples_on_both_sides |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | 18149 | 40 |  |  |  |  |  |
|  |  |  |  |  |  |  | 18580 | 30 |  |  |  |
|  |  |  |  | 29 |  |  |  |  |  |  | 30 |
| 21796.9 |  | 19518.5 | 2278.4 |  |  |  |  |  | 1.12 |  |  |
|  | 15 |  |  |  |  |  |  |  |  | RESOLVED |  |

## Log
- `16:00:07` free dims (wildcarded in v1.0.1): L_CURR_TYPE, L_PARENT_CTY, L_REP_BANK_TYPE, L_POS_TYPE, L_INSTR, L_MEASURE
## CLAIMS (L_POSITION=C)

- `16:00:16`   A         5J    A     N     A     S      $    21,796.9 bn  2026-Q1
- `16:00:16`   F         5J    A     A     A     S      $    21,112.4 bn  2026-Q1
- `16:00:16`   F         5J    A     N     A     S      $    17,862.8 bn  2026-Q1
- `16:00:16`   A         JP    A     A     A     S      $     4,054.9 bn  2026-Q1
- `16:00:16`   A         US    A     A     A     S      $     4,036.9 bn  2026-Q1
- `16:00:16`   D         5J    A     A     A     S      $     3,934.1 bn  2026-Q1
- `16:00:16`   D         5J    A     N     A     S      $     3,934.1 bn  2026-Q1
- `16:00:16`   A         US    A     N     A     S      $     3,879.8 bn  2026-Q1
- `16:00:16`   A         JP    A     N     A     S      $     3,617.6 bn  2026-Q1
- `16:00:16`   F         5J    A     R     A     S      $     3,157.1 bn  2026-Q1
- `16:00:16`   A         GB    A     A     A     S      $     2,510.7 bn  2026-Q1
- `16:00:16`   A         FR    A     A     A     S      $     2,353.3 bn  2026-Q1
- `16:00:16`   A         FR    A     N     A     S      $     2,131.8 bn  2026-Q1
- `16:00:16`   A         GB    A     N     A     S      $     2,124.4 bn  2026-Q1
- `16:00:16`   … 26 more series
## LIABILITIES (L_POSITION=L)

- `16:00:22`   F         5J    A     A     A     S      $    20,165.6 bn  2026-Q1
- `16:00:22`   A         5J    A     N     A     S      $    19,518.5 bn  2026-Q1
- `16:00:22`   F         5J    A     N     A     S      $    14,115.0 bn  2026-Q1
- `16:00:22`   F         5J    A     R     A     S      $     5,173.8 bn  2026-Q1
- `16:00:22`   A         US    A     N     A     S      $     4,464.8 bn  2026-Q1
- `16:00:22`   A         FR    A     N     A     S      $     2,435.6 bn  2026-Q1
- `16:00:22`   A         GB    A     N     A     S      $     2,033.0 bn  2026-Q1
- `16:00:22`   A         JP    A     N     A     S      $     1,664.1 bn  2026-Q1
- `16:00:22`   A         CA    A     N     A     S      $     1,515.6 bn  2026-Q1
- `16:00:22`   A         DE    A     N     A     S      $     1,139.0 bn  2026-Q1
- `16:00:22`   A         CH    A     N     A     S      $       959.8 bn  2026-Q1
- `16:00:22`   F         5J    A     U     A     S      $       876.8 bn  2026-Q1
- `16:00:22`   F         JP    A     R     A     S      $       789.7 bn  2026-Q1
- `16:00:22`   A         CN    A     N     A     S      $       736.4 bn  2026-Q1
- `16:00:22`   … 16 more series
## Two-sided candidates (same dim tuple on both sides)

- `16:00:22`   A         5J    A     N     A     S      C $   21,796.9  L $   19,518.5  ratio 1.12    ✓
- `16:00:22`   F         5J    A     A     A     S      C $   21,112.4  L $   20,165.6  ratio 1.05    ✓
- `16:00:22`   F         5J    A     N     A     S      C $   17,862.8  L $   14,115.0  ratio 1.27    ✓
- `16:00:22`   A         US    A     N     A     S      C $    3,879.8  L $    4,464.8  ratio 0.87    ✓
- `16:00:22`   A         JP    A     N     A     S      C $    3,617.6  L $    1,664.1  ratio 2.17    ✓
- `16:00:22`   F         5J    A     R     A     S      C $    3,157.1  L $    5,173.8  ratio 0.61    ✓
- `16:00:22`   A         FR    A     N     A     S      C $    2,131.8  L $    2,435.6  ratio 0.88    ✓
- `16:00:22`   A         GB    A     N     A     S      C $    2,124.4  L $    2,033.0  ratio 1.04    ✓
- `16:00:22`   A         CA    A     N     A     S      C $    2,083.9  L $    1,515.6  ratio 1.37    ✓
- `16:00:22`   A         CN    A     N     A     S      C $    1,554.5  L $      736.4  ratio 2.11    ✓
- `16:00:22`   A         DE    A     N     A     S      C $    1,096.0  L $    1,139.0  ratio 0.96    ✓
- `16:00:22`   A         CH    A     N     A     S      C $      959.5  L $      959.8  ratio 1.0     ✓
- `16:00:22`   A         ES    A     N     A     S      C $      483.5  L $      420.0  ratio 1.15    ✓
- `16:00:22`   F         JP    A     R     A     S      C $      435.1  L $      789.7  ratio 0.55    ✓
## Recommendation

- `16:00:22` ✅ canonical tuple: {"L_CURR_TYPE": "A", "L_INSTR": "A", "L_MEASURE": "S", "L_PARENT_CTY": "5J", "L_POS_TYPE": "N", "L_REP_BANK_TYPE": "A"}
