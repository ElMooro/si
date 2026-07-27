# ops 3971 — identify SNB CH02Y / CH03Y series + freshness

**Status:** failure  
**Duration:** 4.3s  
**Finished:** 2026-07-27T05:37:18+00:00  

## Error

```
SystemExit: 1
```

## Data

| bytes | fromDate | index_2y | index_3y | n_series | status |
|---|---|---|---|---|---|
| 5608048 |  |  |  |  | 200 |
|  |  | 1 | 2 | 22 |  |
| 6718 | 2026-04-28 |  |  |  | 200 |

## Log
## A. every series header in the rendoblid cube

- `05:37:17`   [ 0] n=7534   last={'date': '2025-07-31', 'value': -0  Overview=Spot interest rates with different maturities for Confederation bond issues and euro-denominated bond issues - CHF Swiss Confederation bond i
- `05:37:17`   [ 1] n=7534   last={'date': '2025-07-31', 'value': -0  Overview=Spot interest rates with different maturities for Confederation bond issues and euro-denominated bond issues - CHF Swiss Confederation bond i
- `05:37:17`   [ 2] n=7534   last={'date': '2025-07-31', 'value': -0  Overview=Spot interest rates with different maturities for Confederation bond issues and euro-denominated bond issues - CHF Swiss Confederation bond i
- `05:37:17`   [ 3] n=7534   last={'date': '2025-07-31', 'value': 0.  Overview=Spot interest rates with different maturities for Confederation bond issues and euro-denominated bond issues - CHF Swiss Confederation bond i
- `05:37:17`   [ 4] n=7534   last={'date': '2025-07-31', 'value': 0.  Overview=Spot interest rates with different maturities for Confederation bond issues and euro-denominated bond issues - CHF Swiss Confederation bond i
- `05:37:17`   [ 5] n=7534   last={'date': '2025-07-31', 'value': 0.  Overview=Spot interest rates with different maturities for Confederation bond issues and euro-denominated bond issues - CHF Swiss Confederation bond i
- `05:37:17`   [ 6] n=7534   last={'date': '2025-07-31', 'value': 0.  Overview=Spot interest rates with different maturities for Confederation bond issues and euro-denominated bond issues - CHF Swiss Confederation bond i
- `05:37:17`   [ 7] n=7534   last={'date': '2025-07-31', 'value': 0.  Overview=Spot interest rates with different maturities for Confederation bond issues and euro-denominated bond issues - CHF Swiss Confederation bond i
- `05:37:17`   [ 8] n=7534   last={'date': '2025-07-31', 'value': 0.  Overview=Spot interest rates with different maturities for Confederation bond issues and euro-denominated bond issues - CHF Swiss Confederation bond i
- `05:37:17`   [ 9] n=7534   last={'date': '2025-07-31', 'value': 0.  Overview=Spot interest rates with different maturities for Confederation bond issues and euro-denominated bond issues - CHF Swiss Confederation bond i
- `05:37:17`   [10] n=7534   last={'date': '2025-07-31', 'value': 0.  Overview=Spot interest rates with different maturities for Confederation bond issues and euro-denominated bond issues - CHF Swiss Confederation bond i
- `05:37:17`   [11] n=7534   last={'date': '2025-07-31', 'value': 0.  Overview=Spot interest rates with different maturities for Confederation bond issues and euro-denominated bond issues - CHF Swiss Confederation bond i
- `05:37:17`   [12] n=6913   last={'date': '2025-07-31', 'value': 0.  Overview=Spot interest rates with different maturities for Confederation bond issues and euro-denominated bond issues - CHF Swiss Confederation bond i
- `05:37:17`   [13] n=7064   last={'date': '2025-07-31', 'value': 2.  Overview=Spot interest rates with different maturities for Confederation bond issues and euro-denominated bond issues - EUR German government bond iss
- `05:37:17`   [14] n=7534   last={'date': '2025-07-31', 'value': 0.  Overview=Yields on CHF bond issues issued by various borrower categories with a maturity of 8 years - CHF bond issues of Swiss borrowers - Confederati
- `05:37:17`   [15] n=6104   last={'date': '2025-07-31', 'value': 0.  Overview=Yields on CHF bond issues issued by various borrower categories with a maturity of 8 years - CHF bond issues of Swiss borrowers - Cantons
- `05:37:17`   [16] n=6725   last={'date': '2025-07-31', 'value': 0.  Overview=Yields on CHF bond issues issued by various borrower categories with a maturity of 8 years - CHF bond issues of Swiss borrowers - Mortgage bo
- `05:37:17`   [17] n=6104   last={'date': '2025-07-31', 'value': 0.  Overview=Yields on CHF bond issues issued by various borrower categories with a maturity of 8 years - CHF bond issues of Swiss borrowers - Commercial 
- `05:37:17`   [18] n=6104   last={'date': '2025-07-31', 'value': 1.  Overview=Yields on CHF bond issues issued by various borrower categories with a maturity of 8 years - CHF bond issues of Swiss borrowers - Manufacturi
- `05:37:17`   [19] n=6104   last={'date': '2025-07-31', 'value': 0.  Overview=Yields on CHF bond issues issued by various borrower categories with a maturity of 8 years - CHF bond issues of foreign borrowers - AAA
- `05:37:17`   [20] n=6104   last={'date': '2025-07-31', 'value': 0.  Overview=Yields on CHF bond issues issued by various borrower categories with a maturity of 8 years - CHF bond issues of foreign borrowers - AA
- `05:37:17`   [21] n=6104   last={'date': '2025-07-31', 'value': 1.  Overview=Yields on CHF bond issues issued by various borrower categories with a maturity of 8 years - CHF bond issues of foreign borrowers - A
## B. freshness — re-pull with fromDate

- `05:37:18`   series returned: 22
## C. verdict

- `05:37:18`   CH02Y -> rendoblid series index 1
- `05:37:18`   CH03Y -> rendoblid series index 2
- `05:37:18`   NOTE: index position is not a stable contract. The wiring op must match on the HEADER TEXT at fetch time, never on a hardcoded index, or an SNB reordering silently repoints CH02Y at another tenor.
- `05:37:18` ✅   2Y and 3Y series located
- `05:37:18` ✗   fromDate pull returns data
- `05:37:18` ✗ FAILED: ['fromDate pull returns data']
