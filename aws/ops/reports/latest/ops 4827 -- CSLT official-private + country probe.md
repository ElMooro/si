# 1. resolve + enumerate the CSLT release

**Status:** success  
**Duration:** 5.2s  
**Finished:** 2026-08-17T16:40:13+00:00  

## Data

| engine_total_lt_may_bn | fred_key_donor | release_id | release_name |
|---|---|---|---|
|  | justhodl-risk-gate |  |  |
|  |  | 3 | Treasury International Capital: Continuous Securities Long Term (CSLT) |
| 262.8 |  |  |  |

## Log
- `16:40:13` ✅ catalog enumerated: 4260 series (release count=4260)
# 2. official vs private net-transaction ids

- `16:40:13` ✅   official all_lt     FORLTTOTALNET82644       2023-02-01 'Foreign Net Transactions of All U.S. Long-Term Securities: I'
- `16:40:13` ✅   official all_lt     FORLTTOTALNET82652       2023-02-01 'Foreign Net Transactions of All U.S. Long-Term Securities: I'
- `16:40:13` ✅   official treasury   FORLTTREASNET99990       1985-01-01 'Foreign Net Transactions of U.S. Long-Term Treasury Securiti'
- `16:40:13` ✅   official treasury   FORSTTREASNET99990       1984-12-01 'Foreign Net Transactions of U.S. Short-Term Treasury Securit'
- `16:40:13` ✅   official equity     FORLTEQTYNET82644        2023-02-01 'Foreign Net Transactions of U.S. Equity Securities: Issued b'
- `16:40:13` ✅   official equity     FORLTEQTYNET82652        2023-02-01 'Foreign Net Transactions of U.S. Equity Securities: Issued b'
- `16:40:13` ✅   official corporate  FORLTCORPNET82644        2023-02-01 'Foreign Net Transactions of U.S. Long-Term Corporate Bonds: '
- `16:40:13` ✅   official corporate  FORLTCORPNET82652        2023-02-01 'Foreign Net Transactions of U.S. Long-Term Corporate Bonds: '
- `16:40:13` ✅   official agency     FORLTAGCYNET89992        2023-02-01 'Foreign Net Transactions of U.S. Long-Term Agency Bonds: U.S'
- `16:40:13` ✅   official agency     FORLTAGCYNET99990        1985-01-01 'Foreign Net Transactions of U.S. Long-Term Agency Bonds: For'
- `16:40:13` ⚠   official short_term NO MATCH
- `16:40:13` ⚠   private  all_lt     NO MATCH
- `16:40:13` ⚠   private  treasury   NO MATCH
- `16:40:13` ⚠   private  equity     NO MATCH
- `16:40:13` ⚠   private  corporate  NO MATCH
- `16:40:13` ⚠   private  agency     NO MATCH
- `16:40:13` ⚠   private  short_term NO MATCH
# 3. arithmetic cross-validation (May 2026)

- `16:40:13` ⚠   all_lt official/private candidates incomplete -- cannot validate
- `16:40:13`   doc context (FULL-TIC, will differ from LT-only): May private +172.0B / official -39.9B
# 4. country lines (Treasury focus)

- `16:40:13` ✅   China           FORLTTREASPOS41408         1984-12-01 'Foreign Portfolio Holdings of U.S. Long-Term Treasury Se'
- `16:40:13` ✅   China           FORLTTREASVALCHG41408      1985-01-01 'Valuation Change on Foreign Portfolio Holdings of U.S. L'
- `16:40:13` ✅   China           FORSTTREASPOS41408         2003-02-01 'Foreign Portfolio Holdings of U.S. Short-Term Treasury S'
- `16:40:13` ✅   Japan           FORLTTREASPOS42609         1984-12-01 'Foreign Portfolio Holdings of U.S. Long-Term Treasury Se'
- `16:40:13` ✅   Japan           FORLTTREASVALCHG42609      1985-01-01 'Valuation Change on Foreign Portfolio Holdings of U.S. L'
- `16:40:13` ✅   Japan           FORSTTREASPOS42609         2003-02-01 'Foreign Portfolio Holdings of U.S. Short-Term Treasury S'
- `16:40:13` ✅   United Kingdom  FORLTTREASPOS13005         1984-12-01 'Foreign Portfolio Holdings of U.S. Long-Term Treasury Se'
- `16:40:13` ✅   United Kingdom  FORLTTREASPOS13059         1984-12-01 'Foreign Portfolio Holdings of U.S. Long-Term Treasury Se'
- `16:40:13` ✅   United Kingdom  FORLTTREASVALCHG13005      1985-01-01 'Valuation Change on Foreign Portfolio Holdings of U.S. L'
- `16:40:13` ✅   Belgium         FORLTTREASPOS10251         2000-03-01 'Foreign Portfolio Holdings of U.S. Long-Term Treasury Se'
- `16:40:13` ✅   Belgium         FORLTTREASPOS10308         1984-12-01 'Foreign Portfolio Holdings of U.S. Long-Term Treasury Se'
- `16:40:13` ✅   Belgium         FORLTTREASVALCHG10251      2000-04-01 'Valuation Change on Foreign Portfolio Holdings of U.S. L'
- `16:40:13` ✅   Cayman          FORLTTREASPOS36137         2000-03-01 'Foreign Portfolio Holdings of U.S. Long-Term Treasury Se'
- `16:40:13` ✅   Cayman          FORLTTREASVALCHG36137      2000-04-01 'Valuation Change on Foreign Portfolio Holdings of U.S. L'
- `16:40:13` ✅   Cayman          FORSTTREASPOS36137         2003-02-01 'Foreign Portfolio Holdings of U.S. Short-Term Treasury S'
# 5. verdict

- `16:40:13` ✅ probe complete: 21 official + 0 private net-transaction series discovered; wire v1.1 only with identity-proven ids
