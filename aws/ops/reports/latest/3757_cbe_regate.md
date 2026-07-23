# ops 3757 — canary #17: credit-before-equity

**Status:** success  
**Duration:** 11.4s  
**Finished:** 2026-07-23T01:24:15+00:00  

## Data

| awaiting | failed | leads | names | verdict |
|---|---|---|---|---|
| 26 | none | 0 | 26 | PASS_ALL |

## Log
## G0 — key contract

- `01:24:03` PASS G0_key_contract — producer_missing=[] page_missing=[]
## G1 — settle v1.0.0

- `01:24:04` PASS G1_settle — deployed
## G2 — async invoke + freshness

- `01:24:14` PASS G2_artifact — names=26 leads=0 awaiting=26
## G3 — day-one honesty (no lead without a prior obs)

- `01:24:14`   F      DD=1.11   dDD=None    CDS=130.0   dCDS=None    dPx=None    INSUFFICIENT_HISTORY
- `01:24:14`   INTC   DD=0.99   dDD=None    CDS=130.0   dCDS=None    dPx=None    INSUFFICIENT_HISTORY
- `01:24:14`   ORCL   DD=1.08   dDD=None    CDS=130.0   dCDS=None    dPx=None    INSUFFICIENT_HISTORY
- `01:24:14`   GM     DD=1.49   dDD=None    CDS=121.3   dCDS=None    dPx=None    INSUFFICIENT_HISTORY
- `01:24:14`   VZ     DD=2.13   dDD=None    CDS=80.4    dCDS=None    dPx=None    INSUFFICIENT_HISTORY
- `01:24:14`   T      DD=2.15   dDD=None    CDS=79.6    dCDS=None    dPx=None    INSUFFICIENT_HISTORY
- `01:24:14`   BA     DD=2.45   dDD=None    CDS=68.5    dCDS=None    dPx=None    INSUFFICIENT_HISTORY
- `01:24:14`   DIS    DD=3.22   dDD=None    CDS=50.0    dCDS=None    dPx=None    INSUFFICIENT_HISTORY
- `01:24:14`   ledger tickers=26
- `01:24:14` PASS G3_day_one_honesty — rows=26 fabricated_leads=[] ledger_written=True
## G4 — HY issuance gap declared, not faked

- `01:24:14`   gap: HY primary ISSUANCE windows NOT included: FRED issuance series are NBER historical archives (discontinued) or quarterly Z.1 flow-of-funds; SIFMA/TRACE are paid. OAS is a PRICE not a volume a
- `01:24:14`   gap: 26 of 26 names await a second observation — the lead is measured against this engine's own ledger, which accretes daily and cannot be back-filled honestly.
- `01:24:14`   OAS/FRED references in source: 1 (executable: 0)
- `01:24:14` PASS G4_gaps — issuance_gap_declared=True no_oas_substitution=True
## G5 — page served + field coverage + nav

- `01:24:15`   served page CURRENT len=9774 after 0s
- `01:24:15` PASS G5_page_live — len=9774
- `01:24:15` PASS G5_field_coverage — every published key has a render path
- `01:24:15` PASS G5_nav — listed under ('Macro & Liquidity', 'Credit Before Equity')
## VERDICT

- `01:24:15` ✅ PASS_ALL — canary #17 live (leads activate on day two)
