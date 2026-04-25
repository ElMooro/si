# Bond Regime Detector — data source audit

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-04-25T15:35:10+00:00  

## Log
## 1. repo-data.json full structure

- `15:35:10`   Top-level keys: ['data', 'generated_at', 'intelligence', 'stress', 'summary', 'timestamp']
- `15:35:10`   Categories in 'data': ['fed_facilities', 'funding_spreads', 'repo_rates', 'reverse_repo', 'swaps', 'systemic', 'treasury']
- `15:35:10` 
  fed_facilities:
- `15:35:10`     H41RESPPALDKNWW           value=0.0  z=0  history=0pts
- `15:35:10`     Loans_16_90_Days          value=1.79  z=-0.36733405290130544  history=30pts
- `15:35:10`     SWPT                      value=101.0  z=0.22208695449252153  history=30pts
- `15:35:10`     TGA_Status                value=1007.2  z=None  history=0pts
- `15:35:10`     WORAL                     value=4.0  z=-0.22076390182886624  history=30pts
- `15:35:10` 
  funding_spreads:
- `15:35:10`     BAMLC0A4CBBB              value=1.0  z=-0.38505447539353005  history=30pts
- `15:35:10`     BAMLH0A3HYC               value=9.15  z=0.003362741800751018  history=30pts
- `15:35:10`     RIFSPPNAAD90NB            value=3.63  z=None  history=0pts
- `15:35:10`     T10YFF                    value=0.7  z=1.2472835896905843  history=30pts
- `15:35:10` 
  repo_rates:
- `15:35:10`     AMERIBOR_Spread           value=0.0335  z=None  history=0pts
- `15:35:10`     SOFR                      value=3.65  z=-0.2553241653960213  history=30pts
- `15:35:10` 
  reverse_repo:
- `15:35:10`     RRP_Counterparties        value=0  z=None  history=0pts
- `15:35:10`     RRP_Rate                  value=0.0  z=None  history=0pts
- `15:35:10`     RRP_Volume                value=0.1  z=None  history=5pts
- `15:35:10` 
  swaps:
- `15:35:10` 
  systemic:
- `15:35:10`     DRTSCILM                  value=5.3  z=0.1070153317782353  history=30pts
- `15:35:10`     M2                        value=22.67  z=None  history=0pts
- `15:35:10`     NFCI                      value=-0.497  z=-0.011731914274663389  history=30pts
- `15:35:10`     STLFSI4                   value=-0.758  z=0  history=0pts
- `15:35:10` 
  treasury:
- `15:35:10`     yield_curve               value=None  z=None  history=0pts
## 2. fred-cache-secretary.json — series + history depths

- `15:35:10`   Total series: 25
- `15:35:10`   BAMLH0A0HYM2         value=      2.86  history=30pts
- `15:35:10`   BAMLC0A0CM           value=       0.8  history=30pts
- `15:35:10`   T10Y2Y               value=      0.53  history=30pts
- `15:35:10`   T10Y3M               value=      0.62  history=30pts
- `15:35:10`   DTWEXBGS             value=  118.0795  history=30pts
- `15:35:10`   T5YIE                value=      2.61  history=30pts
- `15:35:10`   T10YIE               value=      2.42  history=30pts
- `15:35:10`   VIXCLS               value=     19.31  history=30pts
- `15:35:10` ⚠   MOVE                 MISSING from FRED cache
- `15:35:10`   NFCI                 value=    -0.497  history=30pts
- `15:35:10` ⚠   STLFSI4              MISSING from FRED cache
- `15:35:10`   DCOILWTICO           value=     91.06  history=30pts
- `15:35:10`   BAMLC0A4CBBB         value=       1.0  history=30pts
## 3. Current regime/current.json contents

- `15:35:10`   Regime: NEUTRAL
- `15:35:10`   Strength: 57.9
- `15:35:10`   Extreme: 0/7
- `15:35:10`   Signals (7):
- `15:35:10`     HY OAS               z=-1.18 dir=RISK_ON  extreme=False
- `15:35:10`     IG OAS               z=-1.26 dir=RISK_ON  extreme=False
- `15:35:10`     NFCI                 z=-0.01 dir=RISK_ON  extreme=False
- `15:35:10`     VIX                  z=-0.97 dir=RISK_ON  extreme=False
- `15:35:10`     2s10s velocity       z=-0.78 dir=RISK_ON  extreme=False
- `15:35:10`     DXY 5d               z=-0.62 dir=RISK_ON  extreme=False
- `15:35:10`     5Y BE 5d             z=+1.17 dir=RISK_ON  extreme=False
## 4. archive/repo/ depth — backtest viability

- `15:35:10`   Total snapshots: 400
- `15:35:10`   Earliest: archive/repo/2026/02/23/0655.json  (2026-02-23T06:55:56+00:00)
- `15:35:10`   Latest:   archive/repo/2026/03/17/2330.json  (2026-03-17T23:30:58+00:00)
- `15:35:10`   These are the backbone for future backtest validation.
## 5. Verdict

- `15:35:10`   The detector is running successfully and producing sane output.
- `15:35:10`   If MOVE is genuinely missing from FRED's free tier, that's
- `15:35:10`   fine — we have 7 indicators, well above the MIN_INDICATORS=3
- `15:35:10`   threshold. Phase 1B (cross-asset divergence) can add MOVE
- `15:35:10`   via a different source if needed (e.g., BlackRock's IEF realized
- `15:35:10`   vol as proxy).
- `15:35:10` 
  Next: build Phase 1B (cross-asset divergence scanner).
- `15:35:10` Done
