# ops 4626 — blackswan basis fixes

**Status:** success  
**Duration:** 43.9s  
**Finished:** 2026-08-12T02:12:38+00:00  

## Data

| alarm | amber | extremes | red | resolved | top | with_history |
|---|---|---|---|---|---|---|
| AMBER | 4 | 34 | 0 | 359 | [{"symbol": "FRED:POILBREUSDM", "z": 1.96, "dod_pct": -18.64}, {"symbol": "FRED:NFCILEVERAGE", "z": 1.6, "dod_pct": null}, {"symbol": "FRED:UMCSENT", "z": 1.6, "dod_pct": 10.49}, { | 200 |

## Log
## deploy-settle

- `02:11:55` ✅   [deploy] v1.2.0 live
## run + basis truth

- `02:12:31` FRED:POILBREUSDM                   CALM      pct-z   z=1.96  -18.64% (MoM)
- `02:12:31` FRED:NFCILEVERAGE                  CALM      diff-z  z=1.6   -0.031 Δ (WoW)
- `02:12:31` FRED:RECPROUSM156N                 CALM      diff-z  z=0.02  +0.28 Δ (MoM)
- `02:12:31` FRED:SOFR-FRED:FEDFUNDS            UNRESOLVED -       z=None  None
- `02:12:31` TVC:US30Y-TVC:US10Y                CALM      pct-z   z=0.55  -1.85% (DoD)
- `02:12:31` FRED:DCPF3M-FRED:FEDFUNDS          UNRESOLVED -       z=None  None
- `02:12:31` FRED:BAMLC4A0C710YEY-TVC:US10Y     CALM      pct-z   z=0.04  +0.00% (DoD)
- `02:12:31` ✅   [cadence] Brent labeled MoM: -18.64% (MoM)
- `02:12:31` ✅   [diff-basis] NFCI-leverage on diff basis: -0.031 Δ (WoW)
- `02:12:31` ✅   [no-pct-red] no sigma-less row carries RED (violations: none)
- `02:12:31` ✅   [composites] 6 formula composites on z-basis (e.g. ['TVC:US30Y-TVC:US10Y', 'FRED:OBFR-FRED:SOFR', 'TVC:US03MY/FRED:BAMLH0A0HYM2'])
- `02:12:31` ✅   [resolution] 359/500 resolved
- `02:12:31` ✅   [alarm-valid] recomputed alarm AMBER
## canary + edge

- `02:12:37` ✅   [canary] physical board carries {"state": "AMBER", "n_red": 0, "n_amber": 4, "n_range_extreme": 34, "list": "Black Swan Event", "doctrine": "Khalid's TV
- `02:12:38` ✅   [edge] edge serves the basis-audited strip
## verdict

- `02:12:38` ✅ STRIP AUDITED — alarm AMBER (red=0 amber=4) on honest bases: cadence-labeled, diff-z for sign-crossers, RED reserved for sigma, 6 plumbing composites live
