# 1) Redeploy 3 Lambdas

**Status:** success  
**Duration:** 43.5s  
**Finished:** 2026-05-04T20:15:27+00:00  

## Log
- `20:14:48` ✅   ✓ justhodl-asymmetric-scorer        zip=8,747b  modified=2026-05-04T20:14:44.000+0000
- `20:14:50` ✅   ✓ justhodl-risk-sizer               zip=6,722b  modified=2026-05-04T20:14:48.000+0000
- `20:14:55` ✅   ✓ justhodl-ai-brief                 zip=6,265b  modified=2026-05-04T20:14:51.000+0000
# 2) Invoke producers — verify mirror writes

- `20:14:57`   justhodl-asymmetric-scorer: status=200  duration=1.7s
- `20:14:58`   justhodl-risk-sizer: status=200  duration=1.7s
# 3) Confirm both legacy + canonical paths exist with same data

- `20:14:58` ✅   ✓ opportunities/asymmetric-equity.json         28,540b  2026-05-04T20:14:57+00:00
- `20:14:58` ✅   ✓ data/asymmetric-scorer.json                  28,540b  2026-05-04T20:14:58+00:00  same_size=True
- `20:14:58` ✅   ✓ risk/recommendations.json                    15,995b  2026-05-04T20:14:59+00:00
- `20:14:58` ✅   ✓ data/risk-sizer.json                         15,995b  2026-05-04T20:14:59+00:00  same_size=True
# 4) Trigger AI brief with enriched compressors

- `20:15:27`   status: 200  duration: 28.1s
- `20:15:27`   resp: {"statusCode": 200, "body": "{\"duration_s\": 27.16, \"brief_chars\": 5912, \"snapshot_keys\": [\"as_of\", \"intelligence\", \"calibration\", \"sectors\", \"momentum\", \"allocator\", \"asymmetric_setups\", \"risk_sizer\", \"auction_stress\", \"eurodollar_stress\", \"macro_surprise\", \"insider_buys\", \"earnings_pead\", \"correlation_breaks\", \"alerts\"], \"error\": null}"}
# 5) Verify brief snapshot has rich asymmetric + risk_sizer data

- `20:15:27` 
- `20:15:27`   ASYMMETRIC SETUPS snapshot:
- `20:15:27`     n_setups:                94
- `20:15:27`     n_value_traps:           21
- `20:15:27`     n_quality_passed:        175/503 screened
- `20:15:27`     cutoffs:                 {'quality': 71.1, 'safety': 72.6, 'value': 89.3, 'momentum': 60.9, 'stacked': 45.0}
- `20:15:27`     aaii_signal:             aaii_extreme_bullish (spread +50% — contrarian headwind)
- `20:15:27`     top sectors:             [{'sector': 'Technology', 'n_setups': 31}, {'sector': 'Industrials', 'n_setups': 16}, {'sector': 'Healthcare', 'n_setups': 12}]
- `20:15:27`     top 5 setups:
- `20:15:27`       • INCY   (Healthcare        ) composite=84.0  dims=5
- `20:15:27`       • CF     (Basic Materials   ) composite=75.9  dims=5
- `20:15:27`       • MU     (Technology        ) composite=82.9  dims=4
- `20:15:27`       • NEM    (Basic Materials   ) composite=80.8  dims=4
- `20:15:27`       • NVDA   (Technology        ) composite=77.7  dims=4
- `20:15:27` 
- `20:15:27`   RISK SIZER snapshot:
- `20:15:27`     regime:                  NEUTRAL (strength 56.8)
- `20:15:27`     max_gross_exposure_pct:  75.0%
- `20:15:27`     current_dd_pct:          -0.2%
- `20:15:27`     dd_active_trigger:       no trigger
- `20:15:27`     n_clusters:              15
- `20:15:27`     total_recommended_size:  75.01%
- `20:15:27`     kelly_fraction:          0.25
- `20:15:27`     top 5 sized:
- `20:15:27`       • MU     3.82%  kelly=0.08  conv=82.9  cluster=corr_MU
- `20:15:27`       • NEM    3.73%  kelly=0.08  conv=80.8  cluster=corr_NEM
- `20:15:27`       • NVDA   3.58%  kelly=0.08  conv=77.7  cluster=corr_NVDA
- `20:15:27`       • EXE    3.56%  kelly=0.08  conv=77.2  cluster=sector_energy
- `20:15:27`       • FSLR   3.53%  kelly=0.08  conv=76.6  cluster=sector_energy
- `20:15:27`     warnings:                [{'level': 'medium', 'message': 'Raw signal sum (178%) exceeds 150% — over-signaled, scaled down'}]
# 6) Brief mentions of asymmetric tickers + risk-sizer terms

- `20:15:27`     Piotroski         : - **CHTR** (piotroski_low deterioration)
- `20:15:27`     MU                : 2. **MU** (Technology) — Composite 82.9, quality 100.0, momentum 100.0, sized 3.82%
- `20:15:27`     CF                : - **MOS** (negative FCF)
- `20:15:27`     NEM               : 3. **NEM** (Basic Materials) — Composite 80.8, quality 95.8, safety 96.5, sized 3.73%
- `20:15:27`     value trap        : **Value traps to avoid:**
- `20:15:27`     INCY              : 1. **INCY** (Healthcare) — Composite 84.0, quality 88.5, safety 99.9, value 97.6
- `20:15:27`     EXE               : # JUSTHODL.AI EXECUTIVE BRIEF
- `20:15:27`     asymmetric        : **Asymmetric quality setups (conviction-weighted):**
- `20:15:27`     FSLR              : | **Energy (EXE/FSLR blend)** | 8% | Sector leader momentum; DXY hedge; sized 7.1% in risk_sizer |
