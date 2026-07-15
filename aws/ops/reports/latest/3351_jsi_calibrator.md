## Deploy JSI calibrator

**Status:** success  
**Duration:** 35.4s  
**Finished:** 2026-07-15T17:53:08+00:00  

## Log
- `17:52:32`   zip: 79927 bytes
## 1. Lambda

- `17:52:33`   Lambda missing — creating
- `17:52:38` ✅   ✓ created justhodl-jsi-calibrator
## 2. EB rule + permissions

- `17:52:38` ✅   ✓ created rule jsi-calibrator-weekly
- `17:52:38` ✅   ✓ target → justhodl-jsi-calibrator
- `17:52:39` ✅   ✓ added invoke permission
## Deploy JSI engine v1.1.0

- `17:52:39`   zip: 81612 bytes
## 1. Lambda

- `17:52:39`   Lambda exists — updating
- `17:52:42` ✅   ✓ updated justhodl-stress-index
## 2. EB rule + permissions

- `17:52:42`   rule already correct: jsi-6h (rate(6 hours))
- `17:52:42` ✅   ✓ target → justhodl-stress-index
- `17:52:42` ✅   ✓ added invoke permission
## Run calibrator, verify spine IC fit

- `17:52:43` calibrator invoked; polling data/jsi-calibration.json…
- `17:52:49` calibration report after ~6s
- `17:52:49` spine: mode=empirical sample_size=2502
- `17:52:49` spine IC: {'VIXCLS': 0.1858, 'NFCI': 0.0912, 'KCFSI': 0.3485, 'STLFSI4': -0.0138, 'BAMLH0A0HYM2': -0.0993, 'T10Y2Y': 0.1171, 'BAMLC0A0CM': -0.037}
- `17:52:49` spine weights: {'VIXCLS': 0.20730872518561422, 'NFCI': 0.10270127955347269, 'KCFSI': 0.38722026222947714, 'STLFSI4': 0.05714285714285714, 'BAMLH0A0HYM2': 0.05714285714285714, 'T10Y2Y': 0.13134116160286452, 'BAMLC0A0CM': 0.05714285714285714}
- `17:52:49` overlay: mode=no_history sample_size=0
- `17:52:49` ✅ SPINE FIT ON FULL HISTORY — 2502 paired obs (multi-regime, spans every crisis).
- `17:52:49` ✅ spine weights valid — sum=1.0, range [0.057,0.387].
## Verify JSI engine consumes calibrated weights

- `17:53:08` JSI version=1.1.0 spine_weight_mode=calibrated jsi=33.08
- `17:53:08` spine_meta weights: [('VIXCLS', 0.2073), ('NFCI', 0.1027), ('KCFSI', 0.3872), ('STLFSI4', 0.0571), ('BAMLH0A0HYM2', 0.0571), ('T10Y2Y', 0.1313), ('BAMLC0A0CM', 0.0571)]
- `17:53:08` ✅ JSI engine now running on CALIBRATED spine weights (empirical, full-history).
- `17:53:08` ✅ overlay history writing — 1 snapshot(s) captured.
