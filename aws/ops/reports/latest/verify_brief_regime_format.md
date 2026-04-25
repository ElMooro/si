# Force-invoke morning brief — verify regime hook flows through

**Status:** success  
**Duration:** 16.9s  
**Finished:** 2026-04-25T19:03:38+00:00  

## Data

| bond_markers_found | bond_markers_total | invoke_s | latest_brief |
|---|---|---|---|
| 0 | 5 | 16.3 | elligence/2026/04/25/1210.json |

## Log
## 1. Force-invoke morning-intelligence

- `19:03:37` ✅   Invoked in 16.3s
- `19:03:37`   Response: success=True, khalid={'score': 43, 'regime': 'BEAR', 'signals': [['DXY', -12, '118.1'], ['HY Spread', 5, '2.86%'], ['Unemployment', -8, '4.3%'], ['Net Liq', 3, '$5.70T'], ['SPY Trend', 5, '$714']], 'ts': '2026-04-25T18:59:55.981829'}, regime=BEAR, btc=77329
## 2. Locate the brief that just got written

- `19:03:37`   Latest brief: archive/intelligence/2026/04/25/1210.json
- `19:03:37`   Age: 24768.9s (2026-04-25T12:10:49+00:00)
- `19:03:37` ⚠   Brief is older than expected — may not be from this invoke
## 3. Verify brief structure

- `19:03:38`   Top-level keys: ['action_required', 'calibration', 'data_sources', 'dxy', 'forecast', 'generated_at', 'headline', 'headline_detail', 'metrics_table', 'ml_intelligence', 'phase', 'phase_color', 'plumbing_flags', 'portfolio', 'regime', 'risks', 'scores', 'signals', 'stock_signals', 'swap_spreads', 'timestamp', 'version', 'yield_curve']
## 4. Look for BOND_REGIME and DIVERGENCE markers

- `19:03:38`     ❌ BOND_REGIME marker in brief
- `19:03:38`     ❌ DIVERGENCE marker in brief
- `19:03:38`     ❌ bond_regime field
- `19:03:38`     ❌ bond_extreme_count field
- `19:03:38`     ❌ divergence_extreme_count
- `19:03:38` ⚠ 
  ⚠ Some markers missing — step 150 patch may not be fully active
## 5. Sample of brief content

- `19:03:38`   headline: ⚠️ PRE-CRISIS WARNING
- `19:03:38`   headline_detail: RRP at $0.1B — NEAR ZERO! Lower than March 2020 and Sept 2019. Liquidity buffer exhausted.
- `19:03:38`   action_required: REDUCE ALL RISK. Raise cash to 40%+. Exit leveraged and speculative positions.
- `19:03:38`   forecast: Correction risk -10% to -20%. Liquidity deteriorating rapidly. Credit stress building.
## 6. PAT rotation workflow readiness check

- `19:03:38`   Lambda justhodl-dex-scanner alive (sha=GSS6NicVuzeelGGv...)
- `19:03:38`   TOKEN env var present: True
- `19:03:38`   Workflow file: .github/workflows/rotate-dex-scanner-pat.yml
- `19:03:38` ✅   ✅ Workflow ready — awaiting Khalid PAT generation
- `19:03:38` Done
