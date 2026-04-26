# Verify Phase 9.3a + 9.3b signals

**Status:** success  
**Duration:** 2.0s  
**Finished:** 2026-04-26T21:49:58+00:00  

## Log
## 1. Lambda redeploy status

- `21:49:57`   CodeSha256:   7zAiI1Z+xOZiyLHCtX7IO0urDlG8Iys0y8nRzmk4Viw=
- `21:49:57`   LastModified: 2026-04-26T20:48:39.000+0000
## 2. Manual invoke

- `21:49:58`   ✅ OK (1.5s)
- `21:49:58`   payload: {"statusCode": 200, "body": "{\"status\": \"ok\", \"elapsed_sec\": 0.8, \"composite_signal\": \"NO_DATA\", \"composite_score\": null, \"n_indices\": 0, \"n_flagged\": 0, \"s3_key\": \"data/crisis-plumbing.json\"}"}
## 3. Read S3 output

- `21:49:58`   schema_version: 1.1
- `21:49:58`   generated_at:   2026-04-26T21:49:58.627341+00:00
- `21:49:58`   n_series_fetched: 30
## 4. Phase 9.3a fixes

- `21:49:58` ⚠   ⚠ mmf_composition is None (still null)
- `21:49:58` ⚠   ⚠ BUSLOANS missing or unavailable: {'name': 'C&I Lending (H.8 absolute)', 'available': False}
- `21:49:58`   ✅ H8B1058NCBCMG removed
- `21:49:58` ⚠   ⚠ WGMMNS not available: {'name': 'Government MMF', 'available': False}
- `21:49:58` ⚠   ⚠ WPMMNS not available: {'name': 'Prime MMF', 'available': False}
- `21:49:58` ⚠   ⚠ WTMMNS not available: {'name': 'Tax-Exempt MMF', 'available': False}
## 5. Phase 9.3b — funding & credit signals

- `21:49:58` ⚠   ⚠ SOFR_IORB_SPREAD not available: {'name': 'SOFR – IORB Spread', 'available': False}
- `21:49:58` ⚠   ⚠ HY_OAS not available: {'name': 'HY Credit Spread (ICE BofA US HY OAS)', 'available': False}
- `21:49:58` ⚠   ⚠ IG_BBB_OAS not available: {'name': 'IG BBB Credit Spread', 'available': False}
- `21:49:58` ⚠   ⚠ T10YIE not available: {'name': '10Y TIPS Breakeven Inflation', 'available': False}
- `21:49:58` ⚠   ⚠ DFII10 not available: {'name': '10Y Real Rate (TIPS yield)', 'available': False}
- `21:49:58` ⚠   ⚠ SLOOS_TIGHTEN not available: {'name': 'SLOOS: Banks Tightening C&I Standards (net %)', 'available': False}
- `21:49:58` 
- `21:49:58`   funding_credit_signals: 0/6 populated
## 6. _partials/sidebar.html post-.nojekyll

- `21:49:58`   (separate verifier needed — pages.deploy must propagate first)
- `21:49:58`   This will be checked in step 234 after ~5 min
## FINAL

- `21:49:58`   🟡 Some gaps — see log above
- `21:49:58` Done
