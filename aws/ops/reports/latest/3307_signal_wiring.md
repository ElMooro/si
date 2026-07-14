## 1. Deploy the three emitters

**Status:** success  
**Duration:** 45.0s  
**Finished:** 2026-07-14T18:36:47+00:00  

## Data

| dealer_5yplus_b | dealer_signal | fails | fails_corp_spike | fails_signal | fsi_cross_armed | fsi_latest | gcf_tri_bp | in_wow_b | mmf_pick_keys | out_wow_b | strain_armed | warns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| -13.68 | {'predicted_direction': 'DOWN', 'baseline_price': '84.15', 'benchmark': 'BIL', 'status': 'pending'} |  | True | {'predicted_direction': 'DOWN', 'baseline_price': '107.195', 'benchmark': 'SPY', 'status': 'pending'} |  |  |  |  |  |  |  |  |
|  |  |  |  |  | True | -2.512 | 4.0 |  |  |  | True |  |
|  |  |  |  |  |  |  |  | 97.3 | ['agency_holdings', 'bank_repo_agreements', 'other_assets', 'repo_holdings', 'total_net_assets', 'treasury_holdings'] | None |  |  |
|  |  | [] |  |  |  |  |  |  |  |  |  | [] |

## Log
- `18:36:02`   zip: 81364 bytes
## 1. Lambda

- `18:36:03`   Lambda exists — updating
- `18:36:08` ✅   ✓ updated justhodl-nyfed-pd
- `18:36:08`   zip: 76630 bytes
## 1. Lambda

- `18:36:08`   Lambda exists — updating
- `18:36:11` ✅   ✓ updated justhodl-settlement-fails
- `18:36:12`   zip: 78699 bytes
## 1. Lambda

- `18:36:12`   Lambda exists — updating
- `18:36:15` ✅   ✓ updated justhodl-ofr-stfm
## 2. Wait for fresh docs

## 3. Verify emissions in justhodl-signals

## 4. Polish checks

- `18:36:47` OPS 3307 PASS — dealer/OFR stack wired into closed-loop grading: duration-twist + fails-spike LIVE in justhodl-signals; strain + FSI-cross armed.
