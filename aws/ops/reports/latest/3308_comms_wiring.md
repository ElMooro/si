## 1. Deploy

**Status:** success  
**Duration:** 17.9s  
**Finished:** 2026-07-14T18:50:40+00:00  

## Data

| fails | last_msgs | mi_markers_ok | mi_missing | sentinel_fresh | state_dealer_regime | state_dealer_squeeze | state_fails_spikes | state_fsi_pos | state_gcf_tri_bp | warns |
|---|---|---|---|---|---|---|---|---|---|---|
|  | None |  |  | True | NET_SHORT | True | ['corporate'] | False | 4.0 |  |
|  |  | True | [] |  |  |  |  |  |  |  |
| [] |  |  |  |  |  |  |  |  |  | [] |

## Log
- `18:50:22`   zip: 78576 bytes
## 1. Lambda

- `18:50:22`   Lambda exists — updating
- `18:50:26` ✅   ✓ updated justhodl-alert-sentinel
- `18:50:26`   zip: 97424 bytes
## 1. Lambda

- `18:50:27`   Lambda exists — updating
- `18:50:30` ✅   ✓ updated justhodl-morning-intelligence
## 2. Sentinel runtime verify

## 3. Morning-intelligence deployed-code markers

- `18:50:40` OPS 3308 PASS — dealer/funding stack now surfaces in the 8AM brief and fires Telegram flips (regime, squeeze, fails-spike, strain, FSI-cross).
