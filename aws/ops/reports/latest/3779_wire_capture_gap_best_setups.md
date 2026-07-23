# ops 3779 — capture_gap overlay into best-setups

**Status:** success  
**Duration:** 31.9s  
**Finished:** 2026-07-23T19:50:37+00:00  

## Data

| capture_joined | capture_names | current_setups | forecast_overlap | invoke_seconds | invoke_status | setups_total | structurally_undervalued | with_catchup |
|---|---|---|---|---|---|---|---|---|
|  | 1771 | 50 | 47 |  |  |  |  |  |
|  |  |  |  | 7.8 | 200 |  |  |  |
| 47 |  |  |  |  |  | 50 | 0 | 46 |

## Log
## G0 — read the LIVE artifact, assert per-ROW fields

- `19:50:06` ✅ G0.container :: capture_gap container present
- `19:50:06` ✅ G0.all_rows :: all_rows n=1771
- `19:50:06` ✅ G0.row_ticker :: present on live rows
- `19:50:06` ✅ G0.row_capture_gap :: present on live rows
- `19:50:06` ✅ G0.row_global_capture_gap :: present on live rows
- `19:50:06` ✅ G0.row_tier :: present on live rows
- `19:50:06` ✅ G0.row_mcap_share_pct :: present on live rows
- `19:50:06` ✅ G0.row_undervaluation_score :: present on live rows
- `19:50:06` ✅ G0.catchup_present :: catchup_pct populated on live rows
- `19:50:06` ✅ G0.reads_chokepoint :: best-setups already loads chokepoint.json
- `19:50:06` ✅ G0.anchor :: splice anchor unique
- `19:50:06` ✅ G0.overlap_nonzero :: 47 setups will join (sample: ['ABBV', 'ABNB', 'ADBE', 'AEG', 'AKAM', 'ALB', 'ALGN', 'AMD'])
## Splice overlay (additive, before meta-intelligence block)

- `19:50:06` ✅ spliced + py_compile clean
- `19:50:06` ✅ SPLICE.present :: marker in source
- `19:50:06` ✅ SPLICE.structural_kept :: pre-existing structural overlay untouched
## Deploy

- `19:50:07`   zip: 108714 bytes
## 1. Lambda

- `19:50:07`   Lambda exists — updating
- `19:50:13` ✅   ✓ updated justhodl-best-setups
- `19:50:29` ✅ settled attempt 1
- `19:50:29` ✅ DEPLOY.settled :: new artifact live
## Invoke + prove the join is non-zero on the LIVE output

- `19:50:37` ✅ LIVE.join_nonzero :: 47 of 50 setups carry capture_gap (the 3766/3770 failure mode)
- `19:50:37` ✅ LIVE.fields_carried :: fields survived into the live artifact
## Joined setups (sample)

- `19:50:37`   GILD   gap= +18.8pp global=  +4.3pp catchup=    -43% tier=WATCH
- `19:50:37`   NVO    gap= +18.5pp global=  +3.0pp catchup=    300% tier=WATCH
- `19:50:37`   DBX    gap= +15.2pp global= +64.4pp catchup=    128% tier=WATCH
- `19:50:37`   AEG    gap=  +9.5pp global= -31.2pp catchup=    -14% tier=NONE
- `19:50:37`   ELV    gap=  +2.8pp global= -55.9pp catchup=    -12% tier=WATCH
- `19:50:37`   ADBE   gap=  +1.3pp global= +10.8pp catchup=     33% tier=WATCH
- `19:50:37`   LRCX   gap=  +1.2pp global=  +0.3pp catchup=    -57% tier=WATCH
- `19:50:37`   ASML   gap=  +0.5pp global=  -0.1pp catchup=    -56% tier=WATCH
- `19:50:37`   ABNB   gap=  +0.5pp global=  +9.8pp catchup=    -36% tier=WATCH
- `19:50:37`   ANET   gap=  -0.2pp global=  +3.2pp catchup=    -17% tier=WATCH
- `19:50:37`   NVDA   gap=  -0.6pp global=  +0.0pp catchup=    -60% tier=WATCH
- `19:50:37`   NEE    gap=  -0.9pp global= -35.5pp catchup=    -60% tier=WATCH
## Additive contract — best-setups keys must survive

- `19:50:37` ✅ ADDITIVE.structural_chokepoints :: present
- `19:50:37` ✅ ADDITIVE.top_setups :: present
- `19:50:37` ✅ ADDITIVE.triple_threats :: present
- `19:50:37` ✅ ADDITIVE.by_verdict :: present
- `19:50:37` ✅ ADDITIVE.structural_intact :: structural overlay still functioning
## VERDICT

- `19:50:37` ✅ PASS_ALL — capture_gap now reaches the setups desk as context
