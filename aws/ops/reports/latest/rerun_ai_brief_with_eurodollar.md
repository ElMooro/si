# 1) Re-invoke ai-brief with new eurodollar data

**Status:** success  
**Duration:** 30.9s  
**Finished:** 2026-05-04T19:57:05+00:00  

## Log
- `19:57:05`   status: 200  duration: 30.7s
- `19:57:05`   resp: {"statusCode": 200, "body": "{\"duration_s\": 29.72, \"brief_chars\": 6805, \"snapshot_keys\": [\"as_of\", \"intelligence\", \"calibration\", \"sectors\", \"momentum\", \"allocator\", \"asymmetric_setups\", \"risk_sizer\", \"auction_stress\", \"eurodollar_stress\", \"macro_surprise\", \"insider_buys\", \"earnings_pead\", \"correlation_breaks\", \"alerts\"], \"error\": null}"}
# 2) Verify eurodollar in snapshot

- `19:57:05`   generated_at: 2026-05-04T19:56:36.360086+00:00
- `19:57:05`   duration_s: 29.72
- `19:57:05`   brief_md_chars: 6805
- `19:57:05`   usage: in=3591 out=2500
- `19:57:05` 
- `19:57:05`   Snapshot.eurodollar_stress (was 'not deployed' yesterday):
- `19:57:05`     composite_score: None
- `19:57:05`     severity:        None
- `19:57:05`     regime:          CALM
- `19:57:05`     n_signals_used:  None/None
- `19:57:05`     hot_signals:     []
- `19:57:05`     cold_signals:    []
- `19:57:05` 
- `19:57:05`   Brief mentions of eurodollar terms (first match each):
- `19:57:05`     eurodollar    : - **Curve**: Normal, but stress building in HY credit and eurodollar positioning.
- `19:57:05`     eurod         : - **Curve**: Normal, but stress building in HY credit and eurodollar positioning.
- `19:57:05`     stress        : - **Curve**: Normal, but stress building in HY credit and eurodollar positioning.
- `19:57:05`     FSI           : | **St. Louis Fed FSI** | -0.68 | Score 28.9/100 (calm but negative) | 🟡 DETERIORATING |
