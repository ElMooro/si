# 0) Wait for any in-progress update

**Status:** success  
**Duration:** 41.1s  
**Finished:** 2026-05-04T22:55:51+00:00  

## Log
- `22:55:10`   ready, mod=2026-05-04T22:54:58.000+0000
# 1) Force redeploy

- `22:55:10`   zip size: 11,853b
- `22:55:11`   ✓ update_function_code accepted
- `22:55:15` ✅   ✓ deployed, mod=2026-05-04T22:55:11.000+0000
# 2) Inspect deployed source for Telegram digest code

- `22:55:18`   ✓ build_telegram_digest
- `22:55:18`   ✓ send_telegram
- `22:55:18`   ✓ get_telegram_chat_id
- `22:55:18`   ✓ digest send block
- `22:55:18`   ✓ SKIP_TELEGRAM env var
# 3) Invoke ai-brief end-to-end (will send Telegram)

- `22:55:50`   status: 200, duration: 32.3s
- `22:55:50`   brief_chars: 6811
# 4) Check CloudWatch logs for Telegram send line

# 5) Reconstruct what the Telegram digest looked like

- `22:55:51`   Snapshot inputs available:
- `22:55:51`     cal_v2.highest_weight: {'signal': 'carry_risk', 'weight': 1.453}
- `22:55:51`     cal_v2.weighted_mean_accuracy: 0.5527
- `22:55:51`     cal_v2.top_weighted_signals[:3]: ['carry_risk', 'crisis_hy_oas_vs_hyg', 'ml_risk']
- `22:55:51`     paper.signal_portfolio.n_open: 11
- `22:55:51`     paper.macro_loop2.system_alpha_pct: -0.28
- `22:55:51`     eurodollar.score: 36.5
- `22:55:51`     intel.phase: PRE-CRISIS
