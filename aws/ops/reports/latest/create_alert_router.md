# Create justhodl-alert-router + 30min schedule

**Status:** success  
**Duration:** 16.0s  
**Finished:** 2026-05-04T13:33:07+00:00  

## Log
- `13:32:51`   zip size: 4,829b
- `13:32:51` ✅   ✓ telegram token sourced from justhodl-telegram-bot.TELEGRAM_TOKEN  (len=46)
- `13:32:52` ✅   ✓ created
# EventBridge schedule (every 30 minutes)

- `13:32:57` ✅   ✓ wired
# Smoke test — first run

- `13:33:06`   status: 200  duration: 9.6s
- `13:33:06`   resp: {"statusCode": 200, "body": "{\"candidates\": 8, \"sent\": 8, \"suppressed\": 0, \"duration_s\": 8.62}"}
# S3 verify

- `13:33:07`   alert-history.json: 8 alerts in history
- `13:33:07`   last_run: 2026-05-04T13:33:06.411212+00:00
- `13:33:07`   last_run_summary: {'candidates': 8, 'sent': 8, 'suppressed': 0}
- `13:33:07`     HIGH   [CORRELATION   ] 🔁 Correlation regime break: IWM↔UUP
- `13:33:07`     HIGH   [CORRELATION   ] 🔁 Correlation regime break: USO↔UUP
- `13:33:07`     MEDIUM [SHORT_INTEREST] 🚨 Squeeze risk: LIN
- `13:33:07`     MEDIUM [SHORT_INTEREST] 🚨 Squeeze risk: SHOP
- `13:33:07`     LOW    [SECTOR        ] 📊 Market breadth: NARROW_LEADERSHIP
