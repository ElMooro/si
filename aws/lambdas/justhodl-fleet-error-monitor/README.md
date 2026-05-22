# justhodl-fleet-error-monitor

Deployed: 2026-05-22 as part of AUDIT_2026-05-22.md institutional foundation.

**Schedule:** rate(5 minutes)
**Schedule state:** ENABLED

## Purpose
This Lambda is part of the JustHodl.AI institutional observability foundation.
It runs autonomously and alerts via SNS + Telegram when conditions are met.

## Environment variables required
- `SNS_ARN`: arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts
- `TELEGRAM_BOT_TOKEN`: Khalid's bot token
- `TELEGRAM_CHAT_ID`: Khalid's chat ID
- `ERROR_RATE_THRESHOLD` (default 5.0 %)
- `MIN_INVOCATIONS` (default 5 — filter noise)
- `LOOKBACK_MINUTES` (default 15 min)
