# Step 87 — Activate Telegram alerter (token to SSM)

**Status:** success  
**Duration:** 13.5s  
**Finished:** 2026-04-25T01:17:01+00:00  

## Data

| iam_policy | next_alert | ssm_token_path |
|---|---|---|
| HealthMonitorTelegramSSM | on next green→red transition (or recovery) | /justhodl/telegram/bot_token |

## Log
## 1. Check / create SSM /justhodl/telegram/bot_token

- `01:16:48` ✅   Created SecureString /justhodl/telegram/bot_token
## 2. Patch get_telegram_creds() to read token from SSM

- `01:16:48` ✅   Patched (16735 bytes)
## 3. Verify lambda-execution-role can read the SecureString

- `01:16:48` ✅   Attached inline policy HealthMonitorTelegramSSM
## 4. Re-deploy Lambda

- `01:16:52` ✅   Re-deployed: 8258 bytes
## 5. Sync invoke to verify alerter doesn't error

- `01:17:00` ✅   Invoke clean (status 200)
## 6. Check Lambda log for [ALERTER] lines

## 7. Send confirmation Telegram (one-time)

- `01:17:01` ✅   Test message sent successfully
- `01:17:01` Done
