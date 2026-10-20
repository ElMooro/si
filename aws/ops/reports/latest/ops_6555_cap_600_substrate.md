# ops 6555 - $600 cap substrate (budget, role, control, daily guard, legacy budget)

**Status:** success  
**Duration:** 9.3s  
**Finished:** 2026-10-20T18:11:11+00:00  

## Data

| actual | budgets | mode | new_budget_limit | notifications | policy_statements |
|---|---|---|---|---|---|
| 2803.622 | JustHodl-Daily-Spend-Guard, JustHodl-Monthly-Hard-Cap-500, JustHodl-Monthly-Hard-Cap-600 | enforce | 600.0 | 5 | 15 |

## Log
- `18:11:02` ✅ created JustHodl-Monthly-Hard-Cap-600 ($600 MONTHLY, open-ended, cost types cloned from JustHodl-Monthly-Hard-Cap-500)
- `18:11:05` ✅ notifications on JustHodl-Monthly-Hard-Cap-600: 5 (added 5: ACTUAL 100.0% -> EMAIL:ra***@gmail.com, SNS:governor topic; ACTUAL 50.0% -> EMAIL:ra***@gmail.com, SNS:governor topic; ACTUAL 80.0% -> EMAIL:ra***@gmail.com, SNS:governor topic; ACTUAL 90.0% -> EMAIL:ra***@gmail.com, SNS:governor topic; FORECASTED 100.0% -> EMAIL:ra***@gmail.com, SNS:governor topic)
- `18:11:05` ✅ governor role: + cadence sentinel rights (events/scheduler list, describe, retime; PassRole to Scheduler only)
- `18:11:05` control.json: mode=enforce fuse_interval_h 6 -> 2 (cadence_enforce default True)
- `18:11:05` ✅ JustHodl-Daily-Spend-Guard: $40.0 -> $30 a day
- `18:11:06` ✅ deleted legacy budget 'My Monthly Cost Budget' ($150, e-mail only)
- `18:11:11` ✅ substrate ready; deploy the governor (v1.2.0 reads JustHodl-Monthly-Hard-Cap-600), then ops 6556 verifies and retires JustHodl-Monthly-Hard-Cap-500
