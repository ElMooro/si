# Sunday April 26 — First Real Calibration Run

**Why this matters:** This is the first time the JustHodl.AI calibrator
will run with actual scored outcomes. Every previous Sunday calibration
ran against zero true outcomes (all `correct=None` due to the broken
price-fetch APIs). After the fixes shipped Friday April 24/25, the
learning loop is finally closing.

## What to check Sunday morning (April 26, after 9 AM UTC = 4 AM ET)

### 1. Did the calibrator actually run?

CloudWatch metrics for `justhodl-calibrator`:
```
- Invocations between 09:00 and 09:15 UTC: should be 1
- Errors: should be 0
- Duration: typically 5-30 seconds
```

EB rule that triggered it: `justhodl-calibrator-weekly`
Schedule: `cron(0 9 ? * SUN *)`

### 2. What weights did it produce?

Read SSM parameter `/justhodl/calibration/weights`:
```bash
aws ssm get-parameter --name /justhodl/calibration/weights --query 'Parameter.Value' --output text | jq
```

Expected to contain ~24 signal types (per Week 1 calibrator expansion):
- khalid_index, screener_top_pick, valuation_composite
- cftc_gold, cftc_spx, cftc_bitcoin, cftc_crude
- edge_regime, edge_composite, market_phase
- crypto_btc_signal, crypto_eth_signal, crypto_fear_greed, crypto_risk_score, btc_mvrv
- carry_risk, ml_risk, plumbing_stress
- momentum_spy, momentum_gld, momentum_uso
- cape_ratio, buffett_indicator
- screener_buy, screener_sell

### 3. How does the new accuracy compare to the previous weights?

Read SSM parameter `/justhodl/calibration/accuracy`:
```bash
aws ssm get-parameter --name /justhodl/calibration/accuracy --query 'Parameter.Value' --output text | jq
```

Specifically check:
- **crypto_fear_greed** and **crypto_risk_score** — previously showed
  accuracy=0.0 with n=369. After the price-fetch fix, these should
  show real numbers. Watch for: accuracy stuck at 0% might indicate
  signals genuinely don't predict short-term moves (in which case
  the calibrator will assign them low weights, which is correct).

- **Signal types with `n` (sample count)** — most types will have low
  n at first because the new schema only started capturing baseline_price
  on Friday night. Real n>10 takes about a week of daily/weekly outcome
  scoring.

### 4. Was the full report saved?

Check S3:
```bash
aws s3 ls s3://justhodl-dashboard-live/calibration/history/
```

Expected: a new file `2026-04-26.json` containing the full report
including `top_performing_signals`, `worst_performing_signals`, and
`recommendations` (e.g., "✅ khalid_index: accuracy 73% (n=12) — high
confidence, increase weighting").

### 5. Did outcome-checker keep firing this week?

CloudWatch invocation count for `justhodl-outcome-checker` between
April 25 00:00 UTC and April 26 09:00 UTC:
- Daily rule (cron(30 22 ? * MON-FRI *)): expected 1 fire (Friday 22:30)
- Sunday rule (cron(0 8 ? * SUN *)): expected 1 fire (Sunday 8:00)
- Total: 2 invocations expected

## Likely scenarios and what they mean

### Scenario A: Most signals have n=0 or n=1
**Interpretation:** Normal. The schema-v2 + baseline_price fixes only
took effect Friday night. New signals are still aging through their
check windows (1d/3d/7d/14d/30d). Real meaningful accuracy data needs
2-3 weeks of accumulation.

**Action:** Don't rebalance anything. Let the loop run. Revisit
May 11 (3 weeks out) when most types have n>=10.

### Scenario B: Some signals show 0% accuracy with n>=10
**Interpretation:** Either (a) the signal is genuinely bad — its
predicted_direction logic is wrong, or (b) the underlying scoring
threshold (±0.5%) is too tight for that signal's natural variance.

**Action:** Review the signal's logger logic in
`aws/lambdas/justhodl-signal-logger/source/lambda_function.py`. The
calibrator's `worst_performing_signals` field will list these.

### Scenario C: Some signals show 70%+ accuracy with n>=10
**Interpretation:** Real edge found. The calibrator's
`accuracy_to_weight()` will boost these to weight > 1.0.

**Action:** These are candidates for promotion. Worth investigating
whether they're stable (n grows, accuracy holds) or just lucky early
samples.

### Scenario D: Calibrator failed
**Interpretation:** Bug. Likely a DynamoDB scan timeout (4,400+ legacy
signals plus new ones), or a parsing issue with the new schema v2 fields.

**Action:** Read CloudWatch logs for `/aws/lambda/justhodl-calibrator`,
identify error, file as next ops priority.

## What's still on the roadmap (don't do until calibration data is in)

1. **Week 2B — Backtester Lambda** (~6 hours design + code)
   - Replays signal-logger logic against historical data
   - Walk-forward 90-day windows
   - Don't start until we see 2-3 weeks of real accuracy data

2. **ml-predictions decision** (~10 min decision, 0-3h work)
   - Lambda is broken (calls dead api.justhodl.ai bundled-data endpoint)
   - 0/7 of its data sources exist in S3 directly
   - Decide: retire, resurrect (rewrite to read from S3), or rebuild

3. **Week 3A — Daily Ranker Lambda** (~4 hours)
   - Reads accuracy weights, produces top-10 ranked opportunities
   - Don't start until calibrator weights are trustworthy (n>=10 per signal)

4. **Week 3B — Position sizing layer** (~3 hours)
   - Multipliers from accuracy weights
   - Last in dependency chain

## Repo state at this checkpoint

- Pending folder: empty
- History folder: 14 archived scripts from April 24/25
- Design docs: `aws/ops/design/2026-04-25-week-2-3-architecture.md` and `2026-04-25-decisions-locked.md`
- Latest reports: `aws/ops/reports/latest/`
- Schema v2 verified deployed and capturing on 100% of new signals
- Daily/weekly/monthly outcome-checker EB rules all enabled
- All design questions answered with defaults

## Bottom line

The system is in better shape than it's been since launch. Sunday
morning's calibration is the **first meaningful learning event** since
the platform was built. Even if the numbers look sparse or weird,
that's expected — what matters is that the loop closes for the first
time. Weeks 2 onwards depend on this data accumulating.

Sleep well. Check Sunday morning. Don't touch anything until you've
read the calibration output. 🫡
