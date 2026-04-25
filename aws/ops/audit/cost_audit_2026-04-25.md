# JustHodl.AI — Cost Audit (2026-04-25)

## At a glance

- **30-day actual spend:** Cost Explorer permission still propagating; rerun in 5 min
- **Lambda GB-seconds (30d):** 2,202,138
- **Lambda free tier:** 400,000 GB-s/mo
- **Estimated Lambda spend:** $30.04/mo
- **CloudWatch Logs:** 1.56 GB (107 groups w/ no retention)

## Top 15 Lambdas by cost (GB-seconds)

| Lambda | mem (MB) | inv (30d) | GB-s | avg ms |
|---|---:|---:|---:|---:|
| `justhodl-daily-report-v3` | 1024 | 8,762 | 1,600,467 | 182660 |
| `scrapeMacroData` | 3008 | 90 | 237,938 | 900000 |
| `justhodl-crypto-intel` | 1024 | 5,650 | 102,696 | 18176 |
| `justhodl-options-flow` | 1024 | 8,674 | 89,044 | 10266 |
| `cftc-futures-positioning-agent` | 512 | 9,082 | 47,953 | 10560 |
| `justhodl-bloomberg-v8` | 2048 | 8,642 | 21,276 | 1231 |
| `justhodl-ultimate-orchestrator` | 1024 | 1,933 | 14,548 | 7526 |
| `manufacturing-global-agent` | 512 | 1,932 | 11,921 | 12340 |
| `dollar-strength-agent` | 512 | 1,933 | 10,164 | 10516 |
| `securities-banking-agent` | 512 | 1,933 | 9,015 | 9327 |
| `justhodl-repo-monitor` | 512 | 514 | 7,566 | 29439 |
| `fmp-stock-picks-agent` | 512 | 396 | 6,806 | 34372 |
| `justhodl-stock-screener` | 1024 | 182 | 6,482 | 35616 |
| `bond-indices-agent` | 512 | 1,957 | 5,639 | 5763 |
| `volatility-monitor-agent` | 512 | 1,933 | 5,570 | 5764 |

## Key findings

### `justhodl-daily-report-v3` is the biggest cost driver

- 1,600,467 GB-s/mo = ~$26.67/mo
- Average runtime: 3.0 minutes per invocation
- 1024MB memory, fires every 5 min × 30 days = 8,762 invocations/mo
- Each run takes ~3 minutes, much of which is waiting on FRED/Polygon/FMP API responses

### `scrapeMacroData` is wasted spend

- 237,938 GB-s/mo = ~$3.97/mo
- Average runtime: **15 minutes** per invocation (likely hitting 15-min timeout)
- 3008MB memory, only 90 invocations in 30 days
- This Lambda is at **100% error rate** for 7+ days (per health monitor)
- It's burning $4/mo failing in slow motion. Disable the EB rule until fixed.

## Recommended actions (ranked by safety × $-saved)

### 1. Disable scrapeMacroData EB rule (safe, $4/mo savings)

Lambda has been at 100% error rate for 7+ days, hitting 15-min timeout each invocation. Disabling the schedule stops the bleeding without deleting code. Reversible.

```bash
aws events list-rule-names-by-target --target-arn arn:aws:lambda:us-east-1:857687956942:function:scrapeMacroData
aws events disable-rule --name <rule-name>
```

### 2. Set 14-day retention on log groups (safe, ~$0-2/mo savings)

Currently 107 log groups accumulate forever. Setting 14-day retention is the AWS recommended default and doesn't affect any Lambda function.

```bash
for lg in $(aws logs describe-log-groups --query 'logGroups[?retentionInDays==null].logGroupName' --output text); do
  aws logs put-retention-policy --log-group-name "$lg" --retention-in-days 14
done
```

### 3. Investigate justhodl-daily-report-v3 runtime (medium, $5-10/mo potential savings)

3-minute average runtime is unusual for a 5-minute schedule — most of that time is API I/O. Options:

- **Reduce memory from 1024MB → 768MB** if CPU isn't bottleneck. Saves 25%, but may slow execution. Test first.
- **Parallelize FRED/Polygon/FMP fetches** with `asyncio.gather` if not already. Could cut runtime in half.
- **Reduce schedule from 5min → 10min** if 188 stocks don't move that fast for users. Saves 50%.

Do NOT change without testing — this Lambda is the heart of the system.

### 4. Fix the other 6 100%-error Lambdas (low risk, frees observability)

Per health monitor:

- `news-sentiment-agent` (439 inv/7d, 100% err)
- `global-liquidity-agent-v2` (439 inv/7d, 100% err)
- `fmp-stock-picks-agent` (90 inv/7d, 100% err)
- `daily-liquidity-report` (21 inv/7d, 100% err)
- `ecb-data-daily-updater` (21 inv/7d, 100% err)
- `treasury-auto-updater` (6 inv/7d, 100% err)

Each one warrants a quick triage: read the most-recent CloudWatch log, see what's erroring, fix or disable. Combined cost likely ~$1-2/mo (small invocation counts).

### 5. S3 Glacier lifecycle for archive/* (zero-risk, ~$0/mo)

S3 archive/ has 1,665 files in Standard storage. Move to Glacier Deep Archive after 90 days. Doesn't affect read access. Tiny savings ($0.01/mo) but good hygiene.

### 6. Delete 18 empty DynamoDB tables (zero-risk, $0/mo)

Pay-per-request billing means no idle cost, but cleanup is good hygiene.


## What NOT to touch

- `justhodl-daily-report-v3` 5-min cadence — heart of system; only reduce after testing
- `justhodl-signal-logger` cadence — calibration data feeder
- `justhodl-health-monitor` 15-min cadence — observability; cost is negligible
- `justhodl-signals` and `justhodl-outcomes` DDB tables — your training data

## Estimated total savings if all safe recommendations taken

- Disable scrapeMacroData: ~$4/mo
- Log retention: ~$0-2/mo
- Fix 6 broken Lambdas: ~$1-2/mo
- daily-report-v3 right-sizing (after testing): potentially $5-10/mo
- **Total potential: ~$10-18/mo**
