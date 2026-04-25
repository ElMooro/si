# Plan vs Usage Audit — 2026-04-25 10:13 UTC

**Question:** Are we using everything we're already paying for?

**Findings count:** 8


## Summary

- 🔴 High value / costing data risk: **1**
- 🟡 Optimization opportunity: **7**
- ⚫ Pure waste: **0**

## Findings


### 🔴 DynamoDB: PITR (Point-In-Time-Recovery) NOT enabled on any table — including justhodl-signals + justhodl-outcomes (your learning data)

**Recommendation:** PITR provides 35 days of continuous backup recovery. Costs ~\$0.20/GB-month. Your signals + outcomes tables are tiny — PITR cost would be < \$0.01/mo combined. Enable on justhodl-signals and justhodl-outcomes — losing this data would set the learning loop back to zero. aws dynamodb update-continuous-backups --table-name justhodl-signals --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true

### 🟡 Lambda: 33 Python Lambdas eligible for SnapStart but NONE using it

**Recommendation:** SnapStart is FREE and reduces cold start times by 10x. Enable on Lambdas that are user-facing (justhodl-ai-chat, justhodl-stock-analyzer, justhodl-investor-agents, justhodl-stock-screener Lambda URLs). aws lambda update-function-configuration --snap-start ApplyOn=PublishedVersions

### 🟡 Lambda: Only 0/97 Lambdas on arm64; x86_64 is 20% more expensive

**Recommendation:** Switch Python Lambdas to arm64 (Graviton2). 20% cheaper for same workload, fully compatible with all the boto3/urllib code we use. Migration is a single Architectures=['arm64'] update per Lambda. Could save \$2-4/mo at current spend.

### 🟡 S3: No Intelligent Tiering configured on justhodl-dashboard-live

**Recommendation:** Intelligent Tiering auto-moves objects to cheaper tiers based on access patterns. FREE for objects > 128KB. Could save \$0.10-0.50/mo on archive/ + valuations-archive/ + investor-analysis/. Apply at bucket level via put-bucket-intelligent-tiering-configuration.

### 🟡 S3: Bucket versioning DISABLED on justhodl-dashboard-live

**Recommendation:** Versioning protects against accidental overwrites/deletes. Free in itself (only pay for the extra storage of old versions). Combined with a 30-day expiration lifecycle on old versions, costs essentially nothing but gives you a safety net. Enabling now is one-time, retroactive isn't possible.

### 🟡 Cloudflare: Free Workers tier covers 100K req/day; only 1 Worker (ai-proxy) deployed. KV/D1/R2 not used at all.

**Recommendation:** Cloudflare free tier offers serious value we're leaving unused: (1) **Workers KV** (100K reads/day, 1K writes/day FREE) — perfect for caching FRED API responses globally with low latency. (2) **R2 storage** (10GB free, NO egress fees) — you're paying S3 egress fees if any external service reads from S3; R2 has zero egress. (3) **D1 SQLite** (5GB free) — could replace the read-heavy DDB lookups on justhodl-signals. (4) **Workers Cache API** — already free, can speed up scorecard.json delivery. Worth a separate session to evaluate which to enable.

### 🟡 FMP Premium: Paying for FMP Premium but only using 1 of ~10 valuable premium endpoints

**Recommendation:** FMP Premium gives you analyst ratings, insider trading, earnings transcripts, SEC filings, and economic calendar. We're only hitting screener + quote. Build (1) an insider-trading widget for stocks page, (2) earnings-transcript summarizer using Claude, (3) economic calendar feed for the next-events ticker, (4) analyst ratings consensus. These are all 1-Lambda, ~30-min builds each.

### 🟡 Anthropic API: Anthropic Message Batches API NOT used — 50% cost discount on async work

**Recommendation:** Batch API gives 50% discount + 24h SLA. Perfect for non-real-time work: (1) the 6 investor agents (Buffett/Munger/Burry/Druckenmiller/Lynch/Wood) on the legendary panel — these are async by nature. (2) the morning brief composition. (3) self-improvement of prompt templates. Real-time stays on standard pricing (ai-chat). Just changing the API call site cuts those Anthropic costs in half.

## What we're using well

- Lambda is on PAY_PER_REQUEST equivalent (no idle compute charges)
- DynamoDB on PAY_PER_REQUEST (no idle table charges)
- S3 has lifecycle policy for archive/* → Deep Archive
- 14-day retention on all 107 CloudWatch log groups
- FRED cache shared across Lambdas (88% cache hit rate)
- CI/CD via GitHub Actions (no separate CodePipeline cost)
- Cloudflare Workers (free tier handles ai-proxy)
- ~96 Lambdas under \$30/mo combined (great efficiency)