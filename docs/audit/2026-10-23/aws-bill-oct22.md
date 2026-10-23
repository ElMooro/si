# Why Oct 22 billed $32.55, and what the account really costs a day (2026-10-23)

Khalid: "My monthly AWS is supposed to be $200. Yesterday alone AWS consumed $30, which puts the budget at $1,000 a
month. Investigate." The Daily-Spend-Guard budget ($30/day) went to ALARM for Oct 22 at $32.55.

Every number below is from Cost Explorer or the S3 server access logs, read on the runner (ops 6622-6625, all
read-only); the two fixes are ops 6626 and 6627. Raw data: `aws/ops/reports/latest/ops_662{2,3,4}_*_data.md`.

## 1. Oct 22 = $25.09 one-time + $7.46 normal

| Oct 22 line | $ | Kind |
|---|---|---|
| Archive backfill #2: 2,086,159 replication writes into Standard-IA (`Requests-SIA-Tier1`, $0.01 per 1,000) | 20.99 | one-time |
| S3 Batch Operations for that job (2,086,159 objects at $1/million + $0.25 job) | 2.34 | one-time |
| Reading the Infrequent-Access / Glacier-IR copies it replicated (`Retrieval-SIA` 174 GB, `Retrieval-GIR`) | 1.76 | one-time |
| **One-time total** | **25.09** | |
| Egress to the internet (`DataTransfer-Out-Bytes`, 18.1 GB) | 1.63 | recurring |
| Lambda compute (79.7 k GB-s) | 1.39 | recurring |
| CloudWatch (Metrics Insights alarm $0.64, GetMetricData $0.39, alarms $0.15, API) | 1.26 | recurring |
| Archive replication of the day's writes (83.6 k live PUTs, each copied to the archive) | 0.84 | recurring |
| S3 Standard storage, live bucket (~1.05 TB) | 0.78 | recurring |
| S3 requests on the live bucket (PUT/LIST $0.52, GET $0.21) | 0.72 | recurring |
| S3 Infrequent-Access storage (archive partly filled + warm tier) | 0.29 | recurring |
| Everything else (Secrets Manager 0.12, Cost Explorer API 0.09, ECR 0.09, EBS 0.04, DynamoDB 0.07, inventory, DR, SSM) | 0.55 | recurring |
| **Recurring total** | **7.46** | |

The backfill is the job Khalid approved on Oct 22 ("continue"): job `646ccb69`, 2,086,159 / 2,086,159 succeeded, 0
failed, finished about 00:13 UTC Oct 23. Its cost was estimated beforehand at "$21 of Standard-IA PUTs + $2.3 Batch
Operations" (WAREHOUSE_ARCHIVE.md); the bill matches that, plus $1.76 for reading copies that were already in the
live bucket's Infrequent-Access tier. It does not recur. Oct 21 had the same shape: $22.06 = $14.35 for backfill #1
(1,282,030 objects) + $7.71 normal.

## 2. October so far

Cost Explorer, 2026-10-23 17:44 UTC: usage **$2,869.72**, **credits -$1,783.38** (posted against Oct 12-16, the loop
days: -$72.36, -$441.04, -$448.27, -$446.92, -$374.79), support **$11.58** (AWS Business Support+, Oct 19) = **$1,097.93
net**. The budget's "actual" is this net figure. Earlier documents quoting "$2,803.62 through Oct 19" and "October
closes near $2,900" were gross, written before the credit appeared.

## 3. What was found and fixed today

**a. Apple's crawler was downloading 10-30 GB a day straight from S3 (ops 6623-6626).** The single largest movable
cost. The access logs show the direct (not Cloudflare, not AWS) readers of the live bucket at 10.2 GB on Oct 22 and a
30.4 GB/day pace on Oct 23, round the clock, from 17.166.x addresses that reverse-resolve to
`*.applebot.apple.com`, user agent `... Safari/605.1.15 (Applebot/0.1; +http://www.apple.com/go/applebot)`. Applebot
renders the site's pages; `why.html`'s fleet-forensic block fetches eleven 0.1-4.5 MB files (best-setups, share-flows,
estimate-revisions, buyback-engine, forensic-screen, ...) directly from
`justhodl-dashboard-live.s3.us-east-1.amazonaws.com` with a `?_=<now>` cache-buster - about 6,000 renders a day. At
$0.09/GB that was $0.9-2.7/day. **Fix (ops 6626, 18:10 UTC):** the existing bucket statement
`DenyAbusiveScrapersByUserAgent` (ops 6549, Lightpanda) now also lists `*Applebot*`. Proven on the live bucket:
Applebot UA -> 403, Safari -> 200, Googlebot -> 200, Lightpanda -> 403, all 51 public probe objects answer a browser
exactly as before; rollback copy `s3://justhodl-ops-private-857687956942/backups/bucket-policy/justhodl-dashboard-live-20261023T180953Z.json`.
Applebot still crawls the HTML pages (GitHub Pages / Cloudflare); only its renderer's direct data downloads are refused.

**b. The log-ingestion brake was billed per log group (ops 6627).** `CW:MetricInsightAlarmUsage` was $0.60-0.64/day
($19/month) from the day the fleet thawed: alarm `justhodl-logs-ingestion-2gb-hour` (ops 6580) ran the Metrics
Insights query `SELECT SUM(IncomingBytes) FROM SCHEMA("AWS/Logs", LogGroupName)`, which bills $0.10 per log-group
metric analysed per month. CloudWatch Logs publishes the same sum as the account-level metric `AWS/Logs
IncomingBytes`; the two agreed hour by hour (16:00 and 17:00 UTC exactly, 15:00 within 0.06 MB). The alarm keeps its
name, threshold (> 2 GiB in an hour), actions (justhodl-fleet-alerts) and missing-data rule; it now watches the account
metric ($0.10/month). Backup `backups/alarms/justhodl-logs-ingestion-2gb-hour-20261023T181158Z.json`. The other
Metrics Insights alarm (`justhodl-guard-lambda-recursion-dropped`) analyses no metric and costs nothing; untouched.

## 4. November projection

| | $/day |
|---|---|
| Recurring, as measured on Oct 22 | 7.46 |
| + archive storage at its full 1,032 GB of Standard-IA (128 KB minimum per object) | +0.27 |
| + archive old-copy churn (replaced copies billed for Standard-IA's 30-day minimum; estimate, first visible ~Oct 25) | +0.16 |
| - Applebot's direct downloads refused (Oct 22 level, 10.2 GB) | -0.92 |
| - log-ingestion alarm on the account metric | -0.64 |
| **Steady day** | **6.34** |

30 days = $190; November's always-free tiers (Lambda 400 k GB-s, 100 GB egress, 10 alarms) take ~$17 off: **~$173 of
AWS usage**, plus **AWS Business Support+ ($29/month minimum)** = **~$202**. Band: Cloudflare-path egress was 9.5 GB on
Oct 22 and a 17 GB/day pace on Oct 23; if the higher figure holds, add ~$20/month.

## 5. Decisions for Khalid (nothing below has been changed)

1. **AWS Business Support+** - enrolled 2026-10-19 (prorated $11.58). Price: the greater of $29/month or 9% of the
   month's AWS charges, calculated on gross charges before credits (AWS pricing page). Without it November is ~$173.
   Risk to check now: AWS does not say publicly whether October's fee is 9% of the whole month's gross usage
   (~$2,930 incl. the credited loop days, i.e. up to ~$264) or only the days since sign-up; the Support line on the
   Bills page, or a billing case under the plan itself, answers it before Oct 31. Minimum commitment 30 days.
2. **Archive replication** - continuous replication copies every write (83.6 k/day) into Standard-IA at $0.01 per
   1,000 = $0.84/day ($25/month), about five times the $3-5/month in the archive design; with ~$13-15/month of storage
   and the churn above, the never-erase copy costs ~$45/month in all. Replicating on a schedule instead (e.g. weekly:
   the latest version of every file that changed, from the S3 Inventory) would copy a file that is rewritten every day
   once instead of seven times; how much that saves depends on how many of the 84 k daily writes are rewrites of the
   same files rather than new files, which the inventory can measure before deciding. The trade-off: versions between
   two scheduled copies would live only in the live bucket (which keeps 10 old copies per file). Keeping it continuous
   is a valid choice.
3. **Crawlers through the Worker** - the Cloudflare path still serves crawlers' renders; a Worker rule that answers
   `/data/*` with 403 for crawler user agents (Applebot, Lightpanda; optionally others) would cut part of the
   9.5-17 GB/day there. Search engines would then index pages without their data panels.
4. **Small cleanup (~$7/month)** - 9 Secrets Manager secrets not read since 2025 ($3.60/month; keys live in SSM since
   Sep 2026), two stopped 2025 EC2 instances "web server" and "OpenBB Server" (stopped 2026-08-01) still billing their
   8 GB disks ($1.28/month), ~22 alarms for retired systems (OpenBB, aiapi, scrapeMacroData, indexToOpenSearch,
   Gateway; ~$2.20/month).
5. **Governor meter reads** - GetMetricData is $0.39/day ($12/month); reading Duration only for functions that ran in
   the hour would roughly halve it (a governor code change).

## 6. Facts worth keeping

* Writes: ~84 k objects/day (CE PutObject). Largest writers (access-log sample): the runner's daily research
  refreshes (`statement-source-refresh`, `capital-structure-source-refresh`) ~15 k/day into
  `audit-private/20260909-originals/`, fundamental-graphs ~9-9.5 k/day into `data/fundgraph/`, polygon-fx-regime,
  tail-risk, world-uncertainty-index, esma-ratings, equity-research.
* Lambda by engine (governor meter, Oct 22): $1.32 over 791 engines; symdir $0.23, research-backtest $0.09,
  fundamental-graphs $0.07, equity-research $0.07, causality-scanner $0.06.
* Egress classes (access logs, Oct 22): direct 10.2 GB (Applebot), Cloudflare 9.5 GB, runner 1.0 GB. In-region Lambda
  and replication reads are free.
* Hourly and resource-level Cost Explorer data are not enabled on the payer account (opt-in), so bucket-level and
  hour-level splits come from the access logs.

## 7. Khalid's decisions (2026-10-23 13:21 CT) and what was done

1. **AWS Business Support+: keep** ("AWS support is fine, just leave it"). Nothing changed.
2. **Archive: explained first**, then Khalid chose **B** ("2archive lets do B": protect only what can't be re-downloaded,
   skip what the engines rebuild). Measured per folder (ops 6631, 6632): the archive's copying costs $29.00/month now,
   $46.79/month averaged over a year; scope B (22 prefixes, `config/archive-scope.json`) would save $15.18/month now,
   $23.04 averaged. **Not applied - S3 cannot do it with a rule:** replication filters only include, and the workaround
   (a Disabled higher-priority rule on the folder) was proven useless on a canary prefix - every excluded canary was
   still copied, for 4.3 min (ops 6633) and 20 min (ops 6636); the configuration was put back exactly both times.
   Offered instead (his decision): Intelligent-Tiering for new copies (ops 6637, staged; $29.00 -> $16.44/month now,
   $46.79 -> $27.36 averaged; same protection), or B through a few hundred include rules plus a job adding new folders
   (not recommended). Details: docs/ops/WAREHOUSE_ARCHIVE.md "Narrowing the archive".
3. **Crawlers blocked from the data at Cloudflare and at S3** ("as long as it doesn't affect my system operations"):
   * Worker `justhodl-data-proxy` (commit 14261948, deploy-workers run 40004203815 green): GET/HEAD from a self-declared
     crawler (config/crawler-user-agents.json `worker_tokens`, 69 tokens: Googlebot, bingbot, Applebot, GPTBot, ClaudeBot,
     CCBot, Bytespider, AhrefsBot, SemrushBot, ...) gets 403 before any route or S3 read; `/`, `/health`, `/robots.txt`
     stay open; the workers.dev host serves `robots.txt: Disallow: /`. Never a generic word - the system's own clients are
     JustHodlBot, JustHodl-Bot, justhodl-tv-crawler. tests/worker-crawler-gate.test.js (7) proves both halves; the
     preservation chain carries the new layer (fixtures/worker-crawler-gate); Worker suite 290/290.
   * Bucket (ops 6628): the statement `DenyAbusiveScrapersByUserAgent` carries the 24 `s3_tokens` (crawlers that render
     JavaScript and so reach the S3 URLs pages still use); 51 public probes unchanged, 28 own-client agents 200, policy
     18,544 bytes; rollback copy `backups/bucket-policy/justhodl-dashboard-live-20261023T183709Z.json`.
   * Live proof (ops 6629): on justhodl.ai and the workers.dev host every crawler sample gets 403 and 26 own-client agents
     get data; real renders in headless Chrome (index, why.html?t=NVDA, master-rank, risk) with Chrome's own agent and a
     desktop Chrome agent loaded 184-188 data files, none refused; the same pages with the Googlebot agent: every data
     request refused (Worker + bucket). Cloudflare's own edge already refuses the bare `Python-urllib` agent (also on the
     gate-exempt /health, so not the gate); no engine uses that agent. GPTBot/ClaudeBot/Bytespider are already stopped
     on justhodl.ai by Cloudflare's zone bot setting before the Worker runs.
4. **Small cleanup (ops 6630):** 20 alarms whose metric reported nothing for 30 days deleted (OpenBB, aiapi duration,
   enhanced-openbb-handler, indexToOpenSearch, the old API-gateway and universal-agent-gateway alarms; definitions in
   `backups/alarms/retired-systems-20261023T184828Z.json`), ~$2/month. Kept because their metrics are still live:
   OpenBB-High-Error-Rate, OpenBB-High-Latency, aiapi-lambda-errors, scrapeMacroData-*. **Not done:** the 9 Secrets
   Manager secrets - the runner has no Secrets Manager rights (AccessDenied on DescribeSecret), so they need a console
   delete or a narrow grant; the two stopped servers - terminating deletes their disks for good and the runner may not
   snapshot them first (the 2026-10-20 seal denies ec2:CreateSnapshot*), so they wait for Khalid's word.
