# CANONICAL — this is the source of truth for the health-monitor Lambda.
# A copy at aws/ops/health/expectations.py exists for ops-script compatibility
# (early one-off deploy scripts referenced that path). Edits should be made here;
# the deploy-lambdas.yml workflow zips this directory and pushes to AWS.
#
# ═══════════════════════════════════════════════════════════════════
#  JustHodl.AI Health Expectations
#
#  Defines what "healthy" looks like for every component the monitor
#  Lambda checks. Edited as a YAML-style Python dict (loaded directly
#  by the monitor). Comments document the reasoning per entry.
#
#  Per-component fields:
#    type:           s3_file | lambda | dynamodb | ssm | eb_rule
#    fresh_max:      Max age (sec) for "green". Below this = healthy.
#    warn_max:       Max age (sec) for "yellow". Above this = "red".
#                    None = no upper bound (e.g. weekly schedules).
#    expected_size:  Min bytes — alerts if file shrinks below this
#                    (catches "wrote {} when it should be 50KB" bugs)
#    note:           Human-readable for the dashboard tooltip
#    severity:       critical | important | nice_to_have
#                    (drives Telegram alerting threshold)
#
#  When in doubt — set fresh_max generously (avoid alert fatigue) and
#  let observed misses tighten it later.
# ═══════════════════════════════════════════════════════════════════

EXPECTATIONS = {

    # ─── S3 critical path files ──────────────────────────────────────
    "s3:data/report.json": {
        "type": "s3_file",
        "key": "data/report.json",
        "fresh_max": 600,        # 10 min (writer is every 5 min)
        "warn_max": 1800,        # 30 min
        "expected_size": 500_000, # ~1.7MB normally; alert if <500KB
        "note": "Source of truth — 188 stocks + FRED + regime. daily-report-v3 every 5min.",
        "severity": "critical",
    },
    "s3:crypto-intel.json": {
        "type": "s3_file",
        "key": "crypto-intel.json",
        "fresh_max": 1200,       # 20 min (writer is every 15 min)
        "warn_max": 3600,        # 1h
        "expected_size": 30_000, # ~55KB normally
        "note": "BTC/ETH/SOL technicals + on-chain. crypto-intel every 15min.",
        "severity": "critical",
    },
    "s3:edge-data.json": {
        "type": "s3_file",
        "key": "edge-data.json",
        "fresh_max": 25_000,     # ~7h (writer is every 6h)
        "warn_max": 43_200,      # 12h
        "expected_size": 1_000,  # Healthy compact runs are ~1.2KB; full runs ~11KB.
                                 # Step 86 (2026-04-25) tried this fix but pattern
                                 # mismatched another way — applied correctly now.
        "note": "Composite ML risk score, regime. edge-engine every 6h.",
        "severity": "critical",
    },
    "s3:data/insider-trades.json": {
        "type": "s3_file",
        "key": "data/insider-trades.json",
        "fresh_max": 2400,        # 40 min (writer is every 30 min)
        "warn_max": 7200,         # 2h
        "expected_size": 5_000,   # Even an empty day has stats + headers ~5KB
        "note": "SEC EDGAR Form 4 insider trades. justhodl-insider-trades every 30min.",
        "severity": "important",
    },

    "s3:repo-data.json": {
        "type": "s3_file",
        "key": "repo-data.json",
        "fresh_max": 3600,       # 1h (writer is every 30 min weekdays)
        "warn_max": 14_400,      # 4h
        "expected_size": 5_000,
        "schedule": "weekday_market_hours",   # cron(0/30 13-23 ? * MON-FRI *)
        "note": "Repo plumbing stress. repo-monitor every 30min weekdays.",
        "severity": "critical",
    },
    "s3:flow-data.json": {
        "type": "s3_file",
        "key": "flow-data.json",
        "fresh_max": 18_000,     # 5h (writer is every 4h)
        "warn_max": 32_400,      # 9h
        "expected_size": 15_000,
        "note": "Options/fund flows. options-flow every 4h.",
        "severity": "important",
    },
    "s3:intelligence-report.json": {
        "type": "s3_file",
        "key": "intelligence-report.json",
        "fresh_max": 7200,       # 2h (hourly weekdays)
        "warn_max": 14_400,      # 4h
        "expected_size": 2_000,
        "schedule": "weekday_market_hours",   # cron(5 12-23 ? * MON-FRI *)
        "note": "Cross-system synthesis. Heart of ai-chat + signal-logger.",
        "severity": "critical",
    },
    "s3:screener/data.json": {
        "type": "s3_file",
        "key": "screener/data.json",
        "fresh_max": 18_000,     # 5h
        "warn_max": 32_400,      # 9h
        "expected_size": 100_000,
        "note": "503 stocks Piotroski/Altman scored. stock-screener every 4h.",
        "severity": "important",
    },
    "s3:valuations-data.json": {
        "type": "s3_file",
        "key": "valuations-data.json",
        "fresh_max": 2_678_400,  # 31 days (writer is monthly!)
        "warn_max": 3_456_000,   # 40 days
        "expected_size": 1_000,
        "note": "CAPE, Buffett indicator. valuations-agent monthly (1st 14:00 UTC).",
        "severity": "nice_to_have",
    },
    "s3:calibration/latest.json": {
        "type": "s3_file",
        "key": "calibration/latest.json",
        "fresh_max": 691_200,    # 8 days (writer is weekly Sun 9 UTC)
        "warn_max": 950_400,     # 11 days
        "expected_size": 500,
        "note": "Calibrator output. Sunday 9 UTC.",
        "severity": "important",
    },
    "s3:learning/last_log_run.json": {
        "type": "s3_file",
        "key": "learning/last_log_run.json",
        "fresh_max": 25_200,     # 7h (signal-logger every 6h)
        "warn_max": 43_200,
        "expected_size": 50,
        "note": "signal-logger heartbeat. last_log_run.json updated each invocation.",
        "severity": "critical",
    },

    # ─── KNOWN BROKEN — track but don't alarm (don't hide either) ────
    "s3:predictions.json": {
        "type": "s3_file",
        "key": "predictions.json",
        "fresh_max": None,       # No fresh expectation — known broken
        "warn_max": None,
        "note": "ml-predictions Lambda broken since 2026-04-22 CF migration. "
                "Tracked for visibility; no alert. Downstream bypassed via intelligence fix.",
        "severity": "nice_to_have",
        "known_broken": True,
    },
    "s3:data.json": {
        "type": "s3_file",
        "key": "data.json",
        "fresh_max": None,       # Stale orphan, expected
        "warn_max": None,
        "note": "Legacy orphan, 65+ days stale. Replaced by data/report.json. "
                "Tracked but no alert.",
        "severity": "nice_to_have",
        "known_broken": True,
    },

    # ─── Lambdas (error rates + invocation count via CloudWatch) ─────
    "lambda:justhodl-daily-report-v3": {
        "type": "lambda",
        "name": "justhodl-daily-report-v3",
        "max_error_rate": 0.10,   # 10% errors over 1h is alarming
        "min_invocations_24h": 200,  # Should fire ~288×/day (every 5 min)
        "note": "Writes data/report.json every 5min. Most-frequent Lambda.",
        "severity": "critical",
    },
    "lambda:justhodl-signal-logger": {
        "type": "lambda",
        "name": "justhodl-signal-logger",
        "max_error_rate": 0.20,
        "min_invocations_24h": 3,   # ~4×/day at 6h cadence
        "note": "Logs signals to DynamoDB. Heart of learning loop.",
        "severity": "critical",
    },
    "lambda:justhodl-outcome-checker": {
        "type": "lambda",
        "name": "justhodl-outcome-checker",
        "max_error_rate": 0.20,
        "min_invocations_24h": 0,   # Daily/weekly schedules; can be 0 weekends
        "note": "Scores outcomes. Fires Mon-Fri 22:30 UTC + Sun 8 UTC + 1st of month.",
        "severity": "critical",
    },
    "lambda:justhodl-calibrator": {
        "type": "lambda",
        "name": "justhodl-calibrator",
        "max_error_rate": 0.50,    # Weekly fires; one error means whole week missed
        "min_invocations_24h": 0,   # Most days zero (weekly)
        "note": "Computes per-signal weights. Sunday 9 UTC.",
        "severity": "critical",
    },
    "lambda:justhodl-intelligence": {
        "type": "lambda",
        "name": "justhodl-intelligence",
        "max_error_rate": 0.10,
        "min_invocations_24h": 4,   # Hourly weekdays — relaxed from 10 to handle
                                    # rolling-24h dips after weekend (Mon AM = ~6h
                                    # of invocations + 18h weekend gap)
        "schedule": "weekday_market_hours",   # cron(5 12-23 ? * MON-FRI *)
        "note": "Cross-system synthesis. FIXED 2026-04-25 (adapter pattern).",
        "severity": "critical",
    },
    "lambda:justhodl-crypto-intel": {
        "type": "lambda",
        "name": "justhodl-crypto-intel",
        "max_error_rate": 0.20,
        "min_invocations_24h": 80,  # Every 15 min = 96/day
        "note": "Crypto data. Some Binance modules geoblocked but core works.",
        "severity": "critical",
    },
    "lambda:justhodl-edge-engine": {
        "type": "lambda",
        "name": "justhodl-edge-engine",
        "max_error_rate": 0.20,
        "min_invocations_24h": 3,
        "note": "Edge composite + regime. Every 6h.",
        "severity": "critical",
    },
    "lambda:justhodl-insider-trades": {
        "type": "lambda",
        "name": "justhodl-insider-trades",
        "max_error_rate": 0.30,        # SEC EDGAR has occasional flakiness; 30% is forgiving
        "min_invocations_24h": 30,     # rate(30min) = 48/day, allow some skipped
        "note": "SEC EDGAR Form 4 pipeline. Cluster buys + big buys + sector heat.",
        "severity": "important",
    },

    "lambda:justhodl-repo-monitor": {
        "type": "lambda",
        "name": "justhodl-repo-monitor",
        "max_error_rate": 0.20,
        "min_invocations_24h": 6,   # Every 30min weekdays — relaxed from 10 to
                                    # handle Mon AM rolling-24h dip (Sun gap +
                                    # partial Mon = much less than weekday avg)
        "schedule": "weekday_market_hours",   # cron(0/30 13-23 ? * MON-FRI *)
        "note": "Plumbing stress. Every 30min weekdays.",
        "severity": "critical",
    },
    "lambda:justhodl-ai-chat": {
        "type": "lambda",
        "name": "justhodl-ai-chat",
        "max_error_rate": 0.05,    # User-facing — tighter
        "min_invocations_24h": 0,   # Browser-driven; can be 0
        "note": "User chat. Auth-guarded behind CF Worker.",
        "severity": "critical",
    },

    # ─── DynamoDB tables ────────────────────────────────────────────
    "ddb:justhodl-signals": {
        "type": "dynamodb",
        "table": "justhodl-signals",
        "min_items": 4_000,       # Should be growing; alarm if shrinks
        "max_growth_24h": 100,    # Typical growth ~25/run × 4 runs/day = 100
        "note": "All logged signals. Should grow ~100/day.",
        "severity": "important",
    },
    "ddb:justhodl-outcomes": {
        "type": "dynamodb",
        "table": "justhodl-outcomes",
        "min_items": 700,
        "note": "Scored outcomes. Grows after outcome-checker runs.",
        "severity": "important",
    },
    "ddb:fed-liquidity-cache": {
        "type": "dynamodb",
        "table": "fed-liquidity-cache",
        "min_items": 200_000,
        "note": "FRED data cache.",
        "severity": "nice_to_have",
    },

    # ─── SSM parameters ─────────────────────────────────────────────
    "ssm:/justhodl/calibration/weights": {
        "type": "ssm",
        "name": "/justhodl/calibration/weights",
        "fresh_max": 691_200,    # 8 days (Sunday weekly)
        "warn_max": 950_400,
        "note": "Per-signal weights. Updated by calibrator Sunday 9 UTC.",
        "severity": "important",
    },
    "ssm:/justhodl/calibration/accuracy": {
        "type": "ssm",
        "name": "/justhodl/calibration/accuracy",
        "fresh_max": 691_200,
        "warn_max": 950_400,
        "note": "Per-signal accuracy stats. Updated weekly.",
        "severity": "important",
    },

    # ─── EventBridge rules (state should be ENABLED) ────────────────
    "eb:justhodl-outcome-checker-daily": {
        "type": "eb_rule",
        "name": "justhodl-outcome-checker-daily",
        "expected_state": "ENABLED",
        "note": "Daily outcome scoring (NEW 2026-04-24).",
        "severity": "critical",
    },
    "eb:justhodl-outcome-checker-weekly": {
        "type": "eb_rule",
        "name": "justhodl-outcome-checker-weekly",
        "expected_state": "ENABLED",
        "note": "Sunday outcome scoring.",
        "severity": "critical",
    },
    "eb:justhodl-calibrator-weekly": {
        "type": "eb_rule",
        "name": "justhodl-calibrator-weekly",
        "expected_state": "ENABLED",
        "note": "Sunday 9 UTC calibration. THE event.",
        "severity": "critical",
    },

    # ─── New data sources (Tier S+A integrations) ───────────────
    "s3:data/gdelt-news.json": {
        "type": "s3_file",
        "key": "data/gdelt-news.json",
        "fresh_max": 2400,
        "warn_max": 7200,
        "expected_size": 5000,
        "note": "GDELT 2.0 financial sentiment + asset-level tone. justhodl-gdelt-sentiment every 30min.",
        "severity": "important",
    },
    "s3:data/aaii-sentiment.json": {
        "type": "s3_file",
        "key": "data/aaii-sentiment.json",
        "fresh_max": 100000,
        "warn_max": 200000,
        "expected_size": 500,
        "note": "AAII weekly retail sentiment survey. justhodl-aaii-sentiment daily check.",
        "severity": "nice_to_have",
    },
    "s3:data/dealer-survey.json": {
        "type": "s3_file",
        "key": "data/dealer-survey.json",
        "fresh_max": 700000,
        "warn_max": 1400000,
        "expected_size": 200,
        "note": "NY Fed Survey of Primary Dealers. justhodl-nyfed-dealer-survey weekly check.",
        "severity": "nice_to_have",
    },
    "s3:data/price-redundancy.json": {
        "type": "s3_file",
        "key": "data/price-redundancy.json",
        "fresh_max": 1200,
        "warn_max": 3600,
        "expected_size": 5000,
        "note": "Stooq + Yahoo fallback price feed. justhodl-price-redundancy every 15min.",
        "severity": "important",
    },
    "s3:data/onchain-ratios.json": {
        "type": "s3_file",
        "key": "data/onchain-ratios.json",
        "fresh_max": 25200,
        "warn_max": 86400,
        "expected_size": 500,
        "note": "BTC + ETH on-chain ratios — Glassnode-equivalent. justhodl-onchain-ratios every 6h.",
        "severity": "important",
    },
    "s3:data/options-gamma.json": {
        "type": "s3_file",
        "key": "data/options-gamma.json",
        "fresh_max": 2400,
        "warn_max": 86400,
        "expected_size": 300,
        "note": "SPY dealer gamma exposure (GEX). Off-hours produces ~400-byte marker; market hours produces 10-50KB chain.",
        "severity": "important",
    },
    "s3:data/oecd-cli.json": {
        "type": "s3_file",
        "key": "data/oecd-cli.json",
        "fresh_max": 700000,
        "warn_max": 1400000,
        "expected_size": 1000,
        "note": "OECD Composite Leading Indicators 38 economies. justhodl-oecd-cli weekly.",
        "severity": "important",
    },
    "s3:data/labor-leading.json": {
        "type": "s3_file",
        "key": "data/labor-leading.json",
        "fresh_max": 100000,
        "warn_max": 200000,
        "expected_size": 500,
        "note": "JOLTS + Challenger + Initial Claims. justhodl-labor-leading daily.",
        "severity": "important",
    },
    "s3:data/institutional-positions.json": {
        "type": "s3_file",
        "key": "data/institutional-positions.json",
        "fresh_max": 100000,
        "warn_max": 200000,
        "expected_size": 1000,
        "note": "SEC 13F-HR institutional positions tracker (18 funds). justhodl-sec-13f daily.",
        "severity": "important",
    },
    "s3:data/8k-filings.json": {
        "type": "s3_file",
        "key": "data/8k-filings.json",
        "fresh_max": 2400,
        "warn_max": 7200,
        "expected_size": 5000,
        "note": "SEC 8-K material event filings. justhodl-sec-8k every 30min.",
        "severity": "important",
    },
    "s3:data/10kq-filings.json": {
        "type": "s3_file",
        "key": "data/10kq-filings.json",
        "fresh_max": 16200,
        "warn_max": 36000,
        "expected_size": 5000,
        "note": "SEC 10-K + 10-Q filings (annual + quarterly). justhodl-sec-10kq every 4h.",
        "severity": "important",
    },

    # ─── Lambdas for Tier S+A integrations ──────────────────────
    "lambda:justhodl-gdelt-sentiment": {
        "type": "lambda",
        "name": "justhodl-gdelt-sentiment",
        "max_error_rate": 0.3,
        "min_invocations_24h": 30,
        "note": "GDELT 2.0 sentiment Lambda.",
        "severity": "important",
    },
    "lambda:justhodl-aaii-sentiment": {
        "type": "lambda",
        "name": "justhodl-aaii-sentiment",
        "max_error_rate": 0.5,
        "min_invocations_24h": 1,
        "note": "AAII weekly survey scraper. Most days no new data; allow tolerance.",
        "severity": "important",
    },
    "lambda:justhodl-nyfed-dealer-survey": {
        "type": "lambda",
        "name": "justhodl-nyfed-dealer-survey",
        "max_error_rate": 0.4,
        "min_invocations_24h": 0,
        "schedule": "weekly",   # rate(7 days)
        "note": "NY Fed dealer survey checker.",
        "severity": "important",
    },
    "lambda:justhodl-price-redundancy": {
        "type": "lambda",
        "name": "justhodl-price-redundancy",
        "max_error_rate": 0.3,
        "min_invocations_24h": 60,
        "note": "Stooq+Yahoo price feed redundancy.",
        "severity": "important",
    },
    "lambda:justhodl-onchain-ratios": {
        "type": "lambda",
        "name": "justhodl-onchain-ratios",
        "max_error_rate": 0.3,
        "min_invocations_24h": 3,
        "note": "BTC + ETH on-chain ratios.",
        "severity": "important",
    },
    "lambda:justhodl-options-gamma": {
        "type": "lambda",
        "name": "justhodl-options-gamma",
        "max_error_rate": 0.4,
        "min_invocations_24h": 14,
        "note": "SPY options gamma exposure (GEX).",
        "severity": "important",
    },
    "lambda:justhodl-oecd-cli": {
        "type": "lambda",
        "name": "justhodl-oecd-cli",
        "max_error_rate": 0.4,
        "min_invocations_24h": 0,
        "schedule": "weekly",   # rate(7 days)
        "note": "OECD CLI weekly fetcher.",
        "severity": "important",
    },
    "lambda:justhodl-labor-leading": {
        "type": "lambda",
        "name": "justhodl-labor-leading",
        "max_error_rate": 0.3,
        "min_invocations_24h": 1,
        "note": "JOLTS + Challenger + claims daily.",
        "severity": "important",
    },
    "lambda:justhodl-sec-13f": {
        "type": "lambda",
        "name": "justhodl-sec-13f",
        "max_error_rate": 0.4,
        "min_invocations_24h": 1,
        "note": "SEC 13F institutional position tracker.",
        "severity": "important",
    },
    "lambda:justhodl-sec-8k": {
        "type": "lambda",
        "name": "justhodl-sec-8k",
        "max_error_rate": 0.4,
        "min_invocations_24h": 30,
        "note": "SEC 8-K material events tracker.",
        "severity": "important",
    },
    "lambda:justhodl-sec-10kq": {
        "type": "lambda",
        "name": "justhodl-sec-10kq",
        "max_error_rate": 0.4,
        "min_invocations_24h": 4,
        "note": "SEC 10-K/10-Q filings tracker.",
        "severity": "important",
    },

    # ─── Phase 11A — cross-pollination alerter ──────────────
    "s3:data/redflag-alerts.json": {
        "type": "s3_file",
        "key": "data/redflag-alerts.json",
        "fresh_max": 2400,
        "warn_max": 7200,
        "expected_size": 50,
        "note": "Telegram red-flag alert log. justhodl-redflag-alerter every 30min.",
        "severity": "important",
    },
    "lambda:justhodl-redflag-alerter": {
        "type": "lambda",
        "name": "justhodl-redflag-alerter",
        "max_error_rate": 0.30,
        "min_invocations_24h": 30,
        "note": "Telegram alerter for serious 8-K events. Reads data/8k-filings.json.",
        "severity": "important",
    },

    # ─── Phase 11B — backtest harness ──────────────────────
    "s3:data/backtest-summary.json": {
        "type": "s3_file",
        "key": "data/backtest-summary.json",
        "fresh_max": 90_000,         # 25h (writer is daily)
        "warn_max": 172_800,         # 48h
        "expected_size": 200,
        "note": "Forward-return cohort tracker. justhodl-backtest-harness daily.",
        "severity": "important",
    },
    "lambda:justhodl-backtest-harness": {
        "type": "lambda",
        "name": "justhodl-backtest-harness",
        "max_error_rate": 0.30,
        "min_invocations_24h": 1,
        "note": "Daily backtest snapshot + forward-return tracker. DDB justhodl-backtest.",
        "severity": "important",
    },

    # ─── Tier 3 — liquidity / exchange flows / VIX curve ──────
    "s3:data/liquidity-flow.json": {
        "type": "s3_file",
        "key": "data/liquidity-flow.json",
        "fresh_max": 90000,
        "warn_max": 172800,
        "expected_size": 5000,
        "note": "TGA + RRP + WALCL liquidity tracker. justhodl-liquidity-flow daily.",
        "severity": "important",
    },
    "s3:data/exchange-flows.json": {
        "type": "s3_file",
        "key": "data/exchange-flows.json",
        "fresh_max": 25200,
        "warn_max": 86400,
        "expected_size": 5000,
        "note": "Net BTC/ETH exchange flows. justhodl-exchange-flows every 6h.",
        "severity": "important",
    },
    "s3:data/vix-curve.json": {
        "type": "s3_file",
        "key": "data/vix-curve.json",
        "fresh_max": 16200,
        "warn_max": 36000,
        "expected_size": 200,
        "note": "VIX term structure (9D/30D/3M/6M/VVIX). justhodl-vix-curve every 4h.",
        "severity": "important",
    },
    "lambda:justhodl-liquidity-flow": {
        "type": "lambda",
        "name": "justhodl-liquidity-flow",
        "max_error_rate": 0.3,
        "min_invocations_24h": 1,
        "note": "TGA+RRP+WALCL liquidity daily tracker.",
        "severity": "important",
    },
    "lambda:justhodl-exchange-flows": {
        "type": "lambda",
        "name": "justhodl-exchange-flows",
        "max_error_rate": 0.3,
        "min_invocations_24h": 3,
        "note": "BTC/ETH exchange flows 6h tracker.",
        "severity": "important",
    },
    "lambda:justhodl-vix-curve": {
        "type": "lambda",
        "name": "justhodl-vix-curve",
        "max_error_rate": 0.3,
        "min_invocations_24h": 5,
        "note": "VIX term structure 4h tracker.",
        "severity": "important",
    },

    # ─── 13F position tracker ──────────────────────────────
    "s3:data/13f-positions.json": {
        "type": "s3_file",
        "key": "data/13f-positions.json",
        "fresh_max": 25_200,         # 7h (writer is every 6h)
        "warn_max": 86_400,          # 24h
        "expected_size": 50_000,
        "note": "13F position deltas — NEW/ADD/TRIM/EXIT across 18 funds.",
        "severity": "important",
    },
    "lambda:justhodl-13f-positions": {
        "type": "lambda",
        "name": "justhodl-13f-positions",
        "max_error_rate": 0.30,
        "min_invocations_24h": 4,    # Every 6h
        "note": "13F position parser. Holdings + deltas vs prior quarter.",
        "severity": "important",
    },

    # ─── Time-series history snapshotter (shipped 2026-05-06, step 253) ──
    "s3:data/history-snapshotter-status.json": {
        "type": "s3_file",
        "key": "data/history-snapshotter-status.json",
        "fresh_max": 600,            # 10 min (writer is every 5 min)
        "warn_max": 1800,            # 30 min
        "expected_size": 200,        # status JSON is small
        "note": "Snapshotter heartbeat. Writes after each 5-min run with feeds_checked + n_written.",
        "severity": "important",
    },
    "lambda:justhodl-history-snapshotter": {
        "type": "lambda",
        "name": "justhodl-history-snapshotter",
        "max_error_rate": 0.10,
        "min_invocations_24h": 250,  # 288 = every 5 min × 24h, allow some misses
        "note": "Time-series snapshotter — captures 30 live feeds to DDB justhodl-history with hash-dedup. Foundation for walk-forward backtest.",
        "severity": "important",
    },
    "ddb:justhodl-history": {
        "type": "dynamodb",
        "table": "justhodl-history",
        "min_items": 20,             # Bootstrap captured 26; expect growth
        "note": "Time-series feed snapshots. PK=feed#<key>, SK=ISO8601 timestamp. 365d TTL.",
        "severity": "important",
    },
    "eb:justhodl-history-snapshotter-5m": {
        "type": "eb_rule",
        "name": "justhodl-history-snapshotter-5m",
        "expected_state": "ENABLED",
        "note": "EventBridge rate(5 minutes) — drives history snapshotter.",
        "severity": "important",
    },

    # ─── Backtest engine output (v2.0.1 + v2.1 honest backtest) ──────
    "s3:backtest/results.json": {
        "type": "s3_file",
        "key": "backtest/results.json",
        "fresh_max": 25_200,         # 7h (writer is every 6h)
        "warn_max": 43_200,          # 12h
        "expected_size": 15_000,     # Currently ~26KB; alerts on shrinkage
        "note": "Backtest engine v2.1 full results — v1.1/v1.2/v2.0.1/v2.1 + by_signal + nav curves.",
        "severity": "critical",
    },
    "s3:backtest/summary.json": {
        "type": "s3_file",
        "key": "backtest/summary.json",
        "fresh_max": 25_200,         # 7h
        "warn_max": 43_200,          # 12h
        "expected_size": 3_000,      # ~5KB normally
        "note": "Backtest engine v2.1 slim summary — KPIs + honest_summary + walkforward_summary for fast page loads.",
        "severity": "critical",
    },
    "lambda:justhodl-backtest-engine": {
        "type": "lambda",
        "name": "justhodl-backtest-engine",
        "max_error_rate": 0.20,
        "min_invocations_24h": 3,    # Every 6h
        "note": "Backtest engine v2.1 — calibrated alpha replay + walk-forward. Reads SSM weights + historical snapshots + DDB outcomes.",
        "severity": "critical",
    },
    "lambda:justhodl-calibration-snapshotter": {
        "type": "lambda",
        "name": "justhodl-calibration-snapshotter",
        "max_error_rate": 0.20,
        "min_invocations_24h": None,  # Weekly schedule
        "schedule": "weekly_sunday",
        "note": "Calibration weight snapshotter — Sundays 12:00 UTC. Foundation for v2.1 walk-forward backtest.",
        "severity": "important",
    },
    "s3:calibration/history-index.json": {
        "type": "s3_file",
        "key": "calibration/history-index.json",
        "fresh_max": 7 * 86400 + 3600,    # 7d 1h (weekly write Sundays)
        "warn_max": 8 * 86400,             # 8d
        "expected_size": 100,
        "schedule": "weekly_sunday",
        "note": "Manifest of all calibration snapshots — needed for v2.1 walk-forward.",
        "severity": "important",
    },
    "s3:calibration/latest.json": {
        "type": "s3_file",
        "key": "calibration/latest.json",
        "fresh_max": 7 * 86400 + 3600,    # 7d 1h
        "warn_max": 8 * 86400,
        "expected_size": 1_000,
        "schedule": "weekly_sunday",
        "note": "Pointer to most recent calibration snapshot.",
        "severity": "important",
    },

    # ─── Forensic accounting screen (shipped 2026-05-06, step 255) ───
    "s3:data/forensic-screen.json": {
        "type": "s3_file",
        "key": "data/forensic-screen.json",
        "fresh_max": 50_400,         # 14h (writer is every 12h)
        "warn_max": 86_400,          # 24h
        "expected_size": 50_000,     # SP500 with 4 factors per stock
        "note": "Forensic accounting screen output — Beneish M-Score, Sloan accruals, WC divergence, goodwill bloat for top 200 SP500.",
        "severity": "important",
    },
    "lambda:justhodl-forensic-screen": {
        "type": "lambda",
        "name": "justhodl-forensic-screen",
        "max_error_rate": 0.20,
        "min_invocations_24h": 1,    # Every 12h = 2/day, allow 1 miss
        "note": "Forensic accounting screen — Beneish + Sloan + WC + goodwill. Reads SP500 universe from screener, 12h schedule.",
        "severity": "important",
    },
    "eb:justhodl-forensic-screen-12h": {
        "type": "eb_rule",
        "name": "justhodl-forensic-screen-12h",
        "expected_state": "ENABLED",
        "note": "EventBridge rate(12 hours) — drives forensic screen.",
        "severity": "important",
    },

    # ─── History audit index (Phase 1 of /audit.html) ────────────────
    "s3:data/history-index.json": {
        "type": "s3_file",
        "key": "data/history-index.json",
        "fresh_max": 7200,           # 2h (writer fires hourly at top of hour)
        "warn_max": 14_400,          # 4h
        "expected_size": 500,
        "note": "Audit-trail index — feed-level snapshot counts + recent timestamps from DDB. Drives /audit.html.",
        "severity": "nice_to_have",
    },

    # ─── Alert router (multi-channel: Telegram + webhooks) ─────────────
    "s3:data/alert-history.json": {
        "type": "s3_file",
        "key": "data/alert-history.json",
        "fresh_max": 7 * 86400,        # 7d — only updates when an alert fires
        "warn_max": 14 * 86400,
        "expected_size": 100,
        "note": "Alert history (last 100 fired). Updated by justhodl-alert-router only on fire — slow-moving feed is normal.",
        "severity": "nice_to_have",
    },
    "lambda:justhodl-alert-router": {
        "type": "lambda",
        "name": "justhodl-alert-router",
        "max_error_rate": 0.10,
        "min_invocations_24h": 40,     # Every 30min = 48/day, allow 8 misses
        "note": "Multi-channel alert router — Telegram + Slack/Discord webhooks. Reads /justhodl/alerts/webhook_urls SSM. 12 alert sources.",
        "severity": "critical",
    },
}


# ═══════════════════════════════════════════════════════════════════
#  Status helpers — used by the monitor Lambda
# ═══════════════════════════════════════════════════════════════════

def status_for_age(age_sec, fresh_max, warn_max, schedule=None):
    """Return 'green' | 'yellow' | 'red' | 'unknown'.

    If schedule is provided, applies cadence-aware grace:
      schedule="weekday_market_hours" — fresh_max only enforced
        Mon-Fri 13:00-23:00 UTC. Outside window, status_for_age uses
        max(fresh_max, hours_since_last_market_close).
      schedule="weekday" — Mon-Fri any hour. On weekends, allows
        up to 60h staleness (Fri 17:00 UTC + 60h covers Sat+Sun).
      schedule="weekly" — anything < 8 days fresh; 8-14 days yellow;
        > 14 days red.
    None or unset → original behavior.
    """
    from datetime import datetime, timezone, timedelta

    if fresh_max is None and schedule is None:
        return "unknown"
    if age_sec is None:
        return "red"

    # ── Weekly cadence ─────────────────────────────────────────────
    if schedule == "weekly":
        if age_sec <= 8 * 86_400:        # 8 days
            return "green"
        if age_sec <= 14 * 86_400:       # 14 days
            return "yellow"
        return "red"

    # ── Schedule-aware grace for weekday-only Lambdas ──────────────
    now_utc = datetime.now(timezone.utc)
    weekday = now_utc.weekday()    # 0=Mon, 5=Sat, 6=Sun
    hour_utc = now_utc.hour

    in_market_hours = (weekday < 5 and 13 <= hour_utc < 23)
    in_weekday_any = (weekday < 5)

    if schedule == "weekday_market_hours" and not in_market_hours:
        # Outside the schedule's active window — allow more staleness
        # Compute hours since last Friday 23:00 UTC (latest possible run)
        # Simpler: just allow up to 4 days staleness on weekend mornings
        # which covers Fri 23:00 UTC → Mon 13:00 UTC = 62h
        weekend_grace_sec = 75 * 3600   # 75h covers Fri close → Mon open + buffer
        if age_sec <= weekend_grace_sec:
            return "green"
        if age_sec <= weekend_grace_sec * 1.3:
            return "yellow"
        return "red"

    if schedule == "weekday" and not in_weekday_any:
        weekend_grace_sec = 65 * 3600   # 65h covers Sat 00:00 → Mon 17:00 UTC
        if age_sec <= weekend_grace_sec:
            return "green"
        if age_sec <= weekend_grace_sec * 1.3:
            return "yellow"
        return "red"

    # ── Default behavior (within window or no schedule hint) ──────
    if fresh_max is None:
        return "unknown"
    if age_sec <= fresh_max:
        return "green"
    if warn_max is None or age_sec <= warn_max:
        return "yellow"
    return "red"


def status_for_size(actual_bytes, expected_min):
    """File-size status — alerts on shrinkage."""
    if expected_min is None or actual_bytes is None:
        return "unknown"
    if actual_bytes >= expected_min:
        return "green"
    if actual_bytes >= expected_min * 0.5:
        return "yellow"
    return "red"


def severity_rank(s):
    """For sorting — critical first."""
    return {"critical": 0, "important": 1, "nice_to_have": 2}.get(s, 3)
