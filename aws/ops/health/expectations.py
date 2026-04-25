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
        "expected_size": 10_000,
        "note": "Composite ML risk score, regime. edge-engine every 6h.",
        "severity": "critical",
    },
    "s3:repo-data.json": {
        "type": "s3_file",
        "key": "repo-data.json",
        "fresh_max": 3600,       # 1h (writer is every 30 min weekdays)
        "warn_max": 14_400,      # 4h
        "expected_size": 5_000,
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
        "min_invocations_24h": 10,  # Hourly weekdays
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
    "lambda:justhodl-repo-monitor": {
        "type": "lambda",
        "name": "justhodl-repo-monitor",
        "max_error_rate": 0.20,
        "min_invocations_24h": 10,  # Every 30 min weekdays
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
}


# ═══════════════════════════════════════════════════════════════════
#  Status helpers — used by the monitor Lambda
# ═══════════════════════════════════════════════════════════════════

def status_for_age(age_sec, fresh_max, warn_max):
    """Return 'green' | 'yellow' | 'red' | 'unknown'."""
    if fresh_max is None:
        return "unknown"  # No expectation set
    if age_sec is None:
        return "red"
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
