"""
justhodl-signal-halflife — Exponential Idea #2

Computes per-engine signal decay curves from the learning loop:
  - Scans justhodl-outcomes DDB
  - Groups by signal_type + window_key (day_1/3/7/14/30/60/180/360)
  - Builds decay curve: hit_rate + avg_return_pct at each horizon
  - Identifies half-life (horizon where edge drops to 50% of peak)
  - Classifies edge_status: FRESH (peak in last 30d), DECAYING, DECAYED, STRENGTHENING
  - Flags engines getting arbed away (decay_trend_90d < -0.1)

Output: data/signal-halflife.json
Schedule: weekly Monday 06 UTC (after weekend outcome-checker)

Telegram digest of biggest changes (NEW DECAYED, NEW STRENGTHENING) per run.
"""
import json
import os
import logging
import boto3
import urllib.request
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from decimal import Decimal

# Structured logging (institutional grade)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/signal-halflife.json"
HIST_KEY = "data/history/signal-halflife-history.json"
OUTCOMES_TABLE = "justhodl-outcomes"
SIGNALS_TABLE = "justhodl-signals"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Horizon mappings — window_key → days (covers all observed forms)
WINDOW_MAP = {
    "day_1": 1, "day_3": 3, "day_5": 5, "day_7": 7,
    "day_14": 14, "day_30": 30, "day_60": 60,
    "day_90": 90, "day_180": 180, "day_360": 360,
}

# Minimum sample size per (engine × horizon) to compute statistics
MIN_N = 5

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)


def deci(v):
    """DynamoDB Decimal → Python float."""
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, dict):
        if "N" in v:
            return float(v["N"])
        if "S" in v:
            return v["S"]
        if "BOOL" in v:
            return v["BOOL"]
    return v


def unpack(item):
    """Flatten DynamoDB item to plain dict."""
    out = {}
    for k, v in item.items():
        if isinstance(v, dict) and len(v) == 1:
            out[k] = deci(v)
        else:
            out[k] = v
    return out


def scan_all_outcomes():
    """Paginated full scan of outcomes table. Returns list of dicts."""
    items = []
    last_key = None
    while True:
        kwargs = {"TableName": OUTCOMES_TABLE, "Limit": 1000}
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        r = ddb.scan(**kwargs)
        for raw in r.get("Items", []):
            try:
                items.append(unpack(raw))
            except Exception as e:
                logger.warning(f"unpack_fail: {e}")
        last_key = r.get("LastEvaluatedKey")
        if not last_key:
            break
        if len(items) > 100_000:
            logger.warning("hit_100k_cap")
            break
    return items


def parse_logged_at(s):
    """Outcome's `logged_at` can be ISO or epoch seconds."""
    if not s:
        return None
    s = str(s)
    if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
        try:
            return datetime.fromtimestamp(int(s), tz=timezone.utc)
        except Exception:
            return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def extract_return_pct(item):
    """Get outcome return — DDB nests it under outcome map."""
    outcome = item.get("outcome")
    if isinstance(outcome, dict):
        rp = outcome.get("return_pct")
        if isinstance(rp, dict):
            return deci(rp)
        if rp is not None:
            return float(rp)
    return None


def compute_engine_stats(outcomes):
    """Group by (signal_type, window_key), compute hit_rate + avg_return + n."""
    grouped = defaultdict(lambda: defaultdict(list))  # type -> wkey -> [items]

    now = datetime.now(timezone.utc)
    cutoff_90d = now - timedelta(days=90)

    for o in outcomes:
        sig_type = o.get("signal_type")
        wkey = o.get("window_key")
        if not sig_type or not wkey:
            continue
        grouped[sig_type][wkey].append(o)

    engines = {}
    for engine, by_window in grouped.items():
        # Build decay curve at each defined horizon
        curve = []
        for wkey, items in by_window.items():
            n = len(items)
            if n < MIN_N:
                continue
            correct = sum(1 for i in items if i.get("correct") is True)
            hit_rate = correct / n
            returns = [extract_return_pct(i) for i in items]
            returns = [r for r in returns if r is not None]
            avg_return = sum(returns) / len(returns) if returns else None
            horizon = WINDOW_MAP.get(wkey)
            if horizon is None:
                continue
            curve.append({
                "horizon_days": horizon,
                "window_key": wkey,
                "n": n,
                "hit_rate": round(hit_rate, 4),
                "avg_return_pct": round(avg_return, 4) if avg_return is not None else None,
            })

        if not curve:
            continue
        curve.sort(key=lambda x: x["horizon_days"])

        # Peak hit rate + horizon
        peak = max(curve, key=lambda x: x["hit_rate"])
        peak_hr = peak["hit_rate"]
        # Edge above random = hit_rate - 0.5
        peak_edge = peak_hr - 0.5

        # Half-life: smallest horizon where edge drops below 50% of peak edge
        half_life = None
        if peak_edge > 0:
            for c in curve:
                if c["horizon_days"] <= peak["horizon_days"]:
                    continue
                edge = c["hit_rate"] - 0.5
                if edge < peak_edge * 0.5:
                    half_life = c["horizon_days"]
                    break

        # Decay trend: compute hit-rate over last 90d vs over all-time, at peak horizon
        peak_wkey = peak["window_key"]
        recent = [i for i in by_window[peak_wkey]
                  if parse_logged_at(i.get("logged_at")) and parse_logged_at(i.get("logged_at")) >= cutoff_90d]
        older = [i for i in by_window[peak_wkey]
                 if parse_logged_at(i.get("logged_at")) and parse_logged_at(i.get("logged_at")) < cutoff_90d]
        decay_trend_90d = None
        if len(recent) >= MIN_N and len(older) >= MIN_N:
            recent_hr = sum(1 for i in recent if i.get("correct") is True) / len(recent)
            older_hr = sum(1 for i in older if i.get("correct") is True) / len(older)
            decay_trend_90d = round(recent_hr - older_hr, 4)

        # Edge status
        edge_status = "FRESH"
        if peak_edge <= 0:
            edge_status = "NO_EDGE"
        elif decay_trend_90d is not None:
            if decay_trend_90d < -0.10:
                edge_status = "DECAYING"
            elif decay_trend_90d > 0.10:
                edge_status = "STRENGTHENING"
        if peak_edge < 0.02:
            edge_status = "DECAYED" if edge_status == "DECAYING" else "MARGINAL"

        engines[engine] = {
            "n_signals": sum(c["n"] for c in curve),
            "n_windows": len(curve),
            "decay_curve": curve,
            "peak_hit_rate": peak_hr,
            "peak_edge": round(peak_edge, 4),
            "peak_horizon_days": peak["horizon_days"],
            "half_life_days": half_life,
            "decay_trend_90d": decay_trend_90d,
            "edge_status": edge_status,
            "recommended_allocator_weight": round(max(0.0, min(1.0, peak_edge * 5)), 3),
        }

    return engines


def compute_rankings(engines):
    """Rank engines by various criteria."""
    ranked = list(engines.items())

    # Strongest peak edge
    strongest = sorted(ranked, key=lambda x: -x[1]["peak_edge"])[:10]
    # Fastest decay (shortest half-life relative to peak horizon)
    decaying = [
        (n, e) for n, e in ranked
        if e["edge_status"] in ("DECAYING", "DECAYED")
    ]
    decaying.sort(key=lambda x: (x[1]["decay_trend_90d"] or 0))
    # Strengthening
    strengthening = [
        (n, e) for n, e in ranked
        if e["edge_status"] == "STRENGTHENING"
    ]
    strengthening.sort(key=lambda x: -(x[1]["decay_trend_90d"] or 0))
    # Longest persistence (where edge holds at long horizons)
    persistent = []
    for n, e in ranked:
        if not e["decay_curve"]:
            continue
        # Find longest horizon where edge > 0
        edges_at_long_horizon = [c for c in e["decay_curve"] if c["horizon_days"] >= 30 and c["hit_rate"] > 0.55]
        if edges_at_long_horizon:
            max_h = max(c["horizon_days"] for c in edges_at_long_horizon)
            persistent.append((n, e, max_h))
    persistent.sort(key=lambda x: -x[2])

    return {
        "strongest_edge": [{"engine": n, **e} for n, e in strongest],
        "decaying_or_dead": [{"engine": n, **e} for n, e in decaying[:10]],
        "strengthening": [{"engine": n, **e} for n, e in strengthening[:10]],
        "longest_persistence": [{"engine": n, "max_persistent_horizon_days": h, **e} for n, e, h in persistent[:10]],
    }


def telegram_digest(rankings, engines):
    """Build markdown digest for Telegram."""
    lines = ["📊 *Signal Half-Life Update*", ""]
    strongest = rankings["strongest_edge"][:5]
    if strongest:
        lines.append("*🔥 Strongest edge right now:*")
        for r in strongest:
            edge = r["peak_edge"]
            h = r["peak_horizon_days"]
            hl = r["half_life_days"]
            hl_str = f"{hl}d" if hl else "long"
            lines.append(f"  `{r['engine']}` edge={edge:+.1%} peak@{h}d half-life={hl_str}")
        lines.append("")

    decaying = rankings["decaying_or_dead"][:5]
    if decaying:
        lines.append("*⚠️ Decaying / arbed away:*")
        for r in decaying:
            trend = r.get("decay_trend_90d", 0) or 0
            lines.append(f"  `{r['engine']}` Δ90d={trend:+.1%} status={r['edge_status']}")
        lines.append("")

    strengthening = rankings["strengthening"][:5]
    if strengthening:
        lines.append("*🚀 Strengthening (edge growing):*")
        for r in strengthening:
            trend = r.get("decay_trend_90d", 0) or 0
            lines.append(f"  `{r['engine']}` Δ90d={trend:+.1%}")
        lines.append("")

    lines.append(f"_Total: {len(engines)} engines analysed · "
                 f"[signal-halflife.html](https://justhodl.ai/signal-halflife.html)_")
    return "\n".join(lines)


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }).encode()
        req = urllib.request.Request(url, data=data,
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=15)
    except Exception as e:
        logger.error(f"telegram_fail: {e}")


def update_history(payload):
    """Append snapshot to history for trend over time."""
    try:
        try:
            old = json.loads(s3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
        except s3.exceptions.NoSuchKey:
            old = {"snapshots": []}
        snap = {
            "ts": payload["computed_at"],
            "n_engines": payload["summary"]["n_engines"],
            "n_fresh": payload["summary"]["n_fresh"],
            "n_decaying": payload["summary"]["n_decaying_or_dead"],
            "n_strengthening": payload["summary"]["n_strengthening"],
        }
        old["snapshots"] = (old.get("snapshots") or []) + [snap]
        old["snapshots"] = old["snapshots"][-104:]  # 2 years of weekly snapshots
        s3.put_object(
            Bucket=BUCKET, Key=HIST_KEY,
            Body=json.dumps(old, indent=2).encode(),
            ContentType="application/json",
            CacheControl="max-age=600",
        )
    except Exception as e:
        logger.error(f"history_write_fail: {e}")


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)
    logger.info(f"signal-halflife starting at {started.isoformat()}")

    # 1. Scan all outcomes
    outcomes = scan_all_outcomes()
    logger.info(f"scanned {len(outcomes)} outcomes")
    if not outcomes:
        return {"statusCode": 500, "body": json.dumps({"error": "no outcomes"})}

    # 2. Compute per-engine stats
    engines = compute_engine_stats(outcomes)
    logger.info(f"computed stats for {len(engines)} engines")

    # 3. Rankings
    rankings = compute_rankings(engines)

    # 4. Build payload
    payload = {
        "computed_at": started.isoformat(),
        "elapsed_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 2),
        "n_outcomes_scanned": len(outcomes),
        "summary": {
            "n_engines": len(engines),
            "n_fresh": sum(1 for e in engines.values() if e["edge_status"] == "FRESH"),
            "n_decaying_or_dead": sum(1 for e in engines.values() if e["edge_status"] in ("DECAYING", "DECAYED")),
            "n_strengthening": sum(1 for e in engines.values() if e["edge_status"] == "STRENGTHENING"),
            "n_marginal": sum(1 for e in engines.values() if e["edge_status"] in ("MARGINAL", "NO_EDGE")),
        },
        "engines": engines,
        "rankings": rankings,
        "methodology": {
            "half_life_def": "horizon at which edge (hit_rate - 0.5) drops below 50% of peak edge",
            "min_sample_size": MIN_N,
            "decay_trend_window": "90 days",
            "edge_status_thresholds": {
                "STRENGTHENING": "decay_trend_90d > +0.10",
                "DECAYING": "decay_trend_90d < -0.10",
                "DECAYED": "DECAYING + peak_edge < 0.02",
                "MARGINAL": "peak_edge < 0.02",
            },
        },
    }

    # 5. Write outputs
    s3.put_object(
        Bucket=BUCKET, Key=OUT_KEY,
        Body=json.dumps(payload, default=str, indent=2).encode(),
        ContentType="application/json",
        CacheControl="max-age=3600, public",
    )
    update_history(payload)
    logger.info(f"wrote {OUT_KEY}")

    # 6. Telegram digest
    try:
        send_telegram(telegram_digest(rankings, engines))
    except Exception as e:
        logger.error(f"telegram_digest_fail: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "n_engines": len(engines),
            "n_outcomes": len(outcomes),
            "summary": payload["summary"],
            "elapsed": payload["elapsed_seconds"],
        }),
    }
