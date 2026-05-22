"""
justhodl-behavior-mirror — Exponential Idea #4

Compares system signals (justhodl-signals + justhodl-outcomes) to actual
portfolio decisions (justhodl-portfolio) to discover:

  - Khalid's PERSONAL EDGE — engines where Khalid outperforms the system
    by ignoring its signals
  - Khalid's BLIND SPOTS — high-edge engines where Khalid keeps not acting
  - Sizing leaks — patterns of sizing mismatched with system conviction
  - Action latency — how fast Khalid acts after signal fires
  - Time-of-day patterns — does Khalid act differently morning vs afternoon

Output: data/behavior-mirror.json + Telegram weekly digest

Schedule: weekly Sunday 09 UTC (after calibrator at 09 ET = 13 UTC)

v1 = passive analyzer (no direct Telegram reply capture). Infers actions from
position open/close timestamps vs signal fire timestamps.

v2 (future) = direct Telegram reply capture via webhook handler.
"""
import json, os, logging, urllib.request
import boto3
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/behavior-mirror.json"
HIST_KEY = "data/history/behavior-mirror-history.json"

OUTCOMES_TABLE = "justhodl-outcomes"
PORTFOLIO_TABLE = "justhodl-portfolio"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Window for matching signal → action (how long after signal can a position open
# still be considered "acting on" that signal)
SIGNAL_TO_ACTION_DAYS = 5

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)


def deci(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, dict):
        if "N" in v: return float(v["N"])
        if "S" in v: return v["S"]
        if "BOOL" in v: return v["BOOL"]
    return v


def unpack(item):
    out = {}
    for k, v in item.items():
        if isinstance(v, dict) and len(v) == 1:
            out[k] = deci(v)
        else:
            out[k] = v
    return out


def scan_table(table_name, limit=20000):
    items = []
    last_key = None
    while True:
        kwargs = {"TableName": table_name, "Limit": 1000}
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        try:
            r = ddb.scan(**kwargs)
        except ddb.exceptions.ResourceNotFoundException:
            return []
        for raw in r.get("Items", []):
            try:
                items.append(unpack(raw))
            except Exception:
                pass
        last_key = r.get("LastEvaluatedKey")
        if not last_key or len(items) > limit:
            break
    return items


def parse_iso(s):
    if not s:
        return None
    s = str(s)
    if s.isdigit():
        try: return datetime.fromtimestamp(int(s), tz=timezone.utc)
        except Exception: return None
    try: return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception: return None


def build_position_timeline(positions):
    """Return list of {symbol, opened_at, closed_at, action_type}."""
    timeline = []
    for p in positions:
        sym = p.get("symbol") or p.get("ticker")
        if not sym:
            continue
        opened = parse_iso(p.get("opened_at") or p.get("created_at") or p.get("entry_date"))
        closed = parse_iso(p.get("closed_at") or p.get("exit_date"))
        size = p.get("size") or p.get("position_size") or p.get("notional")
        if opened:
            timeline.append({
                "symbol": sym.upper(),
                "opened_at": opened,
                "closed_at": closed,
                "size": size,
                "status": "CLOSED" if closed else "OPEN",
            })
    return timeline


def index_signals_by_symbol(outcomes):
    """Group outcomes by symbol → list of {ts, engine, predicted_dir, correct, return_pct}."""
    by_symbol = defaultdict(list)
    for o in outcomes:
        sym = o.get("symbol") or o.get("ticker")
        if not sym:
            # Some outcomes are macro (no symbol)
            continue
        ts = parse_iso(o.get("logged_at"))
        if not ts:
            continue
        by_symbol[sym.upper()].append({
            "ts": ts,
            "engine": o.get("signal_type") or o.get("source", "unknown"),
            "predicted_dir": o.get("predicted_dir"),
            "correct": o.get("correct"),
            "return_pct": o.get("outcome", {}).get("return_pct") if isinstance(o.get("outcome"), dict) else None,
            "window_key": o.get("window_key"),
        })
    return by_symbol


def analyze_per_engine(signals_by_symbol, positions):
    """For each engine, compute action_rate, outperformance, etc."""
    # Build positions by symbol
    pos_by_symbol = defaultdict(list)
    for p in positions:
        pos_by_symbol[p["symbol"]].append(p)

    # For each signal, did Khalid open a position within SIGNAL_TO_ACTION_DAYS?
    engine_stats = defaultdict(lambda: {
        "n_signals": 0,
        "n_acted_on": 0,
        "n_correct_signals_acted": 0,
        "n_correct_signals_skipped": 0,
        "n_wrong_signals_acted": 0,
        "n_wrong_signals_skipped": 0,
        "action_returns": [],   # Khalid's realized return when acting
        "skipped_returns": [],  # System's reported return when Khalid skipped
        "action_latency_days": [],
        "tickers_acted": set(),
        "tickers_skipped_high_edge": set(),
    })

    cutoff_days = SIGNAL_TO_ACTION_DAYS

    for sym, sigs in signals_by_symbol.items():
        sym_positions = pos_by_symbol.get(sym, [])
        for sig in sigs:
            engine = sig["engine"]
            s = engine_stats[engine]
            s["n_signals"] += 1
            # Did Khalid act within window?
            window_end = sig["ts"] + timedelta(days=cutoff_days)
            matched = [p for p in sym_positions
                       if p["opened_at"] >= sig["ts"] and p["opened_at"] <= window_end]
            acted = bool(matched)
            if acted:
                s["n_acted_on"] += 1
                if matched:
                    delta_days = (matched[0]["opened_at"] - sig["ts"]).total_seconds() / 86400
                    s["action_latency_days"].append(delta_days)
                s["tickers_acted"].add(sym)
                if sig["correct"]:
                    s["n_correct_signals_acted"] += 1
                else:
                    s["n_wrong_signals_acted"] += 1
                if sig["return_pct"] is not None:
                    s["action_returns"].append(sig["return_pct"])
            else:
                if sig["correct"]:
                    s["n_correct_signals_skipped"] += 1
                    s["tickers_skipped_high_edge"].add(sym)
                else:
                    s["n_wrong_signals_skipped"] += 1
                if sig["return_pct"] is not None:
                    s["skipped_returns"].append(sig["return_pct"])

    # Compute final metrics
    output = {}
    for engine, s in engine_stats.items():
        if s["n_signals"] < 10:
            continue  # Insufficient data
        action_rate = s["n_acted_on"] / s["n_signals"]
        # System's accuracy on this engine
        n_correct = s["n_correct_signals_acted"] + s["n_correct_signals_skipped"]
        system_accuracy = n_correct / s["n_signals"]
        # Khalid's accuracy on signals he acted on
        khalid_accuracy = (s["n_correct_signals_acted"] /
                           s["n_acted_on"]) if s["n_acted_on"] > 0 else None
        # Avg return when acting vs skipping
        avg_action_return = (sum(s["action_returns"]) / len(s["action_returns"])
                             if s["action_returns"] else None)
        avg_skipped_return = (sum(s["skipped_returns"]) / len(s["skipped_returns"])
                              if s["skipped_returns"] else None)
        # Personal edge = Khalid accuracy - system accuracy on this engine
        # Positive = Khalid picks better signals; Negative = Khalid picks worse
        personal_edge = (khalid_accuracy - system_accuracy) if khalid_accuracy is not None else None
        avg_latency = (sum(s["action_latency_days"]) / len(s["action_latency_days"])
                       if s["action_latency_days"] else None)
        output[engine] = {
            "n_signals": s["n_signals"],
            "n_acted_on": s["n_acted_on"],
            "action_rate": round(action_rate, 4),
            "system_accuracy": round(system_accuracy, 4),
            "khalid_accuracy_on_acted": round(khalid_accuracy, 4) if khalid_accuracy is not None else None,
            "personal_edge": round(personal_edge, 4) if personal_edge is not None else None,
            "avg_action_return_pct": round(avg_action_return, 4) if avg_action_return is not None else None,
            "avg_skipped_return_pct": round(avg_skipped_return, 4) if avg_skipped_return is not None else None,
            "avg_action_latency_days": round(avg_latency, 2) if avg_latency is not None else None,
            "n_unique_tickers_acted": len(s["tickers_acted"]),
            "n_high_edge_skips": s["n_correct_signals_skipped"],
            "examples_of_high_edge_skips": list(s["tickers_skipped_high_edge"])[:5],
        }
    return output


def compute_insights(engine_analysis):
    """Surface key behavioral patterns."""
    engines = list(engine_analysis.items())

    # Khalid's strongest personal edge
    by_personal_edge = [(n, e) for n, e in engines if e.get("personal_edge") is not None]
    by_personal_edge.sort(key=lambda x: -(x[1]["personal_edge"] or 0))
    personal_edge_strongest = [
        {"engine": n, "personal_edge_pct": e["personal_edge"], "n_acted": e["n_acted_on"]}
        for n, e in by_personal_edge[:5]
    ]

    # Blind spots: engines with high system accuracy but low Khalid action rate
    blind_spots = []
    for n, e in engines:
        if e["system_accuracy"] > 0.55 and e["action_rate"] < 0.20 and e["n_high_edge_skips"] > 5:
            blind_spots.append({
                "engine": n,
                "system_accuracy_pct": e["system_accuracy"],
                "action_rate_pct": e["action_rate"],
                "n_high_edge_skips": e["n_high_edge_skips"],
                "missed_avg_return_pct": e.get("avg_skipped_return_pct"),
                "example_tickers": e.get("examples_of_high_edge_skips", []),
            })
    blind_spots.sort(key=lambda x: -x["n_high_edge_skips"])

    # Sizing leak — engines with low system accuracy but high action rate
    overrated = []
    for n, e in engines:
        if e["system_accuracy"] < 0.45 and e["action_rate"] > 0.4 and e["n_acted_on"] > 5:
            overrated.append({
                "engine": n,
                "system_accuracy_pct": e["system_accuracy"],
                "action_rate_pct": e["action_rate"],
                "n_acted": e["n_acted_on"],
            })
    overrated.sort(key=lambda x: -x["action_rate_pct"])

    # Fast-action engines
    by_latency = [(n, e) for n, e in engines if e.get("avg_action_latency_days") is not None]
    by_latency.sort(key=lambda x: x[1]["avg_action_latency_days"])
    fastest = [
        {"engine": n, "avg_latency_days": e["avg_action_latency_days"], "n_acted": e["n_acted_on"]}
        for n, e in by_latency[:5]
    ]

    return {
        "personal_edge_strongest": personal_edge_strongest,
        "blind_spots": blind_spots[:8],
        "potentially_overrated": overrated[:5],
        "fastest_action": fastest,
    }


def build_telegram_digest(engine_analysis, insights, n_outcomes, n_positions):
    lines = ["🪞 *Behavior Mirror — Weekly Digest*", ""]
    lines.append(f"_Analysed {n_outcomes:,} outcomes vs {n_positions} portfolio positions_")
    lines.append("")

    bs = insights.get("blind_spots", [])
    if bs:
        lines.append("*🎯 Blind spots (high-edge engines you skip):*")
        for b in bs[:3]:
            tickers = ", ".join(b["example_tickers"][:3])
            lines.append(
                f"  `{b['engine']}` accuracy={b['system_accuracy_pct']:.0%} "
                f"action={b['action_rate_pct']:.0%} skipped={b['n_high_edge_skips']}")
            if tickers:
                lines.append(f"     missed: {tickers}")
        lines.append("")

    pe = insights.get("personal_edge_strongest", [])
    if pe and pe[0]["personal_edge_pct"] is not None and pe[0]["personal_edge_pct"] > 0.05:
        lines.append("*💪 Where you outperform the system:*")
        for r in pe[:3]:
            if r["personal_edge_pct"] is None: continue
            lines.append(f"  `{r['engine']}`: +{r['personal_edge_pct']:.1%} vs system "
                         f"on {r['n_acted']} acted-on signals")
        lines.append("")

    ovr = insights.get("potentially_overrated", [])
    if ovr:
        lines.append("*⚠️ Engines you may be over-acting on:*")
        for r in ovr[:3]:
            lines.append(f"  `{r['engine']}` accuracy={r['system_accuracy_pct']:.0%} "
                         f"but you acted on {r['action_rate_pct']:.0%} of signals")
        lines.append("")

    lines.append("[behavior-mirror.html](https://justhodl.ai/behavior-mirror.html)")
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
    try:
        try:
            old = json.loads(s3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
        except s3.exceptions.NoSuchKey:
            old = {"snapshots": []}
        snap = {
            "ts": payload["computed_at"],
            "n_engines": len(payload["engine_analysis"]),
            "n_blind_spots": len(payload["insights"]["blind_spots"]),
            "top_personal_edge": payload["insights"]["personal_edge_strongest"][:3],
        }
        old["snapshots"] = (old.get("snapshots") or []) + [snap]
        old["snapshots"] = old["snapshots"][-52:]  # 1 year of weekly snapshots
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                      Body=json.dumps(old, indent=2).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=3600")
    except Exception as e:
        logger.error(f"history_write_fail: {e}")


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)
    logger.info("behavior-mirror starting")

    # 1. Scan outcomes
    outcomes = scan_table(OUTCOMES_TABLE)
    logger.info(f"outcomes: {len(outcomes)}")

    # 2. Scan portfolio
    positions = scan_table(PORTFOLIO_TABLE)
    timeline = build_position_timeline(positions)
    logger.info(f"positions: {len(positions)} ({len(timeline)} with valid open_at)")

    # 3. Index signals by symbol
    signals_by_symbol = index_signals_by_symbol(outcomes)
    logger.info(f"unique symbols with signals: {len(signals_by_symbol)}")

    # 4. Compute per-engine analysis
    engine_analysis = analyze_per_engine(signals_by_symbol, timeline)
    logger.info(f"engines analysed: {len(engine_analysis)}")

    # 5. Surface insights
    insights = compute_insights(engine_analysis)

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    payload = {
        "computed_at": started.isoformat(),
        "elapsed_seconds": round(elapsed, 2),
        "n_outcomes_scanned": len(outcomes),
        "n_positions_scanned": len(positions),
        "n_positions_with_valid_open": len(timeline),
        "n_unique_symbols_with_signals": len(signals_by_symbol),
        "n_engines_analysed": len(engine_analysis),
        "engine_analysis": engine_analysis,
        "insights": insights,
        "methodology": {
            "version": "v1_passive_analyzer",
            "signal_to_action_window_days": SIGNAL_TO_ACTION_DAYS,
            "min_signals_per_engine": 10,
            "blind_spot_thresholds": "system_accuracy > 55% AND action_rate < 20% AND n_high_edge_skips > 5",
            "personal_edge_def": "khalid_accuracy_on_acted - system_accuracy_on_engine",
            "limitation": "v1 doesn't capture direct replies; infers actions from portfolio open timestamps",
        },
    }

    # 6. Write outputs
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, default=str, indent=2).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=3600, public")
    update_history(payload)
    logger.info(f"wrote {OUT_KEY}")

    # 7. Telegram digest (only if any blind spots or strong personal edge)
    if insights["blind_spots"] or any(
        r["personal_edge_pct"] and r["personal_edge_pct"] > 0.05
        for r in insights["personal_edge_strongest"]
    ):
        try:
            send_telegram(build_telegram_digest(
                engine_analysis, insights, len(outcomes), len(positions)))
        except Exception as e:
            logger.error(f"telegram_fail: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "n_engines": len(engine_analysis),
            "n_blind_spots": len(insights["blind_spots"]),
            "n_personal_edge": len(insights["personal_edge_strongest"]),
            "elapsed": round(elapsed, 2),
        }),
    }
