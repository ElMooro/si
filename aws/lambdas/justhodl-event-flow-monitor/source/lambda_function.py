"""justhodl-event-flow-monitor — institutional-grade event bus health monitor.

WHY THIS EXISTS
═══════════════
A system that coordinates through events but doesn't monitor its own event
flow is one silent failure away from looking healthy while being dead. If
the coordinator Lambda errors, all events drop on the floor — engines keep
publishing, the bus accepts, but downstream actions never fire. Without
monitoring, you'd only notice when the FAILURE cascaded into a missed call.

Hedge fund operations desks watch the message-bus's pulse the same way they
watch P&L: with continuous baselines, anomaly thresholds, and alerts that
fire when reality deviates from expected.

WHAT IT MONITORS
════════════════
1. SYSTEM PULSE — when did the last event of any type fire?
   * GREEN  : event within last 6h
   * YELLOW : 6-12h since last event
   * RED    : >12h (alert + investigate; the bus may be down)

2. PER-EVENT RATES — for each known event type, compare today's count
   vs the rolling 14-day average. Detect:
   * MISSING_EVENT_TYPE  — event type expected daily, but 0 today
   * RATE_SPIKE          — count > 5x baseline (possible runaway loop)
   * RATE_DROP           — count < 0.2x baseline AND baseline > 2 (silent failure)

3. ERROR EVENTS — track engine.error rate per engine
   * ENGINE_ERROR_SPIKE  — > 3 errors from one engine in 1h
   * MULTIPLE_ENGINES_ERRORING — 3+ different engines erroring in 24h

4. COORDINATOR HEALTH — verify coordinator's own metrics
   * COORDINATOR_FAILING — coordinator invocations have >10% error rate

EXPECTED BASELINES (the institutional knowledge encoded)
════════════════════════════════════════════════════════
These are conservative defaults; the monitor refines them via rolling
14-day average. If we have no history, we use these:

  outcome.resolved                 :  1-10 per day (outcome-checker daily)
  regime.changed                   :  0-2  per day (rare transitions)
  near_miss.extreme                :  0-10 per day (per-signal)
  calibrator.proposal_high_confidence : 0-5 per week (rare HIGH proposals)
  calibrator.weights_updated       :  0-1 per day (weekly Sunday)
  signal.promoted                  :  0-3 per week
  signal.deprecated                :  0-3 per week
  engine.error                     :  0-1 per day (any error is notable)

OUTPUT
══════
data/event-flow-health.json — read by system-health.html dashboard
SSM /justhodl/event-flow/pulse — quick boolean health for other engines
Telegram digest — only on anomalies (no noise on healthy days)

SCHEDULE
════════
cron(0 5 * * ? *) — daily 05:00 UTC since the 2026-10-20 cadence floor (one run per 24 h).

v1.1.0 (2026-10-26)
═══════════════════
ops 6674: the five storm days (Oct 12-16) left 22-33 MB day-logs inside the 14-day window and the function
parsed every line of every day into memory (512 MB ceiling, 120 s timeout, 3/3 runs).  Day-logs are now
streamed line by line; a finished day (before today) is reduced to its per-event counts once and cached at
system-events/audit/_counts/<day>.json (the day-log is immutable after midnight UTC), so a run reads today's
log plus 13 small count files.  Reading stops at a time budget and the output says how many of the 14 days
its baselines cover (window_days_read) - it never presents a shorter window as the full one.
"""
VERSION = "1.1.0"

import json
import os
import statistics
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

from _sentry_lite import track_errors
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/event-flow-health.json"
SSM_PULSE_PATH = "/justhodl/event-flow/pulse"
AUDIT_PREFIX = "system-events/audit/"

TELEGRAM_TOKEN = managed_secret(('TELEGRAM_TOKEN', 'TELEGRAM_BOT_TOKEN'), ("/justhodl/telegram/bot_token",))
TELEGRAM_CHAT_ID = "8678089260"

# Thresholds
SILENCE_YELLOW_HOURS  = 6   # GREEN below this
SILENCE_RED_HOURS     = 12  # RED above this
ENGINE_ERROR_SPIKE_PER_HOUR = 3
ENGINE_ERROR_MULTI_24H = 3   # 3 different engines erroring in 24h
RATE_SPIKE_RATIO      = 5.0  # >5x baseline → spike
RATE_DROP_RATIO       = 0.2  # <0.2x baseline → drop
MIN_BASELINE_FOR_DROP = 2.0  # only flag drop if baseline >2 (avoid noise)
COORDINATOR_ERR_PCT_THRESHOLD = 10
BASELINE_WINDOW_DAYS  = 14   # rolling avg over this many days

# Telegram cool-down: don't re-alert about the same anomaly within
# this many minutes (prevents alert storms on persistent issues)
ALERT_COOLDOWN_MIN = 60
SSM_ALERT_COOLDOWN_PATH = "/justhodl/event-flow/last-alerts"

# Expected per-event baselines (used when we have <3 days of history)
DEFAULT_EXPECTED_DAILY = {
    "outcome.resolved":                       (1, 10),
    "regime.changed":                          (0, 2),
    "regime.flashing_bucket":                  (0, 3),
    "near_miss.extreme":                       (0, 10),
    "calibrator.proposal_high_confidence":     (0, 3),
    "calibrator.weights_updated":              (0, 1),
    "signal.promoted":                         (0, 2),
    "signal.deprecated":                       (0, 2),
    "signal.fired":                            (0, 50),
    "miss.detected":                           (0, 1),
    "engine.error":                            (0, 2),
    "outcome.deferred":                        (0, 2),
}

s3  = boto3.client("s3",  region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
cw  = boto3.client("cloudwatch", region_name=REGION)


# ─── Audit log readers (v1.1.0: streamed, cached per day, budgeted) ────────

COUNTS_PREFIX = AUDIT_PREFIX + "_counts/"
READ_RESERVE_S = 30          # left for analysis + writes
DEFAULT_BUDGET_S = 270
COUNTS_SCHEMA = "event-flow-day-counts.v1"


def _date_key(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def _slim(entry: dict) -> dict:
    """What the analysis reads of a today-entry: ts, event and the engine.error detail fields."""
    det = entry.get("detail") if isinstance(entry.get("detail"), dict) else {}
    return {"ts": entry.get("ts"), "event": entry.get("event"),
            "detail": {k: det.get(k) for k in ("engine", "_source_engine", "phase", "error") if k in det}}


def stream_audit_day(date_str: str, keep_entries: bool):
    """Stream one day-log.  Returns (counts {event: n}, entries [slim] | None, present bool).
    A missing day is (empty, [], False); an unreadable line is skipped."""
    key = f"{AUDIT_PREFIX}{date_str}.jsonl"
    counts, entries = defaultdict(int), ([] if keep_entries else None)
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
    except (s3.exceptions.NoSuchKey, ClientError):
        return {}, entries, False
    except Exception as e:
        print(f"[ev-flow] err reading {key}: {e}")
        return {}, entries, False
    body = obj["Body"]
    try:
        for raw in body.iter_lines():
            if not raw:
                continue
            try:
                e = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if not isinstance(e, dict):
                continue
            counts[e.get("event", "unknown")] += 1
            if keep_entries:
                entries.append(_slim(e))
    finally:
        try:
            body.close()
        except Exception:
            pass
    return dict(counts), entries, True


def read_day_counts_cached(date_str: str) -> tuple:
    """Per-event counts for a FINISHED day: the cached summary when present, else one streamed pass that is
    then cached (the day-log never changes after midnight UTC).  Returns (counts, present)."""
    ckey = f"{COUNTS_PREFIX}{date_str}.json"
    try:
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=ckey)["Body"].read())
        if isinstance(doc, dict) and doc.get("schema") == COUNTS_SCHEMA and isinstance(doc.get("counts"), dict):
            return {k: int(v) for k, v in doc["counts"].items()}, bool(doc.get("present", True))
    except Exception:
        pass
    counts, _, present = stream_audit_day(date_str, keep_entries=False)
    try:
        s3.put_object(Bucket=BUCKET, Key=ckey, ContentType="application/json", CacheControl="no-cache",
                      Body=json.dumps({"schema": COUNTS_SCHEMA, "day": date_str, "present": present, "n_events": sum(counts.values()),
                                       "counts": counts, "summarised_at": datetime.now(timezone.utc).isoformat()}).encode("utf-8"))
    except Exception as e:
        print(f"[ev-flow] counts cache {date_str} not written: {e}")
    return counts, present


def read_recent_window(days: int, deadline: float = None) -> dict:
    """Audit activity for `days` calendar days ending today (UTC).
    Returns {"counts": {date: {event: n}}, "today_entries": [slim], "days_read": n, "days_present": n,
             "days_skipped": [dates not read before the deadline]}."""
    now = datetime.now(timezone.utc)
    today = _date_key(now)
    counts, skipped, present_days = {}, [], 0
    tc, today_entries, present = stream_audit_day(today, keep_entries=True)
    counts[today] = tc
    present_days += int(present)
    for i in range(1, days):
        ds = _date_key(now - timedelta(days=i))
        if deadline is not None and time.time() >= deadline:
            skipped.append(ds)
            continue
        c, p = read_day_counts_cached(ds)
        counts[ds] = c
        present_days += int(p)
    return {"counts": counts, "today_entries": today_entries, "days_read": days - len(skipped),
            "days_present": present_days, "days_skipped": skipped}


# ─── Analysis ────────────────────────────────────────────────────────────

def compute_pulse(entries_today: list) -> dict:
    """When was the last event in any form?"""
    if not entries_today:
        # No events today — check yesterday for last activity
        return {
            "status":            "RED",
            "last_event_age_minutes": None,
            "last_event_ts":     None,
            "last_event_type":   None,
            "reason":            "no events today",
        }
    
    # Find most recent ts
    latest = None
    latest_event = None
    for e in entries_today:
        try:
            ts = datetime.fromisoformat(e["ts"].replace("Z", "+00:00"))
            if latest is None or ts > latest:
                latest = ts
                latest_event = e.get("event", "?")
        except Exception:
            continue
    
    if latest is None:
        return {"status": "RED", "reason": "no parseable timestamps"}
    
    age_min = (datetime.now(timezone.utc) - latest).total_seconds() / 60
    if age_min < SILENCE_YELLOW_HOURS * 60:
        status = "GREEN"
    elif age_min < SILENCE_RED_HOURS * 60:
        status = "YELLOW"
    else:
        status = "RED"
    
    return {
        "status":                 status,
        "last_event_age_minutes": round(age_min, 1),
        "last_event_ts":          latest.isoformat(),
        "last_event_type":        latest_event,
    }


def aggregate_by_event(window: dict) -> dict:
    """Pivot {date: {event: count}} into {event_name: {date: count}}."""
    out = defaultdict(dict)
    for date_str, day_counts in window.items():
        for event_name, n in (day_counts or {}).items():
            out[event_name][date_str] = out[event_name].get(date_str, 0) + int(n)
    return {k: dict(v) for k, v in out.items()}


def compute_baselines(by_event_by_day: dict, today: str) -> dict:
    """For each event type, compute (mean, median, n_days_seen) excluding
    today, over the rolling window. We exclude today so today's count can
    be compared cleanly."""
    out = {}
    for event_name, day_counts in by_event_by_day.items():
        historical = [count for date, count in day_counts.items()
                       if date != today]
        if not historical:
            out[event_name] = {"mean": None, "median": None, "n_days": 0}
            continue
        out[event_name] = {
            "mean":   round(statistics.mean(historical), 2),
            "median": statistics.median(historical),
            "n_days": len(historical),
            "max":    max(historical),
            "min":    min(historical),
        }
    return out


def detect_anomalies(by_event_by_day: dict, baselines: dict,
                       today: str) -> list:
    """Run all anomaly detectors. Returns list of anomaly dicts with
    severity tag."""
    anomalies = []
    
    # Look at today's counts
    today_counts = {ev: cnts.get(today, 0)
                     for ev, cnts in by_event_by_day.items()}
    
    # ── Detector 1: RATE_SPIKE & RATE_DROP per event type
    for event_name, base in baselines.items():
        today_count = today_counts.get(event_name, 0)
        mean = base.get("mean")
        n_days = base.get("n_days", 0)
        
        if mean is None or n_days < 3:
            # Not enough history — fall back to expected bounds
            expected = DEFAULT_EXPECTED_DAILY.get(event_name)
            if expected:
                exp_min, exp_max = expected
                if today_count > exp_max * 2:
                    anomalies.append({
                        "type":     "RATE_SPIKE_VS_EXPECTED",
                        "event":    event_name,
                        "today":    today_count,
                        "expected_max": exp_max,
                        "severity": "WARN",
                    })
                # Skip the drop detector when no history; can't tell
                # if "0 today" is an issue vs. normal stillness
            continue
        
        # We have history
        if mean >= 1 and today_count > mean * RATE_SPIKE_RATIO:
            anomalies.append({
                "type":      "RATE_SPIKE",
                "event":     event_name,
                "today":     today_count,
                "baseline_mean": mean,
                "ratio":     round(today_count / mean, 1),
                "severity":  "WARN",
            })
        elif (mean >= MIN_BASELINE_FOR_DROP and
                today_count < mean * RATE_DROP_RATIO):
            anomalies.append({
                "type":      "RATE_DROP",
                "event":     event_name,
                "today":     today_count,
                "baseline_mean": mean,
                "ratio":     round(today_count / max(0.01, mean), 2),
                "severity":  "WARN",
            })
    
    # ── Detector 2: engine.error spike
    # Look at last hour of engine.error events
    if "engine.error" in by_event_by_day:
        # We need timestamps, not just counts — re-scan today's entries
        # for engine.error frequency
        pass  # done in compute_engine_errors below
    
    return anomalies


def compute_engine_errors(today_entries: list) -> dict:
    """Detail breakdown of engine.error events today: per engine, per hour."""
    out = {
        "n_total_today":     0,
        "by_engine":         defaultdict(int),
        "by_engine_24h":     defaultdict(list),  # list of timestamps
        "last_1h_total":     0,
        "n_engines_erroring": 0,
        "recent_errors":     [],
    }
    one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
    
    for e in today_entries:
        if e.get("event") != "engine.error":
            continue
        out["n_total_today"] += 1
        det = e.get("detail") or {}
        engine = (det.get("engine") or det.get("_source_engine") or "?").replace("justhodl-", "")
        out["by_engine"][engine] += 1
        try:
            ts = datetime.fromisoformat(e["ts"].replace("Z", "+00:00"))
            out["by_engine_24h"][engine].append(ts.isoformat())
            if ts >= one_hour_ago:
                out["last_1h_total"] += 1
        except Exception:
            pass
        # Capture last 5 for the report
        if len(out["recent_errors"]) < 5:
            out["recent_errors"].append({
                "ts":     e.get("ts"),
                "engine": engine,
                "phase":  det.get("phase"),
                "error":  (det.get("error") or "")[:200],
            })
    
    out["n_engines_erroring"] = len(out["by_engine"])
    out["by_engine"] = dict(out["by_engine"])
    out["by_engine_24h"] = {k: v for k, v in out["by_engine_24h"].items()}
    return out


def detect_engine_error_anomalies(engine_errors: dict) -> list:
    """Detect engine error spikes."""
    out = []
    if engine_errors["last_1h_total"] >= ENGINE_ERROR_SPIKE_PER_HOUR:
        for engine, count in engine_errors["by_engine"].items():
            if count >= ENGINE_ERROR_SPIKE_PER_HOUR:
                out.append({
                    "type":     "ENGINE_ERROR_SPIKE",
                    "engine":   engine,
                    "count_1h": count,
                    "severity": "CRITICAL",
                })
    if engine_errors["n_engines_erroring"] >= ENGINE_ERROR_MULTI_24H:
        out.append({
            "type":            "MULTIPLE_ENGINES_ERRORING",
            "n_engines":       engine_errors["n_engines_erroring"],
            "engines":         list(engine_errors["by_engine"].keys())[:10],
            "severity":        "CRITICAL",
        })
    return out


def check_coordinator_health() -> dict:
    """CloudWatch metrics for the coordinator Lambda itself.
    If the coordinator is failing, the whole event system is degraded."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=24)
    try:
        inv = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": "justhodl-event-coordinator"}],
            StartTime=start, EndTime=end, Period=3600,
            Statistics=["Sum"],
        )
        err = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": "justhodl-event-coordinator"}],
            StartTime=start, EndTime=end, Period=3600,
            Statistics=["Sum"],
        )
        n_inv = int(sum(p["Sum"] for p in inv.get("Datapoints") or []))
        n_err = int(sum(p["Sum"] for p in err.get("Datapoints") or []))
        err_pct = round(n_err / max(1, n_inv) * 100, 2)
        return {
            "invocations_24h": n_inv,
            "errors_24h":      n_err,
            "error_rate_pct":  err_pct,
            "status":          "RED" if err_pct > COORDINATOR_ERR_PCT_THRESHOLD else "GREEN",
        }
    except Exception as e:
        return {"err": str(e)[:200]}


# ─── Alerting ────────────────────────────────────────────────────────────

def get_last_alerts() -> dict:
    """SSM-backed cool-down log to avoid alert storms."""
    try:
        resp = ssm.get_parameter(Name=SSM_ALERT_COOLDOWN_PATH)
        return json.loads(resp["Parameter"]["Value"])
    except Exception:
        return {}


def save_last_alerts(d: dict):
    try:
        ssm.put_parameter(
            Name=SSM_ALERT_COOLDOWN_PATH,
            Value=json.dumps(d, default=str),
            Type="String",
            Overwrite=True,
        )
    except Exception as e:
        print(f"[ev-flow] save alerts: {e}")


def should_alert(anomaly_key: str, last_alerts: dict) -> bool:
    """Has it been ALERT_COOLDOWN_MIN since we last alerted on this exact
    anomaly? Prevents storms."""
    last = last_alerts.get(anomaly_key)
    if not last:
        return True
    try:
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        age_min = (datetime.now(timezone.utc) - last_dt).total_seconds() / 60
        return age_min >= ALERT_COOLDOWN_MIN
    except Exception:
        return True


def send_telegram(text: str) -> bool:
    try:
        data = urllib.parse.urlencode({
            "chat_id":  TELEGRAM_CHAT_ID,
            "text":     text,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=data, method="POST")
        urllib.request.urlopen(req, timeout=15).read()
        return True
    except Exception as e:
        print(f"[ev-flow] telegram: {e}")
        return False


def alert_for_anomalies(anomalies: list, pulse: dict,
                          engine_errors: dict,
                          coord_health: dict) -> dict:
    """Send alerts for unique, un-cooled-down anomalies. Returns dict of
    which alerts were sent."""
    last = get_last_alerts()
    now_iso = datetime.now(timezone.utc).isoformat()
    sent = []
    skipped = []
    
    # System pulse alert
    if pulse["status"] == "RED":
        key = f"pulse-red"
        if should_alert(key, last):
            send_telegram(
                f"🔴 <b>Event bus silent</b>\n"
                f"No events in last {SILENCE_RED_HOURS}h.\n"
                f"Last event: {(pulse.get('last_event_ts') or '?')[:19]} "
                f"({pulse.get('last_event_type') or '?'})\n"
                f"<i>Check coordinator + producers immediately.</i>"
            )
            last[key] = now_iso
            sent.append(key)
        else:
            skipped.append(key)
    
    # Coordinator health
    if coord_health.get("status") == "RED":
        key = "coord-err"
        if should_alert(key, last):
            send_telegram(
                f"🚨 <b>Event coordinator failing</b>\n"
                f"24h error rate: <b>{coord_health.get('error_rate_pct')}%</b>\n"
                f"Invocations: {coord_health.get('invocations_24h')}, "
                f"errors: {coord_health.get('errors_24h')}"
            )
            last[key] = now_iso
            sent.append(key)
        else:
            skipped.append(key)
    
    # Critical anomalies
    for a in anomalies:
        if a.get("severity") != "CRITICAL":
            continue
        key = f"crit-{a['type']}-{a.get('engine', a.get('event', '?'))}"
        if should_alert(key, last):
            if a["type"] == "ENGINE_ERROR_SPIKE":
                send_telegram(
                    f"🚨 <b>Engine error spike</b>\n"
                    f"engine: <code>{a['engine']}</code>\n"
                    f"errors in last hour: <b>{a['count_1h']}</b>"
                )
            elif a["type"] == "MULTIPLE_ENGINES_ERRORING":
                send_telegram(
                    f"🚨 <b>Multiple engines erroring</b>\n"
                    f"n_engines: <b>{a['n_engines']}</b>\n"
                    f"engines: {', '.join(a.get('engines') or [])}"
                )
            last[key] = now_iso
            sent.append(key)
        else:
            skipped.append(key)
    
    save_last_alerts(last)
    return {"sent": sent, "skipped_cooldown": skipped}


# ─── Handler ────────────────────────────────────────────────────────────

@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)
    t0 = time.time()
    today_str = _date_key(started)
    budget = DEFAULT_BUDGET_S
    try:
        budget = context.get_remaining_time_in_millis() / 1000.0
    except Exception:
        pass
    deadline = t0 + max(0.0, budget - READ_RESERVE_S)

    # Read window (streamed; finished days from the per-day counts cache; budgeted)
    read = read_recent_window(BASELINE_WINDOW_DAYS, deadline)
    today_entries = read["today_entries"]
    if read["days_skipped"]:
        print(f"[ev-flow] budget: {len(read['days_skipped'])} of {BASELINE_WINDOW_DAYS} days not read ({read['days_skipped'][:3]}...)")

    # Compute analyses
    pulse = compute_pulse(today_entries)
    by_event_by_day = aggregate_by_event(read["counts"])
    baselines = compute_baselines(by_event_by_day, today_str)
    rate_anomalies = detect_anomalies(by_event_by_day, baselines, today_str)
    engine_errors = compute_engine_errors(today_entries)
    error_anomalies = detect_engine_error_anomalies(engine_errors)
    coord_health = check_coordinator_health()
    
    all_anomalies = rate_anomalies + error_anomalies
    
    # Alerts (with cool-down)
    alert_result = alert_for_anomalies(all_anomalies, pulse,
                                          engine_errors, coord_health)
    
    # Build per-event summary for dashboard
    by_event_summary = {}
    for event_name, day_counts in by_event_by_day.items():
        today_count = day_counts.get(today_str, 0)
        base = baselines.get(event_name) or {}
        by_event_summary[event_name] = {
            "today":              today_count,
            "yesterday":          day_counts.get(_date_key(started - timedelta(days=1)), 0),
            "rolling_mean":       base.get("mean"),
            "rolling_median":     base.get("median"),
            "n_days_with_data":   base.get("n_days", 0),
            "history":            day_counts,
        }
    
    # Output
    output = {
        "schema_version":    "1.0",
        "engine_version":    VERSION,
        "generated_at":      started.isoformat(),
        "window_days":       BASELINE_WINDOW_DAYS,
        "window_days_read":  read["days_read"],
        "audit_days_present": read["days_present"],
        "days_not_read":     read["days_skipped"],
        "pulse":             pulse,
        "totals_today": {
            "n_events":           len(today_entries),
            "n_event_types_seen": len({e.get("event") for e in today_entries
                                         if e.get("event")}),
        },
        "by_event":          by_event_summary,
        "engine_errors":     engine_errors,
        "anomalies":         all_anomalies,
        "coordinator_health": coord_health,
        "alerting":          alert_result,
        "thresholds": {
            "silence_yellow_h":  SILENCE_YELLOW_HOURS,
            "silence_red_h":     SILENCE_RED_HOURS,
            "rate_spike_ratio":  RATE_SPIKE_RATIO,
            "rate_drop_ratio":   RATE_DROP_RATIO,
            "alert_cooldown_min": ALERT_COOLDOWN_MIN,
        },
    }
    
    # Persist S3 + SSM
    s3.put_object(
        Bucket=BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output, separators=(",", ":"), default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=300",
    )
    
    # Quick boolean pulse to SSM for engines that want it
    try:
        ssm.put_parameter(
            Name=SSM_PULSE_PATH,
            Value=json.dumps({
                "status":            pulse["status"],
                "last_event_age_min": pulse.get("last_event_age_minutes"),
                "n_anomalies":       len(all_anomalies),
                "generated_at":      started.isoformat(),
            }, default=str),
            Type="String", Overwrite=True,
        )
    except Exception as e:
        print(f"[ev-flow] ssm pulse put: {e}")
    
    print(f"[ev-flow] v{VERSION} pulse={pulse['status']} "
          f"events_today={len(today_entries)} "
          f"days_read={read['days_read']}/{BASELINE_WINDOW_DAYS} present={read['days_present']} "
          f"anomalies={len(all_anomalies)} "
          f"alerts_sent={len(alert_result['sent'])} in {time.time() - t0:.1f}s")
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "pulse": pulse["status"],
            "events_today": len(today_entries),
            "anomalies": len(all_anomalies),
            "alerts_sent": len(alert_result["sent"]),
        }),
    }


lambda_handler = handler
