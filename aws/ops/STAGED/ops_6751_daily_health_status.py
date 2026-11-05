"""ops 6751 - READ-ONLY daily health status (scheduled daily health run, 2026-11-05: third full day after the Nov 2 15:44 UTC
release of the false FREEZE; first run after the strength (ops 6739-6742) and leaders (ops 6743-6750) engine births).

  A. justhodl-spend-governor {"action":"status"}: version, mode, stage, cap, budget actual/forecast, thresholds, held /
     capped / anomaly lists, fuse, cadence sentinel, meter, storage record (last assessment, versioning, per-service
     daily spend);
  B. justhodl-spend-governor {"action":"caps"}: month-to-date top engines by metered Lambda cost;
  C. the governor's own event records since yesterday's health run (ops 6727, 2026-11-04 11:19 UTC), the raw
     describe_budget read, and the GitHub-issues relay's marker (how far behind the newest record it is);
  D. freshness: HEAD every declared output in engine-manifest.json (bucket justhodl-dashboard-live), bucketed with the
     fleet-monitor tiers (<30 h fresh, <7 d stale, >=7 d dead); engines whose EVERY output is 54 h - 7 d old (two daily
     slots missed, not dead) are named;
  E. FMP bandwidth hold: the walker's and the bank's manifests (both were self-holding until 06:41 / 06:50 UTC today);
  F. follow-up on yesterday's finding (ops 6728: 7 engines failing every run, 5 running without writing): for every
     engine section D names as silent plus yesterday's twelve - live triggers, per-day CloudWatch invocations / errors
     / throttles over 4 days, a verdict, and for the ones that fail the tail of their newest log stream (yesterday's
     3-day error filter found no line for ecb-derived and systemic-stress). Log lines are redacted (key=value secrets,
     long opaque tokens) before they are written to the report;
  G. the two scheduled source-refresh workflows that are red (capital-structure-source-refresh.yml: 'Previous refresh
     incomplete or failed; explicit review is required'; statement-source-refresh.yml): the capital-structure refresh
     control's status fields, read only (no release, no retry - the release is ops-numbered human review).
Invokes nothing but the governor's read-only actions; releases nothing; writes nothing but this report.
"""
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
import concurrent.futures as cf
import json
import re
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops" / "checks"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))

REGION = "us-east-1"
ACCOUNT = "857687956942"
LIVE = "justhodl-dashboard-live"
PRIVATE = "justhodl-ops-private-857687956942"
GOVERNOR = "justhodl-spend-governor"
BUDGET_NAME = "JustHodl-Monthly-Hard-Cap-600"
EVENTS_PREFIX = "spend-governor/events/"
RELAY_MARKER = "spend-governor/alerts/github-issues.json"
YESTERDAY_RUN = "20261104T111955"          # ops 6727 finished 2026-11-04T11:19:55Z
FRESH_H, DEAD_H = 30.0, 168.0
TWO_SLOTS_H = 54.0                         # past the 30 h fresh line plus one more daily slot
FMP_MANIFESTS = ("data/warm/fmp/manifest.json", "data/warm/fmp-bank/manifest.json")
NEW_ENGINES = ("data/13f-book.json", "data/strength.json", "data/leaders.json")    # born Nov 3 / Nov 4 / Nov 5
# yesterday's finding (ops 6728): engine -> its verdict then
YESTERDAY = {
    "justhodl-earnings-blackout": "FAILING", "justhodl-eps-revision-velocity": "FAILING",
    "justhodl-revenue-acceleration": "FAILING", "justhodl-momentum-breakout": "FAILING",
    "justhodl-volatility-squeeze-hunter": "FAILING", "justhodl-ecb-derived": "FAILING",
    "justhodl-systemic-stress": "FAILING", "justhodl-crypto-emergence": "RAN_NO_WRITE",
    "justhodl-earnings-quality": "RAN_NO_WRITE", "justhodl-forward-orders": "RAN_NO_WRITE",
    "justhodl-russell-recon-frontrun": "RAN_NO_WRITE", "justhodl-portwatch": "RAN_NO_WRITE",
}
# the manifest attributes some outputs to a differently named producer (ops 6728)
ALIASES = {"justhodl-momentum-breakout": "justhodl-momentum-leaders", "justhodl-naaim": "justhodl-put-call-extreme",
           "justhodl-eps-revision-velocity": "justhodl-nobrainer-rationale"}
PROBE_MAX = 45
SECRET_KV = re.compile(r"(?i)\b(api[_-]?key|apikey|access[_-]?key|token|secret|password|passwd|authorization|signature|key)(\s*[=:]\s*|%3D)[^\s&\"',}]+")
BEARER = re.compile(r"(?i)\bbearer\s+\S+")
LONG_TOKEN = re.compile(r"[A-Za-z0-9_\-+/=]{20,}")
AWS_KEY = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")
HEX32 = re.compile(r"[0-9a-fA-F]{32,}")


def _opaque(tok):
    """A 20+ character run that looks like a credential rather than a name: an AWS access key id, 32+ hex, mixed-case with
    digits, or 28+ alphanumerics with digits and no word separators. Engine names and S3 keys (lowercase words joined
    by - or _) are left alone."""
    if AWS_KEY.fullmatch(tok) or HEX32.fullmatch(tok):
        return True
    has_d, has_u, has_l = any(c.isdigit() for c in tok), any(c.isupper() for c in tok), any(c.islower() for c in tok)
    if has_d and has_u and has_l:
        return True
    return len(tok) >= 28 and has_d and (has_u or has_l) and "-" not in tok and "_" not in tok


def redact(line):
    """A log line safe for a committed report: key=value secrets and long opaque tokens masked. Pure, for the selftest."""
    line = BEARER.sub("Bearer ***", str(line))
    line = SECRET_KV.sub(lambda m: m.group(1) + m.group(2) + "***", line)
    return LONG_TOKEN.sub(lambda m: "***" if _opaque(m.group(0)) else m.group(0), line)


def verdict(exists, reserved, triggers_enabled, inv, err, thr, out_age_h, due_daily, window_h):
    """Pure classification for the selftest. inv/err/thr are sums over the last two days; due_daily says an enabled
    trigger fires at least daily; window_h is the age past which an engine that ran should have written."""
    if not exists:
        return "NO_FUNCTION"
    if reserved == 0:
        return "HELD"
    if thr > 0 and inv == 0:
        return "THROTTLED"
    if inv > 0 and err >= inv:
        return "FAILING"
    if inv > 0 and err > 0:
        return "PARTLY_FAILING"
    if inv > 0 and out_age_h is not None and out_age_h > window_h:
        return "RAN_NO_WRITE"
    if inv > 0:
        return "RAN"
    if not triggers_enabled:
        return "NO_TRIGGER"
    if due_daily:
        return "NEVER_STARTED"
    return "ON_TIME"


def daily_or_faster(expr):
    """True for rate(...) up to a day and for cron expressions with every day-of-month, month and day-of-week. Pure."""
    expr = str(expr or "")
    if expr.startswith("rate("):
        m = re.match(r"rate\((\d+)\s+(minute|hour|day)s?\)", expr)
        return bool(m) and (m.group(2) != "day" or int(m.group(1)) <= 1)
    if expr.startswith("cron("):
        parts = expr[5:].rstrip(")").split()
        return len(parts) >= 5 and parts[2] in ("*", "?") and parts[3] == "*" and parts[4] in ("?", "*")
    return False


def daily_metrics(cw, names, start, end):
    q, ids = [], {}
    for i, n in enumerate(names):
        for tag, m in (("n", "Invocations"), ("e", "Errors"), ("t", "Throttles")):
            ids["%s%d" % (tag, i)] = (n, tag)
            q.append({"Id": "%s%d" % (tag, i), "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": m,
                      "Dimensions": [{"Name": "FunctionName", "Value": n}]}, "Period": 86400, "Stat": "Sum"}})
    out = {n: {} for n in names}
    for i in range(0, len(q), 400):
        tok = None
        while True:
            kw = {"MetricDataQueries": q[i:i + 400], "StartTime": start, "EndTime": end}
            if tok:
                kw["NextToken"] = tok
            res = cw.get_metric_data(**kw)
            for r_ in res["MetricDataResults"]:
                n, tag = ids[r_["Id"]]
                for t, v in zip(r_["Timestamps"], r_["Values"]):
                    out[n].setdefault(t.date().isoformat(), {}).setdefault(tag, 0.0)
                    out[n][t.date().isoformat()][tag] += v
            tok = res.get("NextToken")
            if not tok:
                break
    return out


def tier(age_h):
    if age_h is None:
        return "missing"
    if age_h < FRESH_H:
        return "fresh"
    if age_h < DEAD_H:
        return "stale"
    return "dead"


def hold_state(man, now_ts):
    """(held, until_iso, note) from a walker/bank manifest - pure, for the selftest."""
    until = float(man.get("bandwidth_hold_until") or 0)
    held = until > now_ts
    until_iso = datetime.fromtimestamp(until, timezone.utc).isoformat(timespec="minutes") if until else None
    return held, until_iso, str(man.get("bandwidth_note") or "")[:160]


def missed_two_slots(ages):
    """True when an engine's every declared output is >= TWO_SLOTS_H and < DEAD_H old (pre-freeze write, two daily slots
    since the restore, not yet dead). Pure, for the selftest. A missing output (None) disqualifies: 'never had one' is a
    different state from 'had one and stopped' and is reported separately."""
    return bool(ages) and all(a is not None and TWO_SLOTS_H <= a < DEAD_H for a in ages)


def event_after(key, stamp):
    """Event record key newer than a compact UTC stamp (YYYYMMDDTHHMMSS) - pure, for the selftest."""
    stem = key.rsplit("/", 1)[-1]
    return stem[:15] > stamp


def selftest():
    assert tier(None) == "missing" and tier(1) == "fresh" and tier(40) == "stale" and tier(200) == "dead"
    assert hold_state({"bandwidth_hold_until": 100}, 50)[0] is True
    assert hold_state({"bandwidth_hold_until": 100}, 150)[0] is False
    assert hold_state({}, 150) == (False, None, "")
    assert missed_two_slots([92.0, 93.5]) is True and missed_two_slots([54.0]) is True and missed_two_slots([53.9]) is False
    assert missed_two_slots([92.0, 12.0]) is False          # one fresh output: the engine ran
    assert missed_two_slots([92.0, None]) is False          # a missing output is its own finding
    assert missed_two_slots([200.0]) is False               # dead, counted elsewhere
    assert missed_two_slots([]) is False
    assert event_after(EVENTS_PREFIX + "20261104T154447.324976Z-restore.json", YESTERDAY_RUN) is True
    assert event_after(EVENTS_PREFIX + "20261104T062129.769909Z-release-cap.json", YESTERDAY_RUN) is False
    assert event_after(EVENTS_PREFIX + "20261105T000737.000001Z-x.json", YESTERDAY_RUN) is True
    red = redact("GET https://x/stable/quote?symbol=AAPL&apikey=abc123&limit=5")
    assert "abc123" not in red and "symbol=AAPL" in red and "limit=5" in red
    assert redact("token: s3cr3t rest") == "token: *** rest"
    assert redact("Authorization: Bearer abc.def") .count("abc.def") == 0
    assert redact("id AbCdEfGhIjKlMnOpQrStUvWxYz0123456789 end") == "id *** end"
    assert redact("k 0123456789abcdef0123456789abcdef z") == "k *** z"
    assert redact("AKIA" + "ABCDEFGHIJKLMNOP") == "***"   # built at runtime: the repo's credential scanner (scripts/check_secrets.py) must never see the literal
    assert redact("HTTP 429 Bandwidth Limit Reach") == "HTTP 429 Bandwidth Limit Reach"
    assert redact("justhodl-volatility-squeeze-hunter wrote data/volatility-squeeze.json") == "justhodl-volatility-squeeze-hunter wrote data/volatility-squeeze.json"
    assert redact("RequestId: 1b2c3d4e-aaaa-bbbb-cccc-0123456789ab Duration: 12 ms") == "RequestId: 1b2c3d4e-aaaa-bbbb-cccc-0123456789ab Duration: 12 ms"
    assert verdict(False, None, 0, 0, 0, 0, 100, True, 54) == "NO_FUNCTION"
    assert verdict(True, 0, 1, 0, 0, 0, 100, True, 54) == "HELD"
    assert verdict(True, None, 1, 0, 0, 3, 100, True, 54) == "THROTTLED"
    assert verdict(True, None, 1, 2, 2, 0, 100, True, 54) == "FAILING"
    assert verdict(True, None, 1, 4, 1, 0, 100, True, 54) == "PARTLY_FAILING"
    assert verdict(True, None, 1, 2, 0, 0, 100, True, 54) == "RAN_NO_WRITE"
    assert verdict(True, None, 1, 2, 0, 0, 1, True, 54) == "RAN"
    assert verdict(True, None, 0, 0, 0, 0, 100, False, 54) == "NO_TRIGGER"
    assert verdict(True, None, 1, 0, 0, 0, 100, True, 54) == "NEVER_STARTED"
    assert verdict(True, None, 1, 0, 0, 0, 120, False, 54) == "ON_TIME"
    assert daily_or_faster("rate(1 day)") and daily_or_faster("rate(15 minutes)") and not daily_or_faster("rate(7 days)")
    assert daily_or_faster("cron(37 16 * * ? *)") and not daily_or_faster("cron(0 14 ? * SAT *)") and not daily_or_faster("cron(0 9 1 * ? *)")
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=48)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    facts = {}
    with report("ops_6751_daily_health_status") as r:
        r.heading("ops 6751 - daily health status %s (governor status + caps, event records since ops 6727, relay lag, output "
                  "freshness, FMP bandwidth hold, follow-up probe of the silent engines, source-refresh control; read-only)" % now.date())

        r.section("A. governor status")
        st = {}
        for attempt in range(6):   # reserved concurrency 1: a running tick answers TooManyRequests
            try:
                resp = lam.invoke(FunctionName=GOVERNOR, InvocationType="RequestResponse", Payload=b'{"action":"status"}')
                st = json.loads(resp["Payload"].read() or b"{}")
                if resp.get("FunctionError") or "error" in st:
                    r.fail("status invoke failed: %s" % str(st)[:600])
                    sys.exit(1)
                break
            except lam.exceptions.TooManyRequestsException:
                r.warn("governor busy (attempt %d) - waiting 30 s" % (attempt + 1))
                time.sleep(30)
        if not st:
            r.fail("governor status unavailable after 6 attempts")
            sys.exit(1)
        th = st.get("thresholds") or {}
        facts.update(version=st.get("version"), mode=st.get("mode"), stage=st.get("stage"), stage_since=st.get("stage_since"),
                     cap=st.get("cap"), cap_source=st.get("cap_source"), budget_limit=st.get("budget_limit"),
                     budget_actual=st.get("budget_actual"), budget_forecast=st.get("budget_forecast"), mtd=th.get("mtd"),
                     projected_eom=th.get("projected_eom"), baseline_usd_day=th.get("baseline_usd_day"))
        r.log("version %s mode %s stage %s since %s" % (st.get("version"), st.get("mode"), st.get("stage"), st.get("stage_since")))
        r.log("cap $%s (%s) | budget limit $%s actual $%s forecast $%s" % (st.get("cap"), st.get("cap_source"), st.get("budget_limit"),
                                                                             st.get("budget_actual"), st.get("budget_forecast")))
        r.log("thresholds: %s" % json.dumps(th, sort_keys=True, default=str)[:900])
        r.log("held (stage/lever -> count): %s" % json.dumps(st.get("held") or {}, sort_keys=True))
        r.log("capped (CAP, manual release only): %s" % (st.get("capped") or "none"))
        r.log("anomaly held (ANOM): %s" % (st.get("anomaly_held") or "none"))
        r.log("fuse: %s" % json.dumps(st.get("fuse") or {}, default=str)[:600])
        r.log("suppress_until: %s | last tick: %s" % (st.get("suppress_until"), json.dumps(st.get("last") or {}, default=str)[:700]))
        r.log("cadence sentinel: %s | fixed entries: %s" % (json.dumps(st.get("cadence") or {}, default=str)[:500], st.get("cadence_fixed")))
        r.log("anomalies: %s" % json.dumps(st.get("anomalies") or {}, default=str)[:700])
        r.log("meter: %s" % json.dumps(st.get("meter") or {}, default=str)[:700])
        if st.get("freshness"):
            r.log("governor freshness: %s" % json.dumps(st.get("freshness"), default=str)[:900])
        if st.get("trigger_breaches") or st.get("breaches"):
            r.log("trigger breaches: %s" % json.dumps(st.get("trigger_breaches") or st.get("breaches"), default=str)[:600])
        sto = st.get("storage") or {}
        r.log("storage: day %s | versioning %s | warn_days %s | trip_days %s" % (sto.get("day"), sto.get("versioning"),
                                                                                  sto.get("warn_days"), sto.get("trip_days")))
        r.log("storage last assessment: %s" % json.dumps(sto.get("last") or {}, default=str)[:900])
        r.log("S3 $/day (storage.s3_usd): %s" % json.dumps(sto.get("s3_usd") or {}, sort_keys=True))
        svc = sto.get("svc_usd") or {}
        for day in sorted(svc)[-4:]:
            rows = svc[day] if isinstance(svc[day], dict) else {}
            top = sorted(((k, float(v)) for k, v in rows.items() if isinstance(v, (int, float))), key=lambda kv: -kv[1])[:6]
            r.log("service spend %s: total $%.2f | %s" % (day, sum(float(v) for v in rows.values() if isinstance(v, (int, float))),
                                                          ", ".join("%s $%.2f" % (k[:30], v) for k, v in top)))
        facts.update(capped=len(st.get("capped") or []), anomaly_held=len(st.get("anomaly_held") or []),
                     storage_day=sto.get("day"), versioning=sto.get("versioning"),
                     storage_last_gb=(sto.get("last") or {}).get("gb"), storage_last_level=(sto.get("last") or {}).get("level"))
        held_total = sum(int(v) for v in (st.get("held") or {}).values())
        facts["held_total"] = held_total
        if st.get("stage") != "NORMAL":
            r.warn("stage %s on day %d of the month with %d ledger entries held (NORMAL since 2026-11-02 15:44 UTC at "
                   "yesterday's run - a non-NORMAL stage now is new)" % (st.get("stage"), now.day, held_total))
        if st.get("version") != "1.6.1":
            r.warn("governor version %s - 1.6.1 (deployed 2026-11-03 06:27 UTC) expected; a newer version means a deploy this "
                   "run did not know about" % st.get("version"))
        if st.get("anomaly_held"):
            r.warn("ANOM holds present (6 h cooldown each; a third hold in a month becomes CAP): %s" % st.get("anomaly_held"))
        capped_now = sorted(st.get("capped") or [])
        if capped_now != ["justhodl-event-coordinator", "justhodl-miss-calibrator", "ofrapi"]:
            r.warn("CAP list changed since yesterday (was event-coordinator, miss-calibrator, ofrapi): %s" % capped_now)

        r.section("B. meter: month-to-date top engines")
        try:
            resp = lam.invoke(FunctionName=GOVERNOR, InvocationType="RequestResponse", Payload=b'{"action":"caps"}')
            caps = json.loads(resp["Payload"].read() or b"{}")
        except Exception as exc:  # noqa: BLE001
            caps = {"error": str(exc)[:300]}
        if "error" in caps:
            r.warn("caps invoke failed: %s" % str(caps)[:400])
        else:
            r.log("engine cap $%s/month | overrides %s | held %s" % (caps.get("cap_usd_month"), caps.get("overrides"), caps.get("held")))
            r.log("released: %s" % json.dumps(caps.get("released") or {}, default=str)[:900])
            top = caps.get("month_to_date_top") or []
            r.log("month-to-date top %d: %s" % (len(top), ", ".join("%s $%.2f" % (fn.replace("justhodl-", ""), v) for fn, v in top[:20])))
            facts["meter_top"] = "; ".join("%s $%.2f" % (fn.replace("justhodl-", ""), v) for fn, v in top[:5]) or "(none yet)"
            near = [(fn, v) for fn, v in top if v >= 25.0]
            if near:
                r.warn("engines past half the $50 cap this month: %s" % near)

        r.section("C. governor event records since ops 6727 (2026-11-04 11:19 UTC), the raw budget read, the relay's marker")
        kinds = Counter()
        newest_key = None
        keys = []
        try:
            for day in ("20261104", "20261105"):
                tok = None
                while True:
                    kw = {"Bucket": PRIVATE, "Prefix": EVENTS_PREFIX + day}
                    if tok:
                        kw["ContinuationToken"] = tok
                    res = s3.list_objects_v2(**kw)
                    keys += [o["Key"] for o in res.get("Contents", [])]
                    tok = res.get("NextContinuationToken")
                    if not tok:
                        break
            keys = sorted(keys)
            newest_key = keys[-1] if keys else None
            since = [k for k in keys if event_after(k, YESTERDAY_RUN)]
            r.log("%d event records on Nov 4 + Nov 5; %d since ops 6727 (newest %s)" % (len(keys), len(since), (newest_key or "-").split("/")[-1]))
            for k in since:
                try:
                    ev = json.loads(s3.get_object(Bucket=PRIVATE, Key=k)["Body"].read() or b"{}")
                except Exception as exc:  # noqa: BLE001
                    r.warn("%s unreadable: %s" % (k, str(exc)[:120]))
                    continue
                kind = ev.get("kind") or k.rsplit("-", 1)[-1].replace(".json", "")
                kinds[kind] += 1
                t = ev.get("thresholds") or {}
                extra = ev.get("function") or ev.get("functions") or ev.get("over") or ev.get("engines") or ev.get("held") or ""
                r.log("%s %s | reason %s | mtd %s | counts %s | %s" % (
                    k.split("/")[-1][:28], kind, str(ev.get("reason") or ev.get("stage") or "")[:160], t.get("mtd"),
                    json.dumps(ev.get("counts") or {})[:120], json.dumps(extra, default=str)[:200]))
            facts["events_since_6727"] = json.dumps(dict(kinds), sort_keys=True)
            alerting = {k: v for k, v in kinds.items() if k not in ("fast-fuse-check", "meter-month-close")}
            if alerting:
                r.warn("alert-class event kinds since yesterday's run (each should have a governor-labelled issue): %s" % json.dumps(alerting))
            else:
                r.log("no alert-class event records since yesterday's run (only bookkeeping kinds, if any)")
        except Exception as exc:  # noqa: BLE001
            r.warn("event records unreadable: %s: %s" % (type(exc).__name__, str(exc)[:200]))
        try:
            mk = json.loads(s3.get_object(Bucket=PRIVATE, Key=RELAY_MARKER)["Body"].read() or b"{}")
            last_key = str(mk.get("last_key") or "")
            r.log("relay marker: %s" % json.dumps(mk, default=str)[:400])
            behind = [k for k in keys if k > last_key] if newest_key else []
            alert_behind = [k for k in behind if not any(s in k for s in ("fast-fuse-check", "meter-month-close"))]
            r.log("relay is behind the newest record by %d records (%d alert-class): %s" % (
                len(behind), len(alert_behind), ", ".join(k.split("/")[-1][:40] for k in alert_behind[:10])))
            facts["relay_alert_records_unrelayed"] = len(alert_behind)
            if alert_behind:
                r.warn("alert-class governor records not yet relayed to GitHub issues: %d (governor-alerts.yml cron '41 * * * *'; "
                       "GitHub has been running this repo's scheduled workflows hours late since Nov 1)" % len(alert_behind))
        except Exception as exc:  # noqa: BLE001
            r.warn("relay marker unreadable: %s: %s" % (type(exc).__name__, str(exc)[:160]))
        try:
            b = boto3.client("budgets", region_name="us-east-1", config=cfg).describe_budget(AccountId=ACCOUNT, BudgetName=BUDGET_NAME)["Budget"]
            cs = b.get("CalculatedSpend") or {}
            act = float(cs["ActualSpend"]["Amount"]) if cs.get("ActualSpend") else None
            fc = float(cs["ForecastedSpend"]["Amount"]) if cs.get("ForecastedSpend") else None
            r.log("describe_budget now: limit $%s actual $%s forecast $%s | LastUpdatedTime %s" % (
                b.get("BudgetLimit", {}).get("Amount"), act, fc, b.get("LastUpdatedTime")))
            facts["budget_actual_now"] = act
            facts["budget_last_updated"] = str(b.get("LastUpdatedTime"))
        except Exception as exc:  # noqa: BLE001 - the runner may not hold budgets:ViewBudget; the governor's own read is in A
            r.warn("describe_budget from the runner: %s: %s (the governor's own read is in section A)" % (type(exc).__name__, str(exc)[:160]))
        r.log("MTD sources: %s" % json.dumps(th.get("sources") or {}, sort_keys=True, default=str)[:400])
        sup = {day: rows.get("AWS Business Support+") for day, rows in svc.items() if isinstance(rows, dict) and rows.get("AWS Business Support+")}
        r.log("#138 - 'AWS Business Support+' by day in the governor's 14-day service record: %s" % (json.dumps(sup, sort_keys=True) or "{}"))

        r.section("D. freshness of declared outputs (HEAD, bucket %s) - %.0f h after the Nov 2 15:44 UTC restore" % (
            LIVE, (now - datetime(2026, 11, 2, 15, 44, tzinfo=timezone.utc)).total_seconds() / 3600))
        for key in ("_health/fleet.json", "_health/cadence-manifest.json", "data/_freshness-monitor.json"):
            try:
                h = s3.head_object(Bucket=LIVE, Key=key)
                age = (now - h["LastModified"]).total_seconds() / 3600
                r.log("%s: %.1f h old (%d B) -> %s" % (key, age, h["ContentLength"], tier(age)))
                facts[key] = round(age, 1)
            except Exception as exc:  # noqa: BLE001 - report, do not stop
                r.warn("%s: %s" % (key, str(exc)[:120]))
        manifest = json.loads((ROOT / "engine-manifest.json").read_text(encoding="utf-8"))
        pairs = sorted({(e["engine"], k) for e in manifest["engines"] for k in (e.get("keys") or []) if k.startswith("data/")})
        keys = sorted({k for _, k in pairs})
        r.log("manifest %s: %d engines, %d declared data/ keys" % (manifest.get("generated_at"), len({e for e, _ in pairs}), len(keys)))

        def head(key):
            try:
                h = s3.head_object(Bucket=LIVE, Key=key)
                return key, (now - h["LastModified"]).total_seconds() / 3600
            except Exception:  # noqa: BLE001 - missing objects are a finding, not a crash
                return key, None
        ages = {}
        with cf.ThreadPoolExecutor(max_workers=24) as ex:
            for key, age in ex.map(head, keys):
                ages[key] = age
        tiers = Counter(tier(a) for a in ages.values())
        r.log("outputs by tier: %s (yesterday: fresh 1053, stale 49, dead 208, missing 25)" % dict(tiers))
        facts.update(outputs=len(keys), **{"out_" + k: v for k, v in tiers.items()})
        owner = {}
        for e, k in pairs:
            owner.setdefault(k, []).append(e)
        by_engine = {}
        for e, k in pairs:
            by_engine.setdefault(e, []).append(ages.get(k))
        held_names = set(st.get("capped") or []) | {fn for fn, _ in (st.get("anomaly_held") or [])}
        # the declared schedule, where the cadence manifest knows it
        sched = {}
        try:
            cm = json.loads(s3.get_object(Bucket=LIVE, Key="_health/cadence-manifest.json")["Body"].read() or b"{}")
            rows = cm.get("engines") or cm.get("functions") or cm.get("triggers") or cm
            if isinstance(rows, dict):
                for name, v in rows.items():
                    if isinstance(v, dict):
                        sched[name] = str(v.get("schedule") or v.get("cron") or v.get("expression") or v.get("cadence") or "")[:40]
            elif isinstance(rows, list):
                for v in rows:
                    if isinstance(v, dict) and (v.get("function") or v.get("engine") or v.get("name")):
                        sched[v.get("function") or v.get("engine") or v.get("name")] = str(
                            v.get("schedule") or v.get("cron") or v.get("expression") or v.get("cadence") or "")[:40]
            r.log("cadence manifest: %d schedule entries readable" % len(sched))
        except Exception as exc:  # noqa: BLE001
            r.warn("cadence manifest unreadable: %s" % str(exc)[:120])
        two_slots = sorted(e for e, a in by_engine.items() if missed_two_slots(a))
        r.log("SILENT engines - ALL outputs >= %.0f h and < 7 d old (two daily slots missed, not dead; yesterday 18 at the "
              "88 h line): %d" % (TWO_SLOTS_H, len(two_slots)))
        for e in two_slots[:120]:
            a = [x for x in by_engine[e] if x is not None]
            r.log("  %s%s: %d output(s) %.0f-%.0f h | schedule %s" % (e.replace("justhodl-", ""), " [HELD]" if e in held_names else "",
                                                                   len(a), min(a), max(a), sched.get(e) or sched.get(e.replace("justhodl-", "")) or "?"))
        if len(two_slots) > 120:
            r.log("  ... and %d more" % (len(two_slots) - 120))
        facts["engines_missed_two_slots"] = len(two_slots)
        if two_slots:
            r.warn("%d engines have every declared output 54 h - 7 d old (held engines marked; nothing re-invoked; section F "
                   "probes them)" % len(two_slots))
        not_back = sorted(e for e, a in by_engine.items() if a and all(x is not None and 60 <= x < DEAD_H for x in a))
        r.log("engines with ALL outputs 60 h - 7 d old (yesterday 19): %d" % len(not_back))
        facts["engines_not_back_since_restore"] = len(not_back)
        fresh_engines = sorted(e for e, a in by_engine.items() if any(x is not None and x < FRESH_H for x in a))
        r.log("engines with at least one output < 30 h: %d of %d (yesterday 665 of 779)" % (len(fresh_engines), len(by_engine)))
        facts["engines_fresh"] = len(fresh_engines)
        stale_keys = sorted((k for k, a in ages.items() if a is not None and FRESH_H <= a < DEAD_H), key=lambda k: ages[k])
        r.log("STALE 30h-7d (%d), youngest 40: %s" % (len(stale_keys), ", ".join("%s %.0fh" % (k.replace("data/", ""), ages[k]) for k in stale_keys[:40])))
        dead = [k for k, a in ages.items() if a is not None and a >= DEAD_H]
        newly_dead = sorted((k for k in dead if ages[k] < DEAD_H + 24), key=lambda k: ages[k])
        oldest = sorted(dead, key=lambda k: -ages[k])[:15]
        r.log("DEAD >=7d (%d); crossed the line in the last day (%d): %s" % (len(dead), len(newly_dead), ", ".join(
            "%s %.0fh<-%s" % (k.replace("data/", ""), ages[k], ",".join(owner[k])[:30]) for k in newly_dead[:40])))
        facts["newly_dead"] = len(newly_dead)
        r.log("DEAD oldest: %s" % ", ".join("%s %.0fd" % (k.replace("data/", ""), ages[k] / 24) for k in oldest))
        missing = [k for k, a in ages.items() if a is None]
        r.log("MISSING objects (%d): %s" % (len(missing), ", ".join("%s<-%s" % (k.replace("data/", ""), ",".join(owner[k])[:40]) for k in missing[:40])))
        fresh = [k for k, a in ages.items() if a is not None and a < FRESH_H]
        r.log("FRESH (<30 h): %d of %d" % (len(fresh), len(keys)))
        r.log("engines born this week (13f-book Nov 3, strength Nov 4, leaders Nov 5):")
        for key in NEW_ENGINES:
            try:
                h = s3.head_object(Bucket=LIVE, Key=key)
                age = (now - h["LastModified"]).total_seconds() / 3600
                r.log("  %s: %.1f h old (%d B) -> %s" % (key, age, h["ContentLength"], tier(age)))
                facts[key.replace("data/", "").replace(".json", "") + "_age_h"] = round(age, 1)
            except Exception as exc:  # noqa: BLE001
                r.warn("  %s: %s" % (key, str(exc)[:100]))

        r.section("E. FMP bandwidth hold (walker + bank manifests)")
        now_ts = time.time()
        for key in FMP_MANIFESTS:
            try:
                obj = s3.get_object(Bucket=LIVE, Key=key)
                man = json.loads(obj["Body"].read() or b"{}")
                held, until_iso, note = hold_state(man, now_ts)
                age = (now - obj["LastModified"]).total_seconds() / 3600
                r.log("%s (%.1f h old): version %s phase %s key_status %s | hold %s until %s | %s" % (
                    key, age, man.get("version"), man.get("phase"), man.get("key_status"), "HELD" if held else "clear", until_iso, note))
                facts["fmp_hold_" + key.split("/")[2]] = "held until %s" % until_iso if held else "clear"
            except Exception as exc:  # noqa: BLE001
                r.warn("%s: %s" % (key, str(exc)[:160]))

        r.section("F. silent engines: triggers, invocations, errors, newest log tail (follow-up to ops 6728)")
        cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
        logs = boto3.client("logs", region_name=REGION, config=cfg)
        evb = boto3.client("events", region_name=REGION, config=cfg)
        sch = boto3.client("scheduler", region_name=REGION, config=cfg)
        probe = sorted(set(two_slots) | set(YESTERDAY))
        if len(probe) > PROBE_MAX:
            r.warn("%d engines to probe - only yesterday's twelve and the first %d silent ones are probed" % (len(probe), PROBE_MAX))
            probe = sorted(set(YESTERDAY) | set(sorted(two_slots)[:PROBE_MAX]))
        by_target = {}
        try:
            tok, n_sched = None, 0
            while True:
                kw = {"MaxResults": 100}
                if tok:
                    kw["NextToken"] = tok
                res = sch.list_schedules(**kw)
                for s_ in res.get("Schedules", []):
                    n_sched += 1
                    arn = (s_.get("Target") or {}).get("Arn") or ""
                    fn = arn.split(":function:")[-1].split(":")[0] if ":function:" in arn else ""
                    by_target.setdefault(fn, []).append((s_.get("Name"), s_.get("State"), s_.get("GroupName")))
                tok = res.get("NextToken")
                if not tok:
                    break
            r.log("%d Scheduler schedules listed, %d distinct target functions" % (n_sched, len(by_target)))
        except Exception as exc:  # noqa: BLE001
            r.warn("list_schedules: %s: %s" % (type(exc).__name__, str(exc)[:160]))
        names = sorted(set(probe) | {ALIASES[e] for e in probe if e in ALIASES})
        try:
            met = daily_metrics(cw, names, now - timedelta(days=4), now)
        except Exception as exc:  # noqa: BLE001
            met = {}
            r.warn("get_metric_data: %s: %s" % (type(exc).__name__, str(exc)[:160]))
        since_day = (now - timedelta(hours=48)).date().isoformat()
        verdicts = {}
        for eng in probe:
            eng_ages = [a for a in by_engine.get(eng, []) if a is not None]
            out_age = max(eng_ages, default=None)
            if eng in held_names:
                verdicts[eng] = "HELD"
                r.log("%s -> HELD by the governor (CAP/ANOM) - not probed further, never re-invoked" % eng.replace("justhodl-", ""))
                continue
            fn_name, fconf = None, None
            for c in [eng] + ([ALIASES[eng]] if eng in ALIASES else []):
                try:
                    fconf = lam.get_function_configuration(FunctionName=c)
                    fn_name = c
                    break
                except lam.exceptions.ResourceNotFoundException:
                    continue
                except Exception as exc:  # noqa: BLE001
                    r.warn("%s: get_function_configuration %s: %s" % (c, type(exc).__name__, str(exc)[:120]))
            if not fconf:
                verdicts[eng] = "NO_FUNCTION"
                r.log("%s -> NO_FUNCTION (nothing by that name in the account) | oldest output %s" % (
                    eng.replace("justhodl-", ""), "%.0f h" % out_age if out_age is not None else "none"))
                continue
            reserved = None
            try:
                reserved = lam.get_function_concurrency(FunctionName=fn_name).get("ReservedConcurrentExecutions")
            except Exception:  # noqa: BLE001
                pass
            triggers, enabled, due_daily = [], 0, False
            try:
                for rule in evb.list_rule_names_by_target(TargetArn=fconf["FunctionArn"]).get("RuleNames", []):
                    d = evb.describe_rule(Name=rule)
                    expr = d.get("ScheduleExpression") or ("event-pattern" if d.get("EventPattern") else "?")
                    triggers.append("rule %s %s %s" % (rule, d.get("State"), expr))
                    if d.get("State") == "ENABLED":
                        enabled += 1
                        due_daily = due_daily or daily_or_faster(expr)
            except Exception as exc:  # noqa: BLE001
                triggers.append("rules: %s" % str(exc)[:80])
            for sname, sstate, group in by_target.get(fn_name, []):
                try:
                    expr = sch.get_schedule(Name=sname, GroupName=group or "default").get("ScheduleExpression", "?")
                except Exception as exc:  # noqa: BLE001
                    expr = "?(%s)" % str(exc)[:40]
                triggers.append("sched %s %s %s" % (sname, sstate, expr))
                if sstate == "ENABLED":
                    enabled += 1
                    due_daily = due_daily or daily_or_faster(expr)
            days = met.get(fn_name) or {}
            inv = sum(v.get("n", 0) for d_, v in days.items() if d_ >= since_day)
            err = sum(v.get("e", 0) for d_, v in days.items() if d_ >= since_day)
            thr = sum(v.get("t", 0) for d_, v in days.items() if d_ >= since_day)
            series = ", ".join("%s n%d e%d t%d" % (d_[5:], v.get("n", 0), v.get("e", 0), v.get("t", 0)) for d_, v in sorted(days.items()))
            v = verdict(True, reserved, enabled, inv, err, thr, out_age, due_daily, TWO_SLOTS_H)
            verdicts[eng] = v
            r.log("%s%s -> %s (yesterday %s) | reserved %s | outputs %s h | triggers (%d enabled, daily-or-faster %s): %s" % (
                eng.replace("justhodl-", ""), "" if fn_name == eng else " (=%s)" % fn_name, v, YESTERDAY.get(eng, "-"), reserved,
                "/".join("%.0f" % a for a in sorted(eng_ages)) or "none", enabled, due_daily, "; ".join(triggers)[:420] or "NONE"))
            r.log("    metrics: %s" % (series or "no datapoints in 4 days"))
            if v in ("FAILING", "PARTLY_FAILING", "RAN_NO_WRITE", "THROTTLED"):
                try:
                    streams = logs.describe_log_streams(logGroupName="/aws/lambda/" + fn_name, orderBy="LastEventTime",
                                                        descending=True, limit=1).get("logStreams", [])
                    if not streams:
                        r.log("    log: (no log stream)")
                    else:
                        evs = logs.get_log_events(logGroupName="/aws/lambda/" + fn_name, logStreamName=streams[0]["logStreamName"],
                                                  limit=40, startFromHead=False).get("events", [])
                        keep = [e for e in evs if not e["message"].startswith(("START RequestId", "END RequestId", "INIT_START"))]
                        for e in keep[-(10 if v != "RAN_NO_WRITE" else 6):]:
                            stamp = datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).strftime("%m-%d %H:%M")
                            r.log("    log %s: %s" % (stamp, redact(" ".join(e["message"].split()))[:260]))
                except logs.exceptions.ResourceNotFoundException:
                    r.log("    log: (no log group)")
                except Exception as exc:  # noqa: BLE001
                    r.log("    log: %s: %s" % (type(exc).__name__, str(exc)[:100]))
        groups = {}
        for eng, v in verdicts.items():
            groups.setdefault(v, []).append(eng.replace("justhodl-", ""))
        for v, lst in sorted(groups.items()):
            r.log("%s (%d): %s" % (v, len(lst), ", ".join(sorted(lst))))
            facts["probe_" + v] = ", ".join(sorted(lst))
        recovered = sorted(e.replace("justhodl-", "") for e in YESTERDAY if verdicts.get(e) in ("RAN", "ON_TIME") and e not in two_slots)
        r.log("of yesterday's twelve, recovered (fresh output or on time): %s" % (", ".join(recovered) or "none"))
        facts["recovered_since_yesterday"] = ", ".join(recovered) or "none"
        broken = sum(len(lst) for v, lst in groups.items() if v in ("FAILING", "PARTLY_FAILING", "NEVER_STARTED", "THROTTLED", "RAN_NO_WRITE", "NO_FUNCTION"))
        facts["probe_broken_count"] = broken
        if broken:
            r.warn("%d probed engines are FAILING / PARTLY_FAILING / NEVER_STARTED / THROTTLED / RAN_NO_WRITE / NO_FUNCTION - nothing was changed here" % broken)

        r.section("G. source-refresh control (the two red scheduled workflows; read-only, no release, no retry)")
        try:
            import capital_structure_refresh as csr
            obj = s3.get_object(Bucket=csr.BUCKET, Key=csr.CONTROL)
            ctl = json.loads(obj["Body"].read() or b"{}")
            age = (now - obj["LastModified"]).total_seconds() / 3600
            fields = {k: ctl.get(k) for k in ("contract", "status", "request_id", "started_at", "failed_at", "error_type",
                                              "active_phase", "phase_started_at", "completed_phases", "planned_sources")}
            fields["parts"] = len(ctl.get("parts") or [])
            fields["review"] = {k: (ctl.get("review") or {}).get(k) for k in ("ops", "released_at", "failed_status")} if ctl.get("review") else None
            r.log("capital-structure refresh control (%.0f h since last write): %s" % (age, redact(json.dumps(fields, default=str))[:700]))
            facts["capital_structure_control"] = "%s since %s (phase %s)" % (ctl.get("status"), ctl.get("failed_at") or ctl.get("started_at"), ctl.get("active_phase"))
            if ctl.get("status") not in ("complete", "released"):
                r.warn("capital-structure refresh control is '%s': every scheduled plan stops at 'explicit review is required' until "
                       "release_failed_control is run with an ops number and a review note - a human decision, not this run's" % ctl.get("status"))
        except Exception as exc:  # noqa: BLE001
            r.warn("capital-structure refresh control unreadable: %s: %s" % (type(exc).__name__, redact(str(exc))[:160]))

        r.kv(**{k: str(v) for k, v in facts.items()})
        r.ok("read-only; nothing changed, nothing released")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
