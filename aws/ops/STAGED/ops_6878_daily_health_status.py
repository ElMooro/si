"""ops 6878 - READ-ONLY daily health status (scheduled daily health run, 2026-11-21, a Saturday 11 UTC; yesterday's run
(ops 6858, Friday, issue #159) found: stage NORMAL, MTD $177.85 on day 20, projection $275-297; CAP holds unchanged
(event-coordinator, miss-calibrator, ofrapi); the storage fuse advanced on its own to day 2026-11-19 (live partial list
empty); the FMP bandwidth hold cleared (both walker manifests key_status ok, no hold) and the first FMP consumers wrote
again; 19 engines FAILING, 9 PARTLY_FAILING, 16 RAN_NO_WRITE on probe; corporate-actions 113 of 200 due; fleet-error-monitor
1.3.0 output 03:30 (daily 03:39 slot, not hourly - yesterday's script warned wrongly). Overnight (Nov 20 15:30 - Nov 21
05:30 UTC) a build session shipped ops 6859-6877: split rule + factor book 1.0.9 (ops 6859/6860), the Volatility and
Strength desks (strength 1.1.0 writes data/strength-wide.json; strong.html v5; tearsheet kit 1.7.0 on every desk; phone
widths), the history book justhodl-bank-histories 1.2.0 (write-if-changed; deployed 04:47 UTC, first run 109,043 written /
210,051 left alone / 0 failed, a second pass wrote 0), ops 6870/6872/6874 (where November's money goes: S3 write fees 37%
of the month), ops 6876/6877 (the really-late feeds: FMP's 150 GB / 30-day bandwidth quota is the largest cause - fmp-ratios
314 of 314 refused, the 429s are back even after the Nov 20 06:43 hold expiry), and the backfill conductor change
5d56a595 (fmp-bank / fmp-walker get 0 extra kicks; today's 10:09 UTC conductor run is the first on it). This run reads
the governor, whether the walkers re-held, the history book's first scheduled day (its own state record), the dead-letter /
failing set by name, and the usual status.

  A. justhodl-spend-governor {"action":"status"}: version, mode, stage, cap, budget actual/forecast, thresholds, held /
     capped / anomaly lists, fuse, cadence sentinel, meter, storage record (last assessment, versioning, per-service
     daily spend);
  B. justhodl-spend-governor {"action":"caps"}: month-to-date top engines by metered Lambda cost;
  C. the governor's own event records since the last health run (ops 6858, 2026-11-20 11:22:10 UTC), the raw
     describe_budget read, and the GitHub-issues relay's marker (how far behind the newest record it is);
  D. freshness: HEAD every declared output in engine-manifest.json (bucket justhodl-dashboard-live), bucketed with the
     fleet-monitor tiers (<30 h fresh, <7 d stale, >=7 d dead); engines whose EVERY output is 54 h - 7 d old are named;
  E. FMP bandwidth hold: the walker's and the bank's manifests (clear at the Nov 20 run; ops 6877 saw 429s again at
     11:37 / 12:02 UTC after the conductor's 10:26 kick - re-held? and the conductor now gives them 0 kicks);
  F. follow-up on the Nov 20 finding (ops 6858: 19 FAILING, 9 PARTLY_FAILING, 16 RAN_NO_WRITE, 37 ON_TIME on weekly slots,
     ici-flows NO_TRIGGER by design) plus the 'own fault' dead-letter engines ops 6856 named (signal-genealogy,
     portfolio-snapshot, research-backtest, causality-scanner, dark-pool) and the intermittent ones (port-cargo, ecb-derived,
     feed-catalog, trade-nowcast, share-flows): for those and for the silent engines section D
     names (oldest output first, capped) - live triggers, per-day CloudWatch invocations / errors / throttles over 4 days, a
     verdict, and for the ones that fail the tail of their newest log stream. Log lines are redacted (key=value secrets, long
     opaque tokens) before they are written;
  G. the capital-structure refresh control's status fields, read only (no release, no retry);
  H. the factor book, leaders / strength desks (+ strength-wide.json, born Nov 20), Polygon EOD manifest, the un-stalled
     flow engines and corporate-actions after today's 06:30 UTC sweep (cursor 1229 yesterday, 113 of 200 due);
  I. justhodl-fleet-error-monitor 1.3.0's latest own output (data/_fleet-monitor.json): dead letters by engine, held
     refusals, DLQ depth, what it sent (daily 03:39 UTC slot);
  J. justhodl-bank-histories 1.2.0 - its own state record after the first scheduled day: last run's written / left alone /
     failed, phase, blocks pending (the Nov 21 first full day of the write-if-changed engine).
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
YESTERDAY_RUN = "20261120T112210"          # ops 6858 finished 2026-11-20T11:22:10Z (the last health run)
EVENT_DAYS = ("20261120", "20261121")
FRESH_H, DEAD_H = 30.0, 168.0
TWO_SLOTS_H = 54.0                         # past the 30 h fresh line plus one more daily slot
FMP_MANIFESTS = ("data/warm/fmp/manifest.json", "data/warm/fmp-bank/manifest.json")
NEW_ENGINES = ("data/13f-book.json", "data/strength.json", "data/strength-wide.json", "data/leaders.json", "data/bottom.json",    # born Nov 3 / 4 / 5; bottom 1.3.0 Nov 5; strength-wide Nov 20
               "data/stock-volatility.json", "data/campaign-scan.json",   # born Nov 19-20
               "data/etf-true-flows.json", "data/ici-flows.json", "data/warm/polygon-full/today-manifest.json")   # un-stalled / born Nov 10-11
EOD_MANIFEST = "data/warm/polygon-full/eod/manifest.json"
BOOK_MANIFEST = "data/warm/factor-book/manifest.json"
# the Nov 20 finding (ops 6858, issue #159): engine -> its probe verdict then (the FMP set, the DLQ set of ops 6856, the watched ones)
YESTERDAY = {
    "justhodl-bagger-engine": "FAILING", "justhodl-causality-scanner": "FAILING", "justhodl-chart-patterns": "FAILING", "justhodl-dark-pool": "FAILING",   # every run errored over 2 days (FMP 429 set + own faults)
    "justhodl-earnings-blackout": "FAILING", "justhodl-ecb-derived": "FAILING", "justhodl-estimate-revisions": "FAILING", "justhodl-feed-catalog": "FAILING",
    "justhodl-fmp-ratios": "FAILING", "justhodl-forensic-screen": "FAILING", "justhodl-hiring-velocity": "FAILING", "justhodl-investor-lenses": "FAILING",
    "justhodl-portfolio-snapshot": "FAILING", "justhodl-positioning-analog": "FAILING", "justhodl-predictability": "FAILING", "justhodl-research-backtest": "FAILING",
    "justhodl-share-flows": "FAILING", "justhodl-signal-genealogy": "FAILING", "justhodl-volatility-squeeze-hunter": "FAILING",
    "justhodl-earnings-pead": "PARTLY_FAILING", "justhodl-eps-revision-velocity": "PARTLY_FAILING", "justhodl-momentum-breakout": "PARTLY_FAILING", "justhodl-momentum-leaders": "PARTLY_FAILING",   # some runs errored
    "justhodl-port-cargo": "PARTLY_FAILING", "justhodl-revenue-acceleration": "PARTLY_FAILING", "justhodl-sector-flow-state": "PARTLY_FAILING", "justhodl-stock-screener": "PARTLY_FAILING",
    "justhodl-trade-nowcast": "PARTLY_FAILING",
    "justhodl-auction-crisis-detector": "RAN", "justhodl-corporate-actions": "RAN", "justhodl-crypto-emergence": "RAN", "justhodl-forward-orders": "RAN",   # ran and wrote
    "justhodl-insider-trades": "RAN", "justhodl-portwatch": "RAN",
    "justhodl-china-liquidity": "RAN_NO_WRITE", "justhodl-cross-asset-regime": "RAN_NO_WRITE", "justhodl-earnings-iv-crush": "RAN_NO_WRITE", "justhodl-earnings-quality": "RAN_NO_WRITE",   # ran, output still > 54 h
    "justhodl-equity-ftd": "RAN_NO_WRITE", "justhodl-etf-true-flows": "RAN_NO_WRITE", "justhodl-fundamental-graphs": "RAN_NO_WRITE", "justhodl-gf-value": "RAN_NO_WRITE",
    "justhodl-gold-equity-rotation": "RAN_NO_WRITE", "justhodl-leading-markets": "RAN_NO_WRITE", "justhodl-russell-recon-frontrun": "RAN_NO_WRITE", "justhodl-sec-10kq": "RAN_NO_WRITE",
    "justhodl-sec-8k-enrich": "RAN_NO_WRITE", "justhodl-theme-rotation-engine": "RAN_NO_WRITE", "justhodl-vol-target-unwind": "RAN_NO_WRITE",
    "fmp-fundamentals-agent": "RAN_NO_WRITE",
    "justhodl-ici-flows": "NO_TRIGGER",   # Lambda rule disabled by design; the Chrome lane writes it (ran Nov 20 22:20 UTC)
    "justhodl-imf-full": "ON_TIME",       # weekly; Nov 16 timed out 850 s x3; next slot Nov 23
}
# not an FMP-bandwidth case: read their whole last invocation, not just the tail
DEEP = {"justhodl-corporate-actions", "justhodl-etf-true-flows", "justhodl-sector-flow-state", "justhodl-insider-trades",
        "justhodl-auction-crisis-detector", "justhodl-portwatch", "justhodl-sec-8k-enrich", "justhodl-sec-10kq",
        "justhodl-signal-genealogy", "justhodl-portfolio-snapshot", "justhodl-research-backtest", "justhodl-causality-scanner",
        "justhodl-dark-pool"}
MONITOR_KEY = "data/_fleet-monitor.json"
HISTORY_STATE = "data/warm/history-book/_state/state.json"
HISTORY_MANIFEST = "data/warm/history-book/manifest.json"
CORPACT_KEYS = ("data/corporate-actions/_state.json", "data/corporate-actions-index.json")
# the manifest attributes some outputs to a differently named producer (ops 6728)
ALIASES = {"justhodl-momentum-breakout": "justhodl-momentum-leaders", "justhodl-naaim": "justhodl-put-call-extreme",
           "justhodl-eps-revision-velocity": "justhodl-nobrainer-rationale"}
PROBE_MAX = 60
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
    assert event_after(EVENTS_PREFIX + "20261121T080737.868478Z-error-storm.json", YESTERDAY_RUN) is True
    assert event_after(EVENTS_PREFIX + "20261120T080737.868478Z-error-storm.json", YESTERDAY_RUN) is False
    assert event_after(EVENTS_PREFIX + "20261120T112300.000001Z-x.json", YESTERDAY_RUN) is True
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
    with report("ops_6878_daily_health_status") as r:
        r.heading("ops 6878 - daily health status %s (governor status + caps + storage_selftest, event records since ops 6858 on Nov 20, relay lag, output "
                  "freshness, FMP bandwidth hold after the conductor change, follow-up probe of the silent engines + the dead-letter engines ops 6856 named, source-refresh control, "
                  "desks + corporate-actions, fleet-error-monitor 1.3.0 output, history book 1.2.0 first scheduled day; Saturday; read-only)" % now.date())

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
                   "the Nov 20 run - a non-NORMAL stage now is new)" % (st.get("stage"), now.day, held_total))
        if str(sto.get("day") or "") <= "2026-11-19":
            r.warn("storage fuse has not assessed a day past %s (it was 2026-11-19 at the Nov 20 run; Nov 17-19 was stuck at 2026-11-17 "
                   "until the richer days aged out of the 6-day window) - the read-only storage_selftest below lists the live partial reasons" % sto.get("day"))
        else:
            r.log("storage fuse advanced: day %s (was 2026-11-19 at the Nov 20 run)" % sto.get("day"))
        try:   # read-only: pure scenarios + a live read of the bucket's daily metrics and the versioning status; records nothing
            resp = lam.invoke(FunctionName=GOVERNOR, InvocationType="RequestResponse", Payload=b'{"action":"storage_selftest"}')
            sst = json.loads(resp["Payload"].read() or b"{}")
            live = sst.get("live") or {}
            r.log("storage_selftest ok %s | scenarios %s | live versioning %s | live error %s" % (
                sst.get("ok"), json.dumps(sst.get("scenarios")), live.get("versioning"), live.get("error")))
            r.log("storage_selftest live days: %s" % json.dumps(live.get("days") or {}, sort_keys=True)[:700])
            r.log("storage_selftest live partial: %s" % json.dumps(live.get("partial") or {}, sort_keys=True)[:700])
            facts["storage_live_partial_days"] = ",".join(sorted(live.get("partial") or {})) or "none"
            facts["storage_live_versioning"] = live.get("versioning")
            if live.get("partial"):
                r.warn("storage_selftest: %d of the last 6 days read as partial (%s) - the daily storage check will keep waiting until a day is complete" % (
                    len(live["partial"]), "; ".join("%s: %s" % (d, v) for d, v in sorted(live["partial"].items()))[:500]))
        except Exception as exc:  # noqa: BLE001
            r.warn("storage_selftest invoke: %s: %s" % (type(exc).__name__, str(exc)[:200]))
        if st.get("version") != "1.6.1":
            r.warn("governor version %s - 1.6.1 (deployed 2026-11-03 06:27 UTC) expected; a newer version means a deploy this "
                   "run did not know about" % st.get("version"))
        if st.get("anomaly_held"):
            r.warn("ANOM holds present (6 h cooldown each; a third hold in a month becomes CAP): %s" % st.get("anomaly_held"))
        capped_now = sorted(st.get("capped") or [])
        if capped_now != ["justhodl-event-coordinator", "justhodl-miss-calibrator", "ofrapi"]:
            r.warn("CAP list changed since Nov 20 (was event-coordinator, miss-calibrator, ofrapi): %s" % capped_now)

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

        r.section("C. governor event records since ops 6858 (2026-11-20 11:22 UTC), the raw budget read, the relay's marker")
        kinds = Counter()
        newest_key = None
        keys = []
        try:
            for day in EVENT_DAYS:
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
            r.log("%d event records on Nov 20-21; %d since ops 6858 (newest %s)" % (len(keys), len(since), (newest_key or "-").split("/")[-1]))
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
            facts["events_since_6858"] = json.dumps(dict(kinds), sort_keys=True)
            alerting = {k: v for k, v in kinds.items() if k not in ("fast-fuse-check", "meter-month-close")}
            if alerting:
                r.warn("alert-class event kinds since the Nov 20 run (each should have a governor-labelled issue): %s" % json.dumps(alerting))
            else:
                r.log("no alert-class event records since the Nov 20 run (only bookkeeping kinds, if any)")
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
        r.log("outputs by tier: %s (Friday Nov 20, ops 6858: fresh 1064, stale 71, dead 187, missing 24; Thursday Nov 19: fresh 1060, stale 71, dead 189, missing 24)" % dict(tiers))
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
        r.log("SILENT engines - ALL outputs >= %.0f h and < 7 d old (two daily slots missed, not dead; Friday Nov 20: 38, 37 on weekly/fortnightly slots; Thursday Nov 19: 40): %d" % (
            TWO_SLOTS_H, len(two_slots)))
        two_slots_by_age = sorted(two_slots, key=lambda e: -min(x for x in by_engine[e] if x is not None))   # oldest first
        for e in two_slots_by_age[:120]:
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
        r.log("engines with ALL outputs 60 h - 7 d old (Nov 20: see ops 6858; Nov 19: 39): %d" % len(not_back))
        facts["engines_not_back_since_restore"] = len(not_back)
        fresh_engines = sorted(e for e, a in by_engine.items() if any(x is not None and x < FRESH_H for x in a))
        r.log("engines with at least one output < 30 h: %d of %d (Friday Nov 20: 668 of 784; Thursday Nov 19: 663 of 782)" % (len(fresh_engines), len(by_engine)))
        facts["engines_fresh"] = len(fresh_engines)
        stale_keys = sorted((k for k, a in ages.items() if a is not None and FRESH_H <= a < DEAD_H), key=lambda k: ages[k])
        r.log("STALE 30h-7d (%d), youngest 40: %s" % (len(stale_keys), ", ".join("%s %.0fh" % (k.replace("data/", ""), ages[k]) for k in stale_keys[:40])))
        dead = [k for k, a in ages.items() if a is not None and a >= DEAD_H]
        newly_dead = sorted((k for k in dead if ages[k] < DEAD_H + 24.1), key=lambda k: ages[k])   # one day since the last run
        oldest = sorted(dead, key=lambda k: -ages[k])[:15]
        r.log("DEAD >=7d (%d); crossed the line since the Nov 20 run (%d): %s" % (len(dead), len(newly_dead), ", ".join(
            "%s %.0fh<-%s" % (k.replace("data/", ""), ages[k], ",".join(owner[k])[:30]) for k in newly_dead[:40])))
        facts["newly_dead"] = len(newly_dead)
        r.log("DEAD oldest: %s" % ", ".join("%s %.0fd" % (k.replace("data/", ""), ages[k] / 24) for k in oldest))
        missing = [k for k, a in ages.items() if a is None]
        r.log("MISSING objects (%d): %s" % (len(missing), ", ".join("%s<-%s" % (k.replace("data/", ""), ",".join(owner[k])[:40]) for k in missing[:40])))
        fresh = [k for k, a in ages.items() if a is not None and a < FRESH_H]
        r.log("FRESH (<30 h): %d of %d" % (len(fresh), len(keys)))
        r.log("engines born last fortnight + un-stalled Nov 10-11 (13f-book Nov 3, strength Nov 4 + strength-wide Nov 20, leaders Nov 5, bottom 1.3.0 Nov 5, stock-volatility + campaign-scan Nov 19-20; etf-true-flows 3.0.1 + ici-flows lane + polygon-today Nov 10-11):")
        for key in NEW_ENGINES:
            try:
                h = s3.head_object(Bucket=LIVE, Key=key)
                age = (now - h["LastModified"]).total_seconds() / 3600
                r.log("  %s: %.1f h old (%d B) -> %s" % (key, age, h["ContentLength"], tier(age)))
                facts[key.replace("data/", "").replace(".json", "") + "_age_h"] = round(age, 1)
            except Exception as exc:  # noqa: BLE001
                r.warn("  %s: %s" % (key, str(exc)[:100]))

        r.section("E. FMP bandwidth hold (walker + bank manifests; clear at the Nov 20 11:21 UTC read; ops 6877 saw 429s at 11:37 / 12:02 after the conductor kick; conductor now gives them 0 kicks)")
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

        r.section("F. silent + dead-letter engines: triggers, invocations, errors, newest log tail (follow-up to ops 6858 / 6856 / 6877)")
        cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
        logs = boto3.client("logs", region_name=REGION, config=cfg)
        evb = boto3.client("events", region_name=REGION, config=cfg)
        sch = boto3.client("scheduler", region_name=REGION, config=cfg)
        probe = sorted(set(two_slots) | set(YESTERDAY))
        if len(probe) > PROBE_MAX:
            r.warn("%d engines to probe - only the Nov 20 / DLQ set and the %d silent ones with the oldest output are probed" % (len(probe), PROBE_MAX))
            probe = sorted(set(YESTERDAY) | set(two_slots_by_age[:PROBE_MAX]))
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
            r.log("%s%s -> %s (Nov 20 %s) | reserved %s | outputs %s h | triggers (%d enabled, daily-or-faster %s): %s" % (
                eng.replace("justhodl-", ""), "" if fn_name == eng else " (=%s)" % fn_name, v, YESTERDAY.get(eng, "-"), reserved,
                "/".join("%.0f" % a for a in sorted(eng_ages)) or "none", enabled, due_daily, "; ".join(triggers)[:420] or "NONE"))
            r.log("    metrics: %s" % (series or "no datapoints in 4 days"))
            if v in ("FAILING", "PARTLY_FAILING", "RAN_NO_WRITE", "THROTTLED") or eng in DEEP:
                try:
                    streams = logs.describe_log_streams(logGroupName="/aws/lambda/" + fn_name, orderBy="LastEventTime",
                                                        descending=True, limit=1).get("logStreams", [])
                    if not streams:
                        r.log("    log: (no log stream)")
                    else:
                        deep = eng in DEEP
                        evs = logs.get_log_events(logGroupName="/aws/lambda/" + fn_name, logStreamName=streams[0]["logStreamName"],
                                                  limit=200 if deep else 40, startFromHead=False).get("events", [])
                        if deep:   # the whole last invocation: everything after the last START line
                            starts = [i for i, e in enumerate(evs) if e["message"].startswith("START RequestId")]
                            evs = evs[starts[-1]:] if starts else evs
                        keep = [e for e in evs if not e["message"].startswith(("START RequestId", "END RequestId", "INIT_START"))]
                        if deep:   # one line per expected 403 is noise (Nov 13: 28 identical lines hid the 'done' summary) - count them, keep the rest
                            n403 = sum(1 for e in keep if "HTTPError 403" in e["message"])
                            keep = [e for e in keep if "HTTPError 403" not in e["message"]]
                            if n403:
                                r.log("    log: %d x 'http failed ... HTTPError 403' lines in the last invocation (pre-2021 dividends, plan_history_403)" % n403)
                        for e in keep[-(30 if deep else 10 if v != "RAN_NO_WRITE" else 6):]:
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
        r.log("of the Nov 20 / DLQ set, recovered (fresh output or on time): %s" % (", ".join(recovered) or "none"))
        facts["recovered_since_nov20"] = ", ".join(recovered) or "none"
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


        r.section("H. strength / leaders / factor book, the Polygon EOD manifest, the un-stalled flow engines, corporate-actions after today's 06:30 UTC sweep (read-only; Saturday - last_session should be 2026-11-20)")
        try:
            obj = s3.get_object(Bucket=LIVE, Key=BOOK_MANIFEST)
            man = json.loads(obj["Body"].read() or b"{}")
            ret = (man.get("sources") or {}).get("returns") or {}
            tape = (man.get("as_of") or {}).get("polygon-tape")
            r.log("factor book manifest (%.1f h old): version %s generated %s | as_of polygon-tape %s | returns coverage %s | sessions %s last_session %s symbols %s thin_floor %s thin_skipped %s" % (
                (now - obj["LastModified"]).total_seconds() / 3600, man.get("version"), man.get("generated_at") or man.get("updated_at"), tape,
                (man.get("coverage") or {}).get("returns"), ret.get("sessions"), ret.get("last_session"), ret.get("symbols"), ret.get("thin_floor"),
                json.dumps(ret.get("thin_skipped"), default=str)[:300]))
            facts.update(book_version=man.get("version"), book_tape=tape, book_returns=(man.get("coverage") or {}).get("returns"),
                         book_thin_skipped=json.dumps(ret.get("thin_skipped"), default=str)[:200])
            cov = (man.get("coverage") or {}).get("returns")
            if isinstance(cov, (int, float)) and cov < 5000:
                r.warn("factor book returns coverage %s - under 5,000 (12,063 after the Nov 8 heal): the thin-session gate did not hold" % cov)
        except Exception as exc:  # noqa: BLE001
            r.warn("factor book manifest unreadable: %s: %s" % (type(exc).__name__, str(exc)[:160]))
        try:
            obj = s3.get_object(Bucket=LIVE, Key=EOD_MANIFEST)
            man = json.loads(obj["Body"].read() or b"{}")
            r.log("polygon EOD manifest (%.1f h old): last_session %s first_session %s tickers %s updated_at %s" % (
                (now - obj["LastModified"]).total_seconds() / 3600, man.get("last_session"), man.get("first_session"), man.get("tickers"), man.get("updated_at")))
            facts["eod_last_session"] = man.get("last_session")
            ls = str(man.get("last_session") or "")
            try:
                if datetime.strptime(ls, "%Y-%m-%d").weekday() >= 5:
                    r.warn("polygon EOD manifest's last_session %s is a weekend day - the source-side Saturday file (Nov 8 finding) still labels the bank after Wednesday's fold" % ls)
            except ValueError:
                pass
        except Exception as exc:  # noqa: BLE001
            r.warn("polygon EOD manifest unreadable: %s: %s" % (type(exc).__name__, str(exc)[:160]))
        for key, label in (("data/leaders.json", "leaders"), ("data/strength.json", "strength")):
            try:
                obj = s3.get_object(Bucket=LIVE, Key=key)
                doc = json.loads(obj["Body"].read() or b"{}")
                uni = doc.get("universe") or {}
                n_rows = len(doc.get("rows") or [])
                n_ranked = uni.get("n_ranked") if label == "strength" else None
                r.log("%s (%.1f h old): version %s generated %s | as_of %s | rows %d%s | industries %s | funds %s | bank last_session %s | errors %s" % (
                    key, (now - obj["LastModified"]).total_seconds() / 3600, doc.get("version"), doc.get("generated_at"), doc.get("as_of"), n_rows,
                    " ranked %s" % n_ranked if label == "strength" else "", len(doc.get("industries") or {}) if label == "leaders" else "-",
                    len(doc.get("etfs") or []) if label == "leaders" else "-", (doc.get("bank") or {}).get("last_session"), doc.get("n_errors")))
                facts.update(**{label + "_as_of": doc.get("as_of"), label + "_rows": n_rows, label + "_generated": doc.get("generated_at")})
                try:
                    if datetime.strptime(str(doc.get("as_of") or ""), "%Y-%m-%d").weekday() >= 5:
                        r.warn("%s desk is labelled as of %s, a weekend day" % (label, doc.get("as_of")))
                except ValueError:
                    pass
                if label == "strength":
                    facts["strength_ranked"] = n_ranked
                    facts["strength_thin_days"] = json.dumps((doc.get("bank") or {}).get("thin_days"), default=str)[:160]
                    r.log("  strength bank: manifest_last_session %s thin_days %s" % ((doc.get("bank") or {}).get("manifest_last_session"), facts["strength_thin_days"]))
                    if isinstance(n_ranked, int) and n_ranked < 200:
                        r.warn("strength ranked only %s names (Nov 20 03:40: 825 of 856; Nov 19: 825 of 856)" % n_ranked)
                if label == "leaders" and n_rows < 500:
                    r.warn("leaders desk carries %d leaders (670 after the Nov 8 heal)" % n_rows)
            except Exception as exc:  # noqa: BLE001
                r.warn("%s unreadable: %s: %s" % (key, type(exc).__name__, str(exc)[:160]))

        r.log("the Nov 10-11 un-stalls (etf-true-flows 3.1.0, weekday 15:45 UTC slot - Saturday 11 UTC is ~19 h after Friday's slot; ICI Chrome lane ran Nov 20 22:20 UTC, run 44828161999 green - ici-flows.json should be < 14 h; polygon-today 1.0.0 - last_session should be 2026-11-20):")
        for key, label in (("data/etf-true-flows.json", "etf_true_flows"), ("data/ici-flows.json", "ici_flows"),
                           ("data/warm/polygon-full/today-manifest.json", "polygon_today")):
            try:
                obj = s3.get_object(Bucket=LIVE, Key=key)
                doc = json.loads(obj["Body"].read() or b"{}")
                age = (now - obj["LastModified"]).total_seconds() / 3600
                head_ = {k: doc.get(k) for k in ("version", "generated_at", "updated_at", "as_of", "session", "last_session", "week_ending",
                                                 "source", "transport", "n_errors", "degraded", "status") if doc.get(k) is not None}
                sizes = {k: len(v) for k, v in doc.items() if isinstance(v, (list, dict)) and k not in ("notes",)}
                r.log("  %s (%.1f h old, %d B): %s | containers %s" % (key, age, obj["ContentLength"], redact(json.dumps(head_, default=str))[:360],
                                                                        json.dumps(dict(sorted(sizes.items(), key=lambda kv: -kv[1])[:8]))))
                facts[label + "_age_h"] = round(age, 1)
                facts[label + "_as_of"] = doc.get("as_of") or doc.get("session") or doc.get("last_session") or doc.get("week_ending") or doc.get("generated_at")
                if doc.get("degraded"):
                    r.warn("  %s carries a non-empty degraded field: %s" % (key, redact(json.dumps(doc.get("degraded"), default=str))[:300]))
            except Exception as exc:  # noqa: BLE001
                r.warn("  %s unreadable: %s: %s" % (key, type(exc).__name__, str(exc)[:160]))

        r.log("corporate-actions after today's 06:30 UTC sweep (yesterday's 06:30 sweep: 113 of 200 due in 841 s, cursor 1116 -> 1229):")
        for key in CORPACT_KEYS:
            try:
                obj = s3.get_object(Bucket=LIVE, Key=key)
                doc = json.loads(obj["Body"].read() or b"{}")
                age = (now - obj["LastModified"]).total_seconds() / 3600
                head_ = {k: doc.get(k) for k in ("version", "generated_at", "updated_at", "as_of", "run_at", "stats", "last_run", "cursor",
                                                 "window", "due", "processed", "written", "failed", "factor_gaps", "stopped_early", "duration_s") if doc.get(k) is not None}
                sizes = {k: len(v) for k, v in doc.items() if isinstance(v, (list, dict))}
                r.log("  %s (%.1f h old, %d B): %s | containers %s" % (key, age, obj["ContentLength"], redact(json.dumps(head_, default=str))[:700],
                                                                        json.dumps(dict(sorted(sizes.items(), key=lambda kv: -kv[1])[:8]))))
                facts["corpact_" + key.split("/")[-1].replace(".json", "").strip("_") + "_age_h"] = round(age, 1)
                if key.endswith("_state.json"):
                    facts["corpact_cursor"] = doc.get("cursor")
                    if doc.get("cursor") in (0, 1229):
                        r.warn("  corporate-actions cursor is %s - yesterday it was 1229 after the 06:30 sweep; today's sweep should have moved it "
                               "(0 means a manual reset or a wrap at the end of the universe)" % doc.get("cursor"))
                if age > 30:
                    r.warn("  %s is %.0f h old - the 06:30 UTC run did not write it (section F has its last invocation)" % (key, age))
            except Exception as exc:  # noqa: BLE001
                r.warn("  %s unreadable: %s: %s" % (key, type(exc).__name__, str(exc)[:160]))
        # the sweep's own summary line ('[corpact] done: {...}') from today's 06:30 UTC invocation - processed vs due is the test
        try:
            streams = logs.describe_log_streams(logGroupName="/aws/lambda/justhodl-corporate-actions", orderBy="LastEventTime",
                                                descending=True, limit=3).get("logStreams", [])
            done = None
            for s_ in streams:
                for e in logs.get_log_events(logGroupName="/aws/lambda/justhodl-corporate-actions", logStreamName=s_["logStreamName"],
                                             limit=400, startFromHead=False).get("events", []):
                    if "[corpact] done:" in e["message"]:
                        done = (e["timestamp"], e["message"])
                if done:
                    break
            if done:
                stamp = datetime.fromtimestamp(done[0] / 1000, timezone.utc).isoformat(timespec="minutes")
                body = done[1].split("done:", 1)[1].strip()
                r.log("  last sweep summary %s: %s" % (stamp, redact(body)[:400]))
                try:
                    st_ = json.loads(body)
                    facts.update(corpact_due=st_.get("due"), corpact_processed=st_.get("processed"), corpact_duration_s=st_.get("duration_s"),
                                 corpact_stopped_early=st_.get("stopped_early"), corpact_failed=st_.get("failed"), corpact_sweep_at=stamp)
                    due, proc = int(st_.get("due") or 0), int(st_.get("processed") or 0)
                    if (now - datetime.fromtimestamp(done[0] / 1000, timezone.utc)).total_seconds() > 30 * 3600:
                        r.warn("  the newest '[corpact] done' line is from %s - today's 06:30 UTC sweep left no summary" % stamp)
                    elif due and proc < due * 0.8:
                        r.warn("  today's sweep processed %d of %d due tickers (yesterday 113 of 200 at 841 s, Nov 19 107 of 200, Nov 18 110 of 200) - the 900 s budget did not clear the window; "
                               "stopped_early %s, duration %s s" % (proc, due, st_.get("stopped_early"), st_.get("duration_s")))
                    else:
                        r.log("  today's sweep processed %d of %d due tickers in %s s (stopped_early %s, failed %s)" % (
                            proc, due, st_.get("duration_s"), st_.get("stopped_early"), st_.get("failed")))
                except (ValueError, TypeError) as exc:
                    r.warn("  sweep summary not parseable: %s" % str(exc)[:100])
            else:
                r.warn("  no '[corpact] done:' line in the three newest log streams")
        except Exception as exc:  # noqa: BLE001
            r.warn("  corporate-actions log read: %s: %s" % (type(exc).__name__, str(exc)[:160]))

        r.section("I. justhodl-fleet-error-monitor 1.3.0 - its latest own output (read-only)")
        try:
            obj = s3.get_object(Bucket=LIVE, Key=MONITOR_KEY)
            doc = json.loads(obj["Body"].read() or b"{}")
            age = (now - obj["LastModified"]).total_seconds() / 3600
            head_ = {k: doc.get(k) for k in ("version", "generated_at", "n_lambdas_scanned", "n_alerts_detected", "n_alerts_raised",
                                             "n_alerts_suppressed", "sns_sent", "telegram_sent", "notification_mode", "elapsed_s") if doc.get(k) is not None}
            r.log("%s (%.1f h old, %d B): %s" % (MONITOR_KEY, age, obj["ContentLength"], redact(json.dumps(head_, default=str))[:500]))
            dlq = doc.get("dlq_status") or {}
            r.log("  dlq_status: %s" % json.dumps(dlq, default=str)[:300])
            alerts = doc.get("alerts") or []
            n_dl_fail, n_dl_eng = 0, 0
            for a in (alerts if isinstance(alerts, list) else [alerts])[:12]:
                dl = a.get("dead_letters") if isinstance(a, dict) else None
                if isinstance(dl, dict):
                    failing = dl.get("failing") or {}
                    n_dl_fail += sum(int(v) for v in failing.values()) if isinstance(failing, dict) else 0
                    n_dl_eng += len(failing)
                    r.log("  alert %s %s: dead letters from %d running engines (%s) | held refusals %s" % (
                        a.get("lambda"), a.get("severity"), len(failing), ", ".join("%s %s" % (k.replace("justhodl-", ""), v) for k, v in sorted(failing.items()))[:600],
                        json.dumps(dl.get("held") or {}, default=str)[:200]))
                else:
                    r.log("  alert: %s" % redact(json.dumps(a, default=str))[:400])
            facts["monitor_version"] = doc.get("version")
            facts["monitor_age_h"] = round(age, 1)
            facts["monitor_dlq_depth"] = dlq.get("visible") if isinstance(dlq, dict) else None
            facts["monitor_dead_letter_engines"] = n_dl_eng
            facts["monitor_dead_letters"] = n_dl_fail
            if doc.get("telegram_sent") is False and doc.get("n_alerts_raised"):
                r.log("  telegram_sent False with alerts raised - Telegram delivery from the monitor is still dead (known, #158/#159); SNS e-mail is the channel")
            if str(doc.get("version") or "") != "1.3.0":
                r.warn("  monitor output version %s - 1.3.0 (deployed 2026-11-20 03:27 UTC) expected" % doc.get("version"))
            if age > 30.0:
                r.warn("  monitor output is %.1f h old - its daily 03:39 UTC slot (config.json) should have rewritten it" % age)
            else:
                r.log("  monitor output %.1f h old - within its daily 03:39 UTC slot (yesterday's script warned 'hourly' by mistake)" % age)
        except Exception as exc:  # noqa: BLE001
            r.warn("%s unreadable: %s: %s" % (MONITOR_KEY, type(exc).__name__, str(exc)[:160]))

        r.section("J. justhodl-bank-histories 1.2.0 - the first scheduled day of write-if-changed (its own state record; read-only)")
        try:
            obj = s3.get_object(Bucket=LIVE, Key=HISTORY_STATE)
            doc = json.loads(obj["Body"].read() or b"{}")
            age = (now - obj["LastModified"]).total_seconds() / 3600
            last = doc.get("last_run") or {}
            head_ = {k: doc.get(k) for k in ("version", "phase", "written_at", "updated_at", "as_of") if doc.get(k) is not None}
            lr = {k: last.get(k) for k in ("version", "started_at", "finished_at", "seconds", "puts", "written", "unchanged", "left_alone",
                                           "failed", "confirmed_unread", "blocks", "blocks_read", "phase", "notes") if last.get(k) is not None}
            pending = doc.get("pending") if isinstance(doc.get("pending"), list) else [k for k, v in (doc.get("blocks") or {}).items()
                                                                                        if isinstance(v, dict) and v.get("status") == "pending"]
            r.log("%s (%.1f h old, %d B): %s | last_run %s | pending blocks %d" % (HISTORY_STATE, age, obj["ContentLength"],
                  redact(json.dumps(head_, default=str))[:300], redact(json.dumps(lr, default=str))[:700], len(pending or [])))
            facts["history_book_version"] = doc.get("version") or last.get("version")
            facts["history_book_last_run"] = redact(json.dumps(lr, default=str))[:300]
            if str(doc.get("version") or last.get("version") or "") not in ("1.2.0",):
                r.warn("  history book state says version %s - 1.2.0 (deployed 2026-11-21 04:47 UTC) expected" % (doc.get("version") or last.get("version")))
            wr = last.get("puts") if last.get("puts") is not None else last.get("written")
            if isinstance(wr, (int, float)) and wr > 150000:
                r.warn("  the last run wrote %s documents - more than the 109,043 of the first 1.2.0 pass; the write-if-changed hash may not be holding" % wr)
        except Exception as exc:  # noqa: BLE001
            r.warn("%s unreadable: %s: %s" % (HISTORY_STATE, type(exc).__name__, str(exc)[:160]))
        try:
            obj = s3.get_object(Bucket=LIVE, Key=HISTORY_MANIFEST)
            man = json.loads(obj["Body"].read() or b"{}")
            age = (now - obj["LastModified"]).total_seconds() / 3600
            head_ = {k: man.get(k) for k in ("version", "phase", "as_of", "updated_at", "generated_at", "documents", "blocks", "last_run") if man.get(k) is not None}
            r.log("%s (%.1f h old): %s" % (HISTORY_MANIFEST, age, redact(json.dumps(head_, default=str))[:600]))
            facts["history_book_manifest_version"] = man.get("version")
        except Exception as exc:  # noqa: BLE001
            r.warn("%s unreadable: %s: %s" % (HISTORY_MANIFEST, type(exc).__name__, str(exc)[:160]))

        r.kv(**{k: str(v) for k, v in facts.items()})
        r.ok("read-only; nothing changed, nothing released")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
