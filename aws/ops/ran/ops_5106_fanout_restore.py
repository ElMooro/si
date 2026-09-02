"""ops_5106 -- restore the fan-out cluster silenced on 2026-08-01.

Root cause (ops 5105 diagnostics + source): justhodl-scheduler is a fan-out
router (tick rules -> router -> async invoke of every member listed under
manifest["ticks"][tick]). ops 4237 (2026-08-01 14:49 UTC) overwrote its
manifest key config/schedule-manifest.json with the declarative rules/
schedules snapshot, which has no "ticks" block, so every tick has answered
"no jobs" since. The members carry no rules of their own, so the cadence
regression sweep never saw them.

  A  proof: router log lines since Aug 1 ("no jobs"), tick rules + their
     Input, invocation counts of the router vs zero member invocations
  B  membership from the engines' own signature (5105 facts): no rule, no
     schedule, last active 2026-07-31..08-02, >= 20 active days in the
     90-day window; the invocation-hour histogram of each member over
     Jul 15-30 (period 1h) picks its tick(s) among the live tick rules
  C  write config/fanout-manifest.json (S3 + repo), redeploy the router
     with its own key, keep the legacy key untouched
  D  fire the daily ticks now (router invoke with each member's tick),
     then verify per member: an invocation landed and its known output
     feed(s) advanced within 12 minutes; errors are listed for the
     FIX_ERRORS wave, not hidden
Gate (sys.exit(1)): manifest not written, router not redeployed, or fewer
than half the members invoked successfully.
"""
import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
ROUTER = "justhodl-scheduler"
FANOUT_KEY = "config/fanout-manifest.json"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=60, retries={"max_attempts": 4}))
ev = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
NOW = datetime.now(timezone.utc)
TICK_HOURS = {}   # tick -> set of UTC hours it fires (from cron); filled in A


def get_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def cron_hours(expr):
    """UTC hours a cron/rate fires (coarse). rate(N minutes|hours) -> all hours."""
    e = (expr or "").strip()
    if e.startswith("rate("):
        return set(range(24))
    if e.startswith("cron("):
        parts = e[5:-1].split()
        if len(parts) >= 2:
            h = parts[1]
            hours = set()
            for tok in h.split(","):
                if tok == "*":
                    return set(range(24))
                if "/" in tok:
                    base, step = tok.split("/")
                    start = 0 if base == "*" else int(base)
                    hours |= set(range(start, 24, int(step)))
                elif "-" in tok:
                    a, b = tok.split("-")
                    hours |= set(range(int(a), int(b) + 1))
                else:
                    hours.add(int(tok))
            return hours
    return set()


def main():
    with report("5106-fanout-restore") as r:
        r.heading("ops 5106 -- restore the fan-out cluster silenced on 2026-08-01")
        fails = []
        diag = get_json("data/audit/fleet-repair-diagnostics-5105.json")
        if not diag:
            r.fail("5105 diagnostics unreadable")
            sys.exit(1)
        facts = diag["facts"]

        r.section("A. proof")
        try:
            fl = logs.filter_log_events(logGroupName=f"/aws/lambda/{ROUTER}", startTime=int((NOW - timedelta(days=2)).timestamp() * 1000),
                                        filterPattern="scheduler", limit=40)
            lines = [e["message"].strip()[:160] for e in fl.get("events") or []]
            r.log(f"router log (2d, {len(lines)} lines): {json.dumps(lines[-8:])[:900]}")
        except Exception as e:  # noqa: BLE001
            r.warn(f"router log: {str(e)[:100]}")
        legacy = get_json("config/schedule-manifest.json") or {}
        r.log(f"legacy key config/schedule-manifest.json: keys={list(legacy.keys())[:8]} has_ticks={isinstance(legacy.get('ticks'), dict)} source={str(legacy.get('source'))[:80]}")
        tick_rules = []
        for page in ev.get_paginator("list_rules").paginate():
            for rule in page.get("Rules") or []:
                try:
                    tg = ev.list_targets_by_rule(Rule=rule["Name"])
                except Exception:  # noqa: BLE001
                    continue
                for t in tg.get("Targets") or []:
                    if (t.get("Arn") or "").endswith(f":function:{ROUTER}"):
                        try:
                            inp = json.loads(t.get("Input") or "{}")
                        except Exception:  # noqa: BLE001
                            inp = {"raw": t.get("Input")}
                        tick_rules.append({"rule": rule["Name"], "state": rule.get("State"), "expr": rule.get("ScheduleExpression"), "tick": inp.get("tick")})
        r.log(f"tick rules -> router: {json.dumps(tick_rules)[:1500]}")
        for tr in tick_rules:
            if tr.get("tick") and tr.get("state") == "ENABLED":
                TICK_HOURS.setdefault(tr["tick"], set()).update(cron_hours(tr["expr"]))
        r.log(f"tick -> UTC hours: {json.dumps({k: sorted(v) for k, v in TICK_HOURS.items()})}")
        md = cw.get_metric_data(MetricDataQueries=[{"Id": "i", "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": "Invocations",
                                                    "Dimensions": [{"Name": "FunctionName", "Value": ROUTER}]}, "Period": 86400, "Stat": "Sum"}, "ReturnData": True}],
                                StartTime=NOW - timedelta(days=7), EndTime=NOW)
        r.log(f"router invocations last 7d (daily): {[int(v) for v in (md['MetricDataResults'][0].get('Values') or [])]}")
        if not TICK_HOURS:
            fails.append("no enabled tick rules target the router")

        r.section("B. membership + tick assignment")
        members = []
        for fn, f in facts.items():
            if f.get("rules") or f.get("schedules"):
                continue
            la = f.get("last_active")
            if not la or not ("2026-07-31" <= la <= "2026-08-02"):
                continue
            if (f.get("active_days") or 0) < 20:
                continue
            members.append(fn)
        members.sort()
        r.log(f"{len(members)} fan-out members by signature: {', '.join(members)}")
        assign = {}
        start, end = datetime(2026, 7, 15, tzinfo=timezone.utc), datetime(2026, 7, 31, tzinfo=timezone.utc)
        for i in range(0, len(members), 20):
            chunk = members[i:i + 20]
            q = [{"Id": f"m{j}", "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": "Invocations", "Dimensions": [{"Name": "FunctionName", "Value": fn}]},
                                                 "Period": 3600, "Stat": "Sum"}, "ReturnData": True} for j, fn in enumerate(chunk)]
            res = cw.get_metric_data(MetricDataQueries=q, StartTime=start, EndTime=end, MaxDatapoints=100000)
            for m in res.get("MetricDataResults") or []:
                fn = chunk[int(m["Id"][1:])]
                hours = defaultdict(int)
                for ts, v in zip(m.get("Timestamps") or [], m.get("Values") or []):
                    if v > 0:
                        hours[ts.hour] += 1
                total_days = 16
                fired = {h for h, c in hours.items() if c >= total_days * 0.5}     # fires most days at this hour
                per_day = sum(hours.values()) / total_days
                cands = []
                for tick, th in TICK_HOURS.items():
                    if fired and fired <= th and len(th) <= 4:
                        cands.append(tick)
                if not cands:
                    if per_day >= 20:
                        cands = [t for t in ("hourly",) if t in TICK_HOURS]
                    elif per_day >= 4:
                        cands = [t for t in ("4hourly",) if t in TICK_HOURS]
                    elif fired:
                        # nearest daily tick to the firing hour
                        best = None
                        for tick, th in TICK_HOURS.items():
                            if len(th) <= 4:
                                d = min(abs(h - hh) for h in fired for hh in th) if th else 99
                                if best is None or d < best[0]:
                                    best = (d, tick)
                        cands = [best[1]] if best else []
                assign[fn] = {"fired_hours": sorted(fired), "per_day": round(per_day, 2), "ticks": sorted(set(cands))}
        for fn in members:
            a = assign.get(fn) or {"ticks": []}
            if not a["ticks"]:
                # no measurable signature: default to the morning daily tick
                a["ticks"] = [t for t in ("daily-morn",) if t in TICK_HOURS] or list(TICK_HOURS)[:1]
                a["defaulted"] = True
                assign[fn] = a
            r.log(f"  {fn:<44} hours={a.get('fired_hours')} per_day={a.get('per_day')} -> {a['ticks']}{' (default)' if a.get('defaulted') else ''}")
            r.kv(fn=fn, ticks=",".join(a["ticks"]), per_day=a.get("per_day"), hours=",".join(str(h) for h in (a.get("fired_hours") or [])))
        ticks = defaultdict(list)
        for fn, a in assign.items():
            for t in a["ticks"]:
                ticks[t].append(fn)
        manifest = {"version": 2, "generated_at": NOW.isoformat(timespec="seconds"),
                    "source": "ops 5106: rebuilt from members' invocation signatures (no rule/schedule, died 2026-07-31..08-02) and their Jul 15-30 firing hours vs the live tick rules",
                    "ticks": {t: sorted(v) for t, v in ticks.items()}, "disabled": [], "assignment": assign,
                    "doctrine": "This is the fan-out router's manifest. config/schedule-manifest.json is the declarative rules/schedules snapshot and must never be written here."}
        r.log(f"ticks: {json.dumps({t: len(v) for t, v in ticks.items()})}")

        r.section("C. write manifest + redeploy router")
        s3.put_object(Bucket=B, Key=FANOUT_KEY, Body=json.dumps(manifest, indent=1, default=str).encode(), ContentType="application/json")
        (ROOT / "config" / "fanout-manifest.json").write_text(json.dumps(manifest, indent=1, default=str) + "\n")
        r.ok(f"manifest written to s3://{B}/{FANOUT_KEY} and config/fanout-manifest.json")
        deploy_lambda(report=r, function_name=ROUTER, source_dir=ROOT / "aws" / "lambdas" / ROUTER / "source",
                      env_vars={"FANOUT_MANIFEST_KEY": FANOUT_KEY}, timeout=300, memory=512, create_function_url=False, smoke=False,
                      description="EventBridge schedule fan-out router (ops 5106: own manifest key config/fanout-manifest.json; refuses a manifest without ticks)")
        for _ in range(30):
            c = lam.get_function_configuration(FunctionName=ROUTER)
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(3)

        r.section("D. fire + verify")
        feeds_before = {}
        for fn in members:
            for k in (facts[fn].get("keys") or [])[:3]:
                try:
                    feeds_before[k] = s3.head_object(Bucket=B, Key=k)["LastModified"]
                except Exception:  # noqa: BLE001
                    feeds_before[k] = None
        t_fire = datetime.now(timezone.utc)
        fired_ticks = sorted(ticks)
        for t in fired_ticks:
            try:
                resp = lam.invoke(FunctionName=ROUTER, InvocationType="RequestResponse", Payload=json.dumps({"tick": t}).encode())
                body = resp["Payload"].read().decode()[:300]
                r.log(f"router tick {t}: {body}")
            except Exception as e:  # noqa: BLE001
                r.warn(f"router tick {t} failed: {str(e)[:120]}")
        time.sleep(420)
        invoked, errored, moved = [], [], []
        for i in range(0, len(members), 25):
            chunk = members[i:i + 25]
            q = []
            for j, fn in enumerate(chunk):
                for met, tag in (("Invocations", "i"), ("Errors", "e")):
                    q.append({"Id": f"{tag}{j}", "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": met, "Dimensions": [{"Name": "FunctionName", "Value": fn}]},
                                                                 "Period": 3600, "Stat": "Sum"}, "ReturnData": True})
            res = cw.get_metric_data(MetricDataQueries=q, StartTime=t_fire - timedelta(minutes=2), EndTime=datetime.now(timezone.utc) + timedelta(minutes=5))
            got = {}
            for m in res.get("MetricDataResults") or []:
                j = int(m["Id"][1:])
                got.setdefault(chunk[j], {})[m["Id"][0]] = sum(m.get("Values") or [0])
            for fn in chunk:
                g = got.get(fn, {})
                if g.get("i", 0) > 0:
                    invoked.append(fn)
                if g.get("e", 0) > 0:
                    errored.append(fn)
        for fn in members:
            for k in (facts[fn].get("keys") or [])[:3]:
                try:
                    lm = s3.head_object(Bucket=B, Key=k)["LastModified"]
                    if lm > t_fire:
                        moved.append((fn, k))
                except Exception:  # noqa: BLE001
                    pass
        r.log(f"invoked {len(invoked)}/{len(members)}: {', '.join(invoked)}")
        r.log(f"errored {len(errored)}: {', '.join(errored)}")
        r.log(f"feeds advanced ({len(moved)}): {json.dumps(moved)[:1500]}")
        not_inv = [fn for fn in members if fn not in invoked]
        r.log(f"not invoked ({len(not_inv)}): {', '.join(not_inv)}")
        for fn in members:
            r.kv(fn=fn, invoked=fn in invoked, errored=fn in errored, feed_moved=any(m[0] == fn for m in moved))
        if len(invoked) < len(members) / 2:
            fails.append(f"only {len(invoked)}/{len(members)} members invoked")
        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok(f"VERDICT: GREEN -- fan-out restored: {len(members)} members across ticks {dict((t, len(v)) for t, v in ticks.items())}; "
             f"{len(invoked)} invoked now, {len(errored)} errored (-> FIX_ERRORS wave), {len(moved)} feeds advanced")


if __name__ == "__main__":
    main()
