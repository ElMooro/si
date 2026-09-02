"""ops_5105 -- fleet repair diagnostics (read-only), the input to the repair waves.

Khalid: "take every single one of them one by one". Before touching a schedule
or a line of code, every engine on the 5098 lists gets a fact sheet:

  * schedule: EventBridge rules targeting it (+ state) and Scheduler schedules
    (+ state) -- the schedule manifest is regenerated here from the live APIs
  * invocation history: 90 days of daily Invocations/Errors -> first/last
    active day, death date, cadence before death
  * function URL / API-driven: an idle URL-driven engine is not an orphan
  * output keys: every `data/...json` it writes (source scan) -> consumers:
    site pages/js and other Lambdas that read the key; feed age on S3
  * for erroring engines: last 3 distinct error lines from CloudWatch (7d)
    and the last REPORT line (duration / memory)
  * retirement evidence: description, config, ops reports mentioning retire/
    superseded for the function

Verdict per engine: API_DRIVEN | RETIRED_OR_SUPERSEDED | RESURRECT (consumed
feed, dead schedule) | DORMANT_UNCONSUMED | FIX_ERRORS (with the error class)
| SILENT_SCHEDULE. Writes data/audit/fleet-repair-diagnostics-5105.json.
Never RED unless the 5098 audit doc is unreadable.
"""
import json
import re
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

REGION = "us-east-1"
B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(retries={"max_attempts": 6}))
ev = boto3.client("events", region_name=REGION, config=Config(retries={"max_attempts": 6}))
sch = boto3.client("scheduler", region_name=REGION, config=Config(retries={"max_attempts": 6}))
cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION, config=Config(retries={"max_attempts": 6}))
NOW = datetime.now(timezone.utc)
KEY_RE = re.compile(r"""Key\s*=\s*f?["']((?:data/)[^"'{}]+\.json(?:\.gz)?)["']""")
KEY_RE_DYN = re.compile(r"""Key\s*=\s*f["']((?:data/)[^"']+)["']""")


def get_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception as e:  # noqa: BLE001
        return None


def site_files():
    out = []
    for p in ROOT.iterdir():
        if p.is_file() and p.suffix in (".html", ".js"):
            out.append(p)
        elif p.is_dir() and p.name not in ("aws", ".git", "node_modules", "scripts", "config", "docs", "chrome-extension") and not p.name.startswith("."):
            out.extend(q for q in p.rglob("*") if q.is_file() and q.suffix in (".html", ".js") and "node_modules" not in q.parts)
    return out


def main():
    with report("5105-fleet-repair-diagnostics") as r:
        r.heading("ops 5105 -- fleet repair diagnostics for every engine on the 5098 lists")
        audit = get_json("data/audit/fleet-data-audit-5098.json")
        if not audit:
            r.fail("5098 audit doc unreadable")
            sys.exit(1)
        rows = audit.get("engines") or []
        targets = [x for x in rows if x["cls"] in ("ERRORING", "SOME_ERRORS", "SCHEDULED_SILENT", "ORPHAN_IDLE")]
        r.log(f"{len(targets)} engines to diagnose: " + json.dumps({c: sum(1 for x in targets if x['cls'] == c) for c in ('ERRORING', 'SOME_ERRORS', 'SCHEDULED_SILENT', 'ORPHAN_IDLE')}))

        # ── live schedule inventory ──────────────────────────────────────
        r.section("A. live schedule inventory")
        rules_by_fn = defaultdict(list)
        for page in ev.get_paginator("list_rules").paginate():
            for rule in page.get("Rules") or []:
                try:
                    tg = ev.list_targets_by_rule(Rule=rule["Name"])
                except Exception:  # noqa: BLE001
                    continue
                for t in tg.get("Targets") or []:
                    arn = t.get("Arn") or ""
                    if ":function:" in arn:
                        rules_by_fn[arn.split(":function:")[1].split(":")[0]].append({"rule": rule["Name"], "state": rule.get("State"), "expr": rule.get("ScheduleExpression")})
        sched_by_fn = defaultdict(list)
        n_sched = 0
        for page in sch.get_paginator("list_schedules").paginate():
            for s in page.get("Schedules") or []:
                n_sched += 1
                arn = (s.get("Target") or {}).get("Arn") or ""
                if ":function:" in arn:
                    sched_by_fn[arn.split(":function:")[1].split(":")[0]].append({"schedule": s["Name"], "state": s.get("State"), "group": s.get("GroupName")})
        r.log(f"rules -> {sum(len(v) for v in rules_by_fn.values())} targets on {len(rules_by_fn)} functions; scheduler {n_sched} schedules on {len(sched_by_fn)} functions")

        # ── consumers map: site + other lambdas ─────────────────────────
        r.section("B. output keys -> consumers")
        site = site_files()
        site_text = {p.relative_to(ROOT).as_posix(): p.read_text(encoding="utf-8", errors="replace") for p in site}
        lam_src = {}
        for d in (ROOT / "aws" / "lambdas").iterdir():
            src = d / "source" / "lambda_function.py"
            if src.exists():
                lam_src[d.name] = src.read_text(encoding="utf-8", errors="replace")
        outputs = {}
        for fn in [x["fn"] for x in targets]:
            src = lam_src.get(fn)
            if not src:
                outputs[fn] = {"no_source": True, "keys": []}
                continue
            keys = sorted(set(KEY_RE.findall(src)))
            dyn = sorted(set(k for k in KEY_RE_DYN.findall(src) if "{" in k))[:6]
            outputs[fn] = {"keys": keys[:12], "dynamic": dyn}
        r.log(f"output keys resolved for {sum(1 for v in outputs.values() if v.get('keys'))} of {len(outputs)} engines")

        def consumers_of(key):
            base = key.split("/")[-1]
            stem = base.replace(".json.gz", "").replace(".json", "")
            pages = [f for f, t in site_text.items() if base in t or f"/{stem}.json" in t]
            engines = [n for n, t in lam_src.items() if base in t and f"Key=\"{key}\"" not in t and f"Key=f\"{key}\"" not in t]
            return pages[:8], engines[:8]

        # ── per-engine facts ─────────────────────────────────────────────
        r.section("C. per-engine fact sheets")
        start = NOW - timedelta(days=90)
        facts = {}
        for i, x in enumerate(targets):
            fn = x["fn"]
            f = {"cls": x["cls"], "inv7d": x["inv7d"], "err7d": x["err7d"], "desc": x.get("desc"), "timeout": x.get("timeout"), "mem": x.get("mem"), "modified": x.get("modified")}
            f["rules"] = rules_by_fn.get(fn, [])
            f["schedules"] = sched_by_fn.get(fn, [])
            try:
                cfg = lam.get_function_configuration(FunctionName=fn)
                f["state"] = cfg.get("State")
                f["last_update"] = cfg.get("LastUpdateStatus")
                f["runtime"] = cfg.get("Runtime")
                try:
                    lam.get_function_url_config(FunctionName=fn)
                    f["function_url"] = True
                except Exception:  # noqa: BLE001
                    f["function_url"] = False
            except Exception as e:  # noqa: BLE001
                f["config_error"] = str(e)[:100]
            # 90d daily invocations
            try:
                md = cw.get_metric_data(MetricDataQueries=[
                    {"Id": "i", "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": "Invocations", "Dimensions": [{"Name": "FunctionName", "Value": fn}]}, "Period": 86400, "Stat": "Sum"}, "ReturnData": True},
                    {"Id": "e", "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": "Errors", "Dimensions": [{"Name": "FunctionName", "Value": fn}]}, "Period": 86400, "Stat": "Sum"}, "ReturnData": True}],
                    StartTime=start, EndTime=NOW)
                res = {m["Id"]: list(zip(m.get("Timestamps") or [], m.get("Values") or [])) for m in md.get("MetricDataResults") or []}
                inv = sorted(res.get("i") or [])
                active = [(ts, v) for ts, v in inv if v > 0]
                f["inv90d"] = int(sum(v for _, v in inv))
                f["err90d"] = int(sum(v for _, v in (res.get("e") or [])))
                f["first_active"] = active[0][0].date().isoformat() if active else None
                f["last_active"] = active[-1][0].date().isoformat() if active else None
                f["active_days"] = len(active)
                f["cadence_per_day"] = round(sum(v for _, v in active) / len(active), 1) if active else 0
            except Exception as e:  # noqa: BLE001
                f["metrics_error"] = str(e)[:100]
            # outputs + consumers + feed age
            o = outputs.get(fn) or {}
            f["keys"] = o.get("keys") or []
            f["dynamic_keys"] = o.get("dynamic") or []
            cons = {}
            for k in f["keys"][:6]:
                pages, engines = consumers_of(k)
                age_h = None
                try:
                    lm = s3.head_object(Bucket=B, Key=k)["LastModified"]
                    age_h = round((NOW - lm).total_seconds() / 3600, 1)
                except Exception:  # noqa: BLE001
                    age_h = "missing"
                cons[k] = {"pages": pages, "engines": engines, "age_h": age_h}
            f["consumers"] = cons
            n_cons = sum(len(v["pages"]) + len(v["engines"]) for v in cons.values())
            # errors: last distinct lines
            if x["cls"] in ("ERRORING", "SOME_ERRORS"):
                try:
                    fl = logs.filter_log_events(logGroupName=f"/aws/lambda/{fn}", startTime=int((NOW - timedelta(days=7)).timestamp() * 1000),
                                                filterPattern="?Traceback ?ERROR ?\"Task timed out\" ?\"Runtime.\" ?Error", limit=60)
                    seen, samples = set(), []
                    for e2 in reversed(fl.get("events") or []):
                        m = e2["message"].strip()
                        key = re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}|\d{4}-\d\d-\d\dT[\d:.]+Z?|\d+\.\d+", "#", m)[:120]
                        if key in seen:
                            continue
                        seen.add(key)
                        samples.append(m[:300])
                        if len(samples) >= 4:
                            break
                    f["error_samples"] = samples
                    rep = logs.filter_log_events(logGroupName=f"/aws/lambda/{fn}", startTime=int((NOW - timedelta(days=3)).timestamp() * 1000), filterPattern="REPORT", limit=3)
                    f["report_lines"] = [e2["message"].strip()[:220] for e2 in (rep.get("events") or [])[-2:]]
                except Exception as e:  # noqa: BLE001
                    f["error_samples"] = [f"log read failed: {str(e)[:100]}"]
            # verdict
            desc = (f.get("desc") or "").lower()
            if f.get("function_url") and x["cls"] == "ORPHAN_IDLE":
                verdict = "API_DRIVEN"
            elif x["cls"] in ("ERRORING", "SOME_ERRORS"):
                verdict = "FIX_ERRORS"
            elif x["cls"] == "SCHEDULED_SILENT":
                verdict = "SILENT_SCHEDULE"
            elif any(w in desc for w in ("retired", "superseded", "deprecated", "legacy")):
                verdict = "RETIRED_OR_SUPERSEDED"
            elif n_cons > 0 and (f.get("inv90d") or 0) > 0:
                verdict = "RESURRECT"
            elif n_cons > 0:
                verdict = "RESURRECT_NEVER_RAN_90D"
            else:
                verdict = "DORMANT_UNCONSUMED"
            f["verdict"] = verdict
            f["n_consumers"] = n_cons
            facts[fn] = f
            r.log(f"  {fn:<44} {x['cls']:<16} -> {verdict:<24} rules={[(a['rule'], a['state']) for a in f['rules']][:2]} sched={[(a['schedule'], a['state']) for a in f['schedules']][:2]} "
                  f"url={f.get('function_url')} inv90={f.get('inv90d')} last_active={f.get('last_active')} cadence/d={f.get('cadence_per_day')} keys={f['keys'][:3]} consumers={n_cons}")
            for k, v in list(cons.items())[:3]:
                r.log(f"      {k}: age={v['age_h']}h pages={v['pages'][:4]} engines={v['engines'][:4]}")
            for smp in (f.get("error_samples") or [])[:3]:
                r.log(f"      ERR: {smp[:220]}")
            for rl in (f.get("report_lines") or [])[:1]:
                r.log(f"      {rl[:200]}")
            r.kv(fn=fn, cls=x["cls"], verdict=verdict, inv90d=f.get("inv90d"), last_active=f.get("last_active"), consumers=n_cons,
                 rules=len(f["rules"]), schedules=len(f["schedules"]), url=f.get("function_url"))
            if i % 25 == 24:
                time.sleep(1)

        r.section("D. summary")
        vc = defaultdict(list)
        for fn, f in facts.items():
            vc[f["verdict"]].append(fn)
        for v, fns in sorted(vc.items()):
            r.log(f"{v} ({len(fns)}): {', '.join(sorted(fns))}")
        # death-date clustering for RESURRECT candidates
        deaths = defaultdict(list)
        for fn, f in facts.items():
            if f["verdict"].startswith("RESURRECT") and f.get("last_active"):
                deaths[f["last_active"]].append(fn)
        r.log("death dates of RESURRECT candidates: " + json.dumps({d: len(v) for d, v in sorted(deaths.items())}))
        for d, v in sorted(deaths.items()):
            r.log(f"  {d}: {', '.join(sorted(v))[:600]}")
        s3.put_object(Bucket=B, Key="data/audit/fleet-repair-diagnostics-5105.json",
                      Body=json.dumps({"generated_at": NOW.isoformat(timespec="seconds"), "facts": facts, "verdicts": {v: sorted(f) for v, f in vc.items()},
                                       "deaths": {d: sorted(v) for d, v in deaths.items()}}, default=str).encode(),
                      ContentType="application/json", CacheControl="max-age=300")
        r.ok(f"diagnostics written: {len(facts)} engines, verdicts {json.dumps({v: len(f) for v, f in vc.items()})}")


if __name__ == "__main__":
    main()
