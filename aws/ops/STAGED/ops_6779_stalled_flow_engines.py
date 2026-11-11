"""ops 6779 - READ-ONLY: why are justhodl-ici-flows (data/ici-fund-flows.json, last written 2026-10-02, last week
2026-09-23) and justhodl-etf-true-flows (data/etf-true-flows.json, last written 2026-10-11) stalled? (Khalid 2026-11-11:
"Why are these engines stalled?")

Reads, for each engine: the S3 output (age + the payload's own generated_at / sources / errors / quality), the function
configuration (last deploy, timeout), the EventBridge rule AND the Scheduler schedule (state, expression, targets), the
reconciler's declared state in config/schedule-manifest.json (ici-flows-weekly is declared DISABLED there since the
manifest's first commit on 2026-11-03), CloudWatch daily Invocations / Errors / Throttles / Duration since 2026-09-20,
the last log streams (redacted) plus a filtered scan for timeouts / tracebacks / the engines' own summary lines,
CloudTrail management events on the rule names and function names (who disabled / deployed what, when), and the ETF
engine's retained attempts / runs / outputs under data/etf-research/ (counts per day, failure classes). Then probes the
ICI sources from the runner with the lambda's own parser: ici.org direct, the Wayback availability endpoint, the CDX
index (newest snapshots), and the nearest snapshot. Invokes nothing, changes nothing, writes nothing but this report.
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import importlib.util
import json
import re
import sys
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
SINCE = datetime(2026, 9, 20, tzinfo=timezone.utc)
ENGINES = {
    "justhodl-ici-flows": {"rule": "justhodl-ici-flows-weekly", "out": "data/ici-fund-flows.json",
                           "also": [], "markers": r"Task timed out|Traceback|\[ERROR\]|Status: timeout|\"status\"|fresh_weeks|errors"},
    "justhodl-etf-true-flows": {"rule": "justhodl-etf-true-flows-daily", "out": "data/etf-true-flows.json",
                                "also": ["data/etf-shares-history.json", "data/etf-research/migration.json"],
                                "markers": r"Task timed out|Traceback|\[ERROR\]|Status: timeout|\[etf-research\]"},
}
SECRET_KV = re.compile(r"(?i)\b(api[_-]?key|apikey|access[_-]?key|token|secret|password|passwd|authorization|signature|key)(\s*[=:]\s*|%3D)[^\s&\"',}]+")
BEARER = re.compile(r"(?i)\bbearer\s+\S+")
LONG_TOKEN = re.compile(r"[A-Za-z0-9_\-+/=]{20,}")
AWS_KEY = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")
HEX32 = re.compile(r"[0-9a-fA-F]{32,}")


def _opaque(tok):
    if AWS_KEY.fullmatch(tok) or HEX32.fullmatch(tok):
        return True
    has_d, has_u, has_l = any(c.isdigit() for c in tok), any(c.isupper() for c in tok), any(c.islower() for c in tok)
    if has_d and has_u and has_l:
        return True
    return len(tok) >= 28 and has_d and (has_u or has_l) and "-" not in tok and "_" not in tok


def redact(line):
    line = BEARER.sub("Bearer ***", str(line))
    line = SECRET_KV.sub(lambda m: m.group(1) + m.group(2) + "***", line)
    return LONG_TOKEN.sub(lambda m: "***" if _opaque(m.group(0)) else m.group(0), line)


def selftest():
    red = redact("GET https://x/stable/quote?symbol=AAPL&apikey=abc123&limit=5")
    assert "abc123" not in red and "symbol=AAPL" in red
    assert redact("ici-flows wrote data/ici-fund-flows.json 3087 bytes") == "ici-flows wrote data/ici-fund-flows.json 3087 bytes"
    assert redact("id AbCdEfGhIjKlMnOpQrStUvWxYz0123456789 end") == "id *** end"
    print("selftest ok")


def load_ici_module():
    """the lambda's own parser + URLs + UA, loaded under a private module name (its import builds an S3 client, nothing else)"""
    src = ROOT / "aws" / "lambdas" / "justhodl-ici-flows" / "source" / "lambda_function.py"
    spec = importlib.util.spec_from_file_location("ici_flows_lambda_6779", src)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def http(url, headers, timeout=45):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read()


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec="minutes") if dt else "?"


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
    logs = boto3.client("logs", region_name=REGION, config=cfg)
    evb = boto3.client("events", region_name=REGION, config=cfg)
    sch = boto3.client("scheduler", region_name=REGION, config=cfg)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    ct = boto3.client("cloudtrail", region_name=REGION, config=cfg)
    ssm = boto3.client("ssm", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    facts = {}
    manifest = json.loads((ROOT / "config" / "schedule-manifest.json").read_text())
    declared = {}
    rules_decl = manifest.get("rules") if isinstance(manifest, dict) else manifest
    for rule in rules_decl or []:
        if isinstance(rule, dict) and rule.get("name"):
            declared[rule["name"]] = rule
    with report("ops_6779_stalled_flow_engines") as r:
        r.heading("ops 6779 - why the ICI weekly flows and the ETF creations engines are stalled (%s; read-only)" % now.isoformat(timespec="minutes"))
        r.section("0. the reconciler's declared state (config/schedule-manifest.json, first committed 2026-11-03) and its live mode")
        for name in ("justhodl-ici-flows-weekly", "justhodl-etf-true-flows-daily"):
            d = declared.get(name)
            r.log("%s: %s" % (name, json.dumps({k: d.get(k) for k in ("kind", "expr", "state")}) if d else "NOT DECLARED"))
            facts[name.replace("justhodl-", "") + "_declared"] = (d or {}).get("state", "undeclared")
        try:
            mode = ssm.get_parameter(Name="/justhodl/schedules/mode")["Parameter"]["Value"]
            r.log("SSM /justhodl/schedules/mode = %s" % mode)
            facts["reconciler_mode"] = mode
        except Exception as exc:  # noqa: BLE001
            r.warn("reconciler mode: %s: %s" % (type(exc).__name__, str(exc)[:120]))
        try:
            drift = json.loads(s3.get_object(Bucket=LIVE, Key="data/schedule-drift.json")["Body"].read())
            rows = drift.get("drifts") or drift.get("drift") or drift.get("items") or []
            hits = [x for x in rows if isinstance(x, dict) and any("ici-flows" in str(v) or "etf-true-flows" in str(v) for v in x.values())]
            r.log("data/schedule-drift.json: generated %s, mode %s, %d drift rows, %d about these engines: %s" % (
                drift.get("generated_at") or drift.get("as_of"), drift.get("mode"), len(rows), len(hits), json.dumps(hits)[:600]))
        except Exception as exc:  # noqa: BLE001
            r.warn("schedule-drift: %s: %s" % (type(exc).__name__, str(exc)[:120]))

        for fn, spec in ENGINES.items():
            short = fn.replace("justhodl-", "")
            r.section("%s" % fn)
            # --- the output object + what the payload says about itself
            for key in [spec["out"]] + spec["also"]:
                try:
                    h = s3.head_object(Bucket=LIVE, Key=key)
                    age_d = (now - h["LastModified"]).total_seconds() / 86400
                    r.log("%s: %.1f days old (%d B), LastModified %s" % (key, age_d, h["ContentLength"], iso(h["LastModified"])))
                    if key == spec["out"]:
                        facts[short + "_age_days"] = round(age_d, 1)
                except Exception as exc:  # noqa: BLE001
                    r.warn("%s: %s" % (key, str(exc)[:120]))
            try:
                payload = json.loads(s3.get_object(Bucket=LIVE, Key=spec["out"])["Body"].read())
                if fn == "justhodl-ici-flows":
                    hist = payload.get("history") or []
                    r.log("payload: generated_at %s | count %s | latest_week %s | sources %s | errors %s" % (
                        payload.get("generated_at"), payload.get("count"), (payload.get("latest_week") or {}).get("week_ending"),
                        payload.get("sources"), json.dumps(payload.get("errors"))[:500]))
                    weeks = [w.get("week_ending") for w in hist]
                    r.log("history weeks: first %s, last %s; gaps > 14 days: %s" % (
                        weeks[0] if weeks else None, weeks[-1] if weeks else None,
                        [(a, b) for a, b in zip(weeks, weeks[1:]) if (datetime.fromisoformat(b) - datetime.fromisoformat(a)).days > 14][:12]))
                else:
                    rc = payload.get("reference_calendar") or {}
                    r.log("payload: contract %s | generated_at %s | reference_calendar latest %s | quality %s | source_status_codes %s | replay %s" % (
                        payload.get("contract"), payload.get("generated_at"), rc.get("latest_date"), json.dumps(payload.get("quality"))[:300],
                        json.dumps(payload.get("source_status_codes"))[:400], json.dumps(payload.get("replay"))[:160]))
                    clocks = payload.get("source_clocks") or {}
                    if clocks:
                        vals = sorted(clocks.values())
                        r.log("source_clocks: %d, oldest %s, newest %s" % (len(clocks), vals[0], vals[-1]))
                facts[short + "_generated_at"] = str(payload.get("generated_at"))[:19]
            except Exception as exc:  # noqa: BLE001
                r.warn("payload: %s: %s" % (type(exc).__name__, str(exc)[:120]))
            # --- function configuration (the last deploy)
            try:
                c = lam.get_function_configuration(FunctionName=fn)
                r.log("function: state %s / %s, timeout %ss, memory %s MB, runtime %s, code last modified %s, sha %s" % (
                    c.get("State"), c.get("LastUpdateStatus"), c.get("Timeout"), c.get("MemorySize"), c.get("Runtime"),
                    c.get("LastModified"), str(c.get("CodeSha256"))[:12]))
                facts[short + "_code_modified"] = str(c.get("LastModified"))[:16]
            except Exception as exc:  # noqa: BLE001
                r.warn("get_function_configuration: %s" % str(exc)[:160])
            # --- the EventBridge rule (manifest kind "events") and, in case it moved, the Scheduler schedule
            rule = spec["rule"]
            try:
                d = evb.describe_rule(Name=rule)
                tg = evb.list_targets_by_rule(Rule=rule).get("Targets", [])
                r.log("EventBridge rule %s: %s %s | targets %s" % (rule, d.get("State"), d.get("ScheduleExpression"),
                                                                  [t.get("Arn", "?").split(":")[-1] for t in tg]))
                facts[short + "_rule"] = "%s %s" % (d.get("State"), d.get("ScheduleExpression"))
            except evb.exceptions.ResourceNotFoundException:
                r.log("EventBridge rule %s: absent" % rule)
                facts[short + "_rule"] = "absent"
            except Exception as exc:  # noqa: BLE001
                r.warn("describe_rule %s: %s" % (rule, str(exc)[:160]))
            try:
                s_ = sch.get_schedule(Name=rule, GroupName="default")
                r.log("Scheduler schedule %s: %s %s | target %s" % (rule, s_.get("State"), s_.get("ScheduleExpression"),
                                                                   (s_.get("Target") or {}).get("Arn", "?").split(":")[-1]))
                facts[short + "_schedule"] = "%s %s" % (s_.get("State"), s_.get("ScheduleExpression"))
            except sch.exceptions.ResourceNotFoundException:
                r.log("Scheduler schedule %s: absent (the rule above is the only trigger)" % rule)
            except Exception as exc:  # noqa: BLE001
                r.warn("get_schedule %s: %s" % (rule, str(exc)[:160]))
            # every rule / schedule that targets this function, whatever its name
            try:
                others = []
                for page in evb.get_paginator("list_rule_names_by_target").paginate(TargetArn="arn:aws:lambda:%s:857687956942:function:%s" % (REGION, fn)):
                    others += page.get("RuleNames", [])
                r.log("rules targeting %s: %s" % (fn, others or "none"))
            except Exception as exc:  # noqa: BLE001
                r.warn("list_rule_names_by_target: %s" % str(exc)[:160])
            # --- CloudWatch daily metrics since SINCE
            q = []
            for tag, m, stat in (("n", "Invocations", "Sum"), ("e", "Errors", "Sum"), ("t", "Throttles", "Sum"), ("d", "Duration", "Maximum")):
                q.append({"Id": tag, "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": m,
                          "Dimensions": [{"Name": "FunctionName", "Value": fn}]}, "Period": 86400, "Stat": stat}})
            try:
                res = cw.get_metric_data(MetricDataQueries=q, StartTime=SINCE, EndTime=now)
                days = {}
                for r_ in res["MetricDataResults"]:
                    for t, v in zip(r_["Timestamps"], r_["Values"]):
                        days.setdefault(t.date().isoformat(), {})[r_["Id"]] = v
                active = {d_: v for d_, v in days.items() if v.get("n", 0) or v.get("e", 0)}
                series = ", ".join("%s n%d e%d t%d dmax%.0fs" % (d_[5:], v.get("n", 0), v.get("e", 0), v.get("t", 0), v.get("d", 0) / 1000.0)
                                   for d_, v in sorted(active.items()))
                r.log("invocations per UTC day since %s (days with any): %s" % (SINCE.date(), series or "NONE - the function has not been invoked at all"))
                tot_n = sum(v.get("n", 0) for v in days.values())
                tot_e = sum(v.get("e", 0) for v in days.values())
                last_day = max(active) if active else None
                facts[short + "_invocations_since_0920"] = "%d (errors %d), last %s" % (tot_n, tot_e, last_day)
            except Exception as exc:  # noqa: BLE001
                r.warn("get_metric_data: %s" % str(exc)[:160])
            # --- logs: last 3 streams' final invocation, then a filtered scan for the markers
            group = "/aws/lambda/" + fn
            try:
                streams = logs.describe_log_streams(logGroupName=group, orderBy="LastEventTime", descending=True, limit=3).get("logStreams", [])
                for st in streams:
                    evs = logs.get_log_events(logGroupName=group, logStreamName=st["logStreamName"], limit=150, startFromHead=False).get("events", [])
                    starts = [i for i, e in enumerate(evs) if e["message"].startswith("START RequestId")]
                    last = evs[starts[-1]:] if starts else evs
                    r.log("stream …%s: last event %s, %d events, last invocation %d lines" % (
                        st["logStreamName"][-18:], iso(datetime.fromtimestamp(st.get("lastEventTimestamp", 0) / 1000, timezone.utc)), len(evs), len(last)))
                    for e in last[-30:]:
                        stamp = datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).strftime("%m-%d %H:%M:%S")
                        r.log("  %s %s" % (stamp, redact(" ".join(e["message"].split()))[:600]))
                hits = []
                kw = {"logGroupName": group, "startTime": int(SINCE.timestamp() * 1000), "filterPattern": "", "limit": 10000}
                for page in logs.get_paginator("filter_log_events").paginate(**kw):
                    for e in page.get("events", []):
                        msg = " ".join(e["message"].split())
                        if re.search(spec["markers"], msg):
                            hits.append((e["timestamp"], msg))
                    if len(hits) > 400:
                        break
                kinds = Counter()
                for ts, msg in hits:
                    for k in ("Task timed out", "Traceback", "[ERROR]", "fresh_weeks", "[etf-research]"):
                        if k in msg:
                            kinds[k] += 1
                r.log("filtered scan since %s: %d marker lines: %s" % (SINCE.date(), len(hits), dict(kinds)))
                shown = 0
                for ts, msg in hits:
                    if "REPORT RequestId" in msg or msg.startswith("END RequestId"):
                        continue
                    r.log("  %s %s" % (datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%m-%d %H:%M"), redact(msg)[:700]))
                    shown += 1
                    if shown >= 40:
                        r.log("  … %d more" % (len(hits) - shown))
                        break
                facts[short + "_log_markers"] = json.dumps(dict(kinds))
            except logs.exceptions.ResourceNotFoundException:
                r.log("(no log group %s)" % group)
            except Exception as exc:  # noqa: BLE001
                r.warn("logs: %s: %s" % (type(exc).__name__, str(exc)[:160]))
            # --- CloudTrail: who touched the rule / the function (85 days)
            for resource in (rule, fn):
                try:
                    evs = []
                    for page in ct.get_paginator("lookup_events").paginate(
                            LookupAttributes=[{"AttributeKey": "ResourceName", "AttributeValue": resource}],
                            StartTime=now - timedelta(days=85), EndTime=now, PaginationConfig={"MaxItems": 200}):
                        evs += page.get("Events", [])
                    names = Counter(e.get("EventName") for e in evs)
                    r.log("CloudTrail %s: %d events: %s" % (resource, len(evs), dict(names)))
                    for e in sorted(evs, key=lambda x: x.get("EventTime"))[-14:]:
                        detail = {}
                        try:
                            raw = json.loads(e.get("CloudTrailEvent") or "{}")
                            ident = raw.get("userIdentity") or {}
                            detail = {"by": (ident.get("sessionContext") or {}).get("sessionIssuer", {}).get("userName") or ident.get("userName") or ident.get("invokedBy") or ident.get("type"),
                                      "source": raw.get("eventSource", "").split(".")[0], "error": raw.get("errorCode"),
                                      "state": (raw.get("requestParameters") or {}).get("state"), "ua": str(raw.get("userAgent"))[:40]}
                        except Exception:  # noqa: BLE001
                            pass
                        r.log("  %s %s %s" % (iso(e.get("EventTime")), e.get("EventName"), redact(json.dumps({k: v for k, v in detail.items() if v}))))
                    if resource == rule:
                        facts[short + "_rule_trail"] = json.dumps(dict(names))
                except Exception as exc:  # noqa: BLE001
                    r.warn("CloudTrail %s: %s: %s" % (resource, type(exc).__name__, str(exc)[:160]))
            # --- the ETF engine's retained ledger: attempts (failures), runs (manifests), outputs
            if fn == "justhodl-etf-true-flows":
                for sub in ("attempts", "runs", "outputs"):
                    try:
                        objs = []
                        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix="data/etf-research/%s/" % sub):
                            objs += page.get("Contents", [])
                        per_day = Counter(o["LastModified"].date().isoformat() for o in objs)
                        newest = max(objs, key=lambda o: o["LastModified"]) if objs else None
                        r.log("data/etf-research/%s/: %d objects, newest %s; per day (last 20 days with any): %s" % (
                            sub, len(objs), iso(newest["LastModified"]) if newest else "-",
                            ", ".join("%s:%d" % (d_[5:], n) for d_, n in sorted(per_day.items())[-20:])))
                        facts["etf_%s" % sub] = "%d, newest %s" % (len(objs), iso(newest["LastModified"]) if newest else "-")
                        if sub == "attempts" and objs:
                            classes = Counter()
                            for o in sorted(objs, key=lambda o: o["LastModified"])[-12:]:
                                try:
                                    a = json.loads(s3.get_object(Bucket=LIVE, Key=o["Key"])["Body"].read())
                                    classes[a.get("failure_class")] += 1
                                    r.log("  attempt %s: failure_class %s, source_status_codes %s" % (
                                        iso(o["LastModified"]), a.get("failure_class"), json.dumps(a.get("source_status_codes"))[:400]))
                                except Exception as exc:  # noqa: BLE001
                                    r.warn("  attempt %s: %s" % (o["Key"][-24:], str(exc)[:100]))
                            facts["etf_failure_classes"] = json.dumps(dict(classes))
                    except Exception as exc:  # noqa: BLE001
                        r.warn("list %s: %s: %s" % (sub, type(exc).__name__, str(exc)[:160]))

        r.section("ICI sources from the runner, with the lambda's own parser")
        try:
            ici = load_ici_module()
            probes = [("ici-direct", ici.ICI_FLOWS_URL, 45), ("wayback-available", ici.WAYBACK_AVAILABLE, 30),
                      ("cdx-newest", "https://web.archive.org/cdx/search/cdx?url=www.ici.org/research/stats/flows&output=json&fl=timestamp,statuscode,digest&limit=-8", 30)]
            closest = None
            for name, url, to in probes:
                try:
                    status, body = http(url, ici.UA, to)
                    text = body.decode("utf-8", "ignore")
                    if name == "ici-direct":
                        tbl = ici.parse_ici_table(text)
                        r.log("%s: HTTP %s, %d bytes, parsed weeks %s" % (name, status, len(body), sorted(tbl.keys()) if tbl else "none"))
                        facts["ici_direct"] = "HTTP %s, %d weeks" % (status, len(tbl or {}))
                    elif name == "wayback-available":
                        avail = json.loads(text)
                        closest = ((avail.get("archived_snapshots") or {}).get("closest") or {})
                        r.log("%s: HTTP %s, closest %s" % (name, status, json.dumps(closest)[:300]))
                        facts["wayback_closest"] = str(closest.get("timestamp"))
                    else:
                        rows = json.loads(text)
                        r.log("%s: HTTP %s, newest snapshots %s" % (name, status, rows[1:] if rows else rows))
                        facts["cdx_newest"] = str([x[0] for x in rows[1:]][-3:]) if rows else "none"
                except urllib.error.HTTPError as exc:
                    r.log("%s: HTTP %s %s" % (name, exc.code, exc.reason))
                    facts[name.replace("-", "_")] = "HTTP %s" % exc.code
                except Exception as exc:  # noqa: BLE001
                    r.warn("%s: %s: %s" % (name, type(exc).__name__, str(exc)[:160]))
            if closest and closest.get("url"):
                url = re.sub(r"(/web/\d+)", r"\1id_", closest["url"], count=1)
                try:
                    status, body = http(url, ici.UA, 60)
                    tbl = ici.parse_ici_table(body.decode("utf-8", "ignore"))
                    r.log("closest snapshot %s: HTTP %s, %d bytes, parsed weeks %s" % (closest.get("timestamp"), status, len(body), sorted(tbl.keys()) if tbl else "none"))
                    facts["wayback_parsed_weeks"] = str(sorted(tbl.keys())[-3:] if tbl else "none")
                except Exception as exc:  # noqa: BLE001
                    r.warn("closest snapshot: %s: %s" % (type(exc).__name__, str(exc)[:160]))
        except Exception as exc:  # noqa: BLE001
            r.warn("ici module: %s: %s" % (type(exc).__name__, str(exc)[:160]))
        r.kv(**{k: str(v) for k, v in facts.items()})
        r.ok("read-only; nothing invoked, nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
