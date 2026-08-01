"""
justhodl-fleet-integrity — the standing audit.

WHY THIS EXISTS
The fundamental-census walk ran at 25% completion for months and 90
engines double-fired on every tick. Neither errored. Neither alarmed.
Both were found only because a cost investigation happened to walk past
them. The lesson is not "those were bad bugs" — it is that a fleet this
size has no immune system unless one is built deliberately.

This engine encodes every failure mode that has ever hidden here, runs
them weekly against the whole fleet, and — critically — DIFFS AGAINST A
BASELINE so it reports NEW defects rather than re-reporting the known
backlog every week. An alarm that cries every run is an alarm nobody
reads; that is how the original defects survived.

CHECKS
  D1  unguarded self-invocation (recursion / silent truncation)
  D2  timeout-clipped engines (average pinned at the ceiling)
  D3  duplicate EventBridge targets (double-fire)
  D4  orphan schedules (target function does not exist)
  D6  scheduled but zero invocations
  D7  reserved concurrency low enough to drop runs
  D8  error-rate and throttle outliers
  D9  scheduled + failing + no DLQ or on-failure destination
  D10 deprecated runtimes
  D11 account code-storage headroom (at 100% every deploy fleet-wide dies)
  D12 env vars naming resources that no longer resolve
  D13 dead functions (no schedule, no invocations, no URL)

OUTPUT
  s3://<bucket>/data/fleet-integrity.json     — full ledger for the page
  s3://<bucket>/data/_state/fleet-integrity-baseline.json — known set
  CloudWatch EMF metrics under namespace JustHodl/Integrity, emitted as
  a structured log line so the metrics cost ZERO API calls. This is the
  same pattern that should replace the per-function polling that used to
  cost ~$66/month.

MODES
  event {"mode":"audit"}    default — report, diff, alarm
  event {"mode":"baseline"} accept the current state as the new baseline
                            (use after a deliberate cleanup)
"""

import ast
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

VERSION = "1.1.0"
MARKER = "fleet-integrity v1.1.0 ops4242 guard-aware"

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
ARTIFACT = "data/fleet-integrity.json"
BASELINE = "data/_state/fleet-integrity-baseline.json"

CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"},
             read_timeout=60)
lam = boto3.client("lambda", config=CFG)
cw = boto3.client("cloudwatch", config=CFG)
evb = boto3.client("events", config=CFG)
sch = boto3.client("scheduler", config=CFG)
s3 = boto3.client("s3", config=CFG)


# ---------------------------------------------------------------------
# D1 — self-invocation classification (v1.1.0, ops 4243)
#
# v1.0.0 asked "does the source contain the string CHAIN_MAX or depth?"
# and called everything else unguarded. That produced two false
# positives out of three findings: justhodl-13f-clone-alpha guards with
# `hop < MAX_HOPS`, and justhodl-equity-research guards with a payload
# flag. Reporting a heuristic's output as a finding is the same mistake
# as reading HTTP 200 as success — which is the mistake this whole
# engine exists to catch. So the detector now parses the code.
#
# CLASSES
#   UNGUARDED       a self-invoke reachable with no bounding condition.
#                   The real defect. Severity 1.
#   BOUNDED_COUNTER guarded by a numeric comparison. The bound is
#                   REPORTED, because a bound is not automatically safe:
#                   >= 16 will be broken by AWS mid-walk, and a low bound
#                   silently caps convergence (MAX_HOPS=10 was capping
#                   clone-alpha's backfill at ten hops a week).
#   BOUNDED_FLAG    the self-invoke stamps a key into its own payload
#                   that the handler reads to disable the same branch —
#                   the async-kickoff pattern, structurally depth 2.
#   NO_SELF_INVOKE  clean.
#
# Scanning 766 packages is expensive, so results are cached by
# CodeSha256 — the artifact's own identity. Unchanged code is never
# re-downloaded, and the walk carries a durable cursor so a run that
# runs out of clock resumes instead of restarting.
# ---------------------------------------------------------------------

D1_CACHE_KEY = "data/_state/d1-classification-cache.json"
D1_CURSOR_KEY = "data/_state/d1-scan-cursor.json"
D1_RESERVE_MS = 90000


def _module_ints(tree):
    """Module-level integer constants, including the
    int(os.environ.get("X", "12")) idiom these engines favour."""
    out = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        t = node.targets[0]
        if not isinstance(t, ast.Name):
            continue
        v = node.value
        if isinstance(v, ast.Constant) and isinstance(v.value, int):
            out[t.id] = v.value
        elif isinstance(v, ast.Call) and getattr(v.func, "id", "") == "int":
            for a in ast.walk(v):
                if isinstance(a, ast.Constant) and \
                        isinstance(a.value, str) and a.value.isdigit():
                    out[t.id] = int(a.value)
                    break
    return out


def _is_self_target(node, fname):
    """Does this expression name the function's own identity?"""
    for a in ast.walk(node):
        if isinstance(a, ast.Attribute) and a.attr == "function_name":
            return True
        if isinstance(a, ast.Constant) and isinstance(a.value, str):
            if a.value == fname or a.value.endswith("/" + fname):
                return True
        if isinstance(a, ast.Constant) and \
                a.value == "AWS_LAMBDA_FUNCTION_NAME":
            return True
    return False


def _payload_literal_keys(call):
    """Literal dict keys the self-invoke stamps into its own payload."""
    keys = []
    for a in ast.walk(call):
        if isinstance(a, ast.Dict):
            for k in a.keys:
                if isinstance(k, ast.Constant) and isinstance(k.value, str):
                    keys.append(k.value)
    return keys


def classify_source(src, fname):
    """Returns (class, detail_dict). Pure function — self-testable."""
    try:
        tree = ast.parse(src)
    except Exception as e:
        return "UNPARSEABLE", {"error": str(e)[:90]}

    parent = {}
    for node in ast.walk(tree):
        for ch in ast.iter_child_nodes(node):
            parent[ch] = node

    consts = _module_ints(tree)
    findings = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        if not (isinstance(f, ast.Attribute) and f.attr == "invoke"):
            continue
        target = None
        for kw in node.keywords:
            if kw.arg == "FunctionName":
                target = kw.value
        if target is None and node.args:
            target = node.args[0]
        if target is None or not _is_self_target(target, fname):
            continue

        # walk up collecting every enclosing If test
        bound, flag = None, None
        cur = node
        seen = 0
        while cur in parent and seen < 40:
            cur = parent[cur]
            seen += 1
            if not isinstance(cur, ast.If):
                continue
            for c in ast.walk(cur.test):
                if isinstance(c, ast.Compare):
                    for side in [c.left] + list(c.comparators):
                        if isinstance(side, ast.Constant) and \
                                isinstance(side.value, int):
                            bound = side.value if bound is None \
                                else min(bound, side.value)
                        elif isinstance(side, ast.Name) and \
                                side.id in consts:
                            bv = consts[side.id]
                            bound = bv if bound is None else min(bound, bv)
        pkeys = _payload_literal_keys(node)
        for k in pkeys:
            if ('get("%s")' % k) in src or ("get('%s')" % k) in src:
                flag = k
                break
        if bound is not None:
            findings.append(("BOUNDED_COUNTER", {"bound": bound}))
        elif flag:
            findings.append(("BOUNDED_FLAG", {"flag": flag}))
        else:
            findings.append(("UNGUARDED", {}))

    if not findings:
        return "NO_SELF_INVOKE", {}
    order = {"UNGUARDED": 0, "BOUNDED_FLAG": 1, "BOUNDED_COUNTER": 2}
    findings.sort(key=lambda x: order.get(x[0], 3))
    return findings[0][0], findings[0][1]


SELFTEST = [
    ("unguarded", "fn_a", """
import boto3, json
lam = boto3.client("lambda")
def lambda_handler(event, context):
    lam.invoke(FunctionName=context.function_name,
               InvocationType="Event", Payload=json.dumps({"c": 1}).encode())
""", "UNGUARDED"),
    ("counter", "fn_b", """
import boto3, json
MAX_HOPS = 10
lam = boto3.client("lambda")
def lambda_handler(event, context):
    hop = int(event.get("hop") or 0)
    if not complete and hop < MAX_HOPS:
        lam.invoke(FunctionName=context.function_name,
                   InvocationType="Event",
                   Payload=json.dumps({"hop": hop + 1}).encode())
""", "BOUNDED_COUNTER"),
    ("kickoff", "fn_c", """
import boto3, json
lam = boto3.client("lambda")
def lambda_handler(event, context):
    is_internal = event.get("_internal") == "1"
    kickoff = not is_internal
    if kickoff:
        lam.invoke(FunctionName=context.function_name,
                   InvocationType="Event",
                   Payload=json.dumps({"_internal": "1"}).encode())
""", "BOUNDED_FLAG"),
    ("clean", "fn_d", """
import boto3
lam = boto3.client("lambda")
def lambda_handler(event, context):
    lam.invoke(FunctionName="some-other-function", InvocationType="Event")
""", "NO_SELF_INVOKE"),
]


def run_selftest():
    results = []
    for name, fname, src, expect in SELFTEST:
        got, det = classify_source(src, fname)
        results.append({"case": name, "expect": expect, "got": got,
                        "pass": got == expect, "detail": det})
    return {"passed": all(r["pass"] for r in results), "cases": results}


def d1_scan(context):
    """Incremental, sha-cached AST scan of the fleet."""
    try:
        cache = json.loads(s3.get_object(Bucket=BUCKET,
                                         Key=D1_CACHE_KEY)["Body"].read())
    except Exception:
        cache = {}
    fns = []
    for page in lam.get_paginator("list_functions").paginate():
        for f in page["Functions"]:
            fns.append((f["FunctionName"], f.get("CodeSha256")))
    fns.sort()
    try:
        cur = int(json.loads(s3.get_object(
            Bucket=BUCKET, Key=D1_CURSOR_KEY)["Body"].read()).get("cursor", 0))
        if cur >= len(fns):
            cur = 0
    except Exception:
        cur = 0

    from urllib.request import urlopen
    import io as _io
    import zipfile as _zip
    scanned = cached = failed = 0
    while cur < len(fns):
        if context is not None:
            try:
                if context.get_remaining_time_in_millis() < D1_RESERVE_MS:
                    break
            except Exception:
                pass
        fname, sha = fns[cur]
        cur += 1
        if not sha:
            failed += 1
            continue
        if cache.get(sha, {}).get("fn") == fname:
            cached += 1
            continue
        try:
            loc = lam.get_function(FunctionName=fname)["Code"]["Location"]
            z = _zip.ZipFile(_io.BytesIO(urlopen(loc, timeout=45).read()))
            src = ""
            for n in z.namelist():
                if n.endswith("lambda_function.py"):
                    src = z.read(n).decode("utf-8", "ignore")
                    break
            if not src:
                failed += 1
                continue
            cls, det = classify_source(src, fname)
            cache[sha] = {"fn": fname, "cls": cls, "detail": det}
            scanned += 1
        except Exception as e:
            print("[d1] %s: %s" % (fname, str(e)[:80]))
            failed += 1

    complete = cur >= len(fns)
    s3.put_object(Bucket=BUCKET, Key=D1_CACHE_KEY,
                  Body=json.dumps(cache).encode(),
                  ContentType="application/json")
    s3.put_object(Bucket=BUCKET, Key=D1_CURSOR_KEY,
                  Body=json.dumps({"cursor": 0 if complete else cur,
                                   "total": len(fns), "complete": complete,
                                   "at": _now().isoformat()}).encode(),
                  ContentType="application/json")
    return {"scanned": scanned, "from_cache": cached, "failed": failed,
            "cursor": 0 if complete else cur, "total": len(fns),
            "complete": complete, "cache_entries": len(cache)}


def d1_findings():
    """Read the cache and emit only genuine defects."""
    try:
        cache = json.loads(s3.get_object(Bucket=BUCKET,
                                         Key=D1_CACHE_KEY)["Body"].read())
    except Exception:
        return []
    by_fn = {}
    for sha, v in cache.items():
        by_fn[v.get("fn")] = v
    out = []
    for fn, v in sorted(by_fn.items()):
        cls, det = v.get("cls"), v.get("detail") or {}
        if cls == "UNGUARDED":
            out.append({"id": fn,
                        "detail": "self-invokes with no bounding condition "
                                  "— AWS will break the chain at depth 16 "
                                  "and the remainder of the walk is lost "
                                  "silently"})
        elif cls == "BOUNDED_COUNTER" and det.get("bound", 0) >= 16:
            out.append({"id": fn,
                        "detail": "self-invoke bound is %s, at or above the "
                                  "depth 16 at which AWS breaks the chain"
                                  % det.get("bound")})
    return out


DEPRECATED = {"python3.6", "python3.7", "python3.8", "nodejs12.x",
              "nodejs14.x", "nodejs16.x", "ruby2.7", "go1.x", "dotnet6"}

# Severity drives the page ordering and the alarm. SEV1 = something is
# silently wrong RIGHT NOW; SEV2 = degradation; SEV3 = hygiene.
SEV = {
    "D11_code_storage": 1, "D4_orphan_rule": 1, "D6_scheduled_never_ran": 1,
    "D1_recursion": 1, "D2_timeout_clipped": 1, "D3_double_fire": 1,
    "D8_errors": 2, "D7_concurrency_drop": 2, "D9_no_dlq": 2,
    "D12_stale_env": 2, "D10_runtime": 3, "D13_dead": 3,
}


def _now():
    return datetime.now(timezone.utc)


def emf(defects_by_class, n_fn, storage_pct, new_count):
    """CloudWatch Embedded Metric Format. One log line, no API calls,
    no per-metric request billing. CloudWatch parses this automatically."""
    metrics = [{"Name": "DefectsTotal", "Unit": "Count"},
               {"Name": "DefectsNew", "Unit": "Count"},
               {"Name": "FleetSize", "Unit": "Count"},
               {"Name": "CodeStoragePct", "Unit": "Percent"}]
    doc = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": "JustHodl/Integrity",
                "Dimensions": [[]],
                "Metrics": metrics,
            }],
        },
        "DefectsTotal": sum(len(v) for v in defects_by_class.values()),
        "DefectsNew": new_count,
        "FleetSize": n_fn,
        "CodeStoragePct": round(storage_pct, 1),
    }
    for k, v in defects_by_class.items():
        doc[k] = len(v)
    print(json.dumps(doc))


def collect_metrics(names):
    """Batched GetMetricData. One request per 100 functions rather than
    three per function — the shape that caused the old $66/mo line."""
    spec = [("Invocations", "Sum", "inv"), ("Errors", "Sum", "err"),
            ("Throttles", "Sum", "thr"), ("Duration", "Maximum", "dmax"),
            ("Duration", "Average", "davg")]
    end = _now()
    start = end - timedelta(days=14)
    out = {}
    for i in range(0, len(names), 100):
        chunk = names[i:i + 100]
        q = []
        for j, fn in enumerate(chunk):
            for mt, st, tag in spec:
                q.append({
                    "Id": "m%s_%d" % (tag, j),
                    "MetricStat": {
                        "Metric": {"Namespace": "AWS/Lambda",
                                   "MetricName": mt,
                                   "Dimensions": [{"Name": "FunctionName",
                                                   "Value": fn}]},
                        "Period": 1209600, "Stat": st},
                    "ReturnData": True})
        try:
            res = cw.get_metric_data(MetricDataQueries=q, StartTime=start,
                                     EndTime=end,
                                     ScanBy="TimestampDescending")
        except Exception as e:
            print("[integrity] metric batch %d: %s" % (i, str(e)[:120]))
            continue
        v = {}
        for r in res["MetricDataResults"]:
            vals = r["Values"]
            if not vals:
                v[r["Id"]] = 0.0
            elif r["Id"].startswith("mdmax"):
                v[r["Id"]] = max(vals)
            elif r["Id"].startswith("mdavg"):
                v[r["Id"]] = sum(vals) / len(vals)
            else:
                v[r["Id"]] = sum(vals)
        for j, fn in enumerate(chunk):
            out[fn] = {t: v.get("m%s_%d" % (t, j), 0.0)
                       for _, _, t in spec}
    return out


def schedule_map():
    """fn -> [(kind, rule, expr, state)] plus orphans and duplicate
    targets, gathered in ONE pass so the page and the reconciler agree."""
    wired, orphans, dupes = {}, [], []
    try:
        for page in evb.get_paginator("list_rules").paginate():
            for r in page["Rules"]:
                try:
                    tg = evb.list_targets_by_rule(Rule=r["Name"])
                except Exception:
                    continue
                sigs = set()
                for t in tg.get("Targets", []):
                    arn = t.get("Arn", "")
                    if ":function:" not in arn:
                        continue
                    sig = json.dumps({"a": arn, "i": t.get("Input"),
                                      "p": t.get("InputPath")},
                                     sort_keys=True)
                    if sig in sigs:
                        dupes.append({"rule": r["Name"],
                                      "fn": arn.split(":")[-1]})
                    sigs.add(sig)
                    if r.get("ScheduleExpression"):
                        wired.setdefault(arn.split(":")[-1], []).append(
                            {"kind": "events", "rule": r["Name"],
                             "expr": r["ScheduleExpression"],
                             "state": r.get("State")})
    except Exception as e:
        print("[integrity] rules: %s" % str(e)[:120])
    try:
        for page in sch.get_paginator("list_schedules").paginate():
            for s_ in page["Schedules"]:
                g = s_.get("GroupName", "default")
                try:
                    d = sch.get_schedule(Name=s_["Name"], GroupName=g)
                except Exception:
                    continue
                arn = (d.get("Target", {}) or {}).get("Arn", "") or ""
                if ":function:" not in arn:
                    continue
                wired.setdefault(arn.split(":")[-1], []).append(
                    {"kind": "scheduler", "rule": s_["Name"],
                     "expr": d.get("ScheduleExpression"),
                     "state": d.get("State"), "group": g})
    except Exception as e:
        print("[integrity] schedules: %s" % str(e)[:120])
    return wired, orphans, dupes


def audit():
    D = {}

    def add(cls, item):
        D.setdefault(cls, []).append(item)

    fns = {}
    for page in lam.get_paginator("list_functions").paginate():
        for f in page["Functions"]:
            fns[f["FunctionName"]] = f
    names = sorted(fns)

    # ---- D11 code storage
    storage_pct = 0.0
    try:
        a = lam.get_account_settings()
        used = a["AccountUsage"]["TotalCodeSize"]
        limit = a["AccountLimit"]["TotalCodeSize"]
        storage_pct = 100.0 * used / limit
        if storage_pct > 70:
            add("D11_code_storage",
                {"id": "account", "pct": round(storage_pct, 1),
                 "detail": "at 100%% every deploy in the account fails"})
    except Exception as e:
        print("[integrity] account settings: %s" % str(e)[:100])

    for f in d1_findings():
        add("D1_recursion", f)

    M = collect_metrics(names)
    wired, orphans, dupes = schedule_map()

    for d in dupes:
        add("D3_double_fire",
            {"id": d["rule"], "detail": "target %s listed twice — fires "
                                        "twice per tick" % d["fn"]})

    live = set(names)
    for fn in list(wired):
        if fn not in live:
            for w in wired[fn]:
                add("D4_orphan_rule",
                    {"id": w["rule"],
                     "detail": "targets missing function %s (%s)"
                               % (fn, w["expr"])})

    enabled = {f: [w for w in v if (w.get("state") or "ENABLED") == "ENABLED"]
               for f, v in wired.items() if f in live}
    enabled = {f: v for f, v in enabled.items() if v}

    for fn, v in enabled.items():
        m = M.get(fn, {})
        if m.get("inv", 0) == 0:
            add("D6_scheduled_never_ran",
                {"id": fn, "detail": "scheduled %s but 0 invocations in 14d"
                                     % ", ".join(str(x["expr"]) for x in v)})

    for fn in names:
        m = M.get(fn, {})
        to_ms = fns[fn].get("Timeout", 3) * 1000.0
        if m.get("inv", 0) < 3 or to_ms <= 0:
            continue
        if m.get("dmax", 0) >= to_ms * 0.97 and \
                m.get("davg", 0) >= to_ms * 0.60:
            add("D2_timeout_clipped",
                {"id": fn,
                 "detail": "avg %.0fs of a %.0fs ceiling — the tail of "
                           "every run is being cut off"
                           % (m["davg"] / 1000, to_ms / 1000)})
        er = (100.0 * m.get("err", 0) / m["inv"]) if m.get("inv") else 0
        if m.get("inv", 0) >= 5 and (er >= 20 or m.get("thr", 0) > 0):
            add("D8_errors",
                {"id": fn, "detail": "%.0f%% error rate (%d/%d), %d throttles"
                                     % (er, m.get("err", 0), m["inv"],
                                        m.get("thr", 0))})
        if m.get("thr", 0) > 0:
            try:
                rc = lam.get_function_concurrency(
                    FunctionName=fn).get("ReservedConcurrentExecutions")
            except Exception:
                rc = None
            if rc is not None and rc <= 2:
                add("D7_concurrency_drop",
                    {"id": fn, "detail": "reserved=%s with %d throttled "
                                         "runs — those runs were DROPPED, "
                                         "not delayed" % (rc, m["thr"])})
        if fns[fn].get("Runtime") in DEPRECATED:
            add("D10_runtime", {"id": fn,
                                "detail": "runtime %s is deprecated"
                                          % fns[fn].get("Runtime")})
        if m.get("err", 0) > 0 and fn in enabled:
            has = bool((fns[fn].get("DeadLetterConfig") or {}).get(
                "TargetArn"))
            if not has:
                try:
                    c = lam.get_function_event_invoke_config(FunctionName=fn)
                    has = bool((c.get("DestinationConfig") or {}).get(
                        "OnFailure", {}).get("Destination"))
                except Exception:
                    has = False
            if not has:
                add("D9_no_dlq",
                    {"id": fn, "detail": "%d failed events with no DLQ or "
                                         "on-failure destination — they "
                                         "vanished" % m["err"]})
        ev = (fns[fn].get("Environment") or {}).get("Variables") or {}
        blob = json.dumps(ev)
        for pat in ("es.amazonaws.com", "elb.amazonaws.com",
                    "awsapprunner.com"):
            if pat in blob:
                add("D12_stale_env",
                    {"id": fn, "detail": "env references %s — verify the "
                                         "resource still exists" % pat})
                break
        if fn not in wired and m.get("inv", 0) == 0:
            add("D13_dead", {"id": fn,
                             "detail": "no schedule, no invocations in 14d"})

    return D, len(names), storage_pct


def lambda_handler(event=None, context=None):
    event = event or {}
    mode = (event.get("mode") or "audit").lower()
    t0 = time.time()

    if mode == "selftest":
        r = run_selftest()
        print("[d1] selftest passed=%s" % r["passed"])
        for c in r["cases"]:
            print("[d1] %-10s expect=%-16s got=%-16s %s"
                  % (c["case"], c["expect"], c["got"],
                     "OK" if c["pass"] else "FAIL"))
        return {"ok": r["passed"], "mode": "selftest", **r}

    if mode == "d1scan":
        r = d1_scan(context)
        print("[d1] scan %s" % json.dumps(r))
        return {"ok": True, "mode": "d1scan", **r}

    D, n_fn, storage_pct = audit()

    # ---- baseline diff: report what is NEW, not the whole backlog
    keys_now = {"%s|%s" % (c, i["id"]) for c, v in D.items() for i in v}
    try:
        base = set(json.loads(s3.get_object(
            Bucket=BUCKET, Key=BASELINE)["Body"].read()).get("keys") or [])
    except Exception:
        base = set()
    new = sorted(keys_now - base)
    fixed = sorted(base - keys_now)

    rows = []
    for cls, items in D.items():
        for it in items:
            k = "%s|%s" % (cls, it["id"])
            rows.append({"cls": cls, "sev": SEV.get(cls, 3),
                         "id": it["id"], "detail": it["detail"],
                         "is_new": k in (keys_now - base)})
    rows.sort(key=lambda r: (r["sev"], not r["is_new"], r["cls"], r["id"]))

    doc = {"version": VERSION, "marker": MARKER,
           "generated_at": _now().isoformat(),
           "fleet_size": n_fn, "code_storage_pct": round(storage_pct, 1),
           "totals": {c: len(v) for c, v in D.items()},
           "n_defects": len(rows), "n_new": len(new), "n_fixed": len(fixed),
           "new": new, "fixed": fixed, "rows": rows,
           "sev1": sum(1 for r in rows if r["sev"] == 1),
           "sev2": sum(1 for r in rows if r["sev"] == 2),
           "sev3": sum(1 for r in rows if r["sev"] == 3),
           "elapsed_s": round(time.time() - t0, 1)}

    s3.put_object(Bucket=BUCKET, Key=ARTIFACT,
                  Body=json.dumps(doc).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=300")

    if mode == "baseline" or not base:
        s3.put_object(Bucket=BUCKET, Key=BASELINE,
                      Body=json.dumps({"keys": sorted(keys_now),
                                       "at": _now().isoformat()}).encode(),
                      ContentType="application/json")
        print("[integrity] baseline set (%d keys)" % len(keys_now))

    emf(D, n_fn, storage_pct, len(new))
    print("[integrity] defects=%d new=%d fixed=%d sev1=%d elapsed=%.0fs"
          % (len(rows), len(new), len(fixed), doc["sev1"], doc["elapsed_s"]))
    for k in new[:25]:
        print("[integrity] NEW %s" % k)
    return {"ok": True, "n_defects": len(rows), "n_new": len(new),
            "n_fixed": len(fixed), "sev1": doc["sev1"]}
