"""
ops_4252 — CAPACITY & MALFUNCTION AUDIT.

The census ran at 25% for months because its truncation looked like
success. Today's control loops catch that class going FORWARD; this op
looks BACKWARD, because the contract gate's row floors were learned from
current state — an engine that collapsed before today had its collapsed
output blessed as the baseline. Detection needs history, and the one
history that exists for every function is CloudWatch: 15 months of
Duration and Invocations. An engine whose average duration fell 65%
and stayed there is doing a fraction of its former work no matter what
its exit code says.

SECTIONS
  A  INFRA: enable S3 versioning on the LIVE bucket — every artifact
     overwrite becomes reversible and future contracts get real
     history — PAIRED with a 14-day noncurrent-version lifecycle,
     because versioning without lifecycle on a bucket rewritten hourly
     is a storage leak, not a safety net. Existing lifecycle rules are
     MERGED, never clobbered. Both verified by read-back.
  B  WORK-TRAJECTORY, fleet-wide, 60 days daily: for every function
     still being invoked, compare recent active-day mean duration to its
     own 60-day p75 baseline. Sustained ratio < 0.35 on a non-trivial
     baseline = WORK COLLAPSE — the 20%-capacity class, named per
     engine with the ratio and the dates.
  C  STALE TRIAGE with DEATH DATES: join the 129 cadence-STALE
     artifacts to their producers and classify each with evidence:
       PRODUCER-DEAD       zero invocations 14d; death-dated from the
                           last active day in the 60d series
       PRODUCER-ERRORING   invoked but failing
       RUNS-BUT-SILENT     invoked, healthy, artifact still frozen —
                           the write path itself is dead. Nastiest.
       EVENT-DRIVEN?       state/alert/config-shaped keys; human review
  D  ERROR RE-MEASURE: the 14 engines whose timeouts ops 4234 raised,
     24h post-change, against their recorded baselines.
  E  Deploy contract-gate v1.2.0 — daily row-count history, so from
     today collapse detection has data instead of memory. Gated:
     self-test 5/5, marker in the deployed zip, and the dated history
     object actually exists after a live check.

Findings are findings — the op fails only on its own mechanics.
"""

import io
import json
import os
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"},
             read_timeout=180)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)
NOW = datetime.now(timezone.utc)
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
OUT = {"ops": 4252, "ts": NOW.isoformat()}

CG = "justhodl-contract-gate"
CG_MARK = "contract-gate v1.2.0 ops4252 rowcount-history"
QUAR = {"macro-report-api", "multi-agent-orchestrator",
        "nyfed-financial-stability-fetcher", "nyfed-primary-dealer-fetcher",
        "nyfedapi-isolated", "ultimate-multi-agent"}


def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root:
                continue
            for f in files:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, src))
    return buf.getvalue()


def wait_active(fn, b=200):
    t0 = time.time()
    while time.time() - t0 < b:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and \
                    c.get("LastUpdateStatus") in (None, "Successful"):
                return True
        except Exception:
            pass
        time.sleep(4)
    return False


def series(names, metric, stat):
    """60 daily datapoints per function, batched."""
    start, end = NOW - timedelta(days=60), NOW
    out = {}
    for i in range(0, len(names), 240):
        chunk = names[i:i + 240]
        q = [{"Id": "m%d" % j,
              "MetricStat": {"Metric": {"Namespace": "AWS/Lambda",
                                        "MetricName": metric,
                                        "Dimensions": [
                                            {"Name": "FunctionName",
                                             "Value": fn}]},
                             "Period": 86400, "Stat": stat},
              "ReturnData": True} for j, fn in enumerate(chunk)]
        nxt = None
        vals = {}
        while True:
            kw = {"MetricDataQueries": q, "StartTime": start,
                  "EndTime": end, "ScanBy": "TimestampAscending"}
            if nxt:
                kw["NextToken"] = nxt
            res = cw.get_metric_data(**kw)
            for r in res["MetricDataResults"]:
                vals.setdefault(r["Id"], {"t": [], "v": []})
                vals[r["Id"]]["t"] += [t.strftime("%m-%d")
                                       for t in r["Timestamps"]]
                vals[r["Id"]]["v"] += r["Values"]
            nxt = res.get("NextToken")
            if not nxt:
                break
        for j, fn in enumerate(chunk):
            out[fn] = vals.get("m%d" % j, {"t": [], "v": []})
    return out


def p75(xs):
    if not xs:
        return 0.0
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(0.75 * len(xs)))]


with report("4252_capacity_audit") as rep:
    rep.heading("ops 4252 — capacity & malfunction audit")
    fails = []

    # ================================================================ A
    rep.section("A. Versioning + lifecycle on the LIVE bucket")
    try:
        v = s3.get_bucket_versioning(Bucket=BUCKET).get("Status")
        rep.log("versioning before: %s" % (v or "Disabled"))
        if v != "Enabled":
            s3.put_bucket_versioning(
                Bucket=BUCKET,
                VersioningConfiguration={"Status": "Enabled"})
        v2 = s3.get_bucket_versioning(Bucket=BUCKET).get("Status")
        (rep.ok if v2 == "Enabled" else rep.fail)(
            "versioning read-back: %s — every artifact overwrite is now "
            "reversible" % v2)
        if v2 != "Enabled":
            fails.append("versioning")
        try:
            cur = s3.get_bucket_lifecycle_configuration(
                Bucket=BUCKET).get("Rules", [])
        except Exception:
            cur = []
        cur = [r for r in cur if r.get("ID") != "jh-noncurrent-14d"]
        cur.append({"ID": "jh-noncurrent-14d", "Status": "Enabled",
                    "Filter": {},
                    "NoncurrentVersionExpiration": {"NoncurrentDays": 14},
                    "AbortIncompleteMultipartUpload":
                        {"DaysAfterInitiation": 3}})
        s3.put_bucket_lifecycle_configuration(
            Bucket=BUCKET, LifecycleConfiguration={"Rules": cur})
        back = s3.get_bucket_lifecycle_configuration(
            Bucket=BUCKET).get("Rules", [])
        got = any(r.get("ID") == "jh-noncurrent-14d" and
                  r.get("Status") == "Enabled" for r in back)
        (rep.ok if got else rep.fail)(
            "lifecycle read-back: %d rule(s), jh-noncurrent-14d=%s — "
            "old versions expire at 14d, so this is a safety net, not a "
            "storage leak (%d pre-existing rule(s) preserved)"
            % (len(back), got, len(back) - 1))
        if not got:
            fails.append("lifecycle")
    except Exception as e:
        fails.append("versioning/lifecycle: %s" % str(e)[:160])

    # ---------------------------------------------------------------- load
    fns = []
    for page in lam.get_paginator("list_functions").paginate():
        fns += [f["FunctionName"] for f in page["Functions"]]
    fns = sorted(set(fns) - QUAR)
    rep.log("fleet under audit: %d functions (quarantined excluded)"
            % len(fns))
    dur = series(fns, "Duration", "Average")
    inv = series(fns, "Invocations", "Sum")
    err = series(fns, "Errors", "Sum")

    # ================================================================ B
    rep.section("B. WORK COLLAPSE — the 20%%-capacity class, fleet-wide")
    collapses = []
    for fn in fns:
        d, iv = dur.get(fn, {}), inv.get(fn, {})
        pairs = [(t, dv, ivv) for t, dv, ivv in
                 zip(d.get("t", []), d.get("v", []),
                     iv.get("v", []) + [0] * 99) if ivv and ivv > 0]
        if len(pairs) < 14:
            continue
        base = p75([dv for _, dv, _ in pairs[:-7]])
        recent_pairs = pairs[-5:]
        recent = sum(dv for _, dv, _ in recent_pairs) / len(recent_pairs)
        if base < 8000:
            continue
        ratio = recent / base if base else 1.0
        if ratio < 0.35:
            since = None
            for t, dv, _ in pairs:
                if dv < base * 0.35:
                    since = t
                else:
                    since = None
            collapses.append({"fn": fn, "base_s": round(base / 1000, 1),
                              "now_s": round(recent / 1000, 1),
                              "pct_of_former": round(ratio * 100),
                              "since": since or recent_pairs[0][0]})
    collapses.sort(key=lambda x: x["pct_of_former"])
    rep.log("engines still running at a FRACTION of their former work: %d"
            % len(collapses))
    for c in collapses[:25]:
        rep.fail("  %-42s was %6.1fs -> now %5.1fs  (%d%% of former, "
                 "since ~%s)"
                 % (c["fn"][:42], c["base_s"], c["now_s"],
                    c["pct_of_former"], c["since"]))
        rep.kv(section="work_collapse", **c)
    if not collapses:
        rep.ok("no sustained duration collapse anywhere in the fleet — "
               "the census was the only member of its class")
    OUT["work_collapse"] = collapses

    # ================================================================ C
    rep.section("C. STALE triage — who died, and WHEN")
    try:
        viol = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/contract-violations.json"
        )["Body"].read())
        prod = json.loads(s3.get_object(
            Bucket=BUCKET, Key="config/artifact-producers.json"
        )["Body"].read()).get("producers", {})
        stale = [v for v in viol.get("violations", [])
                 if v["cls"] == "STALE"]
        rep.log("cadence-STALE artifacts: %d" % len(stale))
        classes = {"PRODUCER-DEAD": [], "PRODUCER-ERRORING": [],
                   "RUNS-BUT-SILENT": [], "EVENT-DRIVEN?": [],
                   "NO-PRODUCER-MAPPED": []}
        for v in stale:
            key = v["artifact"]
            base = key.split("/")[-1]
            if any(w in base for w in ("alert", "state", "config",
                                       "probe", "manifest", "snapshot",
                                       "crisis", "history")):
                classes["EVENT-DRIVEN?"].append((key, "-", v["detail"]))
                continue
            pfns = [f for f in prod.get(key, []) if f not in QUAR]
            if not pfns:
                classes["NO-PRODUCER-MAPPED"].append((key, "-",
                                                      v["detail"]))
                continue
            best = None
            for fn in pfns:
                iv = inv.get(fn, {"t": [], "v": []})
                ev = err.get(fn, {"v": []})
                recent14 = sum(x for t, x in zip(iv["t"], iv["v"])
                               if True) and sum(iv["v"][-14:])
                tot_err = sum(ev.get("v", [])[-14:]) if ev else 0
                active = [t for t, x in zip(iv["t"], iv["v"]) if x > 0]
                last = active[-1] if active else "60d+"
                row = (fn, recent14 or 0, tot_err, last)
                if best is None or row[1] > best[1]:
                    best = row
            fn, r14, e14, last = best
            if (r14 or 0) == 0:
                classes["PRODUCER-DEAD"].append(
                    (key, fn, "last ran %s" % last))
            elif r14 and e14 / max(r14, 1) >= 0.3:
                classes["PRODUCER-ERRORING"].append(
                    (key, fn, "%d/%d failing" % (int(e14), int(r14))))
            else:
                classes["RUNS-BUT-SILENT"].append(
                    (key, fn, "%d clean runs 14d, artifact frozen — "
                              "write path dead" % int(r14)))
        for cls in ("RUNS-BUT-SILENT", "PRODUCER-DEAD",
                    "PRODUCER-ERRORING", "NO-PRODUCER-MAPPED",
                    "EVENT-DRIVEN?"):
            rows = classes[cls]
            rep.log("")
            rep.log("%s: %d" % (cls, len(rows)))
            for key, fn, why in rows[:18]:
                (rep.fail if cls in ("RUNS-BUT-SILENT", "PRODUCER-DEAD")
                 else rep.warn)(
                    "   %-42s %-34s %s"
                    % (key.split("/")[-1][:42], fn[:34], why[:60]))
                rep.kv(section=cls.lower().replace("?", "").replace("-", "_"),
                       artifact=key, producer=fn, evidence=why[:90])
        OUT["stale_triage"] = {k: len(v) for k, v in classes.items()}
    except Exception as e:
        fails.append("stale triage: %s" % str(e)[:160])

    # ================================================================ D
    rep.section("D. Error re-measure — the 14 raised timeouts, 24h on")
    try:
        base = json.loads((ROOT / "aws" / "ops" / "reports" /
                           "4234_defect_remediation.json").read_text())
        raised = sorted({c["fn"] for c in base.get("changes", [])
                         if c.get("a") == "timeout"}
                        | {"justhodl-signal-scorecard"})
        diag = base.get("diagnoses", {})
        fixed = better = same = worse = 0
        for fn in raised:
            iv = inv.get(fn, {"v": []})["v"][-1:] or [0]
            ev = err.get(fn, {"v": []})["v"][-1:] or [0]
            i24, e24 = sum(iv), sum(ev)
            pct = 100.0 * e24 / i24 if i24 else None
            b = (diag.get(fn) or {}).get("err_pct")
            if i24 == 0:
                verdict = "idle"
            elif pct <= 2:
                verdict = "FIXED"
                fixed += 1
            elif b and pct < b - 10:
                verdict = "improved"
                better += 1
            elif b and pct > b + 10:
                verdict = "WORSE"
                worse += 1
            else:
                verdict = "same"
                same += 1
            rep.log("   %-40s before=%s%%  now=%s%%  runs=%d  %s"
                    % (fn[:40],
                       "%.0f" % b if b is not None else "?",
                       "%.0f" % pct if pct is not None else "-",
                       int(i24), verdict))
            rep.kv(section="remeasure", function=fn, before=b,
                   now=(round(pct, 1) if pct is not None else None),
                   runs=int(i24), verdict=verdict)
        rep.log("fixed=%d improved=%d same=%d worse=%d"
                % (fixed, better, same, worse))
    except Exception as e:
        rep.warn("re-measure: %s" % str(e)[:140])

    # ================================================================ E
    rep.section("E. Deploy contract-gate v1.2.0 (row-count history)")
    try:
        wait_active(CG)
        lam.update_function_code(FunctionName=CG, ZipFile=zip_fn(CG))
        ok = False
        for i in range(30):
            time.sleep(6)
            try:
                loc = lam.get_function(FunctionName=CG)["Code"]["Location"]
                src = zipfile.ZipFile(
                    io.BytesIO(urlopen(loc, timeout=60).read())
                ).read("lambda_function.py").decode("utf-8", "ignore")
                if CG_MARK in src:
                    ok = True
                    break
            except Exception:
                pass
        (rep.ok if ok else rep.fail)("marker %s"
                                     % ("verified" if ok else "MISSING"))
        if not ok:
            fails.append("gate marker")
        wait_active(CG)
        r = lam.invoke(FunctionName=CG, InvocationType="RequestResponse",
                       Payload=json.dumps({"mode": "selftest"}).encode())
        b = json.loads(r["Payload"].read() or b"{}")
        (rep.ok if b.get("passed") else rep.fail)(
            "self-test %s" % ("5/5" if b.get("passed") else "FAILED"))
        if not b.get("passed"):
            fails.append("gate selftest")
        wait_active(CG)
        r = lam.invoke(FunctionName=CG, InvocationType="RequestResponse",
                       Payload=json.dumps({"mode": "check"}).encode())
        b = json.loads(r["Payload"].read() or b"{}")
        rep.log("check -> %s" % json.dumps(b)[:220])
        key = "data/_state/rowcounts/%s.json" % NOW.strftime("%Y%m%d")
        h = s3.head_object(Bucket=BUCKET, Key=key)
        rep.ok("history object EXISTS: %s (%d bytes) — collapse "
               "detection has ground truth from today forward"
               % (key, h["ContentLength"]))
    except Exception as e:
        fails.append("gate deploy: %s" % str(e)[:170])

    (ROOT / "aws" / "ops" / "reports" / "4252_capacity_audit.json"
     ).write_text(json.dumps(OUT, indent=1, default=str), encoding="utf-8")

    rep.section("RESULT")
    if fails:
        for f in fails:
            rep.fail("  %s" % f)
        raise SystemExit("FAILS: %s" % "; ".join(fails[:3]))
    rep.ok("OPS 4252 PASS — findings above are the product")
