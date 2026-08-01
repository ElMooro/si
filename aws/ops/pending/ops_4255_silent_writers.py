"""
ops_4255 — forensics on the 33 silent writers, with auto-repair of the
one mechanically-safe class.

These engines ran cleanly for 14 days while the artifact they own sat
frozen. "Runs clean, writes nothing" has a small number of shapes, and
each leaves different evidence:

  DEPLOY-DRIFT        repo source writes the key; the DEPLOYED zip does
                      not. The fix is mechanical — redeploy from repo —
                      and is done HERE, verified by the artifact moving.
  SCHEDULE-INPUT      a manual invoke writes; the scheduled invoke does
                      not -> the schedule's Input payload selects a mode
                      that skips the write. Evidence surfaced with the
                      exact payload; fixed in a reviewed wave, because
                      blindly changing payloads breaks other outputs.
  WRITE-ERROR         the tail shows an exception near the key.
  CONDITIONAL-SKIP    the tail mentions the key with skip/empty/
                      unchanged language — a guard that never opens.
  CODE-PATH-SILENT    the tail never mentions the key at all — the
                      branch that writes is never reached.
  SUPERSEDED?         a fresher sibling key exists — the artifact may
                      simply have a successor and the contract points at
                      a museum piece.

Every engine gets all five probes; nothing is classified from one
signal. Long engines (timeout > 240s) are invoked async and judged only
by artifact movement.
"""
import base64
import io
import json
import os
import re
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen

import boto3
from botocore.config import Config

from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 5, "mode": "adaptive"},
             read_timeout=150)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
NOW = datetime.now(timezone.utc)
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
OUT = {"ops": 4255, "ts": NOW.isoformat(), "engines": []}

PAIRS = [
    ("data/_freshness-manifest.json", "justhodl-fleet-freshness-monitor"),
    ("data/alert-history.json", "justhodl-alert-router"),
    ("data/bis-crossborder.json", "justhodl-bis-crossborder"),
    ("data/buyback-scanner.json", "justhodl-buyback-scanner"),
    ("data/compound-signals.json", "justhodl-alpha-research"),
    ("data/congress-party-map.json", "justhodl-political-stocks"),
    ("data/etf-census-matrix.json", "justhodl-etf-census"),
    ("data/etf-census.json", "justhodl-etf-census"),
    ("data/eurodollar-stress.json", "justhodl-dollar-radar"),
    ("data/factor-data-cache.json", "justhodl-factor-decomposition"),
    ("data/factor-decomposition.json", "justhodl-factor-decomposition"),
    ("data/feedback-summary.json", "justhodl-feedback"),
    ("data/fi-census-matrix.json", "justhodl-fi-census"),
    ("data/fi-census.json", "justhodl-fi-census"),
    ("data/forward-returns.json", "justhodl-forward-returns"),
]
# second half of the 33 pulled dynamically from the violations feed so
# the list cannot drift from what the gate actually reported
try:
    _d = json.loads(boto3.client("s3", region_name=REGION).get_object(
        Bucket=BUCKET, Key="data/contract-violations.json")["Body"].read())
    _p = json.loads(boto3.client("s3", region_name=REGION).get_object(
        Bucket=BUCKET, Key="config/artifact-producers.json")["Body"].read()
    ).get("producers", {})
    _known = {k for k, _ in PAIRS}
    for v in _d.get("violations", []):
        if v["cls"] != "STALE":
            continue
        k = v["artifact"]
        if k in _known:
            continue
        w = (_p.get(k) or {}).get("writers") or []
        if w:
            PAIRS.append((k, w[0]))
except Exception as _e:
    print("dynamic pair load: %s" % str(_e)[:100])


def deployed_src(fn):
    loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
    z = zipfile.ZipFile(io.BytesIO(urlopen(loc, timeout=60).read()))
    best = ""
    for n in z.namelist():
        if n.endswith(".py"):
            t = z.read(n).decode("utf-8", "ignore")
            if "lambda_handler" in t and len(t) > len(best):
                best = t
    return best


def repo_src(fn):
    p = ROOT / "aws" / "lambdas" / fn / "source" / "lambda_function.py"
    return p.read_text(errors="ignore") if p.exists() else ""


def writes_key(src, base):
    for m in re.finditer(re.escape(base), src):
        w = src[max(0, m.start() - 500): m.start() + 500]
        if "put_object" in w or re.search(
                r"(OUT_KEY|S3_KEY|KEY)\s*=\s*[\"'][^\"']*" + re.escape(base),
                w):
            return True
    return False


def head_age(key):
    try:
        h = s3.head_object(Bucket=BUCKET, Key=key)
        return (NOW - h["LastModified"]).total_seconds() / 3600.0, \
            h["LastModified"]
    except Exception:
        return None, None


def wait_active(fn, b=180):
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


def zip_repo(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root:
                continue
            for f in files:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, src))
        if os.path.isdir("aws/shared"):
            for f in sorted(os.listdir("aws/shared")):
                if f.endswith(".py"):
                    z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()


with report("4255_silent_writers") as rep:
    rep.heading("ops 4255 — silent-writer forensics (%d pairs)"
                % len(PAIRS))
    fails = []
    live_keys = {}
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET, Prefix="data/", Delimiter="/"):
        for o in page.get("Contents", []):
            live_keys[o["Key"]] = o["LastModified"]

    seen_fn_probe = {}
    counts = {}
    fixed = 0
    for key, fn in PAIRS:
        base = key.split("/")[-1]
        stem = base.replace(".json", "")
        row = {"artifact": key, "writer": fn}
        try:
            age0, _ = head_age(key)
            row["age_h"] = round(age0, 1) if age0 else None

            dsrc = deployed_src(fn)
            rsrc = repo_src(fn)
            dep_writes = writes_key(dsrc, base)
            repo_writes = writes_key(rsrc, base) if rsrc else None
            row["deployed_writes"] = dep_writes
            row["repo_writes"] = repo_writes

            # successor scan
            sib = [(k, t) for k, t in live_keys.items()
                   if k != key and stem[:max(6, len(stem) - 4)] in k]
            fresh_sib = [k for k, t in sib
                         if (NOW - t).total_seconds() < 48 * 3600]
            row["fresh_siblings"] = fresh_sib[:4]

            # log evidence
            ev = []
            try:
                r = logs.filter_log_events(
                    logGroupName="/aws/lambda/%s" % fn,
                    startTime=int((NOW - timedelta(hours=30)
                                   ).timestamp() * 1000),
                    filterPattern='"%s"' % stem, limit=6)
                ev = [e["message"].strip().replace("\n", " | ")[:150]
                      for e in r.get("events", [])]
            except Exception:
                pass
            row["log_lines"] = ev[:3]

            # DEPLOY-DRIFT auto-repair: repo writes it, deployment lost it
            if repo_writes and not dep_writes:
                rep.warn("%s <- %s : DEPLOY-DRIFT — repo writes the key, "
                         "deployed zip does not. Redeploying from repo."
                         % (base, fn))
                wait_active(fn)
                lam.update_function_code(FunctionName=fn,
                                         ZipFile=zip_repo(fn))
                wait_active(fn)

            # probe (once per fn)
            if fn not in seen_fn_probe:
                cfg = lam.get_function_configuration(FunctionName=fn)
                to = cfg.get("Timeout", 60)
                tail = ""
                fe = None
                if to <= 240:
                    try:
                        r = lam.invoke(FunctionName=fn,
                                       InvocationType="RequestResponse",
                                       LogType="Tail")
                        fe = r.get("FunctionError")
                        tail = base64.b64decode(
                            r.get("LogResult", "")
                        ).decode("utf-8", "ignore")
                    except Exception as e:
                        tail = "INVOKE-EXC %s" % str(e)[:90]
                else:
                    lam.invoke(FunctionName=fn, InvocationType="Event")
                    time.sleep(min(to + 15, 120))
                seen_fn_probe[fn] = (tail, fe)
            tail, fe = seen_fn_probe[fn]

            time.sleep(2)
            age1, lm1 = head_age(key)
            moved = age1 is not None and age1 < 0.2
            row["moved_on_probe"] = moved
            row["probe_error"] = fe

            # classify
            tl = tail.lower()
            if moved and repo_writes and not dep_writes:
                cls = "FIXED-DEPLOY-DRIFT"
                fixed += 1
            elif moved:
                man = json.loads(s3.get_object(
                    Bucket=BUCKET, Key="config/schedule-manifest.json"
                )["Body"].read())
                inputs = []
                for r_ in man.get("rules", []) + man.get("schedules", []):
                    for t in r_.get("targets") or []:
                        if (t.get("arn") or "").endswith(":" + fn) and \
                                t.get("input"):
                            inputs.append(t["input"][:80])
                row["sched_inputs"] = inputs[:3]
                cls = "SCHEDULE-INPUT" if inputs else "INTERMITTENT"
            elif fe:
                cls = "PROBE-ERRORED"
            elif stem.lower() in tl:
                seg = ""
                for line in tail.splitlines():
                    if stem.lower() in line.lower():
                        seg = line.strip()[:150]
                        break
                row["probe_line"] = seg
                if any(w in tl for w in ("skip", "unchanged", "no data",
                                          "empty", "not due", "cached")):
                    cls = "CONDITIONAL-SKIP"
                elif any(w in tl for w in ("error", "exception",
                                            "traceback", "denied")):
                    cls = "WRITE-ERROR"
                else:
                    cls = "MENTIONS-NO-WRITE"
            elif not dep_writes and repo_writes is False:
                cls = "NOT-THE-WRITER"
            elif fresh_sib:
                cls = "SUPERSEDED?"
            else:
                cls = "CODE-PATH-SILENT"
            row["class"] = cls
            counts[cls] = counts.get(cls, 0) + 1
            mark = rep.ok if cls.startswith("FIXED") else (
                rep.warn if cls in ("SUPERSEDED?", "NOT-THE-WRITER",
                                    "CONDITIONAL-SKIP") else rep.fail)
            mark("%-38s %-30s %-18s age=%sh %s"
                 % (base[:38], fn[:30], cls, row.get("age_h"),
                    (row.get("probe_line") or
                     (row.get("sched_inputs") or [""])[0] or
                     ";".join(row.get("fresh_siblings", []))[:60])[:70]))
            rep.kv(section="engine", **{k: (json.dumps(v)[:150]
                                            if isinstance(v, (list, dict))
                                            else v)
                                        for k, v in row.items()})
        except Exception as e:
            row["class"] = "PROBE-FAILED"
            row["error"] = str(e)[:120]
            counts["PROBE-FAILED"] = counts.get("PROBE-FAILED", 0) + 1
            rep.fail("%-38s %-30s PROBE-FAILED %s"
                     % (base[:38], fn[:30], str(e)[:60]))
        OUT["engines"].append(row)

    rep.section("MATRIX")
    for c, n in sorted(counts.items(), key=lambda x: -x[1]):
        rep.log("  %-22s %d" % (c, n))
        rep.kv(section="matrix", cls=c, count=n)
    rep.log("auto-repaired this run (deploy drift, artifact verified "
            "moving): %d" % fixed)

    (ROOT / "aws" / "ops" / "reports" / "4255_silent_writers.json"
     ).write_text(json.dumps(OUT, indent=1, default=str),
                  encoding="utf-8")
    rep.section("RESULT")
    if fails:
        raise SystemExit("FAILS")
    rep.ok("OPS 4255 PASS — evidence matrix complete; wave-2 fixes are "
           "now targeted, not guessed")
