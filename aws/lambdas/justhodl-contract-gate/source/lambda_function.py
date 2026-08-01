"""
justhodl-contract-gate — does the output still mean what it used to mean?

THE GAP THIS CLOSES
justhodl-deal-scanner returned HTTP 200 on the 29% of runs that survived
its UnboundLocalError. Every monitor in the account read that as success,
because every monitor asks "did it throw?" — nobody asks "is the answer
still the right shape?" An engine can run, exit clean, publish a document
with two rows where it used to publish four hundred, and no alarm in the
system will ever notice.

DESIGN DECISION: EXTERNAL VALIDATOR, NOT AN INLINE ASSERTION
The obvious implementation is a shared module every engine calls before
it writes. That means editing the write path of 766 functions, which is
an enormous blast radius for a safety feature — a bug in the safety code
would take down the fleet it exists to protect. So this validates from
the OUTSIDE: it reads the published artifacts and asserts their shape.
Zero risk to anything currently running, and it catches the same class of
failure. Inline assertion can follow later for engines that want to fail
fast rather than fail visible.

CONTRACTS ARE LEARNED, NOT HAND-WRITTEN
Nobody is going to hand-author 300 contracts, and a contract nobody
writes is a contract nobody has. Learn mode derives each artifact's
shape from its current healthy state: top-level keys, the location and
size of its principal row collection, and an age bound inferred from how
stale it actually is today. The result is committed and human-editable —
learned as a floor, tightened by hand where it matters.

VIOLATION CLASSES
  MISSING        the artifact a contract names is gone
  UNPARSEABLE    present but not valid JSON
  ROW_COLLAPSE   principal collection fell below its floor — the
                 deal-scanner failure, made visible
  MISSING_KEYS   top-level keys the contract requires have disappeared
  STALE          older than its learned age bound

MODES
  {"mode":"learn"}   re-derive contracts from the current state
  {"mode":"check"}   default — validate and publish violations
"""

import json
import math
import re
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

VERSION = "1.1.0"
MARKER = "contract-gate v1.1.0 ops4249 cadence-bounds"

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
CONTRACTS_KEY = "config/engine-contracts.json"
VIOLATIONS_KEY = "data/contract-violations.json"

CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"},
             read_timeout=60)
s3 = boto3.client("s3", config=CFG)

# Artifacts that are control-plane output rather than engine output.
# Validating our own violation report would be circular.
SKIP = {"contract-violations.json", "fleet-integrity.json",
        "schedule-drift.json", "engine-manifest.json"}

SEV = {"MISSING": 1, "UNPARSEABLE": 1, "ROW_COLLAPSE": 1,
       "MISSING_KEYS": 1, "STALE": 2}


def now():
    return datetime.now(timezone.utc)


def get_json(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def list_artifacts():
    """Top-level data/*.json only. Nested prefixes are state and history,
    not published engine output."""
    out = []
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET, Prefix="data/", Delimiter="/"):
        for o in page.get("Contents", []):
            k = o["Key"]
            if not k.endswith(".json"):
                continue
            name = k.split("/")[-1]
            if name in SKIP:
                continue
            out.append({"key": k, "size": o["Size"],
                        "modified": o["LastModified"]})
    return out


def principal_rows(doc):
    """Find the collection that carries the engine's actual payload: the
    largest list, checked one level deep.

    v1.0.1: the path is a LIST OF SEGMENTS, not a dotted string. v1.0.0
    joined segments with "." and split them again on read, which silently
    broke for any document whose keys contain dots — and this fleet is
    full of them, because artifacts are keyed by filename
    ("page_reads" -> "risk-regime.html"). The resolver returned None, the
    gate read None as a row collapse, and it reported two failures that
    were entirely its own. A validator that manufactures false positives
    trains people to ignore it, which is the exact failure it exists to
    prevent, so this is fixed at the representation rather than patched
    at the parse.

    Returns (path_segments, count). ["$"] means the document is itself
    the list."""
    best = (None, 0)
    if isinstance(doc, list):
        return (["$"], len(doc))
    if not isinstance(doc, dict):
        return best
    for k, v in doc.items():
        if isinstance(v, list) and len(v) > best[1]:
            best = ([k], len(v))
        elif isinstance(v, dict):
            for k2, v2 in v.items():
                if isinstance(v2, list) and len(v2) > best[1]:
                    best = ([k, k2], len(v2))
    return best


def rows_at(doc, path):
    """Accepts the v1.0.1 segment list and, for registries written by
    v1.0.0, a dotted string — resolved whole-key-first so a key
    containing a dot still resolves correctly."""
    if isinstance(path, str):
        path = [path] if (isinstance(doc, dict) and path in doc) \
            else path.split(".")
    if not path:
        return None
    if path == ["$"]:
        return len(doc) if isinstance(doc, list) else 0
    cur = doc
    for part in path:
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return len(cur) if isinstance(cur, list) else None


def doc_age_h(doc, fallback_modified):
    for k in ("generated_at", "captured_at", "asof", "updated_at", "ts"):
        v = (doc.get(k) if isinstance(doc, dict) else None)
        if isinstance(v, str) and len(v) >= 10:
            try:
                t = datetime.fromisoformat(v.replace("Z", "+00:00"))
                if t.tzinfo is None:
                    t = t.replace(tzinfo=timezone.utc)
                return (now() - t).total_seconds() / 3600.0, k
            except Exception:
                pass
    if fallback_modified:
        return (now() - fallback_modified).total_seconds() / 3600.0, \
            "s3:LastModified"
    return None, None



# ---------------------------------------------------------------------
# v1.1.0 (ops 4249): staleness bounds come from DECLARED CADENCE, not
# observed age.
#
# v1.0 learned each artifact's age bound from how stale it happened to
# be at learn time: bound = max(48h, age*2). The signal scorecard was
# already FIVE DAYS frozen when contracts were learned, so the gate
# recorded a ~240-hour bound and certified the freeze as healthy. I had
# named this exact trap while building the schedule reconciler —
# "snapshotting a dirty fleet enshrines the mess as desired state" —
# and then walked into it in the same session. Learned baselines encode
# the moment you learn them, degradation included.
#
# The fix uses ground truth that already exists: the schedule manifest
# declares every producer's cadence, and an artifact-producers map (built
# by grepping each artifact key against engine source, generated by ops
# 4249 and refreshed by ops runs) links artifacts to producers. Bound =
# 2*cadence + 6h grace, floored at 12h — an hourly artifact goes STALE
# after ~8h missed, a daily one after 54h, a weekly one after ~2 weeks.
# Where no producer can be resolved the learned formula survives but is
# CAPPED at 72h and labelled, and any artifact already older than its
# bound at learn time is emitted as a SUSPECT instead of being blessed.
# ---------------------------------------------------------------------
PRODUCERS_KEY = "config/artifact-producers.json"
MANIFEST_KEY = "config/schedule-manifest.json"


def _cadence_hours(expr):
    """Conservative parse of rate()/cron() into an interval in hours.
    Returns None when the expression cannot be read confidently — an
    honest None beats a confident guess."""
    if not expr:
        return None
    e = expr.strip().lower()
    m = re.match(r"rate\((\d+)\s+(minute|hour|day)s?\)", e)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        return {"minute": n / 60.0, "hour": float(n),
                "day": n * 24.0}[unit]
    m = re.match(r"cron\(([^)]+)\)", e)
    if not m:
        return None
    f = m.group(1).split()
    if len(f) < 6:
        return None
    minute, hour, dom, month, dow = f[0], f[1], f[2], f[3], f[4]
    mm = re.match(r"\*/(\d+)$", minute)
    if mm:
        return int(mm.group(1)) / 60.0
    hm = re.match(r"\*/(\d+)$", hour)
    if hm:
        return float(hm.group(1))
    if hour == "*":
        return 1.0
    if re.match(r"^[a-z]{3}$", dow) or re.match(r"^\d$", dow):
        return 168.0
    if "," in hour:
        return max(1.0, 24.0 / (hour.count(",") + 1))
    return 24.0


def _load_cadences():
    """artifact key -> cadence hours, via producers map + manifest."""
    try:
        prod = json.loads(get_json_raw(PRODUCERS_KEY))
    except Exception:
        return {}
    try:
        man = json.loads(get_json_raw(MANIFEST_KEY))
    except Exception:
        return {}
    fn_cad = {}
    for r in (man.get("rules") or []) + (man.get("schedules") or []):
        if (r.get("state") or "ENABLED") != "ENABLED":
            continue
        h = _cadence_hours(r.get("expr"))
        if h is None:
            continue
        for t in r.get("targets") or []:
            fn = (t.get("arn") or "").split(":")[-1]
            if fn:
                fn_cad[fn] = min(h, fn_cad.get(fn, 1e9))
    out = {}
    for key, fns in (prod.get("producers") or {}).items():
        cads = [fn_cad[f] for f in fns if f in fn_cad]
        if cads:
            out[key] = min(cads)
    return out


def get_json_raw(key):
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def learn():
    contracts = {}
    suspects = []
    cadences = _load_cadences()
    print("[contracts] cadence map: %d artifacts resolvable" % len(cadences))
    for a in list_artifacts():
        try:
            doc = get_json(a["key"])
        except Exception:
            continue
        path, n = principal_rows(doc)
        age_h, src = doc_age_h(doc, a["modified"])
        cad = cadences.get(a["key"])
        if cad is not None:
            bound = max(12.0, 2.0 * cad + 6.0)
            bsrc = "cadence(%.1fh)" % cad
        elif age_h is None:
            bound, bsrc = 48.0, "default"
        elif age_h < 30:
            bound, bsrc = 36.0, "observed"
        else:
            bound = min(72.0, max(48.0, math.ceil(age_h * 2.0)))
            bsrc = "observed-capped"
        if age_h is not None and age_h > bound:
            suspects.append({"key": a["key"],
                             "age_h": round(age_h, 1),
                             "bound_h": bound,
                             "note": "already STALE at learn time — this "
                                     "artifact was NOT blessed"})
        keys = sorted(doc.keys())[:40] if isinstance(doc, dict) else []
        contracts[a["key"]] = {
            "rows_path": path,
            "min_rows": max(1, int(n * 0.70)) if n else 0,
            "learned_rows": n,
            "required_keys": keys,
            "max_age_hours": bound,
            "bound_source": bsrc,
            "age_source": src,
            "learned_at": now().isoformat(),
        }
    doc = {"version": VERSION, "marker": MARKER,
           "generated_at": now().isoformat(),
           "note": "Learned floors, not hand-authored ceilings. min_rows "
                   "is 70% of the count observed at learn time; tighten by "
                   "hand where an engine's output should be exact.",
           "n_contracts": len(contracts),
           "n_cadence_bounded": sum(1 for c in contracts.values()
                                    if str(c.get("bound_source", ""))
                                    .startswith("cadence")),
           "n_suspects": len(suspects), "suspects": suspects,
           "contracts": contracts}
    s3.put_object(Bucket=BUCKET, Key=CONTRACTS_KEY,
                  Body=json.dumps(doc, indent=1).encode(),
                  ContentType="application/json")
    return doc


def check():
    try:
        reg = get_json(CONTRACTS_KEY)
    except Exception as e:
        return {"ok": False, "error": "no contract registry: %s" % str(e)[:90]}
    contracts = reg.get("contracts", {})
    live = {a["key"]: a for a in list_artifacts()}
    violations = []

    def v(cls, key, detail):
        violations.append({"cls": cls, "sev": SEV.get(cls, 2),
                           "artifact": key, "detail": detail})

    for key, c in contracts.items():
        a = live.get(key)
        if not a:
            v("MISSING", key, "contracted artifact no longer exists")
            continue
        try:
            doc = get_json(key)
        except Exception as e:
            v("UNPARSEABLE", key, "will not parse: %s" % str(e)[:80])
            continue
        n = rows_at(doc, c.get("rows_path") or ["$"])
        if c.get("min_rows") and (n is None or n < c["min_rows"]):
            v("ROW_COLLAPSE", key,
              "%s has %s rows, contract floor is %d (learned %d) — the "
              "engine ran but produced a fraction of its output"
              % (".".join(c.get("rows_path") or []), n,
                 c["min_rows"], c.get("learned_rows", 0)))
        if isinstance(doc, dict):
            missing = [k for k in (c.get("required_keys") or [])
                       if k not in doc]
            if missing:
                v("MISSING_KEYS", key,
                  "absent top-level keys: %s" % ", ".join(missing[:8]))
        age_h, _ = doc_age_h(doc, a["modified"])
        if age_h is not None and age_h > c.get("max_age_hours", 48):
            v("STALE", key, "%.0fh old, bound is %.0fh"
              % (age_h, c.get("max_age_hours", 48)))

    uncontracted = sorted(set(live) - set(contracts))
    violations.sort(key=lambda x: (x["sev"], x["cls"], x["artifact"]))
    doc = {"version": VERSION, "marker": MARKER,
           "generated_at": now().isoformat(),
           "n_contracts": len(contracts), "n_artifacts": len(live),
           "n_violations": len(violations),
           "sev1": sum(1 for x in violations if x["sev"] == 1),
           "sev2": sum(1 for x in violations if x["sev"] == 2),
           "by_class": {},
           "uncontracted": uncontracted[:100],
           "n_uncontracted": len(uncontracted),
           "violations": violations[:400]}
    for x in violations:
        doc["by_class"][x["cls"]] = doc["by_class"].get(x["cls"], 0) + 1
    s3.put_object(Bucket=BUCKET, Key=VIOLATIONS_KEY,
                  Body=json.dumps(doc).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=300")
    return doc


def lambda_handler(event=None, context=None):
    event = event or {}
    mode = (event.get("mode") or "check").lower()
    t0 = time.time()
    if mode == "learn":
        d = learn()
        print("[contracts] learned %d contracts (%d cadence-bounded, "
              "%d suspects)" % (d["n_contracts"],
                                d.get("n_cadence_bounded", 0),
                                d.get("n_suspects", 0)))
        for x in (d.get("suspects") or [])[:15]:
            print("[contracts] SUSPECT %s age=%sh bound=%sh"
                  % (x["key"], x["age_h"], x["bound_h"]))
        out = {"ok": True, "mode": "learn",
               "n_contracts": d["n_contracts"],
               "n_cadence_bounded": d.get("n_cadence_bounded", 0),
               "n_suspects": d.get("n_suspects", 0)}
    else:
        d = check()
        if not d.get("ok", True):
            print("[contracts] %s" % d.get("error"))
            return d
        print(json.dumps({
            "_aws": {"Timestamp": int(time.time() * 1000),
                     "CloudWatchMetrics": [{
                         "Namespace": "JustHodl/Contracts",
                         "Dimensions": [[]],
                         "Metrics": [
                             {"Name": "Violations", "Unit": "Count"},
                             {"Name": "ViolationsSev1", "Unit": "Count"},
                             {"Name": "ArtifactsChecked", "Unit": "Count"}]}]},
            "Violations": d["n_violations"],
            "ViolationsSev1": d["sev1"],
            "ArtifactsChecked": d["n_contracts"]}))
        print("[contracts] %d contracts, %d violations (sev1=%d) %s"
              % (d["n_contracts"], d["n_violations"], d["sev1"],
                 d["by_class"]))
        for x in d["violations"][:20]:
            print("[contracts] S%d %-14s %s — %s"
                  % (x["sev"], x["cls"], x["artifact"], x["detail"][:110]))
        out = {"ok": True, "mode": "check",
               "n_contracts": d["n_contracts"],
               "n_violations": d["n_violations"], "sev1": d["sev1"],
               "by_class": d["by_class"]}
    out["elapsed_s"] = round(time.time() - t0, 1)
    return out
