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

v1.5.0 (2026-10-08 audit) — LEARN IS NOW HISTORY-AWARE AND HONEST ABOUT DRIFT
The 2026-08-01 contracts aged into 365 violations while 822 of 909 engines
ran clean: the fleet moved to a fail-closed output doctrine (empty boards
with execution_eligible=false are a truthful state), incidental keys such as
elapsed_s were "required", and 77 contracted artifacts had no writer at all.
A gate that is wrong about the fleet 365 times teaches people to skim it.
  * required_keys  = keys present now AND in the previous contract (stable
                     keys), else current keys minus a volatile denylist.
  * regressed[]    = artifacts whose principal rows fell below half of the
                     max seen in data/_state/rowcounts/ history. They are
                     contracted at today's floor but LISTED, never silently
                     blessed — the review trail lives in the registry.
  * orphaned[]     = artifacts with no source-bound writer in the producers
                     map and no update for ORPHAN_DAYS. Not contracted; not
                     counted as uncontracted; listed with their age.
  * previous bounds are kept for artifacts that are STALE at learn time, so
    a dead feed cannot widen its own age bound by being learned while dead.
"""

import json
import math
import re
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from private_artifact import is_private_source
from reviewed_contracts import (apply_contracts, apply_producers, artifact_function_name,
                                load_overlay, validate_fields)

VERSION = "1.5.2"
MARKER = "contract-gate v1.5.0 history-aware relearn, stable keys, orphan ledger"

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
       "MISSING_KEYS": 1, "FIELD_TYPE": 1, "FIELD_VALUE": 1, "STALE": 2}

# v1.5.0: keys that describe the run, not the result. They come and go with
# code paths (error only on failure, elapsed_s only when timed) and must
# never make a healthy artifact "MISSING_KEYS".
VOLATILE_KEYS = {"elapsed_s", "elapsed_sec", "elapsed", "duration_s", "duration",
                 "error", "errors", "warning", "warnings", "note", "notes", "debug",
                 "method", "trace", "request_id", "run_id", "attempt", "retries"}
ORPHAN_DAYS = 30
REGRESSION_RATIO = 0.5
ROWCOUNT_PREFIX = "data/_state/rowcounts/"


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


def doc_age_h(doc, fallback_modified, preferred_field=None):
    for k in ([preferred_field] if preferred_field else
              ("generated_at", "captured_at", "asof", "updated_at", "ts")):
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
    """Conservative parse of rate()/cron() into (interval_hours,
    weekday_only). Returns None when unreadable — an honest None beats a
    confident guess.

    v1.3.0: the day-of-week field is no longer ignored. v1.1's parser
    read cron(15 14-19 ? * MON-FRI *) as a 4-hour cadence and set a 14h
    staleness bound — so every MON-FRI engine's artifact would go
    "STALE" every single weekend (Fri close -> Mon open is ~67h). The
    Saturday check that jumped 129 -> 136 violations was this parser
    manufacturing findings, exactly like the weekend false positives in
    the ops-4250 sweep. weekday_only feeds a 78h floor at bound time so
    the weekend gap is inside contract, not a weekly false alarm."""
    if not expr:
        return None
    e = expr.strip().lower()
    m = re.match(r"rate\((\d+)\s+(minute|hour|day)s?\)", e)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        return ({"minute": n / 60.0, "hour": float(n),
                 "day": n * 24.0}[unit], False)
    m = re.match(r"cron\(([^)]+)\)", e)
    if not m:
        return None
    f = m.group(1).split()
    if len(f) < 6:
        return None
    minute, hour, dom, month, dow = f[0], f[1], f[2], f[3], f[4]
    dl = dow.lower()
    weekday_only = (bool(re.search(r"mon|tue|wed|thu|fri", dl))
                    and not re.search(r"sat|sun", dl)) \
        or bool(re.match(r"^[2-6]-[2-6]$", dl)) \
        or bool(re.match(r"^[1-5]-[1-5]$", dl))
    single_day = bool(re.match(r"^[a-z]{3}$", dl)) or \
        bool(re.match(r"^\d$", dl))
    mm = re.match(r"\*/(\d+)$", minute)
    if mm:
        return (int(mm.group(1)) / 60.0, weekday_only)
    hm = re.match(r"\*/(\d+)$", hour)
    if hm:
        return (float(hm.group(1)), weekday_only)
    if hour == "*":
        return (1.0, weekday_only)
    if single_day and not weekday_only:
        return (168.0, False)
    if "," in hour:
        return (max(1.0, 24.0 / (hour.count(",") + 1)), weekday_only)
    return (24.0, weekday_only)


def _staleness_bound(cad_tuple, age_h):
    """Pure bound policy, extracted so the self-test can hold it still.
    Returns (bound_hours, bound_source, learned_while_stale)."""
    if cad_tuple is not None:
        cad, wd = cad_tuple
        bound = max(12.0, 2.0 * cad + 6.0)
        if wd:
            bound = max(bound, 78.0)
        bsrc = "cadence(%.1fh%s)" % (cad, ",wd" if wd else "")
    elif age_h is None:
        bound, bsrc = 48.0, "default"
    elif age_h < 30:
        bound, bsrc = 36.0, "observed"
    else:
        bound = min(72.0, max(48.0, math.ceil(age_h * 2.0)))
        bsrc = "observed-capped"
    suspect = age_h is not None and age_h > bound
    return bound, bsrc, suspect


def _load_cadences():
    """artifact key -> cadence hours, via producers map + manifest."""
    try:
        prod = apply_producers(json.loads(get_json_raw(PRODUCERS_KEY)))
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
        ct = _cadence_hours(r.get("expr"))
        if ct is None:
            continue
        for t in r.get("targets") or []:
            fn = artifact_function_name(t.get("arn"))
            if fn:
                prev = fn_cad.get(fn)
                if prev is None or ct[0] < prev[0]:
                    fn_cad[fn] = ct
    out = {}
    for key, entry in (prod.get("producers") or {}).items():
        if isinstance(entry, dict):
            fns = entry.get("writers") or ([] if entry.get("authoritative_writers") else
                  entry.get("readers") or entry.get("mentions") or [])
        else:
            fns = entry
        cads = [fn_cad[f] for f in fns if f in fn_cad]
        if cads:
            best = min(c for c, _ in cads)
            wd = any(w for c, w in cads if abs(c - best) < 0.01)
            out[key] = (best, wd)
    return out


def get_json_raw(key):
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def _rowcount_history_max():
    """Max principal-row count per artifact over the daily ledger written by
    check() since v1.2.0. Empty dict when the ledger is unreadable."""
    best = {}
    days = 0
    try:
        for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=BUCKET, Prefix=ROWCOUNT_PREFIX):
            for o in page.get("Contents", []):
                try:
                    rows = get_json(o["Key"]).get("rows") or {}
                except Exception:
                    continue
                days += 1
                for k, n in rows.items():
                    if isinstance(n, (int, float)) and n > best.get(k, 0):
                        best[k] = int(n)
    except Exception as e:
        print("[contracts] rowcount history unreadable: %s" % str(e)[:90])
    return best, days


def _stable_keys(doc, previous):
    """v1.5.0 key policy: stable across learn points, never volatile."""
    if not isinstance(doc, dict):
        return []
    cur = set(doc.keys())
    prev = set((previous or {}).get("required_keys") or [])
    keys = (cur & prev) if prev else (cur - VOLATILE_KEYS)
    return sorted(keys)[:40]


def _writers(producers, key):
    """Source-bound writers first; then writers the 2026-08-01 map knew (v3 AST inventory lost
    10 dynamic ones); then engines whose key_patterns cover this key (per-symbol caches)."""
    rec = (producers or {}).get(key) or {}
    for field in ("writers", "legacy_writers", "pattern_writers"):
        w = list(rec.get(field) or [])
        if w:
            return w
    return []


def _classify_dormant(rows, producers):
    """Second pass over learn() rows: a stale-at-learn artifact whose writer still has some OTHER
    output inside its bound is a dormant secondary output (history/state branch no longer taken),
    not evidence that the engine is dead. If none of its writers has any fresh output, the engine
    itself has stopped — that stays a STALE violation. Returns {key: reason}."""
    fresh_by_writer = {}
    for r in rows:
        if not r["was_stale"]:
            for w in r["writers"]:
                fresh_by_writer.setdefault(w, []).append(r["key"])
    dormant = {}
    for r in rows:
        if not r["was_stale"] or not r["writers"]:
            continue
        live = [w for w in r["writers"] if fresh_by_writer.get(w)]
        if live:
            dormant[r["key"]] = {"writers_live": live[:4],
                                 "fresh_example": fresh_by_writer[live[0]][0]}
    return dormant


def learn():
    contracts = {}
    suspects = []
    regressed = []
    orphaned = []
    cadences = _load_cadences()
    print("[contracts] cadence map: %d artifacts resolvable" % len(cadences))
    try:
        previous = apply_contracts(get_json(CONTRACTS_KEY)).get("contracts", {})
    except Exception:
        previous = {}
    try:
        producers = apply_producers(
            json.loads(get_json_raw(PRODUCERS_KEY))).get("producers", {})
    except Exception:
        producers = {}
    hist_max, hist_days = _rowcount_history_max()
    print("[contracts] rowcount history: %d days, %d artifacts; previous "
          "contracts: %d; producers: %d" % (hist_days, len(hist_max),
                                             len(previous), len(producers)))
    rows = []
    for a in list_artifacts():
        key = a["key"]
        try:
            doc = get_json(key)
        except Exception:
            continue
        path, n = principal_rows(doc)
        age_h, src = doc_age_h(doc, a["modified"])
        writers = _writers(producers, key)
        if not writers and age_h is not None and age_h > ORPHAN_DAYS * 24:
            orphaned.append({"key": key, "age_h": round(age_h, 1),
                             "readers": list((producers.get(key) or {})
                                             .get("readers") or [])[:6],
                             "note": "no source-bound writer and no update for "
                                     "%d+ days — retired output, not contracted"
                                     % ORPHAN_DAYS})
            continue
        cad = cadences.get(key)
        bound, bsrc, was_stale = _staleness_bound(cad, age_h)
        # Keep only scalars per artifact: holding every parsed document for the second pass
        # exhausted the function's memory (v1.5.1 first cut, ops 6502/6505).
        rows.append({"key": key, "path": path, "n": n, "age_h": age_h,
                     "src": src, "writers": writers, "bound": bound, "bsrc": bsrc,
                     "was_stale": was_stale,
                     "stable_keys": _stable_keys(doc, previous.get(key) or {})})
        del doc
    dormant_map = _classify_dormant(rows, producers)
    dormant = []
    for r in rows:
        key, path, n, age_h, src, writers, bound, bsrc, was_stale = (
            r["key"], r["path"], r["n"], r["age_h"], r["src"],
            r["writers"], r["bound"], r["bsrc"], r["was_stale"])
        prev = previous.get(key) or {}
        is_dormant = False
        if was_stale and key in dormant_map:
            is_dormant = True
            dormant.append({"key": key, "age_h": round(age_h, 1), "bound_h": bound,
                            "writers": writers[:4], **dormant_map[key],
                            "note": "stale at learn time but its writer still produces "
                                    "other fresh output — dormant secondary output; "
                                    "rows/keys contracted, age not enforced until it "
                                    "updates again"})
        elif was_stale:
            suspects.append({"key": key, "age_h": round(age_h, 1),
                             "bound_h": bound, "writers": writers[:4],
                             "note": "already STALE at learn time and no writer has "
                                     "any fresh output — this artifact was NOT blessed"})
            if prev.get("max_age_hours"):
                bound, bsrc = prev["max_age_hours"], \
                    "kept-previous(%s)" % prev.get("bound_source", "?")
        hmax = hist_max.get(key, 0)
        if hmax and n < REGRESSION_RATIO * hmax:
            regressed.append({"key": key, "rows_now": n, "rows_path": path,
                              "history_max_rows": hmax,
                              "previous_learned_rows": prev.get("learned_rows"),
                              "writers": writers[:4],
                              "note": "principal rows below %d%% of history max; "
                                      "contracted at today's floor, listed for "
                                      "review" % int(REGRESSION_RATIO * 100)})
        contracts[key] = {
            "rows_path": path,
            "min_rows": max(1, int(n * 0.70)) if n else 0,
            "learned_rows": n,
            "history_max_rows": hmax or None,
            "required_keys": r["stable_keys"],
            "max_age_hours": None if is_dormant else bound,
            "bound_source": "dormant(%s)" % bsrc if is_dormant else bsrc,
            "dormant": True if is_dormant else None,
            "age_source": src,
            "writers": writers[:6],
            "learned_at": now().isoformat(),
            "previous_learned_at": prev.get("learned_at"),
        }
    doc = {"version": VERSION, "marker": MARKER,
           "generated_at": now().isoformat(),
           "note": "Learned floors, not hand-authored ceilings. min_rows "
                   "is 70% of the count observed at learn time; tighten by "
                   "hand where an engine's output should be exact. v1.5.0: "
                   "required_keys are stable across learn points; regressed[] "
                   "and orphaned[] are review ledgers, not silent blessings.",
           "n_contracts": len(contracts),
           "n_cadence_bounded": sum(1 for c in contracts.values()
                                    if str(c.get("bound_source", ""))
                                    .startswith("cadence")),
           "rowcount_history_days": hist_days,
           "n_suspects": len(suspects), "suspects": suspects,
           "n_regressed": len(regressed), "regressed": regressed,
           "n_orphaned": len(orphaned), "orphaned": orphaned,
           "n_dormant": len(dormant), "dormant": dormant,
           "contracts": contracts}
    s3.put_object(Bucket=BUCKET, Key=CONTRACTS_KEY,
                  Body=json.dumps(doc, indent=1).encode(),
                  ContentType="application/json")
    return doc


def check():
    try:
        reg = apply_contracts(get_json(CONTRACTS_KEY))
    except Exception as e:
        return {"ok": False, "error": "CONTRACT_REGISTRY_UNAVAILABLE"}
    all_contracts = reg.get("contracts", {})
    orphaned = {o["key"] for o in (reg.get("orphaned") or []) if isinstance(o, dict)}
    n_dormant_checked = 0
    private_excluded = sum(is_private_source(key) for key in all_contracts)
    contracts = {key: value for key, value in all_contracts.items() if not is_private_source(key)}
    # Explicit, reviewed exemptions — one-shot reports and event-driven
    # state files whose silence is their normal condition. An exemption
    # ledger keeps the violations feed meaning "something is wrong"
    # instead of "here is the same known-benign list again"; a feed that
    # repeats itself trains people to skim, which is how CRITICAL sat
    # unread for two days.
    try:
        exempt = json.loads(get_json_raw(
            "config/contract-exemptions.json")).get("exempt") or {}
    except Exception:
        exempt = {}
    live = {a["key"]: a for a in list_artifacts() if not is_private_source(a["key"])}
    # Named root/calibration outputs do not appear in top-level data/ discovery.
    # Observe only reviewed exact keys; never expand into private/history trees.
    for key in load_overlay()["entries"]:
        if key not in live and key in contracts:
            try:
                head = s3.head_object(Bucket=BUCKET, Key=key)
                live[key] = {"key": key, "size": head.get("ContentLength"), "modified": head.get("LastModified")}
            except Exception:
                pass  # Existing MISSING check reports it; no registry mutation.
    violations = []
    rowcounts = {}
    exempted_hits = []

    def v(cls, key, detail):
        violations.append({"cls": cls, "sev": SEV.get(cls, 2),
                           "artifact": key, "detail": detail})

    for key, c in contracts.items():
        if key in exempt:
            exempted_hits.append(key)
            a = live.get(key)
            if a:
                try:
                    doc = get_json(key)
                    nn = rows_at(doc, c.get("rows_path") or ["$"])
                    if nn is not None:
                        rowcounts[key] = nn
                except Exception:
                    pass
            continue
        a = live.get(key)
        if not a:
            v("MISSING", key, "contracted artifact no longer exists")
            continue
        try:
            doc = get_json(key)
        except Exception as e:
            v("UNPARSEABLE", key, "ARTIFACT_READ_OR_PARSE_FAILED")
            continue
        for cls, detail in validate_fields(doc, c):
            v(cls, key, detail)
        n = rows_at(doc, c.get("rows_path") or ["$"])
        if n is not None:
            rowcounts[key] = n
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
        age_h, _ = doc_age_h(doc, a["modified"], c.get("timestamp_field"))
        bound = c.get("max_age_hours", 48)
        if c.get("dormant") and bound is None:
            n_dormant_checked += 1  # age deliberately not enforced; see contracts.dormant[]
        elif age_h is not None and age_h > (bound or 48):
            v("STALE", key, "%.0fh old, bound is %.0fh" % (age_h, bound or 48))

    # v1.2.0 (ops 4252): persist today's principal-row counts.
    #
    # The learn-time floor (70% of observed) has a blind spot the cadence
    # fix did not touch: an engine that collapsed BEFORE contracts were
    # learned had its collapsed output blessed as the baseline — the
    # census-at-25% class is covered forward, not backward. Detection
    # needs ground truth over time, and none existed. This writes one
    # small dated file per check (866 integers), so from today the
    # question "did this engine used to produce more?" has an answer
    # that is data, not memory. A future learn can take max-over-history
    # as the floor instead of a single day's snapshot.
    try:
        s3.put_object(
            Bucket=BUCKET,
            Key="data/_state/rowcounts/%s.json"
                % now().strftime("%Y%m%d"),
            Body=json.dumps({"at": now().isoformat(),
                             "rows": rowcounts}).encode(),
            ContentType="application/json")
    except Exception as e:
        print("[contracts] rowcount history write failed: %s" % str(e)[:90])

    uncontracted = sorted(set(live) - set(contracts) - orphaned)
    violations.sort(key=lambda x: (x["sev"], x["cls"], x["artifact"]))
    doc = {"version": VERSION, "marker": MARKER,
           "source_overlay": reg.get("source_overlay"),
           "generated_at": now().isoformat(),
           "n_contracts": len(contracts), "n_artifacts": len(live),
           "private_contracts_excluded": private_excluded, "scope": "PUBLIC_ENGINE_OUTPUTS",
           "n_violations": len(violations),
           "sev1": sum(1 for x in violations if x["sev"] == 1),
           "sev2": sum(1 for x in violations if x["sev"] == 2),
           "by_class": {},
           "n_exempted": len(exempted_hits),
           "exempted": sorted(exempted_hits),
           "n_orphaned": len(orphaned & set(live)),
           "orphaned": sorted(orphaned & set(live)),
           "n_regressed": reg.get("n_regressed", 0),
           "n_dormant": n_dormant_checked,
           "contracts_learned_at": reg.get("generated_at"),
           "uncontracted": uncontracted,
           "n_uncontracted": len(uncontracted),
           "violations": violations}
    for x in violations:
        doc["by_class"][x["cls"]] = doc["by_class"].get(x["cls"], 0) + 1
    s3.put_object(Bucket=BUCKET, Key=VIOLATIONS_KEY,
                  Body=json.dumps(doc).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=300")
    return doc


SELFTEST = [
    ("dotted_keys", lambda: principal_rows(
        {"page_reads": {"risk-regime.html": [1, 2, 3],
                        "other.html": [1]}})
        == (["page_reads", "risk-regime.html"], 3)),
    ("legacy_dotted_path", lambda: rows_at({"a.b": [1, 2]}, "a.b") == 2),
    ("rate_parse", lambda: _cadence_hours("rate(2 hours)") == (2.0, False)),
    ("weekday_cron", lambda: _cadence_hours(
        "cron(15 14,15,16,17,18,19 ? * MON-FRI *)") == (4.0, True)),
    ("weekend_floor", lambda: _staleness_bound((4.0, True), 20.0)[0] == 78.0
        and _staleness_bound((4.0, True), 20.0)[2] is False),
    ("weekday_agnostic", lambda: _staleness_bound((1.0, False), 5.0)[0]
        == 12.0),
    ("learned_while_stale", lambda: _staleness_bound((1.0, False), 120.0)[2]
        is True),
    ("observed_cap", lambda: _staleness_bound(None, 500.0)[0] == 72.0),
    ("stable_keys_intersect", lambda: _stable_keys(
        {"a": 1, "b": 2, "elapsed_s": 3}, {"required_keys": ["a", "zz"]}) == ["a"]),
    ("stable_keys_no_previous", lambda: _stable_keys(
        {"a": 1, "b": 2, "elapsed_s": 3, "error": None}, None) == ["a", "b"]),
    ("stable_keys_list_doc", lambda: _stable_keys([1, 2], None) == []),
]


def run_selftest():
    cases = []
    for name, fn in SELFTEST:
        try:
            ok = bool(fn())
            cases.append({"case": name, "pass": ok})
        except Exception as e:
            cases.append({"case": name, "pass": False,
                          "error": str(e)[:90]})
    return {"passed": all(c["pass"] for c in cases), "cases": cases}


def lambda_handler(event=None, context=None):
    event = event or {}
    mode = (event.get("mode") or "check").lower()
    if mode == "selftest":
        r = run_selftest()
        for c in r["cases"]:
            print("[gate-selftest] %-20s %s%s"
                  % (c["case"], "PASS" if c["pass"] else "FAIL",
                     " " + c.get("error", "") if not c["pass"] else ""))
        return {"ok": r["passed"], "mode": "selftest", **r}
    t0 = time.time()
    if mode == "learn":
        d = learn()
        print("[contracts] learned %d contracts (%d cadence-bounded, "
              "%d suspects, %d regressed, %d orphaned)"
              % (d["n_contracts"], d.get("n_cadence_bounded", 0),
                 d.get("n_suspects", 0), d.get("n_regressed", 0),
                 d.get("n_orphaned", 0)))
        for x in (d.get("suspects") or [])[:15]:
            print("[contracts] SUSPECT %s age=%sh bound=%sh"
                  % (x["key"], x["age_h"], x["bound_h"]))
        out = {"ok": True, "mode": "learn",
               "n_contracts": d["n_contracts"],
               "n_cadence_bounded": d.get("n_cadence_bounded", 0),
               "n_suspects": d.get("n_suspects", 0),
               "n_regressed": d.get("n_regressed", 0),
               "n_orphaned": d.get("n_orphaned", 0)}
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
