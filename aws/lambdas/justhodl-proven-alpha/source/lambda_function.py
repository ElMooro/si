"""justhodl-proven-alpha v1.0.0 (ops 3519) — the Proven Alpha Report
engine: one honest, self-updating scoreboard of every signal family the
platform emits, computed from GRADED outcomes in justhodl-signals (the
same rows checker-v3 writes), never from claims.

Per family (signal_type, incl inv:* mirrors): pending count, graded
count, per-window {n, hit, avg/med excess bps}, primary window (day_21
when present else max-n), verdict — SUPPRESSED (signal-suppress feed)
> PROVEN (n>=10 & hit>=60% primary) > EVALUATING (some grades) >
PENDING (none yet, with a first-grades ETA from the earliest pending
check_timestamp). alpha-triage verdicts joined for context. Daily
22:40 after the graders.
"""
import json
import time
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
BUCKET = "justhodl-dashboard-live"
KEY = "data/proven-alpha.json"
S3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")


def rnd(v, n=2):
    try:
        return round(float(v), n)
    except (TypeError, ValueError):
        return None


def _rj(key, default):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:  # noqa: BLE001
        return default


def scan_rows(table):
    rows, kw = [], {"ProjectionExpression":
                    "signal_type,outcomes,#st,logged_at,check_timestamps",
                    "ExpressionAttributeNames": {"#st": "status"}}
    while True:
        r = table.scan(**kw)
        rows += r.get("Items") or []
        lek = r.get("LastEvaluatedKey")
        if not lek:
            return rows
        kw["ExclusiveStartKey"] = lek


def excess_of(o):
    """bps excess: relative rows carry excess_return (pct), directional
    carry return_pct. Missing -> None (excluded from averages)."""
    for k in ("excess_return", "return_pct"):
        v = o.get(k)
        if isinstance(v, (int, float)):
            return float(v) * 100.0
        try:
            return float(v) * 100.0
        except (TypeError, ValueError):
            continue
    return None


def build_families(rows, suppressed, invert_src):
    fams = {}
    for it in rows:
        st = str(it.get("signal_type") or "")
        if not st:
            continue
        f = fams.setdefault(st, {"family": st, "pending": 0,
                                 "graded": 0, "windows": {},
                                 "first": None, "last": None,
                                 "eta": None})
        d = str(it.get("logged_at") or "")[:10]
        if d:
            f["first"] = min(f["first"] or d, d)
            f["last"] = max(f["last"] or d, d)
        outs = it.get("outcomes") or {}
        outs = {k: v for k, v in outs.items()
                if isinstance(v, dict) and "correct" in v}
        if not outs:
            f["pending"] += 1
            cts = it.get("check_timestamps") or {}
            first_ct = min((str(v)[:10] for v in cts.values()), default=None)
            if first_ct:
                f["eta"] = min(f["eta"] or first_ct, first_ct)
            continue
        f["graded"] += 1
        for wk, o in outs.items():
            w = f["windows"].setdefault(wk, {"n": 0, "hits": 0, "ex": []})
            w["n"] += 1
            w["hits"] += 1 if o.get("correct") in (True, 1, "true") else 0
            e = excess_of(o)
            if e is not None:
                w["ex"].append(e)
    out = []
    for st, f in fams.items():
        wins = {}
        for wk, w in f["windows"].items():
            ex = sorted(w["ex"])
            wins[wk] = {"n": w["n"],
                        "hit": rnd(100.0 * w["hits"] / w["n"], 1),
                        "avg_excess_bps": rnd(sum(ex) / len(ex), 1)
                        if ex else None,
                        "med_excess_bps": rnd(ex[len(ex) // 2], 1)
                        if ex else None}
        primary = ("day_21" if "day_21" in wins else
                   (max(wins, key=lambda k: wins[k]["n"]) if wins else None))
        pw = wins.get(primary) or {}
        if st in suppressed:
            verdict = "SUPPRESSED"
        elif pw.get("n", 0) >= 10 and (pw.get("hit") or 0) >= 60:
            verdict = "PROVEN"
        elif f["graded"] > 0:
            verdict = "EVALUATING"
        else:
            verdict = "PENDING"
        out.append({"family": st, "verdict": verdict,
                    "inverted_source": st in invert_src,
                    "pending": f["pending"], "graded": f["graded"],
                    "primary_window": primary, "windows": wins,
                    "hit_primary": pw.get("hit"),
                    "avg_excess_bps": pw.get("avg_excess_bps"),
                    "first": f["first"], "last": f["last"],
                    "first_grades_eta": f["eta"]
                    if f["graded"] == 0 else None})
    rank = {"PROVEN": 0, "EVALUATING": 1, "PENDING": 2, "SUPPRESSED": 3}
    out.sort(key=lambda r: (rank[r["verdict"]], -(r["graded"] or 0),
                            r["family"]))
    return out


def lambda_handler(event, context):
    t0 = time.time()
    sup = set((_rj("data/signal-suppress.json", {}) or {})
              .get("suppressed") or [])
    tri = _rj("data/alpha-triage.json", {}) or {}
    invert_src = set()
    for v in (tri.get("verdicts") or []):
        if isinstance(v, dict) and str(v.get("verdict", "")).upper() \
                .startswith("INVERT"):
            invert_src.add(str(v.get("family") or v.get("name") or ""))
    rows = scan_rows(ddb.Table("justhodl-signals"))
    fams = build_families(rows, sup, invert_src)
    doc = {"ok": True, "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": rnd(time.time() - t0, 1),
           "n_rows_scanned": len(rows),
           "summary": {"n_families": len(fams),
                       "n_proven": sum(1 for f in fams
                                       if f["verdict"] == "PROVEN"),
                       "n_evaluating": sum(1 for f in fams
                                           if f["verdict"] == "EVALUATING"),
                       "n_pending": sum(1 for f in fams
                                        if f["verdict"] == "PENDING"),
                       "n_suppressed": sum(1 for f in fams
                                           if f["verdict"] == "SUPPRESSED"),
                       "n_graded_signals": sum(f["graded"] for f in fams),
                       "n_pending_signals": sum(f["pending"]
                                                for f in fams)},
           "families": fams}
    S3.put_object(Bucket=BUCKET, Key=KEY,
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    print(json.dumps({"n": len(fams), "summary": doc["summary"]}))
    return doc
