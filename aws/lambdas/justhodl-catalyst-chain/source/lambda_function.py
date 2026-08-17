"""justhodl-catalyst-chain v1.0.0 -- Fusion 3.
Marker: catalyst-chain v1.0.0

The chain the fleet could see only in pieces, fused into one state
machine per ticker (fields bind ONLY what probe ops 4852 printed):

  S1 EVENT       a classed catalyst fired (catalyst.json
                 by_ticker) -- first-order on the event ticker,
                 second-order when readthrough names the ticker a
                 beneficiary of someone else's event.
  S2 PROPAGATED  readthrough tier + edge_confidence says the
                 order/demand flows to this ticker (first-order =
                 trivially propagated, conf 1.0).
  S3 CONFIRMED   the flow LANDED in filings: backlog.json shows
                 RPO qoq > 0, deferred_accelerating, or deferred
                 qoq > 10 (metric + as-of recorded).
  S4 REPRICED    the street moved: estimate-revisions
                 direction_map == UP.

ALPHA = chains alive through S2/S3 where S4 is still FLAT/DOWN:
the event happened, the supply chain received it, the filings
confirm it, and consensus has not moved.

score = event_strength x edge_confidence x (1 + 0.5 if S3)
        x street_mult    (UP -> 0 = priced; FLAT -> 1.0;
                          DOWN -> 1.2 max divergence;
                          unknown -> 1.0 flagged)
event_strength: first-order = catalyst score; second-order = sum
of source-ticker catalyst scores, else readthrough max_score / 20.

Honesty: missing backlog coverage is a NAMED gap (S3 unknowable,
not false-negative silently); stale inputs are stamped per-feed;
`unpriced` and `completed` never intersect; spine (catalyst +
readthrough) missing -> INSUFFICIENT.  Daily 22:15 UTC.
"""
import gzip
import json
import os
import time
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
K_CAT = "data/catalyst.json"
K_RT = "data/readthrough.json"
K_BL = "data/backlog.json"
K_ER = "data/estimate-revisions.json"
OUT_KEY = "data/catalyst-chain.json"
TOP_N = 60
STALE_H = 48

s3 = boto3.client("s3")


def _g(key):
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return None


def _put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, separators=(",", ":")).encode(),
                  ContentType="application/json")


def age_h(doc, now):
    g = str((doc or {}).get("generated_at") or "")[:19]
    try:
        dt = datetime.fromisoformat(g)
        return round((now.replace(tzinfo=None) - dt)
                     .total_seconds() / 3600, 1)
    except ValueError:
        return None


def stage3_check(bl_row):
    """(confirmed, why) from a backlog row; (None, gap) when no
    coverage."""
    if not bl_row:
        return None, "no filing coverage in backlog census"
    rq = bl_row.get("rpo_qoq")
    if isinstance(rq, (int, float)) and rq > 0:
        return True, "RPO qoq %+.1f%% (asof %s)" % (
            rq, bl_row.get("rpo_asof"))
    if bl_row.get("deferred_accelerating") is True:
        return True, "deferred revenue accelerating (qoq %s%%)" \
            % bl_row.get("deferred_qoq")
    dq = bl_row.get("deferred_qoq")
    if isinstance(dq, (int, float)) and dq > 10:
        return True, "deferred qoq %+.1f%%" % dq
    return False, "filings covered, no acceleration"


def street_mult(direction):
    if direction == "UP":
        return 0.0, "repriced"
    if direction == "DOWN":
        return 1.2, "street cutting INTO a live chain"
    if direction == "FLAT":
        return 1.0, "street flat"
    return 1.0, "street unknown (not in direction_map)"


def build():
    t0 = time.time()
    now = datetime.now(timezone.utc)
    doc = {"v": VERSION, "engine": "justhodl-catalyst-chain",
           "as_of": now.date().isoformat(),
           "generated_at": now.isoformat(),
           "doctrine": "alpha = event -> propagation -> filing "
                       "confirmation while the street is still "
                       "asleep",
           "inputs": {}, "chains": [], "unpriced": [],
           "completed": [], "diag": {}}
    cat = _g(K_CAT)
    rt = _g(K_RT)
    bl = _g(K_BL) or {}
    er = _g(K_ER) or {}
    for name, d, key in (("catalyst", cat, K_CAT),
                         ("readthrough", rt, K_RT),
                         ("backlog", bl, K_BL),
                         ("est_revisions", er, K_ER)):
        a = age_h(d, now)
        doc["inputs"][name] = {
            "present": bool(d), "age_h": a,
            "stale": (a is not None and a > STALE_H)}
    if not cat or not rt:
        doc["status"] = "INSUFFICIENT_DATA"
        doc["why"] = "spine missing (catalyst and/or readthrough)"
        return doc
    cat_by = cat.get("by_ticker") or {}
    bl_by = bl.get("by_ticker") or {}
    dmap = er.get("direction_map") or {}
    booms = rt.get("industry_boom_context") or {}

    conf_by_t = {}
    detail_by_t = {}
    for b in rt.get("beneficiaries") or []:
        t = b.get("ticker")
        if not t:
            continue
        c = b.get("edge_confidence") or 0
        if c >= conf_by_t.get(t, -1):
            conf_by_t[t] = c
            detail_by_t[t] = b
    rollup = {r.get("ticker"): r
              for r in rt.get("by_beneficiary") or []
              if r.get("ticker")}

    subjects = {}
    for t, row in cat_by.items():
        subjects[t] = {"order": 1, "sources": [t],
                       "ev": row.get("score") or 0,
                       "classes": [c.get("class") for c in
                                   (row.get("catalysts") or [])],
                       "conf": 1.0, "tier": "SELF",
                       "why_prop": "first-order event ticker"}
    for t, r in rollup.items():
        if t in subjects:
            continue
        srcs = [s for s in (r.get("catalysts") or []) if s]
        ev = sum((cat_by.get(s) or {}).get("score") or 0
                 for s in srcs)
        if ev <= 0:
            ev = round((r.get("max_score") or 0) / 20.0, 3)
        det = detail_by_t.get(t) or {}
        subjects[t] = {"order": 2, "sources": srcs,
                       "ev": ev,
                       "classes": [(cat_by.get(s) or {})
                                   .get("top_class")
                                   for s in srcs
                                   if s in cat_by],
                       "conf": conf_by_t.get(t, 0) or 0,
                       "tier": det.get("tier")
                       or r.get("best_tier"),
                       "why_prop": det.get("why")
                       or "readthrough beneficiary",
                       "quadrant": r.get("quadrant"),
                       "implied_usd": r.get(
                           "implied_order_usd_total")}

    chains = []
    n_gap = 0
    for t, sub in subjects.items():
        confirmed, why3 = stage3_check(bl_by.get(t))
        if confirmed is None:
            n_gap += 1
        direction = dmap.get(t)
        mult, why4 = street_mult(direction)
        stage = 4 if direction == "UP" else \
            (3 if confirmed else
             (2 if (sub["order"] == 1 or sub["conf"] > 0) else 1))
        score = round(sub["ev"] * (sub["conf"] or 0)
                      * (1.5 if confirmed else 1.0) * mult, 3)
        pri_score = round(sub["ev"] * (sub["conf"] or 0)
                          * (1.5 if confirmed else 1.0), 3)
        row = {"t": t, "order": sub["order"],
               "sources": sub["sources"][:6],
               "classes": [c for c in sub["classes"] if c][:4],
               "stage": stage,
               "s2_conf": round(sub["conf"] or 0, 3),
               "s2_tier": sub.get("tier"),
               "s3_confirmed": confirmed,
               "s3_why": why3,
               "s4_direction": direction or "UNKNOWN",
               "score": score,
               "score_if_unpriced": pri_score,
               "why": ["%s x%d ev=%.2f" % (
                   "/".join(c for c in sub["classes"] if c)[:40]
                   or "event", len(sub["sources"]), sub["ev"]),
                   sub["why_prop"][:70], why3[:70], why4]}
        if sub.get("quadrant"):
            row["rt_quadrant"] = sub["quadrant"]
        ind = (bl_by.get(t) or {}).get("group")
        if ind and ind in booms:
            row["industry_boom"] = booms[ind]
        chains.append(row)

    chains.sort(key=lambda r: (-r["score_if_unpriced"],
                               -r["stage"]))
    doc["chains"] = chains[:TOP_N]
    doc["unpriced"] = [r for r in chains
                       if r["s4_direction"] in ("FLAT", "DOWN",
                                                "UNKNOWN")
                       and r["stage"] >= 2
                       and r["score_if_unpriced"] > 0][:30]
    doc["completed"] = [r for r in chains
                        if r["s4_direction"] == "UP"][:30]
    hist = {}
    for r in chains:
        hist[str(r["stage"])] = hist.get(str(r["stage"]), 0) + 1
    doc["diag"].update({
        "n_subjects": len(subjects),
        "stage_hist": hist,
        "n_no_filing_coverage": n_gap,
        "runtime_ms": int((time.time() - t0) * 1000)})
    doc["status"] = "LIVE"
    return doc


def lambda_handler(event, context):
    doc = build()
    _put(OUT_KEY, doc)
    return {"ok": doc.get("status") == "LIVE",
            "status": doc.get("status"),
            "n_chains": len(doc.get("chains") or []),
            "n_unpriced": len(doc.get("unpriced") or []),
            "stage_hist": (doc.get("diag") or {}).get(
                "stage_hist")}
