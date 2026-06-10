"""
justhodl-confluence-meta v1.0 — Items 15 + 17 + 18-lite
=======================================================
  15 Confluence-conditional edge curves: per-day distinct-engine alignment count
     → REAL forward SPX distributions by confluence bucket (deep base).
  17 Fade Index: engines with scored≥10 & hit<35% published with inverted
     expectancy — the system trades against its own broken signals.
  18-lite Provenance ledger: immutable daily fingerprint (key+ETag+age) of the
     12 core briefs → data/_ledger/{date}.json, the replay/audit substrate.
"""
import json, os, time
from datetime import datetime, timezone, timedelta
from statistics import mean
import boto3
from boto3.dynamodb.conditions import Attr

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/confluence-meta.json"
VERSION = "1.0.0"
CORE_BRIEFS = ["data/best-setups.json", "data/vol-surface.json", "data/carry-surface.json",
               "data/sector-rotation.json", "data/eurodollar-stress.json", "data/auction-crisis.json",
               "data/historical-analogs.json", "data/alert-backtests.json", "data/apex-fusion.json",
               "data/bottleneck-boom.json", "data/ignition.json", "data/liquidity-inflection.json",
               "data/crisis-canaries.json", "data/global-tide.json"]


def s3j(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    # ── scan signals (120d) ──
    cutoff = int((now - timedelta(days=120)).timestamp())
    tbl = DDB.Table("justhodl-signals")
    items, kwargs, n = [], {"FilterExpression": Attr("logged_epoch").gte(cutoff)}, 0
    while n < 30:
        r = tbl.scan(**kwargs)
        items.extend(r.get("Items", []))
        if "LastEvaluatedKey" in r:
            kwargs["ExclusiveStartKey"] = r["LastEvaluatedKey"]; n += 1
        else:
            break

    def meta_engine(it):
        m = it.get("metadata")
        if isinstance(m, str):
            try:
                m = json.loads(m)
            except Exception:
                m = {}
        return (m or {}).get("engine") or it.get("signal_type") or "?"

    # per-day per-direction distinct engines
    by_day = {}
    for it in items:
        d = str(it.get("logged_at", ""))[:10]
        dr = str(it.get("predicted_direction", "")).upper()
        if d and dr in ("UP", "DOWN"):
            by_day.setdefault(d, {"UP": set(), "DOWN": set()})[dr].add(meta_engine(it))

    # forward SPX from deep base
    spx_doc = s3j("data/spx-history-deep.json") or {}
    spx = {d: float(v) for d, v in (spx_doc.get("points") or []) if v is not None}
    sdates = sorted(spx)
    sidx = {d: i for i, d in enumerate(sdates)}

    def fwd(d, w):
        j = sidx.get(d)
        if j is None:
            nxt = next((x for x in sdates if x >= d), None)
            j = sidx.get(nxt) if nxt else None
        if j is None or j + w >= len(sdates):
            return None
        return (spx[sdates[j + w]] / spx[sdates[j]] - 1) * 100

    curves = {}
    for direction in ("UP", "DOWN"):
        buckets = {"1": [], "2": [], "3": [], "4+": []}
        for d, dd in by_day.items():
            c = len(dd[direction])
            if c == 0:
                continue
            b = "4+" if c >= 4 else str(c)
            buckets[b].append(d)
        tab = {}
        for b, ds in buckets.items():
            row = {"n_days": len(ds)}
            for w in (5, 21, 63):
                rs = [r for r in (fwd(d, w) for d in ds) if r is not None]
                if rs:
                    rs.sort()
                    row[f"med_{w}d"] = round(rs[len(rs) // 2], 2)
                    row[f"hit_{w}d"] = round(100 * sum(1 for r in rs
                                                       if (r > 0) == (direction == "UP")) / len(rs), 1)
                    row[f"n_{w}d"] = len(rs)
            tab[b] = row
        curves[direction] = tab

    # ── fade index from skill ──
    sk = s3j("data/_skill/frontrun-skill-index.json") or {}
    fade = []
    for eng, v in (sk.get("by_engine") or {}).items():
        ns, hr = v.get("n_scored") or 0, v.get("hit_rate")
        if ns >= 10 and hr is not None and hr < 0.35:
            fade.append({"engine": eng, "n_scored": ns, "hit_rate": round(hr, 3),
                         "inverted_expectancy_hit": round(1 - hr, 3),
                         "avg_claimed_conf": v.get("avg_claimed_confidence"),
                         "action": "FADE — take the opposite side; engine is reliably wrong"})
    fade.sort(key=lambda x: x["hit_rate"])

    # ── provenance ledger (18-lite) ──
    ledger = {"date": now.date().isoformat(), "written_at": now.isoformat(), "briefs": {}}
    for k in CORE_BRIEFS:
        try:
            h = S3.head_object(Bucket=BUCKET, Key=k)
            ledger["briefs"][k] = {"etag": h["ETag"].strip('"'),
                                   "last_modified": h["LastModified"].isoformat(),
                                   "bytes": h["ContentLength"]}
        except Exception:
            ledger["briefs"][k] = None
    S3.put_object(Bucket=BUCKET, Key=f"data/_ledger/{now.date().isoformat()}.json",
                  Body=json.dumps(ledger).encode(), ContentType="application/json")

    out = {"engine": "confluence-meta", "version": VERSION,
           "generated_at": now.isoformat(), "duration_s": round(time.time() - t0, 1),
           "signals_window_days": 120, "n_signals": len(items),
           "n_active_days": len(by_day),
           "today_confluence": {d: {k: sorted(v) for k, v in dd.items()}
                                 for d, dd in by_day.items()
                                 if d >= (now - timedelta(days=3)).date().isoformat()},
           "confluence_curves": curves,
           "fade_index": fade,
           "ledger_briefs_ok": sum(1 for v in ledger["briefs"].values() if v),
           "methodology": ("Confluence = distinct engines aligned same-direction per calendar day "
                           "(closed-loop log, 120d). Forward distributions are computed against the "
                           "1971+ deep equity base — real n shown per bucket. Fade Index lists engines "
                           "with ≥10 graded calls and <35% hit: their inverse is the signal. Daily "
                           "provenance ledger fingerprints every core brief (ETag+timestamp) for "
                           "point-in-time replay and audit.")}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[confluence] signals={len(items)} days={len(by_day)} fade={len(fade)} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"n": len(items), "fade": len(fade)})}
