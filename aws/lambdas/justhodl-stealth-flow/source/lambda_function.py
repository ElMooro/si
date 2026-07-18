"""justhodl-stealth-flow v1.0 — the measured edge, finally traded (ops 3442).

The event study proved it out-of-sample style: STEALTH detections (flat price
+ real inflows, |z|>=2 vs 60d baseline) hit 75% at +298bps over 5 days, while
chase-cohort detections LOST -406bps. This engine emits each fresh STEALTH
detection as a graded UP signal — windows 5/21 vs its ladder benchmark — so
the PROVEN gate (not the study) decides composer entry.

Feed: data/stealth-flow.json · Signals: type "stealth-flow".
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone

import boto3

from signals_emit import log_signal, yprice

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SRC_KEY = "data/etf-flows/event-study.json"
OUT_KEY = "data/stealth-flow.json"
s3 = boto3.client("s3", "us-east-1")
ddb = boto3.resource("dynamodb", "us-east-1")


def lambda_handler(event, context):
    t0 = time.time()
    try:
        doc = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=SRC_KEY)["Body"].read())
    except Exception as e:
        print(f"[stealth] source unreadable: {str(e)[:80]}")
        doc = {}
    cutoff = (datetime.now(timezone.utc) - timedelta(days=2)).date().isoformat()
    hits, seen = [], set()

    def walk(o):
        if isinstance(o, dict):
            q = str(o.get("quadrant") or o.get("cohort") or "")
            tk = o.get("ticker") or o.get("symbol")
            d = str(o.get("date") or o.get("detected") or o.get("detected_at")
                    or "")[:10]
            if tk and "STEALTH" in q.upper() and d >= cutoff:
                k = str(tk).upper()
                if k not in seen:
                    seen.add(k)
                    hits.append({"ticker": k, "quadrant": q, "date": d,
                                 "z": o.get("z") or o.get("flow_z"),
                                 "bench": o.get("bench") or o.get("benchmark")
                                 or "SPY"})
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(doc)

    tbl = ddb.Table("justhodl-signals")
    logged = 0
    for h in hits[:15]:
        mark = yprice(h["ticker"])
        time.sleep(0.15)
        h["mark"] = mark
        if mark and log_signal(
                tbl, "stealth-flow", h["ticker"], "UP", [5, 21], mark,
                confidence=0.62,
                rationale=(f"STEALTH flow detection {h['date']} "
                           f"(z={h.get('z')}): inflows without price — the "
                           "75%/+298bps cohort from the event study"),
                benchmark=str(h["bench"]),
                metadata={"engine": "stealth-flow", "quadrant": h["quadrant"]}):
            logged += 1
    out = {"ok": True, "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": round(time.time() - t0, 2),
           "recent_stealth": hits, "n_recent": len(hits), "logged": logged,
           "methodology": ("Fresh (<=2d) STEALTH-quadrant detections from the "
                           "ETF flow event study, emitted as graded UP "
                           "signals vs their ladder benchmark. PROVEN gate "
                           "controls promotion.")}
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=600")
    print(f"[stealth] recent={len(hits)} logged={logged} "
          f"{round(time.time() - t0, 1)}s")
    return {"statusCode": 200, "body": json.dumps(
        {"ok": True, "n": len(hits), "logged": logged})}
