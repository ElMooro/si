"""justhodl-auction-tail v1.0 — auction demand as a graded duration family
(creative #6, ops 3446).

Hypothesis, submitted to the loop rather than assumed: WEAK coupon auctions
(big positive tail / D-F composite) → DOWN the tenor's duration ETF over the
next days; STRONG stop-throughs (negative tail, A-tier) → UP. Graded vs BIL
(cash) so carry is neutralized; the PROVEN gate decides if either side is
real. Reads data/auction-grades.json (justhodl-auction-grader, 16:00 M-F).

Feed: data/auction-tail.json · Signals: type "auction-tail" [3,10,21].
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone

import boto3

from signals_emit import log_signal, yprice

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SRC_KEY = "data/auction-grades.json"
OUT_KEY = "data/auction-tail.json"
s3 = boto3.client("s3", "us-east-1")
ddb = boto3.resource("dynamodb", "us-east-1")


def etf_for(term):
    t = str(term or "")
    if any(x in t for x in ("20-Year", "30-Year")):
        return "TLT"
    if any(x in t for x in ("5-Year", "7-Year", "10-Year")):
        return "IEF"
    if any(x in t for x in ("2-Year", "3-Year")):
        return "SHY"
    return None


def plan(rows, cutoff):
    """Pure rule → planned signals (probe-testable, no side effects)."""
    plans = []
    for r in rows:
        if not isinstance(r, dict) or not r.get("overall_grade"):
            continue
        if str(r.get("auction_date") or "")[:10] < cutoff:
            continue
        if str(r.get("security_type") or "").upper() not in ("NOTE", "BOND"):
            continue
        etf = etf_for(r.get("security_term"))
        if not etf:
            continue
        tail = ((r.get("dimensions") or {}).get("tail_bp") or {}).get("value")
        g = str(r.get("overall_grade"))
        direction = None
        if (isinstance(tail, (int, float)) and tail >= 1.2) or g in ("D+", "D", "D-", "F"):
            direction = "DOWN"
        elif (isinstance(tail, (int, float)) and tail <= -1.0) and g.startswith("A"):
            direction = "UP"
        if direction:
            plans.append({"etf": etf, "direction": direction,
                          "term": r.get("security_term"), "grade": g,
                          "tail_bp": tail,
                          "auction_date": str(r.get("auction_date"))[:10]})
    return plans


def lambda_handler(event, context):
    t0 = time.time()
    try:
        doc = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=SRC_KEY)["Body"].read())
    except Exception as e:
        print(f"[auction-tail] source unreadable: {str(e)[:80]}")
        doc = {}
    rows = []

    def walk(o):
        if isinstance(o, dict):
            if o.get("overall_grade") and o.get("auction_date"):
                rows.append(o)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(doc if not (event or {}).get("_probe") else {})
    if (event or {}).get("_probe"):
        rows = event["_probe"].get("rows") or []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=2)).date().isoformat()
    plans = plan(rows, cutoff if not (event or {}).get("_probe") else "1900-01-01")
    if (event or {}).get("_probe"):
        return {"statusCode": 200, "body": json.dumps({"plans": plans})}

    tbl = ddb.Table("justhodl-signals")
    logged = 0
    for p in plans[:8]:
        mark = yprice(p["etf"])
        time.sleep(0.15)
        p["mark"] = mark
        if mark and log_signal(
                tbl, "auction-tail", p["etf"], p["direction"], [3, 10, 21],
                mark, confidence=0.60,
                rationale=(f"{p['term']} auction {p['auction_date']} graded "
                           f"{p['grade']} (tail {p['tail_bp']}bp) → "
                           f"{p['direction']} duration vs cash"),
                benchmark="BIL",
                metadata={"engine": "auction-tail", "grade": p["grade"],
                          "tail_bp": p["tail_bp"]}):
            logged += 1
    out = {"ok": True, "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": round(time.time() - t0, 2),
           "n_graded_rows": len(rows), "plans": plans, "logged": logged,
           "methodology": ("Fresh (<=2d) NOTE/BOND auction grades → duration "
                           "ETF signals: tail>=+1.2bp or D/F = DOWN; "
                           "stop-through<=-1.0bp with A-tier = UP; graded "
                           "[3,10,21] vs BIL. PROVEN gate controls "
                           "promotion.")}
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=600")
    print(f"[auction-tail] rows={len(rows)} plans={len(plans)} logged={logged}")
    return {"statusCode": 200, "body": json.dumps(
        {"ok": True, "plans": len(plans), "logged": logged})}
