"""justhodl-auction-tail v1.0 — auction-tail continuation family (ops 3446).

The documented pattern: coupon auctions that TAIL (weak demand — high yield
above when-issued, low bid-to-cover, D/F grade) precede short-horizon
duration weakness; auctions that stop THROUGH precede strength. The grader
already scores every auction at 16:00; this engine turns fresh grades into a
two-sided graded family vs cash (BIL benchmark = pure duration alpha), and
the PROVEN gate decides if it ever sizes.

Tenor map: 20Y/30Y->TLT · 5Y/7Y/10Y->IEF · 2Y/3Y->SHY · bills skipped.
Signals: type "auction-tail", windows 3/10/21 · Feed: data/auction-tail.json
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone

import boto3

from signals_emit import log_signal, yprice

VERSION = "1.0.1"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SRC = "data/auction-grades.json"
OUT = "data/auction-tail.json"
s3 = boto3.client("s3", "us-east-1")
ddb = boto3.resource("dynamodb", "us-east-1")


def etf_of(term):
    t = str(term or "").upper()
    if "30" in t or "20" in t:
        return "TLT"
    if "10" in t or "7" in t or "5" in t:
        return "IEF"
    if "3" in t or "2" in t:
        return "SHY"
    return None


def lambda_handler(event, context):
    t0 = time.time()
    try:
        doc = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=SRC)["Body"].read())
    except Exception as e:
        print(f"[auction-tail] source unreadable: {str(e)[:80]}")
        doc = {}
    cutoff = (datetime.now(timezone.utc) - timedelta(days=2)).date().isoformat()
    rows, seen = [], set()

    def walk(o):
        if isinstance(o, dict):
            dims = o.get("dimensions") or {}
            tv = None
            for _dk, _dv in (dims.items() if isinstance(dims, dict) else []):
                if "tail" in str(_dk).lower():
                    tv = (_dv.get("value") if isinstance(_dv, dict) else _dv)
                    break
            d = str(o.get("auction_date") or o.get("date") or "")[:10]
            sec_type = str(o.get("security_type") or "")
            if d and (tv is not None or o.get("overall_grade"))                     and "BILL" not in sec_type.upper():
                term = (o.get("tenor") or o.get("term") or o.get("security_term")
                        or o.get("security") or "")
                key = f"{term}|{d}"
                if key not in seen:
                    seen.add(key)
                    rows.append({
                        "term": str(term), "date": d,
                        "tail_bp": (float(tv) if tv is not None else None),
                        "grade": o.get("overall_grade") or o.get("grade")
                        or o.get("letter"),
                        "composite": o.get("composite_score"),
                        "btc": ((dims.get("bid_to_cover") or {}).get("value")
                                if isinstance(dims.get("bid_to_cover"), dict)
                                else None),
                        "etf": etf_of(term)})
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(doc)

    fresh = [r for r in rows if r["date"] >= cutoff and r["etf"]]
    tbl = ddb.Table("justhodl-signals")
    logged = 0
    for r in fresh:
        g = str(r.get("grade") or "").upper()[:1]
        tb = r.get("tail_bp")
        weak = (tb is not None and tb >= 1.2) or g in ("D", "F")
        strong = (tb is not None and tb <= -1.0) or g == "A"
        if not weak and not strong:
            r["action"] = "NEUTRAL"
            continue
        direction = "DOWN" if weak else "UP"
        r["action"] = direction
        mark = yprice(r["etf"])
        time.sleep(0.15)
        r["mark"] = mark
        conf = min(0.8, 0.55 + min(abs(r["tail_bp"]), 4.0) / 20.0)
        if mark and log_signal(
                tbl, "auction-tail", r["etf"], direction, [3, 10, 21], mark,
                confidence=conf,
                rationale=(f"{r['term']} auction {r['date']}: tail "
                           f"{(r.get('tail_bp') if r.get('tail_bp') is not None else 0):+.1f}bp, grade {r.get('grade')}, "
                           f"b/c {r.get('btc')} — "
                           + ("weak demand, duration fade"
                              if weak else "stopped through, duration bid")),
                benchmark="BIL",
                signal_value=str(r.get("tail_bp")),
                metadata={"engine": "auction-tail", "term": r["term"]}):
            logged += 1
    out = {"ok": True, "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": round(time.time() - t0, 2),
           "n_graded_seen": len(rows), "n_fresh": len(fresh),
           "logged": logged, "fresh": fresh,
           "rules": {"weak": "tail>=+1.2bp or grade D/F -> DOWN etf vs BIL",
                     "strong": "tail<=-1.0bp or grade A -> UP etf vs BIL",
                     "windows": [3, 10, 21]}}
    s3.put_object(Bucket=S3_BUCKET, Key=OUT,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=600")
    print(f"[auction-tail] seen={len(rows)} fresh={len(fresh)} "
          f"logged={logged} {round(time.time() - t0, 1)}s")
    return {"statusCode": 200, "body": json.dumps(
        {"ok": True, "fresh": len(fresh), "logged": logged})}
