"""justhodl-inverse-harvester v1.0 — shadow fades of proven anti-predictive
families (ops 3426, alpha-triage INVERT verdicts).

eng:ai-infra-stack (flip 89%/100% by half, +18.4%/sig after cost) and
eng:finnhub-signals (90%/80%, +10.6%) keep emitting; this engine mirrors
each fresh signal as type "inv:<family>" with the direction FLIPPED and the
same benchmark — graded FORWARD, fully out-of-sample. The PROVEN gate, not
the in-sample study, decides if the fades ever reach the composer.
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr

from signals_emit import log_signal, yprice

VERSION = "1.1.0"
FALLBACK_INVERT = ["eng:ai-infra-stack", "eng:finnhub-signals"]


def invert_families():
    """v1.1 (ops 3442): the fade desk is now SELF-UPDATING — families come
    from data/alpha-triage.json verdicts, so future triage runs change the
    book without touching this engine."""
    try:
        import boto3 as _b
        j = json.loads(_b.client("s3", "us-east-1").get_object(
            Bucket="justhodl-dashboard-live",
            Key="data/alpha-triage.json")["Body"].read())
        fams = sorted(f for f, v in (j.get("families") or {}).items()
                      if (v or {}).get("verdict") == "INVERT")
        return fams or FALLBACK_INVERT
    except Exception as e:
        print(f"[inverse] triage read failed ({str(e)[:60]}) -> fallback")
        return FALLBACK_INVERT
ddb = boto3.resource("dynamodb", "us-east-1")


def lambda_handler(event, context):
    t0 = time.time()
    tbl = ddb.Table("justhodl-signals")
    fams = invert_families()
    if (event or {}).get("_families_probe"):
        return {"statusCode": 200, "body": json.dumps({"families": fams})}
    if (event or {}).get("_test_suppress"):
        ok = log_signal(tbl, event["_test_suppress"], "SPY", "UP", [5],
                        100.0, confidence=0.5, rationale="suppress-test")
        return {"statusCode": 200,
                "body": json.dumps({"suppress_test": event["_test_suppress"],
                                    "logged": bool(ok)})}
    today = datetime.now(timezone.utc).date().isoformat()
    rows, lek = [], None
    fe = Attr("signal_type").is_in(fams)
    while True:
        kw = {"FilterExpression": fe}
        if lek:
            kw["ExclusiveStartKey"] = lek
        r = tbl.scan(**kw)
        rows += [x for x in r.get("Items", [])
                 if str(x.get("logged_at", ""))[:10] == today]
        lek = r.get("LastEvaluatedKey")
        if not lek:
            break
    mirrored = 0
    for x in rows:
        fam = x["signal_type"]
        tk = str(x.get("measure_against") or x.get("ticker") or "").upper()
        if not tk:
            continue
        d = "DOWN" if str(x.get("predicted_direction") or "UP") == "UP" else "UP"
        base = x.get("baseline_price")
        try:
            base = float(base)
        except Exception:
            base = None
        if not base:
            base = yprice(tk)
            time.sleep(0.1)
        if not base:
            continue
        if log_signal(tbl, f"inv:{fam}", tk, d, [7, 14, 30], base,
                      confidence=0.55,
                      rationale=f"shadow fade of {fam} (alpha-triage INVERT)",
                      benchmark=str(x.get("benchmark") or "SPY"),
                      metadata={"engine": "inverse-harvester", "src": fam}):
            mirrored += 1
    print(f"[inverse] src_today={len(rows)} mirrored={mirrored} "
          f"{round(time.time() - t0, 1)}s")
    return {"statusCode": 200, "body": json.dumps(
        {"ok": True, "src": len(rows), "mirrored": mirrored})}
