"""justhodl-self-critique — F7 v1 (ops 4458).

Daily prediction-vs-actual measurement substrate. Snapshots today's stated
verdicts from the predicting engines (risk-gate state, rotation regime,
breadth-thrust signal, bitcoin-rainbow zone, four-canary verdict, credit-
first stage), diffs against yesterday's snapshot, and records which calls
HELD, FLIPPED, or were UNAVAILABLE -> data/llm/self-critique/{date}.json
+ rolling summary. The LLM synthesis pass (turning deltas into brain-rule
proposals) is deliberately NOT automatic: per F6, any rule change it
suggests must be filed to data/audit/approvals.json for KHALID — measured
here, gated there, nothing self-applies."""
import json
import os
from datetime import datetime, timedelta, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")

WATCH = {
    "risk_gate": ("data/risk-gate.json", ["state", "verdict", "gate"]),
    "rotation_regime": ("data/rotation-dashboard.json",
                        ["regime", "nowcast", "state"]),
    "breadth_thrust": ("data/breadth-thrust.json",
                       ["state", "signal_strength"]),
    "btc_rainbow": ("data/bitcoin-rainbow.json",
                    ["zone", "verdict", "band"]),
    "four_canary": ("data/plumbing-stress.json",
                    ["enrichment.four_canary.verdict"]),
    "credit_first": ("liquidity-data.json",
                     ["part4.credit_first_sequence.verdict"]),
}


def _get(k):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception:
        return None


def _pluck(d, paths):
    for p in paths:
        cur = d
        ok = True
        for seg in p.split("."):
            if isinstance(cur, dict) and seg in cur:
                cur = cur[seg]
            else:
                ok = False
                break
        if ok and isinstance(cur, (str, int, float)):
            return str(cur)
    return None


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    yday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    snap = {}
    for name, (key, paths) in WATCH.items():
        d = _get(key)
        v = _pluck(d, paths) if d else None
        snap[name] = (v if v is not None else
                      {"data_unavailable": True,
                       "reason": "feed or field missing"})
    prev = _get(f"data/llm/self-critique/{yday}.json") or {}
    pv = prev.get("verdicts") or {}
    diffs = {}
    held = flipped = unavail = 0
    for name, v in snap.items():
        p = pv.get(name)
        if isinstance(v, dict) or isinstance(p, dict) or p is None:
            diffs[name] = {"status": "UNAVAILABLE_OR_FIRST",
                           "today": v, "yesterday": p}
            unavail += 1
        elif v == p:
            diffs[name] = {"status": "HELD", "value": v}
            held += 1
        else:
            diffs[name] = {"status": "FLIPPED", "from": p, "to": v}
            flipped += 1
    doc = {"date": today,
           "as_of": now.isoformat(timespec="seconds"),
           "verdicts": snap, "vs_yesterday": diffs,
           "counts": {"held": held, "flipped": flipped,
                      "unavailable": unavail},
           "governance": "LLM synthesis of these deltas into brain-rule "
                         "proposals routes through data/audit/approvals."
                         "json (F6) — Khalid decides; nothing self-applies"}
    s3.put_object(Bucket=BUCKET, Key=f"data/llm/self-critique/{today}.json",
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json")
    s3.put_object(Bucket=BUCKET, Key="data/llm/self-critique/latest.json",
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True, "counts": doc["counts"],
           "verdicts": {k: (v if isinstance(v, str) else "N/A")
                        for k, v in snap.items()}}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
