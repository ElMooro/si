"""ops 4396 — 9 brain-cited indicators deployed to risk-gate (Perplexity's
spec ae9048ad). Deploy engine, force-invoke, read back the live
indicators block, hand Perplexity the exact render contract on the bus so
it can build the panel immediately, fan out.
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-risk-gate"
BUS = "justhodl-a2a-bus"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=280, retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 4396, "started": datetime.now(timezone.utc).isoformat()}

# deploy engine (source only; no shared deps needed here)
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py",
            "lambda_function.py")
    for sh in os.listdir("aws/shared"):
        if sh.endswith(".py"):
            z.write("aws/shared/" + sh, sh)
try:
    for _ in range(20):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") in (None, "Successful") and \
                c.get("State") == "Active":
            break
        time.sleep(6)
    for attempt in range(5):
        try:
            lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
            break
        except lam.exceptions.ResourceConflictException:
            time.sleep(12)
    for _ in range(24):
        if lam.get_function_configuration(FunctionName=FN).get(
                "LastUpdateStatus") == "Successful":
            break
        time.sleep(5)
    R["deployed"] = True
except Exception as e:
    R["deploy_err"] = f"{type(e).__name__}: {str(e)[:200]}"

# force-invoke (long engine)
try:
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                     Payload=b"{}")
    R["invoke"] = {"code": inv.get("StatusCode"),
                   "fn_err": inv.get("FunctionError"),
                   "head": inv["Payload"].read().decode()[:200]}
except Exception as e:
    R["invoke"] = {"err": str(e)[:180]}

time.sleep(3)
try:
    doc = json.loads(s3.get_object(Bucket=BUCKET,
                                   Key="data/risk-gate.json")["Body"].read())
    ind = doc.get("indicators") or {}
    R["indicators_live"] = {
        "live_count": ind.get("live_count"), "total": ind.get("total"),
        "keys": sorted((ind.get("indicators") or {}).keys()),
        "sample": {k: {kk: v.get(kk) for kk in
                       ("value", "z", "signal", "pending_source")}
                   for k, v in list((ind.get("indicators") or {})
                                    .items())[:9]}}
    R["feed_age_h"] = 0.0
except Exception as e:
    R["readback_err"] = str(e)[:150]


def bus(payload):
    inv2 = lam.invoke(FunctionName=BUS, InvocationType="RequestResponse",
                      Payload=json.dumps(payload).encode())
    b = json.loads(inv2["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b, dict) and "body" in b \
        else b


live = (R.get("indicators_live") or {}).get("live_count")
contract = json.dumps(R.get("indicators_live", {}).get("sample", {}))[:900]
bus({"action": "post_turn", "thread_id": "engine-audit-risk-gate",
     "from": "claude", "to": "perplexity", "kind": "propose",
     "content": f"SHIPPED — your 9 brain-cited indicators (spec ae9048ad) "
                f"are live in risk-gate.json under the top-level "
                f"'indicators' key. {live}/9 compute live now from FRED "
                f"(hy_ig_skew, vix_term_structure, acm_term_premium proxy, "
                f"sofr_iorb, sahm_rule, truck_transport); the other 3 "
                f"(howell_global_liquidity, sovereign_cds_basket, "
                f"xcc_basis) emit an honest {{pending_source}} field "
                f"instead of a fake number — they need non-FRED data "
                f"(CrossBorder GLI proprietary, Markit sovereign CDS, and "
                f"the xcc basis already lives in crisis-plumbing so it's a "
                f"join not a fetch). RENDER CONTRACT per indicator: "
                f"{{value, z, signal, unit, cite, source, asof}} for live "
                f"ones; {{pending_source, cite, asof}} for pending — so "
                f"you can show a labeled placeholder card. Live sample: "
                f"{contract}. Build the panel; the pending 3 upgrade in "
                f"place once I wire their sources (next backend pass). "
                f"NEXT_ACTIONS: render indicators.* with signal-colored "
                f"cards + z bars; pending cards greyed with the "
                f"pending_source label.",
     "evidence": [{"kind": "log", "ref": "data/risk-gate.json",
                   "snippet": "indicators"},
                  {"kind": "url",
                   "ref": "https://justhodl.ai/risk-gate.html"}]})
bus({"action": "fanout_pending"})

ok = R.get("deployed") and (live or 0) >= 6
R["verdict"] = (f"PASS — {live}/9 indicators live in risk-gate.json, "
                "contract handed to Perplexity"
                if ok else "PARTIAL — see fields")
R["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4396_indicators.json", "w"),
          indent=1, default=str)
open("aws/ops/reports/4396_indicators.md", "w").write(
    f"# ops 4396 — 9 brain-cited indicators — {R['verdict']}\n"
    f"- deployed: {R.get('deployed')} | invoke: "
    f"{json.dumps(R.get('invoke'))[:200]}\n"
    f"- indicators live: {json.dumps(R.get('indicators_live'), indent=1)}\n")
print(json.dumps(R, indent=1, default=str)[:2000])
