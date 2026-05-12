#!/usr/bin/env python3
"""Step 482 — Verify all 3 portfolio Lambdas deployed.
Demo end-to-end: add a position via admin → invoke snapshot → invoke risk →
remove the position. Shows what the system would look like with real holdings."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/482_portfolio_verify.json"
NAME = "justhodl-tmp-482"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def invoke(name, payload):
    try:
        resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
                            Payload=json.dumps(payload).encode())
        body = resp["Payload"].read().decode("utf-8")
        parsed = json.loads(body)
        return json.loads(parsed["body"]) if parsed.get("body") else parsed
    except Exception as e:
        return {"err": str(e)[:300]}

def lambda_handler(event, context):
    out = {"step1_lambdas": {}, "step2_admin_add": None, "step3_snapshot": None,
           "step4_risk": None, "step5_admin_list": None, "step6_admin_remove": None,
           "step7_snapshot_after": None, "snapshot_data": None, "risk_data": None}

    # 1. Verify all 3 Lambdas exist
    for name in ["justhodl-portfolio-admin", "justhodl-portfolio-snapshot", "justhodl-portfolio-risk"]:
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            out["step1_lambdas"][name] = {
                "exists": True,
                "last_modified": cfg["LastModified"][:19],
                "env_keys": list((cfg.get("Environment") or {}).get("Variables", {}).keys()),
                "memory": cfg["MemorySize"], "timeout": cfg["Timeout"],
            }
        except Exception as e:
            out["step1_lambdas"][name] = {"exists": False, "err": str(e)[:200]}

    if not all(v.get("exists") for v in out["step1_lambdas"].values()):
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # 2. Add 3 demo positions via admin (will remove after demo)
    demo_positions = [
        {"action": "add_position", "symbol": "LLY", "qty": 50,
         "cost_basis_per_share": 925.00, "stop_loss": 890,
         "target_weight_pct": 25, "sector": "Healthcare",
         "notes": "Daily brief LONG · regime-adj 84"},
        {"action": "add_position", "symbol": "VRT", "qty": 100,
         "cost_basis_per_share": 365.00, "stop_loss": 340,
         "target_weight_pct": 35, "sector": "Industrials",
         "notes": "TIER S confluence (6/7 firing)"},
        {"action": "add_position", "symbol": "MU", "qty": 60,
         "cost_basis_per_share": 790.00, "stop_loss": 730,
         "target_weight_pct": 40, "sector": "Technology",
         "notes": "TIER A confluence · AI memory demand"},
    ]
    out["step2_admin_add"] = []
    for pos in demo_positions:
        r = invoke("justhodl-portfolio-admin", pos)
        out["step2_admin_add"].append({"symbol": pos["symbol"], "ok": r.get("ok"),
                                          "err": r.get("err")})

    time.sleep(2)

    # 3. Invoke snapshot
    out["step3_snapshot"] = invoke("justhodl-portfolio-snapshot", {})
    time.sleep(2)

    # Read the snapshot
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="portfolio/snapshot.json")
        out["snapshot_data"] = json.loads(obj["Body"].read())
    except Exception as e:
        out["snapshot_read_err"] = str(e)[:300]

    # 4. Invoke risk
    out["step4_risk"] = invoke("justhodl-portfolio-risk", {})
    time.sleep(2)

    # Read the risk report
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="portfolio/risk.json")
        out["risk_data"] = json.loads(obj["Body"].read())
    except Exception as e:
        out["risk_read_err"] = str(e)[:300]

    # 5. Admin list (sanity)
    out["step5_admin_list"] = invoke("justhodl-portfolio-admin", {"action": "list", "filter": "POSITION"})

    # 6. Remove demo positions (clean up)
    out["step6_admin_remove"] = []
    for sym in ["LLY", "VRT", "MU"]:
        r = invoke("justhodl-portfolio-admin", {"action": "remove_position", "symbol": sym})
        out["step6_admin_remove"].append({"symbol": sym, "ok": r.get("ok"), "existed": r.get("existed")})

    # 7. Snapshot after cleanup (should show 0 positions)
    out["step7_snapshot_after"] = invoke("justhodl-portfolio-snapshot", {})

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 180s for Lambda deploys...")
    _time.sleep(180)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=600, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    _time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:30000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
