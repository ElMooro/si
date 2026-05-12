#!/usr/bin/env python3
"""Step 473 — Verify all 3 Lambdas exist after config-schema fix + invoke them."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/473_three_lambdas_verify.json"
NAME = "justhodl-tmp-473"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {"lambdas": {}, "invokes": {}, "sidecars": {}}
    LAMBDAS = ["justhodl-alpha-confluence", "justhodl-alpha-alerts", "justhodl-alpha-daily-brief"]
    for name in LAMBDAS:
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            env_keys = list((cfg.get("Environment") or {}).get("Variables", {}).keys())
            out["lambdas"][name] = {"exists": True, "last_modified": cfg["LastModified"],
                                     "env_keys": env_keys, "memory": cfg["MemorySize"]}
        except Exception as e:
            out["lambdas"][name] = {"exists": False, "err": str(e)[:200]}

    # Invoke confluence (won't side-effect Telegram)
    if out["lambdas"].get("justhodl-alpha-confluence", {}).get("exists"):
        try:
            resp = lam.invoke(FunctionName="justhodl-alpha-confluence",
                                InvocationType="RequestResponse", Payload=b"{}")
            body = resp["Payload"].read().decode("utf-8")
            parsed = json.loads(body)
            out["invokes"]["confluence"] = json.loads(parsed["body"]) if parsed.get("body") else parsed
        except Exception as e:
            out["invokes"]["confluence_err"] = str(e)[:300]

    # Read sidecars
    for key, label in [("signals/confluence.json", "confluence"),
                         ("signals/regime-picks.json", "regime_picks")]:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
            body = obj["Body"].read()
            p = json.loads(body)
            if label == "confluence":
                tier_s = p.get("tier_s_confluence") or []
                tier_a = p.get("tier_a_confluence") or []
                out["sidecars"][label] = {
                    "size_kb": round(len(body)/1024, 1),
                    "regime": p.get("regime"),
                    "regime_confidence": p.get("regime_confidence"),
                    "tier_distribution": p.get("tier_distribution"),
                    "top_5_tier_s": [{"sym": s["symbol"], "alpha": s.get("alpha_score"),
                                       "n_firing": s.get("confluence_count"),
                                       "sig0": (s.get("top_signals") or [None])[0]}
                                      for s in tier_s[:5]],
                    "top_5_tier_a": [{"sym": s["symbol"], "alpha": s.get("alpha_score"),
                                       "n_firing": s.get("confluence_count")}
                                      for s in tier_a[:5]],
                }
            else:
                picks = p.get("regime_picks") or []
                avoids = p.get("regime_avoids") or []
                out["sidecars"][label] = {
                    "size_kb": round(len(body)/1024, 1),
                    "regime": p.get("regime"),
                    "n_picks": len(picks), "n_avoids": len(avoids),
                    "sector_prefs": p.get("regime_sector_preferences"),
                    "top_picks": [{"sym": r["symbol"], "alpha": r.get("alpha_score"),
                                     "sector": r.get("sector"),
                                     "adj_score": r.get("regime_adj_score")}
                                    for r in picks[:5]],
                }
        except Exception as e:
            out["sidecars"][label + "_err"] = str(e)[:200]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=180, Code={"ZipFile": zb})
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
