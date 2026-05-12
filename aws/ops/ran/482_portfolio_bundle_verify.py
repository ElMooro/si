#!/usr/bin/env python3
"""Step 482 — Verify portfolio Lambdas deployed + invoke snapshot+risk
end-to-end + read both sidecars."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/482_portfolio_bundle_verify.json"
NAME = "justhodl-tmp-482"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {"lambdas": {}, "invokes": {}, "sidecars": {}}
    LAMBDAS = ["justhodl-portfolio-admin", "justhodl-portfolio-snapshot", "justhodl-portfolio-risk"]
    for name in LAMBDAS:
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            out["lambdas"][name] = {
                "exists": True,
                "last_modified": cfg["LastModified"][:19],
                "env_keys": list((cfg.get("Environment") or {}).get("Variables", {}).keys()),
                "memory": cfg["MemorySize"], "timeout": cfg["Timeout"],
                "code_size": cfg["CodeSize"],
            }
        except Exception as e:
            out["lambdas"][name] = {"exists": False, "err": str(e)[:200]}

    # Invoke snapshot first (writes portfolio/snapshot.json)
    if out["lambdas"].get("justhodl-portfolio-snapshot", {}).get("exists"):
        try:
            resp = lam.invoke(FunctionName="justhodl-portfolio-snapshot",
                                InvocationType="RequestResponse", Payload=b"{}")
            body = resp["Payload"].read().decode("utf-8")
            parsed = json.loads(body)
            out["invokes"]["snapshot"] = json.loads(parsed["body"]) if parsed.get("body") else parsed
        except Exception as e:
            out["invokes"]["snapshot_err"] = str(e)[:400]

    # Then invoke risk (reads snapshot.json)
    if out["lambdas"].get("justhodl-portfolio-risk", {}).get("exists"):
        try:
            resp = lam.invoke(FunctionName="justhodl-portfolio-risk",
                                InvocationType="RequestResponse", Payload=b"{}")
            body = resp["Payload"].read().decode("utf-8")
            parsed = json.loads(body)
            out["invokes"]["risk"] = json.loads(parsed["body"]) if parsed.get("body") else parsed
        except Exception as e:
            out["invokes"]["risk_err"] = str(e)[:400]

    # Read both sidecars
    for key, label in [("portfolio/snapshot.json", "snapshot"),
                         ("portfolio/risk.json", "risk")]:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
            body = obj["Body"].read()
            p = json.loads(body)
            if label == "snapshot":
                out["sidecars"][label] = {
                    "size_kb": round(len(body)/1024, 1),
                    "generated_at": p.get("generated_at"),
                    "elapsed_seconds": p.get("elapsed_seconds"),
                    "portfolio_summary": p.get("portfolio_summary"),
                    "counts": p.get("counts"),
                    "n_positions": len(p.get("positions") or []),
                    "n_watchlist": len(p.get("watchlist") or []),
                    "top_watchlist": [{"sym": w["symbol"], "alpha": w.get("alpha_score"),
                                         "tier": w.get("tier"),
                                         "source": w.get("source")}
                                        for w in (p.get("watchlist") or [])[:10]],
                    "watchlist_sync_summary": p.get("watchlist_sync"),
                    "sector_concentration_keys": list((p.get("sector_concentration") or {}).keys())[:6],
                }
            else:
                out["sidecars"][label] = {
                    "size_kb": round(len(body)/1024, 1),
                    "generated_at": p.get("generated_at"),
                    "elapsed_seconds": p.get("elapsed_seconds"),
                    "status": p.get("status"),
                    "message": p.get("message"),
                    "n_positions": p.get("n_positions"),
                    "total_market_value": p.get("total_market_value"),
                    "portfolio_beta_spy": p.get("portfolio_beta_spy"),
                    "portfolio_vol_annual_pct": p.get("portfolio_vol_annual_pct"),
                    "var_1d_99_dollars": p.get("var_1d_99_dollars"),
                    "var_1d_99_pct": p.get("var_1d_99_pct"),
                    "max_sector_concentration_pct": p.get("max_sector_concentration_pct"),
                    "concentration_hhi": p.get("concentration_hhi"),
                    "concentration_label": p.get("concentration_label"),
                    "n_correlation_clusters": len(p.get("correlation_clusters") or []),
                    "n_stops_hit": len(p.get("stops_hit") or []),
                    "alerts_summary": p.get("alerts_summary"),
                    "n_scenarios": len(p.get("historical_scenarios") or []),
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
