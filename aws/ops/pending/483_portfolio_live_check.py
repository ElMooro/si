#!/usr/bin/env python3
"""Step 483 — verify live state of #9+#10 system (no demo, real state only)."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/483_portfolio_live_check.json"
NAME = "justhodl-tmp-483"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # 1. Lambdas — last modified + schedule
    eb = boto3.client("events", region_name="us-east-1")
    for name in ["justhodl-portfolio-admin", "justhodl-portfolio-snapshot", "justhodl-portfolio-risk"]:
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            info = {
                "last_modified": cfg["LastModified"][:19],
                "memory": cfg["MemorySize"], "timeout": cfg["Timeout"],
                "env_keys": list((cfg.get("Environment") or {}).get("Variables", {}).keys()),
                "code_size_kb": round(cfg["CodeSize"]/1024, 1),
            }
            # Check scheduled rule
            rule_name = f"{name}-hourly"
            try:
                rule = eb.describe_rule(Name=rule_name)
                info["scheduled"] = True
                info["cron"] = rule.get("ScheduleExpression")
                info["state"] = rule.get("State")
            except Exception:
                info["scheduled"] = False
            out[name] = info
        except Exception as e:
            out[name] = {"err": str(e)[:200]}

    # 2. DDB table state
    table = ddb.Table("justhodl-portfolio")
    try:
        # Count by partition key
        from boto3.dynamodb.conditions import Key
        ddb_state = {}
        for pk in ["POSITION", "WATCHLIST", "STOPLOSS", "META"]:
            r = table.query(KeyConditionExpression=Key("pk").eq(pk))
            ddb_state[pk] = {"count": len(r["Items"]),
                            "symbols": [i.get("symbol", i.get("sk")) for i in r["Items"][:30]]}
        out["ddb_state"] = ddb_state
    except Exception as e:
        out["ddb_err"] = str(e)[:300]

    # 3. Latest sidecars on S3
    for key in ["portfolio/snapshot.json", "portfolio/risk.json"]:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
            body = obj["Body"].read()
            p = json.loads(body)
            out[key] = {
                "size_kb": round(len(body)/1024, 1),
                "last_modified": str(obj["LastModified"])[:19],
                "generated_at": p.get("generated_at"),
                "status": p.get("status"),
                "n_positions": p.get("n_positions") or (p.get("portfolio_summary") or {}).get("n_positions"),
                "n_watchlist": (p.get("counts") or {}).get("watchlist"),
                "total_value": p.get("total_market_value") or (p.get("portfolio_summary") or {}).get("total_market_value"),
                "var_1d_99_pct": p.get("var_1d_99_pct"),
                "portfolio_vol": p.get("portfolio_vol_annual_pct"),
                "portfolio_beta": p.get("portfolio_beta_spy"),
            }
        except Exception as e:
            out[key] = {"err": str(e)[:200]}

    # 4. /portfolio/ page deployed?
    try:
        import urllib.request
        req = urllib.request.Request("https://justhodl.ai/portfolio/")
        with urllib.request.urlopen(req, timeout=8) as r:
            body = r.read(2000).decode("utf-8", errors="ignore")
        out["page"] = {"http_status": "200", "has_title": "Portfolio + Risk" in body,
                       "size_partial": len(body)}
    except Exception as e:
        out["page"] = {"err": str(e)[:200]}

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
                            MemorySize=512, Timeout=120, Code={"ZipFile": zb})
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
