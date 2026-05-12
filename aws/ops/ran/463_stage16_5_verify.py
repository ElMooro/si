#!/usr/bin/env python3
"""Step 463 — Verify Stage 16.5: 68-fund inverted index after deploy.
Should see new flagship positions from Maverick/Pabrai/Greenblatt/Sequoia.
"""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/463_stage16_5_verify.json"
NAME = "justhodl-tmp-463"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    cfg = lam.get_function_configuration(FunctionName="justhodl-smart-money-holdings")
    out["lambda"] = {"last_modified": cfg["LastModified"], "code_size": cfg["CodeSize"]}

    resp = lam.invoke(FunctionName="justhodl-smart-money-holdings",
                        InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    parsed = json.loads(body)
    out["invoke"] = json.loads(parsed["body"]) if parsed.get("body") else parsed

    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/smart-money-holdings.json")
    body = obj["Body"].read()
    p = json.loads(body)
    out["s3_size_mb"] = round(len(body)/1024/1024, 2)
    out["n_symbols"] = p.get("n_symbols")
    out["n_funds_scanned"] = p.get("n_funds_scanned")
    out["n_funds_attempted"] = p.get("n_funds_attempted")

    holdings = p.get("holdings") or {}
    flagship = sum(1 for e in holdings.values() if isinstance(e, dict) and (e.get("max_pct_of_fund") or 0) >= 10)
    high_conv = sum(1 for e in holdings.values() if isinstance(e, dict) and (e.get("max_pct_of_fund") or 0) >= 5)
    notable = sum(1 for e in holdings.values() if isinstance(e, dict) and (e.get("max_pct_of_fund") or 0) >= 1)
    out["stats"] = {"flagship": flagship, "high_conv": high_conv, "notable": notable}

    # New famous funds — top 5 each
    NEW = [
        ("0000934639", "Maverick Capital (Ainslie)"),
        ("0001720792", "Sequoia Fund (Ruane Cunniff)"),
        ("0001549575", "Pabrai (Dalal Street)"),
        ("0000883965", "Wally Weitz"),
        ("0000807985", "Mason Hawkins / Longleaf"),
        ("0001056831", "Bruce Berkowitz / Fairholme"),
        ("0001569049", "Light Street"),
        ("0001510387", "Joel Greenblatt / Gotham"),
        ("0000905567", "Yacktman"),
        ("0000936753", "Ariel Investments"),
        ("0001353316", "Hound Partners"),
        ("0001835549", "Engine No. 1"),
        ("0001112520", "Akre Capital"),
    ]
    funds = p.get("funds") or []
    fund_by_cik = {f["cik"]: f for f in funds}
    out["new_funds"] = {}
    for cik, label in NEW:
        f = fund_by_cik.get(cik)
        if not f:
            out["new_funds"][label] = {"missing": True}
            continue
        top5 = (f.get("top_holdings") or [])[:5]
        out["new_funds"][label] = {
            "cik": cik,
            "n_holdings": f.get("n_holdings"),
            "total_b": round((f.get("total_value") or 0)/1e9, 2),
            "top5": [{"sym": h.get("symbol"), "value_b": round((h.get("value") or 0)/1e9, 2),
                      "pct": h.get("pct_of_fund")} for h in top5],
        }

    # Top 30 flagships
    flagships = []
    for sym, e in holdings.items():
        if not isinstance(e, dict): continue
        mp = e.get("max_pct_of_fund")
        if mp is not None and mp >= 10:
            hs_sorted = sorted(e.get("holders") or [], key=lambda h: -(h.get("pct_of_fund") or 0))
            top = hs_sorted[0] if hs_sorted else None
            flagships.append({"sym": sym, "max_pct": mp,
                               "top_holder": top.get("name") if top else None,
                               "top_pct": top.get("pct_of_fund") if top else None,
                               "top_value_b": round((top.get("value") or 0)/1e9, 2) if top else None})
    flagships.sort(key=lambda f: -f["max_pct"])
    out["flagships_top30"] = flagships[:30]
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 90s for deploy...")
    _time.sleep(90)
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
        out["raw"] = body[:20000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
