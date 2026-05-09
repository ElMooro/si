#!/usr/bin/env python3
"""Step 376 — Verify auctions.html + bonds.html enhancements live."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/376_auctions_bonds_verify.json"
NAME = "justhodl-tmp-auctions-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request

def fetch(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Verify/376"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, r.read().decode("utf-8", errors="ignore")
    except Exception as e:
        return None, f"ERROR: {type(e).__name__}: {e}"

def lambda_handler(event, context):
    out = {"pages": {}}

    # auctions.html
    s, body = fetch("https://justhodl.ai/auctions.html")
    out["pages"]["auctions.html"] = {
        "status": s,
        "size": len(body) if isinstance(body, str) else None,
        "has_btc_tops": "BTC_TOPS" in body if isinstance(body, str) else False,
        "has_btc_bottoms": "BTC_BOTTOMS" in body if isinstance(body, str) else False,
        "has_btc_bulls": "BTC_BULLS" in body if isinstance(body, str) else False,
        "has_btc_bears": "BTC_BEARS" in body if isinstance(body, str) else False,
        "has_anchors": "HISTORICAL_ANCHORS" in body if isinstance(body, str) else False,
        "has_forward_matrix": "FORWARD_RETURNS" in body if isinstance(body, str) else False,
        "has_decisive_call": "DECISIVE CALL" in body if isinstance(body, str) else False,
        "has_methodology": "Methodology" in body if isinstance(body, str) else False,
        "has_live_fetch": "auction-crisis.json" in body if isinstance(body, str) else False,
        "ok": s == 200 and isinstance(body, str) and "BTC_TOPS" in body,
    }

    # bonds.html
    s, body = fetch("https://justhodl.ai/bonds.html")
    out["pages"]["bonds.html"] = {
        "status": s,
        "size": len(body) if isinstance(body, str) else None,
        "has_overlay": "Cross-Asset Interpretation Overlay" in body if isinstance(body, str) else False,
        "has_btc_inflections": "BTC inflection" in body if isinstance(body, str) else False,
        "has_asymmetric": "asymmetric truth" in body if isinstance(body, str) else False,
        "has_render_overlay": "renderCrossAssetOverlay" in body if isinstance(body, str) else False,
        "links_to_auctions": "/auctions.html" in body if isinstance(body, str) else False,
        "ok": s == 200 and isinstance(body, str) and "Cross-Asset Interpretation Overlay" in body,
    }

    # index.html
    s, body = fetch("https://justhodl.ai/")
    out["pages"]["index.html"] = {
        "status": s,
        "has_auctions_link": '/auctions.html' in body if isinstance(body, str) else False,
        "has_bonds_link": '/bonds.html' in body if isinstance(body, str) else False,
        "has_auctions_card": "TREASURY AUCTIONS" in body if isinstance(body, str) else False,
        "has_bonds_card": "BONDS &amp; YIELD CURVE" in body if isinstance(body, str) else False,
        "has_auctions_nav": "🏛 AUCTIONS" in body if isinstance(body, str) else False,
        "has_bonds_nav": "📈 BONDS" in body if isinstance(body, str) else False,
        "ok": s == 200 and isinstance(body, str) and "🏛 AUCTIONS" in body,
    }

    # Check 5 sample pages got the nav links
    for page in ["master-rank.html", "alpha-scoreboard.html", "today.html", "macro-data.html", "stress.html"]:
        s, body = fetch(f"https://justhodl.ai/{page}")
        out["pages"][page] = {
            "status": s,
            "has_auctions": "/auctions.html" in body if isinstance(body, str) else False,
            "has_bonds": "/bonds.html" in body if isinstance(body, str) else False,
            "ok": s == 200 and isinstance(body, str) and "/auctions.html" in body and "/bonds.html" in body,
        }

    n_ok = sum(1 for v in out["pages"].values() if v.get("ok"))
    out["summary"] = {"passed": n_ok, "total": len(out["pages"])}
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=256, Timeout=180, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed else parsed
    except Exception:
        out["raw"] = body[:5000]
    try:
        lam.delete_function(FunctionName=NAME)
    except Exception:
        pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
