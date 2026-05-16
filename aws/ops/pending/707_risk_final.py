"""ops/707 — verify bank-stress unit fix landed + defcon.html live."""
import json, os, base64, urllib.request
import boto3
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    report = {"started": datetime.now(timezone.utc).isoformat()}

    # 1. re-invoke bank-stress, confirm sane units
    r = lam.invoke(FunctionName="justhodl-bank-stress",
                    InvocationType="RequestResponse", Payload=b"{}", LogType="Tail")
    body = r["Payload"].read().decode("utf-8", "replace")
    report["bank_stress_invoke"] = {"status": r.get("StatusCode"),
                                     "fn_error": r.get("FunctionError"),
                                     "response": body[:400]}
    try:
        sc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/bank-stress.json")["Body"].read())
        el = sc.get("emergency_liquidity", {})
        ra = sc.get("reserve_adequacy", {})
        dw = el.get("discount_window_bn")
        report["bank_stress"] = {
            "bank_stress_score": sc.get("bank_stress_score"),
            "regime": sc.get("regime"),
            "emergency_draw": sc.get("emergency_draw"),
            "discount_window_bn": dw,
            "btfp_bn": el.get("btfp_outstanding_bn"),
            "swap_lines_bn": el.get("swap_lines_bn"),
            "reserves_bn": ra.get("reserves_bn"),
            "reserves_to_gdp_pct": ra.get("reserves_to_gdp_pct"),
            "checks": {
                "discount_window_sane": dw is not None and 0 <= dw <= 300,
                "score_sane": sc.get("bank_stress_score") is not None
                              and 0 <= sc.get("bank_stress_score") <= 100,
            },
        }
    except Exception as e:
        report["bank_stress"] = {"err": str(e)[:200]}

    # 2. defcon.html live
    try:
        req = urllib.request.Request("https://justhodl.ai/defcon.html",
                                      headers={"User-Agent": "JustHodl-Verify/1.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            code = resp.getcode()
            html = resp.read().decode("utf-8", "replace")
    except Exception as e:
        code, html = None, str(e)[:200]
    markers = ["Risk &amp; Opportunity Command Center", "crisis-composite.json",
               "capitulation.json", "china-liquidity.json", "bank-stress.json",
               "Master Crisis Score", "Capitulation"]
    report["defcon_page"] = {
        "http_status": code,
        "size_bytes": len(html) if isinstance(html, str) else None,
        "all_markers_ok": (isinstance(html, str) and code == 200
                            and all(m in html for m in markers)),
        "missing": [m for m in markers if isinstance(html, str) and m not in html],
    }

    report["summary"] = {
        "bank_stress_units_fixed": all((report.get("bank_stress", {}).get("checks") or {}).values()),
        "defcon_page_live": report["defcon_page"]["all_markers_ok"],
    }
    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/707_risk_final.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 707_risk_final.json :: " + json.dumps(report["summary"]))


if __name__ == "__main__":
    main()
