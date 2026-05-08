#!/usr/bin/env python3
"""Step 363 — Verify the 6 new latent-Lambda UIs are live + rendering."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/363_six_uis_verify.json"
NAME = "justhodl-tmp-six-uis-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request

PAGES = {
    "narrative": ("https://justhodl.ai/narrative.html",
                  ["Narrative Density", "narrative-density.json", "themeRow", "renderRow", "alzheimer", "TIER_"]),
    "cross-asset": ("https://justhodl.ai/cross-asset.html",
                    ["Cross-Asset Regime", "cross-asset-regime.json", "regime-card", "correlation_matrix", "renderRegimeCard"]),
    "cot-extremes": ("https://justhodl.ai/cot-extremes.html",
                     ["COT Extremes", "cot/extremes/current.json", "Cluster", "percentile", "filterMatch"]),
    "dealer-survey": ("https://justhodl.ai/dealer-survey.html",
                      ["Dealer Survey", "dealer-survey.json", "PDF parsing", "Roadmap", "FOMC date"]),
    "gdelt": ("https://justhodl.ai/gdelt.html",
              ["GDELT Sentiment", "gdelt-news.json", "tone-gauge", "Most negative headlines", "asset_sentiment"]),
    "options-scanner": ("https://justhodl.ai/options-scanner.html",
                        ["Options Flow Scanner", "options-flow.json", "TIER_A_BULLISH_FLOW", "FINRA", "renderDetail"]),
}

def lambda_handler(event, context):
    out = {}
    for name, (url, markers) in PAGES.items():
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Verify/363"})
            with urllib.request.urlopen(req, timeout=15) as r:
                body = r.read().decode("utf-8", errors="ignore")
                size = len(body)
                hits = {m: m in body for m in markers}
                pct = sum(1 for v in hits.values() if v) / len(markers) * 100
                out[name] = {
                    "status": r.status, "size": size,
                    "markers_found": sum(1 for v in hits.values() if v),
                    "markers_total": len(markers),
                    "marker_pct": round(pct, 1),
                    "has_pwa_manifest": "manifest.json" in body,
                    "has_sw_register": "serviceWorker.register" in body,
                    "ok": r.status == 200 and pct >= 80,
                }
        except Exception as e:
            out[name] = {"error": f"{type(e).__name__}: {str(e)[:200]}"}
    # Summary
    n_pass = sum(1 for v in out.values() if v.get("ok"))
    out["summary"] = {"passed": n_pass, "total": len(PAGES), "rate": f"{n_pass}/{len(PAGES)}"}
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''

def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()
    try:
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=256, Timeout=120, Code={"ZipFile": zb},
        )
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
