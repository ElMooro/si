#!/usr/bin/env python3
"""Step 332 — Re-deploy global-macro (FMP endpoint fix) + morning-brief-tg
(Phase E with Fed Speak + Global Macro sections) + sync-test both.

Note: Re-deploys are handled automatically by the deploy-lambdas.yml workflow
when these source files change. This ops just sleeps for the deploy to settle,
then sync-invokes for verification.
"""
import json
import os
import time
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
REPORT = "aws/ops/reports/332_global_macro_fmp_fix_and_brief_phase_e.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def sync_invoke(name):
    started = time.time()
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
    return {
        "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "duration_s": round(time.time() - started, 1),
        "response_body": resp["Payload"].read().decode("utf-8")[:500],
    }


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Wait for deploy workflow to complete (typical 60-90s)
    print("[332] Sleeping 90s for deploy-lambdas workflow to settle…")
    time.sleep(90)

    # Re-test global-macro with new FMP endpoint
    print("[332] Sync invoke global-macro (with new FMP endpoint)…")
    out["global_macro"] = sync_invoke("justhodl-global-macro")

    # Read updated S3 output
    time.sleep(2)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/global-macro.json")
        data = json.loads(obj["Body"].read())
        countries_with_etf = sum(1 for c in (data.get("countries") or [])
                                  if c.get("equity_3m") and c["equity_3m"].get("score_0_100") is not None)
        out["global_macro_check"] = {
            "n_countries": data.get("n_countries"),
            "n_with_etf_data": countries_with_etf,
            "global_avg": data.get("global_avg_composite"),
            "global_regime": data.get("global_regime"),
            "rankings": data.get("rankings"),
            "etf_samples": [
                {
                    "code": c.get("code"),
                    "name": c.get("name"),
                    "etf": (c.get("equity_3m") or {}).get("ticker"),
                    "ret_3m": (c.get("equity_3m") or {}).get("return_pct"),
                    "n_components": c.get("n_components"),
                    "composite": c.get("composite_score"),
                    "regime": c.get("regime"),
                }
                for c in (data.get("countries") or [])[:8]
            ],
        }
    except Exception as e:
        out["global_macro_s3_err"] = str(e)[:200]

    # Verify morning-brief-tg has Phase E sections — sync invoke
    print("[332] Sync invoke morning-brief-tg (verify Phase E sections render)…")
    out["morning_brief"] = sync_invoke("justhodl-morning-brief-tg")

    # Read brief output to verify our sections are present
    time.sleep(2)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/morning-brief-latest.json")
        data = json.loads(obj["Body"].read())
        msg = data.get("message", "")
        out["brief_check"] = {
            "msg_length": len(msg),
            "has_fed_section": "🏦 Fed Speak" in msg,
            "has_global_section": "🌍 Global Macro" in msg,
            "fed_excerpt": "",
            "global_excerpt": "",
        }
        # Extract excerpts
        for marker, key in (("🏦 Fed Speak", "fed_excerpt"),
                              ("🌍 Global Macro", "global_excerpt")):
            idx = msg.find(marker)
            if idx >= 0:
                out["brief_check"][key] = msg[idx:idx+400]
    except Exception as e:
        out["brief_s3_err"] = str(e)[:200]

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:6000])


if __name__ == "__main__":
    main()
