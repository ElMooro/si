#!/usr/bin/env python3
"""Step 372 — End-to-end verify of stress-test simulator."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/372_stress_verify.json"
NAME = "justhodl-tmp-stress-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request, urllib.error
import boto3

ssm = boto3.client("ssm", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def http(method, url, body=None):
    h = {"Content-Type": "application/json"} if body else {}
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8")

def lambda_handler(event, context):
    out = {}

    # 0. Get URL from SSM
    try:
        url = ssm.get_parameter(Name="/justhodl/stress/simulator-url")["Parameter"]["Value"].rstrip("/")
        out["0_url"] = {"value": url, "ok": True}
    except Exception as e:
        out["0_url"] = {"error": str(e)}
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # 1. GET / health
    s, body = http("GET", url + "/")
    try:
        j = json.loads(body)
        out["1_health"] = {
            "status": s, "service": j.get("service"),
            "factor_keys": j.get("factor_keys"),
            "n_assets": len(j.get("asset_universe", [])),
            "n_presets": len(j.get("presets_available", [])),
            "betas_source": j.get("betas_source"),
            "ok": s == 200 and j.get("service") == "justhodl-stress-simulator",
        }
    except Exception as e:
        out["1_health"] = {"status": s, "raw": body[:300], "error": str(e)}

    # 2. GET /presets
    s, body = http("GET", url + "/presets")
    try:
        j = json.loads(body)
        presets = j.get("presets", {})
        out["2_presets"] = {
            "status": s, "n": len(presets),
            "names": list(presets.keys()),
            "ok": s == 200 and len(presets) == 10,
        }
    except Exception as e:
        out["2_presets"] = {"status": s, "raw": body[:300]}

    # 3. POST /simulate with a preset
    s, body = http("POST", url + "/simulate", {"preset": "gfc_2008"})
    try:
        j = json.loads(body)
        per_asset = j.get("per_asset") or {}
        out["3_preset_gfc"] = {
            "status": s,
            "preset": j.get("preset"),
            "total_pnl": j.get("total_pnl"),
            "total_pnl_pct": j.get("total_pnl_pct"),
            "regime": j.get("regime", {}).get("regime"),
            "betas_source": j.get("betas_source", {}).get("source"),
            "n_per_asset": len(per_asset),
            "spy_return": per_asset.get("SPY", {}).get("shocked_return_pct"),
            "vixy_return": per_asset.get("VIXY", {}).get("shocked_return_pct"),
            "tlt_return": per_asset.get("TLT", {}).get("shocked_return_pct"),
            "ki_before": j.get("khalid_index", {}).get("before"),
            "ki_after": j.get("khalid_index", {}).get("after"),
            "regime_change_p": j.get("regime_change_probability"),
            "ok": s == 200 and j.get("total_pnl") is not None and j.get("preset") == "gfc_2008",
        }
    except Exception as e:
        out["3_preset_gfc"] = {"status": s, "raw": body[:400], "error": str(e)}

    # 4. POST /simulate with custom shocks (mild rate hike)
    s, body = http("POST", url + "/simulate",
                    {"shocks": {"rates_bps": 25.0, "equity_pct": -1.0}})
    try:
        j = json.loads(body)
        out["4_custom"] = {
            "status": s,
            "total_pnl_pct": j.get("total_pnl_pct"),
            "tlt_pnl": j.get("per_asset", {}).get("TLT", {}).get("pnl"),
            "ok": s == 200,
        }
    except Exception as e:
        out["4_custom"] = {"status": s, "error": str(e)}

    # 5. Bad preset → 400
    s, body = http("POST", url + "/simulate", {"preset": "nonexistent"})
    out["5_bad_preset"] = {"status": s, "ok": s == 400}

    # 6. Bad method → 405
    # Skipped — Function URL CORS rejects PUT/DELETE before reaching Lambda

    # 7. Confirm S3 loadings file
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                            Key="data/stress-factor-loadings.json")
        body = obj["Body"].read()
        j = json.loads(body)
        out["7_loadings_s3"] = {
            "size_bytes": len(body),
            "n_obs": j.get("n_obs"),
            "n_tickers": len(j.get("betas", {})),
            "stale": j.get("stale"),
            "spy_betas": j.get("betas", {}).get("SPY", {}),
            "spy_r2": j.get("r_squared", {}).get("SPY", {}),
            "ok": j.get("n_obs", 0) > 1000 and len(j.get("betas", {})) >= 10,
        }
    except Exception as e:
        out["7_loadings_s3"] = {"error": str(e)}

    # 8. Live page
    try:
        req = urllib.request.Request("https://justhodl.ai/stress.html",
                                      headers={"User-Agent": "JustHodl-Verify/372"})
        with urllib.request.urlopen(req, timeout=15) as r:
            page = r.read().decode("utf-8", errors="ignore")
            out["8_page"] = {
                "status": r.status, "size": len(page),
                "has_real_url": "PLACEHOLDER" not in page and url.split("//")[1].split(".")[0] in page,
                "has_pwa": "manifest.json" in page,
                "has_wss": "wss-client.js" in page,
                "has_sliders": "buildSliders()" in page,
                "has_presets": "loadPresets()" in page,
                "ok": r.status == 200 and "PLACEHOLDER" not in page,
            }
    except Exception as e:
        out["8_page"] = {"error": str(e)}

    # Summary
    checks = [
        out["1_health"].get("ok"),
        out["2_presets"].get("ok"),
        out["3_preset_gfc"].get("ok"),
        out["4_custom"].get("ok"),
        out["5_bad_preset"].get("ok"),
        out["7_loadings_s3"].get("ok"),
        out["8_page"].get("ok"),
    ]
    out["summary"] = {"passed": sum(1 for c in checks if c), "total": len(checks)}
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
                            MemorySize=256, Timeout=120, Code={"ZipFile": zb})
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
