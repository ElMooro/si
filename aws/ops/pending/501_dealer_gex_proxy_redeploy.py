#!/usr/bin/env python3
"""Step 501 — Redeploy dealer-gex with yahoo-proxy.justhodl.ai, verify chains arrive."""
import io, json, os, time as _time, zipfile, base64, urllib.request
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/501_dealer_gex_proxy_redeploy.json"
SOURCE = "aws/lambdas/justhodl-dealer-gex/source/lambda_function.py"
NAME = "justhodl-dealer-gex"
lam = boto3.client("lambda", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def zip_source(path):
    with open(path, "rb") as f: code = f.read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def health_check_proxy():
    """Verify yahoo-proxy.justhodl.ai is live from AWS network (different IP than sandbox)."""
    try:
        token = ssm.get_parameter(Name="/justhodl/ai-chat/auth-token",
                                    WithDecryption=True)["Parameter"]["Value"]
    except Exception as e:
        return {"err": f"ssm: {str(e)[:200]}"}
    headers = {"x-justhodl-token": token, "User-Agent": "JustHodl-OpsHealth/1.0"}
    result = {}
    # 1. health endpoint
    try:
        req = urllib.request.Request("https://yahoo-proxy.justhodl.ai/health", headers=headers)
        with urllib.request.urlopen(req, timeout=10) as r:
            result["health"] = {"status": r.status, "body": r.read().decode("utf-8")[:500]}
    except Exception as e:
        result["health_err"] = str(e)[:300]
    # 2. SPY options chain via proxy
    try:
        req = urllib.request.Request("https://yahoo-proxy.justhodl.ai/options/SPY", headers=headers)
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8")
            data = json.loads(body)
            result_arr = ((data.get("optionChain") or {}).get("result") or [])
            if result_arr:
                cd = result_arr[0]
                ops = cd.get("options") or [{}]
                first = ops[0] if ops else {}
                result["spy_chain"] = {
                    "status": r.status,
                    "spot": (cd.get("quote") or {}).get("regularMarketPrice"),
                    "n_expirations": len(cd.get("expirationDates") or []),
                    "first_expiry_calls": len(first.get("calls") or []),
                    "first_expiry_puts": len(first.get("puts") or []),
                }
            else:
                result["spy_chain_err"] = f"empty response: {body[:300]}"
    except Exception as e:
        result["spy_chain_err"] = str(e)[:300]
    return result


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ─── 1. Health-check proxy from sandbox (will likely fail due to IP block) ───
    # Actually skip this — we'll let the Lambda do its own health check.

    # ─── 2. Deploy Lambda v1.1.0 ───
    zb = zip_source(SOURCE)
    try:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        out["deploy"] = "ok"
    except Exception as e:
        out["deploy_err"] = str(e)[:300]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)
        return

    # ─── 3. Invoke from Lambda environment (verifies proxy from AWS network) ───
    _time.sleep(3)
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            parsed = json.loads(body)
            out["invoke_response"] = json.loads(parsed["body"]) if parsed.get("body") else parsed
        except: out["invoke_raw"] = body[:1500]
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-3500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:400]

    # ─── 4. Read sidecar ───
    try:
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/dealer-gex.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "market_composite": p.get("market_composite"),
            "n_squeeze_candidates": len(p.get("squeeze_candidates") or []),
            "squeeze_top_3": (p.get("squeeze_candidates") or [])[:3],
            "underlyings_summary": {
                sym: ({
                    "spot": r.get("spot"),
                    "gex_b": r.get("total_dealer_gex_billions"),
                    "flip": r.get("zero_gamma_flip_level"),
                    "pct_to_flip": r.get("spot_pct_to_flip"),
                    "regime": r.get("regime"),
                    "n_contracts": r.get("n_contracts_modeled"),
                    "pcr_oi": r.get("pcr_oi"),
                    "max_pain_next": list((r.get("max_pain_by_expiry") or {}).items())[:1],
                } if not r.get("err") else {"err": r["err"]})
                for sym, r in (p.get("underlyings") or {}).items()
            },
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
