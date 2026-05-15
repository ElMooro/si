#!/usr/bin/env python3
"""590 — Verify margin-lending unit/Wilshire-fallback fix AND intelligence
page is live on GitHub Pages (longer wait since 589 ran too fast)."""
import io, json, os, time as _time, base64, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/590_margin_intelligence_final.json"
REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ── PART 1: Margin lending fix verification ──────────────────────
    NAME = "justhodl-margin-lending"

    # Wait for deploy
    pre_mod = None
    try:
        cfg = lam.get_function_configuration(FunctionName=NAME)
        pre_mod = cfg.get("LastModified")
    except Exception: pass

    for i in range(40):
        try:
            cfg = lam.get_function_configuration(FunctionName=NAME)
            mod = cfg.get("LastModified")
            if mod != pre_mod and cfg.get("State")=="Active" and cfg.get("LastUpdateStatus")=="Successful":
                out["margin_new_modified"] = mod
                break
        except Exception: pass
        _time.sleep(8)
    _time.sleep(3)

    # Force invoke
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["margin_invoke_status"] = resp.get("StatusCode")
        out["margin_fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["margin_response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except: out["margin_raw"] = body[:300]
        if resp.get("LogResult"):
            log = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")
            out["margin_log_tail"] = log[-1500:]
    except Exception as e:
        out["margin_invoke_err"] = str(e)[:200]

    # Read sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/margin-lending.json")
        out["margin_sidecar"] = json.loads(obj["Body"].read())
    except Exception as e:
        out["margin_sidecar_err"] = str(e)[:200]

    # ── PART 2: Intelligence page on GH Pages ───────────────────────
    page_url = "https://justhodl.ai/intelligence/"
    try:
        req = urllib.request.Request(page_url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", "replace")
            out["intel_page_status"] = r.status
            out["intel_page_size_kb"] = round(len(html)/1024, 1)
            markers = [
                "🧠 INTELLIGENCE", "Adaptive Khalid", "Stress Scenarios",
                "Political Trades", "Reversal Radar", "Auction Grades",
                "Repo &amp; Lending", "data/khalid-adaptive.json",
                "data/stress-scenarios.json", "data/political-trades.json",
                "data/reversal-radar.json", "data/auction-grades.json",
                "data/repo-lending.json",
            ]
            mf = {m: (m in html) for m in markers}
            out["intel_markers_found"] = mf
            out["intel_all_pass"] = all(mf.values())
    except urllib.error.HTTPError as e:
        out["intel_err"] = f"HTTP {e.code} {e.reason}"
    except Exception as e:
        out["intel_err"] = str(e)[:200]

    try:
        req = urllib.request.Request("https://justhodl.ai/", headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            home = r.read().decode("utf-8", "replace")
            out["home_has_intel_link"] = "/intelligence/" in home and "🧠 INTELLIGENCE" in home
    except Exception as e:
        out["home_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
