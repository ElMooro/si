"""1102 — re-invoke detector with fix + confirm correct FRED series mapping."""
import json, pathlib, time, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1102_fix_verify.json"
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=180))
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Invoke
    print("[1102] invoking detector with corrected parse_term_days…")
    t0 = time.time()
    r = lam.invoke(FunctionName="justhodl-auction-crisis-detector",
                    InvocationType="RequestResponse", Payload=b"{}")
    out["invoke_elapsed_s"] = round(time.time() - t0, 1)
    out["invoke_status"]    = r.get("StatusCode")
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        if isinstance(p.get("body"), str):
            try:
                out["summary"] = json.loads(p["body"])
            except Exception:
                pass
    except Exception:
        out["raw"] = body[:300]
    
    # Read updated S3 file
    time.sleep(2)
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/auction-crisis.json")
    d = json.loads(obj["Body"].read())
    out["s3"] = {
        "schema":        d.get("schema_version"),
        "last_modified": obj["LastModified"].isoformat(),
        "size_kb":       round(obj["ContentLength"]/1024, 1),
    }
    
    # Inspect every forward calendar entry — verify series mapping is correct
    out["all_entries"] = []
    for f in (d.get("forward_calendar") or []):
        out["all_entries"].append({
            "auction_date":   f.get("auction_date"),
            "security_type":  f.get("security_type"),
            "security_term":  f.get("security_term"),
            "series":         f.get("concession_series"),
            "c5_bp":          f.get("concession_5d_bp"),
            "c1_bp":          f.get("concession_1d_bp"),
            "today_yield":    f.get("concession_today_yield"),
            "regime":         f.get("concession_regime"),
        })
    
    # Regime counts
    counts = {}
    for e in out["all_entries"]:
        r = e.get("regime") or "MISSING"
        counts[r] = counts.get(r, 0) + 1
    out["regime_counts"] = counts
    
    # Fetch live page + JS to confirm deploy
    print("[1102] fetching live page + JS…")
    try:
        with urllib.request.urlopen("https://justhodl.ai/auction-crisis.html", timeout=20) as r:
            html = r.read().decode("utf-8")
        out["html_size_kb"] = round(len(html)/1024, 1)
        out["html_has_concession_callout"] = "concession-callout" in html
        out["html_has_callout_section_hdr"] = "Pre-Auction Concession Signals" in html
        out["html_js_cache_buster"]        = "auction-crisis.js?t=20260601c" in html
    except Exception as e:
        out["html_err"] = str(e)[:150]
    
    try:
        with urllib.request.urlopen("https://justhodl.ai/auction-crisis.js?t=20260601c", timeout=20) as r:
            js = r.read().decode("utf-8")
        out["js_size_kb"] = round(len(js)/1024, 1)
        out["js_has_renderConcessionCallout"] = "function renderConcessionCallout" in js
    except Exception as e:
        out["js_err"] = str(e)[:150]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1102] DONE")


if __name__ == "__main__":
    main()
