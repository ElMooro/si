"""1115 — re-invoke deep-research after max_tokens fix."""
import json, pathlib, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1115_final.json"
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=300))
s3 = boto3.client("s3", region_name="us-east-1")

def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    print("[1115] invoke after fix…")
    t0 = time.time()
    r = lam.invoke(FunctionName="justhodl-ticker-deep-research",
                    InvocationType="RequestResponse", Payload=b"{}")
    out["elapsed_s"] = round(time.time() - t0, 1)
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        if isinstance(p.get("body"), str):
            try: out["summary"] = json.loads(p["body"])
            except: out["body"] = p["body"][:400]
    except: out["raw"] = body[:400]
    
    # Read full bundle
    time.sleep(2)
    obj = s3.get_object(Bucket="justhodl-dashboard-live",
                          Key="data/ticker-research-bundle.json")
    d = json.loads(obj["Body"].read())
    
    out["bundle"] = {
        "size_kb":       round(obj["ContentLength"]/1024, 1),
        "last_modified": obj["LastModified"].isoformat(),
        "schema":        d.get("schema_version"),
        "elapsed_sec":   d.get("elapsed_sec"),
        "claude_elapsed": d.get("claude_elapsed"),
        "n_tickers":     d.get("n_tickers"),
        "tickers":       d.get("tickers"),
        "status":        d.get("status", "ok"),
    }
    if d.get("status") == "error":
        out["bundle"]["error"] = d.get("error")
        out["bundle"]["preview"] = d.get("raw_preview", "")[:600]
    
    # Pull 3 sample dossiers
    research = d.get("research", {})
    out["samples"] = []
    for tk in (d.get("tickers") or [])[:3]:
        if tk in research:
            r = research[tk]
            out["samples"].append({
                "ticker":              tk,
                "convergence_summary": r.get("convergence_summary"),
                "bull_headline":       (r.get("bull_thesis") or {}).get("headline"),
                "bull_thesis":         (r.get("bull_thesis") or {}).get("thesis"),
                "bull_catalysts":      (r.get("bull_thesis") or {}).get("key_catalysts"),
                "bull_leading_signals": (r.get("bull_thesis") or {}).get("leading_signals"),
                "risk_headline":       (r.get("risk_assessment") or {}).get("headline"),
                "risk_primary":        (r.get("risk_assessment") or {}).get("primary_risks"),
                "risk_valuation":      (r.get("risk_assessment") or {}).get("valuation_concern"),
                "risk_breaks_thesis":  (r.get("risk_assessment") or {}).get("what_breaks_thesis"),
                "trade_framework":     r.get("trade_framework"),
                "one_liner":           r.get("ai_one_liner"),
            })
    
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1115] DONE")

if __name__ == "__main__":
    main()
