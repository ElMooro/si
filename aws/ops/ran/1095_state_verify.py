"""1095 — verify current AI Lambda state. ops/1094 script succeeded but report commit failed."""
import json, pathlib
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1095_state_verify.json"
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=180))
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Check Lambda exists + has key
    try:
        info = lam.get_function(FunctionName="justhodl-auction-crisis-ai")
        cfg = info["Configuration"]
        out["lambda"] = {
            "exists":       True,
            "state":        cfg.get("State"),
            "last_modified": cfg.get("LastModified"),
            "has_anthropic_key": bool(cfg.get("Environment", {}).get("Variables", {}).get("ANTHROPIC_API_KEY")),
            "env_keys":     list(cfg.get("Environment", {}).get("Variables", {}).keys()),
        }
    except Exception as e:
        out["lambda"] = {"err": str(e)[:200]}
    
    # Read AI output from S3
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/auction-crisis-ai.json")
        d = json.loads(obj["Body"].read())
        out["ai"] = {
            "size_kb":         round(obj["ContentLength"]/1024, 1),
            "last_modified":   obj["LastModified"].isoformat(),
            "status":          d.get("status", "ok"),
            "model":           d.get("model"),
            "regime":          d.get("regime"),
            "composite":       d.get("composite"),
            "claude_elapsed":  d.get("claude_elapsed_sec"),
            "data_age_min":    d.get("data_age_minutes"),
        }
        
        if d.get("status") == "error":
            out["ai"]["error"]        = d.get("error")
            out["ai"]["raw_preview"]  = d.get("raw_response_preview", "")[:200]
        else:
            ai = d.get("ai_commentary") or {}
            out["ai_commentary"] = {
                "sections":              list(ai.keys()),
                "executive_summary":     ai.get("executive_summary", ""),
                "what_changed":          ai.get("what_changed", ""),
                "historical_analog_discussion": ai.get("historical_analog_discussion", ""),
                "tail_risk_assessment":  ai.get("tail_risk_assessment", ""),
                "actionable_triggers":   ai.get("actionable_triggers", ""),
                "decisive_call":         ai.get("decisive_call", ""),
                "n_forward_predictions": len(ai.get("forward_predictions") or []),
                "n_indicator_interp":    len(ai.get("indicator_interpretation") or []),
                "sample_forward":        (ai.get("forward_predictions") or [None])[0],
                "sample_interp":         (ai.get("indicator_interpretation") or [None])[0],
            }
            
            with open("aws/ops/reports/1095_ai_full.json", "w") as f:
                json.dump(d, f, indent=2, default=str)
    except Exception as e:
        out["ai"] = {"err": str(e)[:200]}
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1095] DONE")


if __name__ == "__main__":
    main()
