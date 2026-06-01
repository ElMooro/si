"""1091 — invoke AI Lambda + verify Claude narrative."""
import json, pathlib, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1091_ai_verify.json"
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=180))
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Check that the new Lambda exists
    try:
        info = lam.get_function(FunctionName="justhodl-auction-crisis-ai")
        out["lambda_exists"] = True
        out["last_modified"] = info["Configuration"]["LastModified"]
        out["env"] = list(info["Configuration"].get("Environment", {}).get("Variables", {}).keys())
    except Exception as e:
        out["lambda_exists"] = False
        out["err"] = str(e)[:200]
        pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
        return
    
    # Invoke
    print("[1091] invoking AI Lambda (will call Claude — expect 15-30s)…")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName="justhodl-auction-crisis-ai",
                        InvocationType="RequestResponse", Payload=b"{}")
        out["elapsed_s"] = round(time.time() - t0, 1)
        body = r["Payload"].read().decode("utf-8", errors="replace")
        out["raw"] = body[:500]
        try:
            p = json.loads(body)
            out["invoke_status"] = p.get("statusCode")
            if isinstance(p.get("body"), str):
                inner = json.loads(p["body"])
                out["summary"] = inner
        except Exception:
            pass
    except Exception as e:
        out["invoke_err"] = str(e)[:300]
    
    # Read the S3 output
    time.sleep(2)
    print("[1091] reading auction-crisis-ai.json…")
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/auction-crisis-ai.json")
        d = json.loads(obj["Body"].read())
        out["s3_size_kb"]      = round(obj["ContentLength"]/1024, 1)
        out["s3_last_modified"] = obj["LastModified"].isoformat()
        out["status"]          = d.get("status", "ok")
        out["model"]           = d.get("model")
        out["regime"]          = d.get("regime")
        out["composite"]       = d.get("composite")
        out["data_age_min"]    = d.get("data_age_minutes")
        out["claude_elapsed"]  = d.get("claude_elapsed_sec")
        
        if d.get("status") == "error":
            out["error_msg"]       = d.get("error")
            out["raw_preview"]     = d.get("raw_response_preview", "")[:500]
        else:
            ai = d.get("ai_commentary", {})
            out["commentary_sections"] = list(ai.keys())
            # Show the actual commentary content
            out["executive_summary"] = ai.get("executive_summary", "")
            out["what_changed"]      = ai.get("what_changed", "")
            out["historical_analog"] = ai.get("historical_analog_discussion", "")
            out["tail_risk"]         = ai.get("tail_risk_assessment", "")
            out["triggers_narrative"] = ai.get("actionable_triggers", "")
            out["decisive_call"]     = ai.get("decisive_call", "")
            out["n_forward_predictions"] = len(ai.get("forward_predictions") or [])
            out["n_indicator_interpretations"] = len(ai.get("indicator_interpretation") or [])
            
            # Show one forward prediction sample
            fps = ai.get("forward_predictions") or []
            if fps:
                out["sample_forward_prediction"] = fps[0]
            ips = ai.get("indicator_interpretation") or []
            if ips:
                out["sample_indicator_interp"] = ips[0]
        
        # Save full output for reference
        with open("aws/ops/reports/1091_ai_full.json", "w") as f:
            json.dump(d, f, indent=2, default=str)
    except Exception as e:
        out["s3_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1091] DONE")


if __name__ == "__main__":
    main()
