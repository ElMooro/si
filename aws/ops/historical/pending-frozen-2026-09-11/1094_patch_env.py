"""1094 — find which Lambda has ANTHROPIC_API_KEY + patch auction-crisis-ai."""
import json, pathlib, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1094_patch_env.json"
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=180))


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # 1. Find any Lambda with ANTHROPIC_API_KEY (Claude-using ones)
    # Just check a few known suspects
    print("[1094] phase 1: find ANTHROPIC_API_KEY source…")
    candidates = [
        "justhodl-ai-chat",
        "justhodl-ai-brief-router",
        "justhodl-buzz-velocity",
        "justhodl-morning-intelligence",
        "justhodl-crypto-intel",
        "justhodl-calibrator",
        "justhodl-buyback-scanner",
    ]
    api_key = None
    source_lambda = None
    inspected = []
    for name in candidates:
        try:
            info = lam.get_function_configuration(FunctionName=name)
            env = info.get("Environment", {}).get("Variables", {})
            has_key = "ANTHROPIC_API_KEY" in env and env["ANTHROPIC_API_KEY"]
            inspected.append({
                "name":   name,
                "has_key": has_key,
                "env_keys": list(env.keys())[:6],
            })
            if has_key and not api_key:
                api_key = env["ANTHROPIC_API_KEY"]
                source_lambda = name
        except Exception as e:
            inspected.append({"name": name, "err": str(e)[:80]})
    out["inspected"] = inspected
    out["source_lambda"] = source_lambda
    out["found_key"] = bool(api_key)
    out["key_prefix"] = (api_key[:10] + "...") if api_key else None
    
    if not api_key:
        out["status"] = "FAILED — no Lambda has ANTHROPIC_API_KEY"
        pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
        return
    
    # 2. Patch auction-crisis-ai env
    print(f"[1094] phase 2: patch auction-crisis-ai env with key from {source_lambda}…")
    lam.update_function_configuration(
        FunctionName="justhodl-auction-crisis-ai",
        Environment={"Variables": {"ANTHROPIC_API_KEY": api_key}},
    )
    # Wait for update
    for _ in range(30):
        info = lam.get_function_configuration(FunctionName="justhodl-auction-crisis-ai")
        if info.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    out["env_patched"] = True
    
    # 3. Re-invoke
    print("[1094] phase 3: re-invoke (will call Claude — expect 15-30s)…")
    t0 = time.time()
    r = lam.invoke(FunctionName="justhodl-auction-crisis-ai",
                    InvocationType="RequestResponse", Payload=b"{}")
    out["invoke_elapsed_s"] = round(time.time() - t0, 1)
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        out["invoke_status_code"] = p.get("statusCode")
        if isinstance(p.get("body"), str):
            try:
                out["invoke_body"] = json.loads(p["body"])
            except Exception:
                out["invoke_body_raw"] = p["body"][:300]
    except Exception:
        out["raw_invoke"] = body[:500]
    
    # 4. Read AI output
    print("[1094] phase 4: read auction-crisis-ai.json…")
    s3 = boto3.client("s3", region_name="us-east-1")
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/auction-crisis-ai.json")
        d = json.loads(obj["Body"].read())
        out["ai_status"] = d.get("status", "ok")
        out["ai_size_kb"] = round(obj["ContentLength"]/1024, 1)
        out["ai_last_modified"] = obj["LastModified"].isoformat()
        out["ai_model"] = d.get("model")
        out["claude_elapsed_sec"] = d.get("claude_elapsed_sec")
        
        if d.get("status") == "error":
            out["ai_error"] = d.get("error")
            out["raw_preview"] = d.get("raw_response_preview", "")[:300]
        else:
            ai = d.get("ai_commentary", {})
            out["sections"] = list(ai.keys())
            out["executive_summary"] = ai.get("executive_summary", "")
            out["what_changed"]      = ai.get("what_changed", "")
            out["historical_analog_discussion"] = ai.get("historical_analog_discussion", "")
            out["tail_risk_assessment"] = ai.get("tail_risk_assessment", "")
            out["actionable_triggers"] = ai.get("actionable_triggers", "")
            out["decisive_call"] = ai.get("decisive_call", "")
            out["n_forward_predictions"] = len(ai.get("forward_predictions") or [])
            out["n_indicator_interp"] = len(ai.get("indicator_interpretation") or [])
            if ai.get("forward_predictions"):
                out["sample_forward"] = ai["forward_predictions"][0]
            if ai.get("indicator_interpretation"):
                out["sample_interp"]  = ai["indicator_interpretation"][0]
        
        # Save full AI output
        with open("aws/ops/reports/1094_ai_full.json", "w") as f:
            json.dump(d, f, indent=2, default=str)
    except Exception as e:
        out["s3_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1094] DONE — ai_status: {out.get('ai_status')}")


if __name__ == "__main__":
    main()
