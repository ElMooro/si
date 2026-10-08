"""1092 — Direct Lambda creation for justhodl-auction-crisis-ai.

The deploy-lambdas.yml workflow uses `aws lambda update-function-code`
which silently fails for brand-new Lambdas (workflow reports "success"
because the script never errored, but the create-function command was
never issued).

This ops script bypasses the workflow by building the zip manually
(bundling aws/shared/*.py + source/*.py exactly like the workflow does)
and calling boto3 create_function directly.

Then sets up the EventBridge Scheduler, invokes the new Lambda, and
verifies the auction-crisis-ai.json output.
"""
import io, json, os, pathlib, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT       = "aws/ops/reports/1092_ai_lambda_create.json"
REPO_ROOT    = pathlib.Path(".")
SHARED_DIR   = REPO_ROOT / "aws/shared"
LAMBDA_DIR   = REPO_ROOT / "aws/lambdas/justhodl-auction-crisis-ai"
SOURCE_DIR   = LAMBDA_DIR / "source"

lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=180))
ebs = boto3.client("scheduler", region_name="us-east-1")


def build_zip() -> bytes:
    """Two-layer zip: aws/shared/*.py defaults, source/* overrides."""
    buf = io.BytesIO()
    seen_files = set()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # Layer 1: source/ files (override anything)
        for f in SOURCE_DIR.iterdir():
            if f.is_file():
                zf.write(f, arcname=f.name)
                seen_files.add(f.name)
        # Layer 2: shared/ files (only if not already present from source)
        for f in SHARED_DIR.iterdir():
            if f.is_file() and f.suffix == ".py" and f.name not in seen_files:
                zf.write(f, arcname=f.name)
                seen_files.add(f.name)
    return buf.getvalue()


def get_api_key_from_buyback() -> str:
    """Inherit ANTHROPIC_API_KEY from justhodl-buyback-scanner env."""
    info = lam.get_function_configuration(FunctionName="justhodl-buyback-scanner")
    env = info.get("Environment", {}).get("Variables", {})
    key = env.get("ANTHROPIC_API_KEY")
    if not key:
        raise RuntimeError("ANTHROPIC_API_KEY not found in buyback-scanner env")
    return key


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    cfg_path = LAMBDA_DIR / "config.json"
    cfg = json.loads(cfg_path.read_text())
    fn_name = "justhodl-auction-crisis-ai"
    
    # 1. Check current state
    print(f"[1092] phase 1: check if {fn_name} exists…")
    try:
        info = lam.get_function(FunctionName=fn_name)
        out["already_existed"] = True
        out["last_modified_pre"] = info["Configuration"]["LastModified"]
        print(f"  ✓ Lambda already exists, will update")
    except lam.exceptions.ResourceNotFoundException:
        out["already_existed"] = False
        print(f"  ✗ Lambda does NOT exist — will create")
    
    # 2. Inherit Anthropic API key
    print("[1092] phase 2: inherit ANTHROPIC_API_KEY from buyback-scanner…")
    api_key = get_api_key_from_buyback()
    out["api_key_inherited"] = bool(api_key)
    out["api_key_prefix"]    = api_key[:8] + "..." if api_key else None
    
    # 3. Build the zip
    print("[1092] phase 3: build zip with shared/ + source/…")
    zip_bytes = build_zip()
    out["zip_size_kb"] = round(len(zip_bytes) / 1024, 1)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        out["zip_files"] = zf.namelist()
    
    # 4. Create or update the Lambda
    if not out["already_existed"]:
        print(f"[1092] phase 4: create_function {fn_name}…")
        resp = lam.create_function(
            FunctionName=fn_name,
            Runtime=cfg.get("runtime", "python3.12"),
            Role=cfg.get("role_arn", "arn:aws:iam::857687956942:role/lambda-execution-role"),
            Handler=cfg.get("handler", "lambda_function.lambda_handler"),
            Code={"ZipFile": zip_bytes},
            Timeout=cfg.get("timeout", 120),
            MemorySize=cfg.get("memory", 512),
            Description=cfg.get("description", "AI commentary on auction crisis"),
            Environment={
                "Variables": {
                    "ANTHROPIC_API_KEY": api_key,
                }
            },
            Publish=False,
        )
        out["create_response_arn"] = resp.get("FunctionArn")
        out["lambda_state"]        = resp.get("State")
    else:
        print(f"[1092] phase 4: update_function_code + config…")
        lam.update_function_code(FunctionName=fn_name, ZipFile=zip_bytes)
        # Wait for code update
        for _ in range(30):
            info = lam.get_function(FunctionName=fn_name)
            if info["Configuration"]["LastUpdateStatus"] == "Successful":
                break
            time.sleep(1)
        lam.update_function_configuration(
            FunctionName=fn_name,
            Timeout=cfg.get("timeout", 120),
            MemorySize=cfg.get("memory", 512),
            Description=cfg.get("description", "AI commentary"),
            Environment={"Variables": {"ANTHROPIC_API_KEY": api_key}},
        )
    
    # Wait for ACTIVE
    print("[1092] phase 5: wait for ACTIVE state…")
    for i in range(60):
        info = lam.get_function(FunctionName=fn_name)
        state = info["Configuration"]["State"]
        if state == "Active":
            break
        time.sleep(1)
    out["final_state"] = state
    out["function_arn"] = info["Configuration"]["FunctionArn"]
    
    # 5. Set up EventBridge Scheduler
    sched_cfg = cfg.get("eventbridge_scheduler", {})
    if sched_cfg:
        sched_name = sched_cfg["schedule_name"]
        print(f"[1092] phase 6: setup EventBridge Scheduler {sched_name}…")
        try:
            try:
                ebs.delete_schedule(Name=sched_name)
                time.sleep(1)
            except ebs.exceptions.ResourceNotFoundException:
                pass
            ebs.create_schedule(
                Name=sched_name,
                ScheduleExpression=sched_cfg["cron"],
                ScheduleExpressionTimezone=sched_cfg.get("timezone", "UTC"),
                Description=sched_cfg.get("description", ""),
                State="ENABLED",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={
                    "Arn":     out["function_arn"],
                    "RoleArn": sched_cfg["role_arn"],
                    "Input":   json.dumps({"source": "scheduler"}),
                },
            )
            out["schedule_created"] = sched_name
        except Exception as e:
            out["schedule_err"] = str(e)[:200]
    
    # 6. Invoke + verify
    print("[1092] phase 7: invoke Lambda (will call Claude — expect 15-30s)…")
    t0 = time.time()
    r = lam.invoke(FunctionName=fn_name,
                    InvocationType="RequestResponse",
                    Payload=b"{}")
    out["invoke_elapsed_s"] = round(time.time() - t0, 1)
    body = r["Payload"].read().decode("utf-8", errors="replace")
    out["invoke_status"] = r.get("StatusCode")
    try:
        p = json.loads(body)
        if isinstance(p.get("body"), str):
            try:
                inner = json.loads(p["body"])
                out["invoke_summary"] = inner
            except Exception:
                out["body_preview"] = p["body"][:300]
    except Exception:
        out["raw_invoke"] = body[:500]
    
    # 7. Read AI output
    print("[1092] phase 8: read auction-crisis-ai.json…")
    s3 = boto3.client("s3", region_name="us-east-1")
    obj = s3.get_object(Bucket="justhodl-dashboard-live",
                          Key="data/auction-crisis-ai.json")
    d = json.loads(obj["Body"].read())
    out["ai_status"]   = d.get("status", "ok")
    out["ai_size_kb"]  = round(obj["ContentLength"]/1024, 1)
    out["ai_model"]    = d.get("model")
    out["ai_regime"]   = d.get("regime")
    out["ai_composite"] = d.get("composite")
    out["claude_elapsed"] = d.get("claude_elapsed_sec")
    
    if d.get("status") == "error":
        out["ai_error"]      = d.get("error")
        out["raw_preview"]   = d.get("raw_response_preview", "")[:300]
    else:
        ai = d.get("ai_commentary") or {}
        out["sections"]                = list(ai.keys())
        out["executive_summary"]       = ai.get("executive_summary", "")
        out["what_changed"]            = ai.get("what_changed", "")
        out["historical_analog_discussion"] = ai.get("historical_analog_discussion", "")
        out["tail_risk_assessment"]    = ai.get("tail_risk_assessment", "")
        out["actionable_triggers"]     = ai.get("actionable_triggers", "")
        out["decisive_call"]           = ai.get("decisive_call", "")
        out["n_forward_predictions"]   = len(ai.get("forward_predictions") or [])
        out["n_indicator_interp"]      = len(ai.get("indicator_interpretation") or [])
        if ai.get("forward_predictions"):
            out["sample_forward"] = ai["forward_predictions"][0]
        if ai.get("indicator_interpretation"):
            out["sample_interp"]  = ai["indicator_interpretation"][0]
    
    # Save full output
    with open("aws/ops/reports/1092_ai_full.json", "w") as f:
        json.dump(d, f, indent=2, default=str)
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1092] DONE — Lambda state: {out.get('final_state')}, ai_status: {out.get('ai_status')}")


if __name__ == "__main__":
    main()
