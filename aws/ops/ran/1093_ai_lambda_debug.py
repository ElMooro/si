"""1093 — debug ops 1092 failure + retry Lambda creation with detailed error capture."""
import io, json, os, pathlib, time, traceback, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1093_debug.json"

def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}
    
    def phase(name, fn):
        try:
            result = fn()
            out["phases"].append({"name": name, "status": "ok", "result": result})
            return result
        except Exception as e:
            out["phases"].append({
                "name": name,
                "status": "ERROR",
                "error": str(e)[:300],
                "traceback": traceback.format_exc()[:1500],
            })
            return None
    
    lam = boto3.client("lambda", region_name="us-east-1",
                         config=Config(read_timeout=180))
    
    # Phase 1: check current Lambda state
    def check_existing():
        try:
            info = lam.get_function(FunctionName="justhodl-auction-crisis-ai")
            return {"exists": True, "last_modified": info["Configuration"]["LastModified"],
                     "state": info["Configuration"]["State"]}
        except lam.exceptions.ResourceNotFoundException:
            return {"exists": False}
    phase("check_existing", check_existing)
    
    # Phase 2: get API key
    def get_api_key():
        info = lam.get_function_configuration(FunctionName="justhodl-buyback-scanner")
        env = info.get("Environment", {}).get("Variables", {})
        key = env.get("ANTHROPIC_API_KEY", "")
        return {"has_key": bool(key), "prefix": key[:8] + "..." if key else None}
    api_key_result = phase("get_api_key", get_api_key)
    
    # Phase 3: build zip
    def build_zip():
        buf = io.BytesIO()
        seen = set()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            srcdir = pathlib.Path("aws/lambdas/justhodl-auction-crisis-ai/source")
            for f in sorted(srcdir.iterdir()):
                if f.is_file():
                    zf.write(f, arcname=f.name); seen.add(f.name)
            shdir = pathlib.Path("aws/shared")
            for f in sorted(shdir.iterdir()):
                if f.is_file() and f.suffix == ".py" and f.name not in seen:
                    zf.write(f, arcname=f.name); seen.add(f.name)
        return {"size_kb": round(len(buf.getvalue())/1024, 1),
                 "files": sorted(seen), "_bytes": buf.getvalue()}
    zip_result = phase("build_zip", build_zip)
    
    # Phase 4: get API key value
    info = lam.get_function_configuration(FunctionName="justhodl-buyback-scanner")
    api_key = info.get("Environment", {}).get("Variables", {}).get("ANTHROPIC_API_KEY", "")
    
    # Phase 5: create the Lambda
    def create_lambda():
        if not zip_result or not zip_result.get("_bytes"):
            return {"skipped": "no zip"}
        resp = lam.create_function(
            FunctionName="justhodl-auction-crisis-ai",
            Runtime="python3.12",
            Role="arn:aws:iam::857687956942:role/lambda-execution-role",
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_result["_bytes"]},
            Timeout=120, MemorySize=512,
            Description="AI commentary on auction crisis (ops/1093)",
            Environment={"Variables": {"ANTHROPIC_API_KEY": api_key}},
        )
        return {"arn": resp.get("FunctionArn"), "state": resp.get("State")}
    create_result = phase("create_lambda", create_lambda)
    
    # Phase 6: wait for ACTIVE
    def wait_active():
        for i in range(60):
            info = lam.get_function(FunctionName="justhodl-auction-crisis-ai")
            state = info["Configuration"]["State"]
            if state == "Active":
                return {"state": state, "waited_s": i}
            time.sleep(1)
        return {"state": "TIMEOUT"}
    phase("wait_active", wait_active)
    
    # Phase 7: invoke
    def invoke_it():
        t0 = time.time()
        r = lam.invoke(FunctionName="justhodl-auction-crisis-ai",
                        InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        elapsed = round(time.time() - t0, 1)
        try:
            p = json.loads(body)
            return {"elapsed_s": elapsed, "status": p.get("statusCode"),
                     "body_preview": (p.get("body") or "")[:300]}
        except Exception:
            return {"elapsed_s": elapsed, "raw": body[:500]}
    phase("invoke", invoke_it)
    
    # Phase 8: setup EventBridge Scheduler
    def setup_schedule():
        ebs = boto3.client("scheduler", region_name="us-east-1")
        try:
            ebs.delete_schedule(Name="justhodl-auction-crisis-ai-hourly")
            time.sleep(1)
        except ebs.exceptions.ResourceNotFoundException:
            pass
        ebs.create_schedule(
            Name="justhodl-auction-crisis-ai-hourly",
            ScheduleExpression="cron(10 * * * ? *)",
            ScheduleExpressionTimezone="UTC",
            Description="AI commentary on auction crisis, hourly at 10min past",
            State="ENABLED",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn":     f"arn:aws:lambda:us-east-1:857687956942:function:justhodl-auction-crisis-ai",
                "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role",
                "Input":   json.dumps({"source": "scheduler"}),
            },
        )
        return {"created": True}
    phase("schedule", setup_schedule)
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    # Strip bytes from output
    for p in out["phases"]:
        if isinstance(p.get("result"), dict):
            p["result"].pop("_bytes", None)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1093] DONE")

if __name__ == "__main__":
    main()
