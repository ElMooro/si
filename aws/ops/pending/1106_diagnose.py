"""1106 — diagnose 1105 failure with per-phase error capture."""
import io, json, pathlib, time, traceback, urllib.request, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1106_diagnose.json"


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
        return r
    except Exception as e:
        out["phases"].append({
            "name":      name,
            "status":    "ERROR",
            "error":     str(e)[:300],
            "traceback": traceback.format_exc()[:1500],
        })
        return None


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}
    lam = boto3.client("lambda", region_name="us-east-1",
                        config=Config(read_timeout=300))
    s3 = boto3.client("s3", region_name="us-east-1")
    fn = "justhodl-ai-website-synthesis"
    
    def check_lambda():
        try:
            info = lam.get_function(FunctionName=fn)
            return {
                "exists":         True,
                "state":          info["Configuration"]["State"],
                "last_modified":  info["Configuration"]["LastModified"],
            }
        except lam.exceptions.ResourceNotFoundException:
            return {"exists": False}
    state = phase(out, "check_existing", check_lambda)
    
    if not state or not state.get("exists"):
        # Create directly
        def create():
            cfg = json.load(open(f"aws/lambdas/{fn}/config.json"))
            # Get API key
            src = lam.get_function_configuration(FunctionName="justhodl-ai-chat")
            api_key = src["Environment"]["Variables"]["ANTHROPIC_API_KEY"]
            # Build zip
            buf = io.BytesIO()
            seen = set()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                src_dir = pathlib.Path(f"aws/lambdas/{fn}/source")
                for f in sorted(src_dir.iterdir()):
                    if f.is_file():
                        zf.write(f, arcname=f.name); seen.add(f.name)
                sh_dir = pathlib.Path("aws/shared")
                if sh_dir.is_dir():
                    for f in sorted(sh_dir.iterdir()):
                        if f.is_file() and f.suffix == ".py" and f.name not in seen:
                            zf.write(f, arcname=f.name); seen.add(f.name)
            # Create
            resp = lam.create_function(
                FunctionName=fn,
                Runtime=cfg["runtime"],
                Role=cfg["role_arn"],
                Handler=cfg["handler"],
                Code={"ZipFile": buf.getvalue()},
                Timeout=cfg["timeout"],
                MemorySize=cfg["memory"],
                Description=cfg["description"],
                Environment={"Variables": {"ANTHROPIC_API_KEY": api_key}},
            )
            # Wait for Active
            for _ in range(45):
                info = lam.get_function(FunctionName=fn)
                if info["Configuration"]["State"] == "Active":
                    break
                time.sleep(1)
            return {"arn": resp["FunctionArn"], "state": "Active"}
        phase(out, "create_lambda", create)
        
        # Setup schedule
        def setup_schedule():
            ebs = boto3.client("scheduler", region_name="us-east-1")
            cfg = json.load(open(f"aws/lambdas/{fn}/config.json"))["eventbridge_scheduler"]
            try:
                ebs.delete_schedule(Name=cfg["schedule_name"]); time.sleep(1)
            except ebs.exceptions.ResourceNotFoundException:
                pass
            ebs.create_schedule(
                Name=cfg["schedule_name"],
                ScheduleExpression=cfg["cron"],
                ScheduleExpressionTimezone=cfg["timezone"],
                Description=cfg.get("description", ""),
                State="ENABLED",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={
                    "Arn":     f"arn:aws:lambda:us-east-1:857687956942:function:{fn}",
                    "RoleArn": cfg["role_arn"],
                    "Input":   json.dumps({"source": "scheduler"}),
                },
            )
            return {"created": True}
        phase(out, "setup_schedule", setup_schedule)
    
    # Invoke
    def invoke():
        t0 = time.time()
        r = lam.invoke(FunctionName=fn,
                        InvocationType="RequestResponse", Payload=b"{}")
        elapsed = round(time.time() - t0, 1)
        body = r["Payload"].read().decode("utf-8", errors="replace")
        try:
            p = json.loads(body)
            if isinstance(p.get("body"), str):
                try:
                    return {"elapsed": elapsed, "summary": json.loads(p["body"])}
                except Exception:
                    return {"elapsed": elapsed, "body": p["body"][:300]}
            return {"elapsed": elapsed, "raw": str(p)[:300]}
        except Exception:
            return {"elapsed": elapsed, "raw_body": body[:300]}
    phase(out, "invoke", invoke)
    
    # Read output
    def read_output():
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/ai-website-synthesis.json")
        d = json.loads(obj["Body"].read())
        return {
            "size_kb":        round(obj["ContentLength"]/1024, 1),
            "last_modified":  obj["LastModified"].isoformat(),
            "status":         d.get("status", "ok"),
            "model":          d.get("model"),
            "global_posture": (d.get("synthesis") or {}).get("global_posture"),
            "headline":       ((d.get("synthesis") or {}).get("headline") or "")[:200],
            "decisive_call":  ((d.get("synthesis") or {}).get("decisive_call") or "")[:200],
            "engines_loaded": d.get("engines_loaded"),
        }
    phase(out, "read_output", read_output)
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1106] DONE")


if __name__ == "__main__":
    main()
