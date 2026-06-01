"""1123 — catalyst classifier: schedule, invoke, show graded catalysts."""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1123_catalysts.json"
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=350))
s3  = boto3.client("s3", region_name="us-east-1")
ebs = boto3.client("scheduler", region_name="us-east-1")


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
        return r
    except Exception as e:
        out["phases"].append({"name": name, "status": "ERROR", "error": str(e)[:400],
                                "traceback": traceback.format_exc()[:1500]})
        return None


def setup_schedule(fn_name: str) -> dict:
    cfg = json.load(open(f"aws/lambdas/{fn_name}/config.json"))["eventbridge_scheduler"]
    name = cfg["schedule_name"]
    try:
        ebs.delete_schedule(Name=name); time.sleep(1)
    except ebs.exceptions.ResourceNotFoundException: pass
    ebs.create_schedule(
        Name=name, ScheduleExpression=cfg["cron"],
        ScheduleExpressionTimezone=cfg["timezone"],
        Description=cfg.get("description", ""), State="ENABLED",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={"Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{fn_name}",
                  "RoleArn": cfg["role_arn"],
                  "Input": json.dumps({"source": "scheduler"})})
    return {"schedule": name, "cron": cfg["cron"]}


def invoke(fn_name: str) -> dict:
    t0 = time.time()
    r = lam.invoke(FunctionName=fn_name, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        if isinstance(p.get("body"), str):
            try: return {"elapsed_s": elapsed, "summary": json.loads(p["body"])}
            except Exception: return {"elapsed_s": elapsed, "body": p["body"][:600]}
        return {"elapsed_s": elapsed, "p": str(p)[:600]}
    except Exception: return {"elapsed_s": elapsed, "raw": body[:600]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}

    # Confirm Lambda exists + env inherited
    def check():
        d = lam.get_function(FunctionName="justhodl-catalyst-classifier")["Configuration"]
        return {
            "state":     d["State"],
            "code_kb":   round(d["CodeSize"]/1024, 1),
            "has_anth":  bool((d.get("Environment", {}) or {}).get("Variables", {}).get("ANTHROPIC_API_KEY")),
        }
    phase(out, "check_lambda", check)

    phase(out, "schedule",  lambda: setup_schedule("justhodl-catalyst-classifier"))
    phase(out, "invoke",    lambda: invoke("justhodl-catalyst-classifier"))

    def read_catalysts():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/catalysts.json")
        d = json.loads(obj["Body"].read())
        pathlib.Path("aws/ops/reports/1123_catalysts_full.json").write_text(
            json.dumps(d, indent=2, default=str))
        if d.get("status") == "error":
            return {"status": "error", "error": d.get("error"), "preview": (d.get("raw_preview") or "")[:300]}
        return {
            "size_kb":       round(obj["ContentLength"]/1024, 1),
            "elapsed_sec":   d.get("elapsed_sec"),
            "claude_elapsed": d.get("claude_elapsed"),
            "n_classified":  d.get("n_classified"),
            "by_grade":      d.get("by_grade"),
            "by_type":       d.get("by_type"),
            "flagged":       d.get("flagged"),
            "catalysts":     d.get("catalysts", []),
        }
    phase(out, "read_catalysts", read_catalysts)

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1123] DONE")


if __name__ == "__main__":
    main()
