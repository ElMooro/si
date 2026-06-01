"""1124 — catalyst clusters: schedule, invoke, show actions."""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1124_clusters.json"
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
            except Exception: return {"elapsed_s": elapsed, "body": p["body"][:800]}
        return {"elapsed_s": elapsed, "p": str(p)[:800]}
    except Exception: return {"elapsed_s": elapsed, "raw": body[:800]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}

    phase(out, "schedule", lambda: setup_schedule("justhodl-catalyst-clusters"))
    phase(out, "invoke",   lambda: invoke("justhodl-catalyst-clusters"))

    def read():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/catalyst-clusters.json")
        d = json.loads(obj["Body"].read())
        pathlib.Path("aws/ops/reports/1124_clusters_full.json").write_text(
            json.dumps(d, indent=2, default=str))
        if d.get("status") == "error":
            return {"status": "error", "error": d.get("error")}
        return {
            "size_kb":     round(obj["ContentLength"]/1024, 1),
            "macro_regime": d.get("macro_regime"),
            "n_clusters":  d.get("n_clusters"),
            "n_temporal":  d.get("n_temporal"),
            "n_thematic":  d.get("n_thematic"),
            "clusters":    d.get("clusters"),
            "basket_action_summary": d.get("basket_action_summary"),
            "current_sizes":      d.get("current_sizes"),
            "proposed_new_sizes": d.get("proposed_new_sizes"),
            "size_deltas":        d.get("size_deltas"),
        }
    phase(out, "read_clusters", read)

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1124] DONE")


if __name__ == "__main__":
    main()
