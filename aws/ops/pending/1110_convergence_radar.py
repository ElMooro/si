"""1110 — set up EventBridge schedule, invoke convergence-radar, verify output."""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1110_convergence_radar.json"
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=300))
s3 = boto3.client("s3", region_name="us-east-1")
ebs = boto3.client("scheduler", region_name="us-east-1")


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
            "traceback": traceback.format_exc()[:1200],
        })
        return None


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}
    fn = "justhodl-convergence-radar"

    # Phase 1: confirm Lambda exists
    def check():
        info = lam.get_function(FunctionName=fn)
        return {
            "state":         info["Configuration"]["State"],
            "last_modified": info["Configuration"]["LastModified"],
            "code_size_kb":  round(info["Configuration"]["CodeSize"]/1024, 1),
            "memory":        info["Configuration"]["MemorySize"],
            "timeout":       info["Configuration"]["Timeout"],
        }
    state = phase(out, "check_lambda", check)
    if not state:
        out["finished"] = datetime.now(timezone.utc).isoformat()
        pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
        return

    # Phase 2: create/replace EventBridge schedule
    def setup_schedule():
        cfg = json.load(open(f"aws/lambdas/{fn}/config.json"))["eventbridge_scheduler"]
        sched_name = cfg["schedule_name"]
        try:
            ebs.delete_schedule(Name=sched_name); time.sleep(1)
        except ebs.exceptions.ResourceNotFoundException:
            pass
        ebs.create_schedule(
            Name=sched_name,
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
        return {"schedule": sched_name, "cron": cfg["cron"]}
    phase(out, "setup_schedule", setup_schedule)

    # Phase 3: invoke once
    def invoke():
        t0 = time.time()
        r = lam.invoke(FunctionName=fn,
                        InvocationType="RequestResponse",
                        Payload=b"{}")
        elapsed = round(time.time() - t0, 1)
        body = r["Payload"].read().decode("utf-8", errors="replace")
        try:
            p = json.loads(body)
            if isinstance(p.get("body"), str):
                try:
                    return {"elapsed_s": elapsed, "summary": json.loads(p["body"])}
                except Exception:
                    return {"elapsed_s": elapsed, "body": p["body"][:400]}
            return {"elapsed_s": elapsed, "p": str(p)[:300]}
        except Exception:
            return {"elapsed_s": elapsed, "raw": body[:400]}
    phase(out, "invoke", invoke)

    # Phase 4: read output
    def read_output():
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/convergence-radar.json")
        d = json.loads(obj["Body"].read())
        result = {
            "size_kb":       round(obj["ContentLength"]/1024, 1),
            "last_modified": obj["LastModified"].isoformat(),
            "schema":        d.get("schema_version"),
            "elapsed":       d.get("elapsed_sec"),
            "summary":       d.get("summary"),
            "alert_info":    d.get("alert_info"),
        }
        # Top 15 records with details
        result["top_15"] = []
        for r in (d.get("tickers") or [])[:15]:
            result["top_15"].append({
                "ticker":             r["ticker"],
                "tier":               r["tier"],
                "n_engines":          r["n_engines"],
                "convergence_score":  r["convergence_score"],
                "domain_coverage":    r["domain_coverage"],
                "engines_list":       sorted(r["engines"].keys()),
            })
        # Save full for offline review
        pathlib.Path("aws/ops/reports/1110_full.json").write_text(
            json.dumps(d, indent=2, default=str)
        )
        return result
    phase(out, "read_output", read_output)

    # Phase 5: look up the alert tickers specifically (SAP, CXAI, NBIS, ARM, RDDT)
    def check_alert_tickers():
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/convergence-radar.json")
        d = json.loads(obj["Body"].read())
        target_tickers = ["SAP", "CXAI", "NBIS", "ARM", "RDDT"]
        hits = {}
        for r in (d.get("tickers") or []):
            if r["ticker"] in target_tickers:
                hits[r["ticker"]] = {
                    "tier":              r["tier"],
                    "n_engines":         r["n_engines"],
                    "convergence_score": r["convergence_score"],
                    "domain_coverage":   r["domain_coverage"],
                    "engines":           list(r["engines"].keys()),
                }
        return {"hits": hits, "found": list(hits.keys()),
                "missing": [t for t in target_tickers if t not in hits]}
    phase(out, "check_alert_tickers", check_alert_tickers)

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1110] DONE")


if __name__ == "__main__":
    main()
