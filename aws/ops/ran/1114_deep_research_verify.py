"""1114 — set up schedule, invoke deep-research, verify research bundle.

Expects to see Claude-generated bull thesis + risk assessment for top 15
pump candidates.
"""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1114_deep_research_verify.json"
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
        out["phases"].append({"name": name, "status": "ERROR",
                                "error": str(e)[:400],
                                "traceback": traceback.format_exc()[:1500]})
        return None


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}
    fn = "justhodl-ticker-deep-research"

    # Check Lambda exists
    def check():
        info = lam.get_function(FunctionName=fn)
        cfg = info["Configuration"]
        return {
            "state":         cfg["State"],
            "last_modified": cfg["LastModified"],
            "code_size_kb":  round(cfg["CodeSize"]/1024, 1),
            "memory":        cfg["MemorySize"],
            "timeout":       cfg["Timeout"],
            "has_api_key":   bool((cfg.get("Environment",{}) or {}).get("Variables",{}).get("ANTHROPIC_API_KEY")),
        }
    state = phase(out, "check", check)
    if not state:
        pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
        return

    # Setup schedule
    def setup_schedule():
        cfg = json.load(open(f"aws/lambdas/{fn}/config.json"))["eventbridge_scheduler"]
        name = cfg["schedule_name"]
        try:
            ebs.delete_schedule(Name=name); time.sleep(1)
        except ebs.exceptions.ResourceNotFoundException: pass
        ebs.create_schedule(
            Name=name,
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
        return {"schedule": name, "cron": cfg["cron"]}
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
                    return {"elapsed_s": elapsed, "summary": json.loads(p["body"])}
                except Exception:
                    return {"elapsed_s": elapsed, "body": p["body"][:400]}
            return {"elapsed_s": elapsed, "p": str(p)[:400]}
        except Exception:
            return {"elapsed_s": elapsed, "raw": body[:600]}
    phase(out, "invoke", invoke)

    # Read research bundle
    def read_bundle():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/ticker-research-bundle.json")
        d = json.loads(obj["Body"].read())
        result = {
            "size_kb":       round(obj["ContentLength"]/1024, 1),
            "last_modified": obj["LastModified"].isoformat(),
            "schema":        d.get("schema_version"),
            "elapsed_sec":   d.get("elapsed_sec"),
            "claude_elapsed": d.get("claude_elapsed"),
            "n_tickers":     d.get("n_tickers"),
            "tickers":       d.get("tickers"),
        }
        if d.get("status") == "error":
            result["status"] = "error"
            result["error"]  = d.get("error")
            result["raw_preview"] = d.get("raw_preview", "")[:300]
        else:
            # Sample dossier — first ticker
            research = d.get("research", {})
            if research:
                first_t = list(research.keys())[0]
                first = research[first_t]
                result["sample_dossier"] = {
                    "ticker":              first_t,
                    "convergence_summary": first.get("convergence_summary"),
                    "bull_headline":       (first.get("bull_thesis") or {}).get("headline"),
                    "bull_thesis":         (first.get("bull_thesis") or {}).get("thesis"),
                    "bull_catalysts":      (first.get("bull_thesis") or {}).get("key_catalysts"),
                    "risk_headline":       (first.get("risk_assessment") or {}).get("headline"),
                    "risk_primary_risks":  (first.get("risk_assessment") or {}).get("primary_risks"),
                    "valuation_concern":   (first.get("risk_assessment") or {}).get("valuation_concern"),
                    "trade_framework":     first.get("trade_framework"),
                    "ai_one_liner":        first.get("ai_one_liner"),
                }
            # Save full bundle for offline inspection
            pathlib.Path("aws/ops/reports/1114_full_bundle.json").write_text(
                json.dumps(d, indent=2, default=str))
        return result
    phase(out, "read_bundle", read_bundle)

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1114] DONE")


if __name__ == "__main__":
    main()
