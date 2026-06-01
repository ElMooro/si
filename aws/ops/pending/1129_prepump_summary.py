"""1129 — schedule + invoke prepump-summary, verify slim file size."""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1129_prepump_summary.json"
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=120))
s3  = boto3.client("s3", region_name="us-east-1")
ebs = boto3.client("scheduler", region_name="us-east-1")


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
    except Exception as e:
        out["phases"].append({"name": name, "status": "ERROR", "error": str(e)[:300],
                                "traceback": traceback.format_exc()[:1200]})


def setup_schedule(fn_name: str) -> dict:
    cfg = json.load(open(f"aws/lambdas/{fn_name}/config.json"))["eventbridge_scheduler"]
    name = cfg["schedule_name"]
    try: ebs.delete_schedule(Name=name); time.sleep(1)
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

    phase(out, "schedule", lambda: setup_schedule("justhodl-prepump-summary"))
    phase(out, "invoke",   lambda: invoke("justhodl-prepump-summary"))

    def read_summary():
        time.sleep(2)
        # Get the gzipped object's metadata
        h = s3.head_object(Bucket="justhodl-dashboard-live", Key="data/pump-radar-summary.json")
        gz_size = h["ContentLength"]
        ce = h.get("ContentEncoding")
        cc = h.get("CacheControl")

        # Fetch and decode for inspection
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/pump-radar-summary.json")
        body_bytes = obj["Body"].read()
        if obj.get("ContentEncoding") == "gzip":
            import gzip
            decoded = gzip.decompress(body_bytes)
        else:
            decoded = body_bytes
        d = json.loads(decoded)

        # Get plain fallback size for comparison
        try:
            h2 = s3.head_object(Bucket="justhodl-dashboard-live", Key="data/pump-radar-summary.plain.json")
            plain_size = h2["ContentLength"]
        except Exception:
            plain_size = None

        # Compare with old payload (the 5 files we WERE loading)
        old_keys = ["data/pump-radar-brief.json", "data/pump-positioning.json",
                     "data/catalysts.json", "data/catalyst-clusters.json",
                     "data/velocity-acceleration.json"]
        total_old = 0
        for k in old_keys:
            try:
                hh = s3.head_object(Bucket="justhodl-dashboard-live", Key=k)
                total_old += hh["ContentLength"]
            except Exception: pass

        pathlib.Path("aws/ops/reports/1129_summary_full.json").write_text(
            json.dumps(d, indent=2, default=str))

        return {
            "gzipped_size_bytes":    gz_size,
            "gzipped_size_kb":       round(gz_size/1024, 2),
            "plain_size_bytes":      plain_size,
            "plain_size_kb":         round(plain_size/1024, 2) if plain_size else None,
            "content_encoding":      ce,
            "cache_control":         cc,
            "old_payload_kb":        round(total_old/1024, 1),
            "shrinkage_pct":         round(100 * (1 - gz_size/total_old), 1) if total_old else None,
            "conviction":            d.get("conviction"),
            "n_top_picks":           len(d.get("top_picks") or []),
            "n_clusters":            len(d.get("clusters") or []),
            "n_suggested":           len(d.get("suggested_additions") or []),
            "sources_loaded":        d.get("sources_loaded"),
            "sources_freshness":     d.get("sources_freshness"),
            "top_picks_preview":     d.get("top_picks", [])[:3],
        }
    phase(out, "verify_summary", read_summary)

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1129] DONE")


if __name__ == "__main__":
    main()
