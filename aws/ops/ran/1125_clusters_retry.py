"""1125 — retry catalyst-clusters after expansion fix."""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT = "aws/ops/reports/1125_clusters_retry.json"
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=350))
s3 = boto3.client("s3", region_name="us-east-1")

def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
    except Exception as e:
        out["phases"].append({"name": name, "status": "ERROR", "error": str(e)[:300],
                                "traceback": traceback.format_exc()[:1200]})

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
    except Exception: return {"elapsed_s": elapsed, "raw": body[:800]}

def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}
    phase(out, "invoke_clusters", lambda: invoke("justhodl-catalyst-clusters"))
    def read():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/catalyst-clusters.json")
        d = json.loads(obj["Body"].read())
        pathlib.Path("aws/ops/reports/1125_clusters_full.json").write_text(
            json.dumps(d, indent=2, default=str))
        return {
            "n_clusters":  d.get("n_clusters"),
            "n_temporal":  d.get("n_temporal"),
            "n_thematic":  d.get("n_thematic"),
            "clusters":    d.get("clusters", []),
            "basket_action_summary": d.get("basket_action_summary"),
            "size_deltas": d.get("size_deltas"),
        }
    phase(out, "read_clusters", read)
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1125] DONE")

if __name__ == "__main__":
    main()
