"""1104 — check macro-frontrun-sniffer.json state + identify why it's not loading."""
import json, pathlib, time
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1104_macro_check.json"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # 1. Does the data file exist?
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/macro-frontrun-sniffer.json")
        d = json.loads(obj["Body"].read())
        out["data_file"] = {
            "exists":         True,
            "size_kb":        round(obj["ContentLength"]/1024, 1),
            "last_modified":  obj["LastModified"].isoformat(),
            "age_h":          (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600,
            "top_keys":       list(d.keys()),
            "generated_at":   d.get("generated_at"),
            "macro_regime":   d.get("macro_regime"),
            "overall_score":  d.get("overall_macro_score"),
        }
    except s3.exceptions.NoSuchKey:
        out["data_file"] = {"exists": False, "err": "NoSuchKey"}
    except Exception as e:
        out["data_file"] = {"exists": False, "err": str(e)[:200]}
    
    # 2. List all macro-related keys
    try:
        resp = s3.list_objects_v2(Bucket="justhodl-dashboard-live",
                                    Prefix="data/", MaxKeys=1000)
        out["all_macro_keys"] = sorted([
            obj["Key"] for obj in (resp.get("Contents") or [])
            if "macro" in obj["Key"].lower() or "frontrun" in obj["Key"].lower()
            or "sniffer" in obj["Key"].lower()
        ])
    except Exception as e:
        out["list_err"] = str(e)[:200]
    
    # 3. Check the ai-brief-router Lambda — does it have macro_frontrun context?
    try:
        info = lam.get_function(FunctionName="justhodl-ai-brief-router")
        cfg = info["Configuration"]
        out["router"] = {
            "state":         cfg["State"],
            "last_modified": cfg["LastModified"],
            "code_size_kb":  round(cfg["CodeSize"]/1024, 1),
        }
    except Exception as e:
        out["router_err"] = str(e)[:200]
    
    # 4. Check the schedules that produce this
    try:
        sch_client = boto3.client("scheduler", region_name="us-east-1")
        resp = sch_client.list_schedules(MaxResults=100)
        out["schedules_with_macro"] = []
        for s in resp.get("Schedules", []):
            if "macro" in s.get("Name", "").lower():
                out["schedules_with_macro"].append({
                    "name":  s["Name"],
                    "state": s["State"],
                })
    except Exception as e:
        out["sch_err"] = str(e)[:200]
    
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1104] DONE")

if __name__ == "__main__":
    main()
