"""1089 — fetch current auction-crisis.json schema + Lambda data ranges."""
import json, os, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1089_auction_inspect.json"
s3 = boto3.client("s3", region_name="us-east-1")

def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Fetch current S3 data
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/auction-crisis.json")
        d = json.loads(obj["Body"].read())
        out["s3_meta"] = {
            "last_modified": obj["LastModified"].isoformat(),
            "size_kb":       round(obj["ContentLength"]/1024, 1),
        }
        out["top_keys"] = list(d.keys())
        out["composite_score"] = d.get("composite_score")
        out["regime"]          = d.get("regime")
        out["generated_at"]    = d.get("generated_at")
        
        # Schema deep-dive
        out["schema_deep"] = {}
        for k, v in d.items():
            if isinstance(v, dict):
                out["schema_deep"][k] = {"_type": "dict", "keys": list(v.keys())[:12]}
                # Show first nested level
                for k2, v2 in list(v.items())[:3]:
                    if isinstance(v2, dict):
                        out["schema_deep"][k][k2] = list(v2.keys())[:8]
                    elif isinstance(v2, list):
                        out["schema_deep"][k][k2] = f"<list[{len(v2)}]>"
                    else:
                        out["schema_deep"][k][k2] = str(v2)[:60]
            elif isinstance(v, list):
                out["schema_deep"][k] = {"_type": "list", "len": len(v)}
                if v and isinstance(v[0], dict):
                    out["schema_deep"][k]["sample"] = list(v[0].keys())[:10]
            else:
                out["schema_deep"][k] = str(v)[:80]
        
        # Save the actual data file so we can inspect locally
        pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
        with open("aws/ops/reports/1089_auction_data.json", "w") as f:
            json.dump(d, f, indent=2, default=str)
        out["full_data_saved"] = "aws/ops/reports/1089_auction_data.json"
    except Exception as e:
        out["err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1089] DONE")

if __name__ == "__main__":
    main()
