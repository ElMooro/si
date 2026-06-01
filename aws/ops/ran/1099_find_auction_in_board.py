"""1099 — find auction crisis entry in signal-board.json with correct key inspection."""
import json, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1099_board_find.json"
s3 = boto3.client("s3", region_name="us-east-1")

def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/signal-board.json")
    d = json.loads(obj["Body"].read())
    
    out["top_keys"] = list(d.keys())
    
    # Look at the structure of engines
    engines = d.get("engines") or []
    out["n_engines"] = len(engines)
    if engines:
        out["engine_sample"] = engines[0]
        out["engine_keys"] = list(engines[0].keys()) if isinstance(engines[0], dict) else None
    
    # Search for auction in any way
    out["matches"] = []
    for e in engines:
        s = json.dumps(e).lower()
        if "auction" in s:
            out["matches"].append(e)
    
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1099] DONE")

if __name__ == "__main__":
    main()
