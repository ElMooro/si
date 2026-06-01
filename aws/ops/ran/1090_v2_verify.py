"""1090 — invoke v2 auction-crisis Lambda + inspect new output schema."""
import io, json, os, pathlib, time, urllib.request, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1090_v2_verify.json"
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=180))
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Verify the deployed zip contains v2 module
    print("[1090] phase 1: verify v2 module in deployed zip…")
    info = lam.get_function(FunctionName="justhodl-auction-crisis-detector")
    url = info["Code"]["Location"]
    with urllib.request.urlopen(url, timeout=30) as r:
        zb = r.read()
    files = []
    with zipfile.ZipFile(io.BytesIO(zb)) as zf:
        files = zf.namelist()
    out["zip_files"] = files
    out["has_v2_module"] = "auction_crisis_v2.py" in files
    out["last_modified"] = info["Configuration"]["LastModified"]
    
    # Invoke
    print("[1090] phase 2: sync-invoke (will take 30-60s with all new layers)…")
    t0 = time.time()
    r = lam.invoke(FunctionName="justhodl-auction-crisis-detector",
                    InvocationType="RequestResponse", Payload=b"{}")
    out["elapsed_s"] = round(time.time() - t0, 1)
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        out["invoke_status"] = p.get("statusCode")
        if isinstance(p.get("body"), str):
            try:
                inner = json.loads(p["body"])
                out["summary"] = inner
            except Exception:
                out["body_preview"] = p["body"][:300]
    except Exception:
        out["raw_invoke"] = body[:500]
    
    # Read updated S3 file
    print("[1090] phase 3: inspect new schema in S3…")
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/auction-crisis.json")
    d = json.loads(obj["Body"].read())
    
    out["s3_last_modified"] = obj["LastModified"].isoformat()
    out["s3_size_kb"]       = round(obj["ContentLength"]/1024, 1)
    out["schema_version"]   = d.get("schema_version")
    out["composite_score"]  = d.get("composite_score")
    out["regime"]           = d.get("regime")
    
    # Audit each new v2 section
    v2_sections = ["tenor_decomposition", "forward_calendar", "historical_analog",
                    "cross_signals", "composite_history", "tail_risk", "triggers"]
    out["v2_sections"] = {}
    for k in v2_sections:
        v = d.get(k)
        if v is None:
            out["v2_sections"][k] = {"status": "MISSING"}
        elif isinstance(v, dict):
            out["v2_sections"][k] = {
                "status": "ok",
                "type":   "dict",
                "keys":   list(v.keys())[:10],
                "n_keys": len(v),
            }
            # Sample one
            if v:
                first_key = next(iter(v))
                first_val = v[first_key]
                if isinstance(first_val, dict):
                    out["v2_sections"][k]["sample"] = {
                        "key": first_key,
                        "value_keys": list(first_val.keys())[:8],
                    }
                else:
                    out["v2_sections"][k]["sample"] = {
                        "key": first_key,
                        "value": str(first_val)[:120],
                    }
        elif isinstance(v, list):
            out["v2_sections"][k] = {
                "status": "ok",
                "type":   "list",
                "len":    len(v),
            }
            if v and isinstance(v[0], dict):
                out["v2_sections"][k]["item_keys"] = list(v[0].keys())[:10]
    
    # Dump the entire file for reference
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    with open("aws/ops/reports/1090_auction_v2_full.json", "w") as f:
        json.dump(d, f, indent=2, default=str)
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1090] DONE")


if __name__ == "__main__":
    main()
