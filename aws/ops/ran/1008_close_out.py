#!/usr/bin/env python3
"""Step 1008 — Final close-out: deploy fixed code, invoke, verify.

Single-commit ops execution. Reads latest source from disk (this runner has
the repo checked out), updates both Lambdas, invokes each, reads outputs.
"""
import io, json, os, time, zipfile, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1008_close_out.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def deploy(name):
    src_dir = pathlib.Path(f"aws/lambdas/{name}/source")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in src_dir.glob("*.py"):
            zf.writestr(f.name, f.read_bytes())
    zb = buf.getvalue()
    lam.update_function_code(FunctionName=name, ZipFile=zb, Publish=False)
    lam.get_waiter("function_updated").wait(FunctionName=name)
    return len(zb)


def invoke(name, timeout_sec=600):
    r = lam.invoke(FunctionName=name, InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", errors="replace")
    out = {"fn_err": r.get("FunctionError"), "status": r.get("StatusCode")}
    try:
        p = json.loads(body)
        out["result"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
    except Exception:
        out["raw"] = body[:600]
    return out


def s3_get_size(key):
    try:
        obj = s3.head_object(Bucket=BUCKET, Key=key)
        return {"size": obj["ContentLength"], "modified": str(obj["LastModified"])}
    except Exception as e:
        return {"missing": str(e)[:80]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    for name in ("justhodl-magnitude-distributions", "justhodl-miss-detector"):
        print(f"[1008] deploying {name}…")
        rec = {}
        try:
            rec["zip_size"] = deploy(name)
            time.sleep(2)
            print(f"[1008] invoking {name}…")
            rec["invoke"] = invoke(name)
        except Exception as e:
            rec["error"] = f"{type(e).__name__}: {str(e)[:300]}"
        out[name] = rec
    
    # Also re-invoke alpha-compass now that magdist may have data
    print("[1008] re-invoking alpha-compass…")
    try:
        rec = {"invoke": invoke("justhodl-alpha-compass")}
        out["justhodl-alpha-compass"] = rec
    except Exception as e:
        out["justhodl-alpha-compass"] = {"error": str(e)[:200]}
    
    # S3 state
    out["s3"] = {}
    for k in ("data/magnitude-distributions.json", "data/alpha-compass.json",
              "data/miss-summary.json"):
        out["s3"][k] = s3_get_size(k)
    
    # Read magdist content to see real numbers
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/magnitude-distributions.json")
        d = json.loads(obj["Body"].read().decode())
        out["magdist_payload"] = {
            "totals": d.get("totals"),
            "stacks_returned": len(d.get("stacks", [])),
            "by_signal_count": len(d.get("by_signal", {})),
            "top_3_stacks": d.get("stacks", [])[:3],
        }
    except Exception as e:
        out["magdist_payload_err"] = str(e)[:200]
    
    # Read miss-summary content
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/miss-summary.json")
        out["miss_summary_payload"] = json.loads(obj["Body"].read().decode())
    except Exception as e:
        out["miss_summary_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
