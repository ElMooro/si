"""ops 1078 — verify patches:
  1. Invoke justhodl-concentration-liquidity → check it reads firm-book + writes output
  2. Invoke justhodl-news-wire → check FMP /stable/news/* now returns headlines
  3. Read S3 outputs to confirm
"""
import json, os, time, base64
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())


def wait_idle(lam, fn, max_wait=120):
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") in ("Successful", None):
                return cfg
        except Exception:
            pass
        time.sleep(3)
    return None


def invoke_and_read(lam, s3, fn, output_key):
    out = {"fn": fn, "output_key": output_key}
    cfg = wait_idle(lam, fn)
    if not cfg:
        out["err"] = "function not idle"
        return out
    try:
        inv = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", LogType="Tail")
        out["status_code"] = inv["StatusCode"]
        out["function_error"] = inv.get("FunctionError")
        log = base64.b64decode(inv.get("LogResult", "")).decode("utf-8", errors="replace")
        out["log_tail"] = log[-1200:]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]
        return out
    time.sleep(3)
    try:
        s3o = s3.get_object(Bucket=BUCKET, Key=output_key)
        body = s3o["Body"].read().decode("utf-8", errors="replace")
        parsed = json.loads(body)
        out["s3_size"] = len(body)
        out["s3_last_modified"] = s3o["LastModified"].isoformat()
        # top-level summary
        if isinstance(parsed, dict):
            out["top_keys"] = list(parsed.keys())
            # extract a useful summary subset
            for k in ("summary", "generated_at", "n_positions", "total_nav", "top10_pct",
                      "concentration_alerts", "n_headlines", "high_impact_count",
                      "top_24h", "drift_state", "n_new", "no_action"):
                if k in parsed:
                    val = parsed[k]
                    if isinstance(val, list):
                        out[f"{k}_count"] = len(val)
                    else:
                        out[k] = val
            out["sample"] = json.dumps(parsed, default=str)[:1200]
    except Exception as e:
        out["s3_err"] = str(e)[:200]
    return out


def main():
    lam = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "results": [
            invoke_and_read(lam, s3, "justhodl-concentration-liquidity", "data/concentration-liquidity.json"),
            invoke_and_read(lam, s3, "justhodl-news-wire", "data/news-wire.json"),
        ],
    }
    report["finished_at"] = datetime.now(timezone.utc).isoformat()

    out_path = os.path.join(REPO_ROOT, "aws/ops/reports/1078.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str)[:4500])


if __name__ == "__main__":
    main()
