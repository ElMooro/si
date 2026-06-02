"""1148 — diagnose 'Failed to fetch' bug end-to-end.

Checks:
  1. Does the equity-research/{TICKER}.json file exist in S3?
  2. What's the public ACL / bucket policy status?
  3. Does the Lambda URL respond at all to a basic request?
  4. CF proxy URL test
  5. Lambda function URL CORS configuration
"""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import urllib.request, urllib.error
import boto3
from botocore.exceptions import ClientError

REPORT = "aws/ops/reports/1148_failed_fetch_diag.json"
S3_BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-equity-research"
LAMBDA_URL = "https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
CF_PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
    except Exception as e:
        out["phases"].append({"name": name, "status": "ERROR",
                                "error": str(e)[:400],
                                "tb": traceback.format_exc()[:600]})


def check_s3_files():
    """List what's in equity-research/ prefix + check ACL on a sample object."""
    r = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="equity-research/", MaxKeys=20)
    files = [{"Key": o["Key"], "Size": o["Size"], "Modified": o["LastModified"].isoformat()}
                for o in (r.get("Contents") or [])]
    sample_acl = None
    if files:
        try:
            acl = s3.get_object_acl(Bucket=S3_BUCKET, Key=files[0]["Key"])
            sample_acl = {
                "Owner": (acl.get("Owner") or {}).get("DisplayName"),
                "Grants": [{"Type": g["Grantee"]["Type"], "URI": g["Grantee"].get("URI"),
                            "Permission": g["Permission"]} for g in acl["Grants"]],
            }
        except Exception as e:
            sample_acl = {"error": str(e)[:200]}
    return {"n_files": len(files), "files": files, "sample_acl": sample_acl}


def check_bucket_policy():
    try:
        p = s3.get_bucket_policy(Bucket=S3_BUCKET)
        return {"policy": json.loads(p["Policy"])}
    except ClientError as e:
        return {"no_policy": True, "code": e.response.get("Error", {}).get("Code")}


def check_public_access_block():
    try:
        pab = s3.get_public_access_block(Bucket=S3_BUCKET)
        return pab.get("PublicAccessBlockConfiguration")
    except ClientError as e:
        return {"code": e.response.get("Error", {}).get("Code")}


def check_lambda_url_config():
    try:
        cfg = lam.get_function_url_config(FunctionName=LAMBDA_NAME)
        return {
            "AuthType":      cfg.get("AuthType"),
            "InvokeMode":    cfg.get("InvokeMode"),
            "Cors":          cfg.get("Cors"),
            "FunctionUrl":   cfg.get("FunctionUrl"),
        }
    except Exception as e:
        return {"error": str(e)[:200]}


def http_test(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-1148/1.0"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            return {
                "url":       url,
                "http":      r.status,
                "elapsed_s": round(time.time() - t0, 2),
                "size":      len(body),
                "head":      body[:200].decode("utf-8", errors="replace"),
                "headers":   dict(r.getheaders()),
            }
    except urllib.error.HTTPError as e:
        return {"url": url, "http_error": e.code, "msg": e.reason,
                "elapsed_s": round(time.time() - t0, 2)}
    except Exception as e:
        return {"url": url, "error": str(e)[:300],
                "elapsed_s": round(time.time() - t0, 2)}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}

    phase(out, "s3_equity_research_files", check_s3_files)
    phase(out, "bucket_policy",            check_bucket_policy)
    phase(out, "public_access_block",      check_public_access_block)
    phase(out, "lambda_url_config",        check_lambda_url_config)

    # HTTP tests
    phase(out, "lambda_basic_call",
              lambda: http_test(f"{LAMBDA_URL}?ticker=AAPL", timeout=10))
    phase(out, "cf_proxy_research_file",
              lambda: http_test(f"{CF_PROXY}/equity-research/AAPL.json", timeout=10))
    phase(out, "cf_proxy_random_data_file",
              lambda: http_test(f"{CF_PROXY}/dashboard-state.json", timeout=10))
    phase(out, "lambda_with_options",
              lambda: http_test(LAMBDA_URL, timeout=5))  # OPTIONS-style preflight test

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1148] DONE")


if __name__ == "__main__":
    main()
