#!/usr/bin/env python3
"""583 — Retry auction-grader bootstrap with file existence checks."""
import io, json, os, time as _time, base64, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/583_auction_grader_retry.json"
NAME = "justhodl-auction-grader"
ACCOUNT = "857687956942"
REGION = "us-east-1"
ROLE = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
RULE_NAME = f"{NAME}-hourly"
SCHEDULE = "cron(45 * ? * * *)"
SOURCE_DIR = "aws/lambdas/justhodl-auction-grader/source"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # File existence diagnostics
    out["cwd"] = os.getcwd()
    out["source_dir_exists"] = os.path.exists(SOURCE_DIR)
    if os.path.exists(SOURCE_DIR):
        out["source_listing"] = sorted(os.listdir(SOURCE_DIR))
        for fn in os.listdir(SOURCE_DIR):
            fp = os.path.join(SOURCE_DIR, fn)
            if os.path.isfile(fp):
                out.setdefault("file_sizes", {})[fn] = os.path.getsize(fp)
    else:
        # Try absolute path
        candidates = [
            "/github/workspace/" + SOURCE_DIR,
            "/home/runner/work/si/si/" + SOURCE_DIR,
        ]
        for c in candidates:
            out.setdefault("alt_check", {})[c] = os.path.exists(c)

    # Build zip
    buf = io.BytesIO()
    files_added = []
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _d, files in os.walk(SOURCE_DIR):
            for fn in files:
                if fn.endswith(".pyc") or "__pycache__" in root: continue
                fp = os.path.join(root, fn)
                rel = os.path.relpath(fp, SOURCE_DIR)
                zf.write(fp, rel)
                files_added.append(rel)
    zip_bytes = buf.getvalue()
    out["zip_size_kb"] = round(len(zip_bytes)/1024, 1)
    out["files_in_zip"] = files_added

    if len(zip_bytes) < 100:
        out["abort"] = "zip is empty — source files missing"
        out["finished"] = datetime.now(timezone.utc).isoformat()
        os.makedirs(os.path.dirname(REPORT), exist_ok=True)
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)
        return

    env_vars = {
        "S3_BUCKET": "justhodl-dashboard-live",
    }
    try:
        env_vars["TELEGRAM_TOKEN"] = ssm.get_parameter(
            Name="/justhodl/telegram/token", WithDecryption=True
        )["Parameter"]["Value"]
        env_vars["TELEGRAM_CHAT_ID"] = ssm.get_parameter(
            Name="/justhodl/telegram/chat_id", WithDecryption=True
        )["Parameter"]["Value"]
    except Exception: pass

    try:
        lam.get_function(FunctionName=NAME)
        lam.update_function_code(FunctionName=NAME, ZipFile=zip_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        lam.update_function_configuration(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=256, Timeout=60, Role=ROLE,
            Environment={"Variables": env_vars},
        )
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        out["action"] = "update"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE,
            MemorySize=256, Timeout=60, Architectures=["arm64"],
            Code={"ZipFile": zip_bytes},
            Environment={"Variables": env_vars},
            Description="Treasury auction A-F grader",
        )
        lam.get_waiter("function_active").wait(FunctionName=NAME)
        out["action"] = "create"

    events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                     Description="Auction Grader hourly")
    try:
        lam.add_permission(
            FunctionName=NAME, StatementId=f"{RULE_NAME}-invoke",
            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE_NAME}",
        )
    except lam.exceptions.ResourceConflictException: pass
    events.put_targets(Rule=RULE_NAME, Targets=[{
        "Id": "1",
        "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{NAME}",
    }])
    out["eventbridge"] = "OK"

    _time.sleep(3)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                       LogType="Tail", Payload=b"{}")
    out["invoke_status"] = resp.get("StatusCode")
    out["fn_error"] = resp.get("FunctionError")
    body = resp["Payload"].read().decode("utf-8")
    try:
        p = json.loads(body)
        out["response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
    except: out["raw"] = body[:500]
    if resp.get("LogResult"):
        log = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")
        out["log_tail"] = log[-2000:]

    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/auction-grades.json")
        out["sidecar"] = json.loads(obj["Body"].read())
    except Exception as e:
        out["sidecar_err"] = str(e)[:120]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
