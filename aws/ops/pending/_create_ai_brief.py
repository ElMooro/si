"""Deploy justhodl-ai-brief — synthesizes 14 systems via Claude every 4h."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-ai-brief"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-ai-brief/source"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def make_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(SOURCE_DIR):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                full = os.path.join(root, f)
                rel = os.path.relpath(full, SOURCE_DIR)
                zf.write(full, rel)
    buf.seek(0)
    return buf.read()


def fetch_anthropic_key():
    """Pull Anthropic key from existing Lambda's env vars (morning-intelligence has it)."""
    for n in ["justhodl-morning-intelligence", "justhodl-ai-chat", "justhodl-watchlist-debate", "justhodl-prompt-iterator"]:
        try:
            cfg = lam.get_function_configuration(FunctionName=n)
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            for k in ["ANTHROPIC_API_KEY", "ANTHROPIC_KEY"]:
                if k in env and env[k]:
                    return env[k], n, k
        except Exception:
            continue
    return None, None, None


def main():
    with report("create_ai_brief") as r:
        r.heading("Deploy justhodl-ai-brief")

        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")

        anthropic_key, source_lambda, source_key = fetch_anthropic_key()
        if anthropic_key:
            r.ok(f"  ✓ anthropic key sourced from {source_lambda}.{source_key}  (len={len(anthropic_key)})")
        else:
            r.log(f"  ⚠ no Anthropic key found — Lambda will deploy but skip AI synthesis until key is wired")

        env_vars = {}
        if anthropic_key:
            env_vars["ANTHROPIC_KEY"] = anthropic_key

        try:
            lam.get_function(FunctionName=LAMBDA_NAME)
            lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
            time.sleep(3)
            lam.update_function_configuration(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                Timeout=180,
                MemorySize=512,
                Role=ROLE_ARN,
                Environment={"Variables": env_vars},
            )
            r.ok("  ✓ updated existing")
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Role=ROLE_ARN,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": zb},
                Timeout=180,
                MemorySize=512,
                Architectures=["x86_64"],
                Environment={"Variables": env_vars},
            )
            r.ok("  ✓ created")

        for _ in range(20):
            cfg = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)

        # Schedule: every 4h at :05
        r.heading("EventBridge schedule (every 4h at :05)")
        rule_name = f"{LAMBDA_NAME}-4h"
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="cron(5 0,4,8,12,16,20 ? * * *)",
            State="ENABLED",
            Description="AI brief synthesis — runs every 4h",
        )
        fn_arn = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]["FunctionArn"]
        events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": fn_arn}])
        try:
            lam.add_permission(
                FunctionName=LAMBDA_NAME,
                StatementId=f"{rule_name}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule_name}",
            )
        except lam.exceptions.ResourceConflictException:
            pass
        r.ok("  ✓ wired")

        # Smoke test
        r.heading("Smoke test (will call Claude — ~15-30s)")
        t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:400]}")

        # Verify
        r.heading("S3 verify")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ai-brief.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  generated_at: {d.get('generated_at')}")
            r.log(f"  duration_s: {d.get('duration_s')}")
            r.log(f"  model: {d.get('model')}")
            r.log(f"  brief_md_chars: {len(d.get('brief_md', ''))}")
            r.log(f"  usage: {d.get('usage')}")
            if d.get("error"):
                r.log(f"  ⚠ error: {d.get('error')}")
            md = d.get("brief_md", "")
            if md:
                r.log("")
                r.log("=== BRIEF PREVIEW (first 3000 chars) ===")
                for line in md[:3000].split("\n"):
                    r.log(f"    {line}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
