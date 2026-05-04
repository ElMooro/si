"""
Create justhodl-ab-test Lambda + daily schedule + smoke test.
"""
import os
import time
import zipfile
import io
import json
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-ab-test"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-ab-test/source"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
iam = boto3.client("iam", region_name=REGION)


def get_anthropic_key():
    """Pull Anthropic key from an existing Lambda env (e.g. justhodl-ai-chat) so
    we don't have to thread a secret through CI."""
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-ai-chat")
        env = cfg.get("Environment", {}).get("Variables", {})
        for k in ("ANTHROPIC_KEY", "ANTHROPIC_API_KEY"):
            if env.get(k):
                return env[k], k
    except Exception as e:
        print(f"could not source anthropic key from ai-chat: {e}")
    return "", "ANTHROPIC_KEY"


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


def main():
    with report("create_ab_test") as r:
        r.heading("Create justhodl-ab-test + smoke test")

        anthropic_key, anthropic_var = get_anthropic_key()
        r.log(f"  anthropic key sourced: {bool(anthropic_key)} (var: {anthropic_var})")

        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")

        env_vars = {
            anthropic_var: anthropic_key,
        }
        # Always set both env names so the Lambda can find one
        env_vars["ANTHROPIC_KEY"] = anthropic_key
        env_vars["ANTHROPIC_API_KEY"] = anthropic_key

        try:
            lam.get_function(FunctionName=LAMBDA_NAME)
            r.log(f"  function exists — updating")
            lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
            time.sleep(3)
            lam.update_function_configuration(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                MemorySize=512,
                Timeout=300,
                Environment={"Variables": env_vars},
            )
            r.ok(f"  ✓ updated")
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                Role=ROLE_ARN,
                Code={"ZipFile": zb},
                MemorySize=512,
                Timeout=300,
                Environment={"Variables": env_vars},
                Description="A/B test of competing prompt strategies",
            )
            r.ok(f"  ✓ created")

        r.section("EventBridge schedule (daily 16 UTC)")
        try:
            rule_name = f"{LAMBDA_NAME}-daily"
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="cron(0 16 * * ? *)",
                State="ENABLED",
                Description=f"Daily trigger for {LAMBDA_NAME}",
            )
            arn = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]["FunctionArn"]
            events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": arn}])
            try:
                lam.add_permission(
                    FunctionName=LAMBDA_NAME,
                    StatementId=f"{rule_name}-perm",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule_name}",
                )
            except lam.exceptions.ResourceConflictException:
                pass
            r.ok(f"  ✓ wired")
        except Exception as e:
            r.fail(f"  ✗ {e}")

        time.sleep(8)

        r.section("Smoke test")
        try:
            t0 = time.time()
            inv = lam.invoke(FunctionName=LAMBDA_NAME, Payload=b"{}")
            payload = inv["Payload"].read().decode()
            r.log(f"  status: {inv['StatusCode']} duration: {time.time()-t0:.1f}s")
            r.log(f"  resp: {payload[:600]}")
            if inv.get("FunctionError"):
                r.fail(f"  ✗ {inv['FunctionError']}")
                return
        except Exception as e:
            r.fail(f"  ✗ {e}")
            return

        r.section("S3 verify")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ab-test-results.json")
            data = json.loads(obj["Body"].read())
            r.log(f"  as_of: {data.get('as_of')}")
            r.log(f"  n_variants_tracked: {data.get('n_variants_tracked')}")
            r.log(f"  winner: {data.get('winner')}")

            r.section("📊 Challenger signals today")
            cs = data.get("challenger_signals_today", {})
            for v, payload in cs.items():
                if "error" in payload:
                    r.log(f"  {v:18s} ERROR: {payload['error']}")
                else:
                    r.log(f"  {v:18s} call={payload.get('call'):8s} conf={payload.get('confidence')} logged={payload.get('logged')}")

            r.section("🏆 Leaderboard")
            for row in data.get("leaderboard", [])[:6]:
                acc = row.get("accuracy_pct")
                acc_str = f"{acc:>5.1f}%" if acc is not None else "  n/a"
                r.log(
                    f"  {row['variant']:18s} acc={acc_str} n={row['n_scored']:>3d} "
                    f"CI=[{row['ci_95_low_pct']:>5.1f}%, {row['ci_95_high_pct']:>5.1f}%] "
                    f"sufficient={row['sufficient_data']}"
                )
        except Exception as e:
            r.fail(f"  ✗ {e}")


if __name__ == "__main__":
    main()
