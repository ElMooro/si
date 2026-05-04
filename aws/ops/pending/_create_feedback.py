"""
Create justhodl-feedback Lambda + DynamoDB table + Function URL + smoke test.
"""
import os
import time
import zipfile
import io
import json
import secrets
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-feedback"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-feedback/source"
TABLE = "justhodl-feedback"

lam = boto3.client("lambda", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
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


def ensure_table(r):
    try:
        ddb.describe_table(TableName=TABLE)
        r.log(f"  ✓ table {TABLE} exists")
        return
    except ddb.exceptions.ResourceNotFoundException:
        pass
    ddb.create_table(
        TableName=TABLE,
        AttributeDefinitions=[{"AttributeName": "signal_id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "signal_id", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    waiter = ddb.get_waiter("table_exists")
    waiter.wait(TableName=TABLE)
    r.ok(f"  ✓ table {TABLE} created")


def ensure_token(r):
    path = "/justhodl/feedback/auth-token"
    try:
        v = ssm.get_parameter(Name=path, WithDecryption=True)["Parameter"]["Value"]
        r.log(f"  ✓ existing auth-token in SSM (len {len(v)})")
        return v
    except ssm.exceptions.ParameterNotFound:
        token = "fb_" + secrets.token_urlsafe(24)
        ssm.put_parameter(Name=path, Value=token, Type="SecureString", Overwrite=False)
        r.ok(f"  ✓ generated auth-token in SSM")
        return token


def main():
    with report("create_feedback") as r:
        r.heading("Create justhodl-feedback + table + URL")

        ensure_table(r)
        token = ensure_token(r)

        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")

        env_vars = {"FEEDBACK_AUTH_TOKEN": token}

        try:
            lam.get_function(FunctionName=LAMBDA_NAME)
            r.log(f"  function exists — updating")
            lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
            time.sleep(3)
            lam.update_function_configuration(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                MemorySize=256,
                Timeout=30,
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
                MemorySize=256,
                Timeout=30,
                Environment={"Variables": env_vars},
                Description="User feedback labels for signals",
            )
            r.ok(f"  ✓ created")

        r.section("Function URL")
        try:
            try:
                cfg = lam.get_function_url_config(FunctionName=LAMBDA_NAME)
                url = cfg["FunctionUrl"]
                r.log(f"  ✓ existing URL: {url}")
            except lam.exceptions.ResourceNotFoundException:
                cfg = lam.create_function_url_config(
                    FunctionName=LAMBDA_NAME,
                    AuthType="NONE",
                    Cors={
                        "AllowOrigins": ["*"],
                        "AllowMethods": ["GET", "POST", "OPTIONS"],
                        "AllowHeaders": ["content-type", "x-justhodl-token"],
                        "MaxAge": 86400,
                    },
                )
                url = cfg["FunctionUrl"]
                try:
                    lam.add_permission(
                        FunctionName=LAMBDA_NAME,
                        StatementId="FunctionURLAllowPublicAccess",
                        Action="lambda:InvokeFunctionUrl",
                        Principal="*",
                        FunctionUrlAuthType="NONE",
                    )
                except lam.exceptions.ResourceConflictException:
                    pass
                r.ok(f"  ✓ created URL: {url}")
            ssm.put_parameter(Name="/justhodl/feedback/url", Value=url, Type="String", Overwrite=True)
        except Exception as e:
            r.fail(f"  ✗ URL setup: {e}")

        time.sleep(5)

        r.section("Smoke test — GET /signals")
        try:
            t0 = time.time()
            inv = lam.invoke(
                FunctionName=LAMBDA_NAME,
                Payload=json.dumps({
                    "requestContext": {"http": {"method": "GET", "path": "/signals"}},
                    "rawPath": "/signals",
                    "queryStringParameters": {"limit": "5"},
                    "headers": {},
                }).encode(),
            )
            payload = inv["Payload"].read().decode()
            r.log(f"  status: {inv['StatusCode']} duration: {time.time()-t0:.1f}s")
            data = json.loads(payload)
            body = json.loads(data.get("body", "{}"))
            r.log(f"  signals returned: {len(body.get('signals', []))}")
            for s in body.get("signals", [])[:3]:
                r.log(f"    {s.get('signal_id')[:40]:40s} type={s.get('signal_type'):25s} val={s.get('signal_value')}")
            if inv.get("FunctionError"):
                r.fail(f"  ✗ {inv['FunctionError']}: {payload[:300]}")
                return
        except Exception as e:
            r.fail(f"  ✗ {e}")

        r.section("Smoke test — POST /label")
        try:
            test_payload = json.dumps({
                "requestContext": {"http": {"method": "POST", "path": "/label"}},
                "rawPath": "/label",
                "headers": {"x-justhodl-token": token, "origin": "https://justhodl.ai"},
                "body": json.dumps({
                    "signal_id": "test_smoke_" + str(int(time.time())),
                    "label": "GOOD_CALL",
                    "note": "smoke test from deployment",
                }),
            }).encode()
            inv = lam.invoke(FunctionName=LAMBDA_NAME, Payload=test_payload)
            payload = inv["Payload"].read().decode()
            data = json.loads(payload)
            r.log(f"  status: {inv['StatusCode']}")
            body = json.loads(data.get("body", "{}"))
            r.log(f"  body: {json.dumps(body)[:300]}")
        except Exception as e:
            r.fail(f"  ✗ {e}")

        r.section("Smoke test — GET /list")
        try:
            inv = lam.invoke(
                FunctionName=LAMBDA_NAME,
                Payload=json.dumps({
                    "requestContext": {"http": {"method": "GET", "path": "/list"}},
                    "rawPath": "/list",
                    "queryStringParameters": {"limit": "5"},
                    "headers": {},
                }).encode(),
            )
            payload = inv["Payload"].read().decode()
            data = json.loads(payload)
            body = json.loads(data.get("body", "{}"))
            r.log(f"  feedback count: {len(body.get('feedback', []))}")
            for f in body.get("feedback", [])[:3]:
                r.log(f"    {f.get('signal_id')[:40]:40s} label={f.get('label'):12s} at={f.get('updated_at')}")
        except Exception as e:
            r.fail(f"  ✗ {e}")


if __name__ == "__main__":
    main()
