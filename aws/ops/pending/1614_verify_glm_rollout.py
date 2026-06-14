"""
aws/ops/pending/1614_verify_glm_rollout.py
Verify GLM rollout in production by invoking crypto-intel (uses shared
llm_router) and confirming the served model. Proves both GLM serving and
that the bundled aws/shared/llm_router import resolves at runtime.
"""
import json
import boto3
from botocore.config import Config

lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300, retries={"max_attempts": 0}))


def probe(fn, event):
    try:
        resp = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                          Payload=json.dumps(event).encode())
        raw = resp["Payload"].read().decode()
        used = "glm-5.1" if "glm-5.1" in raw else ("sonnet/claude" if "claude" in raw else "?")
        print(f"{fn}: lambda_status={resp.get('StatusCode')} served_model_marker={used}")
        print(f"   snippet: {raw[:280]}")
        return used
    except Exception as e:
        print(f"{fn}: invoke error {e!r}")
        return None


print("=== crypto-intel (shared router path) ===")
probe("justhodl-crypto-intel", {})
