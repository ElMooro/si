"""
aws/ops/pending/1610_zai_endpoint_diag.py

Diagnostic: ops 1609 got 'choices' KeyError. Print the RAW response (status +
body) from both candidate Z.ai endpoints so we can see what's actually
returned. Prints no secret.
"""
import json
import urllib.request
import urllib.error
import boto3

ssm = boto3.client("ssm", region_name="us-east-1")
KEY = ssm.get_parameter(Name="/justhodl/zai-api-key", WithDecryption=True)["Parameter"]["Value"]
print(f"key length {len(KEY)}\n")

ENDPOINTS = [
    "https://api.z.ai/api/paas/v4/chat/completions",
    "https://api.z.ai/api/openai/v1/chat/completions",
]

for url in ENDPOINTS:
    body = json.dumps({
        "model": "glm-4.6",
        "messages": [{"role": "user", "content": "Reply with: OK"}],
        "max_tokens": 16,
    }).encode("utf-8")
    req = urllib.request.Request(
        url, data=body,
        headers={"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"},
        method="POST",
    )
    print("=" * 60)
    print("URL:", url)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            status = r.status
            raw = r.read().decode("utf-8", "ignore")
        print("HTTP", status)
        print("BODY:", raw[:800])
    except urllib.error.HTTPError as e:
        print("HTTP", e.code)
        print("BODY:", e.read().decode("utf-8", "ignore")[:800])
    except Exception as e:
        print("EXC:", repr(e))
    print()
