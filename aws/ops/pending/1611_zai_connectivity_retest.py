"""
aws/ops/pending/1611_zai_connectivity_retest.py

Re-test Z.ai/GLM after balance loaded. Uses the CORRECT endpoint
(/api/paas/v4). Reads key from SSM. Tests glm-4.6 and glm-5.1, prints
reply + token usage + per-call cost estimate. No secret printed.
"""
import json
import urllib.request
import urllib.error
import boto3

REGION = "us-east-1"
BASE_URL = "https://api.z.ai/api/paas/v4"
MODELS = ["glm-4.6", "glm-5.1"]
PRICES = {"glm-4.6": (0.60, 2.20), "glm-5.1": (1.00, 3.00)}  # approx $/1M (in,out)

ssm = boto3.client("ssm", region_name=REGION)
KEY = ssm.get_parameter(Name="/justhodl/zai-api-key", WithDecryption=True)["Parameter"]["Value"]
print(f"Key loaded from SSM (length {len(KEY)}).\n")


def call(model):
    body = json.dumps({
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a terse assistant."},
            {"role": "user", "content": "Reply with exactly: connection OK"},
        ],
        "temperature": 0,
        "max_tokens": 32,
    }).encode("utf-8")
    req = urllib.request.Request(
        f"{BASE_URL}/chat/completions", data=body,
        headers={"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


all_ok = True
for model in MODELS:
    try:
        d = call(model)
        reply = d["choices"][0]["message"]["content"]
        u = d.get("usage", {})
        pin, pout = u.get("prompt_tokens", 0), u.get("completion_tokens", 0)
        cin, cout = PRICES.get(model, (0, 0))
        cost = (pin / 1e6) * cin + (pout / 1e6) * cout
        print(f"[OK]   {model}: {reply!r} | {pin} in / {pout} out | ~${cost:.6f}/call")
    except urllib.error.HTTPError as e:
        all_ok = False
        print(f"[FAIL] {model}: HTTP {e.code} — {e.read().decode('utf-8','ignore')[:300]}")
    except Exception as e:
        all_ok = False
        print(f"[FAIL] {model}: {e!r}")

print("\nRESULT:", "ALL MODELS OK" if all_ok else "ONE OR MORE FAILED")
if not all_ok:
    raise SystemExit(1)
