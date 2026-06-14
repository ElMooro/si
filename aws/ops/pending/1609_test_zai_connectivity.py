"""
aws/ops/pending/test_zai_connectivity.py

Runs on AWS via GitHub Actions (run-ops pipeline). Verifies Z.ai/GLM
connectivity end to end: reads the key from SSM, calls GLM-4.6 and GLM-5.1,
and prints results + token usage to the Actions log. Prints NO secret.

Move to aws/ops/history/ after a confirmed green run.
"""

import json
import urllib.request
import urllib.error

import boto3

REGION = "us-east-1"
SSM_KEY_NAME = "/justhodl/zai-api-key"
BASE_URL = "https://api.z.ai/api/openai/v1"
MODELS = ["glm-4.6", "glm-5.1"]

PRICES = {  # approx $/1M (in, out) — for the log estimate only
    "glm-4.6": (0.60, 2.20),
    "glm-5.1": (1.00, 3.00),
}


def get_key():
    ssm = boto3.client("ssm", region_name=REGION)
    resp = ssm.get_parameter(Name=SSM_KEY_NAME, WithDecryption=True)
    return resp["Parameter"]["Value"]


def call(model, key):
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
        f"{BASE_URL}/chat/completions",
        data=body,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def main():
    print("Reading Z.ai key from SSM...")
    key = get_key()
    print(f"Key loaded (length {len(key)}). Testing models...\n")

    all_ok = True
    for model in MODELS:
        try:
            data = call(model, key)
            reply = data["choices"][0]["message"]["content"]
            u = data.get("usage", {})
            pin, pout = u.get("prompt_tokens", 0), u.get("completion_tokens", 0)
            cin, cout = PRICES.get(model, (0, 0))
            cost = (pin / 1e6) * cin + (pout / 1e6) * cout
            print(f"[OK]   {model}: {reply!r} | {pin} in / {pout} out | ~${cost:.6f}")
        except urllib.error.HTTPError as e:
            all_ok = False
            print(f"[FAIL] {model}: HTTP {e.code} — {e.read().decode('utf-8','ignore')[:200]}")
        except Exception as e:
            all_ok = False
            print(f"[FAIL] {model}: {e!r}")

    print("\nRESULT:", "ALL MODELS OK" if all_ok else "ONE OR MORE FAILED")
    if not all_ok:
        raise SystemExit(1)  # fail the Actions run so it shows red


if __name__ == "__main__":
    main()
