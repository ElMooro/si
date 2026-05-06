"""Debug AI brief 400 error — capture full response body."""
import json
import os
import urllib.request
import boto3
from ops_report import report

lam = boto3.client("lambda", region_name="us-east-1")


def main():
    with report("debug_ai_brief") as r:
        r.heading("Test Anthropic API call directly with key from morning-intelligence")
        cfg = lam.get_function_configuration(FunctionName="justhodl-morning-intelligence")
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        key = env.get("ANTHROPIC_KEY") or env.get("ANTHROPIC_API_KEY")
        r.log(f"  key prefix: {key[:25] if key else 'MISSING'}...  len={len(key) if key else 0}")

        # Try simplest possible call
        url = "https://api.anthropic.com/v1/messages"
        body = json.dumps({
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 100,
            "messages": [{"role": "user", "content": "Reply with just: ok"}],
        }).encode()
        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                d = json.loads(resp.read().decode())
                r.ok(f"  ✓ direct call worked: {json.dumps(d)[:300]}")
        except urllib.error.HTTPError as e:
            err_body = e.read().decode()[:500]
            r.log(f"  ✗ HTTP {e.code}: {err_body}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
