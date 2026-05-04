"""Diagnose justhodl-ai-brief HTTP 400 — capture the actual Anthropic error body.

Steps:
1. Pull the same Anthropic key the Lambda uses.
2. Reconstruct the snapshot (load all 14 sources from S3) and measure prompt size.
3. Call Anthropic with a tiny prompt first to validate model+key.
4. Then call with the real prompt and capture HTTPError body if it 400s.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

import boto3
from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
MODEL = "claude-haiku-4-5-20251001"

s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def load_json(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        return {"_load_error": str(e)}


def get_anthropic_key():
    """Same lookup as the Lambda."""
    # Try env on a known Lambda
    for fn in ["justhodl-ai-brief", "justhodl-morning-intelligence", "justhodl-ai-chat"]:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            for k in ["ANTHROPIC_API_KEY", "ANTHROPIC_KEY"]:
                if env.get(k):
                    return env[k], f"{fn}.{k}"
        except Exception:
            continue
    try:
        v = ssm.get_parameter(Name="/justhodl/anthropic/api-key", WithDecryption=True)["Parameter"]["Value"]
        return v, "ssm:/justhodl/anthropic/api-key"
    except Exception:
        return None, None


def call(prompt, key, model=MODEL, max_tokens=200):
    body = json.dumps({"model": model, "max_tokens": max_tokens, "messages": [{"role": "user", "content": prompt}]}).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={"Content-Type": "application/json", "x-api-key": key, "anthropic-version": "2023-06-01"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read().decode()), None
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        return e.code, None, body_text
    except Exception as e:
        return None, None, str(e)


def main():
    with report("diagnose_ai_brief_400") as r:
        r.heading("Diagnose ai-brief HTTP 400")

        # 1. Key lookup
        key, source = get_anthropic_key()
        if not key:
            r.log("  ✗ no Anthropic key found")
            return
        r.log(f"  ✓ key from {source}, len={len(key)}, prefix={key[:12]}…")

        # 2. Tiny test
        r.heading("Test 1: tiny prompt to validate model + key")
        code, ok, err = call("Reply with 'ok'", key, max_tokens=10)
        r.log(f"  status: {code}")
        if ok:
            content = ok.get("content", [])
            text = "".join(b.get("text", "") for b in content if b.get("type") == "text")
            r.log(f"  ✓ response: {text[:60]!r}")
            r.log(f"  usage: {ok.get('usage')}")
        else:
            r.log(f"  ✗ error: {err}")

        # 3. Reconstruct snapshot the same way the Lambda does
        r.heading("Test 2: reconstruct snapshot, measure size")
        keys = [
            ("intelligence",       "intelligence-report.json"),
            ("calibration",        "data/calibration-snapshot.json"),
            ("sectors",            "data/sector-rotation.json"),
            ("momentum",           "data/momentum-scanner.json"),
            ("allocator",          "data/allocator.json"),
            ("asymmetric_setups",  "data/asymmetric-scorer.json"),
            ("risk_sizer",         "data/risk-sizer.json"),
            ("auction_stress",     "data/auction-crisis.json"),
            ("eurodollar_stress",  "data/eurodollar-stress.json"),
            ("macro_surprise",     "data/macro-surprise.json"),
            ("insider_buys",       "data/insider-trades.json"),
            ("earnings_pead",      "data/earnings-tracker.json"),
            ("correlation_breaks", "data/correlation-surface.json"),
            ("alerts",             "data/alert-history.json"),
        ]
        snap = {}
        sizes = {}
        for label, key_path in keys:
            d = load_json(key_path)
            s = json.dumps(d, default=str)
            sizes[label] = len(s)
            snap[label] = d
        total_chars = sum(sizes.values())
        r.log(f"  total snapshot chars: {total_chars:,}")
        r.log(f"  size by source (chars):")
        for label, sz in sorted(sizes.items(), key=lambda x: -x[1]):
            r.log(f"    {label:25s} {sz:>10,}")

        # Build a basic prompt
        snapshot_str = json.dumps(snap, default=str, indent=2)
        prompt_chars = len(snapshot_str) + 2000  # approx prompt overhead
        r.log(f"  full snapshot JSON chars: {len(snapshot_str):,}")
        r.log(f"  approx total prompt chars: {prompt_chars:,}")
        # Anthropic context window for haiku-4-5: 200k tokens, ~800k chars
        # Anthropic max_input_tokens for messages API: 200k
        # 1 token ≈ 4 chars, so 200k tokens ≈ 800k chars. Should be fine UNLESS something else.

        # 4. Try with snapshot — may hit 400 if max_tokens > model's max_output
        r.heading("Test 3: real prompt, capture 400 body")
        prompt = f"Summarize this in 3 bullets:\n```json\n{snapshot_str}\n```"
        # Try with max_tokens=2500 (what Lambda uses)
        code, ok, err = call(prompt, key, max_tokens=2500)
        r.log(f"  with max_tokens=2500: status={code}")
        if err:
            r.log(f"  error body: {err[:600]}")
        if ok:
            usage = ok.get("usage", {})
            r.log(f"  ✓ ok, in_tok={usage.get('input_tokens')} out_tok={usage.get('output_tokens')}")

        # 5. Try without max_tokens (which could be the bug — must be set, but at acceptable level for model)
        r.heading("Test 4: with smaller max_tokens=1024")
        code2, ok2, err2 = call(prompt, key, max_tokens=1024)
        r.log(f"  with max_tokens=1024: status={code2}")
        if err2:
            r.log(f"  error body: {err2[:600]}")
        if ok2:
            usage = ok2.get("usage", {})
            r.log(f"  ✓ ok, in_tok={usage.get('input_tokens')} out_tok={usage.get('output_tokens')}")

        # 6. Test with a smaller subset to isolate which payload is breaking
        r.heading("Test 5: minimal subset")
        small_prompt = "Reply with one word."
        code3, ok3, err3 = call(small_prompt, key, max_tokens=2500)
        r.log(f"  tiny prompt with max_tokens=2500: status={code3}")
        if err3:
            r.log(f"  error body: {err3[:600]}")


if __name__ == "__main__":
    main()
