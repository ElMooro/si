"""Verify Anthropic credits restored and run ai-brief end-to-end.

Steps:
1. Tiny direct Anthropic call to confirm credits.
2. Invoke justhodl-ai-brief Lambda.
3. Pull data/ai-brief.json from S3 and print full brief_md.
4. Show usage/cost stats.
"""
import json
import time
import urllib.error
import urllib.request

import boto3
from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def get_anthropic_key():
    cfg = lam.get_function_configuration(FunctionName="justhodl-ai-brief")
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    return env.get("ANTHROPIC_KEY") or env.get("ANTHROPIC_API_KEY")


def call(prompt, key, max_tokens=10):
    body = json.dumps({
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-api-key": key,
            "anthropic-version": "2023-06-01",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode()), None
    except urllib.error.HTTPError as e:
        return e.code, None, e.read().decode("utf-8", errors="replace")


def main():
    with report("verify_credits_and_run_ai_brief") as r:
        # 1. Verify credits
        r.heading("1) Verify Anthropic credits restored")
        key = get_anthropic_key()
        if not key:
            r.log("  ✗ no key found")
            return
        r.log(f"  key prefix: {key[:14]}…  len={len(key)}")
        code, ok, err = call("Reply: ok", key, max_tokens=10)
        if ok:
            text = "".join(b.get("text", "") for b in ok.get("content", []))
            r.ok(f"  ✓ credits OK — response: {text!r}  usage: {ok.get('usage')}")
        else:
            r.log(f"  ✗ status {code}, error body: {err[:400]}")
            return

        # 2. Invoke ai-brief
        r.heading("2) Invoke justhodl-ai-brief")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-ai-brief", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:600]}")

        # 3. Pull from S3
        r.heading("3) Read data/ai-brief.json from S3")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/ai-brief.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  generated_at: {d.get('generated_at')}")
            r.log(f"  duration_s: {d.get('duration_s')}")
            r.log(f"  model: {d.get('model')}")
            r.log(f"  used_ai: {d.get('used_ai', '<not set>')}")
            r.log(f"  brief_md_chars: {len(d.get('brief_md') or '')}")
            r.log(f"  usage: {d.get('usage')}")
            err_field = d.get("error")
            if err_field:
                r.log(f"  ⚠ error: {err_field}")
            r.log("")
            r.heading("4) FULL BRIEF MARKDOWN")
            md = d.get("brief_md", "")
            for line in md.splitlines():
                r.log(line)
        except Exception as e:
            r.log(f"  ✗ s3 read: {e}")

        # 5. Cost estimate (haiku-4-5 pricing approx)
        r.heading("5) Cost estimate")
        try:
            usage = d.get("usage") or {}
            in_tok = usage.get("input_tokens", 0)
            out_tok = usage.get("output_tokens", 0)
            # Haiku 4.5: $1/M input, $5/M output (approx)
            cost_in = in_tok * 1.0 / 1_000_000
            cost_out = out_tok * 5.0 / 1_000_000
            r.log(f"  input tokens: {in_tok:,} (~${cost_in:.4f})")
            r.log(f"  output tokens: {out_tok:,} (~${cost_out:.4f})")
            r.log(f"  per-run cost: ~${cost_in + cost_out:.4f}")
            r.log(f"  6 runs/day × 30 days: ~${(cost_in + cost_out) * 180:.2f}/month")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
