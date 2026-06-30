"""justhodl-llm-health — the LLM provider + AI-output health monitor.

Closes the gap exposed on 2026-06-29: an Anthropic outage ran for ~13 days
completely unnoticed because (a) nothing pinged the providers, and (b) the
fleet freshness monitors only check that *files* update — and they did, just
with null/error AI fields inside. This engine:

  1. Pings BOTH providers (Anthropic + Z.ai/GLM) with a tiny request and
     records the exact failure (e.g. "credit balance too low", "recharge").
  2. Scans key AI-output files for the "file fresh but AI field dead" pattern.
  3. Emits data/llm-health.json with HEALTHY / DEGRADED / CRITICAL + redundancy.

Runs every few hours, so the next outage is caught in hours, not weeks.
"""
import anthropic_shim  # resilient LLM fallback (Anthropic->GLM via llm_router)
import json
import os
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
ssm = boto3.client("ssm")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/llm-health.json"
ANTHROPIC_KEY = (os.environ.get("ANTHROPIC_KEY")
                 or os.environ.get("ANTHROPIC_API_KEY", ""))


def ping_anthropic():
    try:
        body = json.dumps({"model": "claude-haiku-4-5-20251001", "max_tokens": 8,
                           "messages": [{"role": "user", "content": "OK"}]}).encode()
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages", data=body,
            headers={"Content-Type": "application/json", "x-api-key": ANTHROPIC_KEY,
                     "anthropic-version": "2023-06-01"})
        urllib.request.urlopen(req, timeout=20).read()
        return {"provider": "anthropic", "ok": True, "status": 200}
    except urllib.error.HTTPError as e:
        msg = (e.read().decode() or "")[:200]
        low = msg.lower()
        return {"provider": "anthropic", "ok": False, "status": e.code, "error": msg,
                "billing_issue": ("credit" in low or "balance" in low or e.code == 400)}
    except Exception as e:
        return {"provider": "anthropic", "ok": False, "error": str(e)[:140]}


def ping_zai():
    try:
        key = ssm.get_parameter(Name="/justhodl/zai-api-key",
                                WithDecryption=True)["Parameter"]["Value"]
        body = json.dumps({"model": "glm-5.1", "max_tokens": 8,
                           "messages": [{"role": "user", "content": "OK"}]}).encode()
        req = urllib.request.Request(
            "https://api.z.ai/api/paas/v4/chat/completions", data=body,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"})
        urllib.request.urlopen(req, timeout=20).read()
        return {"provider": "zai_glm", "ok": True, "status": 200}
    except urllib.error.HTTPError as e:
        msg = (e.read().decode() or "")[:200]
        low = msg.lower()
        return {"provider": "zai_glm", "ok": False, "status": e.code, "error": msg,
                "billing_issue": ("balance" in low or "recharge" in low
                                  or "insufficient" in low or e.code == 429)}
    except Exception as e:
        return {"provider": "zai_glm", "ok": False, "error": str(e)[:140]}


def check_ai_outputs():
    """Detect the 'file is fresh but its AI field is null/error' pattern that the
    freshness monitors miss."""
    targets = [
        ("data/brain.json", "regime_read",
         lambda v: isinstance(v, dict) and not v.get("_error") and bool(v.get("regime"))),
        ("data/brain.json", "directive",
         lambda v: isinstance(v, dict) and bool(v.get("hard_rules"))),
    ]
    checks = []
    for key, field, ok in targets:
        try:
            d = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            checks.append({"key": key, "field": field, "healthy": bool(ok(d.get(field)))})
        except Exception as e:
            checks.append({"key": key, "field": field, "healthy": None, "err": str(e)[:70]})
    return checks


def lambda_handler(event=None, context=None):
    providers = [ping_anthropic(), ping_zai()]
    up = [p for p in providers if p.get("ok")]
    if len(up) == 2:
        status = "HEALTHY"
    elif len(up) == 1:
        status = "DEGRADED"        # one provider down — router fallback still works
    else:
        status = "CRITICAL"        # BOTH down — every AI engine is blind

    ai_checks = check_ai_outputs()
    degraded = [c for c in ai_checks if c.get("healthy") is False]
    billing = [p["provider"] for p in providers if p.get("billing_issue")]

    headline = (f"LLM providers {len(up)}/2 up [{status}] — "
                + ", ".join(f"{p['provider']}={'UP' if p.get('ok') else 'DOWN'}"
                            for p in providers))
    if billing:
        headline += f" | BILLING action needed: {', '.join(billing)}"
    if degraded:
        headline += f" | {len(degraded)} AI output(s) returning null"

    out = {
        "engine": "llm-health", "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "headline": headline,
        "redundancy": "intact" if len(up) >= 1 else "NONE — both providers down",
        "providers": providers,
        "billing_action_needed": billing,
        "ai_output_checks": ai_checks,
        "degraded_outputs": degraded,
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, indent=2).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    print(f"[llm-health] {headline}")
    return {"statusCode": 200, "body": status}
