"""justhodl-public-api-demo

Reference implementation showing how to use aws/shared/api_auth.py
in a public-facing Lambda. Hit it with a valid API key to see the
full auth + rate-limit flow.

USAGE:

  # Get a key from the admin Lambda first:
  curl -X POST <admin_url> \
    -H "Authorization: Bearer <admin_token>" \
    -d '{"action":"create","tier":"FREE","owner_email":"you@example.com"}'

  # Use it against this demo:
  curl <demo_url> -H "Authorization: Bearer jhd_..."

  # Or via x-api-key:
  curl <demo_url> -H "x-api-key: jhd_..."

RESPONSE on success:
  {
    "ok": true,
    "tier": "FREE",
    "tier_label": "Free",
    "owner_email": "you@example.com",
    "label": "...",
    "request_id": "...",
    "timestamp": "2026-05-06T...",
    "echo": <whatever was in the request body>,
    "limits": {"per_hour": 100, "per_day": 500, "per_sec": 5}
  }

RATE LIMIT (after exceeding):
  HTTP 429
  {
    "error": "rate_limit_exceeded_hour",
    "tier": "FREE",
    "limit_per_hour": 100,
    "current": 101,
    "resets_in_seconds": 1234
  }
  Retry-After: 1234
"""
import json
import os
import sys
from datetime import datetime, timezone

# Inject /var/task into path so Python finds the bundled api_auth.py
# (when deployed, api_auth.py sits next to lambda_function.py in the zip)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api_auth import authorize, TIERS


def lambda_handler(event, context):
    method = (event.get("requestContext", {}).get("http", {}).get("method")
              or event.get("httpMethod") or "GET").upper()

    # Preflight — Lambda Function URL CORS handles most of this, but be safe
    if method == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type, Authorization, x-api-key",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            },
            "body": "",
        }

    # Auth + rate limit gate — single line
    key_meta, err = authorize(event)
    if err:
        return err

    # Pull request body if any (for POST)
    raw_body = event.get("body") or ""
    body = None
    try:
        body = json.loads(raw_body) if raw_body else None
    except Exception:
        body = raw_body[:200] if isinstance(raw_body, str) else None

    # Echo back the auth context + tier limits + timestamp
    tier_limits = TIERS.get(key_meta["tier"], {})
    response_body = {
        "ok": True,
        "tier": key_meta["tier"],
        "tier_label": key_meta.get("tier_label"),
        "owner_email": key_meta.get("owner_email", ""),
        "label": key_meta.get("label", ""),
        "request_id": (context.aws_request_id if context else "no-context"),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "method": method,
        "echo": body,
        "limits": {
            "per_hour": tier_limits.get("per_hour"),
            "per_day": tier_limits.get("per_day"),
            "per_sec": tier_limits.get("per_sec"),
        },
        "message": "auth + rate-limit middleware working — your request was authorized",
    }

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "X-Tier": key_meta["tier"],
        },
        "body": json.dumps(response_body, default=str),
    }
