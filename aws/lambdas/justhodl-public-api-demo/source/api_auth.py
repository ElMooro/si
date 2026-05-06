"""Shared API auth + rate limiting for Public API Lambdas.

USAGE in a Lambda:

    from api_auth import authorize, AuthError

    def lambda_handler(event, context):
        # First line of every authed handler
        key_meta, err = authorize(event)
        if err:
            return err   # already a CORS-aware Lambda response dict

        # ... business logic ...
        return {"statusCode": 200, "headers": {...}, "body": json.dumps(result)}

DESIGN

  Auth header — accepts both:
    Authorization: Bearer jhd_<key>      (RFC 6750 standard)
    x-api-key:    jhd_<key>              (SDK convenience)

  Storage — only the SHA-256 hash of the key is ever stored:
    DDB justhodl-api-keys:  key_hash (PK) → tier, owner_email,
                            created_at, last_used_at, revoked_at, label
    Plain key is shown to user once at issuance, never again.

  Tiers:
    FREE        100 req/hour  500 req/day  5 req/sec burst
    PRO       5,000 req/hour  100k/day   20 req/sec burst
    ENTERPRISE unlimited (logged but not capped)

  Rate limiting — DynamoDB conditional UpdateItem:
    DDB justhodl-api-rate:  pk={hash}#h{epoch_hour}  →  count, ttl
    ADD count :inc with TTL auto-expiry (every hour ≈ a fresh row)

  Failure responses:
    401 unauthorized        — missing/invalid key
    403 forbidden          — key revoked
    429 too_many_requests  — over rate limit  (with Retry-After header)

  Performance:
    - 1 GetItem on api-keys (~5ms in-region)
    - 1 atomic UpdateItem on api-rate (~5ms)
    - Total auth overhead: ~10ms per request
    - For high-traffic Lambdas, key_meta can be cached in Lambda
      runtime memory (5min TTL) — caller's choice
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Optional, Tuple

import boto3
from botocore.exceptions import ClientError

REGION = os.environ.get("AWS_REGION", "us-east-1")
KEYS_TABLE = os.environ.get("JUSTHODL_API_KEYS_TABLE", "justhodl-api-keys")
RATE_TABLE = os.environ.get("JUSTHODL_API_RATE_TABLE", "justhodl-api-rate")

# Tier definitions — limits per (hour, day, burst_per_sec)
# Burst is checked via a separate 1-second window that's stricter.
TIERS = {
    "FREE": {
        "per_hour": 100,
        "per_day":  500,
        "per_sec":    5,
        "label":   "Free",
    },
    "PRO": {
        "per_hour": 5_000,
        "per_day":  100_000,
        "per_sec":  20,
        "label":    "Pro",
    },
    "ENTERPRISE": {
        "per_hour":  None,   # unlimited
        "per_day":   None,
        "per_sec":   None,
        "label":     "Enterprise",
    },
}

# Module-level boto3 clients — reused across invocations
_ddb = None


def _get_ddb():
    global _ddb
    if _ddb is None:
        _ddb = boto3.client("dynamodb", region_name=REGION)
    return _ddb


def _hash_key(plain: str) -> str:
    """SHA-256 hash of the plain API key, hex-encoded."""
    return hashlib.sha256(plain.encode("utf-8")).hexdigest()


def _extract_key_from_event(event: dict) -> Optional[str]:
    """Pull the API key from the request, supporting both header conventions."""
    # Lambda Function URL passes lower-cased headers in event["headers"]
    headers = event.get("headers") or {}
    # Normalise to lowercase since clients vary
    headers_lower = {k.lower(): v for k, v in headers.items() if isinstance(k, str)}

    # Try Authorization: Bearer <key>
    auth = headers_lower.get("authorization", "")
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
        if token:
            return token

    # Try x-api-key: <key>
    x_key = headers_lower.get("x-api-key")
    if x_key and x_key.strip():
        return x_key.strip()

    return None


def _cors_headers():
    """Minimal CORS — Lambda Function URLs typically have CORS configured at
    the URL level, but our error responses still need at least these."""
    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type, Authorization, x-api-key",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    }


def _err(status: int, code: str, message: str, extra: Optional[dict] = None,
         retry_after: Optional[int] = None) -> dict:
    """Build a standard error response."""
    body = {"error": code, "message": message}
    if extra:
        body.update(extra)
    headers = _cors_headers()
    if retry_after is not None:
        headers["Retry-After"] = str(retry_after)
    return {
        "statusCode": status,
        "headers": headers,
        "body": json.dumps(body),
    }


def _lookup_key_meta(key_hash: str) -> Optional[dict]:
    """Fetch key record from DDB. Returns None if not found."""
    try:
        resp = _get_ddb().get_item(
            TableName=KEYS_TABLE,
            Key={"key_hash": {"S": key_hash}},
            ConsistentRead=False,   # keys don't change often
        )
        item = resp.get("Item")
        if not item:
            return None
        # Decode DDB attribute types into a flat dict
        out = {}
        for k, v in item.items():
            if "S" in v: out[k] = v["S"]
            elif "N" in v: out[k] = int(v["N"])
            elif "BOOL" in v: out[k] = v["BOOL"]
            elif "NULL" in v: out[k] = None
        return out
    except ClientError as e:
        # If the table doesn't exist, return None rather than 500ing — the
        # caller can decide how to handle (typically: return 503 service
        # unavailable, but for early bootstrap we want to surface the issue).
        print(f"[api_auth] _lookup_key_meta error: {e}")
        return None


def _atomic_increment_window(key_hash: str, window_pk: str, ttl_seconds: int) -> int:
    """Atomically increment the rate-limit counter for (key, window).
    Returns the new count after increment."""
    now = int(time.time())
    expires = now + ttl_seconds
    try:
        resp = _get_ddb().update_item(
            TableName=RATE_TABLE,
            Key={"pk": {"S": window_pk}},
            UpdateExpression="ADD #c :one SET #ttl = if_not_exists(#ttl, :exp), #kh = if_not_exists(#kh, :kh)",
            ExpressionAttributeNames={
                "#c": "count",
                "#ttl": "ttl",
                "#kh": "key_hash",
            },
            ExpressionAttributeValues={
                ":one": {"N": "1"},
                ":exp": {"N": str(expires)},
                ":kh": {"S": key_hash},
            },
            ReturnValues="UPDATED_NEW",
        )
        return int(resp["Attributes"]["count"]["N"])
    except ClientError as e:
        # If something goes wrong with the rate table, fail OPEN (allow the
        # request) — better to over-serve a few than to break the whole API
        # because of a transient DDB issue.
        print(f"[api_auth] _atomic_increment_window error: {e}")
        return 0


def _update_last_used(key_hash: str) -> None:
    """Best-effort timestamp update on the keys table. Failure is not blocking."""
    try:
        _get_ddb().update_item(
            TableName=KEYS_TABLE,
            Key={"key_hash": {"S": key_hash}},
            UpdateExpression="SET last_used_at = :now ADD usage_total :one",
            ExpressionAttributeValues={
                ":now": {"S": str(int(time.time()))},
                ":one": {"N": "1"},
            },
        )
    except ClientError:
        pass  # ignore — non-critical


def authorize(event: dict, allowed_origins: Optional[list] = None) -> Tuple[Optional[dict], Optional[dict]]:
    """Auth + rate-limit gate for a Lambda invocation.

    DUAL MODE
    ─────────
    Strict mode (default, allowed_origins=None):
        API key is required. No origin bypass.

    Origin-bypass mode (allowed_origins is a non-empty list):
        If the request's Origin or Referer header matches one of the
        allowed origins, the request is allowed through WITHOUT a key,
        treated as ENTERPRISE-tier (no rate limit). This is intended
        for migrating existing Lambdas where the justhodl.ai frontend
        calls them directly — adding auth would otherwise break the
        page. External callers (curl, third-party apps) still need
        a valid jhd_<key>.

        Example:
            authorize(event, allowed_origins=[
                "https://justhodl.ai",
                "https://www.justhodl.ai",
            ])

    Args:
        event: Lambda event dict (Function URL format).
        allowed_origins: Optional list of origins that bypass the
                         API key requirement. If None or empty,
                         strict mode applies.

    Returns:
        (key_meta, error_response)
          - On success: (key_meta_dict, None)
          - On failure: (None, lambda_response_dict)

        On origin-bypass success, key_meta has:
          {tier: "ENTERPRISE", auth_mode: "origin", origin: "..."}
        On API-key success, key_meta has:
          {key_hash, tier, owner_email, label, created_at,
           auth_mode: "api_key"}
    """
    plain = _extract_key_from_event(event)

    # Origin-bypass mode — only triggers if no API key was provided
    # AND a matching origin/referer is present.
    if not plain and allowed_origins:
        bypass_origin = _check_origin_bypass(event, allowed_origins)
        if bypass_origin:
            # Pass through as ENTERPRISE-equivalent. We don't rate-limit
            # internal frontend traffic; it's already protected by CORS
            # at the Function URL level and by per-Lambda reserved
            # concurrency.
            return {
                "auth_mode": "origin",
                "tier": "ENTERPRISE",
                "tier_label": "Enterprise (frontend internal)",
                "origin": bypass_origin,
                "owner_email": "",
                "label": "frontend-internal",
                "created_at": "",
            }, None

    # Strict mode — API key required
    if not plain:
        return None, _err(401, "unauthorized",
                          "Missing API key. Pass 'Authorization: Bearer jhd_<key>' "
                          "or 'x-api-key: jhd_<key>'.")

    if not plain.startswith("jhd_") or len(plain) < 20:
        return None, _err(401, "unauthorized",
                          "Invalid API key format. Keys start with 'jhd_'.")

    key_hash = _hash_key(plain)
    meta = _lookup_key_meta(key_hash)
    if not meta:
        return None, _err(401, "unauthorized", "Unknown API key.")
    if meta.get("revoked_at"):
        return None, _err(403, "forbidden",
                          "API key has been revoked.",
                          extra={"revoked_at": meta.get("revoked_at")})

    tier = meta.get("tier", "FREE")
    if tier not in TIERS:
        # Defensive: if the stored tier name is unrecognized, treat as FREE
        tier = "FREE"
    limits = TIERS[tier]

    # Rate limiting — three windows: per-second (burst), per-hour, per-day
    now = int(time.time())
    epoch_sec = now
    epoch_hour = now // 3600
    epoch_day = now // 86400

    # Order matters: check tightest first so we don't increment looser counters
    # for requests that the tightest limit would reject. We check sec → hour → day.
    # For ENTERPRISE all three limits are None — skip checks but still log usage.

    if limits["per_sec"] is not None:
        sec_count = _atomic_increment_window(key_hash, f"{key_hash}#s{epoch_sec}", 60)
        if sec_count > limits["per_sec"]:
            return None, _err(429, "rate_limit_exceeded_burst",
                              f"Burst limit: {limits['per_sec']} req/sec for {tier} tier.",
                              extra={"tier": tier, "limit_per_sec": limits["per_sec"],
                                     "current": sec_count},
                              retry_after=1)

    if limits["per_hour"] is not None:
        hour_count = _atomic_increment_window(key_hash, f"{key_hash}#h{epoch_hour}", 3700)
        if hour_count > limits["per_hour"]:
            seconds_to_reset = 3600 - (now % 3600)
            return None, _err(429, "rate_limit_exceeded_hour",
                              f"Hourly limit: {limits['per_hour']} req/hour for {tier} tier.",
                              extra={"tier": tier, "limit_per_hour": limits["per_hour"],
                                     "current": hour_count,
                                     "resets_in_seconds": seconds_to_reset},
                              retry_after=seconds_to_reset)

    if limits["per_day"] is not None:
        day_count = _atomic_increment_window(key_hash, f"{key_hash}#d{epoch_day}", 90000)
        if day_count > limits["per_day"]:
            seconds_to_reset = 86400 - (now % 86400)
            return None, _err(429, "rate_limit_exceeded_day",
                              f"Daily limit: {limits['per_day']} req/day for {tier} tier.",
                              extra={"tier": tier, "limit_per_day": limits["per_day"],
                                     "current": day_count,
                                     "resets_in_seconds": seconds_to_reset},
                              retry_after=seconds_to_reset)

    # Best-effort usage timestamp update
    _update_last_used(key_hash)

    return {
        "auth_mode": "api_key",
        "key_hash": key_hash,
        "tier": tier,
        "tier_label": limits["label"],
        "owner_email": meta.get("owner_email", ""),
        "label": meta.get("label", ""),
        "created_at": meta.get("created_at", ""),
    }, None


def _check_origin_bypass(event: dict, allowed_origins: list) -> Optional[str]:
    """If the request's Origin or Referer header matches an allowed
    origin (case-insensitive, trailing-slash insensitive), return the
    matched origin string. Otherwise return None.

    Browsers send Origin on cross-origin requests and Referer on most
    requests — we check both to handle same-origin GETs that lack Origin.
    """
    headers = event.get("headers") or {}
    headers_lower = {k.lower(): v for k, v in headers.items() if isinstance(k, str)}

    # Normalize allowed origins for comparison
    allowed_set = {o.rstrip("/").lower() for o in allowed_origins}

    # Check Origin header (most reliable)
    origin = (headers_lower.get("origin") or "").rstrip("/").lower()
    if origin and origin in allowed_set:
        return origin

    # Fall back to Referer (browser sends this for same-origin nav)
    referer = (headers_lower.get("referer") or "").lower()
    if referer:
        # Extract scheme://host from referer URL
        try:
            from urllib.parse import urlparse
            parsed = urlparse(referer)
            ref_origin = f"{parsed.scheme}://{parsed.netloc}".rstrip("/").lower()
            if ref_origin in allowed_set:
                return ref_origin
        except Exception:
            pass

    return None



class AuthError(Exception):
    """Raised by Lambdas that prefer exception-based control flow over
    the (meta, err) tuple. Optional — not used internally."""
    def __init__(self, response: dict):
        self.response = response
        super().__init__(response.get("body", "Unauthorized"))
