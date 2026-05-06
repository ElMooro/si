"""justhodl-api-keys-admin

Admin Lambda for managing API keys used by the Public API auth tier system.

ENDPOINTS (POST JSON to the Function URL with admin token):

  {"action": "create", "tier": "FREE|PRO|ENTERPRISE", "owner_email": "...", "label": "..."}
    → {"key": "jhd_abc...xyz", "key_hash": "...", "tier": "...", ...}
       The plain key is shown ONCE here. Store it securely — it can't
       be retrieved again. If lost, revoke + create a new one.

  {"action": "revoke", "key_hash": "..."}
    → {"revoked_at": "...", "key_hash": "..."}

  {"action": "list", "tier": "FREE|PRO|ENTERPRISE" (optional)}
    → {"keys": [{key_hash, tier, owner_email, label, created_at,
                 last_used_at, revoked_at, usage_total}, ...]}

  {"action": "rotate", "key_hash": "..."}
    → Creates a new key with the same metadata, revokes the old one.

AUTHENTICATION

  This Lambda is itself protected by an admin token in SSM at
  /justhodl/api-admin/token (SecureString). Pass via header:
    Authorization: Bearer <admin_token>
  This is separate from user API keys — only Khalid (or trusted ops)
  should know this admin token.

TYPICAL FLOW

  1. Issue a key for someone:
       curl -X POST <admin_url> \
         -H "Authorization: Bearer <admin_token>" \
         -d '{"action":"create","tier":"PRO","owner_email":"alice@example.com","label":"Alice prod"}'
  2. Send the returned `key` to the user securely (the plain `jhd_...`)
  3. They use it as `Authorization: Bearer jhd_...` against any
     API-tier-protected Lambda.
"""
import hashlib
import json
import os
import secrets
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = os.environ.get("AWS_REGION", "us-east-1")
KEYS_TABLE = os.environ.get("JUSTHODL_API_KEYS_TABLE", "justhodl-api-keys")
ADMIN_TOKEN_SSM = os.environ.get("ADMIN_TOKEN_SSM", "/justhodl/api-admin/token")

ddb = boto3.client("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

# Cache the admin token between invocations (Lambda warm-start)
_cached_admin_token = None
_cached_admin_token_ts = 0
_ADMIN_TOKEN_TTL = 300  # 5 min


def _cors():
    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
    }


def _err(status, code, message, extra=None):
    body = {"error": code, "message": message}
    if extra:
        body.update(extra)
    return {"statusCode": status, "headers": _cors(), "body": json.dumps(body)}


def _ok(body):
    return {"statusCode": 200, "headers": _cors(), "body": json.dumps(body, default=str)}


def _get_admin_token():
    global _cached_admin_token, _cached_admin_token_ts
    if _cached_admin_token and (time.time() - _cached_admin_token_ts) < _ADMIN_TOKEN_TTL:
        return _cached_admin_token
    resp = ssm.get_parameter(Name=ADMIN_TOKEN_SSM, WithDecryption=True)
    _cached_admin_token = resp["Parameter"]["Value"]
    _cached_admin_token_ts = time.time()
    return _cached_admin_token


def _check_admin(event):
    headers = {(k or "").lower(): v for k, v in (event.get("headers") or {}).items()}
    auth = headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        return _err(401, "unauthorized", "Pass admin token as 'Authorization: Bearer <token>'.")
    token = auth[7:].strip()
    try:
        expected = _get_admin_token()
    except Exception as e:
        return _err(503, "service_unavailable", f"Cannot retrieve admin token: {str(e)[:200]}")
    if token != expected:
        return _err(403, "forbidden", "Invalid admin token.")
    return None


def _generate_key():
    """Generate a fresh API key. Returns (plain_key, sha256_hash)."""
    # 32 bytes of randomness → URL-safe base64 → 43 chars
    raw = secrets.token_urlsafe(32)
    plain = f"jhd_{raw}"
    h = hashlib.sha256(plain.encode("utf-8")).hexdigest()
    return plain, h


def _action_create(body):
    tier = (body.get("tier") or "FREE").upper()
    if tier not in ("FREE", "PRO", "ENTERPRISE"):
        return _err(400, "bad_request", f"Invalid tier '{tier}'. Use FREE, PRO, or ENTERPRISE.")
    owner_email = body.get("owner_email") or ""
    label = body.get("label") or ""

    plain, key_hash = _generate_key()
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

    item = {
        "key_hash": {"S": key_hash},
        "tier": {"S": tier},
        "owner_email": {"S": owner_email},
        "label": {"S": label},
        "created_at": {"S": now_iso},
        "usage_total": {"N": "0"},
    }
    try:
        ddb.put_item(
            TableName=KEYS_TABLE,
            Item=item,
            ConditionExpression="attribute_not_exists(key_hash)",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            # Vanishingly improbable — but if there were a hash collision, retry
            return _err(500, "internal_error", "Key collision — please retry.")
        raise

    return _ok({
        "key": plain,                          # ONLY shown once
        "key_hash": key_hash,
        "tier": tier,
        "owner_email": owner_email,
        "label": label,
        "created_at": now_iso,
        "warning": "Save this key — it cannot be retrieved again. If lost, revoke and create a new one.",
    })


def _action_revoke(body):
    key_hash = body.get("key_hash")
    if not key_hash:
        return _err(400, "bad_request", "Missing 'key_hash'.")
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    try:
        resp = ddb.update_item(
            TableName=KEYS_TABLE,
            Key={"key_hash": {"S": key_hash}},
            ConditionExpression="attribute_exists(key_hash)",
            UpdateExpression="SET revoked_at = :t",
            ExpressionAttributeValues={":t": {"S": now_iso}},
            ReturnValues="ALL_NEW",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return _err(404, "not_found", f"No key with hash '{key_hash[:8]}...'.")
        raise
    return _ok({
        "key_hash": key_hash,
        "revoked_at": now_iso,
        "tier": resp["Attributes"].get("tier", {}).get("S", ""),
        "owner_email": resp["Attributes"].get("owner_email", {}).get("S", ""),
    })


def _action_list(body):
    """Scan the keys table — paginated. For a small admin tool this is OK."""
    filter_tier = (body.get("tier") or "").upper()
    items = []
    paginator_args = {"TableName": KEYS_TABLE}
    if filter_tier:
        paginator_args["FilterExpression"] = "tier = :t"
        paginator_args["ExpressionAttributeValues"] = {":t": {"S": filter_tier}}
    last_key = None
    while True:
        if last_key:
            paginator_args["ExclusiveStartKey"] = last_key
        elif "ExclusiveStartKey" in paginator_args:
            del paginator_args["ExclusiveStartKey"]
        resp = ddb.scan(**paginator_args)
        for item in resp.get("Items", []):
            decoded = {}
            for k, v in item.items():
                if "S" in v: decoded[k] = v["S"]
                elif "N" in v: decoded[k] = int(v["N"])
                elif "BOOL" in v: decoded[k] = v["BOOL"]
            items.append(decoded)
        last_key = resp.get("LastEvaluatedKey")
        if not last_key or len(items) >= 1000:
            break
    # Sort newest first
    items.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return _ok({"keys": items, "count": len(items)})


def _action_rotate(body):
    """Create a new key with same metadata, revoke the old one."""
    old_hash = body.get("key_hash")
    if not old_hash:
        return _err(400, "bad_request", "Missing 'key_hash'.")
    # Read old metadata
    resp = ddb.get_item(TableName=KEYS_TABLE, Key={"key_hash": {"S": old_hash}})
    if "Item" not in resp:
        return _err(404, "not_found", f"No key with hash '{old_hash[:8]}...'.")
    old = resp["Item"]
    # Create new
    create_resp = _action_create({
        "tier": old.get("tier", {}).get("S", "FREE"),
        "owner_email": old.get("owner_email", {}).get("S", ""),
        "label": (old.get("label", {}).get("S", "") + " (rotated)").strip(),
    })
    if create_resp["statusCode"] != 200:
        return create_resp
    # Revoke old
    revoke_resp = _action_revoke({"key_hash": old_hash})
    if revoke_resp["statusCode"] != 200:
        return revoke_resp
    # Combine info
    new_data = json.loads(create_resp["body"])
    return _ok({
        "rotated": True,
        "new_key": new_data["key"],
        "new_key_hash": new_data["key_hash"],
        "old_key_hash": old_hash,
        "old_revoked_at": json.loads(revoke_resp["body"])["revoked_at"],
        "warning": new_data["warning"],
    })


def lambda_handler(event, context):
    method = (event.get("requestContext", {}).get("http", {}).get("method")
              or event.get("httpMethod") or "POST").upper()

    if method == "OPTIONS":
        return {"statusCode": 200, "headers": _cors(), "body": ""}

    auth_err = _check_admin(event)
    if auth_err:
        return auth_err

    raw_body = event.get("body") or "{}"
    try:
        body = json.loads(raw_body) if isinstance(raw_body, str) else raw_body
    except Exception:
        return _err(400, "bad_request", "Body must be JSON.")

    action = (body.get("action") or "").lower()
    if action == "create":
        return _action_create(body)
    if action == "revoke":
        return _action_revoke(body)
    if action == "list":
        return _action_list(body)
    if action == "rotate":
        return _action_rotate(body)
    return _err(400, "bad_request",
                f"Unknown action '{action}'. Use: create, revoke, list, rotate.")
