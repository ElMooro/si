"""
justhodl-push-api — Web Push subscription manager + sender.

Endpoints (Lambda Function URL):
  GET  /vapid-public-key       → returns base64url-encoded uncompressed P-256 public key
  POST /subscribe              → register {subscription, user_agent} in DDB
  POST /unsubscribe            → remove {endpoint} from DDB
  POST /send                   → broadcast push to all subscriptions (admin only)
  POST /send-test              → send to a single endpoint (admin only, for verify)

DynamoDB:  justhodl-push-subscriptions   PK=endpoint_hash (sha256 of endpoint, 32 hex chars)

VAPID keys (P-256 ECDSA) in SSM:
  /justhodl/push/vapid-public-key   plain — base64url(04 || X || Y), ~87 chars
  /justhodl/push/vapid-private-key  SecureString — 64-char hex (32 bytes)
  /justhodl/push/vapid-subject      plain — mailto:contact@justhodl.ai
  /justhodl/push/admin-token        SecureString — 32-char random hex

NO external Python deps — uses only stdlib for ES256 signing
(pure-Python P-256 ECDSA implementation below).
"""
import json
import os
import time
import hashlib
import hmac
import base64
import secrets
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

DDB = boto3.resource("dynamodb", region_name="us-east-1")
SSM = boto3.client("ssm", region_name="us-east-1")
TABLE = DDB.Table(os.environ.get("DDB_TABLE", "justhodl-push-subscriptions"))

ALLOWED_ORIGINS = {"https://justhodl.ai", "https://www.justhodl.ai"}

# ─── Pure-Python P-256 ECDSA (FIPS 186-4, SEC1) ───
P256_P  = 0xffffffff00000001000000000000000000000000ffffffffffffffffffffffff
P256_N  = 0xffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632551
P256_A  = P256_P - 3
P256_GX = 0x6b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296
P256_GY = 0x4fe342e2fe1a7f9b8ee7eb4a7c0f9e162bce33576b315ececbb6406837bf51f5

def _ec_add(P1, P2):
    if P1 is None: return P2
    if P2 is None: return P1
    x1, y1 = P1
    x2, y2 = P2
    if x1 == x2:
        if (y1 + y2) % P256_P == 0:
            return None
        m = (3 * x1 * x1 + P256_A) * pow(2 * y1, -1, P256_P) % P256_P
    else:
        m = (y2 - y1) * pow(x2 - x1, -1, P256_P) % P256_P
    x3 = (m * m - x1 - x2) % P256_P
    y3 = (m * (x1 - x3) - y1) % P256_P
    return (x3, y3)

def _ec_mul(k, P):
    if k == 0 or P is None:
        return None
    k = k % P256_N
    result = None
    addend = P
    while k:
        if k & 1:
            result = _ec_add(result, addend)
        addend = _ec_add(addend, addend)
        k >>= 1
    return result

def ecdsa_sign(message_hash_int, priv_int):
    while True:
        k = secrets.randbelow(P256_N - 1) + 1
        point = _ec_mul(k, (P256_GX, P256_GY))
        if point is None:
            continue
        r = point[0] % P256_N
        if r == 0:
            continue
        k_inv = pow(k, -1, P256_N)
        s = (k_inv * (message_hash_int + r * priv_int)) % P256_N
        if s == 0:
            continue
        return r, s

def derive_public_key(priv_int):
    point = _ec_mul(priv_int, (P256_GX, P256_GY))
    if point is None:
        raise ValueError("Invalid private key — produces point at infinity")
    x, y = point
    return b"\x04" + x.to_bytes(32, "big") + y.to_bytes(32, "big")

# ─── Cached VAPID keys ───
_VAPID = {"public_b64": None, "priv_int": None, "subject": "mailto:contact@justhodl.ai"}

def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

def _b64url_decode(s: str) -> bytes:
    pad = "=" * ((4 - len(s) % 4) % 4)
    return base64.urlsafe_b64decode(s + pad)

def _load_vapid():
    if _VAPID["public_b64"] and _VAPID["priv_int"]:
        return
    try:
        _VAPID["public_b64"] = SSM.get_parameter(Name="/justhodl/push/vapid-public-key")["Parameter"]["Value"]
        priv_hex = SSM.get_parameter(Name="/justhodl/push/vapid-private-key", WithDecryption=True)["Parameter"]["Value"]
        _VAPID["priv_int"] = int(priv_hex, 16)
        try:
            _VAPID["subject"] = SSM.get_parameter(Name="/justhodl/push/vapid-subject")["Parameter"]["Value"]
        except ClientError:
            pass
    except ClientError as e:
        print(f"[push-api] WARN: VAPID keys not in SSM: {e}")

# ─── Helpers ───
def _hash_endpoint(endpoint: str) -> str:
    return hashlib.sha256(endpoint.encode("utf-8")).hexdigest()[:32]

def _cors_headers(origin):
    allow = origin if origin in ALLOWED_ORIGINS else "https://justhodl.ai"
    return {
        "Access-Control-Allow-Origin": allow,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, X-Justhodl-Admin-Token",
        "Access-Control-Max-Age": "3600",
        "Content-Type": "application/json",
    }

def _resp(status, body, origin=None):
    return {"statusCode": status, "headers": _cors_headers(origin), "body": json.dumps(body, default=str)}

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _verify_admin(headers):
    token = headers.get("x-justhodl-admin-token")
    if not token:
        return False
    try:
        expected = SSM.get_parameter(Name="/justhodl/push/admin-token", WithDecryption=True)["Parameter"]["Value"]
    except ClientError:
        return False
    return hmac.compare_digest(token, expected)

# ─── VAPID JWT ───
def _generate_vapid_auth(audience):
    _load_vapid()
    if not _VAPID["priv_int"] or not _VAPID["public_b64"]:
        return None
    header = {"typ": "JWT", "alg": "ES256"}
    claims = {"aud": audience, "exp": int(time.time()) + 12 * 3600, "sub": _VAPID["subject"]}
    h_b64 = _b64url_encode(json.dumps(header, separators=(",", ":")).encode())
    c_b64 = _b64url_encode(json.dumps(claims, separators=(",", ":")).encode())
    signing_input = f"{h_b64}.{c_b64}".encode("ascii")
    msg_int = int.from_bytes(hashlib.sha256(signing_input).digest(), "big")
    r, s = ecdsa_sign(msg_int, _VAPID["priv_int"])
    sig = r.to_bytes(32, "big") + s.to_bytes(32, "big")
    jwt = f"{h_b64}.{c_b64}.{_b64url_encode(sig)}"
    return f"vapid t={jwt},k={_VAPID['public_b64']}"

# ─── Endpoint handlers ───
def handle_vapid_public_key(origin):
    _load_vapid()
    if not _VAPID["public_b64"]:
        return _resp(503, {"error": "VAPID keys not configured. Run ops/pending/359_pwa_setup.py first."}, origin)
    return _resp(200, {"publicKey": _VAPID["public_b64"]}, origin)

def handle_subscribe(body, origin):
    sub = body.get("subscription") or {}
    endpoint = sub.get("endpoint")
    keys = sub.get("keys") or {}
    p256dh = keys.get("p256dh")
    auth = keys.get("auth")
    if not endpoint or not p256dh or not auth:
        return _resp(400, {"error": "Subscription missing endpoint, p256dh, or auth"}, origin)
    item = {
        "endpoint_hash": _hash_endpoint(endpoint),
        "endpoint": endpoint, "p256dh": p256dh, "auth": auth,
        "user_agent": (body.get("user_agent") or "")[:300],
        "subscribed_at": _now_iso(),
        "push_count": 0,
    }
    try:
        TABLE.put_item(Item=item)
    except ClientError as e:
        return _resp(500, {"error": "store_failed", "detail": str(e)}, origin)
    return _resp(200, {"ok": True, "endpoint_hash": item["endpoint_hash"]}, origin)

def handle_unsubscribe(body, origin):
    endpoint = body.get("endpoint")
    if not endpoint:
        return _resp(400, {"error": "Missing endpoint"}, origin)
    try:
        TABLE.delete_item(Key={"endpoint_hash": _hash_endpoint(endpoint)})
    except ClientError as e:
        return _resp(500, {"error": "delete_failed", "detail": str(e)}, origin)
    return _resp(200, {"ok": True}, origin)

def _send_to_subscription(item, payload):
    endpoint = item["endpoint"]
    parsed = urllib.parse.urlparse(endpoint)
    aud = f"{parsed.scheme}://{parsed.netloc}"
    auth_header = _generate_vapid_auth(aud)
    if not auth_header:
        return False, 0
    body_bytes = json.dumps(payload).encode("utf-8")
    headers = {
        "Authorization": auth_header,
        "TTL": "86400",
        "Urgency": "normal",
        "Content-Type": "application/octet-stream",
        "Content-Encoding": "aes128gcm",
        "Content-Length": str(len(body_bytes)),
    }
    try:
        req = urllib.request.Request(endpoint, data=body_bytes, method="POST", headers=headers)
        with urllib.request.urlopen(req, timeout=10) as r:
            return True, r.status
    except urllib.error.HTTPError as e:
        return False, e.code
    except Exception as e:
        print(f"[push-api] Send error: {type(e).__name__}: {e}")
        return False, 0

def handle_send(body, headers, origin):
    if not _verify_admin(headers):
        return _resp(401, {"error": "unauthorized"}, origin)
    payload = {"title": body.get("title") or "JustHodl alert", "body": body.get("body") or "", "url": body.get("url") or "/"}
    for opt in ("tag", "icon", "requireInteraction"):
        if opt in body:
            payload[opt] = body[opt]
    sent = failed = removed = 0
    last_evaluated = None
    while True:
        kwargs = {"Limit": 100}
        if last_evaluated:
            kwargs["ExclusiveStartKey"] = last_evaluated
        page = TABLE.scan(**kwargs)
        for item in page.get("Items", []):
            ok, status = _send_to_subscription(item, payload)
            if ok:
                sent += 1
                try:
                    TABLE.update_item(
                        Key={"endpoint_hash": item["endpoint_hash"]},
                        UpdateExpression="SET last_push_at=:t ADD push_count :one",
                        ExpressionAttributeValues={":t": _now_iso(), ":one": 1},
                    )
                except ClientError:
                    pass
            else:
                failed += 1
                if status in (404, 410):
                    try:
                        TABLE.delete_item(Key={"endpoint_hash": item["endpoint_hash"]})
                        removed += 1
                    except ClientError:
                        pass
        last_evaluated = page.get("LastEvaluatedKey")
        if not last_evaluated:
            break
    return _resp(200, {"ok": True, "sent": sent, "failed": failed, "removed_dead": removed,
                      "title": payload["title"], "body": payload["body"][:120]}, origin)

def handle_send_test(body, headers, origin):
    if not _verify_admin(headers):
        return _resp(401, {"error": "unauthorized"}, origin)
    endpoint_hash = body.get("endpoint_hash")
    if not endpoint_hash:
        return _resp(400, {"error": "Missing endpoint_hash"}, origin)
    try:
        item = TABLE.get_item(Key={"endpoint_hash": endpoint_hash}).get("Item")
    except ClientError as e:
        return _resp(500, {"error": str(e)}, origin)
    if not item:
        return _resp(404, {"error": "subscription not found"}, origin)
    payload = {"title": body.get("title", "JustHodl test"), "body": body.get("body", "Verify push pipeline"), "url": "/"}
    ok, status = _send_to_subscription(item, payload)
    return _resp(200, {"ok": ok, "http_status": status}, origin)

def lambda_handler(event, context):
    method = (event.get("requestContext", {}).get("http", {}).get("method") or event.get("httpMethod") or "GET").upper()
    raw_path = event.get("rawPath") or event.get("path") or "/"
    headers_in = {(k or "").lower(): v for k, v in (event.get("headers") or {}).items()}
    origin = headers_in.get("origin") or ""
    if method == "OPTIONS":
        return _resp(200, {"ok": True}, origin)
    body = {}
    raw_body = event.get("body")
    if raw_body:
        try:
            if event.get("isBase64Encoded"):
                raw_body = base64.b64decode(raw_body).decode("utf-8")
            body = json.loads(raw_body)
        except Exception:
            body = {}
    if raw_path.endswith("/vapid-public-key") and method == "GET":
        return handle_vapid_public_key(origin)
    if raw_path.endswith("/subscribe") and method == "POST":
        return handle_subscribe(body, origin)
    if raw_path.endswith("/unsubscribe") and method == "POST":
        return handle_unsubscribe(body, origin)
    if raw_path.endswith("/send") and method == "POST":
        return handle_send(body, headers_in, origin)
    if raw_path.endswith("/send-test") and method == "POST":
        return handle_send_test(body, headers_in, origin)
    if raw_path == "/" or raw_path == "":
        _load_vapid()
        return _resp(200, {
            "service": "justhodl-push-api", "version": "1.0",
            "endpoints": ["/vapid-public-key", "/subscribe", "/unsubscribe", "/send", "/send-test"],
            "vapid_configured": bool(_VAPID.get("public_b64")),
        }, origin)
    return _resp(404, {"error": "not_found", "path": raw_path}, origin)
