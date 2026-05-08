"""
openbb-websocket-broadcast — broadcast messages to all WebSocket subscribers.

Two invocation modes:

  A) S3 event (ObjectCreated:*) — automatic broadcast when a tracked S3 key
     updates. Maps S3 key → channel; reads new object content; pushes a
     concise summary to all subscribers of that channel.

  B) HTTP POST via Lambda Function URL (admin-only, for ad-hoc broadcasts):
       POST / { "channel": "alerts", "title": "...", "body": "...", "data": {...} }
       Header: X-Justhodl-Admin-Token: <SSM /justhodl/push/admin-token>

For each broadcast: scan WebSocketConnections, filter by channel, post via
ApiGatewayManagementApi to each connectionId. Removes 410-Gone connections
from DDB (client disconnected without $disconnect firing).
"""
import json
import os
import time
import hmac
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

DDB = boto3.resource("dynamodb", region_name="us-east-1")
SSM = boto3.client("ssm", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
TABLE = DDB.Table(os.environ.get("DDB_TABLE", "WebSocketConnections"))
WS_API_ID = os.environ.get("WS_API_ID", "")
WS_STAGE = os.environ.get("WS_STAGE", "prod")
REGION = os.environ.get("AWS_REGION", "us-east-1")
ALLOWED_ORIGINS = {"https://justhodl.ai", "https://www.justhodl.ai"}

# Map S3 key prefix → broadcast channel
KEY_TO_CHANNEL = {
    "data/report.json": "report",
    "data/macro-nowcast.json": "regime",
    "data/compound-signals.json": "compound",
    "data/cross-asset-regime.json": "cross_asset",
    "data/options-flow.json": "options_flow",
    "data/eurodollar-stress.json": "eurodollar",
    "data/nobrainers.json": "nobrainers",
    "data/narrative-density.json": "narrative",
}


def _now():
    return datetime.now(timezone.utc).isoformat()


def _cors_headers(origin):
    allow = origin if origin in ALLOWED_ORIGINS else "https://justhodl.ai"
    return {
        "Access-Control-Allow-Origin": allow,
        "Access-Control-Allow-Methods": "GET, POST",
        "Access-Control-Allow-Headers": "Content-Type, X-Justhodl-Admin-Token",
        "Content-Type": "application/json",
    }


def _resp(status, body, origin=None):
    return {"statusCode": status, "headers": _cors_headers(origin), "body": json.dumps(body, default=str)}


def _verify_admin(headers):
    token = (headers or {}).get("x-justhodl-admin-token")
    if not token:
        return False
    try:
        expected = SSM.get_parameter(Name="/justhodl/push/admin-token", WithDecryption=True)["Parameter"]["Value"]
    except ClientError:
        return False
    return hmac.compare_digest(token, expected)


def _broadcast(channel, payload):
    """Push payload to every connection subscribed to channel."""
    if not WS_API_ID:
        return {"error": "WS_API_ID not configured"}
    endpoint = f"https://{WS_API_ID}.execute-api.{REGION}.amazonaws.com/{WS_STAGE}"
    client = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint, region_name=REGION)

    msg = json.dumps({
        "action": "push",
        "channel": channel,
        "ts": _now(),
        "payload": payload,
    }).encode("utf-8")

    sent = failed = removed = total = 0
    last_evaluated = None
    while True:
        kwargs = {"Limit": 200}
        if last_evaluated:
            kwargs["ExclusiveStartKey"] = last_evaluated
        page = TABLE.scan(**kwargs)
        for item in page.get("Items", []):
            total += 1
            channels = set(item.get("channels") or [])
            if channel not in channels:
                continue
            cid = item["connectionId"]
            try:
                client.post_to_connection(ConnectionId=cid, Data=msg)
                sent += 1
            except client.exceptions.GoneException:
                # Client disconnected without $disconnect firing
                try:
                    TABLE.delete_item(Key={"connectionId": cid})
                    removed += 1
                except ClientError:
                    pass
                failed += 1
            except Exception as e:
                print(f"[broadcast] send failed for {cid}: {e}")
                failed += 1
        last_evaluated = page.get("LastEvaluatedKey")
        if not last_evaluated:
            break

    return {"channel": channel, "sent": sent, "failed": failed, "removed_dead": removed, "scanned": total}


def _build_summary(key, body_bytes):
    """For each tracked key, build a small summary dict to push (full payload in S3)."""
    try:
        j = json.loads(body_bytes.decode("utf-8"))
    except Exception:
        return {"updated": True}

    if key == "data/report.json":
        ki = j.get("khalid_index") or {}
        regime = j.get("regime") or {}
        return {
            "updated": True,
            "khalid_index": ki.get("score") if isinstance(ki, dict) else ki,
            "regime": regime.get("current") if isinstance(regime, dict) else regime,
            "as_of": j.get("as_of") or j.get("generated_at"),
        }
    if key == "data/macro-nowcast.json":
        return {
            "updated": True,
            "regime": (j.get("current_regime") or {}).get("regime") if isinstance(j.get("current_regime"), dict) else j.get("regime"),
            "confidence": (j.get("current_regime") or {}).get("confidence"),
            "as_of": j.get("generated_at"),
        }
    if key == "data/compound-signals.json":
        ranked = j.get("ranked") or []
        return {
            "updated": True,
            "n_signals": len(ranked),
            "top": [{"symbol": r.get("symbol"), "score": r.get("score")} for r in ranked[:5]],
            "as_of": j.get("generated_at"),
        }
    if key == "data/cross-asset-regime.json":
        return {
            "updated": True,
            "regime_5d": (j.get("regime_5d") or {}).get("regime"),
            "regime_20d": (j.get("regime_20d") or {}).get("regime"),
            "regime_60d": (j.get("regime_60d") or {}).get("regime"),
            "as_of": j.get("generated_at"),
        }
    if key == "data/options-flow.json":
        return {
            "updated": True,
            "n_tier_a": (j.get("stats") or {}).get("n_tier_a"),
            "top_5": [{"symbol": r.get("symbol"), "score": r.get("score")}
                      for r in (j.get("summary") or {}).get("top_25_overall", [])[:5]],
            "as_of": j.get("generated_at"),
        }
    if key == "data/eurodollar-stress.json":
        return {
            "updated": True,
            "stress_score": j.get("stress_score") or j.get("composite_score"),
            "as_of": j.get("generated_at") or j.get("as_of"),
        }
    if key == "data/nobrainers.json":
        ta = j.get("tier_a") or []
        return {"updated": True, "n_tier_a": len(ta) if isinstance(ta, list) else None, "as_of": j.get("generated_at")}
    if key == "data/narrative-density.json":
        return {
            "updated": True,
            "n_tier_a": (j.get("stats") or {}).get("n_tier_a"),
            "n_tier_b": (j.get("stats") or {}).get("n_tier_b"),
            "as_of": j.get("generated_at"),
        }
    return {"updated": True, "key": key}


def lambda_handler(event, context):
    """Dispatcher — figures out S3 vs HTTP."""
    # ─── S3 event ───
    if "Records" in event and event["Records"] and "s3" in (event["Records"][0] or {}):
        results = []
        for rec in event["Records"]:
            try:
                bucket = rec["s3"]["bucket"]["name"]
                key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
                channel = KEY_TO_CHANNEL.get(key)
                if not channel:
                    continue
                # Fetch the object to build a summary
                try:
                    obj = S3.get_object(Bucket=bucket, Key=key)
                    summary = _build_summary(key, obj["Body"].read())
                except Exception as e:
                    summary = {"updated": True, "key": key, "fetch_error": str(e)}
                summary["s3_key"] = key
                result = _broadcast(channel, summary)
                results.append(result)
            except Exception as e:
                results.append({"error": str(e), "key": rec.get("s3", {}).get("object", {}).get("key")})
        return {"statusCode": 200, "body": json.dumps({"s3_events_processed": len(results), "results": results})}

    # ─── HTTP request (Function URL) ───
    method = (event.get("requestContext", {}).get("http", {}).get("method") or event.get("httpMethod") or "GET").upper()
    headers = {(k or "").lower(): v for k, v in (event.get("headers") or {}).items()}
    origin = headers.get("origin") or ""

    if method == "OPTIONS":
        return _resp(200, {"ok": True}, origin)

    if method == "GET":
        # Status / health
        try:
            count = TABLE.scan(Select="COUNT").get("Count", 0)
        except Exception:
            count = -1
        return _resp(200, {
            "service": "openbb-websocket-broadcast",
            "version": "1.0",
            "ws_api_id": WS_API_ID or "(unset)",
            "stage": WS_STAGE,
            "connections": count,
            "channels": sorted(KEY_TO_CHANNEL.values()) + ["alerts", "system"],
            "tracked_s3_keys": sorted(KEY_TO_CHANNEL.keys()),
        }, origin)

    if method == "POST":
        if not _verify_admin(headers):
            return _resp(401, {"error": "unauthorized"}, origin)
        try:
            body = json.loads(event.get("body") or "{}")
        except Exception:
            return _resp(400, {"error": "invalid JSON"}, origin)
        channel = body.get("channel") or "alerts"
        payload = {k: v for k, v in body.items() if k != "channel"}
        if not payload:
            payload = {"updated": True, "ts": _now()}
        result = _broadcast(channel, payload)
        return _resp(200, result, origin)

    return _resp(405, {"error": "method not allowed"}, origin)
