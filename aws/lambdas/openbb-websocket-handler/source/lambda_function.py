"""
openbb-websocket-handler — API Gateway WebSocket route handler.

Routes:
  $connect    — store {connectionId, connected_at, channels=[]} in DDB
  $disconnect — remove the connection
  $default    — handle subscribe / unsubscribe / ping client messages

Client message protocol (JSON):
  { "action": "subscribe",   "channels": ["report", "compound", "regime"] }
  { "action": "unsubscribe", "channels": ["report"] }
  { "action": "ping" }                              → server replies pong

DDB: WebSocketConnections (PK=connectionId)
  connected_at: ISO-8601
  channels:     SS — set of channel strings
  last_ping:    ISO-8601
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

DDB = boto3.resource("dynamodb", region_name="us-east-1")
TABLE = DDB.Table(os.environ.get("DDB_TABLE", "WebSocketConnections"))

# Default channels every new connection auto-subscribes to (so a vanilla
# client gets useful pushes without sending any message).
DEFAULT_CHANNELS = {"report", "regime", "alerts"}

# Valid channels the server is willing to broadcast on. Anything else is dropped.
ALLOWED_CHANNELS = {
    "report",                # data/report.json (every 5 min)
    "regime",                # data/macro-nowcast.json (regime change)
    "compound",              # data/compound-signals.json
    "cross_asset",           # data/cross-asset-regime.json
    "options_flow",          # data/options-flow.json
    "eurodollar",            # data/eurodollar-stress.json
    "nobrainers",            # data/nobrainers.json
    "narrative",             # data/narrative-density.json
    "alerts",                # any redflag-alerter / alert-router push
    "system",                # health, deployments, system-level events
}


def _now():
    return datetime.now(timezone.utc).isoformat()


def _resp(status, body=None):
    return {"statusCode": status, "body": json.dumps(body) if body is not None else ""}


def _post_to_connection(api_id, region, stage, connection_id, payload):
    """Push a message back to the client mid-handshake (used for ack)."""
    endpoint = f"https://{api_id}.execute-api.{region}.amazonaws.com/{stage}"
    client = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint, region_name=region)
    try:
        client.post_to_connection(ConnectionId=connection_id, Data=json.dumps(payload).encode("utf-8"))
    except Exception as e:
        print(f"[handler] post_to_connection failed: {e}")


def lambda_handler(event, context):
    rc = event.get("requestContext", {})
    route = rc.get("routeKey")
    connection_id = rc.get("connectionId")
    api_id = rc.get("apiId")
    stage = rc.get("stage")
    region = os.environ.get("AWS_REGION", "us-east-1")

    if not connection_id:
        return _resp(400, {"error": "no connectionId"})

    if route == "$connect":
        try:
            TABLE.put_item(Item={
                "connectionId": connection_id,
                "connected_at": _now(),
                "channels": list(DEFAULT_CHANNELS),
                "last_ping": _now(),
                "user_agent": (event.get("headers") or {}).get("User-Agent", "")[:200],
            })
            print(f"[handler] $connect {connection_id} → channels={DEFAULT_CHANNELS}")
        except ClientError as e:
            print(f"[handler] DDB put failed: {e}")
            return _resp(500)
        return _resp(200)

    if route == "$disconnect":
        try:
            TABLE.delete_item(Key={"connectionId": connection_id})
            print(f"[handler] $disconnect {connection_id}")
        except ClientError as e:
            print(f"[handler] DDB delete failed: {e}")
        return _resp(200)

    if route == "$default":
        # Parse client message
        body = {}
        try:
            body = json.loads(event.get("body") or "{}")
        except Exception:
            pass
        action = (body.get("action") or "").lower()

        if action == "ping":
            try:
                TABLE.update_item(
                    Key={"connectionId": connection_id},
                    UpdateExpression="SET last_ping=:t",
                    ExpressionAttributeValues={":t": _now()},
                )
            except ClientError:
                pass
            _post_to_connection(api_id, region, stage, connection_id,
                                {"action": "pong", "server_time": _now()})
            return _resp(200)

        if action in ("subscribe", "unsubscribe"):
            requested = body.get("channels") or []
            if not isinstance(requested, list):
                _post_to_connection(api_id, region, stage, connection_id,
                                    {"action": "error", "message": "channels must be a list"})
                return _resp(400)
            valid = [c for c in requested if c in ALLOWED_CHANNELS]
            try:
                # Read current
                cur = TABLE.get_item(Key={"connectionId": connection_id}).get("Item", {})
                current_channels = set(cur.get("channels") or [])
                if action == "subscribe":
                    new_channels = current_channels | set(valid)
                else:
                    new_channels = current_channels - set(valid)
                TABLE.update_item(
                    Key={"connectionId": connection_id},
                    UpdateExpression="SET channels=:c, last_ping=:t",
                    ExpressionAttributeValues={":c": list(new_channels), ":t": _now()},
                )
                _post_to_connection(api_id, region, stage, connection_id, {
                    "action": "subscribed" if action == "subscribe" else "unsubscribed",
                    "channels": sorted(new_channels),
                    "applied": valid,
                    "rejected": [c for c in requested if c not in ALLOWED_CHANNELS],
                })
            except ClientError as e:
                _post_to_connection(api_id, region, stage, connection_id,
                                    {"action": "error", "message": str(e)})
                return _resp(500)
            return _resp(200)

        # Unknown action
        _post_to_connection(api_id, region, stage, connection_id, {
            "action": "error",
            "message": f"unknown action '{action}'. Valid: subscribe, unsubscribe, ping",
            "allowed_channels": sorted(ALLOWED_CHANNELS),
        })
        return _resp(400)

    return _resp(400, {"error": f"unknown route '{route}'"})
