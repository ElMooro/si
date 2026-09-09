"""
justhodl-portfolio-admin — Roadmap #9 portfolio CRUD

═══════════════════════════════════════════════════════════════════════
COMMAND-DRIVEN POSITION MANAGEMENT
─────────────────────────────────────
Invoked manually (no schedule) to add/remove/update positions. Event
payload specifies action + parameters. Returns JSON result.

Actions:
  add_position       symbol, qty, cost_basis_per_share, [stop_loss, target_weight_pct, sector, notes]
  remove_position    symbol
  update_position    symbol, [new_qty, new_cost_basis_per_share, new_stop_loss, new_target_weight, new_notes]
  set_stop_loss      symbol, stop_price
  list               filter: "POSITION" | "WATCHLIST" | "STOPLOSS" | "ALL"
  add_watchlist      symbol, source ("MANUAL" default)
  remove_watchlist   symbol
  clear_auto_watchlist (removes all AUTO_TIER_S/A entries — for clean re-sync)

Example invoke payload:
  {"action": "add_position", "symbol": "LLY", "qty": 50,
   "cost_basis_per_share": 925.00, "stop_loss": 890,
   "target_weight_pct": 7, "sector": "Healthcare",
   "notes": "Daily brief 2026-05-12 LONG recommendation"}

═══════════════════════════════════════════════════════════════════════
"""
import json
from decimal import Decimal
from datetime import datetime, timezone

import boto3

TABLE_NAME = "justhodl-portfolio"
ddb = boto3.resource("dynamodb", region_name="us-east-1")
table = ddb.Table(TABLE_NAME)

# ── auth + pipeline plumbing (Function URL path) ──
import base64
TOKEN_SSM_NAME = "/justhodl/portfolio-admin/token"
ALLOWED_ORIGINS = {"https://justhodl.ai", "https://www.justhodl.ai"}
SNAPSHOT_FN = "justhodl-portfolio-snapshot"
# mutations to the book that should refresh the downstream snapshot
_MUTATING = {"add_position", "remove_position", "update_position",
             "set_stop_loss"}
_ssm = boto3.client("ssm", region_name="us-east-1")
_lam = boto3.client("lambda", region_name="us-east-1")
_token_cache = {"v": None}


def _admin_token():
    """SSM SecureString token, cached for the warm container lifetime."""
    if _token_cache["v"] is None:
        _token_cache["v"] = _ssm.get_parameter(
            Name=TOKEN_SSM_NAME, WithDecryption=True)["Parameter"]["Value"]
    return _token_cache["v"]


def _trigger_snapshot():
    """Fire-and-forget refresh of the portfolio snapshot after a book edit."""
    try:
        _lam.invoke(FunctionName=SNAPSHOT_FN, Qualifier="live", InvocationType="Event",
                    Payload=b"{}")
    except Exception as e:  # never let a refresh failure break the write
        print(f"[portfolio-admin] snapshot trigger failed: {e}")


def _dec(v):
    """Safely convert any numeric to Decimal (DDB requirement)."""
    if v is None: return None
    return Decimal(str(v))


def _scrub(item):
    """Convert Decimal back to float for JSON output."""
    if isinstance(item, list):
        return [_scrub(i) for i in item]
    if isinstance(item, dict):
        return {k: _scrub(v) for k, v in item.items()}
    if isinstance(item, Decimal):
        return float(item)
    return item


def add_position(event):
    sym = event["symbol"].upper().strip()
    qty = float(event["qty"])
    cost = float(event["cost_basis_per_share"])
    item = {
        "pk": "POSITION", "sk": sym, "symbol": sym,
        "qty": _dec(qty),
        "cost_basis_per_share": _dec(cost),
        "cost_basis_total": _dec(qty * cost),
        "position_type": "LONG" if qty >= 0 else "SHORT",
        "added_at": datetime.now(timezone.utc).isoformat(),
    }
    for opt_key, opt_type in [
        ("stop_loss", float), ("target_weight_pct", float),
        ("sector", str), ("notes", str), ("entry_thesis", str),
    ]:
        v = event.get(opt_key)
        if v is not None and v != "":
            item[opt_key] = _dec(v) if opt_type is float else str(v)
    table.put_item(Item=item)
    return {"ok": True, "action": "add_position", "item": _scrub(item)}


def remove_position(event):
    sym = event["symbol"].upper().strip()
    resp = table.delete_item(
        Key={"pk": "POSITION", "sk": sym},
        ReturnValues="ALL_OLD",
    )
    removed = resp.get("Attributes")
    return {"ok": True, "action": "remove_position", "symbol": sym,
            "existed": bool(removed),
            "removed_item": _scrub(removed) if removed else None}


def _finite(v, name):
    """audit 2026-09-08 INST-07: numeric edits must be finite numbers."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        raise ValueError("%s must be a number (got %r)" % (name, v))
    if f != f or f in (float("inf"), float("-inf")):
        raise ValueError("%s must be finite" % name)
    return f


def update_position(event):
    sym = event["symbol"].upper().strip()
    # audit 2026-09-08 INST-07: read-validate-write. A quantity-only or cost-only edit used to leave
    # cost_basis_total stale (10 sh @ $100 -> 20 sh kept a $1,000 total = a fabricated $1,000 gain), and
    # position_type was fixed at creation (a flip to -20 stayed LONG, selecting the wrong stop rule).
    # Every dependent field is recomputed from the merged state, the write is conditioned on the item
    # still being the one we read (optimistic concurrency), and upserts to absent positions are refused.
    try:
        cur = table.get_item(Key={"pk": "POSITION", "sk": sym}).get("Item")
    except Exception as e:
        return {"ok": False, "err": "read failed: %s" % str(e)[:120]}
    if not cur:
        return {"ok": False, "err": "position %s does not exist -- use add_position" % sym}
    try:
        new_qty = _finite(event["qty"], "qty") if event.get("qty") is not None else float(cur.get("qty") or 0)
        new_cost = _finite(event["cost_basis_per_share"], "cost_basis_per_share") if event.get("cost_basis_per_share") is not None else float(cur.get("cost_basis_per_share") or 0)
        for k in ("stop_loss", "target_weight_pct"):
            if event.get(k) is not None:
                _finite(event[k], k)
    except ValueError as e:
        return {"ok": False, "err": str(e)}
    # Build UpdateExpression dynamically
    updates, values, names = [], {}, {}
    field_map = {
        "qty": ("qty", float),
        "cost_basis_per_share": ("cost_basis_per_share", float),
        "stop_loss": ("stop_loss", float),
        "target_weight_pct": ("target_weight_pct", float),
        "sector": ("sector", str),
        "notes": ("notes", str),
    }
    for event_key, (attr_name, attr_type) in field_map.items():
        v = event.get(event_key)
        if v is None: continue
        placeholder = f":{event_key}"
        name_alias = f"#{event_key}"
        updates.append(f"{name_alias} = {placeholder}")
        values[placeholder] = _dec(v) if attr_type is float else str(v)
        names[name_alias] = attr_name

    # Recompute EVERY dependent field whenever either input changed (partial edits included)
    if event.get("qty") is not None or event.get("cost_basis_per_share") is not None:
        updates.append("#cbt = :cbt")
        values[":cbt"] = _dec(new_qty * new_cost)
        names["#cbt"] = "cost_basis_total"
        side = "LONG" if new_qty >= 0 else "SHORT"
        if side != cur.get("position_type"):
            updates.append("#pt = :pt")
            values[":pt"] = side
            names["#pt"] = "position_type"

    if not updates:
        return {"ok": False, "err": "No update fields provided"}

    updates.append("#u = :u")
    values[":u"] = datetime.now(timezone.utc).isoformat()
    names["#u"] = "updated_at"

    # optimistic concurrency: the item must still carry the qty/cost/updated_at we based the merge on
    names["#cq"] = "qty"
    values[":cq"] = cur.get("qty")
    cond = "attribute_exists(pk) AND #cq = :cq"
    if cur.get("updated_at") is not None:
        names["#cu"] = "updated_at"
        values[":cu"] = cur.get("updated_at")
        cond += " AND #cu = :cu"
    try:
        resp = table.update_item(
            Key={"pk": "POSITION", "sk": sym},
            UpdateExpression="SET " + ", ".join(updates),
            ConditionExpression=cond,
            ExpressionAttributeValues=values,
            ExpressionAttributeNames=names,
            ReturnValues="ALL_NEW",
        )
    except Exception as e:
        if "ConditionalCheckFailed" in str(e):
            return {"ok": False, "err": "concurrent edit detected for %s -- re-read and retry" % sym}
        raise
    return {"ok": True, "action": "update_position",
             "updated": _scrub(resp.get("Attributes"))}


def set_stop_loss(event):
    sym = event["symbol"].upper().strip()
    stop = float(event["stop_price"])
    resp = table.update_item(
        Key={"pk": "POSITION", "sk": sym},
        UpdateExpression="SET stop_loss = :s, updated_at = :u",
        ExpressionAttributeValues={":s": _dec(stop),
                                     ":u": datetime.now(timezone.utc).isoformat()},
        ReturnValues="ALL_NEW",
    )
    return {"ok": True, "action": "set_stop_loss", "symbol": sym,
            "stop_price": stop, "updated": _scrub(resp.get("Attributes"))}


def add_watchlist(event):
    sym = event["symbol"].upper().strip()
    source = event.get("source", "MANUAL")
    item = {
        "pk": "WATCHLIST", "sk": sym, "symbol": sym,
        "source": source,
        "added_at": datetime.now(timezone.utc).isoformat(),
    }
    if event.get("notes"): item["notes"] = str(event["notes"])
    table.put_item(Item=item)
    return {"ok": True, "action": "add_watchlist", "item": _scrub(item)}


def remove_watchlist(event):
    sym = event["symbol"].upper().strip()
    resp = table.delete_item(
        Key={"pk": "WATCHLIST", "sk": sym},
        ReturnValues="ALL_OLD",
    )
    return {"ok": True, "action": "remove_watchlist", "symbol": sym,
            "existed": bool(resp.get("Attributes"))}


def clear_auto_watchlist(event):
    """Delete all AUTO_TIER_S and AUTO_TIER_A watchlist entries.
    Used before snapshot Lambda re-syncs from current alpha-score."""
    resp = table.query(
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": "WATCHLIST"},
    )
    deleted = []
    with table.batch_writer() as batch:
        for item in resp.get("Items", []):
            if item.get("source", "MANUAL").startswith("AUTO_"):
                batch.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})
                deleted.append(item["symbol"])
    return {"ok": True, "action": "clear_auto_watchlist",
            "deleted_count": len(deleted), "deleted_symbols": deleted}


def list_items(event):
    filt = (event.get("filter") or "ALL").upper()
    out = {"positions": [], "watchlist": [], "stoploss": [], "meta": []}
    if filt in ("POSITION", "ALL"):
        r = table.query(KeyConditionExpression="pk = :pk",
                          ExpressionAttributeValues={":pk": "POSITION"})
        out["positions"] = [_scrub(i) for i in r.get("Items", [])]
    if filt in ("WATCHLIST", "ALL"):
        r = table.query(KeyConditionExpression="pk = :pk",
                          ExpressionAttributeValues={":pk": "WATCHLIST"})
        out["watchlist"] = [_scrub(i) for i in r.get("Items", [])]
    if filt in ("STOPLOSS", "ALL"):
        r = table.query(KeyConditionExpression="pk = :pk",
                          ExpressionAttributeValues={":pk": "STOPLOSS"})
        out["stoploss"] = [_scrub(i) for i in r.get("Items", [])]
    if filt == "ALL":
        r = table.query(KeyConditionExpression="pk = :pk",
                          ExpressionAttributeValues={":pk": "META"})
        out["meta"] = [_scrub(i) for i in r.get("Items", [])]
    out["counts"] = {k: len(v) for k, v in out.items() if isinstance(v, list)}
    out["ok"] = True
    return out


ACTIONS = {
    "add_position":         add_position,
    "remove_position":      remove_position,
    "update_position":      update_position,
    "set_stop_loss":        set_stop_loss,
    "add_watchlist":        add_watchlist,
    "remove_watchlist":     remove_watchlist,
    "clear_auto_watchlist": clear_auto_watchlist,
    "list":                 list_items,
}


def _dispatch(payload):
    """Run one action. `payload` is a dict with `action` + parameters.

    Returns (status_code, result_dict). Used by both the direct-invoke
    path (ops scripts / CLI) and the authenticated Function URL path.
    """
    action = (payload or {}).get("action")
    if not action:
        return 400, {"ok": False, "err": "missing action",
                     "available_actions": list(ACTIONS.keys())}
    handler = ACTIONS.get(action)
    if not handler:
        return 400, {"ok": False, "err": f"unknown action: {action}",
                     "available_actions": list(ACTIONS.keys())}
    try:
        result = handler(payload)
    except KeyError as e:
        return 400, {"ok": False, "err": f"missing parameter: {e}"}
    except Exception as e:
        return 500, {"ok": False,
                     "err": f"{type(e).__name__}: {str(e)[:300]}"}

    if action in _MUTATING and result.get("ok"):
        _trigger_snapshot()
        result["snapshot_refresh"] = "queued"
    return 200, result


def lambda_handler(event, context):
    """Dual-mode entrypoint.

    • Direct invoke (ops scripts / CLI, no HTTP context): the event IS the
      payload — runs un-gated, the caller is already inside AWS.
    • Function URL invoke (browser): requires the x-justhodl-token header to
      match SSM /justhodl/portfolio-admin/token and an allow-listed Origin.
      This endpoint mutates the book, so every call is gated.
    """
    rc = (event or {}).get("requestContext") or {}
    http = rc.get("http") or {}

    # ── direct invoke — unchanged legacy behaviour ──
    if not http:
        status, result = _dispatch(event or {})
        return {"statusCode": status,
                "body": json.dumps(result, default=str)}

    # ── Function URL invoke ──
    method = (http.get("method") or "").upper()
    if method == "OPTIONS":                       # CORS preflight
        return {"statusCode": 200, "body": ""}

    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}

    try:
        expected = _admin_token()
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps(
            {"ok": False, "err": f"auth config error: {str(e)[:160]}"})}

    if headers.get("x-justhodl-token") != expected:
        return {"statusCode": 403,
                "body": json.dumps({"ok": False, "err": "forbidden"})}

    origin = headers.get("origin")
    if origin and origin not in ALLOWED_ORIGINS:
        return {"statusCode": 403,
                "body": json.dumps({"ok": False, "err": "origin not allowed"})}

    raw = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode("utf-8", "replace")
    try:
        payload = json.loads(raw) if raw.strip() else {}
    except Exception:
        return {"statusCode": 400,
                "body": json.dumps({"ok": False, "err": "invalid JSON body"})}

    status, result = _dispatch(payload)
    return {"statusCode": status, "body": json.dumps(result, default=str)}
