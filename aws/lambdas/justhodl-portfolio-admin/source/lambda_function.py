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


def update_position(event):
    sym = event["symbol"].upper().strip()
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

    # If qty or cost_basis_per_share changed, recompute total
    if event.get("qty") is not None and event.get("cost_basis_per_share") is not None:
        total = float(event["qty"]) * float(event["cost_basis_per_share"])
        updates.append("#cbt = :cbt")
        values[":cbt"] = _dec(total)
        names["#cbt"] = "cost_basis_total"

    if not updates:
        return {"ok": False, "err": "No update fields provided"}

    updates.append("#u = :u")
    values[":u"] = datetime.now(timezone.utc).isoformat()
    names["#u"] = "updated_at"

    resp = table.update_item(
        Key={"pk": "POSITION", "sk": sym},
        UpdateExpression="SET " + ", ".join(updates),
        ExpressionAttributeValues=values,
        ExpressionAttributeNames=names,
        ReturnValues="ALL_NEW",
    )
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


def lambda_handler(event, context):
    action = (event or {}).get("action")
    if not action:
        return {"statusCode": 400, "body": json.dumps({
            "ok": False, "err": "missing action",
            "available_actions": list(ACTIONS.keys())})}

    handler = ACTIONS.get(action)
    if not handler:
        return {"statusCode": 400, "body": json.dumps({
            "ok": False, "err": f"unknown action: {action}",
            "available_actions": list(ACTIONS.keys())})}

    try:
        result = handler(event)
    except KeyError as e:
        return {"statusCode": 400, "body": json.dumps({
            "ok": False, "err": f"missing parameter: {e}"})}
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({
            "ok": False, "err": f"{type(e).__name__}: {str(e)[:300]}"})}

    return {"statusCode": 200, "body": json.dumps(result, default=str)}
