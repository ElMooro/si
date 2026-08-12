"""justhodl-blackswan-watch v1.0.1 (ops 4623)

Khalid's TradingView "blackswan" watchlist, run as an institutional
TAIL-RISK CANARY STRIP — the correct mapping for a list with that
name: these are alarms, not expansion legs.

Sources (all already in the fleet, zero new fetchers):
  data/tv-watchlists.json      — list membership (extension-synced;
                                 name match prefers 'swan' per the
                                 ops-4062 audit-gate lesson)
  data/tradingview.json        — Metric Vault live values/series
  data/tv-workbench.json       — fallback per-watchlist vault mirror
  data/symbol-dictionary.json  — human names

Per symbol: latest value, day-over-day move, |z| of that move vs the
symbol's own recent history, and position in its 1y range. Alarm
states carry NO direction semantics — a black-swan strip cares about
ANY violent move; Khalid reads direction from the named row. Output:
data/blackswan-watch.json + a composite alarm the physical-economy
canary board consumes (observed tier, never load-bearing).
"""
import json
import math
import os
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/blackswan-watch.json"
s3 = boto3.client("s3")


def s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def find_swan_list(wl):
    """Walk any plausible watchlist container shape; prefer names
    containing 'swan' (the 4062 lesson), else 'black'."""
    cands = []

    def add(name, symbols):
        if name and symbols:
            cands.append((str(name), [str(x) for x in symbols
                                      if isinstance(x, str)]))

    def syms_of(v):
        if isinstance(v, list):
            out = []
            for x in v:
                if isinstance(x, str):
                    out.append(x)
                elif isinstance(x, dict):
                    for k in ("symbol", "ticker", "s", "name"):
                        if isinstance(x.get(k), str):
                            out.append(x[k])
                            break
            return out
        if isinstance(v, dict):
            for k in ("symbols", "members", "tickers", "items"):
                if isinstance(v.get(k), list):
                    return syms_of(v[k])
        return []

    if isinstance(wl, dict):
        for k in ("watchlists", "lists", "data"):
            if isinstance(wl.get(k), dict):
                for name, v in wl[k].items():
                    add(name, syms_of(v))
            if isinstance(wl.get(k), list):
                for it in wl[k]:
                    if isinstance(it, dict):
                        add(it.get("name") or it.get("title")
                            or it.get("list"), syms_of(it))
        for name, v in wl.items():
            if isinstance(v, (list, dict)) and name not in (
                    "watchlists", "lists", "data"):
                add(name, syms_of(v))
    for pref in ("swan", "black"):
        for name, syms in cands:
            if pref in name.lower():
                return name, syms, len(cands)
    return None, [], len(cands)


def extract_series(node, depth=0):
    """Find a [{date-ish, value-ish}] series anywhere under node."""
    if depth > 4 or node is None:
        return None
    if isinstance(node, list) and node and isinstance(node[0], dict):
        dk = next((k for k in node[0]
                   if k in ("date", "d", "t", "time", "period")), None)
        vk = next((k for k in node[0]
                   if k in ("value", "v", "close", "c", "y")), None)
        if dk and vk:
            out = []
            for o in node[-400:]:
                try:
                    out.append({"date": str(o[dk])[:10],
                                "value": float(o[vk])})
                except Exception:
                    continue
            return out or None
    if isinstance(node, dict):
        for k in ("series", "history", "points", "values", "obs",
                  "data"):
            r = extract_series(node.get(k), depth + 1)
            if r:
                return r
        for v in node.values():
            r = extract_series(v, depth + 1)
            if r:
                return r
    return None


def extract_latest(node, depth=0):
    if depth > 4 or node is None:
        return None, None
    if isinstance(node, dict):
        lv = None
        for k in ("last", "value", "latest", "close", "price", "v"):
            if isinstance(node.get(k), (int, float)):
                lv = float(node[k])
                break
        ld = None
        for k in ("date", "last_date", "as_of", "updated", "t"):
            if node.get(k):
                ld = str(node[k])[:10]
                break
        if lv is not None:
            return lv, ld
        for v in node.values():
            r = extract_latest(v, depth + 1)
            if r[0] is not None:
                return r
    return None, None


_VIDX = {}


def vault_index(vault):
    """4623 lesson: vault 'symbols' is a LIST of row dicts, not a
    map. Index once: exact symbol + base name (prefix stripped)."""
    if _VIDX.get("done"):
        return _VIDX
    rows = (vault or {}).get("symbols")
    if isinstance(rows, list):
        for rr in rows:
            if not isinstance(rr, dict):
                continue
            sy = rr.get("symbol")
            if not isinstance(sy, str):
                continue
            _VIDX[sy] = rr
            if ":" in sy and "/" not in sy and "-" not in sy:
                _VIDX.setdefault(sy.split(":", 1)[1], rr)
    _VIDX["done"] = True
    return _VIDX


def vault_lookup(vault, wb, list_name, sym):
    idx = vault_index(vault)
    hit = idx.get(sym)
    if hit is None and ":" in sym and "/" not in sym \
            and "-" not in sym:
        hit = idx.get(sym.split(":", 1)[1])
    if hit is not None:
        return hit
    if isinstance(wb, dict):
        wls = wb.get("watchlists") or wb.get("lists") or {}
        it = None
        if isinstance(wls, dict):
            it = wls.get(list_name)
        elif isinstance(wls, list):
            it = next((x for x in wls if isinstance(x, dict)
                       and (x.get("name") == list_name)), None)
        if isinstance(it, dict):
            for k in ("symbols", "members", "rows", "items"):
                rows = it.get(k)
                if isinstance(rows, list):
                    for rr in rows:
                        if isinstance(rr, dict) and rr.get(
                                "symbol") == sym:
                            return rr
                if isinstance(rows, dict) and sym in rows:
                    return rows[sym]
    return None


def dict_name(dic, sym):
    if isinstance(dic, dict):
        for k in ("symbols", "dictionary", "data"):
            m = dic.get(k)
            if isinstance(m, dict) and sym in m:
                e = m[sym]
                if isinstance(e, dict):
                    return (e.get("name") or e.get("title")
                            or e.get("full_name") or "")[:70]
                if isinstance(e, str):
                    return e[:70]
        if sym in dic and isinstance(dic[sym], (str, dict)):
            e = dic[sym]
            return (e if isinstance(e, str)
                    else (e.get("name") or e.get("title") or ""))[:70]
    return ""


def analyze(sym, node, name):
    se = extract_series(node)
    if se and len(se) >= 15:
        vals = [o["value"] for o in se]
        last, prev = vals[-1], vals[-2]
        dod = ((last / prev - 1) * 100) if prev else None
        rets = [(vals[i] / vals[i - 1] - 1) * 100
                for i in range(1, len(vals)) if vals[i - 1]]
        tail = rets[-90:]
        mu = sum(tail) / len(tail)
        sd = math.sqrt(sum((x - mu) ** 2 for x in tail)
                       / max(len(tail) - 1, 1))
        z = abs((dod - mu) / sd) if (sd and dod is not None) else None
        w = vals[-260:]
        lo, hi = min(w), max(w)
        pos = ((last - lo) / (hi - lo) * 100) if hi > lo else 50.0
        move_state = ("RED" if z is not None and z >= 3 else
                      "AMBER" if z is not None and z >= 2 else "CALM")
        range_state = ("EXTREME" if pos <= 5 or pos >= 95 else
                       "STRETCHED" if pos <= 12 or pos >= 88 else
                       "NORMAL")
        try:
            age = (datetime.now(timezone.utc)
                   - datetime.fromisoformat(
                       se[-1]["date"]).replace(
                       tzinfo=timezone.utc)).days
        except Exception:
            age = None
        return {"symbol": sym, "name": name, "resolved": True,
                "last": round(last, 4),
                "dod_pct": round(dod, 2) if dod is not None else None,
                "move_z": round(z, 2) if z is not None else None,
                "move_state": move_state,
                "range_pos_pct": round(pos, 1),
                "range_state": range_state,
                "n_obs": len(se), "data_age_days": age}
    lv, ld = extract_latest(node)
    if lv is not None:
        prev = None
        if isinstance(node, dict):
            for k in ("prev", "previous", "prior"):
                if isinstance(node.get(k), (int, float)):
                    prev = float(node[k])
                    break
        dod = None
        if isinstance(node, dict) and isinstance(
                node.get("chg_pct"), (int, float)):
            dod = float(node["chg_pct"])
        elif prev:
            dod = (lv / prev - 1) * 100
        ms = "NO_HISTORY"
        if dod is not None:
            a = abs(dod)
            ms = "RED" if a >= 10 else ("AMBER" if a >= 5
                                        else "CALM")
        return {"symbol": sym, "name": name, "resolved": True,
                "last": round(lv, 4),
                "dod_pct": round(dod, 2) if dod is not None
                else None,
                "move_z": None, "move_state": ms,
                "basis": "pct-move (no per-symbol sigma yet)"
                if dod is not None else "level-only",
                "range_pos_pct": None, "range_state": "NO_HISTORY",
                "n_obs": 2 if prev else 1,
                "data_age_days": None}
    return {"symbol": sym, "name": name, "resolved": False,
            "move_state": "UNRESOLVED", "range_state": "UNRESOLVED"}


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    wl = s3_json("data/tv-watchlists.json")
    vault = s3_json("data/tradingview.json")
    wb = s3_json("data/tv-workbench.json")
    dic = s3_json("data/symbol-dictionary.json")

    list_name, syms, n_lists = find_swan_list(wl)
    rows = []
    if list_name:
        for sym in syms[:150]:
            node = vault_lookup(vault, wb, list_name, sym)
            rows.append(analyze(sym, node, dict_name(dic, sym)))
    resolved = [r for r in rows if r.get("resolved")]
    with_hist = [r for r in resolved
                 if r.get("move_state") in ("CALM", "AMBER", "RED")]
    n_red = sum(1 for r in with_hist if r["move_state"] == "RED")
    n_amber = sum(1 for r in with_hist if r["move_state"] == "AMBER")
    n_extreme = sum(1 for r in with_hist
                    if r.get("range_state") == "EXTREME")
    if n_red >= 2 or (n_red >= 1 and n_amber >= 3):
        alarm = "RED"
    elif n_red >= 1 or n_amber >= 3 or n_extreme >= 5:
        alarm = "AMBER"
    else:
        alarm = "CALM"
    top = sorted(with_hist,
                 key=lambda r: -(r.get("move_z") or 0))[:5]
    payload = {
        "schema_version": "1.0",
        "engine": "justhodl-blackswan-watch",
        "as_of": now.isoformat(timespec="seconds"),
        "list_name": list_name,
        "n_lists_seen": n_lists,
        "n_members": len(syms),
        "n_resolved": len(resolved),
        "n_with_history": len(with_hist),
        "strip": {"alarm": alarm, "n_red": n_red,
                  "n_amber": n_amber, "n_range_extreme": n_extreme,
                  "top_movers": [
                      {"symbol": t["symbol"], "z": t.get("move_z"),
                       "dod_pct": t.get("dod_pct")} for t in top]},
        "rows": sorted(rows, key=lambda r: -(r.get("move_z") or -1)),
        "doctrine": "tail-risk alarm strip: |z| of the daily move vs "
                    "own history + 1y range extremes; no direction "
                    "semantics fabricated — read the named rows",
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=300")
    return {"statusCode": 200,
            "body": json.dumps({"ok": bool(list_name)
                                and len(resolved) >= 10,
                                "list": list_name,
                                "resolved": len(resolved),
                                "alarm": alarm})}
