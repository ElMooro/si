"""justhodl-blackswan-watch v1.3.1 (ops 4623)

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
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/blackswan-watch.json"
WARM = "data/warm/blackswan/"
FRED_KEY = os.environ.get("FRED_API_KEY",
                          "2f057499936072679d8843d7fce99989")
FRED_BUDGET = 90  # bounded per run; cumulative via warm cache
s3 = boto3.client("s3")


def fred_fallback(sym, budget):
    """v1.1.0: FRED: members absent from the vault get real series
    (z + range basis) via a bounded, cached direct fetch."""
    if not sym.startswith("FRED:") or budget["n"] <= 0:
        return None
    sid = sym.split(":", 1)[1]
    wkey = WARM + sid + ".json"
    cached = s3_json(wkey)
    if cached and cached.get("series"):
        try:
            ft = datetime.fromisoformat(cached["fetched_at"])
            if (datetime.now(timezone.utc) - ft
                    ).total_seconds() < 72000:
                return cached
        except Exception:
            pass
    budget["n"] -= 1
    try:
        qs = urllib.parse.urlencode({
            "series_id": sid, "api_key": FRED_KEY,
            "file_type": "json", "sort_order": "desc",
            "limit": 300})
        req = urllib.request.Request(
            "https://api.stlouisfed.org/fred/series/observations?"
            + qs, headers={"User-Agent": "justhodl-blackswan"})
        with urllib.request.urlopen(req, timeout=12) as h:
            d = json.loads(h.read())
        se = [{"date": o["date"], "value": float(o["value"])}
              for o in d.get("observations", [])
              if o.get("value") not in (None, ".", "")]
        se.reverse()
        if not se:
            return None
        env = {"series": se,
               "fetched_at": datetime.now(
                   timezone.utc).isoformat(timespec="seconds")}
        s3.put_object(Bucket=BUCKET, Key=wkey,
                      Body=json.dumps(env).encode(),
                      ContentType="application/json")
        return env
    except Exception:
        return None


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
    if isinstance(node, list) and node and isinstance(
            node[0], (list, tuple)) and len(node[0]) == 2:
        out = []
        for pr in node[-400:]:
            try:
                d0 = pr[0]
                if isinstance(d0, (int, float)):
                    d0 = datetime.fromtimestamp(
                        d0 / (1000 if d0 > 1e11 else 1),
                        tz=timezone.utc).date().isoformat()
                out.append({"date": str(d0)[:10],
                            "value": float(pr[1])})
            except Exception:
                continue
        return out or None
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


# ── fleet-join resolution (Khalid: "find the missing data from
# other engines") — column-aware history extraction over the
# fleet's own published stores, loaded once per run. ─────────────
FLEET_MAP = {
    "TVC:MOVE": ("data/move-index.json", None),
    "TVC:VIX": ("data/vix-curve-history.json", "vix"),
    "CBOE:VIX3M": ("data/vix-curve-history.json", "vix3m"),
    "CBOE:VXN": ("data/vix-curve-history.json", "vxn"),
}  # DXY dropped: dollar-radar-history holds derived scores only
ECON_FRED = {
    "ECONOMICS:USWEI": "WEI", "ECONOMICS:USJC": "ICSA",
    "ECONOMICS:USIJC": "ICSA", "ECONOMICS:USJC4W": "IC4WSA",
    "ECONOMICS:USCJC": "CCSA", "ECONOMICS:USCU": "TCU",
    "ECONOMICS:USBP": "PERMIT",
    "ECONOMICS:USGDPQQ": "A191RL1Q225SBEA",
}
_FLEET_CACHE = {}


def fleet_doc(key):
    if key not in _FLEET_CACHE:
        _FLEET_CACHE[key] = s3_json(key)
    return _FLEET_CACHE[key]


def find_row_table(node, depth=0):
    """First list of dicts carrying a date-ish key, anywhere."""
    if depth > 3 or node is None:
        return None
    if isinstance(node, list) and node and isinstance(node[0], dict):
        if any(k in node[0] for k in ("date", "d", "t", "time",
                                      "period")):
            return node
    if isinstance(node, dict):
        for v in node.values():
            r = find_row_table(v, depth + 1)
            if r:
                return r
    return None


def columnar_zip(doc, dates_key, field):
    """4627 shape lesson: fleet stores are columnar — parallel
    'dates' + per-name value arrays."""
    dates = doc.get(dates_key)
    vals = doc.get(field)
    if not (isinstance(dates, list) and isinstance(vals, list)):
        return None
    n = min(len(dates), len(vals))
    se = []
    for i in range(max(0, n - 400), n):
        try:
            se.append({"date": str(dates[i])[:10],
                       "value": float(vals[i])})
        except Exception:
            continue
    return se if len(se) >= 15 else None


def fleet_column_series(key, field):
    doc = fleet_doc(key)
    if doc is None:
        return None
    if field is None:
        se = extract_series(doc)
        return {"series": se} if se else None
    se = columnar_zip(doc, "dates", field)
    if se:
        return {"series": se}
    rows = find_row_table(doc)
    if not rows:
        return None
    dk = next((k for k in rows[0]
               if k in ("date", "d", "t", "time", "period")), None)
    if not dk:
        return None
    se = []
    for o in rows[-400:]:
        v = o.get(field)
        if v is None:
            continue
        try:
            d0 = o[dk]
            if isinstance(d0, (int, float)):
                d0 = datetime.fromtimestamp(
                    d0 / (1000 if d0 > 1e11 else 1),
                    tz=timezone.utc).date().isoformat()
            se.append({"date": str(d0)[:10], "value": float(v)})
        except Exception:
            continue
    return {"series": se} if len(se) >= 15 else None


def ma200_series(sym):
    """Plain exchange-prefixed tickers from the ma200 closes buffer
    (daily closes for ~700 names)."""
    if ":" not in sym or any(c in sym for c in "!/-+"):
        return None
    tk = sym.split(":", 1)[1]
    doc = fleet_doc("data/_ma200/closes.json")
    if doc is None:
        return None
    ser = doc.get("series") if isinstance(doc, dict) else None
    if isinstance(ser, dict) and tk in ser:
        se = columnar_zip(doc, "dates", None) if False else None
        vals = ser[tk]
        dates = doc.get("dates")
        if isinstance(dates, list) and isinstance(vals, list):
            n = min(len(dates), len(vals))
            se = []
            for i in range(max(0, n - 400), n):
                try:
                    se.append({"date": str(dates[i])[:10],
                               "value": float(vals[i])})
                except Exception:
                    continue
            if len(se) >= 15:
                return {"series": se}
    return None


TVC_MAP = {"US01MY": "DGS1MO", "US03MY": "DGS3MO",
           "US06MY": "DGS6MO", "US01Y": "DGS1", "US02Y": "DGS2",
           "US03Y": "DGS3", "US05Y": "DGS5", "US07Y": "DGS7",
           "US10Y": "DGS10", "US20Y": "DGS20", "US30Y": "DGS30"}


def leg_to_fred(leg):
    if leg.startswith("FRED:"):
        return leg.split(":", 1)[1]
    if leg.startswith("TVC:") and leg.split(":", 1)[1] in TVC_MAP:
        return TVC_MAP[leg.split(":", 1)[1]]
    return None


def eval_composite(sym, budget):
    """v1.2.0: evaluate A-B / A/B / A+B formulas whose every leg is
    FRED-mappable — unlocks the plumbing spreads (SOFR-FF, 30s10s,
    IG-vs-10Y...) with full z+range basis via the warm cache."""
    for op in ("-", "/", "+"):
        parts = sym.split(op)
        if len(parts) < 2 or any(not p for p in parts):
            continue
        sids = [leg_to_fred(p) for p in parts]
        if any(x is None for x in sids):
            continue
        legs = []
        for sid in sids:
            env = fred_fallback("FRED:" + sid, budget)
            if not env or not env.get("series"):
                return None
            legs.append({o["date"]: o["value"]
                         for o in env["series"]})
        common = sorted(set.intersection(
            *[set(m.keys()) for m in legs]))[-300:]
        if len(common) < 15:
            return None
        se = []
        for d0 in common:
            v = legs[0][d0]
            for m in legs[1:]:
                if op == "-":
                    v = v - m[d0]
                elif op == "+":
                    v = v + m[d0]
                else:
                    if not m[d0]:
                        v = None
                        break
                    v = v / m[d0]
            if v is not None:
                se.append({"date": d0, "value": round(v, 6)})
        if len(se) >= 15:
            return {"series": se}
        return None
    return None


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


def cadence_label(se):
    ds = []
    for i in range(max(1, len(se) - 12), len(se)):
        try:
            ds.append((datetime.fromisoformat(se[i]["date"])
                       - datetime.fromisoformat(
                           se[i - 1]["date"])).days)
        except Exception:
            continue
    if not ds:
        return "chg"
    ds.sort()
    med = ds[len(ds) // 2]
    if med <= 3:
        return "DoD"
    if med <= 10:
        return "WoW"
    if med <= 45:
        return "MoM"
    return "QoQ"


def analyze(sym, node, name):
    se = extract_series(node)
    if se and len(se) >= 15:
        vals = [o["value"] for o in se]
        last, prev = vals[-1], vals[-2]
        lab = cadence_label(se)
        w = vals[-260:]
        lo, hi = min(w), max(w)
        # v1.2.0 basis rule: sign-crossing or near-zero series get
        # DIFFERENCE basis — pct-change there is division-by-noise
        # (the RRP/NFCI/CFNAI class from Khalid's page audit).
        diffs = [vals[i] - vals[i - 1] for i in range(1, len(vals))]
        dmu = sum(diffs[-90:]) / max(len(diffs[-90:]), 1)
        dsd = math.sqrt(sum((x - dmu) ** 2 for x in diffs[-90:])
                        / max(len(diffs[-90:]) - 1, 1))
        mean_lvl = sum(w) / len(w)
        diff_basis = (lo < 0 < hi) or (
            dsd > 0 and abs(mean_lvl) < 2 * dsd)
        if diff_basis:
            chg = last - prev
            z = abs((chg - dmu) / dsd) if dsd else None
            chg_str = "%+.3g Δ (%s)" % (chg, lab)
            dod = None
        else:
            dod = ((last / prev - 1) * 100) if prev else None
            rets = [(vals[i] / vals[i - 1] - 1) * 100
                    for i in range(1, len(vals)) if vals[i - 1]]
            tail = rets[-90:]
            mu = sum(tail) / len(tail)
            sd = math.sqrt(sum((x - mu) ** 2 for x in tail)
                           / max(len(tail) - 1, 1))
            z = (abs((dod - mu) / sd)
                 if (sd and dod is not None) else None)
            chg_str = ("%+.2f%% (%s)" % (dod, lab)
                       if dod is not None else None)
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
                "chg_str": chg_str,
                "basis": "diff-z" if diff_basis else "pct-z",
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
            # pct-basis (no sigma) can never mint RED — page-audit
            # lesson: EPU-class daily indexes swing 40%+ normally.
            ms = "AMBER" if abs(dod) >= 7 else "CALM"
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
    budget = {"n": FRED_BUDGET}
    if list_name:
        for sym in syms[:520]:
            node = vault_lookup(vault, wb, list_name, sym)
            if (node is None or not extract_series(node)) \
                    and sym.startswith("FRED:"):
                fb = fred_fallback(sym, budget)
                if fb:
                    node = fb
            if (node is None or not extract_series(node)) \
                    and any(op in sym for op in "-/+") \
                    and ":" in sym:
                fb = eval_composite(sym, budget)
                if fb:
                    node = fb
            if node is None or not extract_series(node):
                if sym in FLEET_MAP:
                    fb = fleet_column_series(*FLEET_MAP[sym])
                    if fb:
                        node = fb
                elif sym in ECON_FRED:
                    fb = fred_fallback("FRED:" + ECON_FRED[sym],
                                       budget)
                    if fb:
                        node = fb
                else:
                    fb = ma200_series(sym)
                    if fb:
                        node = fb
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
