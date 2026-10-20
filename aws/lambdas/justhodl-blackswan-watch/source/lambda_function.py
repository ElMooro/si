"""justhodl-blackswan-watch v1.9.0 (ops 4623)

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
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials
import blackswan_resolver as _bsr  # 2026-10-20: the shared resolver ladder (was a copy in each engine)
from blackswan_resolver import (  # noqa: F401 - names the engine code below uses
    BUCKET, WARM, TE_KEY, TE_DIRECT, MULTPL_SLUGS, s3, _FLEET_CACHE, TVC_MAP, _VIDX, cadence_label, cboe_fallback, columnar_zip, dict_aliases, dict_name, extract_latest, extract_series, find_row_table, fleet_column_series, fleet_doc, fred_fallback, leg_to_fred, ma200_series, multpl_fallback, nasdaq_fallback, s3_json, stoxx_fallback, tcbs_fallback, te_hist_fallback, timedelta_days, vault_index, vault_lookup, yahoo_fallback,
)


OUT_KEY = "data/blackswan-watch.json"

FRED_KEY = os.environ.get("FRED_API_KEY",
                          managed_secret(('FRED_API_KEY', 'FRED_KEY'), ("/justhodl/fred/api-key",)))
_bsr.FRED_KEY = FRED_KEY  # the shared resolver uses this engine's key policy
FRED_BUDGET = 110  # bounded per run; cumulative via warm cache
YH_BUDGET = {"n": 70}
CUR_BUDGET = {"n": 25}   # curated symbols + curated composite legs
CB_BUDGET = {"n": 12}    # CBOE CDN route
MP_BUDGET = {"n": 12}    # multpl route
NQ_BUDGET = {"n": 18}    # api.nasdaq.com index historicals
ST_BUDGET = {"n": 2}     # STOXX txt (single cached doc)
VN_BUDGET = {"n": 2}     # TCBS (single cached doc)
TE_BUDGET = {"n": 8}     # TE historical per-indicator


CURATED_FRED = {"FX:SONIA3M": ("IUDSOIA",
                               "O/N SONIA proxy for 3M")}  # Yahoo-chart alias fallback, same pattern


def leg_series(leg, budget):
    """v1.7.0 universal composite leg resolver: FRED (incl
    ECONOMICS twins + TVC:US*Y), curated Yahoo, MULTPL."""
    sid = leg_to_fred(leg)
    if sid is None and leg in ECON_FRED:
        sid = ECON_FRED[leg]
    if sid:
        return fred_fallback("FRED:" + sid, budget)
    if leg in CURATED_LEG_YH:
        return yahoo_fallback(CURATED_LEG_YH[leg], CUR_BUDGET)
    if leg.startswith("MULTPL:"):
        return multpl_fallback(leg.split(":", 1)[1], MP_BUDGET)
    # plain exchange-ticker legs: ma200 buffer (free) then Yahoo
    if re.fullmatch(r"[A-Z]+:[A-Z0-9]{1,8}", leg):
        fb = ma200_series(leg)
        if fb:
            return fb
        return yahoo_fallback(leg.split(":", 1)[1], YH_BUDGET,
                              "leg heuristic")
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
    "ECONOMICS:USINBR": "DFF",
    "ECONOMICS:USJO": "JTSJOL", "ECONOMICS:USNO": "AMTMNO",
    "ECONOMICS:USTVS": "TOTALSA", "ECONOMICS:USMEMP": "MANEMP",
}
CURATED_LEG_YH = {"ICEUS:DX1!": "DX=F", "TVC:DXY": "DX-Y.NYB"}
CURATED_YH = {
    "ICEUS:DX1!": ("DX=F", None),
    "TVC:DXY": ("DX-Y.NYB", None),
    "NSE:CNXSMALLCAP": ("^CNXSC", None),
    "KRX:KOSPI200": ("^KS200", None),
    "EURONEXT:N100": ("^N100", None),
    "EURONEXT:PX1": ("^FCHI", None),
    "TVC:CAC40": ("^FCHI", None),
    "OMXSTO:OMXS30": ("^OMX", None),
    "INDEX:FTSEMIB": ("FTSEMIB.MI", None),
    "PSE:PSEI": ("PSEI.PS", None),
    "SPCFD:S5INDU": ("^SP500-20", None),
    "TSE:TOPIX": ("1475.T", "TOPIX ETF proxy"),
    "EUREX:FESX2!": ("^STOXX50E", "index proxy for futures"),
    "EUREX:FMEA2!": ("EEMA", "EM-Asia ETF proxy"),
    "EUREX:FMAA1!": ("AAXJ", "AC-Asia ETF proxy"),
    "SGX:SGP2!": ("^STI", "STI index proxy"),
    "OANDA:SG30SGD": ("^STI", "STI index proxy"),
    "EUREX:FMXU2!": ("ACWX", "world-ex-US ETF proxy"),
    "ASX24:AP1!": ("^AXJO", "index proxy for SPI futures"),
    "CBOT:ZB2!": ("ZB=F", "front-month proxy"),
    "CBOT:UB2!": ("UB=F", "front-month proxy"),
    "CME:SR32!": ("SR3=F", "front-month proxy"),
}


COMP_BUDGET = {"n": 40}  # reserved: composite legs (FEDFUNDS-class
# is never a standalone member, so the shared budget starved them)


def eval_composite(sym, budget):
    budget = COMP_BUDGET
    why = []
    """v1.2.0: evaluate A-B / A/B / A+B formulas whose every leg is
    FRED-mappable — unlocks the plumbing spreads (SOFR-FF, 30s10s,
    IG-vs-10Y...) with full z+range basis via the warm cache."""
    for op in ("-", "/", "+"):
        parts = sym.split(op)
        if len(parts) < 2 or any(not p for p in parts):
            continue
        legs = []
        bad = None
        for p2 in parts:
            env = leg_series(p2, budget)
            if not env or not env.get("series"):
                bad = "%s:leg %s unresolvable (budget %d)" % (
                    op, p2[:22], budget["n"])
                break
            legs.append({o["date"]: o["value"]
                         for o in env["series"]})
        if bad:
            why.append(bad)
            return {"comp_why": " | ".join(why)[:160]}
        # union calendar (4630 r4 confession: len-tie picked the
        # sparse monthly leg — 13 dates). Union guarantees daily
        # granularity whenever any leg is daily.
        cal = set()
        for m in legs:
            cal |= set(m.keys())
        common = sorted(cal)[-300:]
        filled = []
        for m in legs:
            keys = sorted(m.keys())
            f, j, lastv = {}, 0, None
            for d0 in common:
                while j < len(keys) and keys[j] <= d0:
                    lastv = m[keys[j]]
                    j += 1
                if lastv is not None:
                    f[d0] = lastv
            filled.append(f)
        legs = filled
        common = [d0 for d0 in common
                  if all(d0 in m for m in legs)]
        if len(common) < 15:
            return {"comp_why": "%s:only %d ffilled common dates"
                    % (op, len(common))}
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
        return {"comp_why": "%s:series shrank to %d" % (op, len(se))}
    return {"comp_why": (" | ".join(why) or "no op matched")[:160]}


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
        age2 = None
        if isinstance(node, dict) and node.get("date"):
            try:
                age2 = (datetime.now(timezone.utc).date()
                        - datetime.fromisoformat(
                            str(node["date"])[:10]).date()).days
            except Exception:
                pass
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
                "via": (node.get("via")
                        if isinstance(node, dict) else None),
                "data_age_days": age2}
    out = {"symbol": sym, "name": name, "resolved": False,
           "move_state": "UNRESOLVED", "range_state": "UNRESOLVED"}
    if isinstance(node, dict) and node.get("comp_why"):
        out["detail"] = node["comp_why"]
    return out


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
                if fb and fb.get("series"):
                    node = fb
                elif fb and fb.get("comp_why"):
                    node = (node if isinstance(node, dict)
                            else {}) | {"comp_why": fb["comp_why"]}
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
                elif sym.startswith("ECONOMICS:"):
                    te = fleet_doc("data/te-feed.json") or {}
                    hit = (te.get("prices") or {}).get(
                        sym.split(":", 1)[1])
                    if hit and hit.get("value") is not None:
                        node = {"value": hit["value"],
                                "date": hit.get("asof"),
                                "via": "TE latest"}
                else:
                    fb = ma200_series(sym)
                    if fb:
                        node = fb
            if node is None or not extract_series(node):
                fsid, yh = dict_aliases(dic, sym)
                if fsid:
                    fb = fred_fallback("FRED:" + fsid, budget)
                    if fb:
                        node = fb
                if (node is None or not extract_series(node)) \
                        and yh:
                    fb = yahoo_fallback(yh, YH_BUDGET)
                    if fb:
                        node = fb
            if node is None or not extract_series(node):
                if sym in CURATED_YH:
                    ya, note = CURATED_YH[sym]
                    fb = yahoo_fallback(ya, CUR_BUDGET, note)
                    if fb:
                        node = fb
                elif sym in CURATED_FRED:
                    fsid2, note2 = CURATED_FRED[sym]
                    fb = fred_fallback("FRED:" + fsid2, budget)
                    if fb:
                        fb = dict(fb)
                        fb["via"] = "FRED %s · %s" % (fsid2, note2)
                        node = fb
                elif sym.startswith("FX_IDC:") and len(
                        sym.split(":", 1)[1]) == 6:
                    pair = sym.split(":", 1)[1]
                    fb = yahoo_fallback(pair + "=X", YH_BUDGET)
                    if not fb:
                        fb = yahoo_fallback(
                            pair[3:] + pair[:3] + "=X",
                            YH_BUDGET, "inverted cross",
                            invert=True)
                    if fb:
                        node = fb
                elif sym.startswith("MULTPL:"):
                    fb = multpl_fallback(sym.split(":", 1)[1],
                                         MP_BUDGET)
                    if fb:
                        node = fb
                elif sym.startswith("CBOE:"):
                    fb = cboe_fallback(sym.split(":", 1)[1],
                                       CB_BUDGET)
                    if fb:
                        node = fb
                elif sym.startswith("NASDAQ:") and \
                        re.fullmatch(r"NASDAQ:(NQ|B3)[A-Z0-9]+",
                                     sym):
                    fb = nasdaq_fallback(sym.split(":", 1)[1],
                                         NQ_BUDGET)
                    if fb:
                        node = fb
                elif sym.startswith("EUREX:FVS"):
                    fb = stoxx_fallback(ST_BUDGET)
                    if fb:
                        node = fb
                elif sym == "HOSE:VNINDEX":
                    fb = tcbs_fallback(VN_BUDGET)
                    if fb:
                        node = fb
                elif sym in TE_DIRECT:
                    fb = te_hist_fallback(sym, TE_BUDGET)
                    if fb:
                        node = fb
                elif re.fullmatch(r"(NASDAQ|NYSE|AMEX|CBOE|BATS|"
                                  r"ICEUS|SPCFD|CME|CBOT|NYMEX|"
                                  r"COMEX):[A-Z0-9]{2,12}", sym):
                    fb = yahoo_fallback(sym.split(":", 1)[1],
                                        YH_BUDGET, "heuristic")
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
    z_rows = [r for r in resolved if r.get("move_z") is not None]
    rng = [r for r in resolved
           if r.get("range_pos_pct") is not None]
    shock_b = (100.0 * sum(1 for r in z_rows
                           if r["move_z"] >= 2) / len(z_rows)
               if z_rows else 0.0)
    ext_b = (100.0 * sum(1 for r in rng
                         if r.get("range_state") == "EXTREME")
             / len(rng) if rng else 0.0)
    str_b = (100.0 * sum(1 for r in rng
                         if r.get("range_state") == "STRETCHED")
             / len(rng) if rng else 0.0)
    baro = round(min(100.0, 0.45 * shock_b + 0.40 * ext_b
                     + 0.15 * str_b), 1)
    blabel = ("CRITICAL" if baro >= 80 else
              "HIGH" if baro >= 60 else
              "ELEVATED" if baro >= 40 else
              "WATCHFUL" if baro >= 20 else "QUIET")
    top_ext = sorted(rng, key=lambda r: -abs(
        r["range_pos_pct"] - 50))[:6]
    barometer = {
        "value": baro, "label": blabel,
        "components": {
            "shock_breadth_pct": round(shock_b, 1),
            "range_extreme_pct": round(ext_b, 1),
            "range_stretched_pct": round(str_b, 1),
            "weights": {"shock": 0.45, "extreme": 0.40,
                        "stretched": 0.15},
            "n_z_rows": len(z_rows), "n_range_rows": len(rng)},
        "top_extremes": [
            {"symbol": t["symbol"],
             "range_pos_pct": t["range_pos_pct"]}
            for t in top_ext],
        "doctrine": "breadth of >=2-sigma shocks (45%) + breadth "
                    "of 1y range extremes (40%) + stretched (15%) "
                    "across the whole Black Swan strip"}
    payload = {
        "schema_version": "1.0",
        "engine": "justhodl-blackswan-watch",
        "as_of": now.isoformat(timespec="seconds"),
        "list_name": list_name,
        "n_lists_seen": n_lists,
        "n_members": len(syms),
        "n_resolved": len(resolved),
        "n_with_history": len(with_hist),
        "barometer": barometer,
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
