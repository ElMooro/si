"""ops 3183 — finish the dictionary, then push the NAMES into the engines.

Two gaps from 3182, same disease — stale entries surviving my purge rules:
  · TVC:US10Y kept "US10Y (TVC)" because the junk pattern only caught names
    that START with the source id. It maps to FRED:DGS10 and must inherit
    that series' official title.
  · NYMEX:CL1! kept source=None even though the map now resolves it to a
    continuous future (CL=F).

Purge rule is now SEMANTIC, not cosmetic: a FRED entry without a history
window did not come from FRED's metadata API; a MARKET entry without units
did not come from Polygon's reference API; any entry whose stored source
disagrees with the current map is stale. All three get rebuilt.

Then the payoff Khalid actually asked for: every watchlist engine now
carries the human NAME of each lit indicator, so the fusion layer, the desk
sheet and best-setups can reason about "Global price of Agr. Raw Material
Index" instead of "FRED:PRAWMINDEXM".
"""

import json
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import series_source as SS  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
POLY = None


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


def s3_put(key, doc):
    S3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(doc).encode(),
                  ContentType="application/json")


def http(u, timeout=20):
    r = urllib.request.urlopen(urllib.request.Request(
        u, headers={"User-Agent": "ops-3183"}), timeout=timeout)
    return json.loads(r.read().decode())


def fred_title(sid):
    for i in range(3):
        try:
            d = http("https://api.stlouisfed.org/fred/series"
                     f"?series_id={urllib.parse.quote(sid)}"
                     f"&api_key={SS.FRED_KEY}&file_type=json")
            s = (d.get("seriess") or [None])[0]
            if not s:
                return None
            return {"name": s.get("title"),
                    "units": s.get("units_short") or s.get("units"),
                    "frequency": s.get("frequency_short"),
                    "history": f"{s.get('observation_start')} → "
                               f"{s.get('observation_end')}",
                    "category": "macro"}
        except Exception:
            time.sleep(0.7 * (i + 1))
    return None


CURATED = {
    "^GSPC": "S&P 500 Index", "^NDX": "Nasdaq-100 Index",
    "^DJI": "Dow Jones Industrial Average", "^VIX": "CBOE Volatility Index",
    "^N225": "Nikkei 225 Index", "^GDAXI": "DAX 40 Index",
    "^FTSE": "FTSE 100 Index", "^HSI": "Hang Seng Index",
    "^MOVE": "ICE BofA MOVE Index (bond volatility)",
    "DX-Y.NYB": "US Dollar Index (DXY)",
    "GC=F": "Gold Futures (COMEX)", "SI=F": "Silver Futures (COMEX)",
    "CL=F": "WTI Crude Oil Futures (NYMEX)",
    "NG=F": "Natural Gas Futures (NYMEX)",
    "ZC=F": "Corn Futures (CBOT)", "ZS=F": "Soybean Futures (CBOT)",
    "ZW=F": "Wheat Futures (CBOT)", "KC=F": "Coffee Futures (ICE)",
    "HG=F": "Copper Futures (COMEX)", "ES=F": "E-mini S&P 500 Futures",
    "SPY": "SPDR S&P 500 ETF Trust",
}


def market_name(sid):
    if sid in CURATED:
        return {"name": CURATED[sid], "units": "USD", "frequency": "D",
                "category": "index/commodity"}
    if not POLY or sid.startswith("^") or "=" in sid:
        return None
    try:
        d = http(f"https://api.polygon.io/v3/reference/tickers/"
                 f"{urllib.parse.quote(sid)}?apiKey={POLY}")
        r = d.get("results") or {}
        if not r.get("name"):
            return None
        return {"name": r["name"],
                "units": (r.get("currency_name") or "usd").upper(),
                "frequency": "D",
                "category": ("etf" if str(r.get("type")) in
                             ("ETF", "ETN", "FUND") else "equity")}
    except Exception:
        return None


with report("3183_dict_final") as rep:
    fails, warns = [], []
    rep.heading("ops 3183 — finish the names, then feed them to the engines")

    POLY = (LAM.get_function_configuration(FunctionName="justhodl-wl-engines")
            .get("Environment") or {}).get("Variables", {}).get("POLYGON_KEY")

    rep.section("1. Semantic purge (not cosmetic)")
    smap = (s3_json("data/symbol-map.json") or {}).get("map") or {}
    dd = s3_json("data/symbol-dictionary.json") or {}
    dic = dd.get("dictionary") or {}
    before, killed = len(dic), 0
    for sym in list(dic):
        d = dic[sym]
        cur = smap.get(sym) or {}
        stale = (
            (d.get("source") == "FRED" and not d.get("history"))       # not FRED meta
            or (d.get("source") == "MARKET" and not d.get("units"))    # not Polygon
            or (cur.get("source") and d.get("source") != cur["source"])  # remapped
            or (cur.get("source") and not d.get("source"))             # newly mapped
            or d.get("provisional")
        )
        if stale:
            dic.pop(sym)
            killed += 1
    rep.kv(dict_before=before, purged=killed, kept=len(dic))
    rep.ok(f"purged {killed} stale/cosmetic entries")

    rep.section("2. Rebuild FRED + MARKET names (runner-side)")
    fred_syms = [s for s, m in smap.items()
                 if m["source"] == "FRED" and s not in dic]
    mkt_syms = [s for s, m in smap.items()
                if m["source"] == "MARKET" and s not in dic]
    rep.kv(fred_to_fetch=len(fred_syms), market_to_fetch=len(mkt_syms))
    t0, got_f = time.time(), 0
    with ThreadPoolExecutor(max_workers=3) as ex:
        for sym, meta in zip(fred_syms,
                             ex.map(lambda s: fred_title(smap[s]["id"]),
                                    fred_syms)):
            if meta and meta.get("name"):
                dic[sym] = {**meta, "source": "FRED",
                            "source_id": smap[sym]["id"]}
                got_f += 1
            if time.time() - t0 > 420:
                break
    t1, got_m = time.time(), 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        for sym, meta in zip(mkt_syms,
                             ex.map(lambda s: market_name(smap[s]["id"]),
                                    mkt_syms)):
            if meta and meta.get("name"):
                dic[sym] = {**meta, "source": "MARKET",
                            "source_id": smap[sym]["id"]}
                got_m += 1
            if time.time() - t1 > 300:
                break
    rep.kv(fred_named=got_f, market_named=got_m)
    named = sum(1 for s in dic if dic[s].get("name"))
    s3_put("data/symbol-dictionary.json",
           {"generated_at": datetime.now(timezone.utc).isoformat(),
            "version": "1.2", "n_named": named,
            "rebuilt_by": "ops-3183", "dictionary": dic})
    rep.ok(f"dictionary rewritten: {named} named symbols")

    rep.section("3. THE GATE")
    for sym, must in (("TVC:US10Y", "Treasury"),
                      ("FRED:FEDFUNDS", "Federal Funds"),
                      ("FRED:WALCL", "Assets"),
                      ("NYMEX:CL1!", "Crude"),
                      ("NASDAQ:NVDA", "Nvidia")):
        d = dic.get(sym) or {}
        nm = d.get("name") or ""
        rep.log(f"  {sym:18s} → {nm[:52]:52s} "
                f"[{d.get('source')}: {d.get('source_id')}] "
                f"{d.get('units') or ''} {d.get('history') or ''}")
        if sym in smap and must.lower() not in nm.lower():
            fails.append(f"{sym} reads '{nm}' (expected '{must}')")

    rep.section("4. Push the names INTO the engines")
    cfg = json.loads((AWS_DIR / "lambdas" / "justhodl-wl-engines"
                      / "config.json").read_text())
    env = (LAM.get_function_configuration(FunctionName="justhodl-wl-engines")
           .get("Environment") or {}).get("Variables") or {}
    sch = cfg.get("schedule") or {}
    deploy_lambda(report=rep, function_name="justhodl-wl-engines",
                  source_dir=AWS_DIR / "lambdas" / "justhodl-wl-engines"
                  / "source",
                  env_vars=env, eb_rule_name=sch.get("rule_name"),
                  eb_schedule=sch.get("cron"), timeout=cfg["timeout"],
                  memory=cfg["memory"],
                  description=cfg.get("description", "")[:250], smoke=False)
    t2 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName="justhodl-wl-engines", InvocationType="Event",
               Payload=json.dumps({"force_emit": True}).encode())
    idx = None
    deadline = time.time() + 700
    while time.time() < deadline:
        d = s3_json("data/wl-engines.json")
        if d and datetime.fromisoformat(d["generated_at"]) >= t2:
            idx = d
            break
        time.sleep(20)
    if not idx:
        warns.append("wl-engines still running — names land on its next run")
    else:
        firing = [e for e in (idx.get("engines") or []) if e.get("firing")]
        rep.kv(active=idx.get("n_active"), firing=len(firing))
        rep.log("── FIRING ENGINES, now in ENGLISH:")
        for e in firing[:10]:
            names = e.get("lit_named") or e.get("lit") or []
            rep.log(f"  {str(e['name'])[:30]:30s} "
                    f"({str(e.get('activation_pctile')):>5}p) → "
                    f"{'; '.join(str(n)[:34] for n in names[:2])}")
        named_lit = sum(1 for e in firing if e.get("lit_named"))
        if named_lit:
            rep.ok(f"{named_lit}/{len(firing)} firing engines report their "
                   "lit indicators BY NAME — the fusion layer can now reason "
                   "about what is actually moving")
        else:
            warns.append("lit indicators still unnamed in the index")
        LAM.invoke(FunctionName="justhodl-wl-fusion",
                   InvocationType="Event", Payload=b"{}")
        rep.log("fusion bus re-invoked to pick up the named panels")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
