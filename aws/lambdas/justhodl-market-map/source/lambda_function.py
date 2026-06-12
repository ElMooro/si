"""
justhodl-market-map v1.0 — Finviz-class Market Map + Sector Groups
==================================================================
The two iconic Finviz tools, rebuilt on the desk's own plumbing:

MAP   S&P 500 treemap — tile area = real market cap (Polygon reference,
      weekly-cached), color = % change over the selected window, sectors
      from FMP constituents. Timeframes (1D/1W/1M/3M/6M/1Y) come FREE from
      the upside-radar close-rings (no new market fetches).
GROUPS  11 SPDR sector ETFs × {1D,1W,1M,3M,6M,1Y,YTD} performance, RS rank
      vs SPY, plus per-sector breadth (advancers/decliners) aggregated from
      the map tiles.
"""
import json, os, time, gzip, urllib.request
from datetime import datetime, timezone, timedelta
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
MAP_KEY = "data/market-map.json"
GRP_KEY = "data/sector-groups.json"
STATE_KEY = "data/_map/state.json"
UP_STATE = "data/_upside/state.json.gz"
POLY_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
VERSION = "1.1.0"
DIAG = []
SECTOR_ETFS = [("XLK", "Technology"), ("XLF", "Financials"), ("XLV", "Health Care"),
                ("XLY", "Consumer Discretionary"), ("XLC", "Communication Services"),
                ("XLI", "Industrials"), ("XLP", "Consumer Staples"), ("XLE", "Energy"),
                ("XLU", "Utilities"), ("XLRE", "Real Estate"), ("XLB", "Materials")]
WINDOWS = [("c", 1), ("w", 5), ("m1", 21), ("m3", 63), ("m6", 126), ("y1", 251)]
SECTOR_ALIAS = {"Financial Services": "Financials", "Consumer Cyclical":
                 "Consumer Discretionary", "Healthcare": "Health Care",
                 "Consumer Defensive": "Consumer Staples",
                 "Basic Materials": "Materials"}
DUAL_DROP = {"GOOG": "GOOGL", "FOX": "FOXA", "NWS": "NWSA"}
INDUSTRY_ETFS = [("SMH","Semiconductors"),("IGV","Software"),("XBI","Biotech"),
  ("KRE","Regional Banks"),("XHB","Homebuilders"),("GDX","Gold Miners"),
  ("URA","Uranium"),("ITA","Aerospace & Defense"),("XOP","Oil & Gas E&P"),
  ("TAN","Solar"),("JETS","Airlines"),("ARKK","Innovation")]
THEMES = {
  "AI Leaders": ["NVDA","MSFT","GOOGL","META","AVGO","ORCL","PLTR","SNOW","CRWD","ANET"],
  "Semiconductors": ["NVDA","AVGO","AMD","TSM","MU","AMAT","LRCX","KLAC","MRVL","ASML"],
  "Crypto Equities": ["COIN","MSTR","MARA","RIOT","CLSK","HOOD"],
  "China ADRs": ["BABA","PDD","JD","BIDU","NTES","TME"],
  "Energy": ["XOM","CVX","OXY","FANG","DVN","HAL","SLB"],
  "Biotech Rotation": ["VRTX","REGN","AMGN","MRNA","BIIB","ALNY","SRPT"],
  "Homebuilders": ["DHI","LEN","PHM","NVR","TOL"],
  "Uranium": ["CCJ","UEC","NXE","DNN","UUUU"],
}
THEMES_KEY = "data/themes.json"


def jget(url, timeout=35, headers=None):
    req = urllib.request.Request(url, headers=headers or
                                  {"User-Agent": "JustHodl Research admin@justhodl.ai"})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def constituents():
    try:
        j = jget(f"https://financialmodelingprep.com/stable/sp500-constituent?apikey={FMP_KEY}")
        out = {r["symbol"]: SECTOR_ALIAS.get(r.get("sector") or "Other", r.get("sector") or "Other") for r in j if r.get("symbol")}
        for d_, keep in DUAL_DROP.items():
            if d_ in out and keep in out:
                del out[d_]
        DIAG.append(f"sp500 constituents: {len(out)}")
        return out
    except Exception as e:
        DIAG.append(f"constituents: {str(e)[:60]}")
        return {}


def mcap_state(tickers):
    """Weekly-cached real market caps via Polygon reference details."""
    try:
        st = json.loads(S3.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read())
    except Exception:
        st = {"mcap": {}, "asof": ""}
    fresh = st.get("asof", "") >= (datetime.now(timezone.utc)
                                     - timedelta(days=7)).date().isoformat()
    missing = [t for t in tickers if t not in st["mcap"]]
    todo = missing if fresh else list(tickers)
    t0 = time.time()
    got = 0
    for t in todo:
        if time.time() - t0 > 420:
            DIAG.append(f"mcap budget hit ({got} fetched, {len(todo)-got} remain)")
            break
        try:
            j = jget(f"https://api.polygon.io/v3/reference/tickers/{t}?apiKey={POLY_KEY}",
                      timeout=12)
            mc = (j.get("results") or {}).get("market_cap")
            if mc:
                st["mcap"][t] = mc
            got += 1
            time.sleep(0.04)
        except Exception:
            pass
    if got:
        st["asof"] = datetime.now(timezone.utc).date().isoformat()
        S3.put_object(Bucket=BUCKET, Key=STATE_KEY, Body=json.dumps(st).encode(),
                      ContentType="application/json")
    DIAG.append(f"mcap cache: {len(st['mcap'])} cached, {got} refreshed")
    return st["mcap"]


def poly_closes(t, days=420):
    end = datetime.now(timezone.utc).date().isoformat()
    start = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    try:
        j = jget(f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/{start}/{end}"
                  f"?adjusted=true&sort=asc&limit=50000&apiKey={POLY_KEY}", timeout=40)
        return [(datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).date().isoformat(),
                 float(r["c"]), float(r.get("v") or 0)) for r in (j.get("results") or [])]
    except Exception as e:
        DIAG.append(f"poly {t}: {str(e)[:40]}")
        return []


def lambda_handler(event=None, context=None):
    t0 = time.time()
    DIAG.clear()
    sec = constituents()
    rings = {}
    as_of = None
    try:
        raw = S3.get_object(Bucket=BUCKET, Key=UP_STATE)["Body"].read()
        up = json.loads(gzip.decompress(raw))
        rings = up.get("rings") or {}
        as_of = up.get("last_date")
        DIAG.append(f"upside rings: {len(rings)} as of {as_of}")
    except Exception as e:
        DIAG.append(f"upside state: {str(e)[:60]}")
    mcap = mcap_state([t for t in sec if t in rings])
    spy_r = rings.get("SPY") or []
    spy63 = (spy_r[-1] / spy_r[-64] - 1) * 100 if len(spy_r) > 64 else None

    tiles = []
    for t, sct in sec.items():
        r = rings.get(t)
        if not r or len(r) < 6:
            continue
        tile = {"t": t, "s": sct, "mc": mcap.get(t)}
        for key, off in WINDOWS:
            if len(r) > off and r[-1 - off]:
                tile[key] = round((r[-1] / r[-1 - off] - 1) * 100, 2)
        look = r[-min(len(r), 252):]
        tile["hi"] = round((r[-1] / max(look) - 1) * 100, 2) if look else None
        if spy63 is not None and "m3" in tile:
            tile["rs"] = round(tile["m3"] - spy63, 2)
        if "c" in tile:
            tiles.append(tile)
    # dv-size fallback flag
    n_mc = sum(1 for x in tiles if x.get("mc"))
    size_mode = "market_cap" if n_mc >= len(tiles) * 0.8 else "dollar_volume_proxy"
    if size_mode != "market_cap":
        dv = (up.get("dv") or {}) if rings else {}
        for x in tiles:
            x["mc"] = x.get("mc") or dv.get(x["t"]) or 1e9

    sector_agg = {}
    adv = dec = 0
    for x in tiles:
        a = sector_agg.setdefault(x["s"], {"mc": 0.0, "adv": 0, "dec": 0,
                                             "wsum": 0.0, "n": 0})
        a["mc"] += x["mc"] or 0
        a["n"] += 1
        if x["c"] > 0:
            a["adv"] += 1; adv += 1
        elif x["c"] < 0:
            a["dec"] += 1; dec += 1
        a["wsum"] += (x["c"] or 0) * (x["mc"] or 0)
    for s_, a in sector_agg.items():
        a["chg_mcap_weighted"] = round(a["wsum"] / a["mc"], 2) if a["mc"] else None
        del a["wsum"]
    spx_w = round(sum((x["c"] or 0) * (x["mc"] or 0) for x in tiles)
                   / max(sum(x["mc"] or 0 for x in tiles), 1), 2)

    map_out = {"engine": "market-map", "version": VERSION,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "as_of_session": as_of, "size_mode": size_mode,
                "n_tiles": len(tiles), "tiles": tiles,
                "sector_agg": sector_agg,
                "breadth": {"advancers": adv, "decliners": dec,
                             "mcap_weighted_chg": spx_w},
                "diagnostics": list(DIAG),
                "methodology": ("S&P 500 treemap: area = real market cap (Polygon "
                                 "reference, weekly cache; dollar-volume proxy if cache "
                                 "cold — labeled), color = % change per window computed "
                                 "from the desk's own close-rings; sectors from FMP "
                                 "constituents. Last-session EOD, not intraday; Groups trimmed to the same session; FMP sector names normalized to SPDR taxonomy; dual share classes deduped (GOOG/FOX/NWS).")}

    # ── GROUPS ──
    def trim(ser):
        return [row for row in ser if not as_of or row[0] <= as_of]
    spy_ser = trim(poly_closes("SPY"))
    spy = {row[0]: row[1] for row in spy_ser}
    spy_dates = sorted(spy)
    ytd0 = next((d for d in spy_dates if d >= f"{spy_dates[-1][:4]}-01-01"), None)
    groups = []
    for etf, name in SECTOR_ETFS:
        ser3 = trim(poly_closes(etf))
        ser = [(row[0], row[1]) for row in ser3]
        vols = [row[2] for row in ser3]
        vol_x = round(vols[-1] / (sum(vols[-21:-1]) / 20), 2) if len(vols) > 25 and sum(vols[-21:-1]) else None
        if len(ser) < 260:
            continue
        d = dict(ser)
        ds = [x for x, _ in ser]
        c = ser[-1][1]
        perf = {}
        for key, off in WINDOWS:
            if len(ser) > off:
                perf[key] = round((c / ser[-1 - off][1] - 1) * 100, 2)
        if ytd0 and ytd0 in d:
            perf["ytd"] = round((c / d[ytd0] - 1) * 100, 2)
        rs63 = None
        if len(ser) > 63 and spy_dates and len(spy_dates) > 63:
            rs63 = round(perf.get("m3", 0) - (spy[spy_dates[-1]] / spy[spy_dates[-64]] - 1) * 100, 2)
        ba = sector_agg.get(name) or {}
        groups.append({"etf": etf, "sector": name, "perf": perf, "rs_3m_vs_spy": rs63, "vol_x_20d": vol_x,
                        "breadth": {"adv": ba.get("adv"), "dec": ba.get("dec"),
                                     "n": ba.get("n")}})
        time.sleep(0.05)
    industries = []
    for etf, name in INDUSTRY_ETFS:
        ser3 = trim(poly_closes(etf))
        if len(ser3) < 260:
            continue
        ser = [(row[0], row[1]) for row in ser3]
        c = ser[-1][1]
        perf = {}
        for key, off in WINDOWS:
            if len(ser) > off:
                perf[key] = round((c / ser[-1 - off][1] - 1) * 100, 2)
        vols = [row[2] for row in ser3]
        vol_x = round(vols[-1] / (sum(vols[-21:-1]) / 20), 2) if len(vols) > 25 and sum(vols[-21:-1]) else None
        rs63 = None
        if len(ser) > 63 and len(spy_ser) > 64:
            rs63 = round(perf.get("m3", 0) - (spy[spy_ser[-1][0]] / spy[spy_ser[-64][0]] - 1) * 100, 2)
        industries.append({"etf": etf, "industry": name, "perf": perf,
                            "rs_3m_vs_spy": rs63, "vol_x_20d": vol_x})
        time.sleep(0.05)
    industries.sort(key=lambda g: -(g["perf"].get("m1") or -99))
    for i_, g in enumerate(industries):
        g["rank_1m"] = i_ + 1

    groups.sort(key=lambda g: -(g["perf"].get("m1") or -99))
    for i, g in enumerate(groups):
        g["rank_1m"] = i + 1
    grp_out = {"engine": "sector-groups", "version": VERSION,
                "generated_at": map_out["generated_at"], "as_of_session": as_of,
                "groups": groups, "industries": industries,
                "leadership": {"top_1m": groups[0]["sector"] if groups else None,
                                "bottom_1m": groups[-1]["sector"] if groups else None},
                "methodology": ("11 SPDR sector ETFs, performance across 1D/1W/1M/3M/"
                                 "6M/1Y/YTD, 3M relative strength vs SPY, breadth from "
                                 "the map's constituent tiles.")}

    themes = []
    for tname, tick_list in THEMES.items():
        rows = []
        for t in tick_list:
            r = rings.get(t)
            if not r or len(r) < 70:
                continue
            row = {"t": t}
            for key, off in WINDOWS:
                if len(r) > off:
                    row[key] = round((r[-1] / r[-1 - off] - 1) * 100, 2)
            look = r[-min(len(r), 252):]
            row["hi"] = round((r[-1] / max(look) - 1) * 100, 2)
            rows.append(row)
        if len(rows) < 3:
            continue
        def avg(k):
            xs = [x[k] for x in rows if x.get(k) is not None]
            return round(sum(xs) / len(xs), 2) if xs else None
        adv = sum(1 for x in rows if (x.get("c") or 0) > 0)
        srt = sorted(rows, key=lambda x: -(x.get("m1") or -99))
        themes.append({"theme": tname, "n": len(rows),
                        "perf": {k: avg(k) for k, _ in WINDOWS},
                        "rs_3m_vs_spy": (round((avg("m3") or 0) - spy63, 2)
                                          if spy63 is not None and avg("m3") is not None else None),
                        "breadth_1d": f"{adv}/{len(rows) - adv}",
                        "leader_1m": srt[0]["t"], "laggard_1m": srt[-1]["t"],
                        "members": rows})
    themes.sort(key=lambda x: -(x["perf"].get("m1") or -99))
    th_out = {"engine": "themes", "version": VERSION,
               "generated_at": map_out["generated_at"], "as_of_session": as_of,
               "themes": themes,
               "methodology": ("Curated thematic watchlists computed from the desk's own "
                                "close-rings: equal-weight performance per window, breadth, "
                                "1M leader/laggard, 3M RS vs SPY. Last-session EOD.")}

    for key, payload in ((MAP_KEY, map_out), (GRP_KEY, grp_out), (THEMES_KEY, th_out)):
        clean = json.loads(json.dumps(payload, default=str), parse_constant=lambda c: None)
        S3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(clean).encode(),
                      ContentType="application/json", CacheControl="public, max-age=1800")
    dur = round(time.time() - t0, 1)
    print(f"[map] tiles={len(tiles)} size={size_mode} breadth {adv}/{dec} "
          f"groups={len(groups)} {dur}s")
    return {"statusCode": 200, "body": json.dumps({"tiles": len(tiles),
                                                     "groups": len(groups)})}
