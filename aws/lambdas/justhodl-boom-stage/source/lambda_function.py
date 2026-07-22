"""justhodl-boom-stage v1.0 — is a boom EARLY or PLATEAUING? Physical proof.

Khalid's directive, generalizing the Korea-chips read: cross VALUE signals
(export value, orders, credit) against physical VOLUME signals (AIS port
calls) per country-industry pair and stage the boom:

  EARLY_PRICE_LED  value surging, volume flat  -> pricing power; demand >
                   capacity; high-value goods may fly not sail (KR chips now)
  BROADENING       value up AND volume up      -> physical expansion, capex
  PLATEAU_FORMING  value cooling while volume high -> supply caught up,
                   price normalization
  CONTRACTION      value down AND volume down  -> demand fade
  MIXED/NA         insufficient or conflicting legs (honest)

All legs read from existing fleet feeds (audit-first, no new sources):
asia-leads (KR flash value, TW orders), china-liquidity (TSF credit value),
portwatch exporters (volume), industry-boom (industry composite context),
air-cargo (high-value volume when live). Emits data/boom-stage.json.
stdlib-only; never fabricates; missing legs stay null with NA stage.
"""
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.6.0"
BUCKET = "justhodl-dashboard-live"
KEY = "data/boom-stage.json"
S3 = boto3.client("s3", region_name="us-east-1")


UA = {"User-Agent": "Mozilla/5.0"}
FRED_KEY = "2f057499936072679d8843d7fce99989"

# 4th factor (Khalid's framework): inventory / utilization — confirms whether
# producers are ramping (build) or drawing down (draw). Proven live in 3681.
FACTOR4 = {
    "semis": ("CAPUTLG3344S", "util", "Semiconductor capacity utilization"),
    "gas_stocks": ("WGTSTUS1", "inv", "US natural gas in storage"),
    "copper_stocks": ("PCU2122302122300", "inv", "Copper ore PPI (proxy)"),
    "mining": ("CAPUTLG21S", "util", "Mining capacity utilization"),
    "mfg": ("TCU", "util", "Total industry capacity utilization"),
    "inv_sales": ("MNFCTRIRSA", "inv", "Manufacturers inventories/sales"),
    "biz_inv": ("ISRATIO", "inv", "Business inventories/sales"),
    "retail_inv": ("RETAILIRSA", "inv", "Retail inventories/sales"),
}


def _fred_pair(sid):
    """latest + 12m-ago value."""
    try:
        u = ("https://api.stlouisfed.org/fred/series/observations?"
             f"series_id={sid}&api_key={FRED_KEY}&file_type=json"
             "&sort_order=desc&limit=16")
        o = json.loads(urllib.request.urlopen(
            urllib.request.Request(u, headers=UA), timeout=20).read())
        obs = [x for x in (o.get("observations") or [])
               if x.get("value") not in (".", "", None)]
        if not obs:
            return None, None, None
        cur = float(obs[0]["value"])
        prior = float(obs[12]["value"]) if len(obs) > 12 else None
        return cur, prior, obs[0]["date"]
    except Exception:
        return None, None, None


def _factor4(kind):
    """Returns dict for a factor-4 key: level, yoy delta, and read."""
    spec = FACTOR4.get(kind)
    if not spec:
        return None
    sid, typ, name = spec
    cur, prior, dt = _fred_pair(sid)
    if cur is None:
        return None
    d = {"name": name, "series": sid, "level": round(cur, 2),
         "date": dt, "type": typ}
    if prior:
        chg = round(cur - prior, 2)
        d["yoy_chg"] = chg
        if typ == "util":
            d["read"] = ("RAMPING" if chg >= 1.5 else
                         "IDLING" if chg <= -1.5 else "STEADY")
            d["means"] = ("producers running hotter — supply responding"
                          if chg >= 1.5 else
                          "capacity going idle — demand not pulling"
                          if chg <= -1.5 else "utilization flat")
        else:
            d["read"] = ("INVENTORY_BUILD" if chg >= 0.04 else
                         "INVENTORY_DRAW" if chg <= -0.04 else "BALANCED")
            d["means"] = ("stock piling up vs sales — slowdown warning"
                          if chg >= 0.04 else
                          "stocks drawn down vs sales — tightening, "
                          "bullish for price" if chg <= -0.04 else
                          "inventories balanced")
    return d


def _refine(stage, f4):
    """4-factor refinement: inventory/utilization confirms or contradicts."""
    if not f4 or not f4.get("read"):
        return stage, None
    r = f4["read"]
    if stage == "EARLY_PRICE_LED" and r == "INVENTORY_DRAW":
        return stage, ("CONFIRMED: inventories drawing down while price "
                       "leads — genuine tightening, boom has room")
    if stage == "EARLY_PRICE_LED" and r in ("INVENTORY_BUILD", "RAMPING"):
        return stage, ("CAUTION: supply is responding (" + r.lower()
                       + ") — pricing power may fade; watch for PLATEAU")
    if stage in ("BROADENING",) and r == "RAMPING":
        return stage, "CONFIRMED: utilization rising with volumes — real capex"
    if stage in ("CONTRACTION", "VALUE_LED_DOWNTURN") \
            and r == "INVENTORY_BUILD":
        return stage, ("CONFIRMED: unsold stock building into falling "
                       "demand — classic slowdown")
    if stage in ("CONTRACTION", "VALUE_LED_DOWNTURN") \
            and r == "INVENTORY_DRAW":
        return stage, ("EARLY-TURN WATCH: stocks drawing down despite weak "
                       "prints — restock impulse may be forming")
    if stage == "SUPPLY_SHOCK_PRICING" and r == "IDLING":
        return stage, ("CONFIRMED: capacity idle while prices spike — "
                       "supply-side, not demand — inflationary and "
                       "growth-negative")
    return stage, None


def _yoy_yahoo(sym):
    """Keyless Yahoo chart: yoy %% of last close vs ~1y ago."""
    try:
        u = ("https://query1.finance.yahoo.com/v8/finance/chart/"
             + urllib.parse.quote(sym) + "?range=1y&interval=1d")
        j = json.loads(urllib.request.urlopen(
            urllib.request.Request(u, headers=UA), timeout=15).read())
        cl = (j["chart"]["result"][0]["indicators"]["quote"][0]["close"])
        cl = [c for c in cl if isinstance(c, (int, float))]
        if len(cl) < 100:
            return None
        return round(100 * (cl[-1] / cl[0] - 1), 1)
    except Exception:
        return None


CANARY = {
    "TW-semis": ("GROWTH", "global tech cycle — foundry is the world's "
                 "narrowest chokepoint"),
    "KR-semis": ("GROWTH", "memory pricing = tech demand pulse"),
    "CL-copper": ("GROWTH", "Dr. Copper: global construction/industrial "
                  "demand"),
    "PE-copper": ("GROWTH", "second copper read — confirms or denies Chile"),
    "FI-pulp": ("GROWTH", "pulp/paper = packaging = physical goods "
                "consumption"),
    "SA-oil": ("INFLATION", "crude supply — the inflation input"),
    "UAE-energy": ("INFLATION", "Gulf re-export + Hormuz transit risk"),
    "BR-commodities": ("INFLATION", "metals/ags price pass-through"),
    "CN-broad": ("GROWTH", "world's #2 economy, credit + gateway volume"),
    "US-freight": ("GROWTH", "US inland demand + carrier pricing power"),
    "AU-iron": ("GROWTH", "iron ore = China steel = global construction"),
    "ID-nickel": ("GROWTH", "nickel = EV battery + stainless demand"),
    "QA-lng": ("INFLATION", "LNG = European/Asian energy input cost"),
    "DE-machinery": ("GROWTH", "capital goods orders = global capex cycle"),
}

TRADE_MAP = {
    "TW-semis": {
        "EARLY_PRICE_LED": ("LONG", "TSM / SOXX / EWT",
                            "foundry pricing power — tech cycle early"),
        "PLATEAU_FORMING": ("TRIM", "TSM / SOXX", "orders cooling"),
        "CONTRACTION": ("AVOID", "semis complex", "tech cycle rolling over")},
    "CL-copper": {
        "EARLY_PRICE_LED": ("LONG", "FCX / COPX / SCCO",
                            "copper price-led, shipments lagging"),
        "BROADENING": ("LONG", "FCX / COPX", "price AND cargoes up — real "
                       "industrial demand"),
        "SUPPLY_SHOCK_PRICING": ("LONG-MINERS/HEDGE-CPI", "FCX / COPX / TIPS",
                                 "mine disruption pricing — inflationary"),
        "CONTRACTION": ("AVOID", "copper complex",
                        "global industrial demand fading")},
    "PE-copper": {
        "EARLY_PRICE_LED": ("LONG", "SCCO / COPX", "second copper confirm"),
        "SUPPLY_SHOCK_PRICING": ("LONG-MINERS/HEDGE-CPI", "SCCO / TIPS",
                                 "Andean supply disruption"),
        "CONTRACTION": ("AVOID", "copper complex", "demand fade confirmed")},
    "FI-pulp": {
        "EARLY_PRICE_LED": ("LONG", "UPM / Stora Enso / Suzano",
                            "pulp pricing power"),
        "BROADENING": ("LONG", "UPM / Suzano", "packaging demand expanding"),
        "CONTRACTION": ("AVOID", "paper/packaging",
                        "physical goods consumption contracting — "
                        "recession tell")},
    "SA-oil": {
        "EARLY_PRICE_LED": ("LONG", "XLE / XOP",
                            "crude price-led with flat liftings"),
        "SUPPLY_SHOCK_PRICING": ("LONG-ENERGY/HEDGE-CPI",
                                 "XLE / tankers / TIPS",
                                 "supply-driven crude spike — inflation "
                                 "shock, growth-negative"),
        "CONTRACTION": ("AVOID", "energy beta", "demand destruction")},
    "KR-semis": {
        "EARLY_PRICE_LED": ("LONG", "MU / SOXX / 000660.KS-proxy",
                            "memory pricing-power phase — ride it"),
        "PLATEAU_FORMING": ("TRIM", "MU / SOXX",
                            "volume caught up, price cooling — the RAM "
                            "sell-signal"),
        "CONTRACTION": ("AVOID", "semis complex", "cycle rolling over")},
    "CN-broad": {
        "EARLY_PRICE_LED": ("WATCH", "FXI / copper", "credit re-igniting"),
        "BROADENING": ("LONG", "FXI / KWEB / copper", "credit + ports up"),
        "CONTRACTION": ("AVOID/SHORT-BIAS", "FXI / AUD / copper demand",
                        "credit and gateway volume both contracting")},
    "UAE-energy": {
        "EARLY_PRICE_LED": ("LONG", "XLE / tankers (FRO/STNG)",
                            "crude price-led while transit volume flat"),
        "PLATEAU_FORMING": ("TRIM", "XLE", "volume normalizing"),
        "CONTRACTION": ("AVOID", "energy beta", "price and flow both down")},
    "BR-commodities": {
        "EARLY_PRICE_LED": ("LONG", "EWZ / FCX / VALE",
                            "metal price-led, shipments lagging"),
        "BROADENING": ("LONG", "EWZ / VALE", "price and cargoes both up"),
        "CONTRACTION": ("AVOID", "EWZ / VALE",
                        "copper and Santos flow both down")},
    "US-freight": {
        "EARLY_PRICE_LED": ("LONG", "carriers: ODFL / UNP / SAIA",
                            "freight $ up on falling shipments = carrier "
                            "pricing power"),
        "PLATEAU_FORMING": ("TRIM", "carriers", "rate power fading"),
        "CONTRACTION": ("AVOID", "transports", "volume and $ both down")},
}


def _get(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def _stage(v_yoy, vol_pct):
    if v_yoy is None or vol_pct is None:
        return "NA", "missing leg"
    if v_yoy >= 10 and vol_pct <= -15:
        return "SUPPLY_SHOCK_PRICING", (
            "price surging while physical volume COLLAPSES — supply "
            "disruption, not demand; this is the inflation signature")
    if v_yoy >= 10 and -15 <= vol_pct <= 5:
        return "EARLY_PRICE_LED", ("value surging while physical volume flat "
                                   "— pricing power / high-value goods; the "
                                   "boom has NOT plateaued")
    if v_yoy >= 8 and vol_pct > 8:
        return "BROADENING", "value AND volume expanding — capex phase"
    if 0 <= v_yoy < 8 and vol_pct > 8:
        return "PLATEAU_FORMING", ("volume caught up while value growth "
                                   "cools — price normalization")
    if v_yoy < 0 and vol_pct < -8:
        return "CONTRACTION", "value and volume both falling — demand fade"
    if v_yoy < 0 <= vol_pct:
        return "VALUE_LED_DOWNTURN", ("value falling first while volume "
                                      "holds — margin squeeze forming")
    return "MIXED", "legs conflict mildly — watch next prints"


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    asia = _get("data/asia-leads.json")
    pw = _get("data/portwatch.json")
    cn = _get("data/china-liquidity.json")
    ib = _get("data/industry-boom.json")
    air = _get("data/air-cargo.json")

    vols = {e.get("country"): e for e in (pw.get("exporters") or [])}

    def vol(country):
        e = vols.get(country) or {}
        return e.get("avg_vs_baseline_pct"), e.get("avg_z"), e.get("n_ports")

    pairs = []

    # KR - semiconductors/memory
    kr = ((asia.get("korea_flash_tape") or {}).get("latest") or {})
    v_yoy = kr.get("yoy_pct")
    vp, vz, npt = vol("Korea")
    st, why = _stage(v_yoy, vp)
    semis = next((r for r in (ib.get("rows") or ib.get("league") or [])
                  if "semi" in str(r.get("industry", "")).lower()), {})
    pairs.append({
        "id": "KR-semis", "label": "Korea · Semiconductors/Memory",
        "stage": st, "why": why,
        "value": {"src": "asia-leads KR 1-20 flash (news-tape)",
                  "yoy_pct": v_yoy, "note": kr.get("headline", "")[:90]},
        "volume": {"src": "portwatch Korea gateway ports",
                   "vs_baseline_pct": vp, "z": vz, "n_ports": npt},
        "context": {"industry_boom_score": semis.get("boom_score"),
                    "air_cargo": (air.get("yoy_pct")
                                  if air.get("ok") else None)},
    })

    # TW value leg (v1.5): orders yoy is still self-building (needs a 12m
    # prior vintage in asia/tw-orders-levels.json, currently 2 months), so
    # fall back to Taiwan goods-EXPORTS yoy which asia-leads already carries
    # live. Orders lead shipments, so orders is preferred when it arrives.
    tw = (asia.get("taiwan_orders") or {})
    twx = (asia.get("taiwan_exports") or {})
    v_yoy2 = tw.get("yoy_pct")
    tw_src = "asia-leads TW export orders (MOEA)"
    if v_yoy2 is None:
        v_yoy2 = tw.get("yoy")
    if v_yoy2 is None and isinstance(twx.get("yoy_pct"), (int, float)):
        v_yoy2 = twx["yoy_pct"]
        tw_src = ("asia-leads TW goods exports yoy (orders series still "
                  "building its 12m vintage)")

    # CN - broad economy (credit value vs port volume)
    tsf = ((cn.get("tsf") or {}).get("pboc_cn") or {})
    dv = tsf.get("yoy_delta_trn")
    v_yoy3 = None
    if isinstance(dv, (int, float)) and tsf.get("flow_trn_cny"):
        prior = tsf["flow_trn_cny"] - dv
        if prior:
            v_yoy3 = round(100 * dv / prior, 1)
    vp3, vz3, npt3 = vol("China")
    st3, why3 = _stage(v_yoy3, vp3)
    pairs.append({
        "id": "CN-broad", "label": "China · Broad economy (credit vs ports)",
        "stage": st3, "why": why3,
        "value": {"src": "PBoC TSF flow (credit impulse)",
                  "yoy_pct": v_yoy3,
                  "flow_trn_cny": tsf.get("flow_trn_cny"),
                  "period": tsf.get("period")},
        "volume": {"src": "portwatch China gateway ports",
                   "vs_baseline_pct": vp3, "z": vz3, "n_ports": npt3},
        "context": {},
    })

    # UAE - energy re-export (Brent value x Jebel Ali volume)
    br_v = _yoy_yahoo("BZ=F")
    vp4, vz4, npt4 = vol("UAE")
    st4, why4 = _stage(br_v, vp4)
    pairs.append({"id": "UAE-energy", "label": "UAE · Energy re-export "
                  "(Brent vs Jebel Ali)", "stage": st4, "why": why4,
                  "value": {"src": "Brent crude (BZ=F) yoy", "yoy_pct": br_v},
                  "volume": {"src": "portwatch UAE gateway ports",
                             "vs_baseline_pct": vp4, "z": vz4,
                             "n_ports": npt4}, "context": {}})

    # BR - bulk commodities (Copper value x Santos volume)
    cu_v = _yoy_yahoo("HG=F")
    vp5, vz5, npt5 = vol("Brazil")
    st5, why5 = _stage(cu_v, vp5)
    pairs.append({"id": "BR-commodities", "label": "Brazil · Bulk "
                  "commodities (Copper vs Santos)", "stage": st5,
                  "why": why5,
                  "value": {"src": "Copper (HG=F) yoy", "yoy_pct": cu_v},
                  "volume": {"src": "portwatch Brazil gateway ports",
                             "vs_baseline_pct": vp5, "z": vz5,
                             "n_ports": npt5}, "context": {}})

    # US - freight carriers (Cass $ value x Cass shipments volume)
    fp = _get("data/freight-pulse.json")
    ce = ((fp.get("series") or {}).get("cass_expend") or {})
    csh = ((fp.get("series") or {}).get("cass_shipments") or {})
    st6, why6 = _stage(ce.get("yoy_pct"), csh.get("yoy_pct"))
    pairs.append({"id": "US-freight", "label": "US · Freight carriers "
                  "(Cass $ vs shipments)", "stage": st6, "why": why6,
                  "value": {"src": "Cass expenditures yoy",
                            "yoy_pct": ce.get("yoy_pct")},
                  "volume": {"src": "Cass shipments yoy",
                             "vs_baseline_pct": csh.get("yoy_pct"),
                             "z": csh.get("z_5y"), "n_ports": None},
                  "context": {"inflection": csh.get("inflection")}})

    # TW - semiconductors (foundry): orders value x Kaohsiung volume
    vp7, vz7, npt7 = vol("Taiwan")
    st7, why7 = _stage(v_yoy2, vp7)

    # CL / PE - copper (LME copper value x Andean port volume)
    for _cc, _lbl in (("Chile", "CL-copper"), ("Peru", "PE-copper")):
        vpx, vzx, nptx = vol(_cc)
        stx, whyx = _stage(cu_v, vpx)
        pairs.append({"id": _lbl,
                      "label": f"{_cc} · Copper (LME price vs port calls)",
                      "stage": stx, "why": whyx,
                      "value": {"src": "Copper (HG=F) yoy", "yoy_pct": cu_v},
                      "volume": {"src": f"portwatch {_cc} ports",
                                 "vs_baseline_pct": vpx, "z": vzx,
                                 "n_ports": nptx}, "context": {}})

    # FI - pulp & paper (proxy value: WOOD/pulp equity complex vs port calls)
    fi_v = _yoy_yahoo("UPM.HE") or _yoy_yahoo("WOOD")
    vp8, vz8, npt8 = vol("Finland")
    st8, why8 = _stage(fi_v, vp8)
    pairs.append({"id": "FI-pulp",
                  "label": "Finland · Pulp & paper (producer tape vs ports)",
                  "stage": st8, "why": why8,
                  "value": {"src": "UPM.HE / WOOD yoy (pulp complex)",
                            "yoy_pct": fi_v},
                  "volume": {"src": "portwatch Finland ports",
                             "vs_baseline_pct": vp8, "z": vz8,
                             "n_ports": npt8}, "context": {}})

    # SA - crude oil (Brent value x Saudi port calls)
    vp9, vz9, npt9 = vol("Saudi Arabia")
    st9, why9 = _stage(br_v, vp9)
    pairs.append({"id": "SA-oil",
                  "label": "Saudi Arabia · Crude oil (Brent vs liftings)",
                  "stage": st9, "why": why9,
                  "value": {"src": "Brent crude (BZ=F) yoy",
                            "yoy_pct": br_v},
                  "volume": {"src": "portwatch Saudi ports",
                             "vs_baseline_pct": vp9, "z": vz9,
                             "n_ports": npt9}, "context": {}})

    # TW pair uses orders value (may be building) — append with volume now live
    pairs.append({"id": "TW-semis",
                  "label": "Taiwan · Semiconductors (orders vs Kaohsiung)",
                  "stage": st7, "why": why7,
                  "value": {"src": tw_src, "yoy_pct": v_yoy2,
                            "note": ("orders yoy self-building"
                                     if v_yoy2 is None else
                                     ("orders levels cached: "
                                      + str(tw.get("levels_cached") or 0)
                                      + "/13 — will switch to true orders "
                                        "when the vintage completes"))},
                  "volume": {"src": "portwatch Taiwan ports",
                             "vs_baseline_pct": vp7, "z": vz7,
                             "n_ports": npt7}, "context": {}})

    # --- new specialization pairs (Khalid tier-1 map) ---
    NEW = [
        ("AU-iron", "Australia", "Australia · Iron ore (price vs ports)",
         "TIO=F", "Iron ore 62% (TIO=F) yoy", "mining"),
        ("ID-nickel", "Indonesia", "Indonesia · Nickel (price vs ports)",
         "NICKEL", "Nickel complex proxy yoy", "mining"),
        ("QA-lng", "Qatar", "Qatar · LNG (gas price vs liftings)",
         "NG=F", "Henry Hub gas (NG=F) yoy", None),
        ("DE-machinery", "Germany", "Germany · Machinery (capital goods "
         "tape vs ports)", "EXS1.DE", "DAX industrials proxy yoy", "mfg"),
    ]
    for pid, ctry, label, sym, vsrc, f4key in NEW:
        vv = _yoy_yahoo(sym) if sym != "NICKEL" else _yoy_yahoo("XME")
        vpn, vzn, nptn = vol(ctry)
        stn, whyn = _stage(vv, vpn)
        f4 = _factor4(f4key) if f4key else None
        stn, note = _refine(stn, f4)
        pairs.append({"id": pid, "label": label, "stage": stn, "why": whyn,
                      "value": {"src": vsrc, "yoy_pct": vv},
                      "volume": {"src": f"portwatch {ctry} ports",
                                 "vs_baseline_pct": vpn, "z": vzn,
                                 "n_ports": nptn},
                      "factor4": f4, "factor4_note": note,
                      "context": {}})

    # attach factor-4 to the original pairs
    F4MAP = {"KR-semis": "semis", "TW-semis": "semis",
             "AU-iron": "mining", "QA-lng": "gas_stocks",
             "DE-machinery": "mfg",
             "CL-copper": "mining", "PE-copper": "mining",
             "US-freight": "biz_inv", "FI-pulp": "retail_inv",
             "CN-broad": "mfg", "BR-commodities": "mining",
             "UAE-energy": None, "SA-oil": None}
    for p in pairs:
        if p.get("factor4") is not None or p["id"] not in F4MAP:
            continue
        k = F4MAP.get(p["id"])
        f4 = _factor4(k) if k else None
        if f4:
            st_new, note = _refine(p["stage"], f4)
            p["factor4"] = f4
            p["factor4_note"] = note

    # ---- history ledger + transitions + sliding-watch signals ----
    try:
        hist = json.loads(S3.get_object(
            Bucket=BUCKET, Key="boom/boom-stage-history.json")["Body"].read())
    except Exception:
        hist = {"days": {}}
    today = now.strftime("%Y-%m-%d")
    prev_day = max([k for k in hist["days"] if k < today], default=None)
    prev = hist["days"].get(prev_day, {}) if prev_day else {}
    hist["days"][today] = {p["id"]: {
        "v": (p["value"] or {}).get("yoy_pct"),
        "vol": (p["volume"] or {}).get("vs_baseline_pct"),
        "stage": p["stage"]} for p in pairs}
    hist["days"] = {k: hist["days"][k]
                    for k in sorted(hist["days"])[-60:]}
    S3.put_object(Bucket=BUCKET, Key="boom/boom-stage-history.json",
                  Body=json.dumps(hist).encode(),
                  ContentType="application/json")

    signals = []
    for p in pairs:
        pid = p["id"]
        pv = prev.get(pid) or {}
        v_now = (p["value"] or {}).get("yoy_pct")
        vol_now = (p["volume"] or {}).get("vs_baseline_pct")
        p["trajectory"] = None
        if pv.get("v") is not None and v_now is not None:
            dv = round(v_now - pv["v"], 1)
            dvol = round((vol_now or 0) - (pv.get("vol") or 0), 1)
            p["trajectory"] = {"d_value": dv, "d_volume": dvol,
                               "prev_day": prev_day}
        tm = TRADE_MAP.get(pid, {})
        if pv.get("stage") and pv["stage"] not in ("NA",)                 and p["stage"] not in ("NA",)                 and pv["stage"] != p["stage"]:
            tr = tm.get(p["stage"])
            signals.append({
                "type": "STAGE_CHANGE", "pair": pid,
                "from": pv["stage"], "to": p["stage"],
                "trade": ({"bias": tr[0], "instruments": tr[1],
                           "line": tr[2]} if tr else None),
                "line": f"{p['label']}: {pv['stage']} -> {p['stage']}"})
        if p["stage"] == "EARLY_PRICE_LED" and p["trajectory"]:
            dv = p["trajectory"]["d_value"]
            dvol = p["trajectory"]["d_volume"]
            if (dv <= -2 or dvol >= 3) and (v_now < 18 or (vol_now or 0) > 0):
                tr = tm.get("PLATEAU_FORMING")
                signals.append({
                    "type": "SLIDING_WATCH", "pair": pid,
                    "toward": "PLATEAU_FORMING",
                    "trade": ({"bias": tr[0], "instruments": tr[1],
                               "line": tr[2]} if tr else None),
                    "line": (f"{p['label']}: sliding toward "
                             f"PLATEAU_FORMING (value {dv:+}pp, volume "
                             f"{dvol:+}pp) — pre-emptive trim signal")})
        if p["stage"] == "CONTRACTION" and p["trajectory"]:
            if p["trajectory"]["d_value"] >= 2                     and p["trajectory"]["d_volume"] >= 2:
                signals.append({
                    "type": "BASING_WATCH", "pair": pid,
                    "toward": "MIXED",
                    "line": f"{p['label']}: both legs improving — "
                            f"basing watch"})
        stage_tr = tm.get(p["stage"])
        if stage_tr:
            p["trade"] = {"bias": stage_tr[0],
                          "instruments": stage_tr[1],
                          "line": stage_tr[2]}

    for p in pairs:
        tag = CANARY.get(p["id"])
        if tag:
            typ, why = tag[0], tag[1]
            # v1.4 FIX: a GROWTH pair printing SUPPLY_SHOCK_PRICING is
            # behaving as an INFLATION canary (Chile: mining util RAMPING but
            # port volume -29.7% = export bottleneck, not demand). Reclassify
            # dynamically so the dials read the world correctly.
            if typ == "GROWTH" and p["stage"] == "SUPPLY_SHOCK_PRICING":
                typ = "INFLATION"
                why = why + " — currently acting as an INFLATION canary " \
                            "(supply bottleneck, not demand)"
                p["reclassified"] = True
            p["canary"] = {"type": typ, "why": why}

    # ---- NEW CANARY A: same-commodity divergence ----
    # Two countries, one commodity, opposite physical flow = the price move
    # is a LOCAL SUPPLY problem, not global demand. Highest-conviction read
    # on the board and nothing else in the fleet computes it.
    COMMODITY_GROUPS = {
        "copper": ["CL-copper", "PE-copper"],
        "crude": ["SA-oil", "UAE-energy"],
        "semis": ["KR-semis", "TW-semis"],
    }
    divergences = []
    byid = {p["id"]: p for p in pairs}
    for comm, ids in COMMODITY_GROUPS.items():
        legs = [byid[i] for i in ids if i in byid
                and (byid[i].get("volume") or {}).get("vs_baseline_pct")
                is not None]
        if len(legs) < 2:
            continue
        vols = [(l["id"], (l["volume"] or {}).get("vs_baseline_pct"))
                for l in legs]
        vols.sort(key=lambda x: x[1])
        lo, hi = vols[0], vols[-1]
        spread = round(hi[1] - lo[1], 1)
        if spread >= 25:
            divergences.append({
                "commodity": comm, "spread_pp": spread,
                "weak": lo[0], "weak_pct": lo[1],
                "strong": hi[0], "strong_pct": hi[1],
                "read": (f"{comm.upper()}: {lo[0]} volume {lo[1]}% vs "
                         f"{hi[0]} {hi[1]}% — {spread}pp gap. Same commodity, "
                         f"opposite flow = LOCAL SUPPLY DISRUPTION in "
                         f"{lo[0].split('-')[0]}, not a global demand signal. "
                         f"Price strength here is supply-driven "
                         f"(inflationary), and producers in "
                         f"{hi[0].split('-')[0]} gain share."),
                "trade": (f"prefer {hi[0].split('-')[0]}-exposed producers "
                          f"over {lo[0].split('-')[0]}-exposed; the spread "
                          f"is the alpha")})
    divergences.sort(key=lambda x: -x["spread_pp"])

    # ---- NEW CANARY B: price-led vs volume-led BREADTH ----
    # One number: is the world's activity real demand or just price?
    price_led = [p["id"] for p in pairs
                 if p["stage"] in ("EARLY_PRICE_LED", "SUPPLY_SHOCK_PRICING")]
    volume_led = [p["id"] for p in pairs if p["stage"] == "BROADENING"]
    falling = [p["id"] for p in pairs
               if p["stage"] in ("CONTRACTION", "VALUE_LED_DOWNTURN")]
    scored = len(price_led) + len(volume_led) + len(falling)
    breadth = None
    if scored >= 4:
        breadth = {
            "price_led_n": len(price_led), "volume_led_n": len(volume_led),
            "falling_n": len(falling), "scored_n": scored,
            "price_led_share": round(100 * len(price_led) / scored),
            "read": ("PRICE-LED WORLD — most activity is price, not volume: "
                     "inflation without real growth"
                     if len(price_led) > (len(volume_led) + len(falling))
                     else "VOLUME-LED WORLD — real physical expansion "
                          "underway" if len(volume_led) >= len(price_led)
                     else "CONTRACTING WORLD — falling pairs dominate"),
            "price_led": price_led, "volume_led": volume_led,
            "falling": falling}

    # ---- NEW CANARY C: stage persistence (how mature is each stage?) ----
    for p in pairs:
        streak = 0
        for day in sorted(hist["days"], reverse=True):
            st_then = (hist["days"][day].get(p["id"]) or {}).get("stage")
            if st_then == p["stage"]:
                streak += 1
            elif st_then:
                break
        p["stage_days"] = streak
        if streak >= 30 and p["stage"] == "EARLY_PRICE_LED":
            p["maturity"] = ("LATE-EARLY: this pricing-power phase has held "
                             f"{streak}d — historically where plateaus form; "
                             "tighten stops")
        elif streak <= 3 and p["stage"] != "NA":
            p["maturity"] = f"FRESH: {streak}d in this stage"

    # ---- NEW CANARY D: chokepoint attribution ----
    CHOKE_MAP = {"UAE-energy": ["hormuz"], "SA-oil": ["hormuz", "bab"],
                 "QA-lng": ["hormuz"], "CN-broad": ["malacca", "taiwan"],
                 "KR-semis": ["taiwan", "malacca"],
                 "TW-semis": ["taiwan"], "DE-machinery": ["suez", "gibraltar"],
                 "BR-commodities": ["panama"], "CL-copper": ["panama"]}
    chokes = {str(c.get("name", "")).lower(): c
              for c in (pw.get("chokepoints") or [])}
    for p in pairs:
        keys = CHOKE_MAP.get(p["id"]) or []
        hits = []
        for k in keys:
            for nm2, c in chokes.items():
                if k in nm2 and c.get("vs_baseline_pct") is not None:
                    hits.append({"name": c.get("name"),
                                 "vs_baseline_pct": c.get("vs_baseline_pct"),
                                 "status": c.get("status")})
                    break
        bad = [h for h in hits if (h.get("vs_baseline_pct") or 0) <= -20]
        if hits:
            p["chokepoints"] = hits[:2]
        if bad and p["stage"] in ("SUPPLY_SHOCK_PRICING", "EARLY_PRICE_LED"):
            p["choke_cause"] = (
                f"{bad[0]['name']} transit {bad[0]['vs_baseline_pct']}% — "
                "the price move has a physical cause upstream, not a demand "
                "cause; treat as supply shock until transit normalizes")

    # ---- MACRO VERDICT: two plain-English dials ----
    SLOW = {"CONTRACTION": 30, "VALUE_LED_DOWNTURN": 20,
            "PLATEAU_FORMING": 12, "SUPPLY_SHOCK_PRICING": 10,
            "MIXED": 5, "EARLY_PRICE_LED": -5, "BROADENING": -15}
    INFL = {"SUPPLY_SHOCK_PRICING": 30, "EARLY_PRICE_LED": 15,
            "BROADENING": 8, "PLATEAU_FORMING": -5,
            "VALUE_LED_DOWNTURN": -12, "CONTRACTION": -20, "MIXED": 0}
    g_pairs = [p for p in pairs
               if (p.get("canary") or {}).get("type") == "GROWTH"
               and p["stage"] != "NA"]
    i_pairs = [p for p in pairs
               if (p.get("canary") or {}).get("type") == "INFLATION"
               and p["stage"] != "NA"]
    inv_tilt = 0
    for p in pairs:
        f4 = p.get("factor4") or {}
        if f4.get("read") == "INVENTORY_BUILD":
            inv_tilt += 4
        elif f4.get("read") == "INVENTORY_DRAW":
            inv_tilt -= 3
        elif f4.get("read") == "IDLING":
            inv_tilt += 3
    inv_tilt = max(-12, min(12, inv_tilt))
    slow = min(100, max(0, 50 + inv_tilt + sum(SLOW.get(p["stage"], 0)
                                               for p in g_pairs)))
    infl = min(100, max(0, 50 + sum(INFL.get(p["stage"], 0)
                                    for p in i_pairs)))

    def band(x, hi, mid):
        return hi if x >= 70 else mid if x >= 40 else "LOW"

    slow_band = band(slow, "HIGH", "MODERATE")
    infl_band = band(infl, "HIGH", "MODERATE")
    weak = [p["label"].split(" · ")[0] for p in g_pairs
            if p["stage"] in ("CONTRACTION", "VALUE_LED_DOWNTURN",
                              "PLATEAU_FORMING")]
    hot = [p["label"].split(" · ")[0] for p in i_pairs
           if p["stage"] in ("SUPPLY_SHOCK_PRICING", "EARLY_PRICE_LED")]
    strong = [p["label"].split(" · ")[0] for p in g_pairs
              if p["stage"] in ("EARLY_PRICE_LED", "BROADENING")]
    parts = []
    parts.append(
        ("Global slowdown risk is " + slow_band + ": ")
        + (("physical demand is contracting in " + ", ".join(weak[:4]))
           if weak else "growth canaries are not flashing contraction")
        + ((" while " + ", ".join(strong[:3]) + " still expand")
           if strong else "") + ".")
    parts.append(
        ("Inflation pressure is " + infl_band + ": ")
        + (("prices are rising in " + ", ".join(hot[:3])
            + (" WITH volumes falling (supply-driven — the bad kind: "
               "raises prices AND hurts growth)"
               if any(p["stage"] == "SUPPLY_SHOCK_PRICING"
                      for p in i_pairs) else " on firm demand"))
           if hot else "inflation canaries are quiet")
        + ".")
    if slow >= 60 and infl >= 60:
        regime = "STAGFLATIONARY SQUEEZE"
    elif slow >= 60:
        regime = "GLOBAL SLOWDOWN"
    elif infl >= 60:
        regime = "INFLATIONARY BOOM"
    elif slow <= 40 and infl <= 40:
        regime = "GOLDILOCKS"
    else:
        regime = "MIXED / TRANSITIONAL"

    for _d in divergences:
        signals.append({
            "type": "DIVERGENCE", "pair": _d["commodity"],
            "line": _d["read"][:230],
            "trade": {"bias": "PAIR", "instruments": _d["trade"][:90],
                      "line": f"{_d['spread_pp']}pp spread"}})

    live = [p for p in pairs if p["stage"] != "NA"]
    doc = {"ok": len(live) >= 8, "version": VERSION,
           "generated_at": now.isoformat(), "pairs": pairs, "signals": signals,
           "divergences": divergences, "breadth": breadth,
           "macro": {"slowdown_risk": slow, "slowdown_band": slow_band,
                      "inflation_pressure": infl, "inflation_band": infl_band,
                      "regime": regime, "plain_english": " ".join(parts),
                      "growth_canaries": len(g_pairs),
                      "inventory_tilt": inv_tilt,
                      "factor4_reads": {p["id"]: (p.get("factor4") or {}).get("read")
                                         for p in pairs if p.get("factor4")},
                      "inflation_canaries": len(i_pairs),
                      "weak": weak, "hot": hot, "strong": strong},
           "signals_note": ("STAGE_CHANGE fires on flips; "
                            "SLIDING_WATCH pre-empts the flip "
                            "(needs >=1 prior day of history)"),
           "headline": next((f"{p['label']}: {p['stage']}"
                             for p in pairs
                             if p["stage"] == "EARLY_PRICE_LED"), None),
           "method": ("stage = f(value_yoy, volume_vs_baseline): "
                     "SUPPLY_SHOCK_PRICING v>=10 & vol<=-15 (price up, flow "
                     "collapsing — inflation signature); EARLY_PRICE_LED "
                     "v>=10 & vol in [-15,5]; BROADENING v>=8 & vol>8; "
                     "PLATEAU_FORMING v in [0,8) & vol>8; VALUE_LED_DOWNTURN "
                     "v<0<=vol; CONTRACTION v<0 & vol<-8. Value legs = "
                     "export value/orders/credit/commodity price; volume "
                     "legs = AIS port calls; 4th factor = capacity "
                     "utilization + inventories/sales; canary class flips to "
                     "INFLATION when a growth pair prints SUPPLY_SHOCK."),
           "doctrine": ("value-vs-volume: surging value on flat volume = "
                        "pricing power (early); volume catch-up with "
                        "cooling value = plateau forming")}
    S3.put_object(Bucket=BUCKET, Key=KEY,
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=1800")
    print(f"[boom-stage] live={len(live)}/3 "
          f"stages={[(p['id'], p['stage']) for p in pairs]}")
    return {"ok": doc["ok"], "stages": [(p["id"], p["stage"])
                                         for p in pairs]}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))

# NOTE (ops 3696): CH-pharma pair REMOVED — Switzerland is landlocked
# (no AIS port volume leg exists) and pharma demand is acyclical by
# design, so it cannot signal expansion or slowdown. Khalid's filter:
# a pair earns its place only if it serves as a growth or inflation
# canary. Better Swiss-pharma reads live in equity-research, not here.
