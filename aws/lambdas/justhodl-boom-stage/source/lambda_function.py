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

VERSION = "1.1.0"
BUCKET = "justhodl-dashboard-live"
KEY = "data/boom-stage.json"
S3 = boto3.client("s3", region_name="us-east-1")


UA = {"User-Agent": "Mozilla/5.0"}


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


TRADE_MAP = {
    "KR-semis": {
        "EARLY_PRICE_LED": ("LONG", "MU / SOXX / 000660.KS-proxy",
                            "memory pricing-power phase — ride it"),
        "PLATEAU_FORMING": ("TRIM", "MU / SOXX",
                            "volume caught up, price cooling — the RAM "
                            "sell-signal"),
        "CONTRACTION": ("AVOID", "semis complex", "cycle rolling over")},
    "TW-electronics": {
        "EARLY_PRICE_LED": ("LONG", "TSM / EWT", "orders value-led"),
        "PLATEAU_FORMING": ("TRIM", "TSM / EWT", "orders cooling vs volume"),
        "CONTRACTION": ("AVOID", "TSM / EWT", "orders and ships both down")},
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

    # TW - electronics/export orders (value yoy self-builds at 12m cache)
    tw = (asia.get("taiwan_orders") or {})
    v_yoy2 = tw.get("yoy_pct")
    vp2, vz2, npt2 = vol("Taiwan")
    st2, why2 = _stage(v_yoy2, vp2)
    pairs.append({
        "id": "TW-electronics", "label": "Taiwan · Electronics (export orders)",
        "stage": st2, "why": why2,
        "value": {"src": "asia-leads TW export orders",
                  "yoy_pct": v_yoy2,
                  "level_usd_bn": tw.get("orders_usd_bn"),
                  "note": ("yoy self-building via levels cache"
                           if v_yoy2 is None else "")},
        "volume": {"src": "portwatch Taiwan gateway ports",
                   "vs_baseline_pct": vp2, "z": vz2, "n_ports": npt2},
        "context": {},
    })

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

    live = [p for p in pairs if p["stage"] != "NA"]
    doc = {"ok": len(live) >= 4, "version": VERSION,
           "generated_at": now.isoformat(), "pairs": pairs, "signals": signals,
           "signals_note": ("STAGE_CHANGE fires on flips; "
                            "SLIDING_WATCH pre-empts the flip "
                            "(needs >=1 prior day of history)"),
           "headline": next((f"{p['label']}: {p['stage']}"
                             for p in pairs
                             if p["stage"] == "EARLY_PRICE_LED"), None),
           "method": ("stage = f(value_yoy, volume_vs_baseline): "
                      "EARLY_PRICE_LED v>=15 & vol in [-12,8]; BROADENING "
                      "v>=8 & vol>8; PLATEAU v in [0,8) & vol>8; "
                      "CONTRACTION v<0 & vol<-8; value legs = exports/"
                      "orders/credit; volume legs = AIS port calls"),
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
