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
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
BUCKET = "justhodl-dashboard-live"
KEY = "data/boom-stage.json"
S3 = boto3.client("s3", region_name="us-east-1")


def _get(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def _stage(v_yoy, vol_pct):
    if v_yoy is None or vol_pct is None:
        return "NA", "missing leg"
    if v_yoy >= 15 and -12 <= vol_pct <= 8:
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

    live = [p for p in pairs if p["stage"] != "NA"]
    doc = {"ok": len(live) >= 2, "version": VERSION,
           "generated_at": now.isoformat(), "pairs": pairs,
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
