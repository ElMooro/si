"""justhodl-risk-regime — authoritative cross-asset Risk-On/Risk-Off synthesizer.

Fuses the entitled Massive data (FX RORO + options put/call & skew) with FRED VIX
and HY credit as cross-confirmation (Massive VIX/futures are NOT entitled — probed
ops 1939) into ONE risk_regime_score [-100 risk-off .. +100 risk-on] + regime.

Blocks (each normalized to [-1..+1], + = risk-on), reweighted over whatever is live:
  • FX RORO        0.35  data/polygon-fx-regime.json fx_roro_score/100
  • Options (Massive) 0.20  SPY put/call vol ratio + 25d put-skew; HYG put demand
  • VIX (FRED)     0.25  VIXCLS level + term structure (VIXCLS vs VXVCLS)
  • Credit (FRED)  0.20  HY OAS (BAMLH0A0HYM2) level percentile + 5d change

OUTPUT: data/risk-regime.json  (+ data/risk-regime-state.json for transition alerts)
"""
import json, os, time, urllib.request, urllib.parse, statistics
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

S3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/risk-regime.json"
STATE_KEY = "data/risk-regime-state.json"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
TG_TOKEN = os.environ.get("TELEGRAM_TOKEN", "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

try:
    from massive import get_massive_key, MASSIVE_BASE
    MKEY = get_massive_key()
except Exception:
    MKEY, MASSIVE_BASE = "", "https://api.massive.com"


def _http(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-risk-regime/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _read(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _clip(x, lo=-1.0, hi=1.0):
    return max(lo, min(hi, x))


# ── FRED ──
def fred_series(sid, days=400):
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}"
           f"&api_key={FRED_KEY}&file_type=json&observation_start={start}&observation_end={end}")
    try:
        obs = _http(url).get("observations", [])
        return [(o["date"], float(o["value"])) for o in obs if o.get("value") not in (".", None, "")]
    except Exception as e:
        print(f"[fred] {sid}: {e}")
        return []


def vix_block():
    """VIX level + term structure. + = risk-on (low/contango), - = risk-off."""
    vix = fred_series("VIXCLS", 120)
    vxv = fred_series("VXVCLS", 120)   # 3-month VIX
    if not vix:
        return None, {}
    lvl = vix[-1][1]
    # level score: 13->+1 (calm), 20->0, 30->-1, 40+ -> -1.5 clipped
    level_score = _clip((19.0 - lvl) / 8.0)
    # term structure: VIXCLS - VXVCLS ; backwardation (spot>3mo) = stress
    ts_score = 0.0
    ts = None
    if vxv:
        ts = round(lvl - vxv[-1][1], 2)
        ts_score = _clip(-ts / 2.5)   # spot well below 3mo (contango) = calm = +
    score = _clip(0.6 * level_score + 0.4 * ts_score)
    tells = []
    if lvl >= 25:
        tells.append(f"RISK-OFF: VIX elevated ({lvl:.1f})")
    if ts is not None and ts > 0:
        tells.append(f"RISK-OFF: VIX backwardation (spot-3mo {ts:+.1f})")
    return score, {"vix": lvl, "vix_3m": vxv[-1][1] if vxv else None,
                   "term_structure": ts, "level_score": round(level_score, 2),
                   "ts_score": round(ts_score, 2), "tells": tells}


def credit_block():
    """HY OAS level percentile + 5d change. Widening = risk-off."""
    hy = fred_series("BAMLH0A0HYM2", 800)
    if len(hy) < 30:
        return None, {}
    vals = [v for _, v in hy]
    cur = vals[-1]
    lo, hi = min(vals), max(vals)
    pctile = (cur - lo) / (hi - lo) if hi > lo else 0.5
    chg_5d = cur - vals[-6] if len(vals) >= 6 else 0.0
    # low OAS = calm = +; high percentile = stress = -
    level_score = _clip(1.0 - 2.0 * pctile)
    chg_score = _clip(-chg_5d / 0.35)   # +35bp widening in 5d ~ -1
    score = _clip(0.5 * level_score + 0.5 * chg_score)
    tells = []
    if chg_5d > 0.20:
        tells.append(f"RISK-OFF: HY credit widening (+{chg_5d*100:.0f}bp 5d)")
    if pctile > 0.7:
        tells.append(f"RISK-OFF: HY OAS high ({cur:.2f}, {pctile*100:.0f}%ile)")
    if chg_5d < -0.20:
        tells.append(f"RISK-ON: HY credit tightening ({chg_5d*100:.0f}bp 5d)")
    return score, {"hy_oas": round(cur, 2), "pctile": round(pctile, 2),
                   "chg_5d_bp": round(chg_5d * 100, 1), "tells": tells}


# ── Massive options ──
def _spot(ticker):
    try:
        d = _http(f"{MASSIVE_BASE}/v2/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={MKEY}", 10)
        return (d.get("results") or [{}])[0].get("c")
    except Exception:
        return None


def _options_snapshot(underlying, spot):
    """Pull near-the-money contracts (±12% strikes, nearest expiries) for put/call + skew."""
    if not spot:
        return []
    lo, hi = spot * 0.88, spot * 1.12
    out, url = [], (
        f"{MASSIVE_BASE}/v3/snapshot/options/{underlying}"
        f"?strike_price.gte={lo:.2f}&strike_price.lte={hi:.2f}&limit=250&apiKey={MKEY}")
    for _ in range(3):  # up to ~750 contracts
        try:
            d = _http(url, 15)
        except Exception:
            break
        out += d.get("results", [])
        nxt = d.get("next_url")
        if not nxt:
            break
        url = nxt + (f"&apiKey={MKEY}" if "apiKey" not in nxt else "")
    return out


def _opt_metrics(contracts, spot):
    calls_v = puts_v = 0
    put_ivs, call_ivs = [], []
    for c in contracts:
        det = c.get("details", {}) or {}
        typ = det.get("contract_type")
        vol = (c.get("day", {}) or {}).get("volume") or 0
        oi = c.get("open_interest") or 0
        iv = c.get("implied_volatility")
        strike = det.get("strike_price")
        delta = (c.get("greeks", {}) or {}).get("delta")
        w = vol + 0.25 * oi
        if typ == "call":
            calls_v += w
            if iv and delta is not None and 0.15 <= abs(delta) <= 0.35:
                call_ivs.append(iv)
        elif typ == "put":
            puts_v += w
            if iv and delta is not None and 0.15 <= abs(delta) <= 0.35:
                put_ivs.append(iv)
    if calls_v + puts_v == 0:
        return None
    pcr = puts_v / max(calls_v, 1)
    skew = None
    if put_ivs and call_ivs:
        skew = statistics.mean(put_ivs) - statistics.mean(call_ivs)
    return {"put_call_ratio": round(pcr, 3), "skew_25d": round(skew, 4) if skew is not None else None,
            "n_contracts": len(contracts)}


def options_block():
    if not MKEY:
        return None, {}
    res, meta = {}, {}
    spots = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        for u, s in zip(("SPY", "HYG"), ex.map(_spot, ("SPY", "HYG"))):
            spots[u] = s
    for u in ("SPY", "HYG"):
        contracts = _options_snapshot(u, spots.get(u))
        m = _opt_metrics(contracts, spots.get(u))
        if m:
            res[u] = m
    if not res.get("SPY"):
        return None, res
    spy = res["SPY"]
    # PCR ~0.9 neutral; >1.3 fear (risk-off); skew higher = fear
    pcr_score = _clip((0.95 - spy["put_call_ratio"]) / 0.45)
    skew_score = 0.0
    if spy.get("skew_25d") is not None:
        skew_score = _clip((0.025 - spy["skew_25d"]) / 0.04)
    score = _clip(0.6 * pcr_score + 0.4 * skew_score)
    tells = []
    if spy["put_call_ratio"] > 1.25:
        tells.append(f"RISK-OFF: SPY put/call elevated ({spy['put_call_ratio']:.2f})")
    if spy.get("skew_25d") and spy["skew_25d"] > 0.05:
        tells.append(f"RISK-OFF: SPY put skew steep ({spy['skew_25d']:.3f})")
    res["tells"] = tells
    res["score"] = round(score, 2)
    return score, res


def lambda_handler(event, context):
    t0 = time.time()
    # FX block (already computed upstream)
    fx = _read("data/polygon-fx-regime.json") or {}
    fx_score_raw = ((fx.get("fx_roro") or {}).get("fx_roro_score"))
    fx_score = (fx_score_raw / 100.0) if isinstance(fx_score_raw, (int, float)) else None
    fx_tells = ((fx.get("fx_roro") or {}).get("tells")) or []

    blocks, results = [], {}
    if fx_score is not None:
        blocks.append(("fx", 0.35, fx_score)); results["fx"] = {
            "score": round(fx_score, 2), "fx_roro_score": fx_score_raw,
            "regime": (fx.get("fx_roro") or {}).get("fx_roro_regime"), "tells": fx_tells}

    with ThreadPoolExecutor(max_workers=3) as ex:
        f_vix = ex.submit(vix_block)
        f_cred = ex.submit(credit_block)
        f_opt = ex.submit(options_block)
        vix_s, vix_m = f_vix.result()
        cred_s, cred_m = f_cred.result()
        opt_s, opt_m = f_opt.result()
    if opt_s is not None:
        blocks.append(("options", 0.20, opt_s)); results["options"] = opt_m
    if vix_s is not None:
        blocks.append(("vix", 0.25, vix_s)); results["vix"] = vix_m
    if cred_s is not None:
        blocks.append(("credit", 0.20, cred_s)); results["credit"] = cred_m

    tw = sum(w for _, w, _ in blocks)
    composite = sum(w * s for _, w, s in blocks) / tw if tw else 0.0
    score = round(_clip(composite) * 100, 1)

    if score >= 35:
        regime = "RISK_ON"
    elif score >= 12:
        regime = "MILD_RISK_ON"
    elif score > -12:
        regime = "NEUTRAL"
    elif score > -35:
        regime = "MILD_RISK_OFF"
    else:
        regime = "RISK_OFF"
    # flight-to-quality override: havens bid AND vix/credit stressed
    havens = (fx.get("fx_roro") or {}).get("havens_bid_count", 0) or 0
    if score <= -35 and havens >= 2 and (vix_m.get("vix", 0) or 0) >= 22:
        regime = "FLIGHT_TO_QUALITY"

    all_tells = []
    for b in ("fx", "options", "vix", "credit"):
        all_tells += (results.get(b, {}) or {}).get("tells", [])

    # position-sizing guidance (consumed by rankers/hedge)
    if score >= 35:
        posture = {"beta_tilt": "lean_long_high_beta", "size_mult": 1.10, "hedge": "light"}
    elif score >= 12:
        posture = {"beta_tilt": "neutral_to_long", "size_mult": 1.05, "hedge": "normal"}
    elif score > -12:
        posture = {"beta_tilt": "neutral", "size_mult": 1.0, "hedge": "normal"}
    elif score > -35:
        posture = {"beta_tilt": "reduce_high_beta", "size_mult": 0.9, "hedge": "raise"}
    else:
        posture = {"beta_tilt": "defensive_low_vol_quality", "size_mult": 0.75, "hedge": "max"}

    # ── participation / breadth overlay (market-internals; confirmation, not core score) ──
    bi = _read("data/market-internals.json") or {}
    rot = bi.get("rotation") or {}
    mc = bi.get("mcclellan") or {}
    participation = None
    if rot:
        rstate = rot.get("state")
        risk_on = score >= 12
        risk_off = score <= -12
        if rstate == "GREAT_ROTATION":
            conf = "ROTATION"
            pnote = ("Breadth broadly positive while the cap-weighted index is red — leadership rotating "
                     "out of mega-caps. Index-level RORO understates the broad market's strength.")
        elif risk_on and rstate == "NARROW_MEGACAP":
            conf = "DIVERGENT"
            pnote = ("Risk-on by cross-asset tells, but breadth is narrow (mega-cap-led). Fragile advance — "
                     "discounting conviction.")
            posture = dict(posture)
            posture["size_mult"] = round(posture["size_mult"] * 0.95, 3)
            posture["participation_warning"] = "narrow_breadth"
        elif risk_on and rstate == "BROAD_RALLY":
            conf = "CONFIRMED"; pnote = "Risk-on confirmed by broad participation."
        elif risk_off and rstate == "BROAD_SELLOFF":
            conf = "CONFIRMED"; pnote = "Risk-off confirmed by broad-based selling."
        else:
            conf = "NEUTRAL"; pnote = "Breadth roughly aligned with the cross-asset read."
        participation = {"confirmation": conf, "rotation_state": rstate,
                         "mcclellan_osc": mc.get("oscillator"), "mcclellan_state": mc.get("state"),
                         "pct_advancers": rot.get("pct_advancers"), "ad_ratio": rot.get("ad_ratio"),
                         "vol_ratio": rot.get("vol_ratio"), "funded": rot.get("funded"), "note": pnote}
        all_tells.append(f"Breadth: {rstate} ({conf})")

    out = {
        "engine": "risk-regime", "version": "1.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "risk_regime_score": score, "risk_regime": regime,
        "scale": "-100 = risk-off / flight-to-quality .. +100 = risk-on",
        "posture": posture,
        "participation": participation,
        "blocks_used": [{"block": b, "weight": w, "score": round(s, 3)} for b, w, s in blocks],
        "components": results,
        "tells": all_tells,
        "elapsed_s": round(time.time() - t0, 2),
        "sources": "Massive FX+options (entitled) + FRED VIX/HY-OAS cross-confirm",
        "methodology": "Weighted cross-asset RORO; blocks reweighted over live inputs. "
                       "Massive VIX/futures not entitled -> VIX/credit sourced from FRED.",
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")

    # transition alert
    prev = _read(STATE_KEY) or {}
    prev_regime = prev.get("risk_regime")
    if prev_regime and prev_regime != regime and not event.get("seed"):
        try:
            arrow = "🟢" if score > (prev.get("risk_regime_score") or 0) else "🔴"
            msg = (f"{arrow} RISK REGIME SHIFT: {prev_regime} → {regime} (score {score})\n"
                   + "\n".join(f"• {t}" for t in all_tells[:5]))
            urllib.request.urlopen(urllib.request.Request(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data=urllib.parse.urlencode({"chat_id": TG_CHAT, "text": msg}).encode()), timeout=10)
        except Exception as e:
            print("[tg]", e)
    S3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                  Body=json.dumps({"risk_regime": regime, "risk_regime_score": score,
                                   "generated_at": out["generated_at"]}).encode(),
                  ContentType="application/json")

    print(f"[risk-regime] score={score} {regime} | blocks={[b for b,_,_ in blocks]} | {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "risk_regime_score": score,
            "risk_regime": regime, "blocks": [b for b, _, _ in blocks], "tells": all_tells[:6]})}
