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


def liquidity_block():
    """Composite liquidity-inflection regime as a risk-on/off input.
    + = liquidity expanding (risk-on tailwind); − = contracting (headwind).
    Reads the blended second-derivative score from the liquidity-inflection engine."""
    j = _read("data/liquidity-inflection.json") or {}
    comp = j.get("composite") or {}
    ls = comp.get("liquidity_score")
    if not isinstance(ls, (int, float)):
        return None, {}
    score = _clip((ls - 50) / 25.0)   # 0-100, 50 neutral → ±25pts ≈ ±1
    tells = []
    reg = comp.get("regime")
    if reg == "EXPANDING":
        tells.append("Liquidity inflecting UP — net-liquidity tailwind")
    elif reg == "CONTRACTING":
        tells.append("Liquidity inflecting DOWN — draining conditions")
    res = j.get("reserves") or {}
    if isinstance(res.get("scarcity_note"), str) and "Below" in res["scarcity_note"]:
        tells.append("Bank reserves below comfort floor (LCLoR risk)")
    # forward trajectory nudge — where the plumbing says liquidity is HEADED
    traj = j.get("trajectory") or {}
    heading = traj.get("heading")
    if heading == "TIGHTENING AHEAD":
        score = _clip(score - 0.25); tells.append("Liquidity trajectory: TIGHTENING AHEAD")
    elif heading == "EASING AHEAD":
        score = _clip(score + 0.25); tells.append("Liquidity trajectory: EASING AHEAD")
    # dollar shortage — an offshore USD scramble is acutely risk-off
    ds = j.get("dollar_shortage") or {}
    ds_status = ds.get("status")
    if ds_status == "SCRAMBLE":
        score = _clip(score - 0.6); tells.append("Dollar SHORTAGE scramble — offshore USD funding stress")
    elif ds_status == "WATCH":
        tells.append("Dollar funding on WATCH")
    m = {"score": round(score, 2), "liquidity_score": ls, "regime": reg,
         "composite_z": comp.get("composite_z"), "trajectory": heading,
         "dollar_shortage": ds_status, "tells": tells}
    return score, m


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

    liq_s, liq_m = liquidity_block()
    if liq_s is not None:
        blocks.append(("liquidity_regime", 0.15, liq_s)); results["liquidity_regime"] = liq_m

    # funding block -- overnight repo plumbing from the repo-market engine
    rp = _read("data/repo-market.json") or {}
    rp_score = rp.get("repo_stress_score")
    if isinstance(rp_score, (int, float)):
        # 0-100 stress -> [-1..+1] risk signal: calm funding is mildly
        # risk-on, a seizing repo market is fully risk-off.
        f_s = max(-1.0, min(1.0, (35.0 - rp_score) / 45.0))
        blocks.append(("funding", 0.15, f_s))
        results["funding"] = {
            "score": round(f_s, 2), "repo_stress_score": rp_score,
            "regime": rp.get("regime"),
            "tail_bps": (rp.get("distribution") or {}).get("tail_bps"),
            "sofr_iorb_bps": ((rp.get("spreads") or {}).get("sofr_iorb")
                              or {}).get("bps"),
            "source": "repo-market engine"}

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
    # dollar-shortage override: an offshore USD scramble forces flight-to-quality
    if (results.get("liquidity_regime") or {}).get("dollar_shortage") == "SCRAMBLE":
        regime = "FLIGHT_TO_QUALITY"

    all_tells = []
    for b in ("fx", "options", "vix", "credit", "liquidity_regime"):
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

    # ── cross-border flow overlay (hot-money; capital-flight confirmation, not core score) ──
    hm = _read("data/hot-money.json") or {}
    cross_border = None
    if hm.get("inflow_leaders") is not None:
        EMR = {"LatAm", "Asia", "MEA"}
        infl = [c for c in hm.get("inflow_leaders", []) if c.get("region") in EMR
                and c.get("conviction") in ("TWIN_ENGINE", "CONFIRMED_INFLOW", "EARLY_ACCUMULATION")]
        outfl = [c for c in hm.get("outflow_leaders", []) if c.get("region") in EMR
                 and c.get("conviction") in ("CONFIRMED_OUTFLOW", "OUTFLOW")]
        em_debt_sig = (hm.get("em_debt_flows") or {}).get("signal") or ""
        seek = len(infl) - len(outfl) + (1 if em_debt_sig.startswith("INFLOW") else
                                         -1 if em_debt_sig.startswith("OUTFLOW") else 0)
        flow_state = "RISK_SEEKING" if seek >= 2 else "RISK_AVERSE" if seek <= -2 else "MIXED"
        risk_on = score >= 12; risk_off = score <= -12
        if risk_on and flow_state == "RISK_AVERSE":
            cconf = "DIVERGENT"
            cnote = ("Cross-asset tells say risk-on, but cross-border capital is leaving EM/high-beta "
                     "(equity + bond outflows). Foreign money not confirming — discount conviction.")
            posture = dict(posture)
            posture["size_mult"] = round(posture["size_mult"] * 0.97, 3)
            posture["crossborder_warning"] = "em_capital_outflow"
        elif risk_off and flow_state == "RISK_SEEKING":
            cconf = "DIVERGENT"
            cnote = "Risk-off tells, yet capital still chasing EM inflows — early bottoming or complacency."
        elif risk_on and flow_state == "RISK_SEEKING":
            cconf = "CONFIRMED"; cnote = "Risk-on confirmed — capital flowing into EM/high-beta (equity + debt)."
        elif risk_off and flow_state == "RISK_AVERSE":
            cconf = "CONFIRMED"; cnote = "Risk-off confirmed — cross-border capital flight from EM."
        else:
            cconf = "NEUTRAL"; cnote = "Cross-border flows roughly aligned with the cross-asset read."
        cross_border = {"confirmation": cconf, "flow_state": flow_state,
                        "em_inflow_countries": len(infl), "em_outflow_countries": len(outfl),
                        "em_debt_signal": em_debt_sig,
                        "top_inflows": [c["country"] for c in hm.get("inflow_leaders", [])[:3]],
                        "top_outflows": [c["country"] for c in hm.get("outflow_leaders", [])[:3]],
                        "note": cnote}
        all_tells.append(f"Cross-border: {flow_state} ({cconf})")

    # ── systemic-stress overlay (wires 7 stress/liquidity/credit islands; confirmation, not core score) ──
    def _lvl_from_score(s, hi, mid):
        if not isinstance(s, (int, float)):
            return None
        return "STRESSED" if s >= hi else "ELEVATED" if s >= mid else "CALM"
    _LMAP = {"CALM": "CALM", "NORMAL": "CALM", "LOW": "CALM", "WATCH": "ELEVATED", "ELEVATED": "ELEVATED",
             "MILD": "ELEVATED", "WARNING": "STRESSED", "CRISIS": "STRESSED", "STRESSED": "STRESSED",
             "BREAKING": "STRESSED", "BREAK": "STRESSED", "TIGHTENING": "ELEVATED"}
    sr = {}
    _bs = _read("data/bank-stress.json") or {}
    sr["bank_funding"] = _lvl_from_score(_bs.get("bank_stress_score"), 55, 30)
    _cc = _read("data/crisis-canaries.json") or {}
    sr["crisis_canaries"] = _LMAP.get(str(_cc.get("level") or _cc.get("level_v3") or "").upper())
    _pl = _read("data/plumbing-stress.json") or {}
    sr["funding_plumbing"] = _LMAP.get(str(_pl.get("composite_label") or "").upper()) or _lvl_from_score(_pl.get("composite_score"), 66, 40)
    _cds = _read("data/cds-monitor.json") or {}
    _gcs = _cds.get("global_credit_stress")
    sr["credit_cds"] = _lvl_from_score(_gcs, 55, 30) if isinstance(_gcs, (int, float)) else _LMAP.get(str(_gcs).upper())
    _ced = _read("data/credit-equity-divergence.json") or {}
    _css = _ced.get("signal_strength")
    sr["credit_equity_div"] = ("STRESSED" if (_css or 0) >= 0.6 else "ELEVATED" if (_css or 0) >= 0.3 else "CALM") if _ced.get("state") else None
    _cor = _read("data/correlation-breaks.json") or {}
    sr["correlation_breaks"] = _LMAP.get(str(_cor.get("signal") or "").upper())
    _cax = _read("data/cross-asset-confirm.json") or {}
    _car = str(_cax.get("regime") or "").upper()
    sr["cross_asset"] = "STRESSED" if ("OFF" in _car or "STRESS" in _car) else ("CALM" if _car else None)
    sr = {k: v for k, v in sr.items() if v}
    systemic_stress = None
    if sr:
        n_stressed = sum(1 for v in sr.values() if v == "STRESSED")
        n_elev = sum(1 for v in sr.values() if v in ("ELEVATED", "STRESSED"))
        stress_level = "STRESSED" if n_stressed >= 2 else "ELEVATED" if n_elev >= 2 else "CALM"
        risk_on = score >= 12; risk_off = score <= -12
        if risk_on and stress_level in ("ELEVATED", "STRESSED"):
            sconf = "DIVERGENT"
            snote = (f"{n_elev} systemic-stress gauges elevated while the cross-asset tape reads risk-on — "
                     "fragile advance; trimming size.")
            posture = dict(posture)
            posture["size_mult"] = round(posture["size_mult"] * 0.96, 3)
            posture["stress_warning"] = stress_level.lower()
        elif risk_off and stress_level == "STRESSED":
            sconf = "CONFIRMED"; snote = "Risk-off confirmed by broad systemic-stress gauges."
        else:
            sconf = "ALIGNED" if stress_level == "CALM" else "WATCH"
            snote = f"Systemic stress {stress_level} ({n_elev} of {len(sr)} gauges elevated)."
        systemic_stress = {"level": stress_level, "confirmation": sconf, "n_gauges": len(sr),
                           "n_elevated": n_elev, "readings": sr, "note": snote}
        all_tells.append(f"Systemic stress: {stress_level} ({sconf})")

    # ── liquidity overlay (wires the liquidity cluster; core RORO driver, confirmation not core score) ──
    def _tilt(regime, pos, neg):
        r = str(regime or "").upper()
        if any(w in r for w in pos):
            return 1
        if any(w in r for w in neg):
            return -1
        return 0
    POSL = ("EXPAND", "EASING", "EASE", "ABUNDANT", "AMPLE", "TAILWIND", "RISING", "LOADED", "INJECT")
    NEGL = ("TIGHT", "CONTRACT", "DRAIN", "SCARCE", "STRESS", "HEADWIND", "FALLING")
    lr = {}
    _gl = _read("data/global-liquidity.json") or {}
    lr["global"] = _tilt(_gl.get("regime"), POSL, NEGL)
    _chl = _read("data/china-liquidity.json") or {}
    lr["china"] = _tilt(_chl.get("regime"), POSL, NEGL)
    _rl = _read("data/repo-lending.json") or {}
    lr["repo_funding"] = _tilt(_rl.get("regime"), POSL, NEGL)
    _cbi = _read("data/cb-injection.json") or {}
    _imp = _cbi.get("global_injection_impulse")
    lr["cb_injection"] = (1 if _imp > 0 else -1 if _imp < 0 else 0) if isinstance(_imp, (int, float)) else 0
    _cl = _read("data/crypto-liquidity.json") or {}
    lr["crypto_drypowder"] = _tilt(_cl.get("regime"), POSL, NEGL)
    lr = {k: v for k, v in lr.items() if k in lr}
    net_liq = sum(lr.values())
    liquidity = None
    if any(_read("data/" + f + ".json") for f in ("global-liquidity", "repo-lending")):
        liq_state = "EXPANSIONARY" if net_liq >= 2 else "CONTRACTIONARY" if net_liq <= -2 else "NEUTRAL"
        risk_on = score >= 12; risk_off = score <= -12
        if risk_on and liq_state == "CONTRACTIONARY":
            lconf = "DIVERGENT"
            lnote = "Risk-on tape without liquidity support (tightening) — advances on contracting liquidity are fragile; trimming."
            posture = dict(posture)
            posture["size_mult"] = round(posture["size_mult"] * 0.97, 3)
            posture["liquidity_warning"] = "contracting"
        elif risk_off and liq_state == "EXPANSIONARY":
            lconf = "DIVERGENT"; lnote = "Risk-off tells, but liquidity is expansionary — a cushion that often precedes a turn."
        elif risk_on and liq_state == "EXPANSIONARY":
            lconf = "CONFIRMED"; lnote = "Risk-on supported by expanding liquidity — the durable kind of advance."
        elif risk_off and liq_state == "CONTRACTIONARY":
            lconf = "CONFIRMED"; lnote = "Risk-off reinforced by contracting liquidity."
        else:
            lconf = "ALIGNED"; lnote = f"Liquidity {liq_state.lower()} (net tilt {net_liq:+d})."
        liquidity = {"state": liq_state, "confirmation": lconf, "net_tilt": net_liq, "readings": lr, "note": lnote}
        all_tells.append(f"Liquidity: {liq_state} ({lconf})")

    # ── capital-inflows overlay (foreign funding of US assets — the dollar/RORO tap) ──
    capital_inflows = None
    _ci = _read("data/capital-inflows.json") or {}
    if _ci.get("ok"):
        ci_reg = _ci.get("regime")
        into12 = (_ci.get("headline") or {}).get("foreign_net_into_us_lt_12mo_b")
        risk_on = score >= 12; risk_off = score <= -12
        supportive = ci_reg in ("ACCELERATING_INFLOW", "STEADY_INFLOW")
        draining = ci_reg in ("SUDDEN_STOP", "PERSISTENT_OUTFLOW")
        if risk_on and draining:
            cconf = "DIVERGENT"
            cnote = "Risk-on tape while foreign capital is leaving US assets — advances without foreign funding are fragile; trimming."
            posture = dict(posture); posture["size_mult"] = round(posture["size_mult"] * 0.96, 3)
            posture["capital_flow_warning"] = str(ci_reg).lower()
        elif risk_off and draining:
            cconf = "CONFIRMED"; cnote = "Risk-off reinforced by foreign capital draining out of US assets."
        elif risk_on and supportive:
            cconf = "CONFIRMED"; cnote = "Risk-on funded by steady foreign inflows — the durable kind of advance."
        elif draining:
            cconf = "WARNING"; cnote = "Foreign funding of US assets is draining — a building headwind even if the tape hasn't turned."
        else:
            cconf = "ALIGNED"; cnote = f"Foreign funding {str(ci_reg).replace('_', ' ').lower()}."
        capital_inflows = {"regime": ci_reg, "foreign_net_into_us_lt_12mo_b": into12,
                           "confirmation": cconf, "note": cnote}
        all_tells.append(f"Capital inflows: {ci_reg} ({cconf})")

    # ── secondary risk overlay: aggregate the vol-structure / credit / stress engines
    #    (these alpha-graded islands fed nothing; they now corroborate the RORO read) ──
    RISK_CHECKS = [
        ("data/systemic-stress.json", "regime", {"ELEVATED", "STRESSED", "CRISIS"}, "systemic stress"),
        ("data/tail-risk.json", "regime", {"ELEVATED", "STRESSED"}, "tail risk elevated"),
        ("data/vix9d-vix-inversion.json", "state",
         {"FULL_INVERSION_ACTIVE", "FULL_INVERSION_RICH", "PARTIAL_INVERSION"}, "VIX term-structure inverted"),
        ("data/credit-equity-divergence.json", "state",
         {"CREDIT_BEAR_ACTIVE", "CREDIT_BEAR_RICH"}, "credit leading equities lower"),
        ("data/vix-backwardation-trigger.json", "state", {"FIRED", "ARMED"}, "VIX backwardation"),
        ("data/vvix-vov-regime.json", "state", {"HIGH", "ELEVATED", "STRESS", "EXTREME", "RISK_OFF"}, "vol-of-vol elevated"),
        ("data/vol-target-unwind.json", "signal", {"UNWIND", "WARNING", "ELEVATED", "RISK_OFF"}, "vol-target unwind risk"),
        ("data/cds-monitor.json", "regime", {"WIDENING", "STRESS", "STRESSED", "ELEVATED", "RISK_OFF"}, "CDS widening"),
        ("data/breadth-divergence.json", "state", {"BEARISH", "DIVERGENT", "WARNING"}, "breadth divergence"),
        ("data/correlation-breaks.json", "state", {"BREAK", "ELEVATED", "WARNING", "STRESSED"}, "correlation breakdown"),
    ]
    fired, available = [], 0
    for key, field, riskvals, label in RISK_CHECKS:
        doc = _read(key)
        if not doc:
            continue
        available += 1
        val = str(doc.get(field) or doc.get("regime") or doc.get("state") or doc.get("signal") or "").upper()
        if val in {v.upper() for v in riskvals}:
            fired.append(label)
    secondary_risk = None
    if available:
        n = len(fired)
        risk_on = score >= 12
        risk_off = score <= -12
        if risk_on and n >= 2:
            sconf = "DIVERGENT"
            snote = (f"Cross-asset RORO reads risk-on, but {n} vol/credit/stress gauges are flashing "
                     f"({', '.join(fired[:4])}). Underlying plumbing not confirming — discount conviction.")
            posture = dict(posture)
            posture["size_mult"] = round(posture["size_mult"] * 0.95, 3)
            posture["secondary_risk_warning"] = "vol_credit_stress_firing"
        elif risk_off and n >= 2:
            sconf = "CONFIRMED"; snote = f"Risk-off corroborated by {n} vol/credit/stress gauges firing."
        elif n >= 3:
            sconf = "STRESS_BUILDING"; snote = f"{n} secondary risk gauges firing ({', '.join(fired[:4])}) — watch."
        else:
            sconf = "QUIET"; snote = f"Secondary risk gauges quiet ({n}/{available} firing)."
        secondary_risk = {"confirmation": sconf, "n_firing": n, "n_available": available,
                          "firing": fired, "note": snote}
        if n:
            all_tells.append(f"Secondary risk: {n} firing ({sconf})")

    out = {
        "engine": "risk-regime", "version": "1.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "wl_research": __import__("wl_fusion").block(('STRESS',)),
        "risk_regime_score": score, "risk_regime": regime,
        "scale": "-100 = risk-off / flight-to-quality .. +100 = risk-on",
        "posture": posture,
        "participation": participation,
        "cross_border": cross_border,
        "systemic_stress": systemic_stress,
        "liquidity": liquidity,
        "capital_inflows": capital_inflows,
        "secondary_risk": secondary_risk,
        "blocks_used": [{"block": b, "weight": w, "score": round(s, 3)} for b, w, s in blocks],
        "components": results,
        "tells": all_tells,
        "elapsed_s": round(time.time() - t0, 2),
        "sources": "Massive FX+options (entitled) + FRED VIX/HY-OAS cross-confirm",
        "methodology": "Weighted cross-asset RORO; blocks reweighted over live inputs. "
                       "Massive VIX/futures not entitled -> VIX/credit sourced from FRED.",
    }
    try:
        _dr = _read("data/dollar-radar.json") or {}
        _rt = _dr.get("risk_transmission") or {}
        out["dollar_context"] = {
            "dollar_pressure": _dr.get("dollar_pressure"),
            "dollar_regime": _dr.get("regime"),
            "risk_transmission_score": _rt.get("score"),
            "risk_transmission_verdict": _rt.get("verdict"),
            "source": "justhodl-dollar-radar v2 (additive context; not "
                      "folded into risk_regime_score pending scorecard)"}
    except Exception as _e:
        print("[dollar-context] %s" % _e)
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
