"""
justhodl-crypto-confluence  ·  v1.0  —  THE CRYPTO SYNTHESIZER
================================================================================
The crypto cluster (ma200 trend, emergence, funding, volume-surge, on-chain,
narratives, liquidity, cycle-risk, altseason) was fragmented — a dozen engines,
almost none reading each other. This is the crypto sibling of equity / earnings /
options / flow-confluence: it fuses the per-coin reads into ONE edge scored on how
many INDEPENDENT crypto dimensions light a coin up, on a market-context backdrop.

PER-COIN DIMENSIONS (independent):
   • TREND     (crypto-ma200)         — 200-DMA breakout / holding a retest
   • EMERGENCE (crypto-emergence)     — relative strength + trend + 3m return composite
   • MOMENTUM  (crypto-opportunities)  — volume-surge + multi-horizon price thrust
   • FUNDING   (crypto-funding)        — positioning / squeeze candidates
   • ONCHAIN   (onchain-ratios)        — BTC/ETH on-chain valuation read

MARKET CONTEXT (overlay, not per-coin): crypto-liquidity (stablecoin dry powder),
crypto-cycle-risk (dump risk), stablecoin-flow (mint/burn), altseason (rotation),
crypto-narratives (breadth/stance). A coin lit across several independent dimensions
WITH a supportive liquidity/cycle backdrop is the strong setup; the same coin into a
high-dump-risk, contracting-stablecoin tape is a trap. Top bullish coins are
scorecard-graded on forward excess-vs-BTC — measure-before-trust.

OUTPUT: data/crypto-confluence.json     SCHEDULE: daily
"""
import json, time, boto3, re
from datetime import datetime, timezone
from decimal import Decimal

S3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crypto-confluence.json"
VERSION = "1.0.0"

# (file, dimension, [lists], score_key|None, score_div, filter|None, base_strength)
BULL = [
    ("crypto-ma200.json",        "trend",     ["fresh_breakouts_above", "retesting_now"], None,             1,   None,                      0.65),
    ("crypto-emergence.json",    "emergence", ["coins"],                                  "emergence_score", 100, ("_gt", "emergence_score", 40), 0.5),
    ("crypto-opportunities.json","momentum",  ["top_volume_surge"],                       "signal_strength", 100, None,                      0.55),
    ("crypto-funding.json",      "funding",   ["squeeze_candidates"],                     None,              1,   None,                      0.5),
]
BEAR = [
    ("crypto-ma200.json",        "trend",     ["fresh_breakdowns_below", "retest_failed"], None,            1,   None,                      0.65),
]


def _read(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _clamp(v, lo=0.0, hi=1.0):
    return max(lo, min(hi, v))


def norm(t):
    """Normalise the many crypto id formats to a bare ticker (BTC, ETH, SOL...)."""
    t = str(t or "").upper().strip()
    t = t.replace("X:", "").replace("/", "").replace("-", "")
    for suf in ("USDT", "USDC", "USD"):
        if t.endswith(suf) and len(t) > len(suf):
            t = t[: -len(suf)]
    return t.strip()


def _idof(it):
    return norm(it.get("ticker") or it.get("coin") or it.get("symbol") or it.get("name"))


def collect(spec):
    hits, seen = {}, []
    for fn, dim, lists, skey, sdiv, filt, base in spec:
        d = _read("data/" + fn)
        if not d:
            seen.append({"engine": fn[:-5], "dimension": dim, "ok": False})
            continue
        seen.append({"engine": fn[:-5], "dimension": dim, "ok": True,
                     "asof": str(d.get("generated_at") or "")[:10]})
        for L in lists:
            for it in (d.get(L) or []):
                if not isinstance(it, dict):
                    continue
                cid = _idof(it)
                if not cid:
                    continue
                if filt:
                    if filt[0] == "_gt" and not ((it.get(filt[1]) or 0) > filt[2]):
                        continue
                raw = it.get(skey) if skey else None
                strength = _clamp((raw / sdiv) if isinstance(raw, (int, float)) else base)
                cur = hits.setdefault(cid, {})
                if dim not in cur or strength > cur[dim]:
                    cur[dim] = round(strength, 2)
    return hits, seen


def onchain_dim(hits):
    """onchain-ratios carries only BTC/ETH; fold a bullish on-chain read into those names."""
    oc = _read("data/onchain-ratios.json") or {}
    for c in ("btc", "eth"):
        node = oc.get(c) or {}
        interp = json.dumps(node).lower() + " " + str(oc.get("interpretation", "")).lower()
        bullish = any(w in interp for w in ("undervalued", "accumulation", "cheap", "bullish", "buy zone", "oversold"))
        if bullish:
            hits.setdefault(c.upper(), {})["onchain"] = 0.6


def market_context():
    liq = _read("data/crypto-liquidity.json") or {}
    cyc = _read("data/crypto-cycle-risk.json") or {}
    sbf = _read("data/stablecoin-flow.json") or {}
    alt = _read("data/altseason.json") or {}
    nar = _read("data/crypto-narratives.json") or {}
    tilt = 0
    liq_regime = str(liq.get("regime") or "")
    if "DRY-POWDER" in liq_regime.upper() or "LOADED" in liq_regime.upper():
        tilt += 1
    elif "DRY" in liq_regime.upper() and "LOW" in liq_regime.upper():
        tilt -= 1
    dump = cyc.get("dump_risk_score")
    if isinstance(dump, (int, float)):
        tilt += 1 if dump < 35 else -1 if dump >= 65 else 0
    sbf_state = str(sbf.get("state") or "").upper()
    if "EXPAND" in sbf_state:
        tilt += 1
    elif "CONTRACT" in sbf_state:
        tilt -= 1
    # ── vol / skew / miner / carry overlays (the new crypto vol-complex engines) ──
    dvol = _read("data/crypto-dvol.json") or {}
    opts = _read("data/crypto-options-surface.json") or {}
    miners = _read("data/crypto-miners.json") or {}
    basis = _read("data/crypto-basis.json") or {}
    if str((dvol.get("btc") or {}).get("regime") or "").upper() in ("ELEVATED", "HIGH"):
        tilt -= 1
    _rr = ((opts.get("btc") or {}).get("headline_30d") or {}).get("rr_25d")
    _vt = str(((opts.get("btc") or {}).get("term_structure") or {}).get("regime") or "").upper()
    if isinstance(_rr, (int, float)) and _rr <= -6 and "BACKWARD" in _vt:
        tilt -= 1                                      # put skew + vol backwardation = hedging
    _pu = (miners.get("puell") or {}).get("value")
    if isinstance(_pu, (int, float)) and _pu < 0.6:
        tilt += 1                                      # deep-value Puell = contrarian support
    if (miners.get("hash_ribbons") or {}).get("state") == "RECOVERY/BUY":
        tilt += 1
    _cc = (basis.get("btc") or {}).get("cash_and_carry_yield_3m_pct")
    if isinstance(_cc, (int, float)) and _cc <= -2:
        tilt -= 1                                      # carry backwardation = deleveraging
    # ── exchange flows / institutional COT / realized price (free-data engines) ──
    xflow = _read("data/crypto-exchange-flows.json") or {}
    cot = _read("data/crypto-cot.json") or {}
    onch = _read("data/onchain-ratios.json") or {}
    _onb = (onch.get("btc") or onch or {})
    _xfp = (xflow.get("btc") or {}).get("cum_30d_pctile")
    if isinstance(_xfp, (int, float)) and _xfp >= 80:
        tilt -= 1                                      # heavy exchange inflow = distribution
    _pvr = _onb.get("price_vs_realized_pct")
    if isinstance(_pvr, (int, float)) and _pvr < 0:
        tilt += 1                                      # below realized price = deep-value support
    _nupl = _onb.get("nupl")
    if isinstance(_nupl, (int, float)) and _nupl >= 0.75:
        tilt -= 1                                      # NUPL euphoria = late-cycle
    # stablecoin peg (active depeg = crypto tail risk) + coinbase premium (US spot demand)
    speg = _read("data/crypto-stablecoin-peg.json") or {}
    cprem = _read("data/coinbase-premium.json") or {}
    if speg.get("gauge") == "red":
        tilt -= 2                                       # active depeg = risk-off
    _cpp = (cprem.get("btc") or {}).get("premium_pct")
    if isinstance(_cpp, (int, float)):
        if _cpp >= 0.3:
            tilt += 1                                   # US/institutional bid
        elif _cpp <= -0.3:
            tilt -= 1                                   # US distribution
    # spot ETF net flows — marginal buyer, event-study CONFIRMED predictive (weight 2)
    etf = _read("data/crypto-etf-flows.json") or {}
    _etfp = (etf.get("btc_etf") or {}).get("cum_30d_pctile")
    if isinstance(_etfp, (int, float)):
        if _etfp >= 80:
            tilt += 2
        elif _etfp <= 20:
            tilt -= 2
    # Hyperliquid perp leverage regime
    hl = _read("data/hyperliquid-perps.json") or {}
    _hlr = str(hl.get("leverage_regime") or "").upper()
    if "BUILDUP" in _hlr:
        tilt -= 1
    elif "CASCADE" in _hlr:
        tilt -= 1
    # dealer gamma: negative gamma below flip = vol-expansion / unstable
    gex = _read("data/crypto-gex.json") or {}
    _gb = gex.get("btc") or {}
    if "NEGATIVE" in str(_gb.get("regime") or "").upper() and isinstance(_gb.get("spot_vs_flip"), (int, float)) and _gb.get("spot_vs_flip") <= -0.5:
        tilt -= 1
    regime = "RISK_ON" if tilt >= 3 else "RISK_OFF" if tilt <= -3 else "NEUTRAL"
    return {
        "regime": regime, "tilt": tilt,
        "liquidity": liq.get("regime"), "dump_risk": dump, "dump_risk_level": cyc.get("risk_level"),
        "stablecoin_flow": sbf.get("state"), "altseason_composite": alt.get("composite"),
        "narrative_stance": nar.get("stance"), "narrative_breadth_pct": nar.get("narrative_breadth_pct"),
        "vol_regime": (dvol.get("btc") or {}).get("regime"), "vol_trend": (dvol.get("btc") or {}).get("trend"),
        "options_skew": (opts.get("btc") or {}).get("interpretation"),
        "vol_term": ((opts.get("btc") or {}).get("term_structure") or {}).get("regime"),
        "puell": (miners.get("puell") or {}).get("value"), "puell_zone": (miners.get("puell") or {}).get("zone"),
        "hash_ribbon": (miners.get("hash_ribbons") or {}).get("state"),
        "cash_carry_3m": (basis.get("btc") or {}).get("cash_and_carry_yield_3m_pct"),
        "carry_regime": (basis.get("btc") or {}).get("regime"),
        "exchange_flow_regime": (xflow.get("btc") or {}).get("regime"),
        "cot_asset_mgr": ((cot.get("btc") or {}).get("asset_mgr") or {}).get("read"),
        "cot_divergence": (cot.get("btc") or {}).get("divergence"),
        "realized_price": _onb.get("realized_price"),
        "price_vs_realized_pct": _onb.get("price_vs_realized_pct"),
        "nupl_zone": _onb.get("nupl_zone"),
        "stablecoin_peg_status": speg.get("status"),
        "stablecoin_worst": speg.get("worst_coin"),
        "coinbase_premium_pct": (cprem.get("btc") or {}).get("premium_pct"),
        "etf_flow_btc_regime": (etf.get("btc_etf") or {}).get("regime"),
        "etf_flow_btc_30d_usd": (etf.get("btc_etf") or {}).get("cum_30d_usd"),
        "etf_flow_eth_regime": (etf.get("eth_etf") or {}).get("regime"),
        "hl_total_oi_usd": hl.get("total_oi_usd"),
        "hl_leverage_regime": hl.get("leverage_regime"),
        "hl_btc_funding_ann_pct": (hl.get("btc") or {}).get("funding_ann_pct"),
        "gex_btc_regime": (gex.get("btc") or {}).get("regime"),
        "gex_btc_gamma_flip": (gex.get("btc") or {}).get("gamma_flip"),
        "gex_btc_call_wall": (gex.get("btc") or {}).get("call_wall"),
        "gex_btc_put_wall": (gex.get("btc") or {}).get("put_wall"),
        "gex_btc_max_pain": (gex.get("btc") or {}).get("max_pain"),
        "note": ("Liquidity/cycle backdrop supports leaning into coin setups." if regime == "RISK_ON"
                 else "High dump-risk / contracting backdrop — treat bullish coin signals as suspect." if regime == "RISK_OFF"
                 else "Mixed backdrop — coin-specific confluence matters more than the tape."),
    }


def score_book(hits, n_dims):
    rows = []
    for cid, dims in hits.items():
        n = len(dims)
        avg = sum(dims.values()) / n if n else 0
        composite = round(min(100.0, (n / n_dims) * 70 + avg * 30), 1)
        rows.append({"coin": cid, "n_dimensions": n, "dimensions": sorted(dims.keys()),
                     "strengths": dims, "avg_strength": round(avg, 2), "composite": composite})
    rows.sort(key=lambda r: (-r["n_dimensions"], -r["composite"]))
    return rows


def lambda_handler(event=None, context=None):
    t0 = time.time()
    bull_hits, bull_seen = collect(BULL)
    onchain_dim(bull_hits)
    bear_hits, bear_seen = collect(BEAR)
    n_bull_dims = len(BULL) + 1  # + onchain
    bull = score_book(bull_hits, n_bull_dims)
    bear = score_book(bear_hits, len(BEAR))
    multi = [r for r in bull if r["n_dimensions"] >= 2]
    ctx = market_context()

    out = {
        "engine": "crypto-confluence", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "thesis": ("Fuses the crypto-engine cluster (trend / emergence / momentum / funding / on-chain) into one "
                   "per-coin edge scored on independent-dimension breadth, on a liquidity/cycle backdrop."),
        "dimensions": ["trend", "emergence", "momentum", "funding", "onchain"],
        "market_context": ctx,
        "sources_bull": bull_seen, "sources_bear": bear_seen,
        "counts": {"bullish_any": len(bull), "bullish_multi": len(multi), "bearish_any": len(bear)},
        "confluence_book": bull[:40],
        "multi_dimension_bullish": multi[:25],
        "deteriorating_book": bear[:20],
        "method": "independent-dimension breadth (70%) + avg strength (30%); >=2 dimensions = confluence",
        "disclaimer": "Synthesis of the platform's own crypto engines — research, not advice.",
    }

    # closed loop: grade strongest multi-dimension coins forward vs BTC
    try:
        nowt = datetime.now(timezone.utc)
        tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
        logged = 0
        for r in multi[:8]:
            if r["coin"] == "BTC":
                continue
            tbl.put_item(Item={
                "signal_id": f"crypto-confluence#{r['coin']}#{nowt.date().isoformat()}",
                "signal_type": "crypto_confluence", "predicted_direction": "UP",
                "signal_value": str(r["composite"]), "confidence": Decimal("0.55"),
                "measure_against": "ticker_vs_benchmark", "benchmark": "BTC",
                "check_windows": ["day_5", "day_21", "day_63"], "outcomes": {}, "accuracy_scores": {},
                "status": "pending", "logged_at": nowt.isoformat(), "logged_epoch": int(nowt.timestamp()),
                "horizon_days_primary": 21, "schema_version": "2",
                "ttl": int(nowt.timestamp()) + 120 * 86400,
                "metadata": {"engine": "crypto-confluence", "v": VERSION, "n_dimensions": r["n_dimensions"],
                             "dimensions": r["dimensions"], "market_regime": ctx["regime"]},
                "rationale": f"{r['coin']} crypto confluence across {r['n_dimensions']} dims {r['dimensions']}"})
            logged += 1
        out["signals_logged"] = logged
    except Exception as e:
        print(f"[loop] {str(e)[:80]}")

    try:
        _dr = _read("data/dollar-radar.json") or {}
        _rt = _dr.get("risk_transmission") or {}
        out["dollar_context"] = {
            "dollar_pressure": _dr.get("dollar_pressure"),
            "dollar_regime": _dr.get("regime"),
            "risk_transmission_score": _rt.get("score"),
            "risk_transmission_verdict": _rt.get("verdict"),
            "source": "justhodl-dollar-radar v2 (crypto is the highest-"
                      "beta expression of the dollar/rates mix; additive "
                      "context, not folded into scores)"}
    except Exception as _e:
        print("[dollar-context] %s" % _e)
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[crypto-confluence] bull_any={len(bull)} multi={len(multi)} bear={len(bear)} "
          f"regime={ctx['regime']} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps(out["counts"])}
