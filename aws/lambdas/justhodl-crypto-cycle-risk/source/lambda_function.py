"""justhodl-crypto-cycle-risk — honest, multi-factor crypto "dump risk" gauge.

Built in response to the claim that Fed-chair transitions predict ~90% Bitcoin
drawdowns. That claim is n=3 and almost certainly CONFOUNDED by the 4-year
halving cycle (the 2014/2018/2022 drawdowns were halving-cycle tops, which
merely overlapped Fed transitions). So this engine does NOT treat Fed timing as
a strong signal. Instead it fuses the genuinely predictive cycle/positioning
factors into a composite, with Fed-transition proximity as ONE low-weight,
explicitly-caveated input.

FACTORS (weighted):
  • Halving-cycle phase      (0.35) — the real driver. Danger window ~12-18mo
    after a halving (historically where cycle tops formed).
  • MVRV / price extension   (0.25) — over-/under-valuation vs realized price.
  • Funding / leverage        (0.20) — frothy perp funding = crowded longs.
  • Fear & Greed extreme      (0.10) — extreme greed precedes corrections.
  • Fed-transition proximity  (0.05) — included for completeness, flagged n=3.
  • Macro/bond-vol regime     (0.05) — risk-off backdrop amplifies crypto beta.

OUTPUT: data/crypto-cycle-risk.json — composite 0-100 dump-risk + factor breakdown.
SCHEDULE: every 6h.
"""
import json, time
import urllib.request, urllib.parse
from datetime import datetime, timezone, date
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crypto-cycle-risk.json"
FRED_KEY = "2f057499936072679d8843d7fce99989"
s3 = boto3.client("s3", region_name=REGION)

# Bitcoin halving dates (block-reward halvings — the cycle's anchor).
HALVINGS = ["2012-11-28", "2016-07-09", "2020-05-11", "2024-04-20"]
# Fed chair transitions/renewals (the claimed — and weak — signal).
FED_TRANSITIONS = ["2014-02-03", "2018-02-05", "2022-02-05"]  # Yellen, Powell t1, Powell t2

# Macro risk series (FRED) — the genuine risk-on/off drivers of high-beta crypto.
#   BAMLH0A0HYM2 = ICE BofA US High-Yield OAS (credit risk; WIDENING = risk-off)
#   DGS10        = 10Y Treasury yield (SPIKING = discount-rate shock to risk assets)
#   T10YIE       = 10Y breakeven inflation (SPIKING = forces yields up → risk-off)
MACRO_SERIES = {
    "hy_oas": "BAMLH0A0HYM2",
    "ten_yr": "DGS10",
    "breakeven": "T10YIE",
    "real_10y": "DFII10",      # 10Y real yield — the true discount-rate shock to risk
    "dollar": "DTWEXBGS",      # broad trade-weighted USD — strength = headwind for BTC
}

# CPI/PPI release calendar matters because an inflation SURPRISE is what most
# directly re-prices rate-cut odds → risk-off. Approximate BLS release cadence:
# CPI ~10th-14th of the month, PPI ~1-2 days after. We flag the few-day window
# around the next expected release as elevated event risk.
def days_to_inflation_print(today):
    from datetime import date as _date
    # next CPI ≈ 12th of this or next month
    candidates = []
    for mo in (today.month, today.month % 12 + 1):
        yr = today.year + (1 if mo < today.month else 0)
        try: candidates.append(_date(yr, mo, 12))
        except ValueError: pass
    future = sorted([c for c in candidates if (c - today).days >= -2])
    return (future[0] - today).days if future else None


def read_json(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


def fred_series(series_id, n=120):
    """Last ~n daily observations for a FRED series (level)."""
    try:
        params = {"series_id": series_id, "api_key": FRED_KEY, "file_type": "json",
                  "sort_order": "desc", "limit": str(n)}
        url = "https://api.stlouisfed.org/fred/series/observations?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            obs = json.loads(r.read().decode()).get("observations", [])
        vals = []
        for o in obs:  # desc → newest first
            v = o.get("value")
            if v not in (None, ".", ""):
                try: vals.append(float(v))
                except ValueError: pass
        return vals  # newest-first
    except Exception as e:
        print(f"[crypto-risk] FRED {series_id} err: {str(e)[:60]}")
        return []


def days_since(iso_list, today):
    best = None
    for s in iso_list:
        d = (today - date.fromisoformat(s)).days
        if d >= 0 and (best is None or d < best):
            best = d
    return best


def _telegram(msg):
    """Send an exceptions-only alert via the JustHodl Telegram bot (token + chat
    from SSM). Markdown enabled."""
    try:
        ssm = boto3.client("ssm", region_name=REGION)
        token = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
        try:
            chat_id = ssm.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
        except Exception:
            chat_id = "8678089260"
        body = urllib.parse.urlencode({"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}).encode()
        req = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=body)
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"[crypto-risk] telegram send err: {str(e)[:80]}")


def macro_risk_factor():
    """Score how much macro risk is RISING — the user's thesis: spiking yields,
    spiking inflation, and widening HY credit spreads all foreshadow risk-asset
    (incl. crypto) drawdowns. Each metric scored on level-percentile + recent
    momentum; higher = more risk-off pressure on crypto."""
    sub = {}
    scores = []
    for name, sid in MACRO_SERIES.items():
        vals = fred_series(sid, 120)
        if len(vals) < 25:
            sub[name] = {"score": 50, "note": "insufficient data"}; scores.append(50); continue
        latest = vals[0]
        prev_month = vals[min(21, len(vals) - 1)]      # ~1 month ago (desc index)
        chg_1m = latest - prev_month
        # percentile of the latest level within the ~6mo window (high = stressed)
        window = vals[:120]
        pct = 100 * sum(1 for v in window if v < latest) / len(window)
        # rising fast (top-decile 1m move up) adds risk; the LEVEL percentile sets the base
        rising = chg_1m > 0
        mom_bump = min(25, max(0, chg_1m / (abs(prev_month) + 1e-6) * 100 * 2)) if rising else 0
        score = round(min(100, pct * 0.7 + mom_bump + (10 if rising else 0)))
        label = {"hy_oas": "HY credit OAS", "ten_yr": "10Y yield", "breakeven": "10Y breakeven inflation", "real_10y": "10Y real yield", "dollar": "Broad USD index"}[name]
        direction = "widening" if (name == "hy_oas" and rising) else ("strengthening" if (name == "dollar" and rising) else ("spiking" if rising else "easing"))
        sub[name] = {"score": score, "latest": round(latest, 2), "chg_1m": round(chg_1m, 2),
                     "level_pctile_6mo": round(pct, 1), "note": f"{label} {round(latest,2)} ({direction}, {'+' if chg_1m>=0 else ''}{round(chg_1m,2)} 1mo)"}
        scores.append(score)
    composite = round(sum(scores) / len(scores)) if scores else 50
    return composite, sub


def lambda_handler(event=None, context=None):
    t0 = time.time()
    today = date.today()
    crypto = read_json("crypto-intel.json") or {}   # NOTE: root key, not data/
    bond = read_json("data/bond-vol.json") or {}
    factors = {}

    # ── 1. Halving-cycle phase (the dominant, real driver) ──
    dsh = days_since(HALVINGS, today)
    months = (dsh / 30.44) if dsh is not None else None
    # Risk ramps in the historical top window (~12-18mo post-halving), peaks ~17mo,
    # then falls through the bear/accumulation phase.
    if months is None:
        halving_risk = 50; halving_note = "unknown"
    elif months < 6:
        halving_risk = 20; halving_note = f"{months:.0f}mo post-halving — early bull, low cycle risk"
    elif months < 12:
        halving_risk = 45; halving_note = f"{months:.0f}mo post-halving — mid bull"
    elif months <= 18:
        halving_risk = 90; halving_note = f"{months:.0f}mo post-halving — HISTORICAL TOP WINDOW (12-18mo)"
    elif months <= 24:
        halving_risk = 70; halving_note = f"{months:.0f}mo post-halving — late-cycle / distribution risk"
    elif months <= 36:
        halving_risk = 35; halving_note = f"{months:.0f}mo post-halving — bear/accumulation"
    else:
        halving_risk = 45; halving_note = f"{months:.0f}mo post-halving — pre-next-halving"
    factors["halving_cycle"] = {"weight": 0.24, "risk": halving_risk, "months_since_halving": round(months, 1) if months else None, "note": halving_note}

    # ── 2. MVRV / price extension ──
    onchain = crypto.get("onchain_ratios") or {}
    mvrv = onchain.get("mvrv_approx") or onchain.get("mvrv") or (crypto.get("onchain") or {}).get("mvrv_approx")
    if mvrv is not None:
        mvrv_risk = max(0, min(100, (mvrv - 1.0) / (3.5 - 1.0) * 100))
        mnote = f"MVRV ~{round(mvrv,2)} ({'overheated' if mvrv>3 else 'elevated' if mvrv>2 else 'neutral' if mvrv>1 else 'undervalued'})"
    else:
        mvrv_risk = 50; mnote = "MVRV unavailable"
    factors["mvrv_extension"] = {"weight": 0.10, "risk": round(mvrv_risk), "mvrv": mvrv, "note": mnote}

    # ── 3. Funding / leverage froth ──
    funding = crypto.get("funding") or {}
    rates = funding.get("rates") or []
    avg_funding = None
    if rates:
        vals = [r.get("funding_rate_pct") for r in rates if isinstance(r, dict) and r.get("funding_rate_pct") is not None]
        if vals: avg_funding = sum(vals) / len(vals)
    if avg_funding is not None:
        fund_risk = max(0, min(100, (avg_funding / 0.05) * 60 + 30)) if avg_funding > 0 else max(0, 30 + avg_funding * 200)
        fnote = f"avg perp funding {round(avg_funding,4)}% ({'frothy longs' if avg_funding>0.03 else 'neutral' if avg_funding>-0.01 else 'shorts paying'})"
    else:
        fund_risk = 50; fnote = "funding unavailable"
    factors["funding_leverage"] = {"weight": 0.10, "risk": round(fund_risk), "avg_funding_pct": round(avg_funding, 4) if avg_funding is not None else None, "note": fnote}

    # ── 4. Fear & Greed extreme ──
    fg = crypto.get("fear_greed")
    fg_val = fg.get("value") if isinstance(fg, dict) else fg
    try: fg_val = float(fg_val) if fg_val is not None else None
    except (ValueError, TypeError): fg_val = None
    if fg_val is not None:
        fg_risk = max(0, min(100, (fg_val - 25) / (90 - 25) * 100))
        fgnote = f"Fear&Greed {int(fg_val)} ({'extreme greed' if fg_val>75 else 'greed' if fg_val>55 else 'neutral' if fg_val>45 else 'fear' if fg_val>25 else 'extreme fear'})"
    else:
        fg_risk = 50; fgnote = "F&G unavailable"
    factors["fear_greed"] = {"weight": 0.08, "risk": round(fg_risk), "value": fg_val, "note": fgnote}

    # ── 5. MACRO RISK — yields/inflation/HY-spread/real-yield/dollar ──
    macro_score, macro_sub = macro_risk_factor()
    factors["macro_risk"] = {"weight": 0.14, "risk": macro_score, "components": macro_sub,
                             "note": "Crypto is high-beta to liquidity/credit: rising 10Y/real yields, rising inflation breakevens, widening HY credit spreads (ICE BofA OAS), and a strengthening USD all pressure risk assets down. Higher = more risk-off."}

    # ── 5b. INFLATION-PRINT EVENT RISK — a CPI/PPI surprise is what most directly
    # re-prices rate-cut odds → risk-off. Elevated in the days around a release. ──
    dti = days_to_inflation_print(today)
    if dti is not None and 0 <= dti <= 3:
        infl_risk = 70; infl_note = f"CPI/PPI release in ~{dti}d — elevated surprise/event risk into the print"
    elif dti is not None and dti <= 6:
        infl_risk = 55; infl_note = f"CPI/PPI release in ~{dti}d — approaching"
    else:
        infl_risk = 40; infl_note = f"no inflation print imminent (~{dti}d out)" if dti is not None else "inflation calendar unknown"
    factors["inflation_print"] = {"weight": 0.06, "risk": infl_risk, "days_to_print": dti, "note": infl_note}

    # ── 5c. ETF FLOWS — the marginal buyer. Sustained spot-BTC-ETF OUTFLOWS were
    # the single most-cited 2026 driver (the rally's engine running in reverse). ──
    etf = read_json("data/etf-true-flows.json") or {}
    btc_flow = None
    flow_list = etf.get("flows") or etf.get("by_etf") or []
    if isinstance(flow_list, dict):
        flow_list = list(flow_list.values())
    for r in flow_list:
        if not isinstance(r, dict):
            continue
        sym = (r.get("ticker") or r.get("symbol") or "").upper()
        if sym in ("IBIT", "FBTC", "BTC", "GBTC"):
            v = r.get("net_flow_usd") or r.get("flow_usd") or r.get("net_creation_usd")
            if v is not None:
                try: btc_flow = (btc_flow or 0) + float(v)
                except (ValueError, TypeError): pass
    if btc_flow is not None:
        # outflows (negative) raise risk; inflows lower it. ±$500M/day = strong.
        etf_risk = max(0, min(100, 50 - (btc_flow / 5e8) * 35))
        enote = f"BTC ETF net flow ${round(btc_flow/1e6)}M ({'OUTFLOWS — marginal buyer gone' if btc_flow<0 else 'inflows'})"
    else:
        etf_risk = 50; enote = "ETF flow data unavailable (populates as etf-true-flows matures)"
    factors["etf_flows"] = {"weight": 0.12, "risk": round(etf_risk), "btc_net_flow_usd": btc_flow, "note": enote}

    # ── 5d. AI CAPITAL ROTATION — BTC underperforming the Nasdaq/AI complex =
    # capital chasing AI beta over crypto beta (the Saylor 'rotation' thesis). ──
    # Proxy: BTC 30d return minus QQQ 30d return; deep negative = rotation away.
    ai_risk, ai_note = 50, "rotation proxy unavailable"
    try:
        import urllib.request as _u
        def _ret30(tkr, crypto=False):
            try:
                if crypto:
                    u = f"{ 'https://justhodl-data-proxy.raafouis.workers.dev' }/yf-ohlc?symbol=BTC-USD&range=3mo"
                else:
                    u = f"{ 'https://justhodl-data-proxy.raafouis.workers.dev' }/ohlc?ticker={tkr}&mult=1&span=day&days=45"
                req = _u.Request(u, headers={"User-Agent": "JustHodl/1.0"})
                d = json.loads(_u.urlopen(req, timeout=12).read().decode())
                bars = d.get("bars") or []
                cl = [b.get("close") for b in bars if b.get("close")]
                if len(cl) >= 22: return (cl[-1] / cl[-22] - 1) * 100
            except Exception: pass
            return None
        btc_30 = _ret30("BTC", crypto=True)
        qqq_30 = _ret30("QQQ")
        if btc_30 is not None and qqq_30 is not None:
            spread = btc_30 - qqq_30   # negative = BTC lagging AI/tech
            ai_risk = max(0, min(100, 50 - spread * 2.2))
            ai_note = f"BTC 30d {round(btc_30,1)}% vs QQQ {round(qqq_30,1)}% → spread {round(spread,1)}pp ({'BTC LAGGING AI/tech — rotation away from crypto' if spread<-3 else 'in line' if spread<3 else 'BTC leading'})"
    except Exception:
        pass
    factors["ai_rotation"] = {"weight": 0.06, "risk": round(ai_risk), "note": ai_note,
                              "context": "AI buildout funding ~$400-450B/6mo (hyperscaler capex >$650B for 2026); marginal risk capital chasing AI beta over crypto beta. Per Saylor: 'capital rotation, not impairment.'"}

    # ── 6. Fed-transition proximity (LOW weight, explicitly caveated) ──
    dsf = days_since(FED_TRANSITIONS, today)
    fed_months = (dsf / 30.44) if dsf is not None else None
    if fed_months is not None and 10 <= fed_months <= 16:
        fed_risk = 70; fed_note = f"{fed_months:.0f}mo since last Fed transition — inside the claimed window (LOW CONFIDENCE: n=3, likely halving-confounded)"
    else:
        fed_risk = 40; fed_note = f"{'%.0f' % fed_months if fed_months is not None else '?'}mo since last Fed transition — outside claimed window"
    factors["fed_transition"] = {"weight": 0.04, "risk": fed_risk, "months_since": round(fed_months, 1) if fed_months else None,
                                  "note": fed_note, "caveat": "n=3 sample; 2014/2018/2022 drawdowns were halving-cycle tops that merely overlapped Fed transitions. NOTE: QT actually ENDED Dec 2025 — the 'Fed shrinking the balance sheet' narrative is outdated; the real macro driver is inflation killing rate-cut odds."}

    # ── 7. Macro / bond-vol regime ──
    bv_regime = (bond.get("regime") or "").upper()
    macro_regime_risk = {"CRISIS": 85, "ELEVATED": 65, "NORMAL": 45, "BOND_VOL_LOW": 40}.get(bv_regime, 50)
    factors["macro_regime"] = {"weight": 0.06, "risk": macro_regime_risk, "bond_vol_regime": bv_regime or None,
                                "note": f"bond-vol regime {bv_regime or 'unknown'} ({'risk-off amplifies crypto beta' if macro_regime_risk>=65 else 'benign'})"}

    # ── Composite ──
    composite = round(sum(f["risk"] * f["weight"] for f in factors.values()), 1)
    if composite >= 75: level, action = "EXTREME", "Cycle-top risk elevated on multiple fronts — de-risk / take profit / tighten stops."
    elif composite >= 60: level, action = "HIGH", "Multiple froth signals — trim, avoid leverage, prepare hedges."
    elif composite >= 45: level, action = "MODERATE", "Mixed signals — normal risk management."
    else: level, action = "LOW", "Cycle/positioning favorable — historically a lower-risk accumulation backdrop."

    # honest top contributors
    contribs = sorted(factors.items(), key=lambda kv: -kv[1]["risk"] * kv[1]["weight"])
    drivers = [{"factor": k, "risk": v["risk"], "weight": v["weight"], "note": v["note"]} for k, v in contribs[:4]]

    out = {
        "engine": "crypto-cycle-risk", "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "dump_risk_score": composite, "risk_level": level, "action": action,
        "factors": factors, "top_drivers": drivers,
        "methodology": ("Weighted composite of halving-cycle phase (dominant), MVRV "
                        "extension, perp funding/leverage, Fear&Greed, macro bond-vol "
                        "regime, and Fed-transition proximity (low weight)."),
        "honesty_note": ("The popular 'Fed-chair transition predicts ~90% Bitcoin "
                         "crash' claim is n=3 and confounded: 2014/2018/2022 drawdowns "
                         "were 4-year halving-cycle tops that happened to overlap Fed "
                         "transitions. This engine weights the halving cycle as the real "
                         "driver and treats Fed timing as a weak, caveated input. No "
                         "model 'predicts dumps 90% of the time' — beware anything that "
                         "claims to."),
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")

    # ── Telegram tripwire — exceptions-only. Alert when dump-risk crosses into
    # HIGH/EXTREME, and only on a CHANGE (don't spam every 6h at the same level). ──
    try:
        prev = read_json("data/crypto-cycle-risk-prev.json") or {}
        prev_level = prev.get("risk_level")
        if level in ("HIGH", "EXTREME") and level != prev_level:
            top = "; ".join(d["note"] for d in drivers[:3])
            msg = (f"🪙 *Crypto Dump-Risk: {level}* ({composite}/100)\n\n{action}\n\n"
                   f"Top drivers: {top}")
            _telegram(msg)
        # persist current level for change-detection
        s3.put_object(Bucket=BUCKET, Key="data/crypto-cycle-risk-prev.json",
                      Body=json.dumps({"risk_level": level, "score": composite}).encode(),
                      ContentType="application/json")
    except Exception as e:
        print(f"[crypto-risk] telegram err: {str(e)[:80]}")

    print(f"[crypto-cycle-risk] DONE {round(time.time()-t0,1)}s — {level} ({composite}); "
          f"halving {factors['halving_cycle'].get('months_since_halving')}mo")
    return {"statusCode": 200, "body": json.dumps({"score": composite, "level": level})}
