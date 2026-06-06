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
from datetime import datetime, timezone, date
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crypto-cycle-risk.json"
s3 = boto3.client("s3", region_name=REGION)

# Bitcoin halving dates (block-reward halvings — the cycle's anchor).
HALVINGS = ["2012-11-28", "2016-07-09", "2020-05-11", "2024-04-20"]
# Fed chair transitions/renewals (the claimed — and weak — signal).
FED_TRANSITIONS = ["2014-02-03", "2018-02-05", "2022-02-05"]  # Yellen, Powell t1, Powell t2


def read_json(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


def days_since(iso_list, today):
    """Smallest non-negative day-distance since the most recent past date."""
    best = None
    for s in iso_list:
        d = (today - date.fromisoformat(s)).days
        if d >= 0 and (best is None or d < best):
            best = d
    return best


def lambda_handler(event=None, context=None):
    t0 = time.time()
    today = date.today()
    crypto = read_json("data/crypto-intel.json") or {}
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
    factors["halving_cycle"] = {"weight": 0.35, "risk": halving_risk, "months_since_halving": round(months, 1) if months else None, "note": halving_note}

    # ── 2. MVRV / price extension ──
    onchain = crypto.get("onchain_ratios") or {}
    mvrv = onchain.get("mvrv_approx") or onchain.get("mvrv")
    if mvrv is not None:
        # MVRV > 3 historically = overheated; < 1 = undervalued.
        mvrv_risk = max(0, min(100, (mvrv - 1.0) / (3.5 - 1.0) * 100))
        mnote = f"MVRV ~{round(mvrv,2)} ({'overheated' if mvrv>3 else 'elevated' if mvrv>2 else 'neutral' if mvrv>1 else 'undervalued'})"
    else:
        mvrv_risk = 50; mnote = "MVRV unavailable"
    factors["mvrv_extension"] = {"weight": 0.25, "risk": round(mvrv_risk), "mvrv": mvrv, "note": mnote}

    # ── 3. Funding / leverage froth ──
    funding = crypto.get("funding") or {}
    rates = funding.get("rates") or []
    avg_funding = None
    if rates:
        vals = [r.get("funding_rate_pct") for r in rates if isinstance(r, dict) and r.get("funding_rate_pct") is not None]
        if vals: avg_funding = sum(vals) / len(vals)
    if avg_funding is not None:
        # Persistently high positive funding (>0.05%/8h) = crowded, frothy longs.
        fund_risk = max(0, min(100, (avg_funding / 0.05) * 60 + 30)) if avg_funding > 0 else max(0, 30 + avg_funding * 200)
        fnote = f"avg perp funding {round(avg_funding,4)}% ({'frothy longs' if avg_funding>0.03 else 'neutral' if avg_funding>-0.01 else 'shorts paying'})"
    else:
        fund_risk = 50; fnote = "funding unavailable"
    factors["funding_leverage"] = {"weight": 0.20, "risk": round(fund_risk), "avg_funding_pct": round(avg_funding, 4) if avg_funding is not None else None, "note": fnote}

    # ── 4. Fear & Greed extreme ──
    fg = crypto.get("fear_greed")
    fg_val = fg.get("value") if isinstance(fg, dict) else fg
    if fg_val is not None:
        fg_val = float(fg_val)
        # Extreme greed (>75) = correction risk; extreme fear (<25) = low dump risk.
        fg_risk = max(0, min(100, (fg_val - 25) / (90 - 25) * 100))
        fgnote = f"Fear&Greed {int(fg_val)} ({'extreme greed' if fg_val>75 else 'greed' if fg_val>55 else 'neutral' if fg_val>45 else 'fear' if fg_val>25 else 'extreme fear'})"
    else:
        fg_risk = 50; fgnote = "F&G unavailable"
    factors["fear_greed"] = {"weight": 0.10, "risk": round(fg_risk), "value": fg_val, "note": fgnote}

    # ── 5. Fed-transition proximity (LOW weight, explicitly caveated) ──
    dsf = days_since(FED_TRANSITIONS, today)
    fed_months = (dsf / 30.44) if dsf is not None else None
    # The claim: drawdowns ~12-14mo after a transition. We give a mild bump in
    # that window ONLY, and cap its influence (weight 0.05).
    if fed_months is not None and 10 <= fed_months <= 16:
        fed_risk = 70; fed_note = f"{fed_months:.0f}mo since last Fed transition — inside the claimed window (LOW CONFIDENCE: n=3, likely halving-confounded)"
    else:
        fed_risk = 40; fed_note = f"{'%.0f' % fed_months if fed_months is not None else '?'}mo since last Fed transition — outside claimed window"
    factors["fed_transition"] = {"weight": 0.05, "risk": fed_risk, "months_since": round(fed_months, 1) if fed_months else None,
                                  "note": fed_note, "caveat": "n=3 sample; 2014/2018/2022 drawdowns were halving-cycle tops that merely overlapped Fed transitions. Treated as a weak, confounded input, not a predictor."}

    # ── 6. Macro / bond-vol regime ──
    bv_regime = (bond.get("regime") or "").upper()
    macro_risk = {"CRISIS": 85, "ELEVATED": 65, "NORMAL": 45, "BOND_VOL_LOW": 40}.get(bv_regime, 50)
    factors["macro_regime"] = {"weight": 0.05, "risk": macro_risk, "bond_vol_regime": bv_regime or None,
                                "note": f"bond-vol regime {bv_regime or 'unknown'} ({'risk-off amplifies crypto beta' if macro_risk>=65 else 'benign'})"}

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
    print(f"[crypto-cycle-risk] DONE {round(time.time()-t0,1)}s — {level} ({composite}); "
          f"halving {factors['halving_cycle'].get('months_since_halving')}mo")
    return {"statusCode": 200, "body": json.dumps({"score": composite, "level": level})}
