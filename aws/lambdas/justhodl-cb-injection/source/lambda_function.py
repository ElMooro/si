"""
justhodl-cb-injection — Global Central Bank Capital-Injection & Carry Engine.

The platform already tracks the *level* of global liquidity (global-liquidity
sums the Fed/ECB/BOJ balance sheets). This engine tracks the thing that
actually moves markets: the *flow* — who is injecting capital and who is
draining it right now, and what that does to the eurodollar system and the
carry trade.

For each major central bank relevant to offshore-USD funding it builds an
INJECTION STANCE — balance-sheet trajectory (expanding vs QT) combined with
policy-rate direction (cutting = injecting, hiking = draining):

  ECB   Eurosystem balance sheet + deposit facility rate. The euro leg of
        the eurodollar system; ECB QT vs the old APP/TLTRO/PEPP injections.
  BOJ   Bank of Japan balance sheet + policy rate. THE carry-trade funding
        central bank — its post-2024 normalisation is the carry story.
  Fed   Balance sheet + funds rate — the QT pace draining global USD.
  SNB   Swiss policy rate + the franc — the second carry funding currency.

It then synthesises:
  - NET GLOBAL INJECTION IMPULSE — are central banks collectively adding or
    withdrawing liquidity.
  - CARRY FUNDING CONDITIONS — JPY and CHF are the funding legs; cheap, weak
    funding currencies with dovish central banks fuel the carry trade.
  - CARRY-UNWIND RISK — a 0-100 score. Leveraged carry unwinds violently when
    a funding currency strengthens sharply or its central bank turns hawkish
    (the August 2024 yen-carry unwind is the reference event).

OUTPUT: data/cb-injection.json   SCHEDULE: daily
Built on real FRED data (the official mirror of ECB/BOJ/Fed statistics) —
not investment advice.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/cb-injection.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

# FRED carries the official ECB / BOJ / Fed statistics
SERIES = {
    "ECB_BS":   "ECBASSETSW",        # ECB total assets, weekly, EUR mn
    "ECB_RATE": "ECBDFR",            # ECB deposit facility rate, %
    "BOJ_BS":   "JPNASSETS",         # BOJ total assets
    "BOJ_RATE": "IR3TIB01JPM156N",   # Japan 3M interbank — BOJ policy proxy
    "FED_BS":   "WALCL",             # Fed total assets, USD mn
    "FED_RATE": "DFEDTARU",          # Fed funds target rate, upper bound
    "SNB_RATE": "IR3TIB01CHM156N",   # Swiss 3M interbank — SNB policy proxy
    "JPY":      "DEXJPUS",           # yen per USD (up = weak yen)
    "CHF":      "DEXSZUS",           # franc per USD (up = weak franc)
    "EUR":      "DEXUSEU",           # USD per euro (up = strong euro)
}


def fred(series_id, limit=800):
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit={limit}")
    last_err, d = None, None
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-cb-injection/1.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                d = json.loads(r.read())
            break
        except Exception as e:
            last_err = e
            if attempt < 2:
                time.sleep(1.0 * (attempt + 1))
    if d is None:
        raise last_err or RuntimeError(f"FRED fetch failed: {series_id}")
    out = []
    for o in d.get("observations", []):
        v = o.get("value")
        if v in (None, ".", ""):
            continue
        try:
            out.append((o["date"], float(v)))
        except (TypeError, ValueError):
            continue
    return out  # newest-first [(date, value)]


def val_days_ago(obs, days):
    """Observation value closest to `days` before the latest reading."""
    if not obs:
        return None
    latest = datetime.fromisoformat(obs[0][0])
    target = latest - timedelta(days=days)
    best = min(obs, key=lambda o: abs(datetime.fromisoformat(o[0]) - target))
    return best[1]


def pct_change(obs, days):
    base = val_days_ago(obs, days)
    if base in (None, 0) or not obs:
        return None
    return round((obs[0][1] - base) / abs(base) * 100, 2)


def level_change(obs, days):
    base = val_days_ago(obs, days)
    if base is None or not obs:
        return None
    return round(obs[0][1] - base, 3)


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def cb_injection_stance(bs_chg_6m, rate_chg_6m):
    """balance-sheet % change + policy-rate pp change -> stance -2..+2."""
    bs = (1 if (bs_chg_6m or 0) > 1.0 else
          -1 if (bs_chg_6m or 0) < -1.0 else 0)
    # cutting rates injects liquidity (+); hiking drains (-)
    rt = (1 if (rate_chg_6m or 0) < -0.10 else
          -1 if (rate_chg_6m or 0) > 0.10 else 0)
    return bs + rt


STANCE_LABEL = {2: "STRONG INJECTION", 1: "INJECTING", 0: "NEUTRAL",
                -1: "DRAINING", -2: "STRONG DRAIN"}


def read_existing(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    data, errors = {}, []
    for tag, sid in SERIES.items():
        try:
            data[tag] = fred(sid)
        except Exception as e:
            errors.append(f"{sid}: {str(e)[:80]}")
            data[tag] = []

    def latest(tag):
        return data[tag][0][1] if data.get(tag) else None

    # ── per-central-bank injection reads ──
    banks = []

    # ECB
    ecb_bs6 = pct_change(data["ECB_BS"], 182)
    ecb_bs12 = pct_change(data["ECB_BS"], 365)
    ecb_rate = latest("ECB_RATE")
    ecb_rate6 = level_change(data["ECB_RATE"], 182)
    ecb_st = cb_injection_stance(ecb_bs6, ecb_rate6)
    # the dedicated ecb-detail engine reads excess liquidity straight from the
    # ECB Data Portal — a sharper drain signal than the balance-sheet total,
    # which can sit flat while excess liquidity actively drains
    ecb_d = read_existing("data/ecb-detail.json") or {}
    ecb_liq = ecb_d.get("liquidity") or {}
    ecb_excess = ecb_liq.get("excess_liquidity_eur_bn")
    if ecb_d.get("ok") and ecb_d.get("ecb_injection_score") is not None:
        ecb_st = int(clamp(ecb_d["ecb_injection_score"], -2, 2))
    ecb_extra = (f" Excess liquidity EUR {ecb_excess:,.0f}bn "
                 f"({str(ecb_liq.get('trend', '')).lower()})."
                 if ecb_excess is not None else "")
    banks.append({
        "cb": "ECB", "currency": "EUR",
        "balance_sheet_eur_mn": latest("ECB_BS"),
        "bs_change_6m_pct": ecb_bs6, "bs_change_12m_pct": ecb_bs12,
        "excess_liquidity_eur_bn": ecb_excess,
        "policy_rate_pct": ecb_rate, "rate_change_6m_pp": ecb_rate6,
        "injection_stance": ecb_st, "stance_label": STANCE_LABEL[ecb_st],
        "read": (f"ECB balance sheet {ecb_bs6:+.1f}% over 6m, deposit "
                 f"facility rate {ecb_rate}% — "
                 f"{STANCE_LABEL[ecb_st].lower()}.{ecb_extra}"
                 if ecb_bs6 is not None and ecb_rate is not None
                 else "ECB data partial")})

    # BOJ — the carry funding central bank
    boj_bs6 = pct_change(data["BOJ_BS"], 182)
    boj_bs12 = pct_change(data["BOJ_BS"], 365)
    boj_rate = latest("BOJ_RATE")
    boj_rate6 = level_change(data["BOJ_RATE"], 182)
    boj_st = cb_injection_stance(boj_bs6, boj_rate6)
    banks.append({
        "cb": "BOJ", "currency": "JPY",
        "balance_sheet_index": latest("BOJ_BS"),
        "bs_change_6m_pct": boj_bs6, "bs_change_12m_pct": boj_bs12,
        "policy_rate_pct": boj_rate, "rate_change_6m_pp": boj_rate6,
        "injection_stance": boj_st, "stance_label": STANCE_LABEL[boj_st],
        "read": (f"BOJ balance sheet {boj_bs6:+.1f}% over 6m, short rate "
                 f"{boj_rate}% ({boj_rate6:+.2f}pp/6m) — "
                 f"{STANCE_LABEL[boj_st].lower()}; the carry funding leg."
                 if boj_bs6 is not None and boj_rate is not None
                 else "BOJ data partial")})

    # Fed
    fed_bs6 = pct_change(data["FED_BS"], 182)
    fed_bs12 = pct_change(data["FED_BS"], 365)
    fed_rate = latest("FED_RATE")
    fed_rate6 = level_change(data["FED_RATE"], 182)
    fed_st = cb_injection_stance(fed_bs6, fed_rate6)
    banks.append({
        "cb": "Fed", "currency": "USD",
        "balance_sheet_usd_mn": latest("FED_BS"),
        "bs_change_6m_pct": fed_bs6, "bs_change_12m_pct": fed_bs12,
        "policy_rate_pct": fed_rate, "rate_change_6m_pp": fed_rate6,
        "injection_stance": fed_st, "stance_label": STANCE_LABEL[fed_st],
        "read": (f"Fed balance sheet {fed_bs6:+.1f}% over 6m (QT pace), funds "
                 f"rate {fed_rate}% — {STANCE_LABEL[fed_st].lower()}."
                 if fed_bs6 is not None and fed_rate is not None
                 else "Fed data partial")})

    # SNB — carry funding currency (rate + franc; no clean FRED balance sheet)
    snb_rate = latest("SNB_RATE")
    snb_rate6 = level_change(data["SNB_RATE"], 182)
    snb_st = (1 if (snb_rate6 or 0) < -0.10 else
              -1 if (snb_rate6 or 0) > 0.10 else 0)
    banks.append({
        "cb": "SNB", "currency": "CHF",
        "policy_rate_pct": snb_rate, "rate_change_6m_pp": snb_rate6,
        "injection_stance": snb_st,
        "stance_label": STANCE_LABEL.get(snb_st, "NEUTRAL"),
        "read": (f"SNB short rate {snb_rate}% ({snb_rate6:+.2f}pp/6m) — "
                 f"the franc is the second carry funding currency."
                 if snb_rate is not None else "SNB data partial")})

    # ── net global injection impulse ──
    weights = {"Fed": 1.4, "ECB": 1.2, "BOJ": 1.1, "SNB": 0.5}
    wsum = sum(weights[b["cb"]] for b in banks)
    impulse = sum(b["injection_stance"] * weights[b["cb"]]
                  for b in banks) / wsum
    if impulse >= 0.6:
        imp_label = "NET INJECTION"
    elif impulse > 0.15:
        imp_label = "MILD INJECTION"
    elif impulse >= -0.15:
        imp_label = "NEUTRAL"
    elif impulse > -0.6:
        imp_label = "MILD DRAIN"
    else:
        imp_label = "NET DRAIN"

    # ── carry-trade synthesis ──
    # funding currencies strengthening = carry unwind pressure.
    # DEXJPUS / DEXSZUS are units-per-USD: a FALL = the funding ccy strengthens
    jpy_3m = pct_change(data["JPY"], 91)      # >0 means yen weakened
    jpy_1m = pct_change(data["JPY"], 30)
    chf_3m = pct_change(data["CHF"], 91)
    boj_hawkish = (boj_rate6 or 0) > 0.10

    risk = 0.0
    if jpy_3m is not None:                    # sharp yen strength
        if jpy_3m < -6:
            risk += 45
        elif jpy_3m < -3:
            risk += 28
        elif jpy_3m < -1:
            risk += 12
    if jpy_1m is not None and jpy_1m < -3:    # acute recent move
        risk += 20
    if chf_3m is not None and chf_3m < -3:    # franc strength
        risk += 12
    if boj_hawkish:                           # hawkish funding CB
        risk += 20
    unwind_risk = int(clamp(round(risk), 0, 100))
    unwind_label = ("ELEVATED" if unwind_risk >= 60 else
                    "RAISED" if unwind_risk >= 35 else
                    "MODERATE" if unwind_risk >= 18 else "LOW")

    # carry funding conditions: dovish + weak funding currencies = supportive
    funding_dovish = (boj_st >= 0) and (snb_st >= 0)
    if unwind_risk >= 45:
        carry_cond = "STRESSED"
    elif funding_dovish and unwind_risk < 25:
        carry_cond = "SUPPORTIVE"
    else:
        carry_cond = "NEUTRAL"

    # ── eurodollar read ──
    if imp_label in ("NET DRAIN", "MILD DRAIN") and unwind_risk >= 35:
        edollar = ("Central banks draining while carry-unwind risk builds — "
                   "offshore-USD funding conditions are tightening.")
    elif imp_label in ("NET INJECTION", "MILD INJECTION") and carry_cond != "STRESSED":
        edollar = ("Central banks adding liquidity with carry funding intact "
                   "— supportive for the eurodollar system and risk assets.")
    else:
        edollar = ("Mixed — no decisive central-bank liquidity impulse; "
                   "watch the carry funding currencies for the next signal.")

    # ── cross-reference existing engines (do not rebuild) ──
    gl = read_existing("data/global-liquidity.json") or {}
    cl = read_existing("data/china-liquidity.json") or {}
    cross = {
        "global_liquidity_regime": gl.get("regime") or gl.get("signal"),
        "china_liquidity_regime": cl.get("regime") or cl.get("signal"),
    }

    headline = (f"Global CB impulse: {imp_label}. "
                f"Carry funding {carry_cond.lower()}, unwind risk "
                f"{unwind_label} ({unwind_risk}/100). "
                f"{[b['cb'] for b in banks if b['injection_stance'] > 0] or 'none'} "
                f"injecting; "
                f"{[b['cb'] for b in banks if b['injection_stance'] < 0] or 'none'} "
                f"draining.")

    out = {
        "schema_version": "1.0",
        "method": "cb_capital_injection_and_carry_synthesis",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "ok": len(banks) >= 3 and len(errors) <= 4,
        "headline": headline,
        "global_injection_impulse": {
            "score": round(impulse, 2), "label": imp_label,
            "read": (f"Weighted central-bank stance is {imp_label.lower()} "
                     f"(impulse {impulse:+.2f}).")},
        "central_banks": banks,
        "carry_trade": {
            "funding_currencies": [
                {"ccy": "JPY", "fx_per_usd": latest("JPY"),
                 "change_1m_pct": jpy_1m, "change_3m_pct": jpy_3m,
                 "funding_cb": "BOJ", "cb_stance": boj_st},
                {"ccy": "CHF", "fx_per_usd": latest("CHF"),
                 "change_3m_pct": chf_3m, "funding_cb": "SNB",
                 "cb_stance": snb_st}],
            "carry_conditions": carry_cond,
            "unwind_risk_score": unwind_risk,
            "unwind_risk_label": unwind_label,
            "read": (f"Carry funding conditions {carry_cond.lower()}; "
                     f"unwind risk {unwind_label.lower()}. A sharp move "
                     f"higher in the yen or franc, or a hawkish BOJ, is what "
                     f"forces a leveraged carry unwind.")},
        "eurodollar_read": edollar,
        "cross_reference": cross,
        "errors": errors,
        "note": ("Per-central-bank capital-injection stance (balance-sheet "
                 "trajectory + policy-rate direction) for the ECB, BOJ, Fed "
                 "and SNB, plus net global liquidity impulse and carry-trade "
                 "funding / unwind-risk synthesis. Complements global-"
                 "liquidity (which tracks the level) by tracking the flow. "
                 "Built on real FRED data — not investment advice."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")

    try:
        snap = {"date": now.date().isoformat(), "impulse": round(impulse, 2),
                "impulse_label": imp_label, "unwind_risk": unwind_risk,
                "carry_conditions": carry_cond}
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"data/cb-injection/snapshots/{now.date().isoformat()}.json",
            Body=json.dumps(snap).encode("utf-8"),
            ContentType="application/json")
    except Exception as e:
        print(f"[cb-injection] snapshot skipped: {e}")

    print(f"[cb-injection] impulse={imp_label} carry={carry_cond} "
          f"unwind={unwind_risk} banks={len(banks)} errors={len(errors)} "
          f"{out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": out["ok"], "impulse": imp_label,
        "carry_conditions": carry_cond, "unwind_risk": unwind_risk})}
