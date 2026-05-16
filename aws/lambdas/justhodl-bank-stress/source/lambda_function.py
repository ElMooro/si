"""justhodl-bank-stress — acute bank & funding-system stress monitor.

The platform's securities-banking-agent tracks slow bank balance-sheet health
(loans, deposits, NPLs, charge-offs, ROA). This is the complement: the ACUTE
funding-stress signals that move in days during a banking crisis — emergency
liquidity facility usage and reserve adequacy. This is the SVB-style early
warning the rest of the stack only covers indirectly.

WHAT IT TRACKS (all FRED — free, official; defensive series resolution):

  EMERGENCY LIQUIDITY DRAWS — banks tapping the lender of last resort:
    WLCFLPCL   Fed discount window — Primary Credit. Near-zero in calm times;
               spiked to ~$150bn in the SVB week. Carries stigma — any real
               draw means a bank could not fund privately.
    H41RESPPALDKNWW  BTFP outstanding (the post-SVB facility).
    SWPT       Central Bank Liquidity Swaps — foreign central banks drawing
               USD. A direct read on GLOBAL dollar-funding stress.

  RESERVE ADEQUACY — the Sept-2019-repo-blowup early warning:
    WRESBAL    Reserve balances banks hold at the Fed.
    GDP        used for reserves-to-GDP. Below ~10-11% of GDP the system
               enters "reserve scarcity" and repo/funding markets get jumpy.

BANK FUNDING STRESS SCORE 0-100 (higher = more stress) -> regime:
    ABUNDANT / ADEQUATE / TIGHTENING / SCARCE

Telegram on regime change or any meaningful emergency draw.
Output: data/bank-stress.json   Schedule: daily.
"""
import json, os, time
from datetime import datetime, timezone
from urllib import request, error
import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/bank-stress.json"
S3_HISTORY_KEY = "data/bank-stress-history.json"
HISTORY_MAX = 260

FRED_KEY = os.environ.get("FRED_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")

SERIES = {
    "discount_window": ["WLCFLPCL"],
    "btfp": ["H41RESPPALDKNWW"],
    "swap_lines": ["SWPT"],
    "reserves": ["WRESBAL"],
    "gdp": ["GDP"],
}


def _get_json(url, timeout=15, retries=3):
    last = None
    for i in range(retries):
        try:
            req = request.Request(url, headers={"User-Agent": "JustHodl-BankStress/1.0"})
            with request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError) as e:
            last = e
            time.sleep(0.5 * (i + 1))
    return None


def fred(series_id, limit=300):
    if not FRED_KEY:
        return []
    url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
           f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={limit}")
    j = _get_json(url)
    if not j:
        return []
    out = []
    for o in j.get("observations", []):
        v = o.get("value")
        if v in (None, ".", ""):
            continue
        try:
            out.append({"date": o.get("date"), "value": float(v)})
        except Exception:
            pass
    return out


def fred_first(cands):
    for sid in cands:
        obs = fred(sid)
        if obs:
            return sid, obs
    return None, []


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return
    try:
        body = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                            "parse_mode": "HTML", "disable_web_page_preview": True}).encode("utf-8")
        req = request.Request(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                               data=body, headers={"Content-Type": "application/json"})
        request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[tg] err: {e}")


def m_to_bn(v):
    """FRED H.4.1 series (WLCFLPCL, BTFP, SWPT, WRESBAL) all report in
    millions of dollars -> convert to billions. GDP is already in billions
    and is NOT passed through here."""
    if v is None:
        return None
    return v / 1e3


def clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, x))


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[bank-stress] starting {datetime.now(timezone.utc).isoformat()}")
    if not FRED_KEY:
        return {"statusCode": 500, "body": json.dumps({"error": "FRED_API_KEY not set"})}

    resolved, raw = {}, {}
    for key, cands in SERIES.items():
        sid, obs = fred_first(cands)
        resolved[key] = sid
        raw[key] = obs
        print(f"[bank-stress] {key}: {sid} ({len(obs)} obs)")
    failed = [k for k, v in raw.items() if not v]

    # ── emergency liquidity draws (USD billions) ──
    dw = raw.get("discount_window", [])
    dw_now = m_to_bn(dw[0]["value"]) if dw else None
    dw_4w_ago = m_to_bn(dw[4]["value"]) if len(dw) > 4 else None

    btfp = raw.get("btfp", [])
    btfp_now = m_to_bn(btfp[0]["value"]) if btfp else None

    swaps = raw.get("swap_lines", [])
    swaps_now = m_to_bn(swaps[0]["value"]) if swaps else None

    # ── reserve adequacy ──
    res = raw.get("reserves", [])
    gdp = raw.get("gdp", [])
    reserves_now = m_to_bn(res[0]["value"]) if res else None
    gdp_now = gdp[0]["value"] if gdp else None  # GDP already in $bn
    reserves_to_gdp = None
    if reserves_now and gdp_now and gdp_now > 0:
        reserves_to_gdp = reserves_now / gdp_now * 100

    # ── component stress scores (0-100) ──
    comps = []

    # discount window: ~0 calm, $150bn = SVB-week crisis
    if dw_now is not None:
        dw_stress = clamp(dw_now / 150.0 * 100)
        comps.append(("Discount window primary credit", dw_stress, 0.30, f"${dw_now:.1f}bn"))

    # BTFP: legacy facility — any large balance = lingering stress
    if btfp_now is not None:
        btfp_stress = clamp(btfp_now / 150.0 * 100)
        comps.append(("BTFP outstanding", btfp_stress, 0.15, f"${btfp_now:.1f}bn"))

    # swap lines: foreign CB drawing USD — >$10bn = real global stress, $500bn = 2008/2020
    if swaps_now is not None:
        sw_stress = clamp(swaps_now / 200.0 * 100)
        comps.append(("Central bank USD swap lines", sw_stress, 0.20, f"${swaps_now:.1f}bn"))

    # reserve adequacy: below ~11% of GDP = scarcity zone (Sept-2019 pattern)
    if reserves_to_gdp is not None:
        # 14%+ abundant (0 stress) -> 8% severe scarcity (100 stress)
        rg_stress = clamp((14.0 - reserves_to_gdp) / 6.0 * 100)
        comps.append(("Reserve scarcity", rg_stress, 0.35,
                       f"{reserves_to_gdp:.1f}% of GDP"))

    if not comps:
        return {"statusCode": 500, "body": json.dumps({"error": "no bank-stress series available"})}

    wsum = sum(w for _, _, w, _ in comps)
    score = sum(v * w for _, v, w, _ in comps) / wsum

    if score >= 70:
        regime = "SCARCE"
        regime_read = ("Acute bank funding stress — emergency facilities in use and/or "
                       "reserves scarce. This is the zone where funding markets seize "
                       "(Sept-2019 repo, March-2023 SVB). Treat as a systemic warning.")
    elif score >= 45:
        regime = "TIGHTENING"
        regime_read = ("Bank funding is tightening — reserves draining toward the "
                       "scarcity zone and/or modest facility usage. Watch repo rates "
                       "(SOFR-IORB) and discount-window draws closely.")
    elif score >= 22:
        regime = "ADEQUATE"
        regime_read = ("Bank funding is adequate. Reserves sufficient, emergency "
                       "facilities quiet. Normal plumbing.")
    else:
        regime = "ABUNDANT"
        regime_read = ("Bank funding is abundant — ample reserves, no emergency draws. "
                       "The banking system has plenty of liquidity cushion.")

    # Reserves in the genuine scarcity zone are the pre-condition for funding
    # stress even before any facility draw — floor the regime at TIGHTENING.
    if (reserves_to_gdp is not None and reserves_to_gdp < 10.5
            and regime in ("ABUNDANT", "ADEQUATE")):
        regime = "TIGHTENING"
        regime_read = ("Reserves are in the scarcity zone (below 10.5% of GDP) — the "
                       "pre-condition for repo/funding stress, even though emergency "
                       "facilities are currently quiet. This is exactly the setup that "
                       "preceded the Sept-2019 repo blow-up. Watch SOFR-IORB closely.")

    # meaningful emergency draw flag
    emergency_draw = False
    draw_notes = []
    if dw_now is not None and dw_now >= 15:
        emergency_draw = True
        draw_notes.append(f"discount window ${dw_now:.0f}bn")
    if dw_now is not None and dw_4w_ago is not None and dw_now - dw_4w_ago >= 10:
        emergency_draw = True
        draw_notes.append(f"discount window +${dw_now-dw_4w_ago:.0f}bn in 4w")
    if swaps_now is not None and swaps_now >= 10:
        emergency_draw = True
        draw_notes.append(f"USD swap lines ${swaps_now:.0f}bn drawn")

    hist = {"snapshots": []}
    try:
        hist = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY)["Body"].read())
    except Exception:
        pass
    prior_regime = hist["snapshots"][-1]["regime"] if hist.get("snapshots") else None

    out = {
        "schema_version": "1.0",
        "method": "bank_stress_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "fred_failed": failed,
        "series_resolved": resolved,
        "bank_stress_score": round(score, 1),
        "regime": regime,
        "regime_read": regime_read,
        "emergency_draw": emergency_draw,
        "emergency_notes": draw_notes,
        "emergency_liquidity": {
            "discount_window_bn": round(dw_now, 1) if dw_now is not None else None,
            "discount_window_4w_change_bn": (round(dw_now - dw_4w_ago, 1)
                                              if (dw_now is not None and dw_4w_ago is not None) else None),
            "btfp_outstanding_bn": round(btfp_now, 1) if btfp_now is not None else None,
            "swap_lines_bn": round(swaps_now, 1) if swaps_now is not None else None,
        },
        "reserve_adequacy": {
            "reserves_bn": round(reserves_now, 1) if reserves_now is not None else None,
            "reserves_to_gdp_pct": round(reserves_to_gdp, 2) if reserves_to_gdp is not None else None,
            "scarcity_threshold_pct": 11.0,
            "read": ("reserves in the scarcity zone — repo stress risk elevated"
                     if (reserves_to_gdp or 99) < 11
                     else "reserves adequate to abundant"),
        },
        "components": [
            {"label": l, "stress": round(v, 1), "weight": w, "value": disp}
            for l, v, w, disp in comps
        ],
        "methodology": (
            "Acute bank funding stress: emergency liquidity draws (discount window, "
            "BTFP, USD swap lines) + reserve adequacy (reserves-to-GDP vs the ~11% "
            "scarcity threshold). Complements securities-banking-agent's slow "
            "balance-sheet health view."
        ),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    hist["snapshots"].append({"ts": out["generated_at"], "regime": regime,
                               "score": round(score, 1),
                               "reserves_to_gdp": reserves_to_gdp,
                               "discount_window_bn": dw_now})
    hist["snapshots"] = hist["snapshots"][-HISTORY_MAX:]
    hist["updated_at"] = out["generated_at"]
    s3.put_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY,
                   Body=json.dumps(hist, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    if (prior_regime and prior_regime != regime) or emergency_draw:
        head = ("EMERGENCY LIQUIDITY DRAW" if emergency_draw
                else "BANK FUNDING REGIME CHANGE")
        maybe_telegram(
            f"[bank-stress] <b>{head}</b>\n"
            + (f"<b>{prior_regime} → {regime}</b>\n" if prior_regime and prior_regime != regime else "")
            + f"Stress score: {score:.0f}/100\n"
            + (f"Draws: {', '.join(draw_notes)}\n" if draw_notes else "")
            + regime_read)

    print(f"[bank-stress] done {out['elapsed_s']}s score={score:.1f} regime={regime} "
          f"emergency={emergency_draw} failed={failed}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "bank_stress_score": round(score, 1), "regime": regime,
        "emergency_draw": emergency_draw, "fred_failed": failed})}
