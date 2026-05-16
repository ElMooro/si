"""justhodl-capitulation — the Capitulation / Generational-Buy engine.

Your platform is excellent at sniffing danger. This is the flip side: the
engine that fires when panic has hit washout extremes and the market is
historically SCREAMING a buy. The best entries of a lifetime — 2009, 2020,
2011, late-2018, 2022 lows — all shared the same fingerprint: breadth
washed out, credit blown out, volatility spiked, AND insiders quietly
buying their own stock into the fear.

TWO IDEAS, kept separate on purpose:

  CAPITULATION SCORE (0-100) — how much blood is in the streets right now.
    Pure washout intensity. High = deeply oversold, panic pricing.

  STABILISATION — has the knife stopped falling? (crisis trend improving,
    breadth ticking up off the low). Capitulation WITHOUT stabilisation =
    "wait, the knife is still falling." Capitulation WITH stabilisation =
    "back up the truck."

WASHOUT INPUTS (fused from existing sidecars — degrades gracefully):
  crisis-composite   master crisis score / DEFCON / trend
  market-internals   breadth washout (low breadth = capitulation)
  credit-stress      HY spread blowout
  vol-surface        volatility spike
  eurodollar-stress  funding panic
  insider-aggregate  SMART-MONEY CONFIRM — insiders accumulating into the fear

SIGNAL:
  GENERATIONAL_BUY      capitulation >=75, stabilising, insiders accumulating
  CAPITULATION_WAIT     capitulation >=75 but knife still falling — be patient
  STRONG_BUY            capitulation >=55 and stabilising
  ACCUMULATE            capitulation >=40 — selective adds
  NO_SIGNAL             normal / greedy market — nothing to do here

Each signal carries a crisis SHOPPING LIST — what historically works from
that level. Output: data/capitulation.json   Schedule: every 3 hours.
"""
import json, os, time
from datetime import datetime, timezone
from urllib import request, error
import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/capitulation.json"
S3_HISTORY_KEY = "data/capitulation-history.json"
HISTORY_MAX = 480

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")


def get_s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[capitulation] missing {key}: {e}")
        return None


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


def dig(obj, *names):
    if not isinstance(obj, dict):
        return None
    for n in names:
        if n in obj and obj[n] is not None:
            return obj[n]
    for v in obj.values():
        if isinstance(v, dict):
            for n in names:
                if n in v and v[n] is not None:
                    return v[n]
    return None


def clamp(x, lo=0.0, hi=100.0):
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return None


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[capitulation] starting {datetime.now(timezone.utc).isoformat()}")

    crisis = get_s3_json("data/crisis-composite.json")
    internals = get_s3_json("data/market-internals.json")
    credit = get_s3_json("data/credit-stress.json")
    vol = get_s3_json("data/vol-surface.json")
    euro = get_s3_json("data/eurodollar-stress.json")
    insider = get_s3_json("data/insider-aggregate.json")

    washout = []   # list of (label, intensity 0-100, weight)

    # 1. crisis severity — DEFCON 1-2 means deep stress
    crisis_score = clamp(dig(crisis, "master_crisis_score"))
    if crisis_score is not None:
        washout.append(("Crisis severity", crisis_score, 0.28))

    # 2. breadth washout — low breadth_score = capitulation
    breadth = dig(internals, "breadth_score", "score")
    if isinstance(breadth, (int, float)):
        washout.append(("Breadth washout", clamp(100 - breadth), 0.22))

    # 3. credit blowout — map HY regime / numeric to intensity
    cv = dig(credit, "composite_stress_score", "composite_score")
    if isinstance(cv, (int, float)):
        washout.append(("Credit blowout", clamp(cv), 0.20))
    else:
        lab = str(dig(credit, "composite_regime", "regime") or "").upper()
        intensity = (92 if "CRISIS" in lab else 75 if ("WIDE" in lab or "STRESS" in lab)
                     else 55 if "ELEVATED" in lab else 30 if "NORMAL" in lab else None)
        if intensity is not None:
            washout.append(("Credit blowout", intensity, 0.20))

    # 4. volatility spike
    volscore = clamp(dig(vol, "composite_stress_score", "stress_score", "score"))
    if volscore is not None:
        washout.append(("Volatility spike", volscore, 0.15))

    # 5. funding panic
    euroscore = clamp(dig(euro, "composite_score", "score"))
    if euroscore is not None:
        washout.append(("Funding panic", euroscore, 0.15))

    if not washout:
        return {"statusCode": 500, "body": json.dumps({"error": "no washout sidecars available"})}

    wsum = sum(w for _, _, w in washout)
    capitulation_score = sum(v * w for _, v, w in washout) / wsum

    # ── smart-money confirm: insiders accumulating into the fear ──
    insider_regime = str(dig(insider, "regime") or "").upper()
    insider_ratio = dig(insider, "headline_ratio_30d_dollar")
    smart_money_confirm = ("ACCUMULAT" in insider_regime
                           or (isinstance(insider_ratio, (int, float)) and insider_ratio >= 0.6))

    # ── stabilisation: has the knife stopped falling? ──
    crisis_trend = str(dig(crisis, "trend") or "").lower()
    stabilising = crisis_trend in ("improving", "stable")

    # ── signal logic ──
    if capitulation_score >= 75 and stabilising and smart_money_confirm:
        signal = "GENERATIONAL_BUY"
        action = ("The rare alignment: deep washout + the selling has stopped + insiders "
                  "buying into the fear. Historically the best risk-adjusted entry there "
                  "is. Deploy aggressively into quality — scale in, do not wait for the "
                  "all-clear (it never rings a bell).")
    elif capitulation_score >= 75 and not stabilising:
        signal = "CAPITULATION_WAIT"
        action = ("Blood is in the streets but the knife is still falling — crisis metrics "
                  "still deteriorating. Prepare the shopping list, ready the cash, but let "
                  "stabilisation confirm before deploying size. Patience here is the edge.")
    elif capitulation_score >= 55 and stabilising:
        signal = "STRONG_BUY"
        action = ("Meaningful washout with signs of stabilisation. Start scaling into "
                  "oversold quality and the bagger-engine top tier. Keep some reserve for "
                  "a deeper flush.")
    elif capitulation_score >= 40:
        signal = "ACCUMULATE"
        action = ("Moderate stress — selective adds in genuinely oversold high-quality "
                  "names. Not a back-up-the-truck moment; stay disciplined on entry price.")
    else:
        signal = "NO_SIGNAL"
        action = ("No capitulation. Market is calm-to-greedy — this engine has nothing to "
                  "say. Run the normal playbook; do not manufacture a bottom that is not "
                  "there.")

    # ── crisis shopping list keyed to the signal ──
    if signal in ("GENERATIONAL_BUY", "STRONG_BUY"):
        shopping = [
            "Bagger-engine POTENTIAL_100X / 25X tier — multibaggers get marked down hardest in panics",
            "High-ROIC compounders trading below their own 5yr average multiple",
            "Quality balance sheets (net cash) that the selloff treated like junk",
            "Long-duration Treasuries IF the selloff is a growth scare (not an inflation shock)",
            "Beaten-down leaders in secular-growth themes — the theme survives the drawdown",
        ]
    elif signal == "CAPITULATION_WAIT":
        shopping = [
            "Build the list now: rank bagger-engine top tier by how far below fair value they have fallen",
            "Hold cash/T-bills as dry powder — do not deploy until stabilisation confirms",
            "Watch crisis-composite trend flip to 'improving' as the green light",
        ]
    elif signal == "ACCUMULATE":
        shopping = [
            "Selective adds: only genuinely oversold high-quality names",
            "Keep most powder dry — a bigger flush may still come",
        ]
    else:
        shopping = ["Nothing to buy on weakness — no washout present. Stay with the core."]

    hist = get_s3_json(S3_HISTORY_KEY) or {"snapshots": []}
    prior_signal = hist["snapshots"][-1]["signal"] if hist.get("snapshots") else None

    out = {
        "schema_version": "1.0",
        "method": "capitulation_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "capitulation_score": round(capitulation_score, 1),
        "signal": signal,
        "action": action,
        "stabilising": stabilising,
        "smart_money_confirm": smart_money_confirm,
        "insider_regime": insider_regime or None,
        "crisis_trend": crisis_trend or None,
        "washout_components": [
            {"label": l, "intensity": round(v, 1), "weight": w} for l, v, w in washout
        ],
        "shopping_list": shopping,
        "interpretation": (
            "Capitulation score = washout intensity (breadth, credit, vol, funding, "
            "crisis severity). GENERATIONAL_BUY requires deep washout AND stabilisation "
            "AND insiders accumulating — one extreme alone is noise; the alignment is "
            "the signal."
        ),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=900")

    hist["snapshots"].append({"ts": out["generated_at"], "signal": signal,
                               "capitulation_score": round(capitulation_score, 1)})
    hist["snapshots"] = hist["snapshots"][-HISTORY_MAX:]
    hist["updated_at"] = out["generated_at"]
    s3.put_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY,
                   Body=json.dumps(hist, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=900")

    # Telegram on entering a buy signal (not on leaving)
    buy_signals = {"GENERATIONAL_BUY", "STRONG_BUY"}
    if signal in buy_signals and prior_signal not in buy_signals:
        maybe_telegram(
            f"[capitulation] <b>{signal.replace('_',' ')}</b> 🎯\n"
            f"Capitulation score: {capitulation_score:.0f}/100\n"
            f"Stabilising: {stabilising} · Insiders: {insider_regime or 'n/a'}\n\n"
            f"{action}")

    print(f"[capitulation] done {out['elapsed_s']}s score={capitulation_score:.1f} "
          f"signal={signal} stabilising={stabilising} smc={smart_money_confirm}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "capitulation_score": round(capitulation_score, 1),
        "signal": signal, "stabilising": stabilising})}
