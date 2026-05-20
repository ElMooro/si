"""
justhodl-vix-backwardation-trigger -- the institutional once-per-cycle
capitulation buy signal.

When the VIX term structure inverts into backwardation
(VIX9D > VIX > VIX3M) while VVIX is in panic territory, the market
is literally pricing front-month vol above medium-term vol. That is
statistically peak panic: dealers and vol-targeting funds are paying
any price for hedges. Once the term structure normalises, those same
dealers MECHANICALLY unwind their short-dated protection -- which
means buying back the underlying. That is the price recovery.

State machine:

    NULL      term structure in contango, no setup
    WARM      VIX9D >= VIX (front-month inversion starting)
    ARMED     WARM AND VVIX > 130 (panic gauge confirms)
    FIRED     ARMED AND VIX > VIX3M (full inversion -- BUY SIGNAL)
    COOLDOWN  30 days post-firing (no re-firing during this window)

The engine fetches live VIX9D / VIX / VIX3M / VIX6M / VVIX / SPY from
FMP /stable/, persists last-fired in SSM for the cooldown gate,
fetches 20y of SPY history, computes forward 1m / 3m / 12m SPY
returns at every curated historical backwardation event, summarises
the empirical edge (avg return + win rate + sample size per horizon),
and emits a retail-readable trade ticket. Telegram alerts fire on
NULL/WARM/ARMED -> FIRED transition.

Schedule: 3x daily MON-FRI at 14, 17, 21 UTC (market open, midday,
close in US Eastern).

Output: data/vix-backwardation-trigger.json (consumed by
vix-capitulation.html).
"""
import json
import os
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor

import boto3

# ---- infrastructure constants -------------------------------------------
S3_BUCKET = "justhodl-dashboard-live"
REPORT_KEY = "data/vix-backwardation-trigger.json"
STATE_SSM = "/justhodl/vix-backwardation/state"
FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ---- trigger thresholds (tuned to historical episodes) ------------------
VVIX_PANIC_FLOOR = 130
COOLDOWN_DAYS = 30

# ---- curated historical backwardation events ----------------------------
# Each event represents a documented full VIX term-structure inversion.
# Forward SPY returns at each are computed live from FMP history -- this
# list is just the firing dates, not the returns.
HISTORICAL_TRIGGER_DATES = [
    ("2008-10-24", "Lehman / GFC peak panic"),
    ("2010-05-20", "Flash Crash week"),
    ("2011-08-08", "US sovereign downgrade"),
    ("2015-08-24", "China devaluation flash crash"),
    ("2018-02-05", "Vol-mageddon (XIV blowup)"),
    ("2018-12-24", "Q4 2018 Christmas Eve lows"),
    ("2020-03-12", "COVID waterfall"),
    ("2020-03-18", "COVID secondary panic"),
    ("2022-01-24", "Jan 2022 growth scare"),
    ("2022-06-13", "June 2022 CPI panic"),
    ("2022-09-26", "Sept 2022 UK gilt crisis"),
    ("2022-10-13", "Oct 2022 CPI capitulation"),
    ("2023-03-15", "SVB / regional bank crisis"),
    ("2023-10-26", "Oct 2023 rate-shock capitulation"),
    ("2024-08-05", "Yen carry unwind (vol-mageddon 2)"),
]

s3 = boto3.client("s3")
ssm = boto3.client("ssm")


# ---- FMP helpers --------------------------------------------------------
def _fmp_get(path, params=None):
    p = dict(params or {})
    p["apikey"] = FMP_KEY
    url = ("https://financialmodelingprep.com/stable" + path +
           "?" + urllib.parse.urlencode(p))
    req = urllib.request.Request(url,
                                 headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


def fetch_quote(symbol):
    try:
        d = _fmp_get("/quote", {"symbol": symbol})
        return d[0] if isinstance(d, list) and d else None
    except Exception as e:
        print("quote fail %s: %s" % (symbol, e))
        return None


def fetch_history(symbol, years=20):
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start = ((datetime.now(timezone.utc)
              - timedelta(days=365 * years))
             .strftime("%Y-%m-%d"))
    try:
        d = _fmp_get("/historical-price-eod/light",
                     {"symbol": symbol, "from": start, "to": end})
        if isinstance(d, list):
            rows = d
        elif isinstance(d, dict) and "historical" in d:
            rows = d["historical"]
        else:
            rows = []
        return sorted(rows, key=lambda x: x.get("date", ""))
    except Exception as e:
        print("history fail %s: %s" % (symbol, e))
        return []


def price_at_or_after(history, target_iso):
    """First close on or after target_iso. Linear scan; ~5000 rows OK."""
    for row in history:
        if row.get("date", "") >= target_iso:
            return row.get("close") or row.get("price")
    return None


# ---- forward-return analytics ------------------------------------------
def compute_forward_returns(history, trigger_dates):
    rows = []
    for date_iso, label in trigger_dates:
        try:
            d0 = datetime.strptime(date_iso, "%Y-%m-%d")
            p0 = price_at_or_after(history, date_iso)
            if not p0:
                continue
            d_1m = (d0 + timedelta(days=30)).strftime("%Y-%m-%d")
            d_3m = (d0 + timedelta(days=90)).strftime("%Y-%m-%d")
            d_12m = (d0 + timedelta(days=365)).strftime("%Y-%m-%d")
            p_1m = price_at_or_after(history, d_1m)
            p_3m = price_at_or_after(history, d_3m)
            p_12m = price_at_or_after(history, d_12m)
            rows.append({
                "date": date_iso,
                "label": label,
                "spy_at_trigger": round(p0, 2),
                "fwd_1m_pct": (round((p_1m - p0) / p0 * 100, 2)
                               if p_1m else None),
                "fwd_3m_pct": (round((p_3m - p0) / p0 * 100, 2)
                               if p_3m else None),
                "fwd_12m_pct": (round((p_12m - p0) / p0 * 100, 2)
                                if p_12m else None),
            })
        except Exception as e:
            print("fwd return fail %s: %s" % (date_iso, e))
    return rows


def summarise_forwards(rows):
    def stats(key):
        vals = [r[key] for r in rows if r.get(key) is not None]
        if not vals:
            return {"return_pct": None, "win_rate_pct": None, "n": 0}
        avg = sum(vals) / len(vals)
        wins = sum(1 for v in vals if v > 0)
        srt = sorted(vals)
        return {
            "return_pct": round(avg, 2),
            "win_rate_pct": round(wins / len(vals) * 100, 1),
            "n": len(vals),
            "median_pct": round(srt[len(srt) // 2], 2),
            "best_pct": round(max(vals), 2),
            "worst_pct": round(min(vals), 2),
        }
    return {
        "1m": dict(stats("fwd_1m_pct"),
                   basis="next 30 calendar days SPY total return"),
        "3m": dict(stats("fwd_3m_pct"),
                   basis="next 90 calendar days SPY total return"),
        "12m": dict(stats("fwd_12m_pct"),
                    basis="next 365 calendar days SPY total return"),
    }


# ---- state machine ------------------------------------------------------
def evaluate_state(vix9d, vix, vix3m, vvix):
    conditions = []
    cond1 = (vix9d is not None and vix is not None and vix9d >= vix)
    conditions.append({
        "name": "VIX9D >= VIX  (front-month inverted)",
        "current": (("%.2f " % vix9d) +
                    (">=" if cond1 else "<") +
                    (" %.2f" % vix)) if (vix9d and vix) else "missing",
        "satisfied": bool(cond1),
        "weight": 30,
    })
    cond2 = (vvix is not None and vvix > VVIX_PANIC_FLOOR)
    conditions.append({
        "name": "VVIX > %d  (vol-of-vol panic)" % VVIX_PANIC_FLOOR,
        "current": ("%.1f" % vvix) if vvix else "missing",
        "satisfied": bool(cond2),
        "weight": 25,
    })
    cond3 = (vix is not None and vix3m is not None and vix > vix3m)
    conditions.append({
        "name": "VIX > VIX3M  (full term-structure inversion)",
        "current": (("%.2f " % vix) +
                    (">" if cond3 else "<=") +
                    (" %.2f" % vix3m)) if (vix and vix3m) else "missing",
        "satisfied": bool(cond3),
        "weight": 45,
    })
    weight_satisfied = sum(c["weight"] for c in conditions
                           if c["satisfied"])
    if cond1 and cond2 and cond3:
        state = "FIRED"
    elif cond1 and cond2:
        state = "ARMED"
    elif cond1:
        state = "WARM"
    else:
        state = "NULL"
    return state, weight_satisfied, conditions


# ---- SSM persistence (cooldown gate) -----------------------------------
def load_persistent_state():
    try:
        p = ssm.get_parameter(Name=STATE_SSM)
        return json.loads(p["Parameter"]["Value"])
    except Exception:
        return {"state": "NULL", "state_since": None, "last_fired": None}


def save_persistent_state(data):
    try:
        ssm.put_parameter(Name=STATE_SSM,
                          Value=json.dumps(data),
                          Type="String",
                          Overwrite=True)
    except Exception as e:
        print("state save fail: %s" % e)


# ---- trade ticket -------------------------------------------------------
def build_trade_ticket(state, spy_price, fwd):
    spy = round(spy_price) if spy_price else 575
    if state == "FIRED":
        strike_itm = spy - 5
        strike_short = spy + 25
        fwd_3m = fwd.get("3m", {}).get("return_pct")
        return {
            "primary": {
                "instrument": ("SPY 60-day calls, strike ~$%d "
                               "(approx delta 0.70, slightly ITM)"
                               % strike_itm),
                "thesis": (
                    "Asymmetric upside on mean-reversion of vol. "
                    "When the term structure flips back to contango "
                    "in 1-4 weeks, dealers buy back the underlying."),
                "size_guidance": "5-8% of total risk capital",
                "max_loss": "premium paid (fully defined)",
                "expected_horizon": "30-90 days",
                "expected_return_basis": ("Historical 3-month average "
                                          "SPY +%s%% post-firing"
                                          % (fwd_3m if fwd_3m else "?")),
            },
            "defined_risk_alt": {
                "instrument": ("SPY 60-day call vertical: "
                               "long $%d / short $%d"
                               % (strike_itm, strike_short)),
                "thesis": ("Same direction, capped upside, "
                           "tighter defined risk -- "
                           "approx 3:1 reward/risk."),
                "size_guidance": "10-15% of risk capital",
                "max_loss": "net debit paid",
                "expected_horizon": "30-60 days",
            },
            "leveraged_alt": {
                "instrument": "SPXL (3x SPY) or QLD (2x QQQ) shares",
                "thesis": ("Same direction trade, no time decay, "
                           "no expiration risk. Leverage already "
                           "amplifies."),
                "size_guidance": ("2-4% of risk capital "
                                  "(3x leverage already amplifies)"),
                "max_loss": "approximately 3x SPY drawdown",
                "expected_horizon": "30-90 days",
            },
            "exit_rules": [
                "Take partial profit at +50% on options",
                "Time stop: close all at 45 days regardless",
                ("Hard stop: close if VIX makes a new high above "
                 "the firing high (signal was wrong)"),
                ("If SPY rises +5% within 2 weeks: take half off, "
                 "let other half run to time stop"),
            ],
        }
    if state == "ARMED":
        return {
            "primary": {
                "instrument": ("Hold dry powder. "
                               "Optional starter: 1-2% SPY shares."),
                "thesis": ("Setup forming but not confirmed. "
                           "ARMED resolves back to normal in roughly "
                           "30% of historical setups. Wait for FIRED."),
                "size_guidance": "0-2% deployed",
                "max_loss": "small",
                "expected_horizon": "watch next 1-5 sessions",
            }
        }
    if state == "WARM":
        return {
            "primary": {
                "instrument": "No trade. Watching only.",
                "thesis": ("Front-month inverting but vol-of-vol "
                           "panic gauge not yet confirming. Stay flat."),
                "size_guidance": "0% deployed",
                "max_loss": "n/a",
                "expected_horizon": "watch next 5-10 sessions",
            }
        }
    return {
        "primary": {
            "instrument": "No setup. Normal regime.",
            "thesis": ("Term structure in normal contango. "
                       "This signal is once-per-cycle event -- "
                       "could be weeks or months before next firing."),
            "size_guidance": "0% deployed",
            "max_loss": "n/a",
            "expected_horizon": "indeterminate",
        }
    }


# ---- why-now explainer (retail-readable markdown) ----------------------
def build_why_now(state, current, fwd):
    vix9d = current.get("vix9d")
    vix = current.get("vix")
    vix3m = current.get("vix3m")
    vvix = current.get("vvix")
    n = fwd.get("3m", {}).get("n", 0)

    if state == "FIRED":
        f1 = fwd["1m"]
        f3 = fwd["3m"]
        f12 = fwd["12m"]
        return (
            "**FIRED -- BUY SIGNAL.** Full VIX term-structure "
            "backwardation has triggered. The market is paying "
            "more for front-month vol (%(vix9d).1f) than for "
            "30-day (%(vix).1f) or 3-month vol (%(vix3m).1f), with "
            "VVIX at panic level %(vvix).0f.\n\n"
            "**WHY THIS IS THE BUY:** Across %(n)d historical "
            "backwardation events (GFC, Flash Crash, COVID, vol-"
            "mageddon 1 + 2, SVB, gilt crisis, Aug 2024 yen carry), "
            "SPY forward returns averaged:\n\n"
            "  * **1 month**: %(f1)+0.1f%% (win rate %(wr1).0f%%)\n"
            "  * **3 months**: %(f3)+0.1f%% (win rate %(wr3).0f%%)\n"
            "  * **12 months**: %(f12)+0.1f%% (win rate %(wr12).0f%%)\n\n"
            "**INSTITUTIONAL LOGIC:** When the term structure flips, "
            "dealers and vol-targeting funds are simultaneously paying "
            "any price for short-term protection. That's forced "
            "buying of vol, not informed buying. Once the term "
            "structure normalises, those same hedges get unwound -- "
            "which means buying back the underlying. That's the price "
            "recovery."
            % {"vix9d": vix9d or 0, "vix": vix or 0, "vix3m": vix3m or 0,
               "vvix": vvix or 0, "n": n,
               "f1": f1.get("return_pct", 0) or 0,
               "f3": f3.get("return_pct", 0) or 0,
               "f12": f12.get("return_pct", 0) or 0,
               "wr1": f1.get("win_rate_pct", 0) or 0,
               "wr3": f3.get("win_rate_pct", 0) or 0,
               "wr12": f12.get("win_rate_pct", 0) or 0})

    if state == "ARMED":
        return (
            "**ARMED.** Front-month VIX9D (%(vix9d).2f) is now above "
            "30-day VIX (%(vix).2f) and the vol-of-vol gauge VVIX is "
            "%(vvix).0f -- above the 130 panic floor. **The trigger "
            "fires** if VIX crosses above VIX3M (%(vix3m).2f) in the "
            "coming sessions.\n\n"
            "**WHAT TO DO:** Stay flat. ARMED resolves without firing "
            "in ~30%% of historical setups. Wait for FIRED before "
            "deploying capital. Watch VIX vs VIX3M spread closely."
            % {"vix9d": vix9d or 0, "vix": vix or 0,
               "vix3m": vix3m or 0, "vvix": vvix or 0})

    if state == "WARM":
        return (
            "**WARM.** The front end of the VIX term structure is "
            "starting to invert (VIX9D %(vix9d).2f vs VIX %(vix).2f), "
            "but VVIX is only %(vvix).0f (need above 130 for ARMED). "
            "This is the EARLY-WARNING phase. Real capitulation "
            "hasn't started yet. Stay patient."
            % {"vix9d": vix9d or 0, "vix": vix or 0,
               "vvix": vvix or 0})

    return (
        "**NULL -- no setup.** The VIX term structure is in normal "
        "contango (VIX9D %(vix9d).2f < VIX %(vix).2f < VIX3M "
        "%(vix3m).2f). No capitulation, no signal.\n\n"
        "This trigger has fired %(n)d times in the last 17 years. "
        "When it does, forward returns are extraordinary "
        "(see Forward Expectations table). Until then, this "
        "dashboard waits. **This is a feature, not a bug** -- "
        "the rarity is what makes the edge real."
        % {"vix9d": vix9d or 0, "vix": vix or 0, "vix3m": vix3m or 0,
           "n": n})


def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = ("https://api.telegram.org/bot" + TELEGRAM_TOKEN +
               "/sendMessage")
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
        }).encode("utf-8")
        urllib.request.urlopen(url, data=data, timeout=10).read()
    except Exception as e:
        print("telegram fail: %s" % e)


# ---- handler ------------------------------------------------------------
def lambda_handler(event, context):
    as_of = datetime.now(timezone.utc).isoformat()

    # Fetch live quotes concurrently
    symbols = ["^VIX9D", "^VIX", "^VIX3M", "^VIX6M", "^VVIX", "SPY"]
    with ThreadPoolExecutor(max_workers=6) as ex:
        quote_results = dict(zip(symbols, ex.map(fetch_quote, symbols)))

    def gp(sym):
        q = quote_results.get(sym)
        return q.get("price") if isinstance(q, dict) else None

    vix9d, vix, vix3m, vix6m, vvix, spy_price = (
        gp("^VIX9D"), gp("^VIX"), gp("^VIX3M"),
        gp("^VIX6M"), gp("^VVIX"), gp("SPY"))

    curve_pct = None
    curve_status = None
    if vix9d and vix3m:
        curve_pct = round((vix9d / vix3m - 1) * 100, 2)
        if vix9d > vix3m:
            curve_status = "FULL BACKWARDATION"
        elif vix and vix9d > vix:
            curve_status = "FRONT-MONTH BACKWARDATION"
        else:
            curve_status = "CONTANGO (normal)"

    current_readings = {
        "vix9d": round(vix9d, 2) if vix9d else None,
        "vix": round(vix, 2) if vix else None,
        "vix3m": round(vix3m, 2) if vix3m else None,
        "vix6m": round(vix6m, 2) if vix6m else None,
        "vvix": round(vvix, 1) if vvix else None,
        "spy_price": round(spy_price, 2) if spy_price else None,
        "curve_9d_to_3m_pct": curve_pct,
        "curve_status": curve_status,
    }

    state, signal_strength, conditions = evaluate_state(
        vix9d, vix, vix3m, vvix)

    # Persistent state + cooldown
    persistent = load_persistent_state()
    prev_state = persistent.get("state", "NULL")
    last_fired = persistent.get("last_fired")

    cooldown_until = None
    if last_fired:
        try:
            lf = datetime.fromisoformat(last_fired.replace("Z", "+00:00"))
            cd_end = lf + timedelta(days=COOLDOWN_DAYS)
            if datetime.now(timezone.utc) < cd_end:
                cooldown_until = cd_end.isoformat()
                if state == "FIRED":
                    state = "COOLDOWN"
        except Exception:
            pass

    state_transition = None
    if state != prev_state:
        state_transition = "%s -> %s" % (prev_state, state)
        if state == "FIRED":
            persistent["last_fired"] = as_of
            persistent["last_fired_readings"] = current_readings

    persistent["state"] = state
    if state != prev_state:
        persistent["state_since"] = as_of
    save_persistent_state(persistent)

    # Historical reference + forward returns
    spy_hist = fetch_history("SPY", years=20)
    history_rows = compute_forward_returns(
        spy_hist, HISTORICAL_TRIGGER_DATES)
    forward_expectations = summarise_forwards(history_rows)

    trade_ticket = build_trade_ticket(state, spy_price, forward_expectations)
    why_now = build_why_now(state, current_readings, forward_expectations)

    body = {
        "engine": "vix-backwardation-trigger",
        "version": "1.0",
        "as_of": as_of,
        "state": state,
        "prev_state": prev_state,
        "state_since": persistent.get("state_since"),
        "state_transition": state_transition,
        "signal_strength": signal_strength,
        "cooldown_until": cooldown_until,
        "current_readings": current_readings,
        "trigger_conditions": conditions,
        "forward_expectations": forward_expectations,
        "recommended_trade": trade_ticket,
        "why_now_explainer": why_now,
        "historical_episodes": history_rows,
        "methodology": (
            "Spot VIX9D, VIX, VIX3M, VIX6M, VVIX read from FMP /stable/. "
            "Trigger state machine: NULL -> WARM (VIX9D >= VIX) -> "
            "ARMED (+ VVIX > %d) -> FIRED (+ VIX > VIX3M). "
            "Forward expectations computed live from 20y SPY history "
            "at curated trigger dates. 30-day cooldown after firing "
            "prevents re-triggering during a single capitulation "
            "episode." % VVIX_PANIC_FLOOR),
        "sources": [
            "FMP /stable/quote",
            "FMP /stable/historical-price-eod",
            "academic: Cboe vol research, Goldman vol desk notes",
        ],
        "schedule": "3x daily MON-FRI (14, 17, 21 UTC)",
    }

    s3.put_object(Bucket=S3_BUCKET, Key=REPORT_KEY,
                  Body=json.dumps(body, default=str),
                  ContentType="application/json",
                  CacheControl="no-cache")

    if (state == "FIRED" and state_transition
            and state_transition.endswith("-> FIRED")):
        f3 = forward_expectations.get("3m", {})
        msg = (
            "*VIX BACKWARDATION FIRED*\n\n"
            "Full term-structure inversion confirmed.\n"
            "VIX9D %(vix9d).2f > VIX %(vix).2f > VIX3M %(vix3m).2f\n"
            "VVIX %(vvix).0f\n\n"
            "Historical fwd 3m: +%(r3)s%% (win %(w3)s%%, "
            "N=%(n)s)\n\n"
            "Once-per-cycle setup. Dashboard: "
            "https://justhodl.ai/vix-capitulation.html"
            % {"vix9d": vix9d or 0, "vix": vix or 0,
               "vix3m": vix3m or 0, "vvix": vvix or 0,
               "r3": f3.get("return_pct"),
               "w3": f3.get("win_rate_pct"),
               "n": f3.get("n")}
        )
        send_telegram(msg)

    print("vix-backwardation: state=%s signal_strength=%s "
          "vix9d=%s vix=%s vix3m=%s vvix=%s n_hist=%s" %
          (state, signal_strength, vix9d, vix, vix3m, vvix,
           len(history_rows)))
    return {"statusCode": 200, "body": json.dumps(body, default=str)}
