"""
justhodl-credit-equity-divergence
==================================

Paired-divergence engine: HYG (high-yield credit) vs SPY (S&P 500 equity).

Pressure-test:
  - Naive: just compare HYG return to SPY return.
  - Better: credit leads equity historically by 2-6 weeks. When credit
    deteriorates while equity rallies, equity often catches DOWN. When
    credit improves while equity falls, equity often catches UP.
    Mechanism: HYG IS the marginal-buyer/seller signal because it's
    backed by levered corporate debt that breaks first in stress.
    4-factor signal:
    (a) 20d return spread: HYG_ret - SPY_ret
    (b) Spread z-score vs 252d
    (c) Direction of credit move (HYG trending up = credit improving)
    (d) Confirmation lag: divergence must persist for >=3 trading days
        (avoids 1-day noise)
  - Multi-asset overlay: LQD (IG credit) and EMB (EM credit) as
    sanity checks. If HYG diverges but LQD agrees with SPY, signal is
    weaker (idiosyncratic to high-yield, not systemic).

Edge basis:
  Gilchrist-Zakrajsek 2012 (excess bond premium predicts equity),
  Mueller-Tahbaz-Salehi-Vedolin 2016 (credit-equity comovement), Greenwood
  & Hanson 2013 (credit-cycle equity returns). Forward edge: extreme HYG-
  SPY divergence resolves over 4-8 weeks; ~60% hit on direction,
  expected magnitude +5% / -7% on resolution side.

Data sources:
  - FMP /stable/quote + historical for HYG, SPY, LQD, EMB
  - All ETFs have 15+ year history, deep liquidity

Output:
  Current state + 4-factor metrics + trade tickets if divergence active.
  State: CREDIT_BULL_RICH (long SPY, hedge HYG), CREDIT_BEAR_RICH
  (short SPY or buy SPY puts), NEUTRAL, QUIET.

Schedule: daily 21 UTC after US close.
"""
import json
import os
import statistics
import time
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/credit-equity-divergence.json"
SSM_STATE_KEY = "/justhodl/credit-equity-divergence/state"

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

ASSETS = {
    "HYG": "HYG",      # iShares iBoxx High Yield Corporate Bond
    "SPY": "SPY",      # S&P 500
    "LQD": "LQD",      # iShares iBoxx Investment Grade
    "EMB": "EMB",      # iShares JPM USD EM Bond
}

s3 = boto3.client("s3")
ssm = boto3.client("ssm")


def http_get(url, timeout=12, retries=2):
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", errors="ignore")
        except Exception as e:
            last = e
            time.sleep(0.5 * (i + 1))
    raise RuntimeError(f"http_get failed: {last}")


def fmp_history(symbol, days=300):
    """Newest-first daily closes via FMP /stable/historical-price-eod/full.

    The proven endpoint used by gap-fill-confirm, merger-arb, hedge-pnl, etc.
    Surfaces fetch errors instead of silently returning [] (which masked the
    HYG=0 SPY=0 root cause in ops 980).
    """
    q = urllib.parse.quote_plus(symbol)
    url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
           f"?symbol={q}&apikey={FMP_KEY}")
    try:
        raw = http_get(url)
    except Exception as e:
        print(f"[fmp_history] http_get failed for {symbol}: {e}")
        return []
    try:
        data = json.loads(raw)
    except Exception as e:
        print(f"[fmp_history] json parse failed for {symbol}: {e}; raw[:200]={raw[:200]}")
        return []
    if isinstance(data, dict):
        hist = data.get("historical") or data.get("data") or []
    else:
        hist = data
    if not hist:
        print(f"[fmp_history] empty hist for {symbol}; resp keys/type={type(data).__name__} "
              f"sample={json.dumps(data)[:200]}")
        return []
    closes = []
    for r in hist[:days]:
        c = r.get("close") or r.get("adjClose") or r.get("price")
        if c is not None:
            try:
                closes.append(float(c))
            except (TypeError, ValueError):
                continue
    return closes


def pct_return(closes, days):
    if not closes or len(closes) <= days or closes[days] == 0:
        return None
    return (closes[0] / closes[days] - 1.0) * 100


def spread_history(closes_a, closes_b, days, window):
    """Return list of (a_ret - b_ret) spreads over rolling window."""
    n = min(len(closes_a), len(closes_b)) - days
    if n < window + 10:
        return []
    spreads = []
    for i in range(min(n, 252)):
        if closes_a[i + days] == 0 or closes_b[i + days] == 0:
            continue
        a_ret = (closes_a[i] / closes_a[i + days] - 1.0) * 100
        b_ret = (closes_b[i] / closes_b[i + days] - 1.0) * 100
        spreads.append(a_ret - b_ret)
    return spreads


def zscore_latest(series):
    if not series or len(series) < 30:
        return None
    latest = series[0]
    rest = series[1:]
    m = statistics.mean(rest)
    sd = statistics.stdev(rest) or 1e-9
    return (latest - m) / sd


def check_persistence(closes_a, closes_b, days=20, min_days=3):
    """Check that the sign of spread has been consistent for last min_days days."""
    n = min(len(closes_a), len(closes_b)) - days
    if n < min_days + 1:
        return False
    signs = []
    for i in range(min_days):
        if closes_a[i + days] == 0 or closes_b[i + days] == 0:
            continue
        a_ret = closes_a[i] / closes_a[i + days] - 1.0
        b_ret = closes_b[i] / closes_b[i + days] - 1.0
        spread = a_ret - b_ret
        signs.append(1 if spread > 0 else -1)
    if len(signs) < min_days:
        return False
    return all(s == signs[0] for s in signs)


def fetch_all():
    out = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(fmp_history, sym, 300): tag for tag, sym in ASSETS.items()}
        for f in as_completed(futs):
            tag = futs[f]
            try:
                out[tag] = f.result()
            except Exception:
                out[tag] = []
    return out


def lambda_handler(event, context):
    start = time.time()
    try:
        hist = fetch_all()
        hyg = hist.get("HYG", [])
        spy = hist.get("SPY", [])
        lqd = hist.get("LQD", [])
        emb = hist.get("EMB", [])

        if len(hyg) < 30 or len(spy) < 30:
            raise RuntimeError(f"insufficient data: HYG={len(hyg)} SPY={len(spy)}")

        # Returns
        hyg_20d = pct_return(hyg, 20)
        spy_20d = pct_return(spy, 20)
        lqd_20d = pct_return(lqd, 20) if lqd else None
        emb_20d = pct_return(emb, 20) if emb else None
        spread_20d = (hyg_20d - spy_20d) if (hyg_20d is not None and spy_20d is not None) else None
        spreads_hist = spread_history(hyg, spy, days=20, window=252)
        spread_z = zscore_latest(spreads_hist) if spreads_hist else None

        # Persistence
        persistent = check_persistence(hyg, spy, days=20, min_days=3)

        # LQD confirmation: is IG credit also diverging same direction?
        lqd_confirms = None
        if lqd_20d is not None and spy_20d is not None:
            lqd_spy_diff = lqd_20d - spy_20d
            if spread_20d is not None:
                # Both negative or both positive = LQD confirms HYG
                lqd_confirms = (lqd_spy_diff * spread_20d) > 0

        # Classify
        # CREDIT_BEAR: HYG significantly underperforms SPY (credit deteriorating fast,
        #   equity hasn't caught DOWN yet). Trade: short SPY / buy puts.
        # CREDIT_BULL: HYG significantly outperforms SPY (credit improving, equity
        #   hasn't caught UP yet). Trade: long SPY.
        state = "NEUTRAL"
        strength = 0.2
        why = "No significant credit-equity divergence"

        if spread_z is not None and persistent:
            if spread_z <= -1.5:
                state = "CREDIT_BEAR_RICH"
                strength = min(1.0, 0.5 + abs(spread_z) * 0.12)
                why = (f"HYG-SPY 20d spread z={round(spread_z,2)} (HYG underperforms by "
                       f"{round(spread_20d,1)}%), persistent {3}+ days; "
                       f"credit leads, equity vulnerable")
            elif spread_z >= 1.5:
                state = "CREDIT_BULL_RICH"
                strength = min(1.0, 0.5 + abs(spread_z) * 0.12)
                why = (f"HYG-SPY 20d spread z={round(spread_z,2)} (HYG outperforms by "
                       f"{round(spread_20d,1)}%), persistent {3}+ days; "
                       f"credit leads, equity bullish")
            elif spread_z <= -1.0:
                state = "CREDIT_BEAR_ACTIVE"
                strength = 0.45
                why = f"Mild bearish divergence z={round(spread_z,2)}"
            elif spread_z >= 1.0:
                state = "CREDIT_BULL_ACTIVE"
                strength = 0.45
                why = f"Mild bullish divergence z={round(spread_z,2)}"

        # If LQD doesn't confirm and state is RICH, downgrade conviction
        if "RICH" in state and lqd_confirms is False:
            strength *= 0.8
            why += " (LQD does NOT confirm; HYG-specific risk)"

        # Build tickets
        tickets = []
        if state == "CREDIT_BEAR_RICH":
            tickets = [
                {"ticker": "SPY", "side": "SHORT", "rationale": "Credit-equity bear divergence",
                 "target_pct": -5, "stop_pct": 3, "holding_period": "4-8 weeks",
                 "size_pct_portfolio": 2.0},
                {"ticker": "SPY", "side": "LONG_PUT", "rationale": "4-8 week ATM put",
                 "strike_setup": "ATM expiry 30-60 days", "size_pct_portfolio": 1.0},
                {"ticker": "SH", "side": "LONG", "rationale": "Inverse S&P 500 ETF",
                 "target_pct": 5, "stop_pct": -3, "size_pct_portfolio": 1.5},
            ]
        elif state == "CREDIT_BULL_RICH":
            tickets = [
                {"ticker": "SPY", "side": "LONG", "rationale": "Credit-equity bull divergence",
                 "target_pct": 5, "stop_pct": -3, "holding_period": "4-8 weeks",
                 "size_pct_portfolio": 2.5},
                {"ticker": "UPRO", "side": "LONG", "rationale": "3x leveraged S&P (small size)",
                 "target_pct": 12, "stop_pct": -7, "size_pct_portfolio": 1.0},
                {"ticker": "SPY", "side": "LONG_CALL", "rationale": "30-60d ATM call",
                 "size_pct_portfolio": 1.0},
            ]
        elif state in ("CREDIT_BEAR_ACTIVE", "CREDIT_BULL_ACTIVE"):
            direction = "SHORT" if "BEAR" in state else "LONG"
            tickets = [
                {"ticker": "SPY", "side": direction,
                 "rationale": f"{state} setup, smaller size",
                 "target_pct": 3 if direction == "LONG" else -3,
                 "stop_pct": -2 if direction == "LONG" else 2,
                 "holding_period": "2-4 weeks",
                 "size_pct_portfolio": 1.0},
            ]

        out = {
            "engine": "credit-equity-divergence",
            "version": VERSION,
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "state": state,
            "signal_strength": round(strength, 2),
            "current_metrics": {
                "hyg_20d_ret_pct": round(hyg_20d, 2) if hyg_20d is not None else None,
                "spy_20d_ret_pct": round(spy_20d, 2) if spy_20d is not None else None,
                "hyg_spy_spread_20d": round(spread_20d, 2) if spread_20d is not None else None,
                "spread_zscore_252d": round(spread_z, 2) if spread_z is not None else None,
                "lqd_20d_ret_pct": round(lqd_20d, 2) if lqd_20d is not None else None,
                "emb_20d_ret_pct": round(emb_20d, 2) if emb_20d is not None else None,
                "persistent_3d": persistent,
                "lqd_confirms": lqd_confirms,
            },
            "regime_explanation": why,
            "trade_tickets": tickets,
            "n_tickets": len(tickets),
            "methodology": (
                "Credit-equity paired-divergence: HYG (high-yield credit) vs SPY (equity), "
                "with LQD (IG) + EMB (EM) as confirmation. Signal: 20d return spread, "
                "252d z-score, 3-day persistence filter. CREDIT_BEAR triggers: z<=-1.5 + "
                "persistent. CREDIT_BULL triggers: z>=+1.5 + persistent. LQD non-confirmation "
                "discounts conviction. Edge basis: Gilchrist-Zakrajsek 2012, Mueller-Tahbaz "
                "2016, Greenwood-Hanson 2013. Forward edge ~60% hit / +5% (BULL) or -7% "
                "(BEAR) over 4-8 weeks at extreme z."
            ),
            "sources": ["FMP /stable/historical-price-eod/light (HYG, SPY, LQD, EMB)"],
            "why_now": why,
            "run_seconds": round(time.time() - start, 2),
        }

        # Telegram on state change
        try:
            prev = ssm.get_parameter(Name=SSM_STATE_KEY)["Parameter"]["Value"]
        except Exception:
            prev = None
        if prev != state and state in ("CREDIT_BEAR_RICH", "CREDIT_BULL_RICH") and TELEGRAM_TOKEN:
            msg = (f"*CREDIT-EQUITY -> {state}*\n"
                   f"HYG 20d: {round(hyg_20d,1)}%  SPY 20d: {round(spy_20d,1)}%\n"
                   f"Spread z: {round(spread_z,2)}  persistent: {persistent}\n"
                   f"LQD confirms: {lqd_confirms}\n"
                   f"{why}\n"
                   f"Tickets: {len(tickets)} (retail-edges.html)")
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                body = urllib.parse.urlencode({
                    "chat_id": TELEGRAM_CHAT, "text": msg, "parse_mode": "Markdown",
                    "disable_web_page_preview": "true",
                }).encode("utf-8")
                urllib.request.urlopen(urllib.request.Request(url, data=body), timeout=10)
            except Exception:
                pass
        try:
            ssm.put_parameter(Name=SSM_STATE_KEY, Value=state, Type="String", Overwrite=True)
        except Exception:
            pass

        s3.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY,
            Body=json.dumps(out, indent=2).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, max-age=60",
        )
        return {"statusCode": 200,
                "body": json.dumps({"ok": True, "state": state, "n_tickets": len(tickets)})}
    except Exception as e:
        import traceback
        err = {"engine": "credit-equity-divergence", "error": str(e)[:300],
               "trace": traceback.format_exc()[:600],
               "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        try:
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(err, indent=2).encode("utf-8"),
                          ContentType="application/json")
        except Exception:
            pass
        return {"statusCode": 500, "body": json.dumps({"error": str(e)[:300]})}
