"""
PHASE N — Historical backtest: would momentum-breakout have caught the pumps
EARLY (before they went parabolic)?

Strategy:
  For each pump-list ticker, pull 180 days of price/volume history.
  Walk through time at +60-day intervals (so 180/60 = 3 historical "as-if" runs).
  At each point in time, simulate what the momentum scoring would have produced
  using only the data available at that point.
  Identify when the score first crosses 60 (TIER_B) and 75 (TIER_A).
  
  Then check: at that point in time, what % gain was already in the stock?
  If the score crossed 60 at +20% gain, that's an early catch.
  If it only crossed at +200% gain, that's late.
"""
import json, time, urllib.request, os
import boto3

REGION = "us-east-1"
S3 = boto3.client("s3", region_name=REGION)
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


PUMP_LIST = [
    ("AXTI",  464.48), ("LWLG",  407.67), ("AAOI",  353.13),
    ("AEHR",  276.84), ("ICHR",  137.88), ("MRVL",  129.81),
    ("INTC",  122.41), ("LITE",  116.25), ("CRDO",  101.23),
]


def fetch_history(symbol, days=270):
    url = f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={symbol}&apikey={FMP_KEY}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Backtest"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
            return sorted([
                {"date": x.get("date"), "close": float(x.get("close") or 0), "volume": float(x.get("volume") or 0)}
                for x in d[:days] if x.get("close") and x.get("date")
            ], key=lambda x: x["date"])
    except Exception as e:
        print(f"history {symbol}: {e}")
        return None


def simulate_score_at(history_window, spy_window):
    """Compute what the momentum score would have been at the END of this window."""
    if not history_window or len(history_window) < 30:
        return None
    closes = [h["close"] for h in history_window]
    volumes = [h["volume"] for h in history_window]
    n = len(closes)
    today_close = closes[-1]
    today_vol = volumes[-1]

    avg_dollar_vol = sum(c * v for c, v in zip(closes[-20:], volumes[-20:])) / min(20, n)
    if avg_dollar_vol < 5000000:
        return None

    ret_5d = (today_close / closes[-6] - 1) * 100 if n >= 6 else None
    ret_10d = (today_close / closes[-11] - 1) * 100 if n >= 11 else None
    ret_20d = (today_close / closes[-21] - 1) * 100 if n >= 21 else None
    ret_60d = (today_close / closes[-61] - 1) * 100 if n >= 61 else None

    max_20d = max(closes[-20:])
    max_60d = max(closes[-60:]) if n >= 60 else max_20d
    pct_from_20d = (today_close / max_20d - 1) * 100
    pct_from_60d = (today_close / max_60d - 1) * 100

    avg_vol_20 = sum(volumes[-20:]) / min(20, n)
    vol_ratio = today_vol / avg_vol_20 if avg_vol_20 > 0 else 0

    rs_20d = None
    rs_60d = None
    if spy_window and len(spy_window) >= 61 and len(history_window) >= 61:
        spy_close = spy_window[-1]["close"]
        spy_ret_20 = (spy_close / spy_window[-21]["close"] - 1) * 100
        spy_ret_60 = (spy_close / spy_window[-61]["close"] - 1) * 100
        rs_20d = ret_20d - spy_ret_20 if ret_20d is not None else None
        rs_60d = ret_60d - spy_ret_60 if ret_60d is not None else None

    is_parabolic = (ret_20d or 0) > 50 or (ret_10d or 0) > 35

    last_20 = list(zip(closes[-20:], volumes[-20:]))
    accum_days = sum(1 for i in range(1, len(last_20))
                       if last_20[i][1] > avg_vol_20 and last_20[i][0] > last_20[i-1][0])
    accum_pct = accum_days / 19 * 100

    score = 0
    if ret_60d is not None:
        score += min(max(ret_60d / 30, 0), 1) * 10
    if ret_20d is not None and not is_parabolic:
        if ret_20d > 5:
            score += min(ret_20d / 25, 1) * 15
    is_at_20d_high = abs(pct_from_20d) < 0.5
    is_at_60d_high = abs(pct_from_60d) < 0.5
    if is_at_60d_high:
        score += 20
    elif is_at_20d_high:
        score += 12
    elif pct_from_60d > -5:
        score += 8
    if vol_ratio > 2.5:
        score += 15
    elif vol_ratio > 1.8:
        score += 10
    elif vol_ratio > 1.3:
        score += 5
    if accum_pct > 60:
        score += 15
    elif accum_pct > 40:
        score += 8
    if rs_20d is not None and rs_20d > 5:
        score += min(rs_20d / 15, 1) * 15
    if rs_60d is not None and rs_60d > 10:
        score += 5
    if is_parabolic:
        score = score * 0.5
    return min(score, 100)


def main():
    section("0) Fetch SPY 270d history (single fetch)")
    spy = fetch_history("SPY", days=270)
    if not spy:
        log("  ❌ SPY history failed")
        return
    log(f"  ✓ SPY: {len(spy)} days, range {spy[0]['date']} → {spy[-1]['date']}")

    section("1) Run as-if backtest for each pump-list name")
    log("  At each evaluation date, only data ≤ that date is used.")
    log("  We track: when did score first cross 60 and 75? And what was % gain at that point?")
    log("")

    for sym, total_gain in PUMP_LIST:
        log(f"  ── {sym} (today's total gain: +{total_gain:.0f}%) ──")
        h = fetch_history(sym, days=270)
        if not h or len(h) < 90:
            log(f"    no history available")
            log("")
            continue

        # Walk through history starting at day 70 (need 60d for window)
        # Evaluate at each day
        first_60 = None
        first_75 = None
        baseline = h[0]["close"]
        peak_to_today = h[-1]["close"]
        full_period_gain = (peak_to_today / baseline - 1) * 100

        # Find date when score first crossed 60 and 75
        for i in range(70, len(h)):
            window = h[:i+1]
            spy_window = [s for s in spy if s["date"] <= window[-1]["date"]]
            score = simulate_score_at(window, spy_window)
            if score is None:
                continue
            current_close = window[-1]["close"]
            gain_so_far = (current_close / baseline - 1) * 100
            future_gain = (peak_to_today / current_close - 1) * 100  # gain from THIS point forward

            if score >= 60 and first_60 is None:
                first_60 = (window[-1]["date"], score, gain_so_far, future_gain)
            if score >= 75 and first_75 is None:
                first_75 = (window[-1]["date"], score, gain_so_far, future_gain)

        if first_60:
            d, sc, sf, fg = first_60
            log(f"    First crossed 60: {d}  score={sc:.0f}  gain_so_far=+{sf:.0f}%  remaining_gain=+{fg:.0f}%")
        else:
            log(f"    Never crossed 60")
        if first_75:
            d, sc, sf, fg = first_75
            log(f"    First crossed 75: {d}  score={sc:.0f}  gain_so_far=+{sf:.0f}%  remaining_gain=+{fg:.0f}%")
        else:
            log(f"    Never crossed 75")
        log("")

    section("2) Conclusion — early-detection capability assessment")
    log("  A 'good catch' = score crosses 60 BEFORE more than 50% of the gain has happened.")
    log("  A 'late catch' = score crosses 60 AFTER more than 50% of gain.")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_n_historical_backtest.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
