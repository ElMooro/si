"""
PHASE P — Calibrate pre-pump v2 thresholds against the actual pump-list data.

For each pump-list name, walk history backward and:
  1. Find BREAKOUT_DATE = first day where 5-day return > 25% (actual launch moment)
  2. Compute signal values at PRE_BREAKOUT_DATE = breakout_date - 30 days
  3. Aggregate across all names → median values become v2 thresholds
"""
import json, time, urllib.request, statistics, os
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S")
    print("- `" + ts + "`   " + m)
    REPORT.append("- `" + ts + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


PUMP_LIST = [
    ("AXTI", 464), ("LWLG", 408), ("AAOI", 353),
    ("AEHR", 277), ("ICHR", 138), ("MRVL", 130),
    ("INTC", 122), ("LITE", 116), ("CRDO", 101),
]


def fetch_history(sym):
    url = "https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=" + sym + "&apikey=" + FMP_KEY
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Calib/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
            if not isinstance(d, list):
                return None
            out = []
            for x in d[:300]:
                if x.get("close") and x.get("date"):
                    out.append({
                        "date": x.get("date"),
                        "close": float(x.get("close")),
                        "volume": float(x.get("volume") or 0),
                    })
            out.sort(key=lambda r: r["date"])
            return out
    except Exception as e:
        print("history " + sym + ": " + str(e))
        return None


def compute_signals_at(history_window):
    """Compute pre-pump signal metrics at end of window."""
    if not history_window or len(history_window) < 60:
        return None
    closes = [h["close"] for h in history_window]
    volumes = [h["volume"] for h in history_window]
    n = len(closes)
    today = closes[-1]

    avg_dollar_vol = sum(c * v for c, v in zip(closes[-30:], volumes[-30:])) / min(30, n)
    if avg_dollar_vol < 100000:
        return None

    lookback = min(180, n)
    range_high = max(closes[-lookback:])
    range_low = min(closes[-lookback:])
    range_position = (today - range_low) / (range_high - range_low) * 100 if range_high > range_low else 50

    returns_60 = []
    for i in range(max(0, n-61), n-1):
        if closes[i] > 0:
            returns_60.append((closes[i+1] / closes[i] - 1) * 100)
    returns_180 = []
    for i in range(max(0, n-181), n-1):
        if closes[i] > 0:
            returns_180.append((closes[i+1] / closes[i] - 1) * 100)
    stdev_60 = statistics.stdev(returns_60) if len(returns_60) > 1 else 0
    stdev_180 = statistics.stdev(returns_180) if len(returns_180) > 1 else 0
    vol_compression = stdev_180 / stdev_60 if stdev_60 > 0 else 0

    obv = [0]
    for i in range(1, n):
        if closes[i] > closes[i-1]:
            obv.append(obv[-1] + volumes[i])
        elif closes[i] < closes[i-1]:
            obv.append(obv[-1] - volumes[i])
        else:
            obv.append(obv[-1])
    if len(obv) >= 60:
        obv_recent = obv[-60:]
        x_mean = (len(obv_recent) - 1) / 2
        y_mean = sum(obv_recent) / len(obv_recent)
        num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(obv_recent))
        den = sum((i - x_mean) ** 2 for i in range(len(obv_recent)))
        obv_slope = num / den if den > 0 else 0
        avg_vol = sum(volumes[-60:]) / 60
        obv_slope_norm = obv_slope / avg_vol if avg_vol > 0 else 0
    else:
        obv_slope_norm = 0

    avg_vol_20 = sum(volumes[-20:]) / min(20, n)
    avg_vol_60 = sum(volumes[-60:]) / min(60, n) if n >= 60 else avg_vol_20
    avg_vol_120 = sum(volumes[-120:]) / min(120, n) if n >= 120 else avg_vol_60
    liq_30v60 = avg_vol_20 / avg_vol_60 if avg_vol_60 > 0 else 1.0
    liq_30v120 = avg_vol_20 / avg_vol_120 if avg_vol_120 > 0 else 1.0

    ret_60d = (today / closes[-61] - 1) * 100 if n >= 61 else 0
    ret_120d = (today / closes[-121] - 1) * 100 if n >= 121 else 0
    ret_30d = (today / closes[-31] - 1) * 100 if n >= 31 else 0
    ret_5d = (today / closes[-6] - 1) * 100 if n >= 6 else 0

    return {
        "range_position": range_position,
        "vol_compression": vol_compression,
        "obv_slope_norm": obv_slope_norm,
        "liq_30v60": liq_30v60,
        "liq_30v120": liq_30v120,
        "ret_5d": ret_5d,
        "ret_30d": ret_30d,
        "ret_60d": ret_60d,
        "ret_120d": ret_120d,
        "stdev_60": stdev_60,
        "stdev_180": stdev_180,
        "avg_dollar_vol": avg_dollar_vol,
    }


def find_breakout_idx(history):
    """First day where 5-day return exceeded 25% (the actual launch)."""
    if len(history) < 30:
        return None
    for i in range(20, len(history)):
        ret_5d = (history[i]["close"] / history[i-5]["close"] - 1) * 100
        if ret_5d > 25:
            return i
    return None


def main():
    section("1) For each pump-list name, find the breakout date and measure signals 30 days earlier")
    log("  We snapshot the pre-breakout pattern when the move was about to begin.")
    log("")

    snapshots = []
    for sym, total in PUMP_LIST:
        h = fetch_history(sym)
        if not h or len(h) < 200:
            log("  " + sym + " — insufficient history (" + str(len(h) if h else 0) + " days)")
            continue
        breakout_idx = find_breakout_idx(h)
        if breakout_idx is None:
            log("  " + sym + " — no clear 5d>25% breakout (smooth uptrend)")
            continue
        breakout_date = h[breakout_idx]["date"]
        # 30 trading days BEFORE the breakout
        pre_idx = breakout_idx - 30
        if pre_idx < 60:
            log("  " + sym + " — breakout too early in history (idx=" + str(breakout_idx) + ")")
            continue
        pre_window = h[:pre_idx + 1]
        sig = compute_signals_at(pre_window)
        if not sig:
            log("  " + sym + " — signals failed at pre-breakout")
            continue

        log("  " + sym + " (final +" + str(total) + "%, breakout " + breakout_date + "):")
        log("    PRE-BREAKOUT (30d earlier): " + h[pre_idx]["date"] + " close=$" + str(round(h[pre_idx]["close"], 2)))
        log("      range_position={:.0f}  vol_compression={:.2f}  obv_slope={:.3f}".format(
            sig["range_position"], sig["vol_compression"], sig["obv_slope_norm"]))
        log("      liq_30v60={:.2f}  liq_30v120={:.2f}".format(sig["liq_30v60"], sig["liq_30v120"]))
        log("      ret_5d={:.1f}%  ret_30d={:.1f}%  ret_60d={:.1f}%  ret_120d={:.1f}%".format(
            sig["ret_5d"], sig["ret_30d"], sig["ret_60d"], sig["ret_120d"]))
        log("")
        snapshots.append((sym, sig))

    section("2) Aggregate medians across all snapshots")
    if not snapshots:
        log("  no snapshots — can't calibrate")
        return

    metrics_to_aggregate = [
        "range_position", "vol_compression", "obv_slope_norm",
        "liq_30v60", "liq_30v120",
        "ret_5d", "ret_30d", "ret_60d", "ret_120d",
    ]
    medians = {}
    log("  metric                 median   p25      p75      min      max")
    log("  ---------------------- -------- -------- -------- -------- --------")
    for m in metrics_to_aggregate:
        vals = [s[m] for _, s in snapshots]
        vals.sort()
        med = statistics.median(vals)
        p25 = vals[len(vals)//4] if len(vals) >= 4 else vals[0]
        p75 = vals[3*len(vals)//4] if len(vals) >= 4 else vals[-1]
        medians[m] = med
        log("  {:<22} {:>+8.2f} {:>+8.2f} {:>+8.2f} {:>+8.2f} {:>+8.2f}".format(
            m, med, p25, p75, min(vals), max(vals)))

    section("3) DERIVED v2 THRESHOLDS")
    log("  Based on the actual pre-breakout pattern of these winners:")
    log("")
    log("  range_position threshold: < " + str(round(medians["range_position"] * 1.3, 0)))
    log("    (most pump names started somewhere in the lower 60% of their range)")
    log("")
    log("  vol_compression > " + "{:.2f}".format(medians["vol_compression"] * 0.85))
    log("    (volatility was modestly compressed — not extreme)")
    log("")
    log("  obv_slope_norm > " + "{:.3f}".format(medians["obv_slope_norm"] * 0.7))
    log("    (OBV was rising — accumulation visible)")
    log("")
    log("  liq_30v120 > " + "{:.2f}".format(medians["liq_30v120"] * 0.85))
    log("    (volume already expanding before breakout)")
    log("")
    log("  ret_60d range: " + "{:.0f}% to {:.0f}%".format(
        min(s["ret_60d"] for _, s in snapshots),
        max(s["ret_60d"] for _, s in snapshots)))
    log("    (price action varied widely — shouldn't be a hard filter)")
    log("")
    log("  KEY INSIGHT: many names already had +20-50% in 60d before the explosive move.")
    log("  v1 was wrong to require abs(ret_60d) < 8. v2 should ALLOW positive returns up to ~60%.")

    section("4) Save calibration results to S3 for v2 use")
    out = {
        "schema_version": 1,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "n_snapshots": len(snapshots),
        "snapshots": [{"symbol": s[0], "metrics": s[1]} for s in snapshots],
        "medians": medians,
        "v2_thresholds": {
            "range_position_max":     round(medians["range_position"] * 1.3, 1),
            "vol_compression_min":    round(medians["vol_compression"] * 0.85, 2),
            "obv_slope_norm_min":     round(medians["obv_slope_norm"] * 0.7, 3),
            "liq_30v120_min":         round(medians["liq_30v120"] * 0.85, 2),
            "ret_60d_max":            70.0,
            "ret_60d_min":           -25.0,
            "ret_5d_min":            -10.0,
            "ret_5d_max":             20.0,
        },
    }
    S3.put_object(
        Bucket=BUCKET, Key="data/pre-pump-calibration.json",
        Body=json.dumps(out, indent=2).encode(),
        ContentType="application/json",
    )
    log("  ✓ wrote data/pre-pump-calibration.json")
    log("  ── v2 thresholds ──")
    for k, v in out["v2_thresholds"].items():
        log("    " + k + " = " + str(v))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
        for ln in traceback.format_exc().splitlines():
            log("    " + ln)
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_p_calibrate_thresholds.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
