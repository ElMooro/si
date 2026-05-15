"""justhodl-vol-surface — Bloomberg OPV / SKEW equivalent (Yahoo Finance edition).

For 6 anchor symbols (SPY, QQQ, IWM, GLD, TLT, HYG), snapshots the implied-vol
surface across strike × expiration and computes:

  • ATM term structure (7d / 30d / 60d)
  • 25-delta call IV vs 25-delta put IV (Bloomberg SKEW)
  • Risk reversal (RR25) = put_IV − call_IV    (>0 = put skew = bearish)
  • Butterfly (BF25)     = (put_IV + call_IV)/2 − ATM   (smile curvature)
  • Term slope (front vs back, per-year basis)
  • Surface regime (TERM_INVERTED / RICH_PUT_SKEW / RICH_CALL_SKEW /
                    FLAT_SMILE / NORMAL)
  • Risk-neutral implied PDF (Breeden-Litzenberger d²C/dK² × e^{rT})
    → quartiles p10/p25/p50/p75/p90 of implied terminal-spot distribution
  • Rolling 30-day percentile of RR25 and ATM-IV per underlying

Data source: query1.finance.yahoo.com/v7/finance/options/{ticker}
  Free; returns full chain with implied_volatility per contract.
  We compute Black-Scholes delta locally (Yahoo doesn't supply it).

Schedule: cron(20 14-20 ? * MON-FRI *)  hourly during US market hours.

Outputs:
  data/vol-surface.json           — current snapshot
  data/vol-surface-history.json   — last 168 light snapshots (~1 week)

Telegram alerts on:
  • Regime flip
  • RR25 percentile ≥ 90 (extreme put skew, late-cycle defensive)
  • RR25 percentile ≤ 5  (extreme call skew, melt-up froth)
  • Term-structure inversion (slope < −0.05/yr, event/panic)
"""
import json, os, time, math
from datetime import datetime, timezone, timedelta
from urllib import request, parse, error
import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/vol-surface.json"
S3_HISTORY_KEY = "data/vol-surface-history.json"
HISTORY_MAX = 168

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
FRED_KEY = os.environ.get("FRED_API_KEY", "")

UNDERLYINGS = ["SPY", "QQQ", "IWM", "GLD", "TLT", "HYG"]
TARGET_DTES = [7, 30, 60]
USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")

s3 = boto3.client("s3", region_name="us-east-1")


# ────────────────────────── HTTP helpers ───────────────────────────────
def _get_json(url, timeout=15, retries=3):
    last_err = None
    for i in range(retries):
        try:
            req = request.Request(url, headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.9",
            })
            with request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError) as e:
            last_err = e
            time.sleep(0.7 * (i + 1))
    raise last_err if last_err else RuntimeError("http")


def fred_latest(series_id, lookback=5):
    if not FRED_KEY:
        return None
    try:
        url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
               f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={lookback}")
        j = _get_json(url)
        for o in j.get("observations", []):
            v = o.get("value")
            if v not in (None, ".", ""):
                return float(v)
    except Exception as e:
        print(f"[fred] {series_id}: {e}")
    return None


# ────────────────────────── Yahoo options ──────────────────────────────
def yahoo_chain(ticker, expiration_unix=None):
    """Return the Yahoo Finance option chain for one (or default-front) expiration."""
    url = f"https://query1.finance.yahoo.com/v7/finance/options/{ticker}"
    if expiration_unix is not None:
        url += f"?date={int(expiration_unix)}"
    return _get_json(url)


def fetch_underlying_data(ticker):
    """Fetch base chain (front exp) to discover spot + all expirations."""
    j = yahoo_chain(ticker)
    res = (j.get("optionChain") or {}).get("result") or []
    if not res:
        return None
    r = res[0]
    quote = r.get("quote") or {}
    spot = quote.get("regularMarketPrice")
    exps_unix = r.get("expirationDates") or []
    return {
        "spot": spot,
        "expirations_unix": exps_unix,
        "first_chain": r,
    }


def pick_target_expirations(exps_unix, target_dtes):
    """For each target DTE, pick the nearest available unix expiration."""
    today = datetime.utcnow().date()
    picks = []
    seen = set()
    for tgt in target_dtes:
        target_date = today + timedelta(days=tgt)
        best, best_dist, best_iso = None, 1e9, None
        for u in exps_unix:
            ed = datetime.fromtimestamp(u, tz=timezone.utc).date()
            d = abs((ed - target_date).days)
            if d < best_dist:
                best_dist, best, best_iso = d, u, ed.isoformat()
        if best and best not in seen:
            seen.add(best)
            picks.append((best, best_iso))
    return picks


# ────────────────────────── BS delta ──────────────────────────────────
def _norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_delta(S, K, T, sigma, r, contract_type):
    """Black-Scholes delta. Returns None on invalid inputs."""
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return None
    try:
        sqrtT = math.sqrt(T)
        d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
        if contract_type == "call":
            return _norm_cdf(d1)
        return _norm_cdf(d1) - 1.0
    except Exception:
        return None


# ────────────────────────── Surface math ──────────────────────────────
def extract_rows(side_arr, S, T, r, contract_type):
    """Normalize one side (calls/puts) into [{strike, iv, delta, mid}]."""
    out = []
    for c in side_arr or []:
        K = c.get("strike")
        iv = c.get("impliedVolatility")
        bid = c.get("bid") or 0
        ask = c.get("ask") or 0
        last = c.get("lastPrice") or 0
        mid = ((bid + ask) / 2) if bid > 0 and ask > 0 else (last if last > 0 else None)
        if K is None or iv is None or iv <= 0 or iv > 5:
            continue
        d = bs_delta(S, K, T, iv, r, contract_type)
        out.append({"strike": K, "iv": iv, "delta": d, "mid": mid})
    return out


def find_25delta(side, target_delta):
    best, best_dist = None, 1e9
    for row in side:
        if row["delta"] is None:
            continue
        d = abs(row["delta"] - target_delta)
        if d < best_dist:
            best_dist, best = d, row
    return best


def atm_iv_interpolated(calls, puts, spot):
    """Interpolate IV at K=spot using both sides."""
    rows = sorted([*calls, *puts], key=lambda r: r["strike"])
    if not rows:
        return None
    below = [r for r in rows if r["strike"] <= spot]
    above = [r for r in rows if r["strike"] >= spot]
    if not below or not above:
        nearest = min(rows, key=lambda r: abs(r["strike"] - spot))
        return nearest["iv"]
    lo, hi = below[-1], above[0]
    if hi["strike"] == lo["strike"]:
        return lo["iv"]
    w = (spot - lo["strike"]) / (hi["strike"] - lo["strike"])
    return lo["iv"] * (1 - w) + hi["iv"] * w


def compute_pdf(calls, spot, T_years, r):
    """Breeden-Litzenberger d²C/dK² × e^{rT}; quartiles of implied terminal dist."""
    pts = sorted([c for c in calls if c["mid"] and c["mid"] > 0],
                  key=lambda c: c["strike"])
    if len(pts) < 5:
        return None
    grid = []
    for i in range(1, len(pts) - 1):
        Kl, K0, Kr = pts[i - 1]["strike"], pts[i]["strike"], pts[i + 1]["strike"]
        Cl, C0, Cr = pts[i - 1]["mid"], pts[i]["mid"], pts[i + 1]["mid"]
        if Kr - Kl <= 0:
            continue
        # uneven-grid 2nd derivative
        d2 = 2 * (Cl * (Kr - K0) - C0 * (Kr - Kl) + Cr * (K0 - Kl)) / (
            (Kr - K0) * (K0 - Kl) * (Kr - Kl))
        density = max(0.0, d2 * math.exp(r * T_years))
        grid.append({"K": K0, "p": density})
    if not grid:
        return None
    # trapezoid normalize
    total = 0.0
    for i in range(1, len(grid)):
        total += 0.5 * (grid[i - 1]["p"] + grid[i]["p"]) * (grid[i]["K"] - grid[i - 1]["K"])
    if total <= 0:
        return None
    for g in grid:
        g["p"] /= total
    quartiles = {}
    targets = {0.10: "p10", 0.25: "p25", 0.50: "p50", 0.75: "p75", 0.90: "p90"}
    cdf = 0.0
    for i in range(1, len(grid)):
        seg = 0.5 * (grid[i - 1]["p"] + grid[i]["p"]) * (grid[i]["K"] - grid[i - 1]["K"])
        prev_cdf = cdf
        cdf += seg
        for t, label in list(targets.items()):
            if prev_cdf <= t <= cdf and label not in quartiles:
                if seg > 0:
                    w = (t - prev_cdf) / seg
                    quartiles[label] = round(grid[i - 1]["K"] + w * (grid[i]["K"] - grid[i - 1]["K"]), 2)
    p50 = quartiles.get("p50")
    return {
        "quartiles": quartiles,
        "support": [round(grid[0]["K"], 2), round(grid[-1]["K"], 2)],
        "downside_skew_50_10_pct": round(100 * (p50 - quartiles["p10"]) / spot, 2)
            if p50 and "p10" in quartiles else None,
        "upside_skew_90_50_pct": round(100 * (quartiles["p90"] - p50) / spot, 2)
            if p50 and "p90" in quartiles else None,
        "n_density_points": len(grid),
    }


def process_expiration(ticker, exp_unix, exp_iso, spot, r_rate):
    today = datetime.utcnow().date()
    exp_date = datetime.fromtimestamp(exp_unix, tz=timezone.utc).date()
    dte = max(1, (exp_date - today).days)
    T = dte / 365.0

    try:
        j = yahoo_chain(ticker, exp_unix)
    except Exception as e:
        print(f"[{ticker} {exp_iso}] chain err: {e}")
        return None
    res = (j.get("optionChain") or {}).get("result") or []
    if not res:
        return None
    opts = res[0].get("options") or []
    if not opts:
        return None
    block = opts[0]
    calls = extract_rows(block.get("calls"), spot, T, r_rate, "call")
    puts = extract_rows(block.get("puts"), spot, T, r_rate, "put")
    if not calls or not puts:
        return None

    atm = atm_iv_interpolated(calls, puts, spot)
    c25 = find_25delta(calls, 0.25)
    p25 = find_25delta(puts, -0.25)
    iv_c25 = c25["iv"] if c25 else None
    iv_p25 = p25["iv"] if p25 else None
    rr25 = (iv_p25 - iv_c25) if (iv_p25 is not None and iv_c25 is not None) else None
    bf25 = (((iv_p25 + iv_c25) / 2) - atm) if (iv_p25 and iv_c25 and atm) else None
    pdf = compute_pdf(calls, spot, T, r_rate)

    return {
        "expiration": exp_iso,
        "dte": dte,
        "atm_iv": round(atm, 4) if atm else None,
        "iv_25d_call": round(iv_c25, 4) if iv_c25 else None,
        "iv_25d_put": round(iv_p25, 4) if iv_p25 else None,
        "rr25": round(rr25, 4) if rr25 is not None else None,
        "bf25": round(bf25, 4) if bf25 is not None else None,
        "c25_strike": c25["strike"] if c25 else None,
        "p25_strike": p25["strike"] if p25 else None,
        "n_calls": len(calls), "n_puts": len(puts),
        "rn_pdf": pdf,
    }


def compute_term_slope(exps):
    valid = [e for e in exps if e and e.get("atm_iv") is not None]
    if len(valid) < 2:
        return None
    front, back = valid[0], valid[-1]
    if back["dte"] == front["dte"]:
        return None
    return round((back["atm_iv"] - front["atm_iv"]) / (back["dte"] - front["dte"]) * 365, 4)


def classify_regime(term_slope, rr25_avg, bf25_avg):
    if term_slope is not None and term_slope < -0.05:
        return "TERM_INVERTED"
    if rr25_avg is not None and rr25_avg > 0.04:
        return "RICH_PUT_SKEW"
    if rr25_avg is not None and rr25_avg < -0.005:
        return "RICH_CALL_SKEW"
    if bf25_avg is not None and bf25_avg < 0.005:
        return "FLAT_SMILE"
    return "NORMAL"


def get_history():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY)
        return json.loads(obj["Body"].read())
    except Exception:
        return {"snapshots": []}


def percentile(values, target):
    if not values or target is None:
        return None
    sorted_v = sorted(v for v in values if v is not None)
    if not sorted_v:
        return None
    n = len(sorted_v)
    return round(100 * sum(1 for v in sorted_v if v <= target) / n, 1)


def compute_percentiles(current, history_snaps, ticker, metric):
    vals = []
    for snap in history_snaps[-720:]:
        u = (snap.get("underlyings") or {}).get(ticker, {})
        v = u.get(metric)
        if v is not None:
            vals.append(v)
    return percentile(vals, current)


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return
    try:
        body = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                           "parse_mode": "HTML", "disable_web_page_preview": True}).encode("utf-8")
        req = request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=body, headers={"Content-Type": "application/json"})
        request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[tg] err: {e}")


# ────────────────────────── Main handler ───────────────────────────────
def lambda_handler(event, context):
    t0 = time.time()
    print(f"[vol-surface] starting {datetime.now(timezone.utc).isoformat()}")

    # risk-free rate proxy
    r_rate = 0.045
    ffr = fred_latest("DFF")
    if ffr is not None:
        r_rate = ffr / 100.0
    print(f"[vol-surface] r_rate={r_rate:.4f}")

    history = get_history()
    history_snaps = history.get("snapshots", [])

    underlyings_out = {}
    alerts = []

    for ticker in UNDERLYINGS:
        try:
            base = fetch_underlying_data(ticker)
            if not base or not base["spot"]:
                print(f"[{ticker}] no spot/chain")
                continue
            spot = base["spot"]
            picks = pick_target_expirations(base["expirations_unix"], TARGET_DTES)
            print(f"[{ticker}] spot={spot} picks={[p[1] for p in picks]}")
            exp_results = []
            for exp_unix, exp_iso in picks:
                er = process_expiration(ticker, exp_unix, exp_iso, spot, r_rate)
                if er:
                    exp_results.append(er)
                time.sleep(0.25)  # gentle pacing per Yahoo
            if not exp_results:
                continue

            rr_vals = [e["rr25"] for e in exp_results if e["rr25"] is not None]
            bf_vals = [e["bf25"] for e in exp_results if e["bf25"] is not None]
            atm_vals = [e["atm_iv"] for e in exp_results if e["atm_iv"] is not None]
            rr25_avg = sum(rr_vals) / len(rr_vals) if rr_vals else None
            bf25_avg = sum(bf_vals) / len(bf_vals) if bf_vals else None
            atm_avg = sum(atm_vals) / len(atm_vals) if atm_vals else None
            term_slope = compute_term_slope(exp_results)
            regime = classify_regime(term_slope, rr25_avg, bf25_avg)
            rr25_pct = compute_percentiles(rr25_avg, history_snaps, ticker, "rr25_avg")
            atm_pct = compute_percentiles(atm_avg, history_snaps, ticker, "atm_avg")

            underlyings_out[ticker] = {
                "spot": round(spot, 2),
                "expirations": exp_results,
                "rr25_avg": round(rr25_avg, 4) if rr25_avg is not None else None,
                "bf25_avg": round(bf25_avg, 4) if bf25_avg is not None else None,
                "atm_avg": round(atm_avg, 4) if atm_avg is not None else None,
                "term_slope_per_year": term_slope,
                "regime": regime,
                "rr25_pctile_history": rr25_pct,
                "atm_pctile_history": atm_pct,
            }

            prior_regime = ((history_snaps[-1].get("underlyings") if history_snaps else {})
                            or {}).get(ticker, {}).get("regime") if history_snaps else None
            if prior_regime and prior_regime != regime:
                alerts.append(f"{ticker} regime: {prior_regime} → {regime}")
            if rr25_pct is not None and rr25_pct >= 90:
                alerts.append(f"{ticker} RR25 pctile {rr25_pct} (extreme put skew)")
            if rr25_pct is not None and rr25_pct <= 5:
                alerts.append(f"{ticker} RR25 pctile {rr25_pct} (extreme call skew/froth)")
            if term_slope is not None and term_slope < -0.05:
                alerts.append(f"{ticker} term-structure INVERTED ({term_slope:+.4f}/yr)")
        except Exception as e:
            print(f"[{ticker}] EXC: {e}")
            continue

    # global signals
    regimes = [v.get("regime") for v in underlyings_out.values()]
    n_inverted = regimes.count("TERM_INVERTED")
    n_put = regimes.count("RICH_PUT_SKEW")
    n_call = regimes.count("RICH_CALL_SKEW")
    n_flat = regimes.count("FLAT_SMILE")
    if n_inverted >= 2:
        global_regime = "CROSS_ASSET_INVERSION"
    elif n_put >= 3:
        global_regime = "DEFENSIVE_BID"
    elif n_call >= 2:
        global_regime = "SPECULATIVE_FROTH"
    elif n_flat >= 3:
        global_regime = "COMPLACENCY"
    else:
        global_regime = "MIXED"

    rr_pcts = [v.get("rr25_pctile_history") for v in underlyings_out.values()
               if v.get("rr25_pctile_history") is not None]
    avg_rr_pct = round(sum(rr_pcts) / len(rr_pcts), 1) if rr_pcts else None

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "r_rate_used": r_rate,
        "underlyings": underlyings_out,
        "global": {
            "regime": global_regime,
            "n_underlyings": len(underlyings_out),
            "n_term_inverted": n_inverted,
            "n_put_skew": n_put,
            "n_call_skew": n_call,
            "n_flat_smile": n_flat,
            "avg_rr25_pctile": avg_rr_pct,
        },
        "alerts": alerts,
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json",
                   CacheControl="public, max-age=300")

    # trimmed history
    light = {
        "generated_at": out["generated_at"], "global": out["global"],
        "underlyings": {
            u: {"spot": v["spot"], "regime": v["regime"],
                "rr25_avg": v["rr25_avg"], "bf25_avg": v["bf25_avg"],
                "atm_avg": v["atm_avg"], "term_slope_per_year": v["term_slope_per_year"],
                "rr25_pctile_history": v["rr25_pctile_history"],
                "atm_pctile_history": v["atm_pctile_history"]}
            for u, v in underlyings_out.items()
        },
    }
    history_snaps.append(light)
    history_snaps = history_snaps[-HISTORY_MAX:]
    s3.put_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY,
                   Body=json.dumps({"snapshots": history_snaps}, default=str).encode("utf-8"),
                   ContentType="application/json",
                   CacheControl="public, max-age=600")

    if alerts:
        emoji = {"DEFENSIVE_BID": "🛡", "SPECULATIVE_FROTH": "🎈",
                  "CROSS_ASSET_INVERSION": "🚨", "COMPLACENCY": "😴", "MIXED": "↔️"}
        msg = (f"{emoji.get(global_regime,'📊')} <b>VOL-SURFACE</b> [{global_regime}]\n"
               + "\n".join(f"• {a}" for a in alerts[:6]))
        maybe_telegram(msg)

    print(f"[vol-surface] done {out['elapsed_s']}s regime={global_regime} alerts={len(alerts)}")

    return {
        "statusCode": 200,
        "body": json.dumps({"ok": True, "regime": global_regime,
                            "n_underlyings": len(underlyings_out),
                            "alerts": len(alerts)}, default=str),
    }
