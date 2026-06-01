"""justhodl-cross-asset-rv — the Cross-Asset Relative Value engine.

Every other engine analyses one market or classifies a regime. None asks the
RV desk's question: "what is mispriced ACROSS assets?" Macro funds make
asymmetric, low-beta money on cross-asset dislocations — when two markets
that should co-move pull apart, one of them is wrong.

METHOD (institutional-standard)
═══════════════════════════════
For each relationship, asset A is valued against driver B. We run an OLS
regression of A on B over a ~2-year window, take the residual, and z-score
it. The latest residual z is how rich (z>0) or cheap (z<0) A is versus what
B implies. |z| >= 2 = DISLOCATED, >= 1.3 = STRETCHED, else NEUTRAL.
Regression captures the true beta and sign, so it works whether the two
series move together or inversely.

THE SIX RELATIONSHIPS
═════════════════════
  1. Gold vs real yields        ln(GLD)   ~ DFII10
  2. HY credit vs equity vol    HY OAS    ~ VIX
  3. Yield curve vs growth      10Y-2Y    ~ copper/gold
  4. Copper/gold vs the 10Y     COPX/GLD  ~ DGS10
  5. Breakevens vs oil          T10YIE    ~ ln(USO)
  6. Equities vs credit         ln(SPY)   ~ HY OAS

Each dislocation comes with a rich/cheap read and the mean-reversion trade.
OUTPUT: data/cross-asset-rv.json   Schedule: daily.
"""
import json, os, time, math
from datetime import datetime, timezone, timedelta
from urllib import request, error
import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1073)

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/cross-asset-rv.json"
S3_HISTORY_KEY = "data/cross-asset-rv-history.json"
HISTORY_MAX = 365

FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")

WINDOW_DAYS = 520          # ~2 trading years
DISLOCATED, STRETCHED = 2.0, 1.3


def _get_json(url, timeout=20, retries=3):
    for i in range(retries):
        try:
            req = request.Request(url, headers={"User-Agent": "JustHodl-RV/1.0"})
            with request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError):
            time.sleep(0.6 * (i + 1))
    return None


def fetch_fred(series_id):
    start = (datetime.now(timezone.utc) - timedelta(days=1100)).strftime("%Y-%m-%d")
    url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
           f"&api_key={FRED_KEY}&file_type=json&observation_start={start}")
    d = _get_json(url)
    out = {}
    if d and "observations" in d:
        for o in d["observations"]:
            v = o.get("value")
            if v and v != ".":
                try:
                    out[o["date"]] = float(v)
                except ValueError:
                    pass
    return out


def fetch_fmp(symbol):
    url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
           f"?symbol={symbol}&apikey={FMP_KEY}")
    d = _get_json(url)
    rows = d.get("historical", []) if isinstance(d, dict) else (d or [])
    out = {}
    for r in rows:
        try:
            c = float(r.get("close") or r.get("adjClose") or 0)
            if c > 0 and r.get("date"):
                out[r["date"][:10]] = c
        except Exception:
            pass
    return out


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


def align(series_a, series_b):
    """Intersect two {date: value} dicts -> (dates, a_vals, b_vals) sorted."""
    common = sorted(set(series_a) & set(series_b))
    common = common[-WINDOW_DAYS:]
    return (common,
            [series_a[d] for d in common],
            [series_b[d] for d in common])


def residual_z(a_vals, b_vals):
    """OLS A on B; z-score of the latest residual + fit stats."""
    n = len(a_vals)
    if n < 120:
        return None
    mx = sum(b_vals) / n
    my = sum(a_vals) / n
    vx = sum((x - mx) ** 2 for x in b_vals)
    vy = sum((y - my) ** 2 for y in a_vals)
    if vx == 0 or vy == 0:
        return None
    cov = sum((b_vals[i] - mx) * (a_vals[i] - my) for i in range(n))
    beta = cov / vx
    alpha = my - beta * mx
    resid = [a_vals[i] - (alpha + beta * b_vals[i]) for i in range(n)]
    mr = sum(resid) / n
    sd = (sum((r - mr) ** 2 for r in resid) / n) ** 0.5
    if sd == 0:
        return None
    return {"z": round((resid[-1] - mr) / sd, 2),
            "beta": round(beta, 4),
            "r2": round((cov ** 2) / (vx * vy), 3),
            "n": n}


# ── relationship definitions ──────────────────────────────────────────────
# build_a / build_b are functions of the raw-series cache.
def _ln(d):
    return {k: math.log(v) for k, v in d.items() if v > 0}


def _ratio(num, den):
    return {k: num[k] / den[k] for k in (set(num) & set(den)) if den[k] != 0}


def _spread(x, y):
    return {k: x[k] - y[k] for k in (set(x) & set(y))}


RELATIONSHIPS = [
    {
        "key": "gold_vs_real_yields",
        "label": "Gold vs real yields",
        "a": lambda c: _ln(c["GLD"]), "b": lambda c: c["DFII10"],
        "pos": "Gold is RICH vs real yields — priced for lower real rates than the bond market shows.",
        "neg": "Gold is CHEAP vs real yields — lagging the real-rate move; catch-up room.",
        "pos_trade": "Fade gold strength, or expect 10Y real yields to fall to justify the price.",
        "neg_trade": "Accumulate gold — the real-yield backdrop supports a higher price.",
    },
    {
        "key": "hy_credit_vs_equity_vol",
        "label": "HY credit vs equity vol",
        "a": lambda c: c["HY_OAS"], "b": lambda c: c["VIX"],
        "pos": "Credit is CHEAP vs equity vol — HY spreads wide for this level of VIX.",
        "neg": "Credit is RICH vs equity vol — HY spreads tight while VIX says otherwise; late-cycle complacency.",
        "pos_trade": "Credit compensating well — own HY carry, or expect VIX to rise to credit.",
        "neg_trade": "Credit complacent — buy protection (HY CDX / puts); spreads tend to catch up to vol.",
    },
    {
        "key": "curve_vs_growth",
        "label": "Yield curve vs the copper/gold growth signal",
        "a": lambda c: _spread(c["DGS10"], c["DGS2"]),
        "b": lambda c: _ratio(c["COPX"], c["GLD"]),
        "pos": "The 2s10s curve is STEEP vs what copper/gold implies — bonds pricing more growth/term premium than metals.",
        "neg": "The curve is FLAT/INVERTED vs copper/gold — the bond market more pessimistic than industrial metals.",
        "pos_trade": "Curve flattener, or expect copper to rally to confirm the steepening.",
        "neg_trade": "Curve steepener — metals say growth is firmer than the curve admits.",
    },
    {
        "key": "copper_gold_vs_10y",
        "label": "Copper/gold vs the 10-year yield",
        "a": lambda c: _ratio(c["COPX"], c["GLD"]), "b": lambda c: c["DGS10"],
        "pos": "Industrial metals are RICH vs the 10Y — copper/gold pricing more growth than bonds.",
        "neg": "Copper/gold is CHEAP vs the 10Y — metals more pessimistic on growth than the bond market.",
        "pos_trade": "Expect the 10Y to rise toward the metals signal, or fade cyclicals.",
        "neg_trade": "Expect the 10Y to fall, or position for a copper/cyclical catch-up.",
    },
    {
        "key": "breakevens_vs_oil",
        "label": "Inflation breakevens vs oil",
        "a": lambda c: c["T10YIE"], "b": lambda c: _ln(c["USO"]),
        "pos": "10Y breakevens are RICH vs oil — inflation expectations running ahead of the energy tape.",
        "neg": "Breakevens are CHEAP vs oil — inflation expectations lagging the move in crude.",
        "pos_trade": "Fade breakevens / receive inflation, or expect oil to rally to justify them.",
        "neg_trade": "Own breakevens / TIPS — energy says inflation expectations should be higher.",
    },
    {
        "key": "equities_vs_credit",
        "label": "Equities vs credit",
        "a": lambda c: _ln(c["SPY"]), "b": lambda c: c["HY_OAS"],
        "pos": "Equities are RICH vs credit — stocks ignoring the message in HY spreads; a classic pre-drawdown divergence.",
        "neg": "Equities are CHEAP vs credit — credit calmer than equities; room for an equity catch-up.",
        "pos_trade": "Trim equity beta / hedge — credit usually leads equities at turns.",
        "neg_trade": "Add equity exposure — credit, the better risk barometer, is sanguine.",
    },
]


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[cross-asset-rv] starting {datetime.now(timezone.utc).isoformat()}")

    cache = {}
    fred_map = {"DFII10": "DFII10", "DGS10": "DGS10", "DGS2": "DGS2",
                "T10YIE": "T10YIE", "HY_OAS": "BAMLH0A0HYM2", "VIX": "VIXCLS"}
    fmp_syms = ["GLD", "COPX", "USO", "SPY"]
    failed = []
    for name, sid in fred_map.items():
        cache[name] = fetch_fred(sid)
        if not cache[name]:
            failed.append(sid)
        print(f"[rv] FRED {sid}: {len(cache[name])} obs")
    for sym in fmp_syms:
        cache[sym] = fetch_fmp(sym)
        if not cache[sym]:
            failed.append(sym)
        print(f"[rv] FMP {sym}: {len(cache[sym])} obs")

    results = []
    for rel in RELATIONSHIPS:
        try:
            a_series = rel["a"](cache)
            b_series = rel["b"](cache)
        except Exception as e:
            print(f"[rv] {rel['key']} build err: {e}")
            results.append({"key": rel["key"], "label": rel["label"],
                            "state": "NO_DATA", "error": str(e)[:120]})
            continue
        dates, a_vals, b_vals = align(a_series, b_series)
        rz = residual_z(a_vals, b_vals)
        if rz is None:
            results.append({"key": rel["key"], "label": rel["label"],
                            "state": "NO_DATA",
                            "error": f"insufficient overlap ({len(dates)})"})
            continue
        z = rz["z"]
        az = abs(z)
        state = ("DISLOCATED" if az >= DISLOCATED else
                 "STRETCHED" if az >= STRETCHED else "NEUTRAL")
        pos = z > 0
        results.append({
            "key": rel["key"], "label": rel["label"],
            "residual_z": z, "abs_z": round(az, 2), "state": state,
            "direction": "A_RICH" if pos else "A_CHEAP",
            "fit_r2": rz["r2"], "fit_beta": rz["beta"], "n_obs": rz["n"],
            "as_of": dates[-1] if dates else None,
            "read": rel["pos"] if pos else rel["neg"],
            "trade": rel["pos_trade"] if pos else rel["neg_trade"],
        })
        print(f"[rv] {rel['key']}: z={z} {state} r2={rz['r2']}")

    scored = [r for r in results if r.get("state") not in ("NO_DATA",)]
    dislocations = sorted([r for r in scored if r["state"] == "DISLOCATED"],
                          key=lambda r: -r["abs_z"])
    stretched = [r for r in scored if r["state"] == "STRETCHED"]
    n_dis, n_str = len(dislocations), len(stretched)

    if n_dis >= 3:
        rv_state = "MULTIPLE_DISLOCATIONS"
        rv_read = (f"{n_dis} cross-asset relationships are dislocated — markets are "
                   f"materially out of line with each other. High RV-opportunity, "
                   f"and a sign one or more asset classes is mispricing macro risk.")
    elif n_dis >= 1:
        rv_state = "DISLOCATION_PRESENT"
        top = dislocations[0]
        rv_read = (f"{n_dis} dislocation(s) flagged — most extreme: {top['label']} "
                   f"(z={top['residual_z']}). {top['read']}")
    elif n_str >= 1:
        rv_state = "STRETCHED"
        rv_read = (f"No outright dislocations, but {n_str} relationship(s) stretched — "
                   f"watch for a cross-asset move.")
    else:
        rv_state = "ALIGNED"
        rv_read = "Cross-asset relationships are broadly in line — no RV edge right now."

    out = {
        "schema_version": "1.0",
        "method": "cross_asset_rv_ols_residual_z",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "data_failed": failed,
        "window_days": WINDOW_DAYS,
        "rv_state": rv_state,
        "rv_read": rv_read,
        "n_dislocated": n_dis,
        "n_stretched": n_str,
        "relationships": results,
        "dislocations": dislocations,
        "interpretation": (
            "Each relationship regresses asset A on driver B over ~2 years; the "
            "residual z-score is how rich (z>0) or cheap (z<0) A is vs what B "
            "implies. |z|>=2 dislocated, >=1.3 stretched. Dislocations are "
            "mean-reversion opportunities and a tell that some asset class is "
            "mispricing macro risk."
        ),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    hist = {"snapshots": []}
    try:
        hist = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY)["Body"].read())
        if "snapshots" not in hist:
            hist = {"snapshots": []}
    except Exception:
        pass
    prior_state = hist["snapshots"][-1]["rv_state"] if hist.get("snapshots") else None
    hist["snapshots"].append({"ts": out["generated_at"], "rv_state": rv_state,
                               "n_dislocated": n_dis, "n_stretched": n_str})
    hist["snapshots"] = hist["snapshots"][-HISTORY_MAX:]
    hist["updated_at"] = out["generated_at"]
    s3.put_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY,
                   Body=json.dumps(hist, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    if prior_state and prior_state != rv_state and n_dis >= 1:
        lines = "\n".join(f"• {d['label']}: z={d['residual_z']} — {d['read']}"
                          for d in dislocations[:4])
        maybe_telegram(
            f"[cross-asset-rv] <b>{rv_state.replace('_',' ')}</b>\n"
            f"{prior_state} → {rv_state}\n{lines}")

    print(f"[cross-asset-rv] done {out['elapsed_s']}s state={rv_state} "
          f"dislocated={n_dis} stretched={n_str} failed={failed}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "rv_state": rv_state, "n_dislocated": n_dis,
        "n_stretched": n_str, "data_failed": failed})}
