"""
justhodl-backtest-harness v1.0 — walk-forward validation for the whole desk
============================================================================
MODE A (rule archetypes): 8 parameterized signal archetypes mapping to the
desk's engine families, evaluated ANCHORED WALK-FORWARD over the rings
universe (top-1500 by dollar-volume, 256 trading days): params are chosen on
the anchored train window only, applied to out-of-sample test folds; trades
are 21-day forward EXCESS returns vs SPY, pooled cross-sectionally.
Gate per rule: OOS Sharpe must clear the Bailey/Lopez-de-Prado expected
maximum Sharpe under N trials (deflated-Sharpe logic), n_trades >= 40,
maxDD >= -40%. PASS = deployable archetype; FAIL = the backtest said no.
MODE B (live signals): every schema-v2 signal in justhodl-signals
(baseline_price + check_windows markers), independently re-graded: realized
21d excess vs SPY in the predicted direction, grouped by signal_type.
Output: data/backtest-harness.json · weekly Sun 12:10 UTC + on-demand.
"""
import json, gzip, math, os, time, urllib.request
from datetime import datetime, timezone
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
UP_STATE = "data/_upside/state.json.gz"
STATE_KEY = "data/_backtest/state.json"
OUT_KEY = "data/backtest-harness.json"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
HORIZON = 21
N_UNIVERSE = 1500
WARMUP = 110
N_FOLDS = 3
VERSION = "1.0.0"
DIAG = []


def jget(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl admin@justhodl.ai"})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def inv_norm(p):
    # Acklam rational approximation of the inverse normal CDF
    if p <= 0 or p >= 1:
        return 0.0
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
          1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
          6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
          -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
          3.754408661907416e+00]
    pl = 0.02425
    if p < pl:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
               ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    if p > 1 - pl:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
                ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    q = p - 0.5
    r = q * q
    return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q / \
           (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)


def expected_max_sr(n_trials, n_trades):
    # Bailey & Lopez de Prado: E[max SR] under N independent trials, null SR=0
    if n_trades < 5 or n_trials < 1:
        return 99.0
    g = 0.5772156649
    z1 = inv_norm(1 - 1.0 / max(2, n_trials))
    z2 = inv_norm(1 - 1.0 / (max(2, n_trials) * math.e))
    return ((1 - g) * z1 + g * z2) / math.sqrt(max(1, n_trades - 1))


# ── rule archetypes: f(prices, i, cfg) -> bool entry at index i ──
def r_mom_breakout(p, i, n):
    return i >= n and p[i] > max(p[i - n:i])

def r_ma_cross(p, i, fast, slow):
    if i < slow + 1:
        return False
    fa = sum(p[i - fast + 1:i + 1]) / fast
    sl = sum(p[i - slow + 1:i + 1]) / slow
    fa0 = sum(p[i - fast:i]) / fast
    sl0 = sum(p[i - slow:i]) / slow
    return fa > sl and fa0 <= sl0

def r_pullback(p, i, lo, hi):
    if i < 200:
        return False
    ma200 = sum(p[i - 199:i + 1]) / 200
    off = p[i] / max(p[max(0, i - 251):i + 1]) - 1
    return p[i] > ma200 and lo <= off * 100 <= hi

def r_meanrev(p, i, x):
    if i < 60:
        return False
    off = p[i] / max(p[max(0, i - 251):i + 1]) - 1
    return off * 100 <= -x

def r_relstr(p, i, spy, lb, top):
    if i < lb or spy[i - lb] <= 0:
        return False
    r_ = p[i] / p[i - lb] - 1
    rs = spy[i] / spy[i - lb] - 1
    return (r_ - rs) * 100 >= top

def r_squeeze(p, i, look, pct_thr):
    if i < look + 40:
        return False
    rets = [p[k] / p[k - 1] - 1 for k in range(i - look + 1, i + 1)]
    m = sum(rets) / len(rets)
    sd = math.sqrt(sum((x - m) ** 2 for x in rets) / len(rets))
    hist = []
    for j in range(i - 39, i + 1):
        rr = [p[k] / p[k - 1] - 1 for k in range(j - look + 1, j + 1)]
        mm = sum(rr) / len(rr)
        hist.append(math.sqrt(sum((x - mm) ** 2 for x in rr) / len(rr)))
    rank = sum(1 for h in hist if h <= sd) / len(hist)
    return rank <= pct_thr and p[i] > max(p[i - 10:i])

def r_consistency(p, i, k):
    if i < k * 21:
        return False
    return all(p[i - j * 21] > p[i - (j + 1) * 21] for j in range(k))

def r_basing_break(p, i, look, band):
    if i < look + 1:
        return False
    w = p[i - look:i]
    lo, hi = min(w), max(w)
    return hi > 0 and (hi / lo - 1) * 100 <= band and p[i] > hi

RULES = {
  "momentum_breakout":   {"fam": "trend/ignition",      "cfgs": [{"n": 50}, {"n": 100}, {"n": 200}],
                           "fn": lambda p, i, spy, c: r_mom_breakout(p, i, c["n"])},
  "ma_cross":            {"fam": "trend",               "cfgs": [{"fast": 10, "slow": 50}, {"fast": 20, "slow": 50}, {"fast": 20, "slow": 100}],
                           "fn": lambda p, i, spy, c: r_ma_cross(p, i, c["fast"], c["slow"])},
  "pullback_in_uptrend": {"fam": "upside-radar",        "cfgs": [{"lo": -15, "hi": -5}, {"lo": -20, "hi": -8}],
                           "fn": lambda p, i, spy, c: r_pullback(p, i, c["lo"], c["hi"])},
  "deep_drawdown_buy":   {"fam": "mean-reversion",      "cfgs": [{"x": 25}, {"x": 35}, {"x": 45}],
                           "fn": lambda p, i, spy, c: r_meanrev(p, i, c["x"])},
  "relative_strength":   {"fam": "momentum/leaders",    "cfgs": [{"lb": 21, "top": 10}, {"lb": 63, "top": 15}, {"lb": 126, "top": 20}],
                           "fn": lambda p, i, spy, c: r_relstr(p, i, spy, c["lb"], c["top"])},
  "vol_squeeze_break":   {"fam": "volatility-squeeze",  "cfgs": [{"look": 20, "pct_thr": 0.2}, {"look": 20, "pct_thr": 0.1}],
                           "fn": lambda p, i, spy, c: r_squeeze(p, i, c["look"], c["pct_thr"])},
  "momentum_consistency":{"fam": "quality-momentum",    "cfgs": [{"k": 3}, {"k": 4}],
                           "fn": lambda p, i, spy, c: r_consistency(p, i, c["k"])},
  "basing_breakout":     {"fam": "pre-pump/basing",     "cfgs": [{"look": 60, "band": 12}, {"look": 60, "band": 18}, {"look": 40, "band": 10}],
                           "fn": lambda p, i, spy, c: r_basing_break(p, i, c["look"], c["band"])},
}


def collect_trades(rule_fn, cfg, uni, spy, i0, i1):
    """entries in [i0,i1); returns list of 21d excess returns vs SPY"""
    out = []
    for t, p in uni.items():
        last_entry = -999
        for i in range(max(i0, 1), i1):
            if i + HORIZON >= len(p):
                break
            if i - last_entry < HORIZON:
                continue
            try:
                if rule_fn(p, i, spy, cfg):
                    r_ = p[i + HORIZON] / p[i] - 1
                    rb = spy[i + HORIZON] / spy[i] - 1
                    out.append(r_ - rb)
                    last_entry = i
            except Exception:
                break
    return out


def stats(trades):
    n = len(trades)
    if n < 5:
        return {"n": n, "sr": None, "hit": None, "avg": None, "maxdd": None, "curve": []}
    m = sum(trades) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in trades) / n) or 1e-9
    sr = m / sd * math.sqrt(252 / HORIZON)
    eq, peak, mdd, curve = 1.0, 1.0, 0.0, []
    for r_ in trades:
        eq *= (1 + r_ / 10.0)          # 10-slot book normalization
        peak = max(peak, eq)
        mdd = min(mdd, eq / peak - 1)
        curve.append(round(eq, 4))
    step = max(1, len(curve) // 60)
    return {"n": n, "sr": round(sr, 2), "hit": round(sum(1 for x in trades if x > 0) / n * 100, 1),
             "avg": round(m * 100, 2), "maxdd": round(mdd * 100, 1), "curve": curve[::step][:60]}


def mode_a(uni, spy):
    L = len(spy)
    seg = (L - WARMUP - HORIZON) // N_FOLDS
    results = []
    for name, R in RULES.items():
        oos = []
        picks = []
        for f in range(N_FOLDS):
            t1 = WARMUP + f * seg
            t2 = t1 + seg
            best, best_sr = None, -99
            for cfg in R["cfgs"]:
                tr = collect_trades(R["fn"], cfg, uni, spy, 1, t1)
                st = stats(tr)
                if st["sr"] is not None and st["sr"] > best_sr:
                    best, best_sr = cfg, st["sr"]
            if best is None:
                best = R["cfgs"][0]
            picks.append(best)
            oos += collect_trades(R["fn"], best, uni, spy, t1, t2)
        st = stats(oos)
        emax = expected_max_sr(len(R["cfgs"]) * N_FOLDS, st["n"])
        gate = bool(st["sr"] is not None and st["sr"] > emax
                     and st["n"] >= 40 and (st["maxdd"] or -99) >= -40)
        results.append({"rule": name, "family": R["fam"], "configs_tried": len(R["cfgs"]),
                         "chosen": picks[-1], "oos": st,
                         "deflated_gate_sr": round(emax, 2), "PASS": gate})
    results.sort(key=lambda x: -(x["oos"]["sr"] if x["oos"]["sr"] is not None else -99))
    return results


def mode_b(uni_full, spy, last_date):
    T = DDB.Table("justhodl-signals")
    items, lek = [], None
    while True:
        kw = {}
        if lek:
            kw["ExclusiveStartKey"] = lek
        r = T.scan(**kw)
        for it in r.get("Items") or []:
            if it.get("baseline_price") and it.get("check_windows") and it.get("ticker"):
                items.append(it)
        lek = r.get("LastEvaluatedKey")
        if not lek:
            break
    DIAG.append(f"mode B: {len(items)} schema-v2 signals")
    st = {}
    try:
        st = json.loads(S3.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read())
    except Exception:
        pass
    pxc = st.get("pxc") or {}
    now = time.time()
    fetched = 0
    by_type = {}
    for it in items:
        ty = it.get("signal_type") or "unknown"
        tick = it.get("ticker")
        ep = int(it.get("logged_epoch") or 0)
        age_td = (now - ep) / 86400 * (252 / 365.0)
        if age_td < HORIZON + 2:
            by_type.setdefault(ty, {"pending": 0, "trades": []})["pending"] += 1
            continue
        d0 = datetime.fromtimestamp(ep, tz=timezone.utc).date().isoformat()
        key = f"{tick}"
        if key not in pxc and fetched < 60:
            try:
                j = jget(f"https://financialmodelingprep.com/stable/historical-price-eod/"
                          f"light?symbol={tick}&apikey={FMP_KEY}")
                rows = j if isinstance(j, list) else (j.get("historical") or [])
                pxc[key] = {x["date"]: x.get("price") or x.get("close") for x in rows[:300]
                             if x.get("date")}
                fetched += 1
            except Exception:
                pxc[key] = {}
        series = pxc.get(key) or {}
        dates = sorted(d for d in series if d >= d0)
        spy_ix = None
        if len(dates) > HORIZON:
            p0, p1 = series.get(dates[0]), series.get(dates[HORIZON])
            if p0 and p1:
                rr = p1 / p0 - 1
                rb = 0.0
                if len(spy) > HORIZON:
                    back = int(age_td)
                    j0 = max(0, len(spy) - 1 - back)
                    j1 = min(len(spy) - 1, j0 + HORIZON)
                    if spy[j0]:
                        rb = spy[j1] / spy[j0] - 1
                ex = (rr - rb) * (1 if (it.get("predicted_direction") or "UP") == "UP" else -1)
                by_type.setdefault(ty, {"pending": 0, "trades": []})["trades"].append(ex)
                continue
        by_type.setdefault(ty, {"pending": 0, "trades": []})["pending"] += 1
    st["pxc"] = {k: v for k, v in list(pxc.items())[-400:]}
    S3.put_object(Bucket=BUCKET, Key=STATE_KEY, Body=json.dumps(st).encode(),
                  ContentType="application/json")
    table = []
    for ty, d in by_type.items():
        tr = d["trades"]
        n = len(tr)
        row = {"signal_type": ty, "graded": n, "pending": d["pending"]}
        if n >= 3:
            m = sum(tr) / n
            row |= {"hit_pct": round(sum(1 for x in tr if x > 0) / n * 100, 1),
                     "avg_excess_pct": round(m * 100, 2)}
        table.append(row)
    table.sort(key=lambda x: -(x.get("graded") or 0))
    return table


def lambda_handler(event=None, context=None):
    t0 = time.time()
    DIAG.clear()
    up = json.loads(gzip.decompress(S3.get_object(Bucket=BUCKET, Key=UP_STATE)["Body"].read()))
    rings, dv = up.get("rings") or {}, up.get("dv") or {}
    spy = rings.get("SPY") or []
    ranked = sorted((t for t in rings if t in dv and len(rings[t]) == len(spy)),
                     key=lambda t: -dv[t])[:N_UNIVERSE]
    uni = {t: rings[t] for t in ranked}
    DIAG.append(f"universe {len(uni)} of {len(rings)} rings · {len(spy)} days · "
                 f"{N_FOLDS} OOS folds · horizon {HORIZON}d excess vs SPY")
    rules = mode_a(uni, spy)
    live = mode_b(rings, spy, up.get("last_date"))
    out = {"engine": "backtest-harness", "version": VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "duration_s": round(time.time() - t0, 1),
            "universe_n": len(uni), "days": len(spy), "folds": N_FOLDS,
            "horizon_days": HORIZON, "n_pass": sum(1 for r in rules if r["PASS"]),
            "rules": rules, "live_signal_types": live, "diagnostics": list(DIAG),
            "methodology": ("Mode A: anchored walk-forward (Pardo) — parameters chosen on "
              "the anchored train window only, applied to 3 sequential out-of-sample "
              "folds; trades are 21-day forward excess returns vs SPY pooled across a "
              "1,500-name dollar-volume-ranked universe; PASS requires OOS Sharpe above "
              "the Bailey/Lopez-de-Prado expected-max-Sharpe under the number of trials "
              "(deflated-Sharpe gate), >=40 trades, maxDD >= -40%. Mode B: every "
              "schema-v2 live signal independently re-graded at 21 trading days, excess "
              "vs SPY in the predicted direction, grouped by signal_type. One year of "
              "ring history limits depth; cross-sectional breadth supplies trade count. "
              "Research, not advice.")}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[harness] {out['n_pass']}/{len(rules)} rules pass · {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"n_pass": out["n_pass"]})}
