"""
justhodl-alert-backtester v1.0 — the monetization differentiator.

For each institutional alert rule, replay its FULL history and report what
actually happened next: "Euribor−OIS ≥ 50bp fired 3× since 2020 → median
SPX −5.1% over the next 21 sessions." Bloomberg sells alerts; nobody ships
the instant historical edge audit OF the alert rule itself.

Mechanics
  • Daily/period series from data/ecb-hist/*.json (own store) + FRED.
  • A rule FIRES on the first bar its condition turns true after being
    false ≥ REARM trading days (de-clusters episodes).
  • Forward SPY returns (Polygon, 1999+) at 5/21/63 sessions per fire.
  • Per rule: n_fires, last fire, median/mean forward, % negative.

Output: data/alert-backtests.json   Schedule: daily 12:00 UTC.
"""
import json
import os
import ssl
import urllib.request
from datetime import datetime, timezone, date
from statistics import mean, median

import boto3

S3 = boto3.client("s3")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
POLY_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
_ctx = ssl.create_default_context()
HORIZONS = [5, 21, 63]
REARM = 21  # trading days condition must be false before a new fire


def fred(series, start="1999-01-01"):
    u = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series}"
         f"&api_key={FRED_KEY}&file_type=json&observation_start={start}&limit=100000")
    try:
        d = json.loads(urllib.request.urlopen(u, timeout=45, context=_ctx).read())
        return [(o["date"], float(o["value"])) for o in d.get("observations", [])
                if o.get("value") not in (".", None, "")]
    except Exception as e:
        print(f"[bt] fred {series} err {str(e)[:60]}")
        return []


def hist(sid):
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key=f"data/ecb-hist/{sid}.json")["Body"].read())
        return [(p[0], float(p[1])) for p in d.get("points", []) if p[1] is not None]
    except Exception:
        return []


def stooq_spx():
    """Deep S&P 500 closes via Stooq (^spx, 1928+). Primary for backtest depth."""
    try:
        raw = urllib.request.urlopen(
            urllib.request.Request("https://stooq.com/q/d/l/?s=%5Espx&i=d",
                                   headers={"User-Agent": "Mozilla/5.0"}), timeout=45).read()
        lines = raw.decode("utf-8", "replace").strip().split("\n")
        out = {}
        for ln in lines[1:]:
            c = ln.split(",")
            if len(c) >= 5 and c[0][:2] in ("19", "20"):
                try:
                    out[c[0]] = float(c[4])
                except ValueError:
                    pass
        return out if len(out) > 5000 else None
    except Exception as e:
        print(f"[spx] stooq failed: {str(e)[:70]}")
        return None


def spy_closes():
    end = date.today().isoformat()
    u = (f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/1999-01-01/{end}"
         f"?adjusted=true&sort=asc&limit=50000&apiKey={POLY_KEY}")
    j = json.loads(urllib.request.urlopen(u, timeout=60, context=_ctx).read())
    out = []
    for row in (j.get("results") or []):
        out.append((datetime.fromtimestamp(row["t"] / 1000, tz=timezone.utc).date().isoformat(),
                    float(row["c"])))
    return out


def rolling_z(pts, lb):
    out = []
    vals = [v for _, v in pts]
    for i, (d, v) in enumerate(pts):
        if i < lb:
            continue
        w = vals[i - lb:i]
        m = sum(w) / lb
        sd = (sum((x - m) ** 2 for x in w) / lb) ** 0.5
        if sd > 0:
            out.append((d, (v - m) / sd))
    return out


def delta(pts, n):
    return [(pts[i][0], pts[i][1] - pts[i - n][1]) for i in range(n, len(pts))]


def join_spread(a, b, scale=100.0):
    bm = dict(b)
    return [(d, (v - bm[d]) * scale) for d, v in a if d in bm]


def fires(cond_series, rearm=REARM):
    """First-true-after-≥rearm-false fire dates from [(date, bool)]."""
    out, false_run = [], rearm  # armed at start
    for d, c in cond_series:
        if c and false_run >= rearm:
            out.append(d)
            false_run = 0
        elif c:
            false_run = 0
        else:
            false_run += 1
    return out


def forward_stats(fire_dates, spy):
    idx = {d: i for i, (d, _) in enumerate(spy)}
    dates_sorted = [d for d, _ in spy]

    def loc(d):
        if d in idx:
            return idx[d]
        # next trading day at/after d
        if d < dates_sorted[0]:
            return None  # fire predates SPX span — never map onto index 0
        import bisect
        i = bisect.bisect_left(dates_sorted, d)
        return i if i < len(spy) else None

    per_h = {h: [] for h in HORIZONS}
    usable = []
    for fd in fire_dates:
        i = loc(fd)
        if i is None:
            continue
        p0 = spy[i][1]
        row = {"date": fd}
        ok = False
        for h in HORIZONS:
            if i + h < len(spy):
                r = (spy[i + h][1] / p0 - 1) * 100
                per_h[h].append(r)
                row[f"fwd_{h}d"] = round(r, 2)
                ok = True
        if ok:
            usable.append(row)
    agg = {}
    for h in HORIZONS:
        v = per_h[h]
        agg[f"{h}d"] = ({"n": len(v), "median_pct": round(median(v), 2), "mean_pct": round(mean(v), 2),
                         "pct_negative": round(100 * sum(1 for x in v if x < 0) / len(v), 0)}
                        if v else {"n": 0})
    return agg, usable


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    sq = stooq_spx()
    spy = sorted(sq.items()) if sq else spy_closes()
    print(f"[bt] spy {len(spy)} bars {spy[0][0]}→{spy[-1][0]}")

    # series pulls
    eo = hist("euribor_ois_bp")
    itde = hist("it_de_10y_bp")
    estr, dfr = hist("estr"), hist("dfr")
    wages = hist("wages_negotiated")
    m3 = hist("m3_yoy")
    t2 = hist("t2_de_minus_it")
    eur = hist("eurusd")
    vix = fred("VIXCLS")
    vxv = fred("VXVCLS", "2007-01-01")
    hy = fred("BAMLH0A0HYM2")
    t10y2y = fred("T10Y2Y")
    nfci = fred("NFCI")

    RULES = []

    def add(rid, desc, source, cond_pts):
        f = fires(cond_pts)
        agg, rows = forward_stats(f, spy)
        RULES.append({"id": rid, "desc": desc, "source": source,
                      "since": cond_pts[0][0] if cond_pts else None,
                      "n_fires": len(f), "last_fired": f[-1] if f else None,
                      "forward_spy": agg, "recent_fires": rows[-6:]})
        print(f"[bt] {rid}: {len(f)} fires")

    if eo:
        add("euribor_ois_ge50", "Euribor−OIS 3M ≥ 50bp (EU interbank stress critical)",
            "ecb-hist euribor_ois_bp", [(d, v >= 50) for d, v in eo])
    if itde:
        add("it_de_ge150", "IT−DE 10Y ≥ 150bp (TPI-watch fragmentation)",
            "ecb-hist it_de_10y_bp", [(d, v >= 150) for d, v in itde])
    if estr and dfr:
        sp = join_spread(estr, dfr)
        add("estr_dfr_ge_m1", "€STR−DFR ≥ −1bp (floor losing grip)",
            "ecb-hist estr/dfr", [(d, v >= -1.0) for d, v in sp])
    if wages:
        add("wages_gt_35", "Negotiated wages > 3.5% YoY (second-round risk)",
            "ecb-hist wages_negotiated", [(d, v > 3.5) for d, v in wages])
    if m3:
        add("m3_lt_2", "M3 < 2% YoY (monetary contraction zone)",
            "ecb-hist m3_yoy", [(d, v < 2.0) for d, v in m3])
    if t2:
        z = rolling_z(t2, 60)
        add("t2_gap_z2", "TARGET2 DE−IT gap z>2 (5y) — capital-flight impulse",
            "ecb-hist t2_de_minus_it", [(d, v > 2.0) for d, v in z])
    if eur:
        z = rolling_z(eur, 252)
        add("eurusd_z_m2", "EUR/USD 1y z < −2 (euro stress extreme)",
            "ecb-hist eurusd", [(d, v < -2.0) for d, v in z])
    if vix and vxv:
        sp = join_spread(vix, vxv, scale=1.0)
        add("vix_term_inversion", "VIX > VIX3M (term-structure inversion)",
            "FRED VIXCLS/VXVCLS", [(d, v > 0) for d, v in sp])
    if hy:
        d3 = delta(hy, 66)
        add("hy_oas_3m_p50bp", "HY OAS +50bp over 3m (credit cracking)",
            "FRED BAMLH0A0HYM2", [(d, v >= 0.50) for d, v in d3])
    if t10y2y:
        add("curve_reinversion", "2s10s < 0 (curve inversion regime)",
            "FRED T10Y2Y", [(d, v < 0) for d, v in t10y2y])
    if nfci:
        add("nfci_positive", "NFCI > 0 (financial conditions tighter than avg)",
            "FRED NFCI", [(d, v > 0) for d, v in nfci])

    fired_recent = [r["id"] for r in RULES if r["last_fired"] and r["last_fired"] >= "2026-05-01"]
    out = {"engine": "alert-backtester", "version": "1.0", "generated_at": now.isoformat(),
           "spy_span": f"{spy[0][0]}→{spy[-1][0]}", "n_rules": len(RULES),
           "horizons_days": HORIZONS, "rearm_days": REARM, "rules": RULES,
           "read": (f"{len(RULES)} institutional alert rules replayed over their full history "
                    f"with SPY forwards to {spy[-1][0]}. Every alert now ships with its own "
                    f"track record — fires, median forward path, and % of negative outcomes. "
                    + (f"Recently active: {', '.join(fired_recent)}." if fired_recent else ""))}
    S3.put_object(Bucket=BUCKET, Key="data/alert-backtests.json",
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=21600")
    print(f"[bt] wrote {len(RULES)} rules")
    return {"statusCode": 200, "body": json.dumps({"rules": len(RULES)})}
