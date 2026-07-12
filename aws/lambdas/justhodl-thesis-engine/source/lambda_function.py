"""justhodl-thesis-engine v1.0 — ops 3165.

Khalid's 207 TradingView watchlists are not stock baskets — they are
NAMED THESES ("Financial Crisis Signs", "fed plumbing", "Banking Sector:
Banks = Liquidity Proxy"), each holding the indicator set that argues
the thesis. This engine makes each one measurable:

  1. RESOLVE members → live series
       FRED:*           → FRED API (546 in his universe)
       NASDAQ/NYSE/…    → Polygon daily closes (1,663)
       TVC:/ECONOMICS:* → mapped to FRED where a true equivalent exists
       formulas         → arithmetic evaluated over resolved operands
                          (e.g. FRED:FEDFUNDS-FRED:BAMLHE00EHYIEY)
  2. ACTIVATION INDEX — per member, z-score vs trailing 252 obs; per
     thesis per date, activation = share of members at |z| >= 1.5.
     Polarity-agnostic on purpose: a thesis panel is "firing" when an
     unusual share of ITS OWN indicators sit at extremes.
  3. EVENT STUDY — days in the top activation quintile vs forward SPY
     5/21/63d returns, against the unconditional base rate. Reports mean
     excess, hit rate, t-stat, n. This answers "does this thesis lead?"
     from HISTORY instead of waiting weeks for forward grading.
  4. SIGNALS — theses that are FIRING NOW and carry a significant
     historical edge (|t| >= 2, n >= 20) emit into justhodl-signals with
     the direction the DATA showed (never a lexicon guess), so the live
     scorecard keeps grading them.

State (gzipped series cache) in data/thesis-state.json.gz keeps daily
runs cheap. Output: data/thesis-engine.json
"""

import gzip
import io
import json
import math
import os
import re
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
POLY = os.environ.get("POLYGON_KEY", "")
FRED = (os.environ.get("FRED_API_KEY") or os.environ.get("FRED_KEY")
        or "2f057499936072679d8843d7fce99989")
OUT_KEY = "data/thesis-engine.json"
STATE_KEY = "data/thesis-state.json.gz"
LISTS_KEY = "data/tv-watchlists.json"

LOOKBACK_DAYS = 780          # ~3y calendar
MIN_MEMBERS = 8              # a thesis needs a real evidence set
MAX_MEMBERS = 120            # cap the 500-symbol monsters
Z_FIRE = 1.5                 # member is "extreme"
MIN_COVERAGE = 0.45          # >=45% of members must resolve

S3 = boto3.client("s3", region_name="us-east-1")

TVC_FRED = {"US02Y": "DGS2", "US03MY": "DTB3", "US10Y": "DGS10",
            "US30Y": "DGS30", "US05Y": "DGS5", "US01Y": "DGS1",
            "US03Y": "DGS3", "US07Y": "DGS7", "US06MY": "DGS6MO",
            "DXY": "DTWEXBGS", "VIX": "VIXCLS", "USOIL": "DCOILWTICO",
            "US02YY": "DGS2", "US10YY": "DGS10", "US05YY": "DGS5",
            "JP10Y": "IRLTLT01JPM156N", "DE10Y": "IRLTLT01DEM156N",
            "GB10Y": "IRLTLT01GBM156N"}
ECON_FRED = {"USCBBS": "WALCL", "USINTR": "FEDFUNDS", "USIRYY": "CPIAUCSL",
             "USUR": "UNRATE", "USM2": "M2SL", "USBBS": "WALCL",
             "USINBR": "TOTRESNS", "USNFP": "PAYEMS", "USCCPI": "CPIAUCSL",
             "USRRP": "RRPONTSYD", "USBOI": "NAPM"}
EQ_EX = {"NASDAQ", "NYSE", "AMEX", "ARCA", "BATS", "CBOE", "OTC"}
OPS_RE = re.compile(r"[+\-*/()]")
NUM_RE = re.compile(r"^[\d.]+$")


# ── infra ────────────────────────────────────────────────────────────
def s3_get(key, default=None, gz=False):
    try:
        b = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if gz:
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return default


def s3_put(key, doc, gz=False):
    b = json.dumps(doc).encode()
    if gz:
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as f:
            f.write(b)
        b = buf.getvalue()
    S3.put_object(Bucket=BUCKET, Key=key, Body=b,
                  ContentType="application/json")


def http(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "jh-thesis/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


# ── resolver ─────────────────────────────────────────────────────────
def resolve(sym):
    s = str(sym).strip().upper()
    if not s:
        return None, None
    if OPS_RE.search(s):
        ops = [o.strip() for o in re.split(r"[+\-*/()]", s)
               if o.strip() and not NUM_RE.match(o.strip())]
        if not ops:
            return None, None
        if all(resolve(o)[0] in ("FRED", "POLY") for o in ops):
            return "FORMULA", s
        return None, None
    if ":" not in s:
        return "POLY", s
    ex, t = s.split(":", 1)
    if ex == "FRED":
        return "FRED", t
    if ex == "TVC":
        f = TVC_FRED.get(t)
        return ("FRED", f) if f else (None, None)
    if ex == "ECONOMICS":
        f = ECON_FRED.get(t)
        return ("FRED", f) if f else (None, None)
    if ex in EQ_EX:
        return "POLY", t
    return None, None


# ── fetchers ─────────────────────────────────────────────────────────
def fred_series(sid, start):
    try:
        d = http("https://api.stlouisfed.org/fred/series/observations"
                 f"?series_id={sid}&api_key={FRED}&file_type=json"
                 f"&observation_start={start}")
        out = {}
        for o in d.get("observations") or []:
            v = o.get("value")
            if v not in (".", "", None):
                try:
                    out[o["date"]] = float(v)
                except Exception:
                    pass
        return sid, out
    except Exception:
        return sid, {}


def poly_series(tk, d0, d1):
    try:
        d = http(f"https://api.polygon.io/v2/aggs/ticker/{tk}/range/1/day/"
                 f"{d0}/{d1}?adjusted=true&sort=asc&limit=900&apiKey={POLY}")
        return tk, {datetime.utcfromtimestamp(r["t"] / 1000).date().isoformat():
                    r["c"] for r in (d.get("results") or [])}
    except Exception:
        return tk, {}


# ── math ─────────────────────────────────────────────────────────────
def ffill(series, dates):
    """align a sparse series onto the date grid, forward-filled."""
    out, last = [], None
    for d in dates:
        if d in series:
            last = series[d]
        out.append(last)
    return out


def eval_formula(expr, resolved, dates):
    """arithmetic over aligned member series; None where any operand is None."""
    toks = re.findall(r"[A-Z0-9_:.!]+|[+\-*/()]|\d+\.?\d*", expr)
    cols = {}
    for t in toks:
        if OPS_RE.fullmatch(t) or NUM_RE.match(t):
            continue
        k, h = resolve(t)
        key = f"{k}:{h}"
        if key in resolved:
            cols[t] = resolved[key]
    if not cols:
        return None
    out = []
    for i in range(len(dates)):
        e = expr
        ok = True
        for t, col in sorted(cols.items(), key=lambda x: -len(x[0])):
            v = col[i] if i < len(col) else None
            if v is None:
                ok = False
                break
            e = e.replace(t, f"({v})")
        if not ok or re.search(r"[A-Z]", e):
            out.append(None)
            continue
        try:
            val = eval(e, {"__builtins__": {}}, {})  # noqa: S307 — operands
            out.append(float(val) if math.isfinite(val) else None)
        except Exception:
            out.append(None)
    return out


def zscores(vals, win=252):
    out = []
    for i in range(len(vals)):
        if vals[i] is None:
            out.append(None)
            continue
        hist = [v for v in vals[max(0, i - win):i + 1] if v is not None]
        if len(hist) < 40:
            out.append(None)
            continue
        mu = sum(hist) / len(hist)
        sd = (sum((h - mu) ** 2 for h in hist) / (len(hist) - 1)) ** 0.5
        out.append(round((vals[i] - mu) / sd, 3) if sd > 1e-9 else 0.0)
    return out


def fwd_ret(spy, i, h):
    if i + h >= len(spy) or spy[i] is None or spy[i + h] is None:
        return None
    return (spy[i + h] / spy[i] - 1) * 100


def tstat(sample, base):
    n = len(sample)
    if n < 8:
        return 0.0
    mu = sum(sample) / n
    sd = (sum((x - mu) ** 2 for x in sample) / (n - 1)) ** 0.5
    return round((mu - base) / (sd / math.sqrt(n)), 2) if sd > 1e-9 else 0.0


# ── main ─────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    src = s3_get(LISTS_KEY) or {}
    lists = [l for l in (src.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    if not lists:
        s3_put(OUT_KEY, {"generated_at": now.isoformat(),
                         "status": "WAITING_FIRST_SYNC"})
        return {"ok": True, "status": "WAITING_FIRST_SYNC"}

    # 1. resolve every member; keep theses with a real evidence set
    theses = []
    need_fred, need_poly = set(), set()
    for l in lists:
        syms = [s.upper() for s in (l.get("symbols") or [])][:MAX_MEMBERS]
        mem = []
        for s in syms:
            k, h = resolve(s)
            if not k:
                continue
            mem.append((k, h, s))
            if k == "FRED":
                need_fred.add(h)
            elif k == "POLY":
                need_poly.add(h)
            elif k == "FORMULA":
                for o in re.split(r"[+\-*/()]", h):
                    o = o.strip()
                    if not o or NUM_RE.match(o):
                        continue
                    ok, oh = resolve(o)
                    if ok == "FRED":
                        need_fred.add(oh)
                    elif ok == "POLY":
                        need_poly.add(oh)
        cov = len(mem) / max(1, len(syms))
        if len(mem) >= MIN_MEMBERS and cov >= MIN_COVERAGE:
            theses.append({"id": str(l.get("id")), "name": l.get("name"),
                           "members": mem, "n_total": len(syms),
                           "n_resolved": len(mem), "coverage": round(cov, 2)})
    need_poly.add("SPY")
    print(f"[thesis] {len(theses)} theses · {len(need_fred)} FRED · "
          f"{len(need_poly)} POLY")

    # 2. fetch series (state cache keeps daily runs cheap)
    state = s3_get(STATE_KEY, {}, gz=True) or {}
    cache = state.get("series") or {}
    fresh = state.get("as_of") == now.date().isoformat()
    d1 = now.date().isoformat()
    d0 = (now.date() - timedelta(days=LOOKBACK_DAYS)).isoformat()

    if not fresh:
        want_f = [s for s in need_fred if s]
        with ThreadPoolExecutor(max_workers=8) as ex:
            for sid, ser in ex.map(lambda s: fred_series(s, d0), want_f):
                if ser:
                    cache[f"FRED:{sid}"] = ser
        want_p = sorted(need_poly)
        with ThreadPoolExecutor(max_workers=8) as ex:
            for tk, ser in ex.map(lambda t: poly_series(t, d0, d1), want_p):
                if ser:
                    cache[f"POLY:{tk}"] = ser
                if time.time() - t0 > 640:
                    break
        state = {"as_of": d1, "series": cache}
        s3_put(STATE_KEY, state, gz=True)
    print(f"[thesis] series cached: {len(cache)} · {round(time.time()-t0)}s")

    # 3. common date grid = SPY trading days
    spy_ser = cache.get("POLY:SPY") or {}
    dates = sorted(spy_ser.keys())[-520:]
    if len(dates) < 120:
        s3_put(OUT_KEY, {"generated_at": now.isoformat(),
                         "status": "NO_PRICE_GRID"})
        return {"ok": False, "error": "no SPY grid"}
    spy = ffill(spy_ser, dates)

    # 4. per-member z-series (aligned + memoized)
    aligned, zcache = {}, {}
    for key, ser in cache.items():
        aligned[key] = ffill(ser, dates)
    for key, col in aligned.items():
        zcache[key] = zscores(col)

    # 5. per-thesis activation + event study
    base = {h: [r for r in (fwd_ret(spy, i, h) for i in range(len(dates)))
                if r is not None] for h in (5, 21, 63)}
    base_mu = {h: (sum(v) / len(v) if v else 0.0) for h, v in base.items()}

    rows = []
    for th in theses:
        zs = []
        for k, h, orig in th["members"]:
            if k == "FORMULA":
                col = eval_formula(h, aligned, dates)
                if col:
                    zs.append(zscores(col))
            else:
                z = zcache.get(f"{k}:{h}")
                if z:
                    zs.append(z)
        if len(zs) < MIN_MEMBERS:
            continue
        act = []
        for i in range(len(dates)):
            live = [z[i] for z in zs if i < len(z) and z[i] is not None]
            act.append(round(100 * sum(1 for v in live if abs(v) >= Z_FIRE)
                             / len(live), 1) if len(live) >= MIN_MEMBERS
                       else None)
        valid = [a for a in act if a is not None]
        if len(valid) < 120:
            continue
        srt = sorted(valid)
        p80 = srt[int(0.8 * len(srt))]
        cur = act[-1]
        pct_now = round(100 * sum(1 for a in valid if a <= (cur or 0))
                        / len(valid), 1) if cur is not None else None
        study = {}
        for hz in (5, 21, 63):
            sample = [fwd_ret(spy, i, hz) for i in range(len(dates) - hz)
                      if act[i] is not None and act[i] >= p80]
            sample = [s for s in sample if s is not None]
            if len(sample) >= 12:
                mu = sum(sample) / len(sample)
                study[f"d{hz}"] = {
                    "n": len(sample),
                    "spy_fwd_mean_pct": round(mu, 2),
                    "excess_vs_base_pct": round(mu - base_mu[hz], 2),
                    "hit_rate_pct": round(100 * sum(1 for s in sample
                                                    if s > 0) / len(sample), 1),
                    "t_stat": tstat(sample, base_mu[hz]),
                }
        best = max((v.get("t_stat", 0) for v in study.values()),
                   key=abs, default=0)
        rows.append({
            "id": th["id"], "name": th["name"],
            "n_members": len(zs), "n_total": th["n_total"],
            "coverage": th["coverage"],
            "activation_now": cur, "activation_pctile": pct_now,
            "fire_threshold_p80": round(p80, 1),
            "firing": bool(cur is not None and cur >= p80),
            "event_study": study,
            "peak_abs_t": abs(best),
        })

    rows.sort(key=lambda r: -(r["peak_abs_t"] or 0))

    # 6. emit live signals for firing theses with a proven historical edge
    logged = 0
    if rows and (now.weekday() == 0 or (event or {}).get("force_emit")):
        try:
            from decimal import Decimal
            tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
            for r in rows[:15]:
                st = (r["event_study"] or {}).get("d21") or {}
                if not r["firing"] or abs(st.get("t_stat", 0)) < 2 \
                        or st.get("n", 0) < 20:
                    continue
                direction = "DOWN" if st["excess_vs_base_pct"] < 0 else "UP"
                slug = re.sub(r"[^a-z0-9]+", "_",
                              str(r["name"]).lower()).strip("_")[:28] or "thesis"
                tbl.put_item(Item={
                    "signal_id": f"thesis-{slug}#{now.date().isoformat()}",
                    "signal_type": f"thesis_{slug}"[:48],
                    "predicted_direction": direction,
                    "signal_value": str(r["activation_now"]),
                    "confidence": Decimal(str(min(0.75, 0.5 + abs(
                        st["t_stat"]) / 20))),
                    "measure_against": "benchmark_forward_return",
                    "baseline_price": str(spy[-1]),
                    "benchmark": "SPY",
                    "check_windows": ["day_5", "day_21", "day_63"],
                    "outcomes": {}, "accuracy_scores": {},
                    "status": "pending", "logged_at": now.isoformat(),
                    "logged_epoch": int(now.timestamp()),
                    "horizon_days_primary": 21, "schema_version": "2",
                    "ttl": int(now.timestamp()) + 120 * 86400,
                    "metadata": {"engine": "thesis-engine",
                                 "thesis": r["name"],
                                 "activation_pctile": r["activation_pctile"],
                                 "hist_t": st["t_stat"], "hist_n": st["n"]},
                    "rationale": (f"Khalid thesis '{r['name']}' firing "
                                  f"({r['activation_now']}% of members at "
                                  f"|z|>=1.5, {r['activation_pctile']}th pct). "
                                  f"History: SPY 21d excess "
                                  f"{st['excess_vs_base_pct']}% "
                                  f"(t={st['t_stat']}, n={st['n']})"),
                })
                logged += 1
        except Exception as e:
            print(f"[thesis] emit failed: {str(e)[:140]}")

    doc = {"generated_at": now.isoformat(), "version": "1.0", "status": "LIVE",
           "n_theses": len(rows), "signals_logged": logged,
           "spy_base_rates_pct": {f"d{h}": round(v, 2)
                                  for h, v in base_mu.items()},
           "method": ("each watchlist = a thesis; members z-scored vs 252-obs "
                      "history; activation = % of members at |z|>=1.5; days in "
                      "the top activation quintile are event-studied against "
                      "forward SPY returns (excess vs base rate, t-stat)"),
           "theses": rows, "elapsed_s": round(time.time() - t0, 1)}
    s3_put(OUT_KEY, doc)
    print(json.dumps({"ok": True, "n_theses": len(rows),
                      "signals_logged": logged,
                      "elapsed": doc["elapsed_s"]}))
    return {"ok": True, "n_theses": len(rows), "signals_logged": logged}
