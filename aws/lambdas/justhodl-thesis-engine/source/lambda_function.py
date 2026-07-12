"""justhodl-thesis-engine v2.0 — ops 3168 (DEEP HISTORY, 1990-2026).

v1 ran on ~2 years (Polygon caps at 5y): n_eff ~5 per thesis, so nothing
could clear FDR — an honest null, but a powerless one.

v2 rebuilds on 36 years via aws/shared/series_source.py:
  FRED    US + OECD macro (1990+; one template covers ~50 countries)
  MARKET  Yahoo -> Stooq -> Polygon chain (S&P/DXY/VIX/Nikkei all 1990)
  data/symbol-map.json (ops 3167) supplies the resolved source per symbol.

Grid is WEEKLY (~1,900 weeks vs 500 days) because Khalid's members are
mostly weekly/monthly macro. Forward SPY horizons 4/13/26 weeks, with:
  · overlap-corrected t  (n_eff = n / horizon — daily/weekly sampling of
    h-period forward returns shares h-1 periods between neighbours)
  · hit-edge vs SPY's OWN base rate
  · Benjamini-Hochberg FDR q=0.10 across every thesis tested
Only FDR survivors that are firing today emit into justhodl-signals.

Output: data/thesis-engine.json   State: data/thesis-state-v2.json.gz
"""

import gzip
import io
import json
import math
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import series_source as SS  # bundled from aws/shared by the deploy helper

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/thesis-engine.json"
STATE_KEY = "data/thesis-state-v2.json.gz"
LISTS_KEY = "data/tv-watchlists.json"
MAP_KEY = "data/symbol-map.json"

START = "1990-01-01"

# Families are defined by KHALID'S OWN NAMING — never by the signs the
# study produced. Selecting a family because its members "looked
# negative" would be fitting the noise we just measured (ops 3169).
FAMILIES = [
    ("STRESS", ("crisis", "stress", "danger", "plumbing", "black swan",
                "recession", "distress", "default", "contagion", "risk")),
    ("LIQUIDITY", ("liquidity", "m2", "balance sheet", "repo", "draining",
                   "money supply", "reserve", "qe", "qt", "printing")),
    ("CREDIT", ("credit", "spread", "yield", "bond", "corp", "junk",
                "high yield", "ig", "hy")),
    ("GROWTH", ("economy", "employment", "consumer", "business cycle",
                "gdp", "pmi", "manufactur", "retail", "industrial", "job")),
    ("INFLATION", ("inflation", "cpi", "ppi", "food", "commodit", "price")),
    ("DOLLAR", ("dxy", "dollar", "currenc", "eurodollar", "fx", "forex")),
    ("BREADTH", ("breadth", "equity", "stock", "indices", "index",
                 "sector", "market")),
    ("CRYPTO", ("bitcoin", "crypto", "btc")),
]


def family_of(name):
    n = str(name or "").lower()
    for fam, keys in FAMILIES:
        if any(k in n for k in keys):
            return fam
    return None
MIN_MEMBERS = 8
MAX_MEMBERS = 60
MIN_COVERAGE = 0.45
Z_FIRE = 1.5
Z_WIN = 156                 # 3y rolling window on the weekly grid
HORIZONS = (4, 13, 26)      # weeks
FETCH_BUDGET_S = 620

S3 = boto3.client("s3", region_name="us-east-1")


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


# ── weekly grid ──────────────────────────────────────────────────────
def week_key(iso):
    y, m, d = (int(x) for x in iso[:10].split("-"))
    iso_y, iso_w, _ = date(y, m, d).isocalendar()
    return f"{iso_y}-{iso_w:02d}"


def to_weekly(series):
    """last observation of each ISO week."""
    out = {}
    for d, v in sorted(series.items()):
        out[week_key(d)] = v
    return out


def ffill(weekly, grid):
    out, last = [], None
    for w in grid:
        if w in weekly:
            last = weekly[w]
        out.append(last)
    return out


# ── stats ────────────────────────────────────────────────────────────
def zscores(vals, win=Z_WIN):
    out = []
    for i in range(len(vals)):
        if vals[i] is None:
            out.append(None)
            continue
        hist = [v for v in vals[max(0, i - win):i + 1] if v is not None]
        if len(hist) < 30:
            out.append(None)
            continue
        mu = sum(hist) / len(hist)
        sd = (sum((h - mu) ** 2 for h in hist) / (len(hist) - 1)) ** 0.5
        out.append(round((vals[i] - mu) / sd, 3) if sd > 1e-9 else 0.0)
    return out


def fwd(spy, i, h):
    if i + h >= len(spy) or not spy[i] or not spy[i + h]:
        return None
    return (spy[i + h] / spy[i] - 1) * 100


def tstat(sample, base, horizon=1):
    n = len(sample)
    if n < 8:
        return 0.0
    mu = sum(sample) / n
    sd = (sum((x - mu) ** 2 for x in sample) / (n - 1)) ** 0.5
    if sd <= 1e-9:
        return 0.0
    n_eff = max(4.0, n / max(1, horizon))
    return round((mu - base) / (sd / math.sqrt(n_eff)), 2)


def norm_p(t):
    return min(1.0, max(1e-9, math.erfc(abs(t) / math.sqrt(2))))


def bh_fdr(pvals, q=0.10):
    idx = sorted(range(len(pvals)), key=lambda i: pvals[i])
    m, keep = len(pvals), set()
    for rank, i in enumerate(idx, 1):
        if pvals[i] <= q * rank / m:
            keep = set(idx[:rank])
    return keep


def eval_formula(expr, cols, grid_len):
    toks = [t for t in re.findall(r"[A-Z0-9_:.!^=\-]+", expr)
            if t in cols]
    if not toks:
        return None
    out = []
    for i in range(grid_len):
        e = expr
        ok = True
        for t in sorted(toks, key=len, reverse=True):
            v = cols[t][i] if i < len(cols[t]) else None
            if v is None:
                ok = False
                break
            e = e.replace(t, f"({v})")
        if not ok or re.search(r"[A-Z]{2,}", e):
            out.append(None)
            continue
        try:
            val = eval(e, {"__builtins__": {}}, {})  # operands only
            out.append(float(val) if math.isfinite(val) else None)
        except Exception:
            out.append(None)
    return out


# ── main ─────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    src = s3_get(LISTS_KEY) or {}
    lists = [l for l in (src.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    smap = (s3_get(MAP_KEY) or {}).get("map") or {}
    if not lists or not smap:
        s3_put(OUT_KEY, {"generated_at": now.isoformat(),
                         "status": "WAITING_MAP"})
        return {"ok": False, "status": "WAITING_MAP"}

    # 1. theses + the series they need
    theses, need = [], {}
    for l in lists:
        syms = [s.upper() for s in (l.get("symbols") or [])][:MAX_MEMBERS]
        mem = [s for s in syms if s in smap]
        if len(mem) < MIN_MEMBERS or len(mem) / max(1, len(syms)) < MIN_COVERAGE:
            continue
        theses.append({"id": str(l.get("id")), "name": l.get("name"),
                       "members": mem, "n_total": len(syms),
                       "coverage": round(len(mem) / len(syms), 2)})
        for s in mem:
            m = smap[s]
            if m["source"] == "FORMULA":
                for o in re.split(r"[+\-*/()]", m["id"]):
                    o = o.strip().upper()
                    if o and o in smap and smap[o]["source"] != "FORMULA":
                        need[o] = smap[o]
            else:
                need[s] = m
    need["__SPY__"] = {"source": "MARKET", "id": "SPY"}
    print(f"[thesis2] {len(theses)} theses · {len(need)} series to fetch")

    # 2. fetch (state cache; weekly to keep memory sane)
    state = s3_get(STATE_KEY, {}, gz=True) or {}
    cache = state.get("weekly") or {}
    fresh_cut = (now - timedelta(days=6)).isoformat()
    todo = [k for k, m in need.items()
            if k not in cache or state.get("stamp", "") < fresh_cut]
    fetched = 0

    def pull(item):
        k, m = item
        ser = SS.fetch(m["source"], m["id"], START)
        return k, (to_weekly(ser) if ser else {})

    if todo:
        with ThreadPoolExecutor(max_workers=10) as ex:
            for k, w in ex.map(pull, [(k, need[k]) for k in todo]):
                if w:
                    cache[k] = w
                    fetched += 1
                if time.time() - t0 > FETCH_BUDGET_S:
                    break
        state = {"stamp": now.isoformat(), "weekly": cache}
        s3_put(STATE_KEY, state, gz=True)
    print(f"[thesis2] cache={len(cache)} fetched={fetched} "
          f"{round(time.time()-t0)}s")

    # 3. weekly grid from SPY
    spy_w = cache.get("__SPY__") or {}
    if len(spy_w) < 300:
        s3_put(OUT_KEY, {"generated_at": now.isoformat(),
                         "status": "NO_BENCHMARK",
                         "spy_weeks": len(spy_w)})
        return {"ok": False, "error": "spy history missing"}
    grid = sorted(spy_w.keys())
    spy = ffill(spy_w, grid)

    aligned = {k: ffill(w, grid) for k, w in cache.items()}
    zc = {k: zscores(col) for k, col in aligned.items()}

    base = {h: [r for r in (fwd(spy, i, h) for i in range(len(grid)))
                if r is not None] for h in HORIZONS}
    base_mu = {h: (sum(v) / len(v) if v else 0.0) for h, v in base.items()}
    base_hit = {h: (100 * sum(1 for x in v if x > 0) / len(v) if v else 0.0)
                for h, v in base.items()}

    # 4. per-thesis activation + event study
    rows, act_series = [], {}
    for th in theses:
        zs = []
        for s in th["members"]:
            m = smap[s]
            if m["source"] == "FORMULA":
                col = eval_formula(m["id"], aligned, len(grid))
                if col:
                    zs.append(zscores(col))
            elif s in zc:
                zs.append(zc[s])
        if len(zs) < MIN_MEMBERS:
            continue
        act = []
        for i in range(len(grid)):
            live = [z[i] for z in zs if i < len(z) and z[i] is not None]
            act.append(round(100 * sum(1 for v in live if abs(v) >= Z_FIRE)
                             / len(live), 1)
                       if len(live) >= MIN_MEMBERS else None)
        valid = [a for a in act if a is not None]
        if len(valid) < 150:                     # need real history
            continue
        srt = sorted(valid)
        p80 = srt[int(0.8 * len(srt))]
        cur = act[-1]
        pct_now = (round(100 * sum(1 for a in valid if a <= cur) / len(valid), 1)
                   if cur is not None else None)
        study = {}
        for h in HORIZONS:
            samp = [fwd(spy, i, h) for i in range(len(grid) - h)
                    if act[i] is not None and act[i] >= p80]
            samp = [x for x in samp if x is not None]
            if len(samp) >= 20:
                mu = sum(samp) / len(samp)
                hit = 100 * sum(1 for x in samp if x > 0) / len(samp)
                t = tstat(samp, base_mu[h], horizon=h)
                study[f"w{h}"] = {
                    "n": len(samp), "n_effective": round(len(samp) / h, 1),
                    "spy_fwd_mean_pct": round(mu, 2),
                    "excess_vs_base_pct": round(mu - base_mu[h], 2),
                    "hit_rate_pct": round(hit, 1),
                    "base_hit_rate_pct": round(base_hit[h], 1),
                    "hit_edge_pp": round(hit - base_hit[h], 1),
                    "t_stat": t, "p_value": round(norm_p(t), 4),
                }
        if not study:
            continue
        key = study.get("w13") or list(study.values())[0]
        act_series[str(th["id"])] = act
        rows.append({
            "family": family_of(th["name"]),
            "id": th["id"], "name": th["name"], "n_members": len(zs),
            "n_total": th["n_total"], "coverage": th["coverage"],
            "history_weeks": len(valid),
            "history_from": grid[len(grid) - len(valid)] if valid else None,
            "activation_now": cur, "activation_pctile": pct_now,
            "fire_threshold_p80": round(p80, 1),
            "firing": bool(cur is not None and cur >= p80),
            "event_study": study,
            "peak_abs_t": abs(key.get("t_stat", 0)),
            # back-compat with the page's d21 column
            "event_study_d21": key,
        })

    # ── 4b. FAMILY COMPOSITES (ops 3169) ────────────────────────────
    # Nine stress panels each showed a negative forward-SPY tilt but none
    # survived FDR alone: classic weak-correlated-signal aggregation.
    # Pooling them into one composite raises power AND collapses the
    # multiple-testing penalty from 56 tests to ~8.
    def study_series(sig, thr_pct=80):
        """event-study any 0-100 activation-like series against SPY."""
        v = [x for x in sig if x is not None]
        if len(v) < 200:
            return None, None
        thr = sorted(v)[int(thr_pct / 100 * len(v))]
        out = {}
        for h in HORIZONS:
            samp = [fwd(spy, i, h) for i in range(len(grid) - h)
                    if sig[i] is not None and sig[i] >= thr]
            samp = [x for x in samp if x is not None]
            if len(samp) >= 20:
                mu = sum(samp) / len(samp)
                hit = 100 * sum(1 for x in samp if x > 0) / len(samp)
                t = tstat(samp, base_mu[h], horizon=h)
                out[f"w{h}"] = {
                    "n": len(samp), "n_effective": round(len(samp) / h, 1),
                    "excess_vs_base_pct": round(mu - base_mu[h], 2),
                    "hit_rate_pct": round(hit, 1),
                    "hit_edge_pp": round(hit - base_hit[h], 1),
                    "t_stat": t, "p_value": round(norm_p(t), 4)}
        return out, thr

    def half_study(sig, half):
        """same study restricted to the first/second half of history —
        an edge that only exists in one half is a period artefact."""
        cut = len(grid) // 2
        lo, hi = (0, cut) if half == 1 else (cut, len(grid))
        v = [x for x in sig[lo:hi] if x is not None]
        if len(v) < 80:
            return None
        thr = sorted(v)[int(0.8 * len(v))]
        h = 13
        samp = [fwd(spy, i, h) for i in range(lo, min(hi, len(grid) - h))
                if sig[i] is not None and sig[i] >= thr]
        samp = [x for x in samp if x is not None]
        if len(samp) < 15:
            return None
        mu = sum(samp) / len(samp)
        return {"n": len(samp), "n_effective": round(len(samp) / h, 1),
                "excess_vs_base_pct": round(mu - base_mu[h], 2),
                "t_stat": tstat(samp, base_mu[h], horizon=h)}

    families = []
    for fam, _ in FAMILIES:
        mem = [r for r in rows if r.get("family") == fam]
        if len(mem) < 3:
            continue
        # z-score each member's activation on a rolling window, then average:
        # panels fire at different natural rates, z puts them on one scale
        zacts = [zscores(act_series[r["id"]]) for r in mem
                 if r["id"] in act_series]
        comp = []
        for i in range(len(grid)):
            live = [z[i] for z in zacts if i < len(z) and z[i] is not None]
            comp.append(round(sum(live) / len(live), 3)
                        if len(live) >= max(3, len(zacts) // 2) else None)
        st, thr = study_series(comp)
        if not st:
            continue
        # sign test across members: how many tilt the same way?
        signs = [1 if (r["event_study"].get("w13", {})
                       .get("excess_vs_base_pct", 0)) < 0 else 0 for r in mem]
        k, n = sum(signs), len(signs)
        maj = max(k, n - k)
        # two-sided binomial tail under p=0.5
        p_sign = min(1.0, 2 * sum(math.comb(n, j) for j in range(maj, n + 1))
                     / (2 ** n))
        families.append({
            "family": fam, "n_theses": len(mem),
            "members": [r["name"] for r in mem][:12],
            "composite_now": comp[-1],
            "fire_threshold": round(thr, 3),
            "firing": bool(comp[-1] is not None and comp[-1] >= thr),
            "event_study": st,
            "half1": half_study(comp, 1), "half2": half_study(comp, 2),
            "sign_test": {"n_theses": n, "n_negative": k,
                          "p_value": round(p_sign, 4)},
            "peak_abs_t": abs((st.get("w13") or {}).get("t_stat", 0)),
        })
    fpv = [(f["event_study"].get("w13") or {}).get("p_value", 1.0)
           for f in families]
    fsurv = bh_fdr(fpv, q=0.10) if fpv else set()
    for i, f in enumerate(families):
        f["fdr_pass"] = i in fsurv
        h1 = (f.get("half1") or {}).get("excess_vs_base_pct")
        h2 = (f.get("half2") or {}).get("excess_vs_base_pct")
        f["stable"] = bool(h1 is not None and h2 is not None
                           and (h1 < 0) == (h2 < 0))
    families.sort(key=lambda f: (not f["fdr_pass"], -f["peak_abs_t"]))

    pv = [r["event_study"].get("w13", {}).get("p_value", 1.0) for r in rows]
    surv = bh_fdr(pv, q=0.10) if pv else set()
    for i, r in enumerate(rows):
        r["fdr_pass"] = i in surv
    rows.sort(key=lambda r: (not r["fdr_pass"], -r["peak_abs_t"]))

    # 5. emit — FDR survivors that are firing now
    logged = 0
    if rows and (now.weekday() == 0 or (event or {}).get("force_emit")):
        try:
            from decimal import Decimal
            tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
            for r in rows:
                st = r["event_study"].get("w13") or {}
                if not (r["firing"] and r["fdr_pass"]
                        and abs(st.get("t_stat", 0)) >= 2
                        and st.get("n_effective", 0) >= 8):
                    continue
                direction = "DOWN" if st["excess_vs_base_pct"] < 0 else "UP"
                slug = re.sub(r"[^a-z0-9]+", "_",
                              str(r["name"]).lower()).strip("_")[:28] or "thesis"
                tbl.put_item(Item={
                    "signal_id": f"thesis-{slug}#{now.date().isoformat()}",
                    "signal_type": f"thesis_{slug}"[:48],
                    "predicted_direction": direction,
                    "signal_value": str(r["activation_now"]),
                    "confidence": Decimal(str(round(min(0.75, 0.5 + abs(
                        st["t_stat"]) / 20), 3))),
                    "measure_against": "benchmark_forward_return",
                    "baseline_price": str(spy[-1]), "benchmark": "SPY",
                    "check_windows": ["day_21", "day_63", "day_126"],
                    "outcomes": {}, "accuracy_scores": {},
                    "status": "pending", "logged_at": now.isoformat(),
                    "logged_epoch": int(now.timestamp()),
                    "horizon_days_primary": 63, "schema_version": "2",
                    "ttl": int(now.timestamp()) + 200 * 86400,
                    "metadata": {"engine": "thesis-engine-v2",
                                 "thesis": r["name"],
                                 "hist_t": st["t_stat"],
                                 "hist_n_eff": st["n_effective"],
                                 "history_from": r["history_from"]},
                    "rationale": (f"'{r['name']}' firing "
                                  f"({r['activation_now']}% of members |z|>=1.5, "
                                  f"{r['activation_pctile']}th pct). Since "
                                  f"{r['history_from']}: SPY 13w excess "
                                  f"{st['excess_vs_base_pct']}% "
                                  f"(t={st['t_stat']}, n_eff={st['n_effective']})"),
                })
                logged += 1
        except Exception as e:
            print(f"[thesis2] emit failed: {str(e)[:140]}")

    # composite signals — the aggregated, FDR-and-stability-screened ones
    for f in families:
        st = f["event_study"].get("w13") or {}
        if not (f["firing"] and f["fdr_pass"] and f["stable"]
                and abs(st.get("t_stat", 0)) >= 2):
            continue
        try:
            from decimal import Decimal
            tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
            direction = "DOWN" if st["excess_vs_base_pct"] < 0 else "UP"
            tbl.put_item(Item={
                "signal_id": f"thesisfam-{f['family'].lower()}#"
                             f"{now.date().isoformat()}",
                "signal_type": f"thesis_family_{f['family'].lower()}"[:48],
                "predicted_direction": direction,
                "signal_value": str(f["composite_now"]),
                "confidence": Decimal(str(round(min(0.8, 0.55 + abs(
                    st["t_stat"]) / 20), 3))),
                "measure_against": "benchmark_forward_return",
                "baseline_price": str(spy[-1]), "benchmark": "SPY",
                "check_windows": ["day_21", "day_63", "day_126"],
                "outcomes": {}, "accuracy_scores": {}, "status": "pending",
                "logged_at": now.isoformat(),
                "logged_epoch": int(now.timestamp()),
                "horizon_days_primary": 63, "schema_version": "2",
                "ttl": int(now.timestamp()) + 200 * 86400,
                "metadata": {"engine": "thesis-engine-v2",
                             "family": f["family"],
                             "n_theses": f["n_theses"],
                             "hist_t": st["t_stat"],
                             "half1": (f.get("half1") or {}).get("t_stat"),
                             "half2": (f.get("half2") or {}).get("t_stat")},
                "rationale": (f"{f['family']} composite firing "
                              f"({f['n_theses']} of Khalid's panels pooled). "
                              f"Since 1990: SPY 13w "
                              f"{st['excess_vs_base_pct']}% vs base, "
                              f"t={st['t_stat']}, stable across both halves"),
            })
            logged += 1
        except Exception as e:
            print(f"[thesis2] family emit failed: {str(e)[:120]}")

    doc = {"generated_at": now.isoformat(), "version": "2.1", "status": "LIVE",
           "families": families,
           "history_start": START, "grid": "weekly",
           "n_weeks": len(grid), "n_theses": len(rows),
           "n_fdr_survivors": sum(1 for r in rows if r["fdr_pass"]),
           "signals_logged": logged, "series_cached": len(cache),
           "spy_base_rates_pct": {f"w{h}": round(base_mu[h], 2)
                                  for h in HORIZONS},
           "spy_base_rates_pct_compat": {"d5": round(base_mu[4], 2),
                                         "d21": round(base_mu[13], 2),
                                         "d63": round(base_mu[26], 2)},
           "method": ("each watchlist = a thesis. Members resolved to FREE "
                      "deep sources (FRED 1990+, Yahoo/Stooq market chain), "
                      "z-scored on a 3y rolling window over a WEEKLY grid "
                      "back to 1990; activation = % of members at |z|>=1.5; "
                      "top-quintile activation weeks are event-studied vs "
                      "forward SPY (4/13/26w). t is OVERLAP-CORRECTED "
                      "(n_eff = n/horizon) and screened by Benjamini-Hochberg "
                      "FDR q=0.10 across all theses. Only FDR survivors emit "
                      "signals."),
           "theses": rows, "elapsed_s": round(time.time() - t0, 1)}
    s3_put(OUT_KEY, doc)
    print(json.dumps({"ok": True, "n_theses": len(rows),
                      "fdr": doc["n_fdr_survivors"], "weeks": len(grid),
                      "signals": logged, "elapsed": doc["elapsed_s"]}))
    return {"ok": True, "n_theses": len(rows),
            "fdr_survivors": doc["n_fdr_survivors"], "signals_logged": logged}
