"""justhodl-wl-engines v1.0 — WATCHLIST ENGINE FRAMEWORK (ops 3176).

Khalid: "every single one of my watchlists should be a separate engine."

Correct — but 207 Lambdas would re-fetch the same series 207 times (SPX,
DXY, FEDFUNDS live in dozens of his lists), blow the saturated EventBridge
rule cap, and hand him 207 maintenance liabilities. So: 207 FIRST-CLASS
ENGINES on ONE multi-tenant runtime, which is how a signal desk actually
runs hundreds of signals.

Each watchlist gets everything that makes an engine an engine:
  · engine_id            wl-<slug>          (stable, registry-visible)
  · own output feed      data/engines/wl-<slug>.json
  · own state           FIRING / QUIET + activation percentile
  · own members         every indicator, with its CURRENT z and whether
                        it is one of the ones lighting the panel up
  · own signal_type     wl_<slug> → outcome-checker grades it → the
                        scorecard grows ONE ROW PER WATCHLIST, so within
                        weeks we know which of HIS engines has alpha
  · own fusion hooks    theme + regime tags that existing engines read

Performance: rolling z-scores are computed ONCE PER SYMBOL in O(n) with
running sums (naive per-member recomputation would be ~800M ops and time
out), stored as array('f') to keep 2,900 symbols x 1,746 weeks in ~20MB.
"""

import gzip
import io
import json
import math
import os
import re
import sys
import time
from array import array
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import series_source as SS  # bundled from aws/shared

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
LISTS_KEY = "data/tv-watchlists.json"
MAP_KEY = "data/symbol-map.json"
STATE_KEY = "data/thesis-state-v2.json.gz"      # shared cache with thesis-engine
INDEX_KEY = "data/wl-engines.json"
ENGINE_PREFIX = "data/engines/"

START = "1990-01-01"
Z_WIN = 156
Z_FIRE = 1.5
MIN_MEMBERS = 6
MAX_MEMBERS = 120
HORIZONS = (4, 13, 26)
FETCH_BUDGET_S = 560

S3 = boto3.client("s3", region_name="us-east-1")

THEME_KEYS = [
    ("STRESS", ("crisis", "stress", "danger", "plumbing", "black swan",
                "recession", "distress", "default", "contagion")),
    ("LIQUIDITY", ("liquidity", "m2", "balance sheet", "repo", "draining",
                   "money supply", "reserve", "qe", "qt", "printing")),
    ("CREDIT", ("credit", "spread", "yield", "bond", "corp", "junk", "hy")),
    ("GROWTH", ("economy", "employment", "consumer", "business cycle",
                "gdp", "pmi", "manufactur", "retail", "job", "industrial")),
    ("INFLATION", ("inflation", "cpi", "ppi", "food", "commodit", "price")),
    ("DOLLAR", ("dxy", "dollar", "currenc", "eurodollar", "fx", "forex")),
    ("BREADTH", ("breadth", "equity", "stock", "indices", "index", "sector")),
    ("CRYPTO", ("bitcoin", "crypto", "btc")),
    ("RATES", ("fed", "rate", "curve", "treasury", "bund", "jgb")),
]

# which platform engines each theme should ENHANCE (phase-2 fusion hooks)
FUSION_TARGETS = {
    "STRESS": ["justhodl-regime-composite", "justhodl-crisis-composite",
               "justhodl-roro", "justhodl-alpha-compass"],
    "LIQUIDITY": ["justhodl-global-liquidity", "justhodl-fed-liquidity",
                  "justhodl-dollar-radar"],
    "CREDIT": ["justhodl-best-setups", "justhodl-credit-danger",
               "justhodl-master-ranker"],
    "GROWTH": ["justhodl-cycle-clock", "justhodl-macro-nowcast"],
    "INFLATION": ["justhodl-macro-nowcast", "justhodl-cycle-clock"],
    "DOLLAR": ["justhodl-dollar-radar", "justhodl-regime-composite"],
    "BREADTH": ["justhodl-breadth-thrust", "justhodl-equity-confluence"],
    "CRYPTO": ["justhodl-crypto-liquidity", "justhodl-crypto-emergence"],
    "RATES": ["justhodl-fed-liquidity", "justhodl-eurodollar-plumbing"],
}


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


def slug(name, used):
    s = re.sub(r"[^a-z0-9]+", "-", str(name).lower()).strip("-")[:34] or "list"
    base, i = s, 2
    while s in used:
        s = f"{base}-{i}"
        i += 1
    used.add(s)
    return s


def theme_of(name):
    n = str(name or "").lower()
    for th, keys in THEME_KEYS:
        if any(k in n for k in keys):
            return th
    return "OTHER"


def week_key(iso):
    y, m, d = (int(x) for x in iso[:10].split("-"))
    iy, iw, _ = date(y, m, d).isocalendar()
    return f"{iy}-{iw:02d}"


def to_weekly(series):
    out = {}
    for d, v in sorted(series.items()):
        out[week_key(d)] = v
    return out


def align(weekly, grid):
    """forward-filled float array on the weekly grid (NaN where unknown)."""
    out = array("f", [float("nan")]) * 0
    last = float("nan")
    for w in grid:
        if w in weekly:
            last = float(weekly[w])
        out.append(last)
    return out


def rolling_z(vals, win=Z_WIN):
    """O(n) rolling z with running sums — the naive O(n*win) version is
    ~800M operations across 2,900 symbols and times the lambda out."""
    n = len(vals)
    out = array("f", [float("nan")]) * 0
    s = s2 = 0.0
    cnt = 0
    from collections import deque
    dq = deque()
    for i in range(n):
        v = vals[i]
        if v == v:                       # not NaN
            dq.append(v)
            s += v
            s2 += v * v
            cnt += 1
            if cnt > win:
                old = dq.popleft()
                s -= old
                s2 -= old * old
                cnt -= 1
        if cnt < 30 or v != v:
            out.append(float("nan"))
            continue
        mu = s / cnt
        var = max(0.0, (s2 / cnt) - mu * mu) * (cnt / max(1, cnt - 1))
        sd = math.sqrt(var)
        out.append((v - mu) / sd if sd > 1e-9 else 0.0)
    return out


def fwd(spy, i, h):
    if i + h >= len(spy):
        return None
    a, b = spy[i], spy[i + h]
    if a != a or b != b or not a:
        return None
    return (b / a - 1) * 100


def tstat(sample, base, horizon=1):
    n = len(sample)
    if n < 8:
        return 0.0
    mu = sum(sample) / n
    sd = (sum((x - mu) ** 2 for x in sample) / (n - 1)) ** 0.5
    if sd <= 1e-9:
        return 0.0
    return round((mu - base) / (sd / math.sqrt(max(4.0, n / max(1, horizon)))), 2)


def norm_p(t):
    return min(1.0, max(1e-9, math.erfc(abs(t) / math.sqrt(2))))


def bh_fdr(pv, q=0.10):
    idx = sorted(range(len(pv)), key=lambda i: pv[i])
    m, keep = len(pv), set()
    for rank, i in enumerate(idx, 1):
        if pv[i] <= q * rank / m:
            keep = set(idx[:rank])
    return keep


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    src = s3_get(LISTS_KEY) or {}
    lists = [l for l in (src.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    smap = (s3_get(MAP_KEY) or {}).get("map") or {}
    if not lists or not smap:
        return {"ok": False, "error": "waiting for watchlists / symbol map"}

    # ── 1. SPEC per watchlist: this is the engine definition ─────────
    used, specs, need = set(), [], {}
    for l in lists:
        syms = [s.upper() for s in (l.get("symbols") or [])][:MAX_MEMBERS]
        resolved = [s for s in syms if s in smap]
        sl = slug(l.get("name") or l.get("id"), used)
        specs.append({
            "engine_id": f"wl-{sl}",
            "name": l.get("name"),
            "tv_id": str(l.get("id")),
            "theme": theme_of(l.get("name")),
            "members_total": len(l.get("symbols") or []),
            "members_resolved": resolved,
            "coverage": round(len(resolved) / max(1, len(syms)), 2),
            "state": "ACTIVE" if len(resolved) >= MIN_MEMBERS else "DORMANT",
        })
        for s in resolved:
            m = smap[s]
            if m["source"] != "FORMULA":
                need[s] = m
    need["__SPY__"] = {"source": "MARKET", "id": "SPY"}
    active = [sp for sp in specs if sp["state"] == "ACTIVE"]
    print(f"[wl] {len(specs)} engines ({len(active)} ACTIVE) · "
          f"{len(need)} unique series")

    # ── 2. shared series cache (the whole point of one runtime) ──────
    state = s3_get(STATE_KEY, {}, gz=True) or {}
    cache = state.get("weekly") or {}
    fresh_cut = (now - timedelta(days=6)).isoformat()
    ser = SS.fetch("MARKET", "SPY", START)
    if ser:
        cache["__SPY__"] = to_weekly(ser)
    todo = [k for k in need
            if k != "__SPY__"
            and (k not in cache or state.get("stamp", "") < fresh_cut)]
    fetched = 0
    if todo:
        def pull(k):
            m = need[k]
            s = SS.fetch(m["source"], m["id"], START)
            return k, (to_weekly(s) if s else {})
        with ThreadPoolExecutor(max_workers=10) as ex:
            for k, w in ex.map(pull, todo):
                if w:
                    cache[k] = w
                    fetched += 1
                if time.time() - t0 > FETCH_BUDGET_S:
                    break
        state = {"stamp": now.isoformat(), "weekly": cache}
        s3_put(STATE_KEY, state, gz=True)
    print(f"[wl] cache={len(cache)} new={fetched} {round(time.time()-t0)}s")

    spy_w = cache.get("__SPY__") or {}
    if len(spy_w) < 300:
        return {"ok": False, "error": "no SPY grid"}
    grid = sorted(spy_w.keys())
    spy = align(spy_w, grid)

    # ── 3. z ONCE PER SYMBOL, on its NATIVE observations ─────────────
    # ops 3177 correctness fix: World Bank series are ANNUAL. Forward-
    # filling them onto a weekly grid and then z-scoring over a 156-week
    # window means the window holds ~3 distinct numbers — garbage z. So
    # z is computed on the symbol's OWN observation sequence (an annual
    # series is scored against its own decades of history) and only then
    # projected onto the weekly grid.
    zc = {}
    for k, w in cache.items():
        if k not in need and k != "__SPY__":
            continue
        obs = sorted(w.keys())
        if len(obs) < 12:
            continue
        vals = array("f", [float(w[o]) for o in obs])
        zs = rolling_z(vals, win=min(Z_WIN, max(12, len(vals))))
        zmap = {obs[i]: zs[i] for i in range(len(obs))}
        zc[k] = align(zmap, grid)          # forward-fill the Z, not the level
    print(f"[wl] z-scored {len(zc)} symbols (native-frequency) in "
          f"{round(time.time()-t0)}s")

    base = {h: [r for r in (fwd(spy, i, h) for i in range(len(grid)))
                if r is not None] for h in HORIZONS}
    base_mu = {h: (sum(v) / len(v) if v else 0.0) for h, v in base.items()}
    base_hit = {h: (100 * sum(1 for x in v if x > 0) / len(v) if v else 0.0)
                for h, v in base.items()}

    # ── 4. run every engine ─────────────────────────────────────────
    rows, pvals = [], []
    for sp in specs:
        if sp["state"] != "ACTIVE":
            rows.append({**{k: sp[k] for k in
                            ("engine_id", "name", "theme", "tv_id",
                             "coverage", "state")},
                         "members_resolved": len(sp["members_resolved"]),
                         "members_total": sp["members_total"],
                         "reason": "needs >=6 members on a free source — "
                                   "map more of its indicators to activate"})
            continue
        zs = [(s, zc[s]) for s in sp["members_resolved"] if s in zc]
        if len(zs) < MIN_MEMBERS:
            sp["state"] = "DORMANT"
            continue
        act = array("f", [float("nan")]) * 0
        for i in range(len(grid)):
            live = [z[i] for _, z in zs if z[i] == z[i]]
            act.append(100.0 * sum(1 for v in live if abs(v) >= Z_FIRE)
                       / len(live) if len(live) >= MIN_MEMBERS
                       else float("nan"))
        valid = [a for a in act if a == a]
        if len(valid) < 100:
            sp["state"] = "DORMANT"
            continue
        srt = sorted(valid)
        p80 = srt[int(0.8 * len(srt))]
        cur = act[-1] if act[-1] == act[-1] else None
        pct = (round(100 * sum(1 for a in valid if a <= cur) / len(valid), 1)
               if cur is not None else None)

        # WHICH indicators are lighting it up right now — the thing a
        # trader actually needs to see
        lit = sorted(((s, round(z[-1], 2)) for s, z in zs
                      if z[-1] == z[-1] and abs(z[-1]) >= Z_FIRE),
                     key=lambda x: -abs(x[1]))[:12]

        study = {}
        for h in HORIZONS:
            samp = [fwd(spy, i, h) for i in range(len(grid) - h)
                    if act[i] == act[i] and act[i] >= p80]
            samp = [x for x in samp if x is not None]
            if len(samp) >= 20:
                mu = sum(samp) / len(samp)
                hit = 100 * sum(1 for x in samp if x > 0) / len(samp)
                t = tstat(samp, base_mu[h], horizon=h)
                study[f"w{h}"] = {
                    "n": len(samp), "n_effective": round(len(samp) / h, 1),
                    "excess_vs_base_pct": round(mu - base_mu[h], 2),
                    "hit_edge_pp": round(hit - base_hit[h], 1),
                    "t_stat": t, "p_value": round(norm_p(t), 4)}
        w13 = study.get("w13") or {}
        pvals.append(w13.get("p_value", 1.0))

        doc = {
            "engine_id": sp["engine_id"], "name": sp["name"],
            "theme": sp["theme"], "tv_id": sp["tv_id"],
            "generated_at": now.isoformat(), "state": "ACTIVE",
            "history_from": grid[0], "n_weeks": len(valid),
            "members_total": sp["members_total"],
            "members_resolved": len(zs), "coverage": sp["coverage"],
            "activation_now": round(cur, 1) if cur is not None else None,
            "activation_pctile": pct,
            "fire_threshold_p80": round(p80, 1),
            "firing": bool(cur is not None and cur >= p80),
            "lit_indicators": [{"symbol": s, "z": z} for s, z in lit],
            "all_members": [{"symbol": s,
                             "z": (round(z[-1], 2) if z[-1] == z[-1] else None)}
                            for s, z in zs],
            "event_study": study,
            "fusion_targets": FUSION_TARGETS.get(sp["theme"], []),
            "signal_type": f"wl_{sp['engine_id'][3:]}"[:48],
        }
        s3_put(f"{ENGINE_PREFIX}{sp['engine_id']}.json", doc)
        rows.append({k: doc[k] for k in
                     ("engine_id", "name", "theme", "state", "coverage",
                      "members_resolved", "members_total", "activation_now",
                      "activation_pctile", "firing", "history_from",
                      "signal_type", "fusion_targets")}
                    | {"w13": w13,
                       "lit": [x["symbol"] for x in doc["lit_indicators"][:5]]})

    # FDR across every engine tested today
    live = [r for r in rows if r.get("state") == "ACTIVE" and r.get("w13")]
    surv = bh_fdr([r["w13"].get("p_value", 1.0) for r in live], q=0.10) \
        if live else set()
    for i, r in enumerate(live):
        r["fdr_pass"] = i in surv

    # ── 5. signals: one per FIRING engine with a defensible edge ─────
    logged = 0
    if (event or {}).get("force_emit") or now.weekday() == 0:
        try:
            from decimal import Decimal
            tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
            for r in live:
                w13 = r["w13"]
                if not (r["firing"] and r.get("fdr_pass")
                        and abs(w13.get("t_stat", 0)) >= 2
                        and w13.get("n_effective", 0) >= 6):
                    continue
                d = "DOWN" if w13["excess_vs_base_pct"] < 0 else "UP"
                tbl.put_item(Item={
                    "signal_id": f"{r['engine_id']}#{now.date().isoformat()}",
                    "signal_type": r["signal_type"],
                    "predicted_direction": d,
                    "signal_value": str(r["activation_now"]),
                    "confidence": Decimal(str(round(min(0.75, 0.5 + abs(
                        w13["t_stat"]) / 20), 3))),
                    "measure_against": "benchmark_forward_return",
                    "baseline_price": str(spy[-1]), "benchmark": "SPY",
                    "check_windows": ["day_21", "day_63", "day_126"],
                    "outcomes": {}, "accuracy_scores": {}, "status": "pending",
                    "logged_at": now.isoformat(),
                    "logged_epoch": int(now.timestamp()),
                    "horizon_days_primary": 63, "schema_version": "2",
                    "ttl": int(now.timestamp()) + 200 * 86400,
                    "metadata": {"engine": r["engine_id"],
                                 "theme": r["theme"],
                                 "lit": r["lit"][:5],
                                 "hist_t": w13["t_stat"]},
                    "rationale": (f"{r['name']} firing "
                                  f"({r['activation_now']}% of members |z|>=1.5"
                                  f", {r['activation_pctile']}th pct). Lit: "
                                  f"{', '.join(r['lit'][:4])}"),
                })
                logged += 1
        except Exception as e:
            print(f"[wl] emit failed: {str(e)[:120]}")

    themes = Counter(r["theme"] for r in rows)
    index = {
        "generated_at": now.isoformat(), "version": "1.0",
        "framework": "watchlist-engine-framework",
        "n_engines": len(rows),
        "n_active": sum(1 for r in rows if r.get("state") == "ACTIVE"),
        "n_dormant": sum(1 for r in rows if r.get("state") == "DORMANT"),
        "n_firing": sum(1 for r in rows if r.get("firing")),
        "n_fdr": sum(1 for r in live if r.get("fdr_pass")),
        "signals_logged": logged,
        "themes": dict(themes),
        "series_cached": len(cache),
        "spy_base_rates_pct": {f"w{h}": round(base_mu[h], 2) for h in HORIZONS},
        "how_to_read": ("Every one of Khalid's watchlists is an engine: its "
                        "own feed (data/engines/<id>.json), its own "
                        "signal_type graded by the outcome-checker, and its "
                        "own scorecard row. They share ONE runtime and ONE "
                        "series cache — 207 engines, not 207 Lambdas."),
        "engines": sorted(rows, key=lambda r: (r.get("state") != "ACTIVE",
                                               -(r.get("activation_pctile")
                                                 or 0))),
        "elapsed_s": round(time.time() - t0, 1),
    }
    s3_put(INDEX_KEY, index)
    print(json.dumps({"ok": True, "engines": len(rows),
                      "active": index["n_active"], "firing": index["n_firing"],
                      "signals": logged, "elapsed": index["elapsed_s"]}))
    return {"ok": True, "n_engines": len(rows), "n_active": index["n_active"],
            "n_firing": index["n_firing"], "signals_logged": logged}
