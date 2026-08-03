"""justhodl-signal-fabric v1.0 — the layer where the engines finally
talk. Google/Microsoft pattern: no point-to-point wiring; every
stance-bearing artifact is distilled through adapters into ONE
canonical envelope keyed by ticker, fused with EMPIRICAL weights from
the engine-leaderboard, with agreement and CONFLICT as first-class
outputs. One read now answers: what does the whole fleet think about
X — and where do proven engines disagree?
Output: data/signal-fabric.json"""
import json, re, time
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
OUT = "data/signal-fabric.json"
TICK_RX = re.compile(r"^[A-Z][A-Z0-9.\-]{0,6}$")
DOWN_RX = re.compile(r"DOWN|UNDER|SHORT|SELL|BEAR|AVOID|TOP_FORM",
                     re.I)
UP_RX = re.compile(r"UP|OUT?PERF|LONG|BUY|BULL|BOTTOM_FORM", re.I)

# ── explicit adapters: (artifact_key, rows_getter, stance_fn) ──
# stance_fn(row) -> (kind, value, direction, confidence) or None


def _g(d, *ks):
    for k in ks:
        v = d.get(k)
        if v not in (None, ""):
            return v
    return None


def _dirn(v):
    sv = str(v or "")
    if DOWN_RX.search(sv):
        return "DOWN"
    if UP_RX.search(sv):
        return "UP"
    return None


def st_best_setups(r0):
    v = _g(r0, "verdict")
    return ("verdict", v, _dirn(v) or "UP",
            (_g(r0, "conviction") or 50) / 100.0)


def st_reversal(r0):
    sc = _g(r0, "reversal_score") or 0
    if sc < 15:
        return None
    d0 = _g(r0, "direction")
    return ("reversal", "%s %.0f" % (d0, sc),
            "DOWN" if d0 == "TOP_FORMING" else "UP",
            min(1.0, sc / 60.0))


def st_compound(r0):
    return ("convergence", "n=%s desk=%s"
            % (_g(r0, "n_systems"), _g(r0, "desk_score")),
            "UP", min(1.0, (_g(r0, "n_systems") or 2) / 6.0))


def st_rerating(r0):
    comp = _g(r0, "composite")
    if comp is None or float(comp) < 55:
        return None
    return ("rerating", "composite %s" % comp, "UP",
            min(1.0, float(comp) / 100.0))


def st_magic(r0):
    rk = _g(r0, "rank", "magic_rank")
    if rk is None or rk > 30:
        return None
    return ("value-rank", "MF #%s" % rk, "UP",
            max(0.3, 1.0 - rk / 40.0))


def st_opps(r0):
    sc = _g(r0, "go_score", "score", "composite")
    if sc is None or sc < 60:
        return None
    return ("opportunity", "score %s" % sc, "UP",
            min(1.0, sc / 100.0))


def st_insider(r0):
    n = _g(r0, "insiders", "n_insiders", "cluster_size") or 0
    if n < 2:
        return None
    return ("insider-cluster", "%s insiders" % n, "UP",
            min(1.0, n / 5.0))


def st_congress(r0):
    t = str(_g(r0, "type", "transaction") or "")
    d0 = "DOWN" if "sale" in t.lower() else "UP"
    return ("congress", "%s %s" % (_g(r0, "filer"), t)[:40], d0,
            0.6)


def st_squeeze(r0):
    sc = _g(r0, "squeeze_score", "days_to_cover")
    if sc is None:
        return None
    scf = float(sc)
    if scf < 6:
        return None
    return ("squeeze", "sqz %.1f" % scf, "UP",
            min(1.0, scf / 15.0))


def st_13f(sym, tf):
    x = (tf or {}).get(sym)
    if not isinstance(x, dict):
        return None
    n0 = x.get("n")
    if not n0:
        return None
    return ("13f-flow", "$net %.1fB" % (n0 / 1e9),
            "UP" if n0 > 0 else "DOWN",
            min(1.0, abs(n0) / 5e9))


ADAPTERS = [
    ("data/best-setups.json", ("top_setups",), st_best_setups,
     "best-setups"),
    ("data/trend-reversal.json", ("rows",), st_reversal,
     "trend-reversal"),
    ("data/compound-signals.json", ("compound",), st_compound,
     "compound-aggregator"),
    ("data/ai-rerating-radar.json", ("all_ranked", "rows"),
     st_rerating, "ai-rerating"),
    ("data/magic-formula.json",
     ("rows", "top", "ranked", "top_50", "stocks"),
     st_magic, "magic-formula"),
    ("data/opportunities.json",
     ("rows", "opportunities", "ranked", "results"),
     st_opps, "opportunities"),
    ("data/insider-clusters.json", ("clusters", "rows"),
     st_insider, "insider-clusters"),
    ("data/squeeze-fuel.json",
     ("rows", "scored", "ranked", "candidates"),
     st_squeeze, "squeeze-fuel"),
]


def resolve_rows(d, keys):
    for k in keys:
        v = (d or {}).get(k)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return v
    for k, v in (d or {}).items():
        if isinstance(v, list) and v and isinstance(v[0], dict) \
                and any(x in v[0] for x in ("ticker", "symbol")):
            return v
    return []
PAGE = {"short-interest": "/short-interest.html",
        "best-setups": "/best-setups.html",
        "trend-reversal": "/trend-reversal.html",
        "compound-aggregator": "/convergence-desk.html",
        "ai-rerating": "/ai-rerating.html",
        "magic-formula": "/magic-formula.html",
        "opportunities": "/opportunities.html",
        "insider-clusters": "/insiders.html",
        "congress-direct": "/political-stocks.html",
        "squeeze-fuel": "/short-interest.html",
        "13f-flows": "/sectors.html"}


def rd(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)
                          ["Body"].read())
    except Exception:
        return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    lb = rd("data/engine-leaderboard.json") or {}
    W = {}
    for x in lb.get("board") or []:
        W[str(x["engine"]).lower()] = {
            "win": x.get("win_pct"), "n": x.get("n")}

    def wt(engine):
        # empirical weight: (win%-50)/50 clipped [0.2, 1.5];
        # ungraded engines get neutral 0.6 (disclosed)
        for k, v in W.items():
            if k in engine.lower() or engine.lower() in k:
                w0 = ((v["win"] or 50) - 50) / 50.0
                return (max(0.2, min(1.5, 0.6 + w0)),
                        "empirical:%s%%(n=%s)" % (v["win"],
                                                  v["n"]))
        return (0.6, "neutral (ungraded)")
    FAB = {}

    def add(sym, engine, kind, value, direction, conf):
        if not sym or not TICK_RX.match(sym):
            return
        w0, basis = wt(engine)
        FAB.setdefault(sym, []).append({
            "engine": engine, "kind": kind,
            "value": str(value)[:60],
            "direction": direction, "confidence": round(
                float(conf or 0.5), 2),
            "weight": round(w0, 2), "weight_basis": basis,
            "link": "https://justhodl.ai"
                    + PAGE.get(engine, "/engine-leaderboard.html")})
    src_stats = {}
    for key, rows_keys, fn, engine in ADAPTERS:
        d = rd(key)
        rows = resolve_rows(d, rows_keys)
        n0 = 0
        for r0 in rows[:600]:
            if not isinstance(r0, dict):
                continue
            sym = str(_g(r0, "ticker", "symbol") or "").upper()
            try:
                st = fn(r0)
            except Exception:
                st = None
            if st:
                add(sym, engine, *st)
                n0 += 1
        src_stats[engine] = n0
    # congress-direct: nested senate/house
    cg = rd("data/congress-direct.json") or {}
    ncg = 0
    for chamber in ("senate", "house"):
        ch = cg.get(chamber) or {}
        for r0 in resolve_rows(ch, ("rows", "transactions",
                                    "filings"))[:300]:
            sym = str(_g(r0, "ticker", "symbol") or "").upper()
            st = st_congress(r0)
            if st:
                add(sym, "congress-direct", *st)
                ncg += 1
    src_stats["congress-direct"] = ncg
    # short-interest: by_ticker map beats squeeze-fuel when empty
    si = (rd("data/short-interest.json") or {}).get("by_ticker") \
        or {}
    nsi = 0
    for sym, r0 in list(si.items())[:800]:
        if not isinstance(r0, dict):
            continue
        st = st_squeeze(r0)
        if st:
            add(str(sym).upper(), "short-interest", *st)
            nsi += 1
    src_stats["short-interest"] = nsi
    tf = (rd("data/13f-flows-by-ticker.json") or {}).get("t") or {}
    n13 = 0
    for sym in list(FAB.keys()):
        st = st_13f(sym, tf)
        if st:
            add(sym, "13f-flows", *st)
            n13 += 1
    src_stats["13f-flows"] = n13
    # fuse: fabric_score, agreement, conflicts
    tickers = []
    conflicts = []
    for sym, envs in FAB.items():
        ups = [e for e in envs if e["direction"] == "UP"]
        dns = [e for e in envs if e["direction"] == "DOWN"]
        score = sum(e["weight"] * e["confidence"] for e in ups) \
            - sum(e["weight"] * e["confidence"] for e in dns)
        n_e = len(envs)
        agree = round(100.0 * max(len(ups), len(dns))
                      / n_e, 0) if n_e else 0
        row = {"ticker": sym, "n_engines": n_e,
               "fabric_score": round(score, 2),
               "net_direction": ("UP" if score > 0 else "DOWN"),
               "agreement_pct": agree,
               "engines": sorted(envs,
                                 key=lambda e: -(e["weight"]
                                                 * e["confidence"]
                                                 ))}
        tickers.append(row)
        if ups and dns and n_e >= 3:
            conflicts.append({
                "ticker": sym, "n_engines": n_e,
                "up": [e["engine"] for e in ups],
                "down": [e["engine"] for e in dns],
                "note": "proven complementarity surface -- "
                        "the fleet is debating this name"})
    tickers.sort(key=lambda x: -abs(x["fabric_score"]))
    conflicts.sort(key=lambda x: -x["n_engines"])
    out = {"engine": "justhodl-signal-fabric", "version": "1.0",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": round(time.time() - t0, 1),
           "architecture": ("N-to-1 fabric: adapters distill each "
                            "artifact into one envelope; empirical "
                            "leaderboard weights fuse them; "
                            "conflict is a first-class output"),
           "source_stats": src_stats,
           "n_tickers": len(tickers),
           "n_conflicts": len(conflicts),
           "tickers": tickers[:400],
           "conflicts": conflicts[:60],
           "by_ticker": {t["ticker"]: t["engines"]
                         for t in tickers[:400]}}
    s3.put_object(Bucket=B, Key=OUT,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    print(json.dumps({"ok": True, "tickers": len(tickers),
                      "conflicts": len(conflicts),
                      "sources": src_stats}))
    return {"ok": True}
