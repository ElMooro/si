"""ops_4336 -- ENGINE LEADERBOARD: aggregate every graded signal by
its originating engine -> win%, n, avg fwd move; publish
data/engine-leaderboard.json; print best/worst with links."""
import json, sys
from datetime import datetime, timezone
import boto3
from ops_report import report
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
PAGE = {  # engine keyword -> house page
    "best-setups": "/best-setups.html", "setups": "/best-setups.html",
    "deal": "/deal-scanner.html", "reversal": "/trend-reversal.html",
    "compound": "/convergence-desk.html",
    "alpha-scoreboard": "/convergence-desk.html",
    "congress": "/political-stocks.html",
    "insider": "/insiders.html", "squeeze": "/short-interest.html",
    "boom": "/industry-boom.html", "rotation": "/rotation.html",
    "risk": "/risk-gate.html", "quantum": "/quantum-desk.html",
    "momentum": "/momentum.html", "magic": "/magic-formula.html",
    "rerating": "/ai-rerating.html",
    "opportunit": "/opportunities.html",
    "13f": "/sectors.html", "clone": "/sectors.html",
}


def link_for(eng):
    e = str(eng).lower()
    for k, v in PAGE.items():
        if k in e:
            return "https://justhodl.ai" + v
    return ("https://justhodl-dashboard-live.s3.amazonaws.com/"
            "data/%s.json" % e)


def rd(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)
                          ["Body"].read())
    except Exception:
        return None
with report("4336_engine_leaderboard") as r:
    r.heading("ops 4336 -- the fleet grades the fleet")
    bt = rd("data/signal-backtest.json") or {}
    r.log("signal-backtest keys: %s" % list(bt)[:14])
    # find graded rows anywhere: dicts w/ engine + outcome-ish
    rows = []

    def scan(o, d=0):
        if d > 3 or len(rows) > 20000:
            return
        if isinstance(o, list) and o and isinstance(o[0], dict):
            k0 = set(o[0])
            if ("engine" in k0 or "source" in k0
                    or "system" in k0) and any(
                    x in k0 for x in ("return_pct", "alpha",
                                      "outcome", "correct", "ret",
                                      "pnl_pct", "fwd_return")):
                rows.extend(o)
                return
        if isinstance(o, dict):
            for v in o.values():
                scan(v, d + 1)
        elif isinstance(o, list):
            for v in o[:50]:
                scan(v, d + 1)
    scan(bt)
    src = "signal-backtest"
    if not rows:
        for alt in ("data/signals-graded.json",
                    "data/research-backtest.json",
                    "data/fleet-signals.json"):
            d2 = rd(alt)
            if d2:
                scan(d2)
                if rows:
                    src = alt
                    break
    r.log("graded rows found: %d (source=%s) sample keys=%s"
          % (len(rows), src,
             list(rows[0])[:12] if rows else None))
    if not rows:
        # fall back: per-engine tables already aggregated?
        agg = None
        for k, v in bt.items():
            if isinstance(v, dict) and v and isinstance(
                    next(iter(v.values())), dict) and "win_rate" \
                    in next(iter(v.values())):
                if any(t in k.lower() for t in
                       ("engine", "system", "source")):
                    agg = (k, v)
                    break
        if agg:
            r.ok("pre-aggregated table found: %s" % agg[0])
            stats = {e: {"n": s0.get("n") or s0.get("count") or 0,
                         "win": s0.get("win_rate"),
                         "avg": s0.get("avg_return_pct")
                         or s0.get("avg_alpha")}
                     for e, s0 in agg[1].items()}
        else:
            r.fail("no per-engine grades locatable -- ledger "
                   "structure printed above for next op")
            sys.exit(1)
    else:
        stats = {}
        for x in rows:
            eng = (x.get("engine") or x.get("source")
                   or x.get("system")
                   or (x.get("metadata") or {}).get("engine")
                   or "?")
            ret = None
            for k in ("alpha", "return_pct", "fwd_return", "ret",
                      "pnl_pct"):
                if isinstance(x.get(k), (int, float)):
                    ret = float(x[k])
                    break
            win = x.get("correct")
            if win is None and ret is not None:
                win = ret > 0
            if win is None:
                continue
            s0 = stats.setdefault(str(eng),
                                  {"n": 0, "w": 0, "rets": []})
            s0["n"] += 1
            s0["w"] += 1 if win else 0
            if ret is not None:
                s0["rets"].append(ret)
        for e, s0 in stats.items():
            s0["win"] = round(100.0 * s0["w"] / s0["n"], 1)
            s0["avg"] = (round(sum(s0["rets"]) / len(s0["rets"]),
                               2) if s0["rets"] else None)
            s0.pop("rets", None)
            s0.pop("w", None)
    board = [{"engine": e, "n": s0["n"], "win_pct": s0.get("win"),
              "avg_move_pct": s0.get("avg"),
              "link": link_for(e)}
             for e, s0 in stats.items() if (s0.get("n") or 0) >= 5]
    board.sort(key=lambda x: (-(x["win_pct"] or 0), -x["n"]))
    out = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "source": src, "min_n": 5, "n_engines": len(board),
           "board": board}
    s3.put_object(Bucket=B, Key="data/engine-leaderboard.json",
                  Body=json.dumps(out).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    r.section("TOP SUCCESS ENGINES")
    for x in board[:8]:
        r.log("%-28s %5.1f%% win · n=%-4d avg %s%% · %s"
              % (x["engine"], x["win_pct"], x["n"],
                 x["avg_move_pct"], x["link"]))
    r.section("TOP FAILURE ENGINES")
    for x in board[-8:][::-1]:
        r.log("%-28s %5.1f%% win · n=%-4d avg %s%% · %s"
              % (x["engine"], x["win_pct"], x["n"],
                 x["avg_move_pct"], x["link"]))
    r.ok("leaderboard published: data/engine-leaderboard.json "
         "(%d engines, n>=5)" % len(board))
    if not board:
        sys.exit(1)
