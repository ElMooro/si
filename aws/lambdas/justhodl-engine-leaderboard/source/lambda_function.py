"""justhodl-engine-leaderboard v1.0 — the fleet's self-grade, daily.
Scans the DynamoDB justhodl-signals ledger (every engine's logged
calls, graded per-window by the fleet signals loop), aggregates per
engine, and publishes the FULL DISTRIBUTION — because tails without
the middle mislead. Output: data/engine-leaderboard.json."""
import json, time
from datetime import datetime, timezone

import boto3

ddb = boto3.resource("dynamodb", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
OUT = "data/engine-leaderboard.json"
PAGE = {"best-setups": "/best-setups.html",
        "setups": "/best-setups.html", "deal": "/deal-scanner.html",
        "reversal": "/trend-reversal.html",
        "compound": "/convergence-desk.html",
        "alpha": "/convergence-desk.html",
        "congress": "/political-stocks.html",
        "political": "/political-stocks.html",
        "insider": "/insiders.html", "squeeze": "/short-interest.html",
        "boom": "/industry-boom.html", "rotation": "/rotation.html",
        "risk": "/risk-gate.html", "onchain": "/risk-gate.html",
        "quantum": "/quantum-desk.html", "momentum": "/momentum.html",
        "magic": "/magic-formula.html", "rerating": "/ai-rerating.html",
        "opportunit": "/opportunities.html", "13f": "/sectors.html"}


def link_for(e):
    el = str(e).lower()
    for k, v in PAGE.items():
        if k in el:
            return "https://justhodl.ai" + v
    return ("https://justhodl-dashboard-live.s3.amazonaws.com/"
            "data/%s.json" % el)


def F(v):
    try:
        return float(v)
    except Exception:
        return None


def eng_of(it):
    st0 = str(it.get("signal_type") or "")
    if st0.startswith("eng:"):
        return st0[4:]
    md = it.get("metadata")
    md = md if isinstance(md, dict) else {}
    return str(md.get("engine") or it.get("engine") or st0 or "?")


def outcome(it):
    oc = it.get("outcomes")
    if not isinstance(oc, dict) or not oc:
        return (None, None)
    prim = str(it.get("horizon_days_primary") or "")
    wkeys = sorted(oc.keys())
    wk = ("day_" + prim if ("day_" + prim) in oc
          else prim if prim in oc else (wkeys[-1] if wkeys else None))
    w0 = oc.get(wk)
    if not isinstance(w0, dict):
        return (None, None)
    ret = None
    for k, v in w0.items():
        if "return" in str(k).lower():
            ret = F(v)
            break
    hit = w0.get("correct")
    if isinstance(hit, str):
        hit = hit.lower() in ("true", "1")
    if hit is None and ret is not None:
        pd = str(it.get("predicted_direction") or "UP").upper()
        hit = (ret > 0) if pd != "DOWN" else (ret < 0)
    return (hit, ret)


def lambda_handler(event=None, context=None):
    t0 = time.time()
    tbl = ddb.Table("justhodl-signals")
    items = []
    kw = {}
    while len(items) < 60000:
        resp = tbl.scan(**kw)
        items += resp.get("Items", [])
        if "LastEvaluatedKey" not in resp:
            break
        kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    stats = {}
    pooled_w = pooled_n = 0
    pooled_rets = []
    for it in items:
        w, mv = outcome(it)
        if w is None:
            continue
        e = eng_of(it)
        s0 = stats.setdefault(e, {"n": 0, "w": 0, "mv": []})
        s0["n"] += 1
        s0["w"] += 1 if w else 0
        pooled_n += 1
        pooled_w += 1 if w else 0
        if mv is not None:
            s0["mv"].append(mv)
            pooled_rets.append(mv if w else -abs(mv) * 0
                               + mv)  # raw move
    board = []
    for e, s0 in stats.items():
        if s0["n"] < 5:
            continue
        wr = round(100.0 * s0["w"] / s0["n"], 1)
        avg = (round(sum(s0["mv"]) / len(s0["mv"]), 2)
               if s0["mv"] else None)
        board.append({"engine": e, "n": s0["n"], "win_pct": wr,
                      "avg_move_pct": avg, "link": link_for(e)})
    board.sort(key=lambda x: (-x["win_pct"], -x["n"]))
    wrs = sorted(x["win_pct"] for x in board)
    med = (wrs[len(wrs) // 2] if wrs else None)
    ge55 = sum(1 for x in wrs if x >= 55)
    hist = {"%d-%d" % (b, b + 10):
            sum(1 for x in wrs if b <= x < b + 10 or
                (b == 90 and x == 100))
            for b in range(0, 100, 10)}
    out = {"engine": "justhodl-engine-leaderboard", "version": "1.0",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": round(time.time() - t0, 1),
           "source": "dynamodb:justhodl-signals",
           "n_items": len(items), "n_graded": pooled_n,
           "min_n": 5, "n_engines": len(board),
           "distribution": {
               "pooled_win_pct": (round(100.0 * pooled_w
                                        / pooled_n, 1)
                                  if pooled_n else None),
               "median_engine_win_pct": med,
               "engines_ge_55_pct": ge55,
               "engines_ge_55_share": (round(100.0 * ge55
                                             / len(board), 1)
                                       if board else None),
               "histogram": hist},
           "top": board[:20], "bottom": board[-20:][::-1],
           "board": board}
    s3.put_object(Bucket=B, Key=OUT,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    print(json.dumps({"ok": True, "graded": pooled_n,
                      "engines": len(board),
                      "pooled": out["distribution"]
                      ["pooled_win_pct"], "median": med,
                      "ge55": ge55}))
    return {"ok": True}
