"""ops_4336 -- ENGINE LEADERBOARD: aggregate every graded signal by
its originating engine -> win%, n, avg fwd move; publish
data/engine-leaderboard.json; print best/worst with links."""
import json, sys
from datetime import datetime, timezone
import boto3
from ops_report import report
s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")
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
    r.heading("ops 4336 -- the fleet grades the fleet (ddb ledger)")
    tbl = ddb.Table("justhodl-signals")
    items = []
    kw = {}
    while len(items) < 30000:
        resp = tbl.scan(**kw)
        items += resp.get("Items", [])
        if "LastEvaluatedKey" not in resp:
            break
        kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    r.log("ledger items scanned: %d" % len(items))
    keyset = {}
    for it in items[:400]:
        for k in it:
            keyset[k] = keyset.get(k, 0) + 1
    r.log("attr frequency (top): %s"
          % sorted(keyset.items(), key=lambda x: -x[1])[:16])
    st = {}
    for it in items[:400]:
        s0 = str(it.get("status"))
        st[s0] = st.get(s0, 0) + 1
    r.log("status sample dist: %s" % st)
    if items:
        samp = next((it for it in items
                     if str(it.get("status")) not in
                     ("pending", "None")), items[0])
        r.log("sample item: %s"
              % json.dumps({k: str(samp[k])[:60]
                            for k in list(samp)[:16]},
                           default=str)[:520])

    def eng_of(it):
        st0 = str(it.get("signal_type") or "")
        if st0.startswith("eng:"):
            return st0[4:]
        md = it.get("metadata")
        md = md if isinstance(md, dict) else {}
        return str(md.get("engine") or it.get("engine")
                   or st0 or "?")

    def F(v):
        try:
            return float(v)
        except Exception:
            return None

    # schema receipt: two live outcomes dicts, verbatim
    _shown = 0
    for it in items:
        oc = it.get("outcomes")
        if isinstance(oc, dict) and oc and _shown < 2:
            r.log("outcomes sample [%s/%s]: %s"
                  % (eng_of(it), it.get("status"),
                     json.dumps(oc, default=str)[:420]))
            _shown += 1

    def outcome(it):
        oc = it.get("outcomes")
        if not isinstance(oc, dict) or not oc:
            return (None, None)
        prim = str(it.get("horizon_days_primary") or "")
        wkeys = list(oc.keys())
        wk = ("day_" + prim if ("day_" + prim) in oc
              else prim if prim in oc
              else (sorted(wkeys)[-1] if wkeys else None))
        w0 = oc.get(wk)
        if not isinstance(w0, dict):
            return (None, None)
        ret = None
        for k, v in w0.items():
            lk = str(k).lower()
            if any(t in lk for t in ("return", "alpha", "chg",
                                     "pct", "move")):
                fv = F(v)
                if fv is not None:
                    ret = fv
                    if "alpha" in lk:
                        break
        hit = None
        for k, v in w0.items():
            lk = str(k).lower()
            if any(t in lk for t in ("hit", "correct", "win",
                                     "success")):
                if isinstance(v, bool):
                    hit = v
                else:
                    sv = str(v).lower()
                    if sv in ("true", "1", "yes", "win", "hit"):
                        hit = True
                    elif sv in ("false", "0", "no", "loss",
                                "miss"):
                        hit = False
                break
        if hit is None and ret is not None:
            pd = str(it.get("predicted_direction")
                     or "UP").upper()
            hit = (ret > 0) if pd != "DOWN" else (ret < 0)
        return (hit, ret)
    stats = {}
    n_graded = 0
    for it in items:
        w, mv = outcome(it)
        if w is None:
            continue
        n_graded += 1
        e = eng_of(it)
        s0 = stats.setdefault(e, {"n": 0, "w": 0, "mv": []})
        s0["n"] += 1
        s0["w"] += 1 if w else 0
        if mv is not None:
            s0["mv"].append(mv)
    r.log("graded items usable: %d across %d engines"
          % (n_graded, len(stats)))
    board = []
    for e, s0 in stats.items():
        if s0["n"] < 5:
            continue
        board.append({
            "engine": e, "n": s0["n"],
            "win_pct": round(100.0 * s0["w"] / s0["n"], 1),
            "avg_move_pct": (round(sum(s0["mv"]) / len(s0["mv"]),
                             2) if s0["mv"] else None),
            "link": link_for(e)})
    board.sort(key=lambda x: (-x["win_pct"], -x["n"]))
    from datetime import datetime as _dt, timezone as _tz
    s3.put_object(Bucket=B, Key="data/engine-leaderboard.json",
                  Body=json.dumps({
                      "generated_at": _dt.now(_tz.utc).isoformat(),
                      "source": "dynamodb:justhodl-signals",
                      "n_items_scanned": len(items),
                      "n_graded": n_graded, "min_n": 5,
                      "board": board}).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    r.section("TOP SUCCESS ENGINES")
    for x in board[:10]:
        r.log("%-30s %5.1f%% · n=%-4d avg=%s · %s"
              % (x["engine"], x["win_pct"], x["n"],
                 x["avg_move_pct"], x["link"]))
    r.section("TOP FAILURE ENGINES")
    for x in board[-10:][::-1]:
        r.log("%-30s %5.1f%% · n=%-4d avg=%s · %s"
              % (x["engine"], x["win_pct"], x["n"],
                 x["avg_move_pct"], x["link"]))
    if not board:
        r.fail("no graded items with n>=5 -- schema printed above")
        sys.exit(1)
    r.ok("leaderboard LIVE: data/engine-leaderboard.json "
         "(%d engines)" % len(board))

# retrigger: ddb ledger scan

# retrigger: outcomes-dict parser v3 + schema receipt

# retrigger: string-metadata guard + day_-prefixed windows
