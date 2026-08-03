"""justhodl-alpha-council v1.0 — Khalid's idea, institutionally armored.
Council membership = engines PROVEN by Wilson 95% lower-bound >= 55%
with n >= 10 (luck can't buy a seat). Each member gets a WHY-profile
(regime-sliced win rates, direction bias, expectancy). The council
votes on the ledger's OPEN signals, weighted by wilson_lb and filtered
by current-regime fitness, and LOGS ITS OWN consensus calls back into
justhodl-signals as eng:alpha-council — so the ultimate engine must
earn its row on the same leaderboard that judges everyone else.
Outputs: data/alpha-council.json"""
import json, math, re, time
from datetime import datetime, timezone
from decimal import Decimal

import boto3

ddb = boto3.resource("dynamodb", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
OUT = "data/alpha-council.json"
Z = 1.96
TICK_RX = re.compile(r"^[A-Z][A-Z0-9.\-]{0,6}$")


def F(v):
    try:
        return float(v)
    except Exception:
        return None


def wilson_lb(w, n):
    if not n:
        return 0.0
    p = w / n
    d = 1 + Z * Z / n
    c = p + Z * Z / (2 * n)
    e = Z * math.sqrt(p * (1 - p) / n + Z * Z / (4 * n * n))
    return round(100.0 * (c - e) / d, 1)


def eng_of(it):
    st0 = str(it.get("signal_type") or "")
    if st0.startswith("eng:"):
        return st0[4:]
    md = it.get("metadata")
    md = md if isinstance(md, dict) else {}
    return str(md.get("engine") or it.get("engine") or st0 or "?")


def sym_of(it):
    md = it.get("metadata")
    md = md if isinstance(md, dict) else {}
    for c in (it.get("symbol"), it.get("ticker"), md.get("symbol"),
              md.get("ticker"), it.get("signal_value")):
        cs = str(c or "").upper().strip()
        if cs and TICK_RX.match(cs) and cs not in ("UP", "DOWN",
                                                   "PICK", "NONE",
                                                   "TRUE", "FALSE"):
            return cs
    return None


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
    # graded pass: overall + per-regime per engine
    G = {}
    for it in items:
        w, mv = outcome(it)
        if w is None:
            continue
        e = eng_of(it)
        rg = str(it.get("regime_at_log") or "UNKNOWN")
        g0 = G.setdefault(e, {"n": 0, "w": 0, "mv": [],
                              "up": 0, "dn": 0, "reg": {}})
        g0["n"] += 1
        g0["w"] += 1 if w else 0
        if mv is not None:
            g0["mv"].append(mv)
        if str(it.get("predicted_direction")
               or "UP").upper() == "DOWN":
            g0["dn"] += 1
        else:
            g0["up"] += 1
        rr = g0["reg"].setdefault(rg, {"n": 0, "w": 0})
        rr["n"] += 1
        rr["w"] += 1 if w else 0
    # current regime
    cur_regime = "UNKNOWN"
    try:
        uc = json.loads(s3.get_object(
            Bucket=B, Key="data/us-cycle.json")["Body"].read())
        cur_regime = str(uc.get("regime") or uc.get("phase")
                         or "UNKNOWN").upper()
    except Exception:
        pass
    posture = None
    try:
        posture = json.loads(s3.get_object(
            Bucket=B, Key="data/risk-gate.json")["Body"].read()
        ).get("posture")
    except Exception:
        pass
    # council selection + WHY profiles
    council = []
    for e, g0 in G.items():
        lb = wilson_lb(g0["w"], g0["n"])
        if g0["n"] >= 10 and lb >= 55.0:
            wr = round(100.0 * g0["w"] / g0["n"], 1)
            wins = [x for x in g0["mv"] if x is not None]
            avg = round(sum(wins) / len(wins), 2) if wins else None
            regtab = {k: {"n": v["n"],
                          "win_pct": round(100.0 * v["w"]
                                           / v["n"], 1)}
                      for k, v in g0["reg"].items()
                      if v["n"] >= 3}
            cr = regtab.get(cur_regime, {})
            council.append({
                "engine": e, "n": g0["n"], "win_pct": wr,
                "wilson_lb": lb, "avg_move_pct": avg,
                "direction_bias": ("DOWN" if g0["dn"] > g0["up"]
                                   else "UP"),
                "win_by_regime": regtab,
                "regime_fit_now": cr.get("win_pct"),
                "why": ("proven at wilson-lb %.1f%% over %d calls; "
                        "%s-biased; strongest regime %s"
                        % (lb, g0["n"],
                           "DOWN" if g0["dn"] > g0["up"] else "UP",
                           max(regtab.items(),
                               key=lambda kv:
                               kv[1]["win_pct"])[0]
                           if regtab else "n/a"))})
    council.sort(key=lambda x: -x["wilson_lb"])
    seats = {c["engine"]: c for c in council}
    # open-signal pass: council votes
    now_ep = time.time()
    votes = {}
    for it in items:
        if str(it.get("status")) not in ("pending", "partial"):
            continue
        e = eng_of(it)
        c = seats.get(e)
        if not c:
            continue
        # regime filter: engine must be fit in current regime
        # (>=52% there, or no regime data -> allow with penalty)
        rf = c.get("regime_fit_now")
        if rf is not None and rf < 52.0:
            continue
        sym = sym_of(it)
        if not sym:
            continue
        ep = F(it.get("logged_epoch")) or 0
        hz = F(it.get("horizon_days_primary")) or 21
        if now_ep - ep > hz * 86400:
            continue
        pd = str(it.get("predicted_direction") or "UP").upper()
        w0 = c["wilson_lb"] / 100.0 * (1.0 if rf is None
                                       else min(1.2, rf / 60.0))
        v0 = votes.setdefault(sym, {"score": 0.0, "engines": [],
                                    "up": 0, "dn": 0})
        v0["score"] += w0 if pd != "DOWN" else -w0
        v0["up" if pd != "DOWN" else "dn"] += 1
        v0["engines"].append({"engine": e,
                              "direction": pd,
                              "wilson_lb": c["wilson_lb"],
                              "regime_fit": rf})
    consensus = []
    for sym, v0 in votes.items():
        n_e = len(v0["engines"])
        if n_e >= 2 and (v0["up"] == 0 or v0["dn"] == 0):
            consensus.append({
                "symbol": sym,
                "direction": "UP" if v0["score"] > 0 else "DOWN",
                "council_n": n_e,
                "weighted_score": round(abs(v0["score"]), 3),
                "engines": sorted(v0["engines"],
                                  key=lambda x: -x["wilson_lb"]),
                "regime": cur_regime})
    consensus.sort(key=lambda x: (-x["council_n"],
                                  -x["weighted_score"]))
    # self-log consensus into the ledger (the council gets graded)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logged = 0
    for c0 in consensus[:12]:
        try:
            tbl.put_item(Item={
                "signal_id": "alpha-council#%s#%s"
                             % (c0["symbol"], today),
                "signal_type": "eng:alpha-council",
                "logged_at": datetime.now(
                    timezone.utc).isoformat(),
                "logged_epoch": Decimal(str(int(now_ep))),
                "status": "pending",
                "predicted_direction": c0["direction"],
                "signal_value": c0["symbol"],
                "horizon_days_primary": Decimal("21"),
                "check_windows": ["7", "14", "21"],
                "regime_at_log": cur_regime,
                "metadata": {"engine": "alpha-council",
                             "council_n": Decimal(
                                 str(c0["council_n"])),
                             "weighted_score": Decimal(
                                 str(c0["weighted_score"]))},
                "ttl": Decimal(str(int(now_ep)
                                   + 180 * 86400))})
            logged += 1
        except Exception as e2:
            print("[council] log fail %s: %s"
                  % (c0["symbol"], e2))
    out = {"engine": "justhodl-alpha-council", "version": "1.0",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": round(time.time() - t0, 1),
           "membership_rule": "wilson_lb>=55 & n>=10",
           "current_regime": cur_regime, "risk_posture": posture,
           "n_council": len(council), "council": council,
           "n_consensus": len(consensus),
           "consensus_calls": consensus[:40],
           "self_logged": logged,
           "note": ("the council logs its own calls as "
                    "eng:alpha-council -- it must earn its row "
                    "on the leaderboard like everyone else")}
    s3.put_object(Bucket=B, Key=OUT,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    print(json.dumps({"ok": True, "council": len(council),
                      "consensus": len(consensus),
                      "self_logged": logged,
                      "regime": cur_regime}))
    return {"ok": True}
