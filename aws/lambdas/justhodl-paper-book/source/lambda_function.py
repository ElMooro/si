"""
justhodl-paper-book — THE PAPER BOOK: research → action, honestly tracked
═══════════════════════════════════════════════════════════════════════════════════════
Every part of the system has been analysis; none of it has been HELD accountable as a P&L.
This engine closes that loop on PAPER first (de-risking the step to real capital). It runs a
systematic, fully-rules-based portfolio that trades the system's actual read — overweight the
sleeves the Risk Map says are BOOMING, avoid those getting DESTROYED, tilt defensive when the
regime turns risk-off — rebalanced weekly with realistic slippage, marked to market daily, and
benchmarked against SPY buy-and-hold. The output is the one thing that turns a research terminal
into a fund: a live, auditable equity curve with Sharpe, drawdown, and excess-vs-SPY.

NO LEVERAGE, long-only, single-name cap 25%, slippage 5bps/trade. Paper capital $100,000.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
S3 = boto3.client("s3", REGION)
KEY = "data/paper-book.json"

START_CAP = 100000.0
SLIPPAGE = 0.0005       # 5 bps per trade notional
MAX_POS = 0.25          # single-name cap
# tradeable ETF universe (maps to Risk Map sleeves) + defensives
TRADEABLE = ["SPY", "QQQ", "IWM", "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU",
             "XLB", "XLC", "GLD", "TLT", "HYG", "EEM", "EFA", "MTUM", "VLUE"]


def last_close(t):
    url = (f"https://api.polygon.io/v2/aggs/ticker/{t}/prev?adjusted=true&apiKey={POLY}")
    try:
        r = json.loads(urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "jh"}), timeout=15).read())
        res = r.get("results", [])
        return res[0]["c"] if res else None
    except Exception:
        return None


def target_weights(rmap):
    """Buy what's booming, avoid what's destroyed, tilt defensive when risk-off. Long-only, sums≤1."""
    ro = {}
    for sleeve in (rmap.get("sleeves") or {}).values():
        for it in sleeve:
            if it["ticker"] in TRADEABLE:
                ro[it["ticker"]] = it["risk_on"]
    reg = rmap.get("regime", {})
    risk_off = (reg.get("equity_avg", 0) < 0) or reg.get("label") in ("BROAD RISK-OFF", "FLIGHT TO QUALITY")
    # weight ∝ positive risk_on, only names risk_on>15
    pos = {t: v for t, v in ro.items() if v > 15}
    raw = {t: float(v) for t, v in pos.items()}
    # defensive overlay
    if risk_off:
        for d in ("GLD", "TLT", "XLU", "XLP"):
            raw[d] = max(raw.get(d, 0), 40)
    s = sum(raw.values())
    if s <= 0:
        return {"GLD": 0.4, "TLT": 0.3}  # nothing booming → defensive
    w = {t: min(MAX_POS, v / s) for t, v in raw.items()}
    # renormalize after cap, leave residual as cash if invested<1
    s2 = sum(w.values())
    if s2 > 1:
        w = {t: v / s2 for t, v in w.items()}
    return w


def lambda_handler(event=None, context=None):
    t0 = time.time()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dow = datetime.now(timezone.utc).weekday()  # 0=Mon
    try:
        _raw = json.loads(S3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
        book = _raw.get("_state") if isinstance(_raw, dict) and "_state" in _raw else _raw
    except Exception:
        book = None
    try:
        rmap = json.loads(S3.get_object(Bucket=BUCKET, Key="data/regime-map.json")["Body"].read())
    except Exception:
        rmap = {}

    px = {}
    for t in TRADEABLE:
        c = last_close(t)
        if c:
            px[t] = c
    spy = px.get("SPY")
    if not spy:
        return {"statusCode": 500, "body": "no SPY price"}

    if book is None:
        book = {"inception": today, "cash": START_CAP, "positions": {}, "spy_inception": spy,
                "history": [], "trades": [], "last_rebalance": None}

    # mark to market
    pos_val = sum(book["positions"].get(t, {}).get("shares", 0) * px.get(t, book["positions"].get(t, {}).get("last", 0))
                  for t in book["positions"])
    nav = book["cash"] + pos_val
    if nav <= 0:
        nav = START_CAP
    bench = START_CAP * spy / book["spy_inception"]

    # rebalance weekly (Mon) OR on regime change OR first run
    regime_changed = bool(rmap.get("regime_changed"))
    do_rebal = (book["last_rebalance"] is None) or (dow == 0) or regime_changed
    if do_rebal and rmap.get("sleeves"):
        tgt = target_weights(rmap)
        cur_val = {t: book["positions"].get(t, {}).get("shares", 0) * px.get(t, 0) for t in set(list(book["positions"]) + list(tgt))}
        for t in set(list(tgt) + list(book["positions"])):
            if t not in px:
                continue
            tgt_dollar = tgt.get(t, 0.0) * nav
            cur_dollar = cur_val.get(t, 0.0)
            delta = tgt_dollar - cur_dollar
            if abs(delta) < nav * 0.01:   # ignore <1% nudges
                continue
            cost = abs(delta) * SLIPPAGE
            shares_delta = delta / px[t]
            new_shares = book["positions"].get(t, {}).get("shares", 0) + shares_delta
            book["cash"] -= (delta + cost)
            if new_shares * px[t] < nav * 0.005:
                book["positions"].pop(t, None)
            else:
                book["positions"][t] = {"shares": round(new_shares, 4), "last": px[t]}
            book["trades"].append({"date": today, "ticker": t, "side": "BUY" if delta > 0 else "SELL",
                                   "notional": round(abs(delta)), "px": px[t], "slippage": round(cost, 2)})
        book["last_rebalance"] = today
        book["trades"] = book["trades"][-120:]
    else:
        for t in list(book["positions"]):
            if t in px:
                book["positions"][t]["last"] = px[t]

    # recompute nav post-trade
    nav = book["cash"] + sum(book["positions"][t]["shares"] * book["positions"][t].get("last", px.get(t, 0)) for t in book["positions"])
    if not book["history"] or book["history"][-1]["date"] != today:
        book["history"].append({"date": today, "nav": round(nav, 2), "bench": round(bench, 2)})
    else:
        book["history"][-1] = {"date": today, "nav": round(nav, 2), "bench": round(bench, 2)}
    book["history"] = book["history"][-400:]

    # stats
    navs = [h["nav"] for h in book["history"]]
    benchs = [h["bench"] for h in book["history"]]
    def series_rets(s):
        return [(s[i] / s[i - 1] - 1) for i in range(1, len(s)) if s[i - 1]]
    rets = series_rets(navs)
    import statistics as st
    sharpe = None
    if len(rets) > 5 and st.pstdev(rets) > 0:
        sharpe = round((st.mean(rets) / st.pstdev(rets)) * (252 ** 0.5), 2)
    peak, mdd = navs[0] if navs else START_CAP, 0.0
    for v in navs:
        peak = max(peak, v); mdd = min(mdd, v / peak - 1)
    total_ret = nav / START_CAP - 1
    bench_ret = bench / START_CAP - 1

    weights = {t: round(book["positions"][t]["shares"] * book["positions"][t].get("last", 0) / nav, 3)
               for t in book["positions"]} if nav else {}
    out = {
        "engine": "justhodl-paper-book", "version": "1.0.0", "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Systematic paper book — trades the system's read (buy booming sleeves, avoid destroyed, "
                  "regime-tilt), realistic slippage, marked to market daily vs SPY. Research → honest track record.",
        "inception": book["inception"], "nav": round(nav, 2), "benchmark_nav": round(bench, 2),
        "total_return_pct": round(total_ret * 100, 2), "benchmark_return_pct": round(bench_ret * 100, 2),
        "excess_vs_spy_pct": round((total_ret - bench_ret) * 100, 2),
        "sharpe": sharpe, "max_drawdown_pct": round(mdd * 100, 2),
        "n_days": len(book["history"]), "last_rebalance": book["last_rebalance"], "cash_pct": round(book["cash"] / nav * 100, 1) if nav else 0,
        "positions": [{"ticker": t, "weight_pct": round(weights[t] * 100, 1),
                       "shares": book["positions"][t]["shares"], "px": book["positions"][t].get("last")}
                      for t in sorted(weights, key=lambda x: -weights[x])],
        "history": book["history"], "recent_trades": list(reversed(book["trades"][-20:])),
        "regime_at_last_rebalance": (rmap.get("regime") or {}).get("label"),
        "note": "Paper capital $100k, long-only, no leverage, 25% single-name cap, 5bps slippage, weekly + "
                "regime-change rebalance. Track record builds from inception — treat early stats as preliminary.",
        "elapsed_s": round(time.time() - t0, 1),
    }
    # persist full state inside same file (positions/cash/trades are the state)
    book_state = {k: book[k] for k in ("inception", "cash", "positions", "spy_inception", "history", "trades", "last_rebalance")}
    out["_state"] = book_state
    S3.put_object(Bucket=BUCKET, Key=KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    print(f"[paper-book] NAV {nav:.0f} vs bench {bench:.0f} | tot {total_ret*100:+.1f}% vs SPY {bench_ret*100:+.1f}% "
          f"(excess {(total_ret-bench_ret)*100:+.1f}) | sharpe {sharpe} mdd {mdd*100:.1f}% | rebal={do_rebal} | {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "nav": round(nav), "excess_vs_spy_pct": out["excess_vs_spy_pct"],
            "n_days": len(book["history"]), "rebalanced": do_rebal})}
