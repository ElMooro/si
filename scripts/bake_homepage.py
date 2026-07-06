#!/usr/bin/env python3
"""Bake last-known values into the homepage at Pages build time (finding 03).

Runs inside pages.yml against _site/index.html. Fetches the SAME feeds the
client binder uses, ports the SAME pick logic, and injects values into the
id'd spans so the served HTML never paints a wall of em-dashes. The client
binder still overwrites everything live after load. NEVER fails the deploy:
any error leaves the file untouched and exits 0.
"""
import html
import json
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


def get(path, timeout=8):
    url = path if path.startswith("http") else f"https://justhodl.ai/{path}"
    try:
        req = urllib.request.Request(f"{url}{'&' if '?' in url else '?'}t={int(time.time())}",
                                     headers={"User-Agent": "jh-bake/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None


def g(d, path):
    try:
        for k in path.split("."):
            d = d[int(k)] if isinstance(d, list) else (d or {}).get(k)
        return d
    except Exception:
        return None


def num(v, dp=1):
    return f"{v:.{dp}f}" if isinstance(v, (int, float)) else None


def first(paths):
    for p in paths:
        d = get(p)
        if d is not None:
            return d
    return None



def tolerant(d, limit=22):
    """last-resort: first short string under a state-ish key (rename-proof)."""
    if not isinstance(d, dict):
        return None
    import re as _re
    for k, v in d.items():
        if isinstance(v, str) and 0 < len(v) <= limit and _re.search(r"state|regime|phase|level|verdict|bias|status|posture", k, _re.I):
            return v
    for v in d.values():
        r = tolerant(v, limit) if isinstance(v, dict) else None
        if r:
            return r
    return None

def main(target):
    V = {}

    tape = get("data/market-tape.json") or {}
    for it in tape.get("items", []):
        lab, i = it.get("label"), it.get("display")
        if not lab or not i:
            continue
        ch = it.get("chg_pct")
        if isinstance(ch, (int, float)):
            i = f"{i} {'+' if ch >= 0 else ''}{ch:.1f}%"
        V[{"SPX": "tp-spx", "NDX": "tp-ndx", "US10Y": "tp-10y", "DXY": "tp-dxy",
           "BTC": "tp-btc", "VIX": "tp-vix", "GOLD": "tp-gold"}.get(lab, "")] = i

    rep = get("data/report.json") or {}
    ko = rep.get("khalid_index")
    if isinstance(ko, dict):
        V["kpi-ka"] = num(ko.get("score"))
    elif isinstance(ko, (int, float)):
        V["kpi-ka"] = num(ko)
    rr = get("data/risk-regime.json") or {}
    V["hd-regime"] = g(rr, "regime")
    uc = get("data/us-cycle.json") or {}
    V["kpi-hmm"] = g(uc, "cycle_score.level") or g(uc, "regime") or tolerant(uc)
    li = get("data/liquidity-inflection.json") or {}
    b = g(li, "net_liquidity_change_13w_usd_b")
    z = g(li, "impulse_z") or g(li, "impulse.z")
    V["kpi-nl"] = (f"{'+$' if b >= 0 else '−$'}{abs(b):.0f}B" if isinstance(b, (int, float))
                   else (f"z {num(z, 2)}" if z is not None else None))
    ce = get("data/crypto-emergence.json") or {}
    V["kpi-cr"] = g(ce, "state") or g(ce, "regime") or tolerant(ce)
    gs = get("data/global-stress.json") or {}
    V["kpi-gsi"] = num(g(gs, "global_stress_index"), 0)
    ea = get("data/engine-alpha.json") or {}
    V["kpi-edge"] = str(len(ea.get("alpha_proven_signals", []))) if ea else None

    st = first(["data/strategist.json", "data/signal-board.json"]) or {}
    body = g(st, "headline") or g(st, "read") or g(st, "summary")
    if isinstance(body, str):
        V["read-body"] = body[:180]
    V["read-stance"] = g(st, "stance") or g(st, "verdict") or g(st, "composite.posture")

    ROWS = [
        ("d-lce", ["data/liquidity-credit.json", "data/eurodollar-plumbing.json"],
         lambda d: g(d, "state") or g(d, "verdict")),
        ("d-auct", ["data/treasury-auctions.json"], lambda d: g(d, "next")),
        ("d-gbc", ["data/global-business-cycle.json"], lambda d: g(d, "aggregate.global_phase")),
        ("d-alpha", ["data/engine-alpha.json"],
         lambda d: f"{len(d.get('alpha_proven_signals', []))} PROVEN"),
        ("d-pump", ["data/pump-radar-summary.json"],
         lambda d: g(d, "temperature.label") or g(d, "conviction")),
        ("d-cro", ["data/crisis-composite.json", "data/risk-regime.json"],
         lambda d: g(d, "defcon_level") or g(d, "posture.hedge") or g(d, "regime")),
        ("d-tail", ["data/hedge-book.json"], lambda d: g(d, "last_action") or g(d, "scenario_class")),
        ("d-div", ["data/cycle-clock.json"],
         lambda d: f"{len(g(d,'divergences') or [])} ACTIVE" if isinstance(g(d, "divergences"), list) else None),
        ("d-optf", ["data/options-analytics.json"],
         lambda d: (lambda dist: ("LONG GAMMA" if dist.get("long_gamma", 0) > dist.get("short_gamma", 0)
                                   else "SHORT GAMMA") if isinstance(dist, dict) and dist else None)
                   (g(d, "distribution"))),
        ("d-cot", ["https://justhodl-dashboard-live.s3.amazonaws.com/cot/extremes/current.json"],
         lambda d: f"{len(d.get('cluster_alerts', []))} CROWDED" if isinstance(d.get("cluster_alerts"), list) else None),
        ("d-conv", ["data/best-setups.json"],
         lambda d: (lambda a: f"{len([x for x in a if 'STRONG' in str(x.get('conviction', x.get('rating','')))])} NAMES"
                    if isinstance(a, list) else None)(g(d, "setups") or g(d, "top") or [])),
        ("d-fleet", ["data/engine-registry.json"],
         lambda d: f"{d.get('count') or len(d.get('engines', {}))} ENGINES"),
    ]
    for bid, paths, pick in ROWS:
        d = first(paths)
        if d:
            try:
                V[bid] = pick(d)
            except Exception:
                pass
            if not V.get(bid):
                V[bid] = tolerant(d)

    er = get("data/engine-registry.json") or {}
    V["ft-eng"] = str(er.get("count") or len(er.get("engines", {})) or "") or None

    # dc-* counts: exact, from the live manifest's real 8 categories.
    # dc-x (Cross-Asset card: Divergences/Options Flow/COT) maps to Vol & Sentiment.
    man = get("nav-manifest.json") or {}
    order = [("dc-macro", r"macro|liquid"), ("dc-alpha", r"equit|alpha|signal"),
             ("dc-x", r"vol|sentiment|cross"), ("dc-risk", r"risk|crisis"),
             ("dc-strat", r"portfolio|execution|strat|wealth"),
             ("dc-data", r"crypto|system|meta|research|tool|data")]
    used = set()
    for did, rx in order:
        tot = 0
        for cat in man.get("categories", []):
            if cat["name"] not in used and re.search(rx, cat["name"], re.I):
                used.add(cat["name"]); tot += cat.get("count", len(cat.get("pages", [])))
        if tot:
            V[did] = str(tot)
    for cat in man.get("categories", []):        # leftovers -> platform bucket
        if cat["name"] not in used:
            V["dc-data"] = str(int(V.get("dc-data", "0")) + cat.get("count", 0))

    # rows whose engines expose no public feed: honest link-arrows, never dead metrics
    for bid in ("d-debate", "d-vol", "d-marb", "d-pairs"):
        if not V.get(bid):
            V[bid] = "OPEN →"
    # schema-tolerant last resorts
    if not V.get("hd-regime"):
        V["hd-regime"] = g(rr, "current.regime") or g(rr, "posture.regime") or tolerant(rr)
    for kid, src in (("kpi-hmm", uc), ("kpi-cr", ce), ("kpi-nl", li),
                     ("read-stance", st)):
        if not V.get(kid):
            V[kid] = tolerant(src)
    if not V.get("read-body"):
        sb = get("data/signal-board.json") or {}
        p = g(sb, "composite.posture") or tolerant(sb)
        if p:
            V["read-body"] = f"Composite posture {p} across the signal board."
    for bid in ("d-pump", "d-tail", "d-optf"):
        pass  # already tolerant()'d in the ROWS loop

    et = datetime.now(ZoneInfo("America/New_York"))
    V["hd-date"] = et.strftime("%a %b %d %Y").upper() + f" · AS OF {et.strftime('%H:%M')} ET"
    V["clock"] = et.strftime("%H:%M:%S ET")

    s = open(target, encoding="utf-8").read()
    n = 0
    for bid, val in V.items():
        if not bid or val in (None, "", "None"):
            continue
        pat = re.compile(r'(id="%s">)—(</span>)' % re.escape(bid))
        s2, k = pat.subn(lambda m: m.group(1) + html.escape(str(val)) + m.group(2), s, count=1)
        if k:
            s, n = s2, n + 1
    open(target, "w", encoding="utf-8").write(s)
    print(f"bake: {n} spans filled, {s.count('\">—</span>')} dashes remain")


if __name__ == "__main__":
    try:
        main(sys.argv[1] if len(sys.argv) > 1 else "_site/index.html")
    except Exception as e:
        print("bake WARN (deploy proceeds unbaked):", e)
    sys.exit(0)
