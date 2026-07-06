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
    V["kpi-hmm"] = g(uc, "cycle_score.level") or g(uc, "regime")
    li = get("data/liquidity-inflection.json") or {}
    b = g(li, "net_liquidity_change_13w_usd_b")
    z = g(li, "impulse_z") or g(li, "impulse.z")
    V["kpi-nl"] = (f"{'+$' if b >= 0 else '−$'}{abs(b):.0f}B" if isinstance(b, (int, float))
                   else (f"z {num(z, 2)}" if z is not None else None))
    ce = get("data/crypto-emergence.json") or {}
    V["kpi-cr"] = g(ce, "state") or g(ce, "regime")
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
         lambda d: (f"HOT ·{g(d,'hot') or g(d,'n_hot') or g(d,'hot_count')}"
                    if (g(d, 'hot') or g(d, 'n_hot') or g(d, 'hot_count')) is not None
                    else g(d, "state"))),
        ("d-cro", ["data/crisis-composite.json", "data/risk-regime.json"],
         lambda d: g(d, "defcon_level") or g(d, "posture.hedge") or g(d, "regime")),
        ("d-tail", ["data/hedge-book.json"], lambda d: g(d, "state") or g(d, "status")),
        ("d-div", ["data/cycle-clock.json"],
         lambda d: f"{len(g(d,'divergences') or [])} ACTIVE" if isinstance(g(d, "divergences"), list) else None),
        ("d-optf", ["data/options-analytics.json"],
         lambda d: g(d, "market_bias") or g(d, "summary.bias")),
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

    er = get("data/engine-registry.json") or {}
    V["ft-eng"] = str(er.get("count") or len(er.get("engines", {})) or "") or None
    man = get("nav-manifest.json") or {}
    MAP = [("dc-macro", r"macro|liquid"), ("dc-alpha", r"alpha|signal|equit"),
           ("dc-risk", r"risk|crisis|hedg"), ("dc-x", r"cross|regime|global"),
           ("dc-strat", r"strat|desk|arb|pair|wealth"), ("dc-data", r"data|api|system|platform|crypto")]
    used, tot = set(), {}
    for cat in man.get("categories", []):
        for did, rx in MAP:
            if re.search(rx, cat.get("name", ""), re.I) and cat["name"] not in used:
                used.add(cat["name"])
                tot[did] = tot.get(did, 0) + len(cat.get("pages", []))
                break
    V.update({k: str(v) for k, v in tot.items()})

    V["clock"] = datetime.now(ZoneInfo("America/New_York")).strftime("%H:%M:%S ET")
    # rows whose engines expose no public feed bake as honest link-arrows, not dead metrics
    for bid in ("d-debate", "d-vol", "d-marb", "d-pairs"):
        V.setdefault(bid, None)
        if not V.get(bid):
            V[bid] = "OPEN →"
    if not V.get("hd-regime"):
        V["hd-regime"] = g(rr, "current.regime") or g(rr, "posture.regime")
    if not V.get("read-stance"):
        V["read-stance"] = g(get("data/signal-board.json") or {}, "composite.posture")
    et = datetime.now(ZoneInfo("America/New_York"))
    V["hd-date"] = et.strftime("%a %b %d %Y").upper() + f" · AS OF {et.strftime('%H:%M')} ET"

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
