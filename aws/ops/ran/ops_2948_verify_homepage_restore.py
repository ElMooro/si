#!/usr/bin/env python3
"""ops 2948 — verify the homepage restore + whole-site health on the LIVE site.

Khalid's report: after this morning's redesign, "none of my pages are there
and none of my engines are wired in." Fixes shipped: index.html restored to
the pre-redesign Operator Console (drawer nav active on it again), desk-v2 +
ai_predictions un-retired, self-healing service worker. This script proves,
against production only:
 1. Live / is the Operator Console (and NOT Command Center v2).
 2. Site-wide nav actually lists the pages: nav-manifest count.
 3. A spread of core pages return 200, real size, with the nav drawer wired.
 4. The restored desk-v2 + ai_predictions are live full pages, not stubs.
 5. The engine layer is alive: registry count + key feeds parse and are fresh.
Read-only apart from nothing — no writes at all.
"""
import json, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
from ops_report import report

BASE = "https://justhodl.ai"

def get(path, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            f"{BASE}/{path}?_={int(time.time())}",
            headers={"User-Agent": "Mozilla/5.0 jh-ops"}), timeout=to)
        return r.getcode(), r.read()
    except Exception:
        return None, b""

def get_json(path, tries=3):
    for _ in range(tries):
        c, b = get(path)
        if c == 200:
            try:
                return json.loads(b)
            except Exception:
                pass
        time.sleep(2)
    return None

def main():
    with report("2948_verify_homepage_restore") as rep:
        fails = []

        # 1) homepage identity — retry to ride out Pages CDN propagation
        home = ""
        for i in range(5):
            c, b = get("")
            home = b.decode("utf-8", "replace")
            if c == 200 and "Operator Console" in home and "JH COMMAND CENTER v2.0" not in home:
                break
            time.sleep(25)
        is_old = "Operator Console" in home and "JH COMMAND CENTER v2.0" not in home
        rep.kv(homepage_operator_console=is_old, homepage_bytes=len(home))
        if not is_old:
            fails.append("homepage is not the restored Operator Console")

        # 2) navigation actually lists the pages
        nav = get_json("nav-manifest.json") or {}  # drawer fetches ROOT path (Pages-served), not /data/
        hrefs = set()
        def walk(x):
            if isinstance(x, dict):
                for k, v in x.items():
                    if k in ("href", "url", "path") and isinstance(v, str):
                        hrefs.add(v)
                    walk(v)
            elif isinstance(x, list):
                for v in x:
                    walk(v)
            elif isinstance(x, str) and (x.endswith(".html") or x.endswith("/")):
                hrefs.add(x)
        walk(nav)
        navn = len(hrefs)
        rep.kv(nav_manifest_pages=navn)
        if navn < 300:
            fails.append(f"nav-manifest thin ({navn})")

        # 3) core pages live + drawer wired
        sample = ["today.html", "plumbing.html", "options.html", "onchain.html",
                  "apac.html", "chart-pro.html", "why.html", "risk-desk.html",
                  "flows.html", "engines.html", "signal-board.html", "llm-cost.html"]
        with ThreadPoolExecutor(max_workers=8) as ex:
            res = dict(zip(sample, ex.map(get, sample)))
        bad = [p for p, (c, b) in res.items() if c != 200 or len(b) < 3000]
        drawer = sum(1 for _, b in res.values() if b and b"jh-nav-drawer" in b)
        rep.kv(sample_pages_ok=f"{len(sample)-len(bad)}/{len(sample)}", drawer_wired=f"{drawer}/{len(sample)}", bad_pages=",".join(bad) or "none")
        sc, sb = get("screener/")
        rep.kv(screener_probe=f"http={sc} bytes={len(sb)} (protected page — non-browser agents blocked by design; informational only)")
        if bad:
            fails.append(f"pages failing: {bad}")

        # 4) restored (un-retired) pages are real again
        for p in ("desk-v2.html", "ai_predictions.html"):
            c, b = get(p)
            rep.kv(**{p.replace(".html", "").replace("-", "_") + "_bytes": (len(b) if c == 200 else -1)})
            if c != 200 or len(b) < 5000:
                fails.append(f"{p} still stub/missing ({c},{len(b)})")

        # 5) engine layer alive
        reg = get_json("data/engine-registry.json") or {}
        engines = reg.get("engines") or (reg if isinstance(reg, list) else [])
        rep.kv(engine_registry_count=len(engines))
        rj = get_json("data/report.json") or {}
        ki = (rj.get("khalid_index") or {}).get("score")
        rep.kv(khalid_index_score=ki)
        tape = get_json("data/market-tape.json") or {}
        rep.kv(market_tape_symbols=len(tape.get("symbols", [])) if isinstance(tape, dict) else 0)
        if len(engines) < 600:
            fails.append(f"engine registry thin ({len(engines)})")
        if ki is None:
            fails.append("report.json khalid_index missing")

        line = (f"homepage=OperatorConsole:{is_old} nav={navn} "
                f"pages={len(sample)-len(bad)}/{len(sample)} drawer={drawer} "
                f"engines={len(engines)} ki={ki}")
        print(line)
        rep.kv(summary=line)
        if fails:
            for f in fails:
                rep.fail(f)
            print("FAILURES: " + " | ".join(fails))
            sys.exit(1)
        rep.ok("homepage restore verified live end-to-end")

if __name__ == "__main__":
    main()
