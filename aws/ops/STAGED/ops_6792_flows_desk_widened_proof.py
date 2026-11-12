"""ops 6792 - live proof of the widened Inflows & outflows desk (commit 953795ff; stablecoin history first light ops
6791): in a fresh headless Chrome against justhodl.ai, every card of the desk resolves - in particular the 24 new ones:
10 TIC net-purchase series (FRED monthly, latest 2026-08), 10 CryptoQuant daily exchange-flow / reserve / ratio series
(columnar feed, latest within 3 days), 4 DefiLlama stablecoin-supply series (latest today or yesterday, printed as
B) - each with a history strip; then the same inside chart.html's ⇄ Flows overlay (fresh context). Also counts the
network cost of the new feeds (one fetch per feed URL). Screenshots embedded. Read-only."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import base64
import json
import re
import subprocess
import sys
import time

import requests

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
SITE = "https://justhodl.ai"
ALL_SETTLED = "() => [...document.querySelectorAll('.macro-item')].length > 0 && [...document.querySelectorAll('.macro-item')].every(c => c.dataset.metricState === 'ready' || c.dataset.metricState === 'error')"
CARDS_JS = """() => [...document.querySelectorAll('.macro-item')].map(c => ({sym: c.dataset.symbol, title: c.querySelector('.mi-title').textContent, state: c.dataset.metricState,
  value: c.querySelector('.macro-latest-value').textContent, asof: c.querySelector('.macro-asof').textContent, err: c.querySelector('.macro-latest-value').title || '', strip: !!c.querySelector('.macro-hist-chart'),
  provider: (c.querySelector('.mi-meta span:last-child') || {}).textContent || ''}))"""
SCROLL_ALL = """async () => { const body = document.getElementById('macro-body'); for (let y = 0; y <= body.scrollHeight; y += 450) { body.scrollTop = y; await new Promise(r => setTimeout(r, 90)); } body.scrollTop = 0; }"""
TIC = ["FRED:FORLTTOTALNET99996", "FRED:FORLTTREASNET99996", "FRED:FORLTEQTYNET99996", "FRED:FORLTCORPNET99996", "FRED:FORLTAGCYNET99996", "FRED:FORLTTOTALNET99990", "FRED:FORLTTOTALNET99991", "FRED:FORLTTREASNET41408", "FRED:FORLTEQTYNET42609", "FRED:FORLTEQTYNET41408"]
CQ = ["flows:cq/btc_exchange_netflow", "flows:cq/btc_exchange_inflow", "flows:cq/btc_exchange_outflow", "flows:cq/btc_exchange_reserve", "flows:cq/eth_exchange_netflow", "flows:cq/eth_exchange_reserve", "flows:cq/btc_whale_ratio", "flows:cq/btc_stablecoins_ratio", "flows:cq/btc_miner_netflow", "flows:cq/btc_fund_flow_ratio"]
STABLE = ["flows:stablecoins/total", "flows:stablecoins/usdt", "flows:stablecoins/usdc", "flows:stablecoins/minted"]


def ensure_playwright(log):
    try:
        import playwright  # noqa: F401
    except Exception:  # noqa: BLE001
        log("installing playwright")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
    from playwright.sync_api import sync_playwright
    return sync_playwright


def launch(p, log):
    try:
        return p.chromium.launch(channel="chrome", headless=True)
    except Exception as e:  # noqa: BLE001
        log("chrome channel unavailable (%s); installing chromium" % type(e).__name__)
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
        return p.chromium.launch(headless=True)


def main():
    from ops_report import report
    now = datetime.now(timezone.utc)
    fails, shots, facts = [], [], {}
    with report("ops_6792_flows_desk_widened_proof") as r:
        r.heading("ops 6792 - widened Inflows & outflows desk, live proof %s" % now.isoformat(timespec="minutes"))

        def check(name, fn):
            try:
                out = fn()
                r.log("ok   %s%s" % (name, (": " + str(out)[:600]) if out not in (None, True) else ""))
                return out
            except Exception as e:  # noqa: BLE001
                fails.append(name)
                r.warn("FAIL %s: %s %s" % (name, type(e).__name__, str(e)[:400]))
                return None

        r.section("1. the feeds through the Worker")
        st = requests.get("%s/data/stablecoin-supply-history.json?t=%d" % (PROXY, int(time.time())), timeout=60)
        doc = st.json() if st.ok else {}
        latest = (doc.get("series") or [{}])[-1]
        r.log("stablecoin-supply-history: HTTP %s, %d KB, %s days, latest %s" % (st.status_code, len(st.content) // 1024, doc.get("count"), json.dumps(latest)[:200]))
        check("stablecoin history: >= 3000 days, latest today/yesterday", lambda: doc.get("count", 0) >= 3000 and latest.get("date") in (now.date().isoformat(), (now - timedelta(days=1)).date().isoformat()) or (_ for _ in ()).throw(AssertionError((doc.get("count"), latest.get("date")))))
        cq = requests.get("%s/data/cryptoquant-series.json?t=%d" % (PROXY, int(time.time())), timeout=90)
        cqd = cq.json() if cq.ok else {}
        r.log("cryptoquant-series: HTTP %s, %d KB, generated %s, %d series" % (cq.status_code, len(cq.content) // 1024, cqd.get("generated_at"), len(cqd.get("series") or {})))
        facts["feeds"] = "stablecoins %s days to %s; cq %d series" % (doc.get("count"), latest.get("date"), len(cqd.get("series") or {}))

        r.section("2. the live desk in a fresh headless Chrome")
        sync_playwright = ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6792")
            page = ctx.new_page()
            page.set_default_timeout(45000)
            errors, requests_seen = [], []
            page.on("pageerror", lambda e: errors.append(str(e)[:160]))
            page.on("request", lambda rq: requests_seen.append(rq.url))
            page.goto("%s/inflows-outflows.html?nocache=%d" % (SITE, int(time.time())), wait_until="load")
            page.wait_for_selector("#macro-search-input", timeout=45000)
            check("the served bundle carries the widened desk", lambda: page.evaluate("""async () => { const s = [...document.scripts].map(x => x.src).find(u => /jh-workspaces[.]js/.test(u)); const t = await (await fetch(s, {cache: 'no-store'})).text(); if (!/flows:cq\\//.test(t) || !/stablecoin-supply-history/.test(t) || !/FORLTTOTALNET99996/.test(t)) throw new Error('bundle lacks the widening: ' + s); return s.replace(location.origin, ''); }"""))
            page.wait_for_selector(".macro-item", timeout=45000)
            page.wait_for_timeout(1200)
            page.evaluate(SCROLL_ALL)
            try:
                page.wait_for_function(ALL_SETTLED, timeout=180000)
            except Exception:  # noqa: BLE001
                r.log("not every card settled within 180 s")
            page.wait_for_timeout(2000)
            cards = page.evaluate(CARDS_JS)
            by = {c["sym"]: c for c in cards}
            ready = [c for c in cards if c["state"] == "ready"]
            bad = [c for c in cards if c["state"] != "ready"]
            r.log("%d cards, %d ready, %d not: %s" % (len(cards), len(ready), len(bad), "; ".join("%s [%s] %s" % (c["sym"], c["state"], c["err"][:60]) for c in bad)[:800]))
            check("every card on the desk resolves (88 expected)", lambda: len(cards) == 88 and not bad or (_ for _ in ()).throw(AssertionError((len(cards), [c["sym"] for c in bad]))))
            for group, syms, rule in (("TIC", TIC, lambda c: c["asof"] >= "2026-08-01" and c["provider"].startswith("FRED")), ("CryptoQuant", CQ, lambda c: c["asof"] >= (now - timedelta(days=4)).date().isoformat() and c["provider"] == "CryptoQuant daily"),
                                      ("stablecoins", STABLE, lambda c: c["asof"] >= (now - timedelta(days=2)).date().isoformat() and c["value"].endswith("B") and c["provider"] == "DefiLlama daily")):
                rows = [by.get(s) for s in syms]
                r.log("%s: %s" % (group, "; ".join("%s=%s @%s" % (s.split("/")[-1] if "/" in s else s.split(":")[-1], c["value"], c["asof"]) if c else s + "=MISSING" for s, c in zip(syms, rows))[:900]))
                check("%s: all %d cards ready, fresh, with strips" % (group, len(syms)), lambda rows=rows, rule=rule: all(c and c["state"] == "ready" and c["strip"] and rule(c) for c in rows) or (_ for _ in ()).throw(AssertionError([(c["sym"], c["asof"], c["value"], c["provider"]) for c in rows if not (c and c["state"] == "ready" and c["strip"] and rule(c))])))
            feeds = sorted({u.split("?")[0].split("/data/")[-1] for u in requests_seen if "/data/" in u and ("cryptoquant-series" in u or "stablecoin-supply" in u or "ici-fund-flows" in u)})
            counts = {f: sum(1 for u in requests_seen if f in u) for f in feeds}
            r.log("feed fetches: %s" % counts)
            check("each flow feed fetched once", lambda: all(n == 1 for n in counts.values()) or (_ for _ in ()).throw(AssertionError(counts)))
            check("no page errors", lambda: not errors or (_ for _ in ()).throw(AssertionError(errors[:3])))
            page.evaluate("() => { const c = [...document.querySelectorAll('.macro-item')].find(x => x.dataset.symbol === 'flows:cq/btc_exchange_netflow'); if (c) c.scrollIntoView({block: 'start'}); }")
            page.wait_for_timeout(600)
            shots.append(("desk_crypto", page.screenshot(type="jpeg", quality=55)))
            page.evaluate("() => { const c = [...document.querySelectorAll('.macro-item')].find(x => x.dataset.symbol === 'FRED:FORLTTOTALNET99996'); if (c) c.scrollIntoView({block: 'start'}); }")
            page.wait_for_timeout(600)
            shots.append(("desk_tic", page.screenshot(type="jpeg", quality=55)))
            page.evaluate("() => { const c = [...document.querySelectorAll('.macro-item')].find(x => x.dataset.symbol === 'flows:stablecoins/total'); c.click(); }")
            page.wait_for_timeout(1500)
            fs = page.evaluate("""() => { const f = document.getElementById('macro-focus'); return {open: f.classList.contains('open'), title: f.querySelector('#macro-focus-title').textContent, latest: f.querySelector('#macro-focus-latest').innerText.replace(/\\n/g, ' ')}; }""")
            check("full screen opens on the stablecoin supply card", lambda: fs["open"] and "Stablecoin" in fs["title"] or (_ for _ in ()).throw(AssertionError(fs)))
            shots.append(("focus_stablecoins", page.screenshot(type="jpeg", quality=55)))
            page.keyboard.press("Escape")
            ctx.close()

            r.section("3. chart.html's ⇄ Flows overlay (fresh context)")
            ctx2 = browser.new_context(viewport={"width": 1440, "height": 900}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6792")
            page2 = ctx2.new_page()
            page2.set_default_timeout(45000)
            page2.goto("%s/chart.html?nocache=%d" % (SITE, int(time.time())), wait_until="load")
            page2.wait_for_selector("#btn-flows", timeout=45000)
            page2.evaluate("() => document.getElementById('btn-flows').click()")
            page2.wait_for_timeout(2500)
            frame = next((f for f in page2.frames if "inflows-outflows" in f.url), None)
            check("overlay frame is /inflows-outflows.html", lambda: frame is not None or (_ for _ in ()).throw(AssertionError([f.url for f in page2.frames])))
            if frame is not None:
                frame.wait_for_selector(".macro-item", timeout=45000)
                frame.evaluate(SCROLL_ALL)
                try:
                    frame.wait_for_function(ALL_SETTLED, timeout=180000)
                except Exception:  # noqa: BLE001
                    r.log("overlay: not every card settled within 180 s")
                cards2 = frame.evaluate(CARDS_JS)
                by2 = {c["sym"]: c for c in cards2}
                r.log("overlay: %d cards, %d ready" % (len(cards2), sum(1 for c in cards2 if c["state"] == "ready")))
                check("overlay: the new crypto, stablecoin and TIC cards are ready", lambda: all(by2.get(s) and by2[s]["state"] == "ready" for s in (CQ[0], STABLE[0], TIC[0])) or (_ for _ in ()).throw(AssertionError([by2.get(s) for s in (CQ[0], STABLE[0], TIC[0])])))
                frame.evaluate("() => { const c = [...document.querySelectorAll('.macro-item')].find(x => x.dataset.symbol === 'flows:stablecoins/total'); if (c) c.scrollIntoView({block: 'center'}); }")
                page2.wait_for_timeout(600)
                shots.append(("overlay_stablecoins", page2.screenshot(type="jpeg", quality=55)))
            ctx2.close()
            browser.close()
        for name, blob in shots:
            r.section("screenshot: %s" % name)
            r.log("<!-- SCREENSHOT_%s_JPEG_BASE64 -->\n%s\n<!-- /SCREENSHOT_%s_JPEG_BASE64 -->" % (name, base64.b64encode(blob).decode(), name))
        r.kv(**{k: str(v) for k, v in facts.items()}, fails=str(fails))
        if fails:
            r.fail("%d check(s) failed: %s" % (len(fails), fails))
        else:
            r.ok("widened desk live: 88 cards resolve on the desk and in the chart overlay")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
