"""ops 6784 - end-to-end proof that the ICI weekly engine is un-stalled (Khalid 2026-11-11: "Do both"), from the data a
visitor actually gets. The Chrome lane's first run (workflow run 42973347141, 06:34 UTC) reported a native publication
and 5 new weeks; this op verifies from the outside, after the pages deploy of 495c22b7: (1) data/ici-fund-flows.json
through the Worker - generated today by ici-flows-lane, 91 weeks, latest 2026-10-28 with the release values
(total -36,549 $M), combined / money-market / ETF-issuance histories present, no seed-only sources, no errors;
(2) data/ici-flows.json exists at last (ici-research.v1, generated today, reconciliation clean); (3) the live
/inflows-outflows.html desk in headless Chrome: the ICI cards resolve with the new week, the new families (Mutual
funds + ETFs, ETF net issuance, ICI money market) resolve, and the bundle served is the new one; (4) chart.html's
⇄ Flows overlay shows the same (fresh context). Screenshots embedded. Read-only."""
from datetime import datetime, timezone
from pathlib import Path
import base64
import json
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
  cap: (c.querySelector('.macro-hist-cap') || {}).textContent || ''}))"""
SCROLL_ALL = """async () => { const body = document.getElementById('macro-body'); for (let y = 0; y <= body.scrollHeight; y += 450) { body.scrollTop = y; await new Promise(r => setTimeout(r, 90)); } body.scrollTop = 0; }"""
EXPECT = {"flows:ici/total": ("2026-10-28", -36549.0), "flows:ici/equity": ("2026-10-28", -22545.0), "flows:ici/bond": ("2026-10-28", -11068.0),
          "flows:ici-combined/total": ("2026-10-28", 9356.0), "flows:ici-combined/equity": ("2026-10-28", 11074.0),
          "flows:ici-etf/total": ("2026-10-28", 45905.0), "flows:ici-mmf/total": ("2026-11-04", 8015.74), "flows:ici-mmf/government": ("2026-11-04", 6622.38)}


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


def num(text):
    t = str(text).replace(",", "").replace("$", "").replace("−", "-").strip()
    for suffix, mult in (("B", 1e9), ("M", 1e6), ("K", 1e3), ("k", 1e3)):
        if t.endswith(suffix):
            return float(t[:-1]) * mult
    return float(t)


def main():
    from ops_report import report
    now = datetime.now(timezone.utc)
    fails, shots = [], []
    facts = {}
    with report("ops_6784_ici_lane_desk_proof") as r:
        r.heading("ops 6784 - ICI weekly un-stall proof from the visitor's side %s" % now.isoformat(timespec="minutes"))

        def check(name, fn):
            try:
                out = fn()
                r.log("ok   %s%s" % (name, (": " + str(out)[:600]) if out not in (None, True) else ""))
                return out
            except Exception as e:  # noqa: BLE001
                fails.append(name)
                r.warn("FAIL %s: %s %s" % (name, type(e).__name__, str(e)[:400]))
                return None

        r.section("1. the desk feed through the Worker")
        feed = requests.get("%s/data/ici-fund-flows.json?t=%d" % (PROXY, int(time.time())), timeout=60).json()
        hist = feed.get("history") or []
        r.log("generated_at %s | engine %s | count %s | latest_week %s | sources %s | errors %s | lane %s" % (
            feed.get("generated_at"), feed.get("engine"), feed.get("count"), json.dumps(feed.get("latest_week"))[:300], feed.get("sources"), feed.get("errors"), json.dumps(feed.get("lane"))[:200]))
        check("feed generated today by the lane", lambda: str(feed.get("generated_at", ""))[:10] == now.date().isoformat() and feed.get("engine") == "ici-flows-lane" or (_ for _ in ()).throw(AssertionError(feed.get("generated_at"))))
        check("91 weeks, latest 2026-10-28, total -36,549 $M", lambda: (feed["count"], hist[-1]["week_ending"], hist[-1]["total"]) == (91, "2026-10-28", -36549.0) or (_ for _ in ()).throw(AssertionError((feed["count"], hist[-1]))))
        check("the five new weeks carry every series", lambda: all(all(k in row and row[k] is not None for k in ("equity", "equity_domestic", "equity_world", "hybrid", "bond", "bond_taxable", "bond_municipal", "total")) for row in hist[-5:]) or (_ for _ in ()).throw(AssertionError(hist[-5:])))
        check("combined 5 weeks (latest total 9,356), money market 3 weeks (latest 8,015.74 $B), ETF issuance 5 weeks (latest 45,905)", lambda: (
            len(feed["combined_history"]), feed["combined_history"][-1]["total"], len(feed["money_market_history"]), feed["money_market_history"][-1]["total"], len(feed["etf_net_issuance_history"]), feed["etf_net_issuance_history"][-1]["total"]) == (5, 9356.0, 3, 8015.74, 5, 45905.0)
            or (_ for _ in ()).throw(AssertionError((feed["combined_history"][-1], feed["money_market_history"][-1], feed["etf_net_issuance_history"][-1]))))
        check("seeded weeks untouched (2026-09-23 total -19,668)", lambda: next(x for x in hist if x["week_ending"] == "2026-09-23")["total"] == -19668.0)
        check("no errors field, sources = seed + chrome-lane", lambda: "errors" not in feed and feed["sources"][0].startswith("seed:") and feed["sources"][-1].startswith("chrome-lane:2026-11-04") or (_ for _ in ()).throw(AssertionError(feed.get("sources"))))
        facts["feed"] = "%s weeks, latest %s, generated %s" % (feed.get("count"), hist[-1]["week_ending"] if hist else None, str(feed.get("generated_at"))[:16])

        r.section("2. the native packet, published for the first time")
        resp = requests.get("%s/data/ici-flows.json?t=%d" % (PROXY, int(time.time())), timeout=60)
        r.log("data/ici-flows.json: HTTP %s, %d bytes" % (resp.status_code, len(resp.content)))
        if resp.ok:
            packet = resp.json()
            q = packet.get("quality") or {}
            r.log("contract %s | generated_at %s | quality %s | mmf total %s on %s | long_term equity 4w %s | replay %s" % (
                packet.get("contract"), packet.get("generated_at"), json.dumps(q)[:200], (packet.get("mmf") or {}).get("total_b"), (packet.get("mmf") or {}).get("date"),
                (packet.get("long_term") or {}).get("equity_sum_4w_m"), json.dumps(packet.get("replay"))[:160]))
            check("ici-research.v1 generated today, measurements reconciled", lambda: packet.get("contract") == "ici-research.v1" and str(packet.get("generated_at", ""))[:10] == now.date().isoformat() and q.get("status") == "dated_measurements" or (_ for _ in ()).throw(AssertionError((packet.get("contract"), packet.get("generated_at"), q))))
            facts["native"] = "%s %s" % (packet.get("contract"), str(packet.get("generated_at"))[:16])
        else:
            fails.append("native packet absent")

        r.section("3. the live desk in headless Chrome (fresh context)")
        sync_playwright = ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6784")
            page = ctx.new_page()
            page.set_default_timeout(45000)
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:160]))
            page.goto("%s/inflows-outflows.html?nocache=%d" % (SITE, int(time.time())), wait_until="load")
            page.wait_for_selector("#macro-search-input", timeout=45000)
            check("the served bundle carries the new ICI feeds", lambda: page.evaluate("""async () => { const s = [...document.scripts].map(x => x.src).find(u => /jh-workspaces[.]js/.test(u)); const t = await (await fetch(s, {cache: 'no-store'})).text(); if (!/ici-combined/.test(t) || !/etf_net_issuance_history/.test(t)) throw new Error('bundle lacks the new feeds: ' + s); return s.replace(location.origin, ''); }"""))
            page.wait_for_selector(".macro-item", timeout=45000)
            page.wait_for_timeout(1200)
            page.evaluate(SCROLL_ALL)
            try:
                page.wait_for_function(ALL_SETTLED, timeout=150000)
            except Exception:  # noqa: BLE001
                r.log("not every card settled within 150 s")
            page.wait_for_timeout(2000)
            cards = page.evaluate(CARDS_JS)
            by = {c["sym"]: c for c in cards}
            ici = [c for c in cards if str(c["sym"]).startswith("flows:ici")]
            r.log("%d cards, %d ICI cards: %s" % (len(cards), len(ici), "; ".join("%s=%s @%s [%s]" % (c["sym"].split("/")[-1] if "/" in c["sym"] else c["sym"], c["value"], c["asof"], c["state"]) for c in ici)[:1500]))
            for sym, (asof, value) in EXPECT.items():
                c = by.get(sym)
                check("card %s ready, as of %s, value %s" % (sym, asof, value), lambda c=c, asof=asof, value=value, sym=sym: (
                    c is not None and c["state"] == "ready" and asof in c["asof"] and abs(num(c["value"]) - value) <= max(1.0, abs(value) * 0.002) and c["strip"]) or (_ for _ in ()).throw(AssertionError(c)))
            check("no page errors", lambda: not errors or (_ for _ in ()).throw(AssertionError(errors[:3])))
            # scroll the ICI family into view for the screenshot
            page.evaluate("() => { const c = [...document.querySelectorAll('.macro-item')].find(x => x.dataset.symbol === 'flows:ici/total'); if (c) c.scrollIntoView({block: 'start'}); }")
            page.wait_for_timeout(600)
            shots.append(("desk_ici", page.screenshot(type="jpeg", quality=55)))
            # the full-screen view of the ICI total: the strip now reaches 2026-10-28
            page.evaluate("() => { const c = [...document.querySelectorAll('.macro-item')].find(x => x.dataset.symbol === 'flows:ici/total'); c.click(); }")
            page.wait_for_timeout(1500)
            fs = page.evaluate("""() => { const f = document.getElementById('macro-focus'); return {open: f.classList.contains('open'), title: f.querySelector('#macro-focus-title').textContent, latest: f.querySelector('#macro-focus-latest').innerText.replace(/\\n/g, ' '), rows: f.querySelectorAll('tbody tr').length}; }""")
            check("full screen opens on the ICI total with the new week", lambda: fs["open"] and "2026-10-28" in fs["latest"] or (_ for _ in ()).throw(AssertionError(fs)))
            shots.append(("focus_ici", page.screenshot(type="jpeg", quality=55)))
            page.keyboard.press("Escape")
            ctx.close()

            r.section("4. chart.html's ⇄ Flows overlay (fresh context)")
            ctx2 = browser.new_context(viewport={"width": 1440, "height": 900}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6784")
            page2 = ctx2.new_page()
            page2.set_default_timeout(45000)
            page2.goto("%s/chart.html?nocache=%d" % (SITE, int(time.time())), wait_until="load")
            page2.wait_for_selector("#btn-flows", timeout=45000)
            page2.evaluate("() => document.getElementById('btn-flows').click()")
            page2.wait_for_timeout(2500)
            frame = next((f for f in page2.frames if "inflows-outflows" in f.url), None)
            check("overlay frame is /inflows-outflows.html", lambda: frame is not None and frame.url.split("?")[0].endswith("/inflows-outflows.html") or (_ for _ in ()).throw(AssertionError([f.url for f in page2.frames])))
            if frame is not None:
                frame.wait_for_selector(".macro-item", timeout=45000)
                frame.evaluate(SCROLL_ALL)
                try:
                    frame.wait_for_function(ALL_SETTLED, timeout=150000)
                except Exception:  # noqa: BLE001
                    r.log("overlay: not every card settled within 150 s")
                cards2 = frame.evaluate(CARDS_JS)
                by2 = {c["sym"]: c for c in cards2}
                c = by2.get("flows:ici/total")
                check("overlay ICI total ready, as of 2026-10-28, -36,549", lambda: c and c["state"] == "ready" and "2026-10-28" in c["asof"] and abs(num(c["value"]) + 36549) <= 1 or (_ for _ in ()).throw(AssertionError(c)))
                c2 = by2.get("flows:ici-mmf/total")
                check("overlay ICI money market total ready, as of 2026-11-04", lambda: c2 and c2["state"] == "ready" and "2026-11-04" in c2["asof"] or (_ for _ in ()).throw(AssertionError(c2)))
                frame.evaluate("() => { const c = [...document.querySelectorAll('.macro-item')].find(x => x.dataset.symbol === 'flows:ici-combined/total'); if (c) c.scrollIntoView({block: 'center'}); }")
                page2.wait_for_timeout(600)
                shots.append(("overlay_ici", page2.screenshot(type="jpeg", quality=55)))
            ctx2.close()
            browser.close()
        for name, blob in shots:
            r.section("screenshot: %s" % name)
            r.log("<!-- SCREENSHOT_%s_JPEG_BASE64 -->\n%s\n<!-- /SCREENSHOT_%s_JPEG_BASE64 -->" % (name, base64.b64encode(blob).decode(), name))
        r.kv(**{k: str(v) for k, v in facts.items()}, fails=str(fails))
        if fails:
            r.fail("%d check(s) failed: %s" % (len(fails), fails))
        else:
            r.ok("ICI weekly un-stalled end to end: lane feed + native packet + live desk + overlay")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
