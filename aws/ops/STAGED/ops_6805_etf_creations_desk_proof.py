"""ops 6805 - live proof of the ETF creations families on the Inflows & outflows desk (commit f22fca00; feeds first
light ops 6804): in a fresh headless Chrome against justhodl.ai, the served bundle carries the five families, every
flows:etfc card resolves with a history strip from the fund's own feed (one fetch per fund), the GLD card prints the
packet's 5-day figure as of the packet's common end date, the full-screen view opens on it with the strip since 2004,
and the same inside chart.html's ⇄ Flows overlay (fresh context). Screenshots embedded. Read-only."""
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


def compact(v):
    a = abs(v)
    for unit, d in (("T", 1e12), ("B", 1e9), ("M", 1e6)):
        if a >= d:
            return ("-" if v < 0 else "") + ("%.2f" % (a / d)) + unit
    return "%.2f" % v


def main():
    from ops_report import report
    now = datetime.now(timezone.utc)
    fails, shots, facts = [], [], {}
    with report("ops_6805_etf_creations_desk_proof") as r:
        r.heading("ops 6805 - ETF creations on the Inflows & outflows desk, live proof %s" % now.isoformat(timespec="minutes"))

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
        packet = requests.get("%s/data/etf-true-flows.json?t=%d" % (PROXY, int(time.time())), timeout=60).json()
        end = packet["aggregation_period"]["end_date"]
        gld = packet["by_etf"]["GLD"]
        index = requests.get("%s/data/etf-creations/index.json?t=%d" % (PROXY, int(time.time())), timeout=60)
        idx = index.json() if index.ok else {}
        r.log("packet %s generated %s, common end %s; GLD 5d %s | index HTTP %s: %s funds generated %s" % (packet.get("version"), packet.get("generated_at"), end, gld.get("net_flow_5d_usd"), index.status_code, idx.get("n_funds"), idx.get("generated_at")))
        feed = requests.get("%s/data/etf-creations/GLD.json?t=%d" % (PROXY, int(time.time())), timeout=60)
        fd = feed.json() if feed.ok else {}
        r.log("GLD feed: HTTP %s, %d KB, %s dates %s -> %s, latest %s" % (feed.status_code, len(feed.content) // 1024, len(fd.get("d") or []), (fd.get("d") or [""])[0], (fd.get("d") or [""])[-1], json.dumps(fd.get("latest"))))
        check("index and GLD feed are this packet's", lambda: idx.get("generated_at") == packet["generated_at"] and fd.get("generated_at") == packet["generated_at"] or (_ for _ in ()).throw(AssertionError((idx.get("generated_at"), fd.get("generated_at")))))
        facts["gld_feed"] = "%s dates, latest %s" % (len(fd.get("d") or []), json.dumps(fd.get("latest")))

        r.section("2. the live desk in a fresh headless Chrome")
        sync_playwright = ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6805")
            page = ctx.new_page()
            page.set_default_timeout(45000)
            errors, requests_seen = [], []
            page.on("pageerror", lambda e: errors.append(str(e)[:160]))
            page.on("request", lambda rq: requests_seen.append(rq.url))
            page.goto("%s/inflows-outflows.html?nocache=%d" % (SITE, int(time.time())), wait_until="load")
            page.wait_for_selector("#macro-search-input", timeout=45000)
            check("the served bundle carries the ETF creations families", lambda: page.evaluate("""async () => { const s = [...document.scripts].map(x => x.src).find(u => /jh-workspaces[.]js/.test(u)); const t = await (await fetch(s, {cache: 'no-store'})).text(); return (t.match(/flows:etfc\\//g) || []).length; }""") >= 70 or (_ for _ in ()).throw(AssertionError("bundle without the families")))
            page.wait_for_selector(".macro-item", timeout=45000)
            page.wait_for_timeout(1200)
            page.evaluate(SCROLL_ALL)
            try:
                page.wait_for_function(ALL_SETTLED, timeout=240000)
            except Exception:  # noqa: BLE001
                r.log("not every card settled within 240 s")
            page.wait_for_timeout(2000)
            cards = page.evaluate(CARDS_JS)
            by = {c["sym"]: c for c in cards}
            etfc = [c for c in cards if (c["sym"] or "").startswith("flows:etfc/")]
            ready = [c for c in etfc if c["state"] == "ready"]
            bad = [c for c in etfc if c["state"] != "ready"]
            r.log("%d cards on the desk, %d ETF creations cards, %d ready, %d not: %s" % (len(cards), len(etfc), len(ready), len(bad), "; ".join("%s [%s] %s" % (c["sym"], c["state"], c["err"][:60]) for c in bad)[:900]))
            r.log("sample: %s" % "; ".join("%s=%s @%s" % (c["sym"].split("/")[-1].split(".")[0], c["value"], c["asof"]) for c in etfc[:24]))
            check("every ETF creations card resolves with a strip (73 expected)", lambda: len(etfc) == 73 and not bad and all(c["strip"] for c in ready) or (_ for _ in ()).throw(AssertionError((len(etfc), [c["sym"] for c in bad][:10], [c["sym"] for c in ready if not c["strip"]][:10]))))
            check("every card is as of the packet's common end date %s" % end, lambda: all(c["asof"] == end for c in ready) or (_ for _ in ()).throw(AssertionError(sorted({c["asof"] for c in ready}))))
            g = by.get("flows:etfc/GLD.flow_5d") or {}
            r.log("GLD card: %s" % json.dumps(g)[:300])
            check("GLD card prints the packet's 5-day figure (%s)" % compact(gld["net_flow_5d_usd"]), lambda: g.get("value") == compact(gld["net_flow_5d_usd"]) or (_ for _ in ()).throw(AssertionError((g.get("value"), compact(gld["net_flow_5d_usd"])))))
            counts = {}
            for u in requests_seen:
                if "/data/etf-creations/" in u:
                    k = u.split("/data/etf-creations/")[1].split("?")[0]
                    counts[k] = counts.get(k, 0) + 1
            r.log("feed fetches: %d funds, max per fund %s" % (len(counts), max(counts.values()) if counts else None))
            check("one fetch per fund feed", lambda: len(counts) == 73 and max(counts.values()) == 1 or (_ for _ in ()).throw(AssertionError((len(counts), max(counts.values()) if counts else None))))
            check("no page errors", lambda: not errors or (_ for _ in ()).throw(AssertionError(errors[:3])))
            facts["desk"] = "%d cards, %d etf creations ready" % (len(cards), len(ready))
            page.evaluate("() => { const c = [...document.querySelectorAll('.macro-item')].find(x => x.dataset.symbol === 'flows:etfc/GLD.flow_5d'); if (c) c.scrollIntoView({block: 'start'}); }")
            page.wait_for_timeout(600)
            shots.append(("desk_etf_creations", page.screenshot(type="jpeg", quality=55)))
            page.evaluate("() => { const c = [...document.querySelectorAll('.macro-item')].find(x => x.dataset.symbol === 'flows:etfc/GLD.flow_5d'); c.click(); }")
            page.wait_for_timeout(2000)
            fs = page.evaluate("""() => { const f = document.getElementById('macro-focus'); return {open: f.classList.contains('open'), title: f.querySelector('#macro-focus-title').textContent, latest: (f.querySelector('#macro-focus-latest') || {}).textContent || '', text: f.innerText.slice(0, 400)}; }""")
            r.log("full screen: %s" % json.dumps(fs)[:500])
            check("full screen opens on the GLD card", lambda: fs["open"] and "GLD" in fs["title"] or (_ for _ in ()).throw(AssertionError(fs)))
            shots.append(("focus_gld", page.screenshot(type="jpeg", quality=55)))
            page.keyboard.press("Escape")
            ctx.close()

            r.section("3. chart.html's ⇄ Flows overlay (fresh context)")
            ctx2 = browser.new_context(viewport={"width": 1440, "height": 900}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6805")
            page2 = ctx2.new_page()
            page2.set_default_timeout(45000)
            page2.goto("%s/chart.html?nocache=%d" % (SITE, int(time.time())), wait_until="load")
            page2.wait_for_selector("#btn-flows", timeout=45000)
            page2.evaluate("() => document.getElementById('btn-flows').click()")
            try:
                page2.wait_for_function("() => [...document.querySelectorAll('iframe')].some(f => /inflows-outflows/.test(f.src || ''))", timeout=30000)
            except Exception:  # noqa: BLE001
                r.log("overlay iframe did not appear within 30 s; frames %s" % [f.url[:60] for f in page2.frames])
            page2.wait_for_timeout(1500)
            frame = next((f for f in page2.frames if "inflows-outflows" in f.url), None)
            check("overlay frame is /inflows-outflows.html", lambda: frame is not None or (_ for _ in ()).throw(AssertionError([f.url for f in page2.frames])))
            if frame is not None:
                frame.wait_for_selector(".macro-item", timeout=45000)
                frame.evaluate(SCROLL_ALL)
                try:
                    frame.wait_for_function(ALL_SETTLED, timeout=240000)
                except Exception:  # noqa: BLE001
                    r.log("overlay: not every card settled within 240 s")
                cards2 = frame.evaluate(CARDS_JS)
                etfc2 = [c for c in cards2 if (c["sym"] or "").startswith("flows:etfc/")]
                r.log("overlay: %d cards, %d ETF creations cards, %d ready" % (len(cards2), len(etfc2), sum(1 for c in etfc2 if c["state"] == "ready")))
                check("overlay: the ETF creations cards are ready", lambda: len(etfc2) == 73 and all(c["state"] == "ready" for c in etfc2) or (_ for _ in ()).throw(AssertionError([c["sym"] for c in etfc2 if c["state"] != "ready"][:10])))
                frame.evaluate("() => { const c = [...document.querySelectorAll('.macro-item')].find(x => x.dataset.symbol === 'flows:etfc/TLT.flow_5d'); if (c) c.scrollIntoView({block: 'center'}); }")
                page2.wait_for_timeout(600)
                shots.append(("overlay_etf_creations", page2.screenshot(type="jpeg", quality=55)))
            ctx2.close()
            browser.close()
        for name, blob in shots:
            r.section("screenshot: %s" % name)
            r.log("<!-- SCREENSHOT_%s_JPEG_BASE64 -->\n%s\n<!-- /SCREENSHOT_%s_JPEG_BASE64 -->" % (name, base64.b64encode(blob).decode(), name))
        r.kv(**{k: str(v) for k, v in facts.items()}, fails=str(fails))
        if fails:
            r.fail("desk proof failed: %s" % fails)
        else:
            r.ok("73 ETF creations cards live on the desk and in the chart overlay, each from its fund's own issuer-tape feed")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
