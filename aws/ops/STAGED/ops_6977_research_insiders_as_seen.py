"""ops 6977 - read-only: what the Research and Insiders tabs show when opened from the chart, and what fails.

Khalid 2026-11-26: "look into : research. insiders, they seem broken". This op opens each tab from chart.html in real
Chrome exactly as he does, waits, and records what is on screen at the top of the page (a picture and the first text),
every script error, every request that failed or answered 4xx / 5xx, and a few facts about the page's own content.
It also attaches the documents behind Liquidity, Plumbing, Breadth, Calendar and Crypto for the page lab.

Nothing is written. Safe to run again.
"""
from pathlib import Path
import base64
import gzip
import importlib.util
import json
import sys
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "aws" / "ops"))
NAME = "ops_6977_research_insiders_as_seen"
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev/"
DOCS = ["data/plumbing-stress.json", "data/plumbing-history.json", "liquidity-data.json", "data/liquidity-flow.json", "data/breadth-thrust.json", "data/breadth-history.json",
        "data/catalyst-calendar.json", "data/onchain-ratios.json", "data/cryptoquant-onchain.json", "data/insider-trades.json"]
FACTS = r"""() => { const t = (document.body.innerText || '').replace(/\n{2,}/g, '\n'); const vis = (e) => { if (!e) return false; const r = e.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
  return { url: location.href, title: document.title, chars: t.length, head: t.slice(0, 1400), scrollH: document.documentElement.scrollHeight,
    content: (document.getElementById('content') || {}).innerText ? document.getElementById('content').innerText.slice(0, 500) : null,
    loaders: Array.from(document.querySelectorAll('.loader,.loading,.spinner,[class*=skeleton]')).filter(vis).map(e => (e.innerText || e.className).slice(0, 80)).slice(0, 12),
    failed: Array.from(document.querySelectorAll('body *')).filter(e => e.children.length === 0 && /fail|error|unavailable|could not|not load|NaN|undefined/i.test(e.textContent || '') && vis(e)).map(e => e.textContent.trim().slice(0, 110)).slice(0, 30),
    tables: document.querySelectorAll('table').length, rows: document.querySelectorAll('tbody tr').length, svgs: document.querySelectorAll('svg').length, canvases: document.querySelectorAll('canvas').length,
    fixed: Array.from(document.querySelectorAll('body *')).filter(e => getComputedStyle(e).position === 'fixed' && vis(e)).map(e => (e.id || e.className || e.tagName).toString().slice(0, 40) + ': ' + (e.innerText || '').trim().slice(0, 50)).slice(0, 14),
    ticker: (window.__JH_TICKER_BUS && window.__JH_TICKER_BUS.current) ? window.__JH_TICKER_BUS.current() : null, input: (document.querySelector('input') || {}).value || null }; }"""


def fetch(url, timeout=120):
    req = urllib.request.Request(url if url.startswith("http") else BASE + url, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw


def trim(node, cap):
    if isinstance(node, dict):
        return {k: trim(v, cap) for k, v in node.items() if str(k).lower() not in ("cusip", "isin")}
    if isinstance(node, list):
        return [trim(v, cap) for v in node[:cap]]
    return node


def main():
    from ops_report import report
    spec = importlib.util.spec_from_file_location("ops_6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    stamp = int(time.time())
    shots, pack = {}, {}
    with report(NAME) as r:
        r.heading("6977 - Research and Insiders as opened from the chart (read-only)")
        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)
            for label, bid, path, wait_ms in (("Research", "btn-rsch", "/why.html", 14000), ("Insiders", "btn-ins", "/insiders.html", 9000)):
                r.section("%s, opened from the chart on NVDA" % label)
                ctx = browser.new_context(viewport={"width": 1500, "height": 1000}, user_agent=UA)
                page = ctx.new_page()
                errs, bad, cons = [], [], []
                page.on("pageerror", lambda e, errs=errs: errs.append(str(e)[:220]))
                page.on("console", lambda m, cons=cons: cons.append(m.text[:200]) if m.type == "error" else None)
                page.on("requestfailed", lambda q, bad=bad: bad.append("FAILED %s %s" % (q.url.split("?")[0][:120], (q.failure or "")[:60])))
                page.on("response", lambda s, bad=bad: bad.append("%d %s" % (s.status, s.url.split("?")[0][:120])) if s.status >= 400 else None)
                try:
                    page.goto("%s/chart.html?s=NVDA&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
                    page.wait_for_selector("#" + bid, timeout=90000, state="attached")
                    page.wait_for_function("() => (window.lastBars || []).length > 300 && typeof window.jhOpenWorkspace === 'function'", timeout=90000)
                    n0 = (len(errs), len(bad), len(cons))
                    page.mouse.move(700, 3)
                    page.wait_for_timeout(900)
                    page.evaluate("(id) => document.getElementById(id).scrollIntoView({ block: 'nearest' })", bid)
                    try:
                        page.click("#" + bid, timeout=6000)
                    except Exception:  # noqa: BLE001
                        page.evaluate("(id) => document.getElementById(id).click()", bid)
                    page.wait_for_selector("#ws-overlay.on iframe", timeout=20000)
                    frame = None
                    for _ in range(80):
                        frame = next((f for f in page.frames if f != page.main_frame and path in f.url), None)
                        if frame:
                            break
                        page.wait_for_timeout(250)
                    page.wait_for_timeout(wait_ms)
                    facts = frame.evaluate(FACTS)
                    shots["%s-in-the-chart" % label.lower()] = page.screenshot(type="jpeg", quality=62)
                    frame.evaluate("() => window.scrollTo(0, 900)")
                    page.wait_for_timeout(700)
                    shots["%s-in-the-chart-lower" % label.lower()] = page.screenshot(type="jpeg", quality=55)
                    r.log("frame %s | title %s | %s characters, page height %s | tables %s, rows %s, svg %s, canvas %s | ticker on the bus: %s, input: %s" % (facts["url"], facts["title"], format(facts["chars"], ","), facts["scrollH"], facts["tables"], facts["rows"], facts["svgs"], facts["canvases"], facts["ticker"], facts["input"]))
                    r.log("first text: " + json.dumps(facts["head"], ensure_ascii=False)[:1500])
                    if facts["content"] is not None:
                        r.log("#content: " + json.dumps(facts["content"], ensure_ascii=False)[:520])
                    r.log("still loading: " + json.dumps(facts["loaders"], ensure_ascii=False)[:500])
                    r.log("texts that read as a failure: " + json.dumps(facts["failed"], ensure_ascii=False)[:1500])
                    r.log("fixed on screen: " + json.dumps(facts["fixed"], ensure_ascii=False)[:900])
                    r.log("script errors after the click (%d): %s" % (len(errs) - n0[0], json.dumps(errs[n0[0]:][:10], ensure_ascii=False)[:1200]))
                    r.log("console errors after the click (%d): %s" % (len(cons) - n0[2], json.dumps(sorted(set(cons[n0[2]:]))[:14], ensure_ascii=False)[:1500]))
                    r.log("requests that failed after the click (%d): %s" % (len(bad) - n0[1], json.dumps(sorted(set(bad[n0[1]:]))[:30], ensure_ascii=False)[:2400]))
                    pack[label] = {"facts": facts, "errors": errs[n0[0]:], "console": cons[n0[2]:][:60], "bad": sorted(set(bad[n0[1]:]))}
                except Exception as exc:  # noqa: BLE001
                    r.log("%s raised %s: %s" % (label, type(exc).__name__, str(exc)[:300]))
                ctx.close()
            browser.close()

        r.section("documents for the page lab")
        docs = {}
        for key in DOCS:
            try:
                raw = fetch((PROXY + key) if not key.startswith("data/") else "/%s?probe=%d" % (key, stamp))
                doc = json.loads(raw)
                docs[key] = trim(doc, 400 if key not in ("data/insider-trades.json",) else 60)
                r.log("%s | %s bytes" % (key, format(len(raw), ",")))
            except Exception as exc:  # noqa: BLE001
                r.log("%s | not read: %s" % (key, str(exc)[:120]))
        pack["docs"] = docs
        body = gzip.compress(json.dumps(pack, separators=(",", ":"), default=str).encode(), 9)
        b64 = base64.b64encode(body).decode("ascii")
        r.lines.append("<!-- FIXTURE seen %d bytes" % len(body))
        r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
        r.lines.append("FIXTURE-END -->")
        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(tabs=2, documents=len(docs), attachment_bytes=len(body))
        r.ok("recorded; nothing written")


if __name__ == "__main__":
    main()
