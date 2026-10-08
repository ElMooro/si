"""ops_5093 -- global-cycle.html rendered in a REAL browser on the runner.

ops 5092 proved the engine, feed, edge and every dependency are green from
the runner's HTTP client, so whatever Khalid sees is client-side. This op
loads https://justhodl.ai/global-cycle.html and /global-cycle/ in headless
Chromium (Playwright; runner's Google Chrome via channel="chrome", falling
back to a downloaded Chromium), and records what a user actually gets:

  * console errors/warnings, uncaught page errors, failed/blocked requests
    (URL + status + failure text), and the request waterfall for the data,
    map-topology and CDN scripts
  * DOM facts after render: decisive text, hero values, freshness pill,
    ladder cell count, region tile count, world-map path counts with/without
    data, whether "Failed to load" is showing, right-rail/reskin injections
  * a desktop (1440x1000) and mobile (390x844) full-page screenshot per page,
    written to aws/ops/reports/latest/ so the runner auto-commits them and
    the fix session can look at them
  * a second desktop visit (service-worker now controlling) to see whether
    the SW changes anything

Gate (sys.exit(1)): any uncaught page error on global-cycle.html, or the
decisive text shows "Failed to load", or the map renders < 25 data paths.
"""
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

OUT_DIR = ROOT / "aws" / "ops" / "reports" / "latest"
PAGES = {
    "live": "https://justhodl.ai/global-cycle.html",
    "history": "https://justhodl.ai/global-cycle/",
}

DOM_PROBE = r"""
() => {
  const q = (id) => { const e = document.getElementById(id); return e ? e.textContent.trim().slice(0, 220) : null; };
  const paths = Array.from(document.querySelectorAll('#world-map svg path'));
  const withData = paths.filter(p => p.classList.contains('has-data')).length;
  const phases = {};
  paths.forEach(p => { const c = Array.from(p.classList).find(x => ['EXPANSION','RECOVERY','AT_RISK','RECESSION','UNKNOWN'].includes(x)); phases[c||'none'] = (phases[c||'none']||0)+1; });
  const svg = document.querySelector('#world-map svg');
  const mapEl = document.getElementById('world-map');
  return {
    title: document.title,
    decisive: q('decisiveText'), genTime: q('genTime'), ageStr: q('ageStr'),
    globalPhase: q('globalPhase'), globalCli: q('globalCli'),
    pctExpansion: q('pctExpansion'), pctRecovery: q('pctRecovery'), pctAtRisk: q('pctAtRisk'), pctRecession: q('pctRecession'),
    countryCount: q('countryCount'), weightCovered: q('weightCovered'), freshCount: q('freshCount'), freshSub: q('freshSub'),
    breadthCells: document.querySelectorAll('#breadthBar > div').length,
    ladderCells: document.querySelectorAll('#ladder .ladder-cell').length,
    regions: document.querySelectorAll('#regionGrid .region').length,
    tiles: document.querySelectorAll('#regionGrid .country-tile').length,
    mapPaths: paths.length, mapWithData: withData, mapPhases: phases,
    mapSvgViewBox: svg ? svg.getAttribute('viewBox') : null,
    mapClientWidth: mapEl ? mapEl.clientWidth : null,
    mapBox: mapEl ? (function(){ const r = mapEl.getBoundingClientRect(); return {w: Math.round(r.width), h: Math.round(r.height), top: Math.round(r.top)}; })() : null,
    hasFailedText: (document.body.innerText || '').includes('Failed to load'),
    bodyChars: (document.body.innerText || '').length,
    rightRail: !!document.querySelector('[class*="right-rail"], #right-rail, .jh-rail'),
    swController: !!(navigator.serviceWorker && navigator.serviceWorker.controller),
    d3: typeof window.d3, topojson: typeof window.topojson,
    dataLoaded: !!(window.DATA && window.DATA.by_country), worldLoaded: !!window.WORLD,
    firstVisibleText: (document.body.innerText || '').slice(0, 400),
  };
}
"""

HIST_PROBE = r"""
() => {
  const svgs = document.querySelectorAll('svg').length;
  const paths = document.querySelectorAll('svg path').length;
  return { title: document.title, svgs, paths, bodyChars: (document.body.innerText||'').length,
           hasFailedText: (document.body.innerText||'').includes('Failed'),
           firstVisibleText: (document.body.innerText||'').slice(0, 300) };
}
"""


def ensure_playwright(r):
    try:
        import playwright  # noqa: F401
        r.log("playwright already importable")
    except Exception:  # noqa: BLE001
        r.log("pip installing playwright")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)


def launch(p, r):
    try:
        b = p.chromium.launch(channel="chrome", headless=True)
        r.log("launched runner Google Chrome (channel=chrome)")
        return b
    except Exception as e:  # noqa: BLE001
        r.warn("channel=chrome failed: %s -- installing chromium" % str(e)[:120])
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
        b = p.chromium.launch(headless=True)
        r.log("launched downloaded Chromium")
        return b


def visit(browser, r, url, tag, viewport, probe_js, settle_ms=9000, context=None):
    own_ctx = context is None
    ctx = context or browser.new_context(viewport=viewport, user_agent=None, ignore_https_errors=True)
    page = ctx.new_page()
    console, errors, failed, responses = [], [], [], []
    page.on("console", lambda m: console.append({"type": m.type, "text": m.text[:300]}))
    page.on("pageerror", lambda e: errors.append(str(e)[:400]))
    page.on("requestfailed", lambda rq: failed.append({"url": rq.url[:160], "failure": (rq.failure or "")[:120]}))

    def on_resp(resp):
        try:
            u = resp.url
            if any(k in u for k in ("global-business-cycle", "world-atlas", "d3@7", "topojson-client", "justhodl.ai/global-cycle", ".js", "service-worker")):
                responses.append({"url": u[:150], "status": resp.status, "type": (resp.headers.get("content-type") or "")[:40]})
        except Exception:  # noqa: BLE001
            pass
    page.on("response", on_resp)
    t0 = time.time()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        try:
            page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:  # noqa: BLE001
            r.warn("%s: networkidle not reached in 20s (keep going)" % tag)
        page.wait_for_timeout(settle_ms)
        dom = page.evaluate(probe_js)
    except Exception as e:  # noqa: BLE001
        dom = {"error": str(e)[:300]}
    load_s = round(time.time() - t0, 1)
    shot = OUT_DIR / ("5093-%s.jpg" % tag)
    try:
        page.screenshot(path=str(shot), full_page=True, type="jpeg", quality=55)
        r.log("%s: screenshot %s (%d bytes)" % (tag, shot.name, shot.stat().st_size))
    except Exception as e:  # noqa: BLE001
        r.warn("%s: screenshot failed: %s" % (tag, str(e)[:120]))
    r.section("%s -- %s @ %dx%d (%.1fs)" % (tag, url, viewport["width"], viewport["height"], load_s))
    for k, v in dom.items():
        if k in ("firstVisibleText",):
            continue
        r.log("  DOM %s = %s" % (k, json.dumps(v)[:300]))
    r.log("  DOM firstVisibleText = %s" % json.dumps(dom.get("firstVisibleText"))[:420])
    r.log("  page errors (%d): %s" % (len(errors), json.dumps(errors)[:1200]))
    bad_console = [c for c in console if c["type"] in ("error", "warning")]
    r.log("  console errors/warnings (%d of %d): %s" % (len(bad_console), len(console), json.dumps(bad_console[:12])[:1800]))
    r.log("  failed requests (%d): %s" % (len(failed), json.dumps(failed[:12])[:1500]))
    key_resp = [x for x in responses if x["status"] != 200 or any(k in x["url"] for k in ("global-business-cycle", "world-atlas", "d3@7", "topojson"))]
    r.log("  key responses: %s" % json.dumps(key_resp[:20])[:1600])
    r.kv(visit=tag, load_s=load_s, page_errors=len(errors), console_err=len(bad_console), failed_req=len(failed),
         decisive=(dom.get("decisive") or dom.get("error") or "")[:80], map_paths=dom.get("mapPaths"),
         map_with_data=dom.get("mapWithData"), tiles=dom.get("tiles"), ladder=dom.get("ladderCells"),
         failed_text=dom.get("hasFailedText"), data_loaded=dom.get("dataLoaded"), world_loaded=dom.get("worldLoaded"))
    page.close()
    if own_ctx:
        ctx.close()
    return dom, errors, ctx if not own_ctx else None


def main():
    with report("5093-gbc-browser-render") as r:
        r.heading("ops 5093 -- global-cycle.html rendered in headless Chromium on the runner")
        r.log("started %s" % datetime.now(timezone.utc).isoformat(timespec="seconds"))
        ensure_playwright(r)
        from playwright.sync_api import sync_playwright
        verdict_fail = []
        with sync_playwright() as p:
            browser = launch(p, r)
            # 1) desktop first visit (no SW yet)
            dom1, err1, _ = visit(browser, r, PAGES["live"], "live-desktop", {"width": 1440, "height": 1000}, DOM_PROBE)
            # 2) same context second visit -> SW controlling
            ctx = browser.new_context(viewport={"width": 1440, "height": 1000})
            visit(browser, r, PAGES["live"], "live-desktop-warm1", {"width": 1440, "height": 1000}, DOM_PROBE, settle_ms=4000, context=ctx)
            dom2, err2, _ = visit(browser, r, PAGES["live"], "live-desktop-warm2", {"width": 1440, "height": 1000}, DOM_PROBE, context=ctx)
            ctx.close()
            # 3) mobile
            dom3, err3, _ = visit(browser, r, PAGES["live"], "live-mobile", {"width": 390, "height": 844}, DOM_PROBE)
            # 4) history page desktop
            dom4, err4, _ = visit(browser, r, PAGES["history"], "history-desktop", {"width": 1440, "height": 1000}, HIST_PROBE)
            browser.close()
        for tag, dom, errs in (("live-desktop", dom1, err1), ("live-desktop-warm2", dom2, err2), ("live-mobile", dom3, err3)):
            if errs:
                verdict_fail.append("%s: %d page errors" % (tag, len(errs)))
            if dom.get("hasFailedText"):
                verdict_fail.append("%s: 'Failed to load' visible" % tag)
            if (dom.get("mapWithData") or 0) < 25:
                verdict_fail.append("%s: map has %s data paths" % (tag, dom.get("mapWithData")))
            if not dom.get("dataLoaded"):
                verdict_fail.append("%s: DATA not loaded" % tag)
        if err4:
            verdict_fail.append("history: %d page errors" % len(err4))
        r.section("verdict")
        if verdict_fail:
            for v in verdict_fail:
                r.fail(v)
            sys.exit(1)
        r.ok("page renders with data in a real browser; look at the screenshots for anything visual")


if __name__ == "__main__":
    main()
