"""ops_5175 -- why is Heatmap undefined in the live page? (READ-ONLY probe)
Fetch the live chart-pro.html, node --check every inline script, and in headless
Chrome record page errors + typeof of the main classes."""
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

with report("ops_5175_heatmap_scope_probe") as R:
    R.heading("ops 5175 -- Heatmap scope probe")
    req = urllib.request.Request("https://justhodl.ai/chart-pro.html", headers={"User-Agent": "ops5175", "Cache-Control": "no-cache"})
    html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "ignore")
    R.log("   live html bytes=%d has_marker=%s has_class=%s" % (len(html), "jh_heatmap_workspace_v3" in html, "class Heatmap {" in html))
    scripts = re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", html, flags=re.S)
    bad = 0
    for i, sc in enumerate(scripts):
        if not sc.strip():
            continue
        p = Path("/tmp/live_s%d.js" % i)
        p.write_text(sc)
        r = subprocess.run(["node", "--check", str(p)], capture_output=True, text=True)
        R.log("   inline script %d: %d chars -> %s %s" % (i, len(sc), "OK" if r.returncode == 0 else "SYNTAX ERROR", r.stderr.strip()[:300]))
        bad += 1 if r.returncode else 0
    try:
        import playwright  # noqa: F401
    except Exception:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        try:
            b = p.chromium.launch(channel="chrome", headless=True)
        except Exception:
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
            b = p.chromium.launch(headless=True)
        pg = b.new_page(viewport={"width": 1440, "height": 1000})
        errs, cons = [], []
        pg.on("pageerror", lambda e: errs.append(str(e)[:400]))
        pg.on("console", lambda m: cons.append("%s: %s" % (m.type, m.text[:200])) if m.type == "error" else None)
        pg.goto("https://justhodl.ai/chart-pro.html?s=SPY&tf=1D", wait_until="domcontentloaded", timeout=60000)
        pg.wait_for_timeout(6000)
        types = pg.evaluate("() => ({Heatmap: typeof Heatmap, MacroData: typeof MacroData, UI: typeof UI, State: typeof State, PROXY: typeof PROXY, btn: !!document.getElementById('heatmap-btn'), modal: !!document.getElementById('heatmap-modal'), editor: !!document.getElementById('hm-editor-modal')})")
        R.log("   typeof -> %s" % types)
        for e in errs[:8]:
            R.warn("   pageerror: %s" % e)
        for c in cons[:8]:
            R.warn("   console: %s" % c)
        b.close()
    if bad or errs:
        sys.exit(1)
    R.ok("probe complete")
