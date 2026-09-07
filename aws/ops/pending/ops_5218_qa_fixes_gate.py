"""ops_5218 -- gate the QA-audit fixes after the merge of qa/2026-09-07-audit (pages.yml deploy).

Verifies in a live Chrome on justhodl.ai:
  B1  /            no request to /config/engine-contracts.json returns 404 any more (site copy is 200)
  B2  /engines.html directory rows >= 850 (was 667 from the July registry); /config/engine-registry.json is 200 with >= 850 engines
  B3  a fresh session on /engines.html raises no CORS console error from the nav-drawer diag beacon
Read-only against production data. sys.exit(1) on any regression.
"""
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

SHOTS = ROOT / "aws" / "ops" / "reports" / "latest" / "shots"
FAILS = []


def http(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ops5218", "Cache-Control": "no-cache", "Pragma": "no-cache"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, b""
    except Exception:
        return 0, b""


with report("ops_5218_qa_fixes_gate") as R:
    R.heading("ops 5218 -- QA fixes gate (B1 contracts 404, B2 engine directory, B3 beacon CORS)")
    R.section("edge deploy")
    ok = False; t0 = time.time()
    while time.time() - t0 < 600:
        st, body = http("https://justhodl.ai/config/engine-contracts.json?v=%d" % int(time.time()))
        st2, body2 = http("https://justhodl.ai/config/engine-registry.json?v=%d" % int(time.time()))
        if st == 200 and st2 == 200:
            ok = True; break
        time.sleep(20)
    R.log("   /config/engine-contracts.json -> %s (%d bytes) ; /config/engine-registry.json -> %s (%d bytes) after %ds" % (st, len(body), st2, len(body2), int(time.time() - t0)))
    if not ok:
        FAILS.append("site copies of engine-contracts/engine-registry did not appear at the edge within 10 min")
    else:
        try:
            reg = json.loads(body2)
            n = reg.get("n_engines") or len(reg.get("engines") or {})
            R.log("   registry n_engines=%s generated_at=%s" % (n, reg.get("generated_at")))
            if n < 850:
                FAILS.append("registry has %s engines (expected >= 850)" % n)
            if "justhodl-jh-fusion" not in (reg.get("engines") or {}):
                FAILS.append("registry lacks justhodl-jh-fusion")
        except Exception as exc:
            FAILS.append("registry unparseable: %s" % str(exc)[:120])
        try:
            c = json.loads(body)
            R.log("   contracts n_contracts=%s" % c.get("n_contracts"))
        except Exception as exc:
            FAILS.append("contracts unparseable: %s" % str(exc)[:120])

    R.section("live Chrome")
    try:
        import playwright  # noqa: F401
    except Exception:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
    from playwright.sync_api import sync_playwright
    SHOTS.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(channel="chrome", headless=True)
        except Exception:
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
            browser = p.chromium.launch(headless=True)
        for route in ("/", "/engines.html"):
            ctx = browser.new_context(viewport={"width": 1440, "height": 1100})
            pg = ctx.new_page()
            errors, console, bad = [], [], []
            pg.on("pageerror", lambda e: errors.append(str(e)[:160]))
            pg.on("console", lambda m: console.append(m.text[:200]) if m.type == "error" else None)
            pg.on("response", lambda rs: bad.append((rs.status, rs.url[:140])) if rs.status >= 400 else None)
            pg.goto("https://justhodl.ai%s?v=%d" % (route, int(time.time())), wait_until="domcontentloaded", timeout=60000)
            pg.wait_for_timeout(10000)
            facts = pg.evaluate("""() => ({ rows: document.querySelectorAll('tbody tr').length, authority: (document.getElementById('hd-authority-title')||{}).textContent || null,
                catalog: (document.getElementById('hd-catalog')||{}).textContent || null, body: (document.body.innerText||'').length })""")
            pg.screenshot(path=str(SHOTS / ("ops5218_%s.png" % (route.strip("/").replace(".html", "") or "home"))))
            cors = [c for c in console if "CORS" in c or "Access to fetch" in c]
            contracts404 = [b for b in bad if "engine-contracts" in b[1] and b[0] == 404]
            R.log("   %s: rows=%s authority=%s catalog=%s | page errors %s | console errors %d (CORS %d) | >=400 %s" % (route, facts["rows"], facts["authority"], (facts["catalog"] or "")[:80], errors[:2], len(console), len(cors), bad[:5]))
            if route == "/" and contracts404:
                FAILS.append("B1 still 404: %s" % contracts404)
            if route == "/engines.html" and facts["rows"] < 850:
                FAILS.append("B2 engines.html rows %s < 850" % facts["rows"])
            if cors:
                FAILS.append("B3 CORS console error still present on %s: %s" % (route, cors[:1]))
            if errors:
                FAILS.append("%s page errors: %s" % (route, errors[:2]))
            ctx.close()
        browser.close()

    if FAILS:
        R.section("FAILS")
        for f in FAILS:
            R.fail(f)
        sys.exit(1)
    R.ok("GREEN -- QA fixes live: contracts served, directory fresh, beacon silent")
    sys.exit(0)
