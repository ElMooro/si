"""ops 6601 - READ-ONLY browser probe: do chart.html's top and side bars disappear and come back only when the mouse
reaches them, on the served site?  (Khalid, 2026-10-21 11:52 ET: "the top and side bars are supposed to disappear and
only appear when I hover my mouse over them".)

jh-chart-autohide.js v=20261021-ah2 fixed two things: (1) the pin that switched auto-hide off was kept in localStorage
for ever, so one accidental Shift+H / pin click left the bars on every load; it now lasts one tab session; (2) a sheet
that slid in under a resting pointer never received mouseenter/mouseleave, so it stayed open until the pointer crossed
it again; hiding is now position-based.  Checked here with headless Chrome against https://justhodl.ai/chart.html:
fresh profile - bars hidden on load, each edge reveals its bar, moving away hides it within ~1 s; a profile carrying
the old persistent pin ("jh-autohide"="0") - bars still hidden on load.  Nothing on the site is changed.
"""
from pathlib import Path
import json
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

BASE = "https://justhodl.ai"
STATE_JS = """() => { const html = document.documentElement; const q = (id) => { const e = document.getElementById(id); if (!e) return null; const r = e.getBoundingClientRect();
  return {show: e.classList.contains('show'), top: Math.round(r.top), left: Math.round(r.left), right: Math.round(r.right), bottom: Math.round(r.bottom), w: Math.round(r.width), h: Math.round(r.height)}; };
  return {ah: html.classList.contains('jh-ah'), pin_ls: (() => { try { return localStorage.getItem('jh-autohide'); } catch (e) { return 'n/a'; } })(),
          pin_ss: (() => { try { return sessionStorage.getItem('jh-autohide-session'); } catch (e) { return 'n/a'; } })(),
          top: q('jh-ah-top'), right: q('jh-ah-right'), left: q('jh-ah-left'), bot: q('jh-ah-bot'), tfbar: q('tfbar'), watch: q('watch'), rail: q('rail'),
          desks: document.querySelectorAll('#tfbar .wsdesk').length}; }"""


def ensure_playwright(log):
    try:
        import playwright  # noqa: F401
    except Exception:
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


def offscreen(st, k, W, H):
    s = st.get(k)
    if not s:
        return None
    if k == "top":
        return s["bottom"] <= 2
    if k == "bot":
        return s["top"] >= H - 2
    if k == "left":
        return s["right"] <= 2
    return s["left"] >= W - 2


def main():
    W, H = 1600, 1000
    with report("ops_6601_chart_autohide_probe") as r:
        r.heading("ops 6601 - chart.html auto-hide probe (headless Chrome on the runner, read-only)")
        req = urllib.request.Request("%s/jh-chart-autohide.js?v=20261021-ah2&nocache=%d" % (BASE, int(time.time())), headers={"User-Agent": "justhodl-ops/6601", "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            js = resp.read().decode("utf-8", errors="replace")
        served_new = "jh-autohide-session" in js and "function near(" in js
        req2 = urllib.request.Request("%s/chart.html?nocache=%d" % (BASE, int(time.time())), headers={"User-Agent": "justhodl-ops/6601", "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req2, timeout=30) as resp:
            html = resp.read().decode("utf-8", errors="replace")
        page_new = "jh-chart-autohide.js?v=20261021-ah2" in html
        r.log("served jh-chart-autohide.js carries the fix: %s; served chart.html references v=20261021-ah2: %s" % (served_new, page_new))
        if not (served_new and page_new):
            r.fail("the served site does not carry the auto-hide fix yet (Pages deploy pending?)")
            raise SystemExit(1)
        sync_playwright = ensure_playwright(r.log)
        verdicts = {}
        with sync_playwright() as p:
            browser = launch(p, r.log)
            for profile in ("fresh", "old-persistent-pin"):
                ctx = browser.new_context(viewport={"width": W, "height": H}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6601 autohide-probe")
                if profile == "old-persistent-pin":
                    ctx.add_init_script("try { localStorage.setItem('jh-autohide', '0'); } catch (e) {}")
                page = ctx.new_page()
                page.set_default_timeout(30000)
                page.goto("%s/chart.html?s=AAPL&nocache=%d" % (BASE, int(time.time())), wait_until="domcontentloaded")
                try:
                    page.wait_for_load_state("networkidle", timeout=15000)
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(6000)
                st = page.evaluate(STATE_JS)
                hidden_on_load = st["ah"] and all(offscreen(st, k, W, H) for k in ("top", "left", "right", "bot"))
                r.log("[%s] on load: auto-hide %s, pin_ls %r pin_ss %r, desks %d; sheets %s" % (profile, st["ah"], st["pin_ls"], st["pin_ss"], st["desks"],
                                                                                              json.dumps({k: st[k] for k in ("top", "left", "right", "bot")})))
                steps = {}
                for k, (x, y) in (("top", (800, 2)), ("right", (W - 2, 500)), ("left", (2, 500)), ("bot", (800, H - 2))):
                    page.mouse.move(x, y)
                    page.wait_for_timeout(350)
                    s1 = page.evaluate(STATE_JS)
                    shown = bool(s1.get(k) and s1[k]["show"] and not offscreen(s1, k, W, H))
                    page.mouse.move(800, 500)
                    page.wait_for_timeout(1200)
                    s2 = page.evaluate(STATE_JS)
                    gone = bool(s2.get(k) and not s2[k]["show"] and offscreen(s2, k, W, H))
                    steps[k] = (shown, gone)
                    r.log("[%s] %-5s edge -> shown %s | away -> hidden %s (%s)" % (profile, k, shown, gone, json.dumps(s2.get(k))))
                # resting pointer case: reveal the top bar, then jump straight to the middle without crossing it
                page.mouse.move(800, 1)
                page.wait_for_timeout(300)
                page.mouse.move(800, 700)
                page.wait_for_timeout(1200)
                s3 = page.evaluate(STATE_JS)
                jump_hidden = bool(s3.get("top") and not s3["top"]["show"])
                r.log("[%s] reveal top then jump to the middle: hidden again %s" % (profile, jump_hidden))
                page.screenshot(path="/tmp/autohide-%s.png" % profile)
                ok = hidden_on_load and all(a and b for a, b in steps.values()) and jump_hidden
                verdicts[profile] = ok
                r.kv(profile=profile, hidden_on_load=hidden_on_load, top="%s/%s" % steps["top"], right="%s/%s" % steps["right"], left="%s/%s" % steps["left"], bot="%s/%s" % steps["bot"], jump_hidden=jump_hidden, verdict="OK" if ok else "FAIL")
                ctx.close()
            browser.close()
        if not all(verdicts.values()):
            r.fail("auto-hide verdicts: %s" % json.dumps(verdicts))
            raise SystemExit(1)
        r.ok("bars hidden on load, revealed at their edge, hidden again when the pointer leaves - fresh profile and old-pin profile alike")


if __name__ == "__main__":
    main()
