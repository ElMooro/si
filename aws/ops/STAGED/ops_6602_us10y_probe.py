"""ops 6602 - READ-ONLY browser probe: what does chart.html show for the 10-year now?  (Khalid, 2026-10-21 13:30 ET:
"why is us10y 2 days old" - the inspector showed TVC:US10Y routed to FRED:DGS10, whose newest observation was Monday.)

The engine change (jh-chart-engine.js + jh-chart-tvwatch.js, 2026-10-21): the market yield symbols route to market
sources - US10Y/TVC:US10Y -> ^TNX, US05Y -> ^FVX, US30Y -> ^TYX (CBOE yield indices through Yahoo, live, asked before
the banked copy), US02Y/US20Y -> Treasury's own par curve (ustpar, the series FRED's H.15 mirrors a day later); FRED:DGS*
stays FRED when asked for by name.  Headless Chrome loads the served chart for TVC:US10Y, US10Y (must show today's
session), TVC:US02Y and FRED:DGS10 (reported) and reads the quote HUD, status line and detail pane.  Also reads the three sources
directly through the worker (/yf-ohlc live, /series ustpar:10Y, /series FRED:DGS10) and reports each one's last date.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import re
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

BASE = "https://justhodl.ai"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
SNAP = """() => { const t = (id, n) => { const e = document.getElementById(id); return e ? (e.innerText || '').replace(/\\s+/g, ' ').trim().slice(0, n || 300) : null; };
  return {quote: t('quote', 320), stat: t('stat', 240), detail: t('detail', 400), tab: (document.querySelector('#tabbar .on') || {}).innerText || null}; }"""


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


def get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops/6602", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.loads(r.read())


def main():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with report("ops_6602_us10y_probe") as r:
        r.heading("ops 6602 - the 10-year on chart.html (read-only probe)")
        r.section("1. the three sources through the worker")
        last = {}
        for name, url in (("yahoo live ^TNX", PROXY + "/yf-ohlc?symbol=%5ETNX&range=5d&interval=1d&nowarehouse=1"),
                          ("ustpar:10Y", PROXY + "/series?id=ustpar:10Y"), ("FRED:DGS10", PROXY + "/series?id=FRED:DGS10")):
            try:
                d = get_json(url + ("&" if "?" in url else "?") + "nocache=%d" % int(time.time()))
                bars = d.get("bars") or d.get("obs") or d.get("observations") or d.get("points") or []
                lb = bars[-1] if bars else None
                if isinstance(lb, dict):
                    when = datetime.fromtimestamp(lb["time"], tz=timezone.utc).strftime("%Y-%m-%d") if "time" in lb else str(lb.get("date") or lb.get("d"))[:10]
                    val = lb.get("close", lb.get("value", lb.get("v")))
                elif isinstance(lb, list):
                    when, val = str(lb[0])[:10], lb[-1]
                else:
                    when, val = None, None
                last[name] = when
                r.log("%-16s last %s = %s | n %s | source %s | declared last %s" % (name, when, val, len(bars), d.get("source"), d.get("last")))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s" % (name, type(e).__name__))
        r.section("2. the served chart")
        html = urllib.request.urlopen(urllib.request.Request(BASE + "/chart.html?nocache=%d" % int(time.time()), headers={"User-Agent": "justhodl-ops/6602", "Cache-Control": "no-cache"}), timeout=30).read().decode("utf-8", "replace")
        m = re.search(r'src="(/jh-chart-engine\.js(?:\?v=[A-Za-z0-9._-]+)?)"', html)
        js = urllib.request.urlopen(urllib.request.Request(BASE + (m.group(1) if m else "/jh-chart-engine.js"), headers={"User-Agent": "justhodl-ops/6602", "Cache-Control": "no-cache"}), timeout=30).read().decode("utf-8", "replace")
        carries = '"TVC:US10Y":"^TNX"' in js and "nowarehouse=1" in js and "ustpar:2Y" in js
        r.log("served chart.html references %s; that engine carries the US-yield route change: %s" % (m.group(1) if m else None, carries))
        if not carries:
            r.fail("the served engine does not carry the change yet (Pages deploy pending?)")
            raise SystemExit(1)
        sync_playwright = ensure_playwright(r.log)
        ok_all = True
        with sync_playwright() as p:
            browser = launch(p, r.log)
            from datetime import timedelta
            expect = {"TVC:US10Y": today, "US10Y": today, "TVC:US02Y": None, "FRED:DGS10": None}  # None = report only (ustpar T-1 before 18:30 ET; FRED is FRED by name)
            for sym in ("TVC:US10Y", "US10Y", "TVC:US02Y", "FRED:DGS10"):
                ctx = browser.new_context(viewport={"width": 1600, "height": 1000}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6602")
                ctx.add_init_script("try { sessionStorage.setItem('jh-autohide-session', '0'); } catch (e) {}")
                page = ctx.new_page()
                page.set_default_timeout(30000)
                page.goto("%s/chart.html?s=%s&nocache=%d" % (BASE, urllib.request.quote(sym), int(time.time())), wait_until="domcontentloaded")
                try:
                    page.wait_for_load_state("networkidle", timeout=15000)
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(8000)
                snap = page.evaluate(SNAP)
                dates = re.findall(r"20\d\d-\d\d-\d\d", (snap.get("quote") or "") + " " + (snap.get("detail") or ""))
                newest = max(dates) if dates else None
                src = snap.get("stat") or ""
                want = expect.get(sym)
                good = (newest == want) if want else True
                ok_all = ok_all and good
                r.log("[%s] newest bar date %s (today %s, expected %s) -> %s | quote %r | stat %r" % (sym, newest, today, want or "report only", "OK" if good else "STALE", (snap.get("quote") or "")[:220], src[:160]))
                r.kv(symbol=sym, newest_bar=newest, today=today, verdict=("OK" if good else "STALE") if want else "reported", stat=src[:120].replace("|", "/"))
                page.screenshot(path="/tmp/us10y-%s.png" % sym.replace(":", "_"))
                ctx.close()
            browser.close()
        r.log("source last dates: %s" % json.dumps(last))
        if not ok_all:
            r.fail("the chart still shows a stale 10-year for at least one route")
            raise SystemExit(1)
        r.ok("the 10-year chart shows today's session on every route")


if __name__ == "__main__":
    main()
