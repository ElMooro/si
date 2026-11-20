"""ops 6862 - READ-ONLY live proof: the tabs on chart.html update on their own (Chrome on the runner, re-runnable).

Khalid 2026-11-20: "On the chart.html there are a lot of tabs that open automatically but they don't update
automatically they should be updating automatically".

Before: each open tab (SPY, QQQ, IWM, AAPL ...) read its last price and day change once, when the page opened.
Now the page re-reads them every 30 seconds while auto refresh is on. chart.html is opened on justhodl.ai and nothing
is clicked while the page is watched:
  1. the page asks /quotes for its tabs by itself, a full pass about every 30 s; after each pass every tab prints that
     pass's price and its change against the prior close; tabs whose figure moved between passes are listed;
  2. the tab strip is patched in place (the same buttons before and after a pass);
  3. one pass is answered with SPY's price moved by +1.11 inside the page (an injected figure, said so) - the tab
     follows it, and returns to the live figure on the next pass;
  4. a symbol opened later (the call a ticker link on any desk makes) gets a tab, and its figures arrive within seconds;
  5. with auto refresh switched off the page stops asking; switched on again it resumes;
  6. the same on a 390 px phone.
Writes nothing.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import json
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome"
TABS = r"""() => Array.from(document.querySelectorAll('#tabs .tab[data-id]')).map(b => { const sp = b.querySelector('span.up,span.dn');
  return { s: b.getAttribute('data-id'), text: sp ? sp.textContent : null, cls: sp ? sp.className : null, on: b.classList.contains('on'), mark: b.__opsMark || null }; })"""
HANDLE = "() => (window.jhTabQuotes ? window.jhTabQuotes() : null)"


def ensure_playwright(log):
    try:
        import playwright  # noqa: F401
    except ImportError:
        log("installing playwright")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
    from playwright.sync_api import sync_playwright
    return sync_playwright


def launch(p, log):
    try:
        return p.chromium.launch(channel="chrome", headless=True)
    except Exception as exc:  # noqa: BLE001
        log("system Chrome not usable (%s): installing chromium" % str(exc)[:80])
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
        return p.chromium.launch(headless=True)


def figures(text):
    """(price, percent) printed on a tab: '715.87 -1.96%' -> (715.87, -1.96)."""
    m = re.match(r"^\s*([\d,]+(?:\.\d+)?)\s+([+\-\u2212]?\d+(?:\.\d+)?)%\s*$", str(text or ""))
    return (float(m.group(1).replace(",", "")), float(m.group(2).replace("\u2212", "-"))) if m else (None, None)


class Watch:
    """Every /quotes answer the page receives, in order."""

    def __init__(self, page):
        self.seen = []
        page.on("response", self._on)

    def _on(self, resp):
        if "/quotes?tickers=" not in resp.url:
            return
        try:
            body = resp.json()
        except Exception:  # noqa: BLE001
            body = None
        asked = re.sub(r"%2C", ",", resp.url.split("tickers=")[1].split("&")[0], flags=re.I).split(",")
        self.seen.append({"t": time.time(), "asked": asked, "rows": (body or {}).get("tickers") or {}, "status": resp.status})

    def wait(self, page, n, timeout_s, pred=None):
        t0 = time.time()
        while time.time() - t0 < timeout_s:
            hits = [x for x in self.seen if pred is None or pred(x)]
            if len(hits) >= n:
                return hits
            page.wait_for_timeout(500)
        return [x for x in self.seen if pred is None or pred(x)]


US = re.compile(r"^[A-Z]{1,5}([.\-][A-Z])?$")


def full_pass(page):
    """A predicate for the tab refresher's own full pass: /quotes asked for exactly the page's US tabs, in tab order
    (the watchlist asks /quotes for its own rows; those calls are not counted)."""
    want = [t["s"].replace("-", ".") for t in (page.evaluate(HANDLE) or {}).get("tabs", []) if US.match(t["s"]) and t.get("src") != "daily bars"]
    return lambda x: x["asked"] == want


def agree(tab, row):
    """A tab's printed figures against one snapshot row (accepted rows only: a session bar and a prior close)."""
    px, pct = figures(tab["text"])
    if px is None or not row or not (row.get("price") and row.get("prevClose") and row.get("dayClose")):
        return None
    want = (row["price"] - row["prevClose"]) / row["prevClose"] * 100
    return abs(px - row["price"]) <= max(0.011, row["price"] * 2e-5) and abs(pct - want) <= 0.011 and tab["cls"] == ("up" if want >= 0 else "dn")


def main():
    with report("ops_6862_chart_tabs_refresh_live_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6862 - chart.html's tabs update on their own, live %s" % now.isoformat(timespec="minutes"))
        stamp = int(time.time())
        fails, shots = [], {}

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL %s" % msg)
            return bool(cond)

        sync_playwright = ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = launch(p, r.log)

            def open_chart(width, height, mobile):
                ctx = browser.new_context(viewport={"width": width, "height": height}, device_scale_factor=1, is_mobile=mobile, has_touch=mobile,
                                          user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) jh-ops-probe Mobile Safari" if mobile else UA)
                page = ctx.new_page()
                errors = []
                page.on("pageerror", lambda e: errors.append(str(e)[:200]))
                watch = Watch(page)
                page.goto("%s/chart.html?s=AAPL&nocache=%d" % (BASE, stamp + width), wait_until="domcontentloaded")
                page.wait_for_function("() => document.querySelectorAll('#tabs .tab[data-id]').length >= 6 && typeof window.jhTabQuotes === 'function'", timeout=90000)
                return ctx, page, watch, errors

            # ------------------------------------------------------------------ 1. nothing is clicked: the page asks by itself
            r.section("1. 1440 px: the page is left alone")
            ctx, page, watch, errors = open_chart(1440, 900, False)
            t_open = time.time()
            full = full_pass(page)
            passes = watch.wait(page, 1, 25, full)
            if not ck(len(passes) >= 1, "no /quotes call for the tabs within 25 s of opening the page"):
                shots["failed_1440"] = page.screenshot(type="jpeg", quality=50)
            page.wait_for_timeout(1200)
            first = page.evaluate(TABS)
            h0 = page.evaluate(HANDLE)
            r.log("opened; first pass %.1fs after the page, asked for %s" % (passes[0]["t"] - t_open if passes else -1, ",".join(passes[0]["asked"]) if passes else ""))
            r.log("tabs after pass 1: %s" % " | ".join("%s %s" % (t["s"], t["text"]) for t in first))
            ck(len(first) >= 10 and all(t["text"] for t in first), "tabs without figures after the first pass: %s" % [t["s"] for t in first if not t["text"]])
            page.evaluate("() => document.querySelectorAll('#tabs .tab[data-id]').forEach((b, i) => { b.__opsMark = 'm' + i; })")
            passes = watch.wait(page, 3, 80, full)
            page.wait_for_timeout(1200)
            ck(len(passes) >= 3, "expected three automatic passes within 80 s, saw %d" % len(passes))
            gaps = [round(passes[i]["t"] - passes[i - 1]["t"], 1) for i in range(1, len(passes))]
            ck(all(24 <= g <= 40 for g in gaps), "passes are not about 30 s apart: %s" % gaps)
            last = page.evaluate(TABS)
            h1 = page.evaluate(HANDLE)
            rows = passes[-1]["rows"] if passes else {}
            judged = {t["s"]: agree(t, rows.get(t["s"])) for t in last}
            ck(sum(1 for v in judged.values() if v) >= 8 and not [s for s, v in judged.items() if v is False], "tabs that do not print the last pass's snapshot: %s" % {s: (next(t["text"] for t in last if t["s"] == s), rows.get(s)) for s, v in judged.items() if v is False})
            moved = [(a["s"], a["text"], b["text"]) for a, b in zip(first, last) if a["s"] == b["s"] and a["text"] != b["text"]]
            r.log("%d automatic passes in %.0fs, %s s apart; nothing was clicked" % (len(passes), time.time() - t_open, gaps))
            r.log("tabs after pass %d: %s" % (len(passes), " | ".join("%s %s" % (t["s"], t["text"]) for t in last)))
            r.log("printed = the pass's snapshot for %d tabs (%s); not judged (no session bar in the snapshot): %s" % (
                sum(1 for v in judged.values() if v), ", ".join(s for s, v in judged.items() if v), ", ".join(s for s, v in judged.items() if v is None) or "none"))
            r.log("figures that moved by themselves between pass 1 and pass %d: %d - %s" % (len(passes), len(moved), "; ".join("%s %s -> %s" % m for m in moved) or "none (the snapshot printed the same prices; see step 3)"))
            ck(h1 and h0 and h1["passes"] > h0["passes"] and h1["every_ms"] == 30000, "the page's own counter did not advance: %s -> %s" % (h0 and h0["passes"], h1 and h1["passes"]))
            r.log("the page's counter: %d passes, %d figure changes; sources %s" % (h1["passes"], h1["changed"], json.dumps({t["s"]: t["src"] for t in h1["tabs"]})))
            # 2. patched in place
            ck(all(t["mark"] for t in last) and [t["mark"] for t in last] == ["m%d" % i for i in range(len(last))], "the tab strip was rebuilt during a pass (marks %s)" % [t["mark"] for t in last][:6])
            r.log("the same %d tab buttons stand after the passes as before them (patched in place)" % len(last))
            shots["tabs_1440"] = page.screenshot(type="jpeg", quality=60, clip={"x": 0, "y": 0, "width": 1440, "height": 110})

            # ------------------------------------------------------------------ 3. an injected figure: the tab follows what the page is told
            r.section("3. one pass answered with SPY moved by +1.11 (injected inside the page, not a market price)")
            n0 = len(watch.seen)
            full = full_pass(page)
            page.evaluate("""() => { const of = window.fetch; window.__opsFetch = of; window.fetch = function (u) {
              if (String(u).indexOf('/quotes?tickers=') < 0) return of.apply(this, arguments);
              return of.apply(this, arguments).then(r => r.json()).then(d => { if (d && d.tickers && d.tickers.SPY) { window.__opsReal = d.tickers.SPY.price; d.tickers.SPY.price = +(d.tickers.SPY.price + 1.11).toFixed(4); window.__opsFake = d.tickers.SPY.price; }
                return new Response(JSON.stringify(d), { status: 200, headers: { 'Content-Type': 'application/json' } }); }); }; }""")
            watch.wait(page, 1, 45, lambda x: full(x) and watch.seen.index(x) >= n0)
            page.wait_for_timeout(1500)
            fake, real = page.evaluate("() => [window.__opsFake || null, window.__opsReal || null]")
            spy = next((t for t in page.evaluate(TABS) if t["s"] == "SPY"), {})
            ck(fake and figures(spy.get("text"))[0] is not None and abs(figures(spy["text"])[0] - fake) <= 0.011, "the SPY tab did not follow the injected figure: tab %s, injected %s (live %s)" % (spy.get("text"), fake, real))
            r.log("live %s, injected %s -> the SPY tab prints %s" % (real, fake, spy.get("text")))
            page.evaluate("() => { window.fetch = window.__opsFetch; }")
            n1 = len(watch.seen)
            back = watch.wait(page, 1, 45, lambda x: full(x) and watch.seen.index(x) >= n1)
            page.wait_for_timeout(1500)
            spy = next((t for t in page.evaluate(TABS) if t["s"] == "SPY"), {})
            ck(back and agree(spy, back[-1]["rows"].get("SPY")) is not False, "the SPY tab did not return to the live figure: %s vs %s" % (spy.get("text"), back and back[-1]["rows"].get("SPY")))
            r.log("injection removed -> next pass: the SPY tab prints %s (snapshot %s)" % (spy.get("text"), back and (back[-1]["rows"].get("SPY") or {}).get("price")))

            # ------------------------------------------------------------------ 4. a tab opened later
            r.section("4. a symbol opened later gets a tab and its figures")
            have = {t["s"] for t in page.evaluate(TABS)}
            sym = next(s for s in ("KO", "JPM", "XOM", "PFE", "WMT") if s not in have)
            n2 = len(watch.seen)
            t_add = time.time()
            page.evaluate("(s) => window.jhOpenSymbol(s)", sym)          # the call a ticker link on any desk makes
            try:
                page.wait_for_function("(s) => { const b = document.querySelector('#tabs .tab[data-id=\"' + s + '\"]'); return b && b.querySelector('span.up,span.dn'); }", arg=sym, timeout=30000)
            except Exception:  # noqa: BLE001
                pass
            tab = next((t for t in page.evaluate(TABS) if t["s"] == sym), None)
            ck(tab is not None and tab["text"], "the new %s tab has no figures after 30 s: %s" % (sym, tab))
            asked = [x for x in watch.seen[n2:] if sym in x["asked"]]
            r.log("opened %s (as a desk's ticker link does) -> tab %s %.1fs later; the page asked /quotes for %s" % (sym, tab and tab["text"], time.time() - t_add, [",".join(x["asked"]) for x in asked][:2]))

            # ------------------------------------------------------------------ 5. auto refresh off / on
            r.section("5. auto refresh off: the page stops asking; on: it resumes")
            page.evaluate("() => document.getElementById('btn-live').click()")
            page.wait_for_timeout(1500)
            n3 = len(watch.seen)
            page.wait_for_timeout(42000)
            mine = full_pass(page)
            quiet = len([x for x in watch.seen[n3:] if mine(x)])
            ck(quiet == 0, "%d /quotes calls in 42 s with auto refresh off" % quiet)
            t_on = time.time()
            page.evaluate("() => document.getElementById('btn-live').click()")
            mine = full_pass(page)
            again = watch.wait(page, 1, 20, lambda x: mine(x) and watch.seen.index(x) >= n3)
            ck(len(again) >= 1, "no /quotes call within 20 s of switching auto refresh back on")
            r.log("off: %d calls in 42 s | on again: a pass %.1fs later" % (quiet, again[0]["t"] - t_on if again else -1))
            ck(not errors, "page errors: %s" % errors[:3])
            ctx.close()

            # ------------------------------------------------------------------ 6. phone
            r.section("6. 390 px")
            ctx, page, watch, errors = open_chart(390, 844, True)
            t_open = time.time()
            passes = watch.wait(page, 2, 55, full_pass(page))
            page.wait_for_timeout(1200)
            tabs = page.evaluate(TABS)
            rows = passes[-1]["rows"] if passes else {}
            judged = {t["s"]: agree(t, rows.get(t["s"])) for t in tabs}
            ck(len(passes) >= 2 and sum(1 for v in judged.values() if v) >= 8 and not [s for s, v in judged.items() if v is False], "phone: %d passes, agree %s" % (len(passes), judged))
            r.log("phone: %d automatic passes in %.0fs; %d tabs print the last pass's snapshot: %s" % (len(passes), time.time() - t_open, sum(1 for v in judged.values() if v), " | ".join("%s %s" % (t["s"], t["text"]) for t in tabs[:6])))
            shots["tabs_390"] = page.screenshot(type="jpeg", quality=55, clip={"x": 0, "y": 0, "width": 390, "height": 150})
            ck(not errors, "phone page errors: %s" % errors[:3])
            ctx.close()
            browser.close()

        r.kv(failures=len(fails), tabs=len(last), moved_by_themselves=len(moved))
        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        if fails:
            r.fail("%d checks failed: %s" % (len(fails), " | ".join(fails)[:1800]))
            sys.exit(1)
        r.ok("LIVE PROOF: chart.html re-reads its tabs every 30 s with nothing clicked, each tab prints that pass's snapshot, a new tab is priced within seconds, and auto refresh off stops it")


if __name__ == "__main__":
    main()
