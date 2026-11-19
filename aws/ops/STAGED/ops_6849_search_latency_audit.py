"""ops 6849 - READ-ONLY audit: how long does the Symbol Search dialog take to show the directory's own answer?

ops 6848 proved that typing sp500 ends with the index first, but the list took 21.9 s to settle on the runner. This
measures where that time goes, without changing anything:

  A. the dialog on the live page, as a visitor arriving: every /symsearch request it makes (how long the browser
     waited, how long the directory itself says the search took, whether the edge answered from its cache) and what
     the first row was at each moment until the pinned index led
  B. the directory asked directly: one at a time, three at once (what the dialog does), six at once
  C. the dialog again on a fresh page, once the directory has just been used

Writes nothing. No engine is invoked other than through the public search route the page itself uses.
"""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import base64
import gzip
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

BASE = "https://justhodl.ai"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = {"User-Agent": "Mozilla/5.0 jh-ops"}
WORDS = [("sp500", "I:SPX"), ("nasdaq", "I:NDX"), ("dow", "I:DJI")]
AGAIN = [("spx", "I:SPX"), ("ndx", "I:NDX"), ("sp500", "I:SPX")]
FIRST = r"""() => { const e = document.querySelector('#jhus .us-row[data-i]'); return e ? e.getAttribute('title') : null; }"""


def ask(q, limit=200, fresh=True):
    """One directory search through the public proxy: wall time, the directory's own search time, the edge's verdict."""
    url = "%s/symsearch?q=%s&limit=%d%s" % (PROXY, urllib.parse.quote(q), limit, "&nocache=1" if fresh else "")
    t0 = time.time()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=120) as resp:
            raw, edge = resp.read(), resp.headers.get("X-Edge-Cache")
        d = json.loads(raw)
        return {"q": q, "limit": limit, "wall_ms": int((time.time() - t0) * 1000), "engine_ms": d.get("ms"), "edge": edge, "rows": len(d.get("rows") or []),
                "bytes": len(raw), "first": [x.get("id") for x in (d.get("rows") or [])[:3]], "total": d.get("total")}
    except Exception as exc:  # noqa: BLE001
        return {"q": q, "limit": limit, "wall_ms": int((time.time() - t0) * 1000), "error": str(exc)[:160]}


def word_of(url):
    q = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
    return (q.get("q") or q.get("text") or q.get("id") or q.get("ids") or [""])[0][:40]


def line(x):
    if x.get("error"):
        return "%-9s limit %-3d  FAILED after %6d ms: %s" % (x["q"], x["limit"], x["wall_ms"], x["error"])
    return "%-9s limit %-3d  waited %6d ms, the directory's own search %6s ms, edge %-4s, %3d rows, %6d bytes, first %s" % (
        x["q"], x["limit"], x["wall_ms"], x.get("engine_ms"), x.get("edge"), x["rows"], x["bytes"], x["first"])


def embed(r, name, doc):
    blob = base64.b64encode(gzip.compress(json.dumps(doc, separators=(",", ":"), default=str).encode("utf-8"), 9)).decode("ascii")
    r.log("%s embedded: %d chars" % (name, len(blob)))
    r.lines.append("<!-- FEED-PACK-GZ-B64-BEGIN %s" % name)
    r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
    r.lines.append("FEED-PACK-GZ-B64-END -->")


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


def dialog_pass(browser, r, words, label, stamp):
    """Open the live chart, type each word into the Symbol Search dialog, time every directory request it makes."""
    out = []
    ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
    page = ctx.new_page()
    started, seen = {}, []

    def on_request(req):
        if "/symsearch" in req.url or "/tv-search" in req.url or "/quote?" in req.url or "/series?" in req.url:
            started[req] = time.time()

    def on_done(req):
        t0 = started.pop(req, None)
        if t0 is None:
            return
        rec = {"at": t0, "wall_ms": int((time.time() - t0) * 1000), "path": urllib.parse.urlsplit(req.url).path, "q": word_of(req.url)}
        try:
            resp = req.response()
            if resp is not None:
                rec["edge"] = resp.headers.get("x-edge-cache")
                if rec["path"] == "/symsearch":
                    d = resp.json()
                    rec["engine_ms"], rec["rows"] = d.get("ms"), len(d.get("rows") or [])
        except Exception as exc:  # noqa: BLE001
            rec["note"] = str(exc)[:80]
        seen.append(rec)

    def on_failed(req):
        t0 = started.pop(req, None)
        if t0 is not None:
            seen.append({"at": t0, "wall_ms": int((time.time() - t0) * 1000), "path": urllib.parse.urlsplit(req.url).path,
                         "q": word_of(req.url), "failed": str(req.failure or "")[:60]})

    page.on("request", on_request)
    page.on("requestfinished", on_done)
    page.on("requestfailed", on_failed)
    t_load = time.time()
    page.goto("%s/chart.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
    page.wait_for_function("() => typeof window.jhOpenSearch === 'function' && (window.lastBars || []).length > 10", timeout=120000)
    page.wait_for_timeout(1500)
    others = [x for x in seen if x["path"] != "/symsearch"]
    r.log("%s: page ready %.1f s after navigation; before any search the page had made %d directory calls for quotes and series (slowest %s ms)" % (
        label, time.time() - t_load, len(others), max([x["wall_ms"] for x in others] or [0])))
    for word, pin in words:
        mark = len(seen)
        t0 = time.time()
        page.evaluate("(w) => window.jhOpenSearch(w)", word)
        timeline, settled = [], None
        while time.time() - t0 < 60:
            page.wait_for_timeout(250)
            first = page.evaluate(FIRST)
            if not timeline or timeline[-1][1] != first:
                timeline.append([round(time.time() - t0, 1), first])
            if first == pin:
                settled = round(time.time() - t0, 1)
                break
        page.wait_for_timeout(1500)
        reqs = [x for x in seen[mark:] if x["path"] in ("/symsearch", "/tv-search")]
        literal = [x for x in reqs if x["path"] == "/symsearch" and x["q"].lower() == word]
        r.log("%s: typed %-7s -> %s; first row over time %s" % (
            label, word, ("the index (%s) led after %.1f s" % (pin, settled)) if settled is not None else ("%s NOT first within 60 s" % pin), timeline))
        for x in sorted(reqs, key=lambda z: z["at"]):
            r.log("    %-10s %-12s sent %+5.1f s, answered in %6d ms%s%s%s" % (
                x["path"], repr(x["q"])[:12], x["at"] - t0, x["wall_ms"],
                (", the directory's own search %s ms" % x["engine_ms"]) if x.get("engine_ms") is not None else "",
                (", edge %s" % x["edge"]) if x.get("edge") else "", (", FAILED %s" % x["failed"]) if x.get("failed") else ""))
        out.append({"word": word, "pin": pin, "settled_s": settled, "timeline": timeline, "requests": [{k: v for k, v in x.items() if k != "at"} | {"sent_s": round(x["at"] - t0, 2)} for x in reqs],
                    "literal_wall_ms": literal[0]["wall_ms"] if literal else None, "literal_engine_ms": literal[0].get("engine_ms") if literal else None})
        page.keyboard.press("Escape")
        page.wait_for_timeout(1200)
    ctx.close()
    return out


def main():
    with report("ops_6849_search_latency_audit") as r:
        pack, stamp = {"A": [], "B": {}, "C": []}, int(time.time())
        sync_playwright = ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = launch(p, r.log)
            r.section("A. the dialog on the live page, arriving after the directory has been idle")
            pack["A"] = dialog_pass(browser, r, WORDS, "arriving", stamp)

            r.section("B. the directory asked directly")
            one = [ask("sp500", 200), ask("sp500", 200), ask("sp500", 12), ask("s&p 500", 200), ask("spy", 200), ask("vantage", 200)]
            for x in one:
                r.log("one at a time   " + line(x))
            with ThreadPoolExecutor(3) as ex:
                three = list(ex.map(lambda q: ask(q, 200), ["sp500", "s&p 500", "spy"]))
            for x in three:
                r.log("three at once   " + line(x))
            with ThreadPoolExecutor(6) as ex:
                six = list(ex.map(lambda q: ask(q, 200), ["sp500", "spx", "nasdaq", "dow", "vix", "gold"]))
            for x in six:
                r.log("six at once     " + line(x))
            with ThreadPoolExecutor(6) as ex:
                six2 = list(ex.map(lambda q: ask(q, 200), ["sp500", "spx", "nasdaq", "dow", "vix", "gold"]))
            for x in six2:
                r.log("six at once, again " + line(x))
            cached = [ask("sp500", 200, fresh=False), ask("sp500", 200, fresh=False)]
            for x in cached:
                r.log("the page's own address (edge cache allowed)   " + line(x))
            pack["B"] = {"one": one, "three": three, "six": six, "six_again": six2, "cached": cached}

            r.section("C. the dialog again on a fresh page, the directory just used")
            pack["C"] = dialog_pass(browser, r, AGAIN, "just used", stamp + 1)
            browser.close()

        slow = [x for x in pack["A"] + pack["C"] if x["settled_s"] is None]
        cold = [x["settled_s"] for x in pack["A"] if x["settled_s"] is not None]
        warm = [x["settled_s"] for x in pack["C"] if x["settled_s"] is not None]
        r.kv(arriving_settle_s=cold, just_used_settle_s=warm, never_settled=len(slow),
             direct_one_at_a_time_ms=[x.get("wall_ms") for x in pack["B"]["one"]], direct_three_at_once_ms=[x.get("wall_ms") for x in pack["B"]["three"]],
             direct_six_at_once_ms=[x.get("wall_ms") for x in pack["B"]["six"]])
        embed(r, "search-latency", pack)
        if slow:
            r.fail("the pinned index never led for %s" % [x["word"] for x in slow])
            sys.exit(1)
        r.ok("measured: arriving %s s, just used %s s to show the index first" % (cold, warm))


if __name__ == "__main__":
    main()
