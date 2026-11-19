"""ops 6850 - READ-ONLY live proof: a benchmark word leads with its index at once, whatever the directory is doing.

ops 6849 measured that the directory's own answer to a search takes 22-26 s whenever the request lands on a directory
process that has not loaded the index, and that until then the list for sp500 led with FRED's copy and then an ETF.
jh-uni-search.js now knows the first pin of each index word. This proves it on the live page, in Chrome on the runner:

  1. the directory silent (every /symsearch request held back, which is what a cold directory process looks like to the
     page): type each index word, read the list 1.2 s later, take the first row, see the index chart; then let the
     directory answer and see its own rows take over, with the same id and the same name in first place
  2. words that are not index words are untouched
  3. nothing held back, a fresh page: type the word and watch the first row from inside the page 25 times a second until
     the directory has answered - it must be the index from the first list drawn; 390 px

Writes nothing. Holding a request back changes no data: it is released to the real directory unchanged.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import base64
import gzip
import json
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

BASE = "https://justhodl.ai"
MIN_INDEX_BARS = 5000
WORDS = [("sp500", "I:SPX"), ("spx", "I:SPX"), ("s&p 500", "I:SPX"), ("s&p", "I:SPX"), ("nasdaq", "I:NDX"), ("ndx", "I:NDX"), ("dow", "I:DJI"), ("dow jones", "I:DJI"), ("russell", "I:RUT")]
CLICK = {"sp500", "nasdaq", "dow", "russell"}          # the first word of each index: take the row while the directory is silent
NATURAL = [("sp500", "I:SPX"), ("nasdaq", "I:NDX"), ("dow", "I:DJI")]
OTHERS = ["aapl", "vix", "gold"]                       # not index words: the dialog must add nothing of its own

STATE = r"""() => { const d = window.lastBars || [];
  const t = (id) => { const e = document.getElementById(id); return e ? (e.innerText || e.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 200) : null; };
  const iso = (x) => typeof x === 'number' ? new Date(x * 1000).toISOString().slice(0, 10) : (x && x.year ? x.year + '-' + ('0' + x.month).slice(-2) + '-' + ('0' + x.day).slice(-2) : String(x || '').slice(0, 10));
  return { bars: d.length, first: d.length ? iso(d[0].time) : null, last: d.length ? iso(d[d.length - 1].time) : null, close: d.length ? d[d.length - 1].close : null,
    active: window.jhWatchlistActive ? window.jhWatchlistActive() : null, quote: t('quote') }; }"""
SHOWN = r"""(a) => { const q = document.getElementById('quote'), t = ((q && (q.innerText || q.textContent)) || '').replace(/\s+/g, ' ').trim(); const s = window.jhWatchlistActive ? String(window.jhWatchlistActive()) : '';
  return s.toUpperCase() === a[0] && t.toUpperCase().indexOf(a[1]) >= 0 && t.toUpperCase().indexOf(a[1]) < 14 && (window.lastBars || []).length > 1 && !/^No bars/i.test(t); }"""
# what the dialog shows now, and what it knows: is the first row the pin known in the browser, has the directory answered
LIST = r"""() => { const U = window.JHUniSearch, G = (U && U._G && U._G()) || {}, m = (U && U.merged) ? U.merged() : [];
  const rows = Array.from(document.querySelectorAll('#jhus .us-row[data-i]')).slice(0, 6).map(e => ({ id: e.getAttribute('title'), text: (e.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 110) }));
  const head = document.querySelector('#jhus .us-grp, #jhus .us-h');
  return { rows: rows, answered: !!G.answered, failed: !!G.failed, first_is_local_pin: !!(m[0] && m[0].bench), first_group: m[0] ? m[0].grp : null,
    first: m[0] ? { id: m[0].id, name: m[0].name, src: m[0].src, type: m[0].type } : null, q: G.q || null, heading: head ? head.textContent.trim().slice(0, 40) : null }; }"""
PIN = r"""(w) => { const U = window.JHUniSearch, b = (U && U.benchRows) ? U.benchRows(w) : []; return b[0] ? { id: b[0].id, name: b[0].name, src: b[0].src } : null; }"""
# open the dialog and watch its first row from inside the page, 25 times a second: [ms since typing, first row, ticks so far].
# The tick count tells a list that was empty from a page that was too busy to be looked at.
WATCH = r"""(w) => { const t0 = performance.now(), tl = [], st = { ticks: 0, t0: t0 }; window.__jhWatch = { tl: tl, st: st };
  const read = () => { const e = document.querySelector('#jhus .us-row[data-i]'); return e ? e.getAttribute('title') : '(no rows)'; };
  st.iv = setInterval(() => { st.ticks++; const id = read(); if (tl[tl.length - 1][1] !== id) tl.push([Math.round(performance.now() - t0), id, st.ticks]); }, 40);
  window.jhOpenSearch(w); tl.push([Math.round(performance.now() - t0), read(), 0]); }"""
WATCHED = r"""() => { const W = window.__jhWatch; clearInterval(W.st.iv); return { timeline: W.tl, ticks: W.st.ticks, ms: Math.round(performance.now() - W.st.t0) }; }"""
ANSWERED = r"""(w) => { const U = window.JHUniSearch, G = (U && U._G && U._G()) || {}; return String(G.q || '').toLowerCase() === w && !!G.answered; }"""


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


def open_chart(ctx, stamp, errors):
    page = ctx.new_page()
    page.on("pageerror", lambda e: errors.append(str(e)[:200]))
    page.goto("%s/chart.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
    page.wait_for_function("() => typeof window.jhOpenSearch === 'function' && typeof window.jhGoSymbol === 'function' && !!(window.JHTvWatchlist && window.JHTvWatchlist.route) && (window.lastBars || []).length > 100", timeout=120000)
    page.wait_for_timeout(5000)
    return page


def main():
    with report("ops_6850_search_index_leads_at_once") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6850 - a benchmark word leads with its index at once, live %s" % now.isoformat(timespec="minutes"))
        stamp, week = int(time.time()), (now - timedelta(days=5)).strftime("%Y-%m-%d")
        fails, pack, shots, errors = [], {"at": now.isoformat(), "silent": [], "natural": [], "others": []}, {}, []

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL %s" % msg)
            return bool(cond)

        sync_playwright = ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
            page = open_chart(ctx, stamp, errors)
            served = page.evaluate("() => !!(window.JHUniSearch && window.JHUniSearch.benchRows)")
            ck(served, "the dialog on the live page does not have the benchmark pins yet (the new build is not served)")
            r.log("the live page serves the new dialog: %s" % served)

            held, holding = [], {"on": False}

            def gate(route):
                if holding["on"]:
                    held.append(route)
                else:
                    route.fallback()

            def release():
                holding["on"] = False
                n = len(held)
                while held:
                    try:
                        held.pop().fallback()
                    except Exception:  # noqa: BLE001
                        pass
                return n

            page.route("**/symsearch*", gate)

            def show_spy():
                page.evaluate("() => window.JHTvWatchlist.route('NASDAQ:SPY')")
                try:
                    page.wait_for_function(SHOWN, arg=["SPY", "SPY"], timeout=45000)
                except Exception:  # noqa: BLE001
                    pass

            r.section("1. the directory silent: what leads 1.2 s after typing, and what the row opens")
            for word, pin in WORDS:
                holding["on"] = True
                page.evaluate("(w) => window.jhOpenSearch(w)", word)
                page.wait_for_timeout(1200)
                before = page.evaluate(LIST)
                known = page.evaluate(PIN, word)
                rec = {"word": word, "pin": pin, "before": before, "known": known, "held": len(held)}
                # the page keeps every directory answer it has received: a word it already asked as another word's alternate
                # ("S&P 500" while searching sp500) is answered from that at once, and nothing is left to hold back
                remembered = before["answered"] and not any("q=" + w in rt.request.url.lower() for rt in held for w in [word.replace(" ", "%20").replace("&", "%26")])
                rec["remembered"] = remembered
                if remembered:
                    ok = ck(before["rows"] and before["rows"][0]["id"] == pin and before["first_group"] == "Best match",
                            "%s, answered from this page's earlier search: first row %s" % (word, before["rows"][:1]))
                    r.log("%-9s already answered by the directory earlier on this page (asked as another word's alternate): row 1 %s | then %s" % (
                        word, before["rows"][0]["text"] if before["rows"] else None, [x["id"] for x in before["rows"][1:4]]))
                else:
                    ok = ck(before["rows"] and before["rows"][0]["id"] == pin and before["first_is_local_pin"] and not before["answered"] and before["first_group"] == "Best match",
                            "%s, directory silent: first row %s (local pin %s, answered %s, group %s)" % (word, before["rows"][:1], before["first_is_local_pin"], before["answered"], before["first_group"]))
                    r.log("%-9s directory silent (%d requests held): row 1 %s | then %s%s" % (
                        word, len(held), before["rows"][0]["text"] if before["rows"] else None, [x["id"] for x in before["rows"][1:4]], "" if ok else "  <-- not the index"))
                if word == "sp500":
                    shots["sp500_directory_silent_1440"] = page.screenshot(type="jpeg", quality=50)
                if word in CLICK and before["rows"]:
                    page.evaluate("() => { const e = document.querySelector('#jhus .us-row[data-i]'); (e.querySelector('.us-sym') || e).click(); }")
                    try:
                        page.wait_for_function(SHOWN, arg=[pin, pin], timeout=45000)
                    except Exception:  # noqa: BLE001
                        pass
                    page.wait_for_timeout(800)
                    st = page.evaluate(STATE)
                    rec["opened"] = st
                    ck(st["active"] == pin and st["bars"] > MIN_INDEX_BARS and (st["last"] or "") >= week and "No bars" not in (st["quote"] or ""),
                       "%s: took the first row with the directory silent -> active %s, %d bars, last %s | %s" % (word, st["active"], st["bars"], st["last"], (st["quote"] or "")[:60]))
                    r.log("          took row 1 with the directory still silent -> the chart shows %s: %d bars %s..%s, last close %s" % (st["active"], st["bars"], st["first"], st["last"], st["close"]))
                    if word == "sp500":
                        shots["sp500_first_row_chart_1440"] = page.screenshot(type="jpeg", quality=50)
                    show_spy()
                    page.evaluate("(w) => window.jhOpenSearch(w)", word)
                    page.wait_for_timeout(900)
                    again = page.evaluate(LIST)
                    ck(again["rows"] and again["rows"][0]["id"] == pin, "%s reopened with the directory still silent: first row %s" % (word, again["rows"][:1]))
                # the directory answers
                t0 = time.time()
                n = release()
                try:
                    page.wait_for_function(ANSWERED, arg=word, timeout=75000)
                except Exception:  # noqa: BLE001
                    pass
                took = time.time() - t0
                page.wait_for_timeout(500)
                after = page.evaluate(LIST)
                rec["after"], rec["directory_answer_s"] = after, round(took, 1)
                first = after["first"] or {}
                ck(after["answered"] and after["rows"] and after["rows"][0]["id"] == pin and not after["first_is_local_pin"],
                   "%s, directory answered: first row %s (answered %s, still the local pin %s)" % (word, after["rows"][:1], after["answered"], after["first_is_local_pin"]))
                ck(known and first.get("id") == known["id"] and first.get("name") == known["name"] and first.get("src") == known["src"],
                   "%s: the pin known in the browser %s is not what the directory lists first %s" % (word, known, first))
                ck(before["rows"] and after["rows"] and before["rows"][0]["text"] == after["rows"][0]["text"],
                   "%s: the first row reads differently once the directory answers: %r -> %r" % (word, before["rows"][:1], after["rows"][:1]))
                r.log("          the directory answered %.1f s after %d requests were let through: %s; row 1 unchanged: %s" % (
                    took, n, [x["id"] for x in after["rows"][:4]], bool(before["rows"] and after["rows"] and before["rows"][0]["text"] == after["rows"][0]["text"])))
                if word == "sp500":
                    ck([x["id"] for x in after["rows"][:3]] == ["I:SPX", "SPY", "fred:SP500"], "sp500 settles as %s" % [x["id"] for x in after["rows"][:3]])
                    shots["sp500_directory_answered_1440"] = page.screenshot(type="jpeg", quality=50)
                pack["silent"].append(rec)
                page.keyboard.press("Escape")
                page.wait_for_timeout(500)

            r.section("2. words that are not index words")
            for word in OTHERS:
                holding["on"] = True
                page.evaluate("(w) => window.jhOpenSearch(w)", word)
                page.wait_for_timeout(1200)
                li = page.evaluate(LIST)
                known = page.evaluate(PIN, word)
                ck(known is None and not li["first_is_local_pin"] and not any(str(x["id"]).startswith("I:") for x in li["rows"]),
                   "%s: the dialog put an index of its own in the list: %s" % (word, li["rows"][:3]))
                r.log("%-9s directory silent: nothing added (first rows %s)" % (word, [x["id"] for x in li["rows"][:3]]))
                pack["others"].append({"word": word, "list": li})
                release()
                page.keyboard.press("Escape")
                page.wait_for_timeout(400)
            ctx.close()

            r.section("3. nothing held back, a fresh page: the first row, watched inside the page until the directory answers")
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
            page = open_chart(ctx, stamp + 1, errors)
            for word, pin in NATURAL:
                t0 = time.time()
                page.evaluate(WATCH, word)
                answered_at = None
                while time.time() - t0 < 75:
                    page.wait_for_timeout(250)
                    if page.evaluate(ANSWERED, word):
                        answered_at = round(time.time() - t0, 1)
                        break
                page.wait_for_timeout(600)
                seen = page.evaluate(WATCHED)
                final = page.evaluate(LIST)
                timeline = seen["timeline"]
                shown = [x for x in timeline if x[1] != "(no rows)"]
                wrong = [x for x in shown if x[1] != pin]
                first_ms = shown[0][0] if shown else None
                ck(shown and not wrong and first_ms is not None and first_ms <= 1500 and final["rows"] and final["rows"][0]["id"] == pin and final["answered"],
                   "%s typed naturally: first row over time %s, directory answered at %s s, final %s" % (word, timeline, answered_at, final["rows"][:1]))
                r.log("%-7s first row %s ms after typing: %s; every first row seen until the directory answered (%s s): %s; the page ran %d of %d possible samples; final rows %s" % (
                    word, first_ms, shown[0][1] if shown else None, answered_at, sorted({x[1] for x in shown}), seen["ticks"], seen["ms"] // 40, [x["id"] for x in final["rows"][:4]]))
                pack["natural"].append({"word": word, "pin": pin, "timeline": timeline, "directory_answer_s": answered_at, "final": final})
                page.keyboard.press("Escape")
                page.wait_for_timeout(600)
            ck(not errors, "page errors: %s" % errors[:3])
            r.log("page errors: %d %s" % (len(errors), errors[:2]))
            ctx.close()

            r.section("at 390 px")
            ctx = browser.new_context(viewport={"width": 390, "height": 844}, device_scale_factor=2, is_mobile=True, has_touch=True, user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) jh-ops-probe Mobile Safari")
            merr = []
            page = open_chart(ctx, stamp + 2, merr)
            mheld, mon = [], {"on": True}
            page.route("**/symsearch*", lambda route: mheld.append(route) if mon["on"] else route.fallback())
            page.evaluate("() => window.jhOpenSearch('sp500')")
            page.wait_for_timeout(1500)
            li = page.evaluate(LIST)
            shots["sp500_directory_silent_390"] = page.screenshot(type="jpeg", quality=55)
            ck(li["rows"] and li["rows"][0]["id"] == "I:SPX" and li["first_is_local_pin"], "390 px, directory silent: first row %s" % li["rows"][:1])
            page.evaluate("() => { const e = document.querySelector('#jhus .us-row[data-i]'); (e.querySelector('.us-sym') || e).click(); }")
            try:
                page.wait_for_function(SHOWN, arg=["I:SPX", "I:SPX"], timeout=45000)
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(800)
            st = page.evaluate(STATE)
            ck(st["active"] == "I:SPX" and st["bars"] > MIN_INDEX_BARS and (st["last"] or "") >= week, "390 px: first row tapped -> active %s, %d bars, last %s" % (st["active"], st["bars"], st["last"]))
            fits = page.evaluate("() => document.documentElement.scrollWidth <= window.innerWidth + 1")
            ck(fits and not merr, "390 px: the page is wider than the screen (%s) or page errors %s" % (not fits, merr[:2]))
            r.log("390 px: directory silent, row 1 %s; tapped -> the chart shows %s, %d bars to %s, last close %s; fits the screen: %s; page errors: %d" % (
                li["rows"][0]["text"] if li["rows"] else None, st["active"], st["bars"], st["last"], st["close"], fits, len(merr)))
            mon["on"] = False
            while mheld:
                try:
                    mheld.pop().fallback()
                except Exception:  # noqa: BLE001
                    pass
            pack["mobile"] = {"list": li, "opened": st, "errors": merr}
            ctx.close()
            browser.close()

        pack["page_errors"], pack["failures"] = errors, fails
        waits = [x.get("directory_answer_s") for x in pack["silent"]] + [x.get("directory_answer_s") for x in pack["natural"]]
        r.kv(failures=len(fails), words=len(pack["silent"]), words_leading_with_the_index=sum(1 for x in pack["silent"] if x["before"]["rows"] and x["before"]["rows"][0]["id"] == x["pin"]),
             words_tested_with_the_directory_silent=sum(1 for x in pack["silent"] if not x.get("remembered")),
             natural_words=len(pack["natural"]), natural_always_index=sum(1 for x in pack["natural"] if any(t[1] == x["pin"] for t in x["timeline"]) and all(t[1] in (x["pin"], "(no rows)") for t in x["timeline"])),
             natural_first_row_ms=[next((t[0] for t in x["timeline"] if t[1] != "(no rows)"), None) for x in pack["natural"]],
             slowest_directory_answer_s=max([w for w in waits if w is not None] or [0]))
        embed(r, "index-leads-proof", pack)
        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        if fails:
            r.fail("%d checks failed: %s" % (len(fails), " | ".join(fails)[:1500]))
            sys.exit(1)
        r.ok("LIVE PROOF: %d index words lead with the index 1.2 s after typing (%d of them with the directory silent); the row opens the index chart; the directory's own rows then take over unchanged" % (
            len(pack["silent"]), sum(1 for x in pack["silent"] if not x.get("remembered"))))


if __name__ == "__main__":
    main()
