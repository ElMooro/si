"""ops 6929 - year-over-year on every chart of the Fed securities held outright desk: live proof in a real browser
(read-only, re-runnable).

Khalid 2026-11-23: "on all the charts on: Fed securities held outright you should also add the option to view the chart
on year to year percentage change. add it to every single chart".

What has to hold on https://justhodl.ai/securities-held-outright.html:
  1. the desk opens as it always did - every chart as Level - with two buttons on every chart (Level, YoY %) and one
     switch for the whole desk; a strip in Level is the same picture before and after a visit to YoY %;
  2. the switch, clicked, redraws every chart as FRED's "percent change from a year ago" - this op reads the same 222
     series itself, one at a time, and works every figure out in Python: ((x(t) / x(t - 364 days)) - 1) * 100, no figure
     where the year-ago print is missing or zero.  Each card has to print this op's latest figure, its two prints and
     their dates, draw this op's number of figures, break the line where this op finds a gap, and stop its scale where
     the rule says (three interquartile ranges beyond the quartiles, only when a figure stands a whole fence beyond);
  3. a line that is itself a change, or that ended before 1996, stays as it is and says why; a line below zero says
     what a plus means;
  4. a chart's own button moves that chart alone; the choice is kept in the browser; a change line's YoY % button
     explains instead of drawing; the full-screen view carries the same two buttons and the same figure;
  5. on a phone nothing scrolls sideways and the buttons are there;
  6. the chart's Fed Outright button opens the desk with the switch; the Dollar, ICE BofA and Macro desks have no
     such buttons and read as before.
JPEGs are embedded (base64).  Nothing on the site changes.
"""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import base64
import json
import re
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
PAGE = "securities-held-outright.html"
FROM = "1996-01-01"
PAUSE = 0.25
CATALOG = json.loads(Path(__file__).with_name("ops_6834_desk_catalogs.json").read_text(encoding="utf-8"))["soma"]
WHY = {"change": "Drawn as it is: this line is itself a change, so a percentage of it would mislead.",
       "none": "Drawn as it is: there are no prints on this strip.",
       "crosses": "Drawn as it is: this line crosses zero, so a percentage of it would mislead.",
       "short": "Drawn as it is: no print has a year-ago print other than zero yet."}
SAMPLE = ["FRED:WSHOSHO", "FRED:WSHOTSL", "FRED:WSHOMCB", "FRED:WSHOBL", "FRED:WUDSHO", "FRED:TREAS15", "FRED:D1WSHOL", "FRED:RAGSOTBO"]


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


def fetch(url, timeout=60):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def series(sid):
    for attempt in range(3):
        try:
            doc = json.loads(fetch(PROXY + "/series?id=" + urllib.parse.quote(sid), 90))
            obs = []
            for row in doc.get("obs") or doc.get("bars") or []:
                d, v = (row[0], row[1]) if isinstance(row, list) else (row.get("time") or row.get("date"), row.get("close", row.get("value")))
                try:
                    v = float(v)
                except (TypeError, ValueError):
                    continue
                if d and v == v:
                    obs.append((str(d)[:10], v))
            return sorted(obs)
        except Exception:  # noqa: BLE001
            time.sleep(2 + 3 * attempt)
    return []


def scale(vals):
    """Every figure, unless one stands more than a whole far-out fence beyond it; zero and the latest always inside."""
    n, last, mn, mx = len(vals), vals[-1], min(vals), max(vals)
    lo, hi = mn, mx
    if n >= 12:
        s = sorted(vals)

        def q(p):
            k = (n - 1) * p
            a = int(k)
            b = min(a + 1, n - 1)
            return s[a] + (s[b] - s[a]) * (k - a)
        q1, q3 = q(0.25), q(0.75)
        if not q3 > q1:
            q1, q3 = q(0.1), q(0.9)
        if q3 > q1:
            flo, fhi = q1 - 3 * (q3 - q1), q3 + 3 * (q3 - q1)
            if mx > fhi + (fhi - flo):
                hi = fhi
            if mn < flo - (fhi - flo):
                lo = flo
    lo, hi = min(lo, 0, last), max(hi, 0, last)
    return lo, hi, ("lo" if mn < lo else "") + ("hi" if mx > hi else "")


def work(obs, tagged):
    """This op's own percent change from a year ago for one weekly line, and what its card has to show."""
    since = [o for o in obs if o[0] >= FROM]
    if tagged:
        return {"code": "change"}
    if not since:
        return {"code": "none"}
    pos, neg = sum(1 for _, v in since if v > 0), sum(1 for _, v in since if v < 0)
    if pos and neg:
        return {"code": "crosses"}
    at, y = dict(obs), []
    for d, v in since:
        back = (date.fromisoformat(d) - timedelta(days=364)).isoformat()
        was = at.get(back)
        if was is not None and was != 0:
            y.append((d, (v / was - 1) * 100, v, was, back))
    if len(y) < 2:
        return {"code": "short"}
    runs = [[y[0]]]
    for a, b in zip(y, y[1:]):
        if (date.fromisoformat(b[0]) - date.fromisoformat(a[0])).days > 17.5:
            runs.append([])
        runs[-1].append(b)
    vals = [p[1] for p in y]
    lo, hi, cut = scale(vals)
    return {"code": "below" if neg else "ok", "n": len(y), "last": y[-1], "lo": lo, "hi": hi, "cut": cut, "lines": sum(1 for run in runs if len(run) > 1), "dots": sum(1 for run in runs if len(run) == 1),
            "max": max(y, key=lambda p: p[1]), "min": min(y, key=lambda p: p[1]), "first": y[0][0],
            "crossings": [y[i][0] for i in range(1, len(y)) if (y[i][1] > 0) != (y[i - 1][1] > 0)]}


def num(text):
    return float(str(text).replace("−", "-").replace(",", "").replace("%", "").replace("+", ""))


def near(shown, want):
    """A printed percentage against this op's figure: one decimal below a thousand, whole numbers above."""
    try:
        return abs(num(shown) - want) <= (0.51 if abs(want) >= 999.95 else 0.0501)
    except ValueError:
        return False


def hydrate(page):
    page.evaluate("() => { document.getElementById('macro-body').scrollTop = 0 }")
    for _ in range(900):
        done = page.evaluate("() => { const b = document.getElementById('macro-body'); b.scrollTop += b.clientHeight * 0.8; return b.scrollTop + b.clientHeight >= b.scrollHeight - 4 }")
        page.wait_for_timeout(260)
        if done:
            break
    for _ in range(240):
        busy = page.evaluate("() => [...document.querySelectorAll('#macro-body .macro-item')].filter(c => !c.dataset.metricState || c.dataset.metricState === 'queued' || c.dataset.metricState === 'loading').length")
        if not busy:
            break
        page.evaluate("() => { const c = [...document.querySelectorAll('#macro-body .macro-item')].find(c => !c.dataset.metricState || c.dataset.metricState === 'queued'); if (c) c.scrollIntoView({ block: 'center' }) }")
        page.wait_for_timeout(500)
    page.wait_for_timeout(1500)


CARDS = """() => [...document.querySelectorAll('#macro-body .macro-item')].map(c => { const svg = c.querySelector('.macro-hist-svg svg'), line = c.querySelector('.macro-hist-yoy'), q = s => c.querySelectorAll(s).length;
  return { s: c.dataset.symbol, state: c.dataset.metricState, view: c.dataset.view || '', code: c.dataset.yoy || '', what: (c.querySelector('.macro-hist-what') || {}).textContent || '',
    btn: [...c.querySelectorAll('.macro-hist-head .macro-view-btn')].map(b => b.dataset.view + (b.classList.contains('on') ? '*' : '') + (b.classList.contains('off') ? '-' : '') + (b.getAttribute('aria-pressed') === 'true' ? 'p' : '')).join(' '),
    n: svg && svg.dataset.yoy ? Number(svg.dataset.yoy) : null, lo: svg && svg.dataset.lo !== undefined ? Number(svg.dataset.lo) : null, hi: svg && svg.dataset.hi !== undefined ? Number(svg.dataset.hi) : null, cut: svg ? (svg.dataset.cut || '') : null,
    label: svg ? svg.getAttribute('aria-label') : '', zero: q('.macro-hist-zero'), lines: q('.macro-hist-line'), dots: q('.macro-hist-pt'), cuts: q('.macro-hist-cut'), bears: q('.macro-hist-bear'), empty: (c.querySelector('.macro-hist-empty') || {}).textContent || '',
    line: line ? line.textContent : null, hidden: line ? line.hidden : null, bold: line && line.querySelector('b') ? line.querySelector('b').textContent : '', cap: (c.querySelector('.macro-hist-cap') || {}).textContent || '',
    y: (c.querySelector('.macro-change[data-horizon="Y"]') || {}).textContent || '', ytitle: (c.querySelector('.macro-change[data-horizon="Y"]') || {}).title || '', note: !!c.querySelector('.macro-change-note'),
    bad: /undefined|NaN|\\[object|Infinity/.test(c.innerText) } })"""
BAR = """() => { const b = document.getElementById('macro-viewbar'); if (!b) return null; const r = b.getBoundingClientRect();
  return { text: b.innerText.replace(/\\s+/g, ' ').trim(), on: [...b.querySelectorAll('[data-allview]')].filter(x => x.classList.contains('on')).map(x => x.dataset.allview).join(','), own: document.getElementById('macro-viewbar-own').textContent.trim(),
    first: document.getElementById('macro-body').firstElementChild.id, w: Math.round(r.width), h: Math.round(r.height) } }"""
SAVED = """() => { const s = JSON.parse(localStorage.getItem('jh_soma_workspace_v1') || 'null'); return s ? { chartView: s.chartView || null, own: s.categories.flatMap(c => c.items).filter(i => i.view).map(i => i.symbol + '=' + i.view) } : null }"""
FOCUS = """() => { const f = document.getElementById('macro-focus'), box = document.getElementById('macro-focus-chartbox'), svg = box && box.querySelector('svg'), t = s => [...(svg ? svg.querySelectorAll(s) : [])].map(e => e.textContent);
  return { open: f.classList.contains('open'), title: document.getElementById('macro-focus-title').textContent, view: box && box.dataset.view ? box.dataset.view : null, btn: [...f.querySelectorAll('.macro-view-btn')].map(b => b.dataset.view + (b.classList.contains('on') ? '*' : '') + (b.classList.contains('off') ? '-' : '')).join(' '),
    line: (document.getElementById('macro-focus-yoy') || {}).textContent || '', now: t('.macro-hist-nowlabel')[0] || '', axis: t('.macro-hist-axis'), zero: svg ? svg.querySelectorAll('.macro-hist-zero').length : 0, lines: svg ? svg.querySelectorAll('.macro-hist-line').length : 0,
    cut: t('.macro-hist-cutlabel'), legend: t('.macro-hist-legend')[0] || '', n: svg && svg.dataset.yoy ? Number(svg.dataset.yoy) : null, events: t('.macro-hist-label'), rows: f.querySelectorAll('.macro-focus-table tbody tr').length,
    changes: [...f.querySelectorAll('.macro-focus-changes .macro-change')].map(e => e.textContent), w: Math.round(f.getBoundingClientRect().width), vw: innerWidth, box: svg ? Math.round(svg.getBoundingClientRect().height) : 0 } }"""
STRIP = "(s) => { const c = document.querySelector('#macro-body .macro-item[data-symbol=\"' + s + '\"]'); const v = c && c.querySelector('.macro-hist-svg svg'); return v ? v.outerHTML : null }"
CARD = "#macro-body .macro-item[data-symbol='%s']"
LINE = re.compile(r"^([+−]?[\d,.]+)% from a year ago · (\d{4}-\d{2}-\d{2}): ([−\d,.]+) against ([−\d,.]+) on (\d{4}-\d{2}-\d{2})(.*)$")


def main():
    with report("ops_6929_held_outright_yoy_live_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6929 - year over year on every chart of the held outright desk, real browser on the live site (read-only), %s" % now.isoformat(timespec="minutes"))
        stamp = int(time.time())
        fails, shots = [], {}

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL: " + msg)
            return bool(cond)

        r.section("0. The live site serves the new build")
        served = False
        for attempt in range(40):
            js = fetch("%s/assets/jh-workspaces.js?nocache=%d" % (BASE, stamp + attempt))
            css = fetch("%s/assets/jh-workspaces.css?nocache=%d" % (BASE, stamp + attempt))
            served = all(k in js for k in ("static yoyHistory(", "static setAllViews(", "data-allview", "static paintFocusChart(")) and ".macro-view-btn.on" in css and ".macro-hist-zero" in css
            if served:
                break
            time.sleep(30)
        r.log("served after %d tries: engine %d bytes, stylesheet %d bytes, year-over-year present %s" % (attempt + 1, len(js), len(css), served))
        if not served:
            r.fail("the served site is not the new build yet (Pages deploy pending?)")
            raise SystemExit(1)
        page_html = fetch("%s/%s?nocache=%d" % (BASE, PAGE, stamp))
        r.log("the page names its engine as: %s" % (re.findall(r'src="(/assets/jh-workspaces\.js[^"]*)"', page_html) or ["?"])[0])

        r.section("1. This op's own figures: 222 series read one at a time, percent change from a year ago worked out in Python")
        items = [("FRED:" + it[0], it[1], cat["cat"], (it[3] if len(it) > 3 else "")) for cat in CATALOG for it in cat["items"]]
        mine, t0, unread = {}, time.time(), []
        for sid, _, _, tag in items:
            obs = series(sid)
            time.sleep(PAUSE)
            if not obs:
                unread.append(sid)
                continue
            days = [date.fromisoformat(d).toordinal() for d, _ in obs]
            gaps = sorted(b - a for a, b in zip(days, days[1:]) if b > a)
            w = work(obs, tag == "C")
            w.update(obs=obs, gap=gaps[len(gaps) // 2] if gaps else None)
            mine[sid] = w
        codes = {}
        for w in mine.values():
            codes[w["code"]] = codes.get(w["code"], 0) + 1
        r.log("read %d of %d in %.0f s; could not read: %s" % (len(mine), len(items), time.time() - t0, unread or "none"))
        r.log("what each line is, by this op: %s | every line weekly: %s" % (json.dumps(codes, sort_keys=True), all(w["gap"] == 7 for w in mine.values())))
        ck(len(mine) >= len(items) - 3, "this op could not read %d series itself: %s" % (len(unread), unread[:6]))
        ck(all(w["gap"] == 7 for w in mine.values()), "a line is no longer weekly: %s" % [s for s, w in mine.items() if w["gap"] != 7][:6])
        drawn = {s: w for s, w in mine.items() if w["code"] in ("ok", "below")}
        cut_n = sum(1 for w in drawn.values() if w["cut"])
        r.log("to be drawn year over year: %d (of them %d below zero throughout); scale stops short on %d; lines with a break %d; largest figure %s" % (
            len(drawn), codes.get("below", 0), cut_n, sum(1 for w in drawn.values() if w["lines"] + w["dots"] > 1),
            max((("%.0f%% on %s" % (w["max"][1], w["max"][0]), s) for s, w in drawn.items()), key=lambda p: mine[p[1]]["max"][1])))
        # the year against the Federal Reserve's own change-from-a-year-ago line
        tot, fed = mine.get("FRED:WSHOSHO"), mine.get("FRED:RESPPALGXCH52NWW")
        if tot and fed:
            at, printed = dict(tot["obs"]), fed["obs"][-520:]
            same = sum(1 for d, v in printed if at.get(d) is not None and at.get((date.fromisoformat(d) - timedelta(days=364)).isoformat()) is not None and abs(at[d] - at[(date.fromisoformat(d) - timedelta(days=364)).isoformat()] - v) <= 1.5)
            r.log("the total less its print 364 days earlier equals the H.4.1's own change-from-a-year-ago line on %d of the last %d weeks" % (same, len(printed)))
            ck(same == len(printed), "the 52-week base no longer equals the H.4.1 change-from-a-year-ago line (%d of %d)" % (same, len(printed)))

        sync_playwright = ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = launch(p, r.log)

            def fresh(width, height=900):
                ctx = browser.new_context(viewport={"width": width, "height": height}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
                page = ctx.new_page()
                errors = []
                page.on("pageerror", lambda e: errors.append("pageerror: %s" % e))
                page.add_init_script("window.open = () => null;")
                return ctx, page, errors

            def opened(page):
                page.goto("%s/%s?nocache=%d" % (BASE, PAGE, stamp), wait_until="load")
                page.wait_for_selector("#macro-body .macro-item", timeout=40000)
                page.wait_for_function("() => document.querySelectorAll('#macro-body .macro-item[data-metric-state=ready]').length >= 3", timeout=90000)

            def settle(page, view, symbols=None):
                # every card that has read is drawn in the view asked for (or says why not)
                page.wait_for_function("""([view, ids]) => [...document.querySelectorAll('#macro-body .macro-item[data-metric-state=ready]')].filter(c => !ids || ids.includes(c.dataset.symbol))
                    .every(c => c.dataset.view === view || (view === 'yoy' && c.dataset.view === 'level' && !['ok', 'below'].includes(c.dataset.yoy)))""", arg=[view, symbols], timeout=60000)
                page.wait_for_timeout(500)

            # ---------------------------------------------------------------- the desk as it opens
            r.section("2. The desk opens as before: every chart as Level, two buttons on each, one switch for all")
            ctx, page, errors = fresh(1440)
            opened(page)
            bar = page.evaluate(BAR)
            r.log("the switch: %s" % json.dumps(bar, ensure_ascii=False))
            ck(bar and bar["on"] == "level" and bar["first"] == "macro-viewbar" and "FRED's formula" in bar["text"] and "53 weeks" in bar["text"], "the desk switch is missing or does not open on Level: %s" % bar)
            hydrate(page)
            rows = page.evaluate(CARDS)
            by = {x["s"]: x for x in rows}
            ready = [x for x in rows if x["state"] == "ready"]
            r.log("%d cards, %d read; saved in the browser: %s; page errors %d" % (len(rows), len(ready), page.evaluate(SAVED), len(errors)))
            ck(len(rows) == 222 and len(ready) == 222, "%d cards, %d read (wanted 222): %s" % (len(rows), len(ready), [x["s"] for x in rows if x["state"] != "ready"][:8]))
            wrong = [x["s"] for x in ready if not (x["view"] == "level" and x["zero"] == 0 and x["n"] is None and x["hidden"] is True and x["what"] == "1996 → today" and x["label"] == "history since 1996")]
            ck(not wrong, "not opened as Level: %s" % wrong[:8])
            wrong = [x["s"] + " [" + x["btn"] + "]" for x in ready if x["s"] in mine and x["btn"] != ("level*p yoy" if mine[x["s"]]["code"] in ("ok", "below") else "level*p yoy-")]
            ck(not wrong, "the two buttons are not as they should be on: %s" % wrong[:6])
            wrong = [x["s"] for x in ready if not (x["lines"] == 1 or x["empty"])]
            ck(not wrong, "a Level strip is not one line: %s" % wrong[:8])
            ck(sum(1 for x in ready if x["note"]) == 60 and not any(x["bad"] for x in ready), "change notes %d, broken text on %s" % (sum(1 for x in ready if x["note"]), [x["s"] for x in ready if x["bad"]][:5]))
            ck(page.evaluate(SAVED)["chartView"] is None, "a first visit already carries a saved view")
            level_before = {s: page.evaluate(STRIP, s) for s in SAMPLE}
            y_before = {x["s"]: x["y"] for x in ready}
            t = by["FRED:WSHOSHO"]["ytitle"]
            r.log("the Y box of the total says where it counts from: %s" % t)
            ck("52 weeks before" in t and "on or before the same date a year earlier" in t, "the Y box does not say which print it counts from")
            page.evaluate("() => { document.getElementById('macro-body').scrollTop = 0 }")
            page.wait_for_timeout(400)
            shots["1_opens_as_level"] = page.screenshot(type="jpeg", quality=60)

            # ---------------------------------------------------------------- the switch
            r.section("3. The switch, clicked: every chart as percent change from a year ago, against this op's figures")
            page.click("#macro-viewbar .macro-view-btn[data-allview='yoy']")
            settle(page, "yoy")
            page.wait_for_timeout(800)
            bar = page.evaluate(BAR)
            rows = page.evaluate(CARDS)
            by = {x["s"]: x for x in rows}
            saved = page.evaluate(SAVED)
            r.log("after the click: switch on '%s'; saved in the browser %s; %d cards drawn year over year, %d left as they are" % (bar["on"], saved, sum(1 for x in rows if x["view"] == "yoy"), sum(1 for x in rows if x["view"] == "level")))
            ck(bar["on"] == "yoy" and saved["chartView"] == "yoy" and not saved["own"], "the switch did not take: %s %s" % (bar, saved))
            checked = {"figure": 0, "prints": 0, "count": 0, "scale": 0, "runs": 0, "cut": 0, "below": 0, "kept": 0}
            diffs = []
            for sid, w in mine.items():
                x = by.get(sid)
                if not x or x["state"] != "ready":
                    diffs.append("%s did not read" % sid)
                    continue
                if w["code"] not in ("ok", "below"):
                    good = x["view"] == "level" and x["code"] == w["code"] and x["line"] == WHY[w["code"]] and x["hidden"] is False and x["zero"] == 0 and x["btn"] == "level*p yoy-" and x["what"] == "1996 → today"
                    if not good:
                        diffs.append("%s (%s) should stay as it is and say why: view %s, code %s, '%s', buttons %s" % (sid, w["code"], x["view"], x["code"], x["line"], x["btn"]))
                    checked["kept"] += good
                    continue
                if not (x["view"] == "yoy" and x["code"] == w["code"] and x["btn"] == "level yoy*p" and x["what"] == "1996 → today" and x["zero"] == 1 and x["label"] == "percent change from a year ago since 1996"):
                    diffs.append("%s is not drawn year over year: view %s code %s buttons %s zero lines %s" % (sid, x["view"], x["code"], x["btn"], x["zero"]))
                    continue
                m = LINE.match(x["line"] or "")
                d, v, level, was, back = w["last"]
                if not m:
                    diffs.append("%s: the sentence does not read: %s" % (sid, x["line"]))
                    continue
                if near(m.group(1), v) and near(x["bold"], v):
                    checked["figure"] += 1
                else:
                    diffs.append("%s: page %s, here %.4f%%" % (sid, m.group(1), v))
                if m.group(2) == d and m.group(5) == back and abs(num(m.group(3)) - level) < 0.0006 and abs(num(m.group(4)) - was) < 0.0006:
                    checked["prints"] += 1
                else:
                    diffs.append("%s: page '%s', here %s %s against %s on %s" % (sid, x["line"][:90], d, level, was, back))
                if x["n"] == w["n"]:
                    checked["count"] += 1
                else:
                    diffs.append("%s: %s figures drawn, %d here" % (sid, x["n"], w["n"]))
                if x["lo"] is not None and abs(x["lo"] - w["lo"]) <= 1e-9 * max(1, abs(w["lo"])) and abs(x["hi"] - w["hi"]) <= 1e-9 * max(1, abs(w["hi"])) and x["cut"] == w["cut"] and x["cuts"] == len(w["cut"]) // 2:
                    checked["scale"] += 1
                else:
                    diffs.append("%s: scale %s to %s cut '%s' (%d edge marks), here %.6f to %.6f cut '%s'" % (sid, x["lo"], x["hi"], x["cut"], x["cuts"], w["lo"], w["hi"], w["cut"]))
                if x["lines"] == w["lines"] and x["dots"] == w["dots"]:
                    checked["runs"] += 1
                else:
                    diffs.append("%s: %d runs and %d single figures drawn, here %d and %d" % (sid, x["lines"], x["dots"], w["lines"], w["dots"]))
                tail = m.group(6)
                if "hi" in w["cut"]:
                    hm = re.search(r"off the scale: high ([+−]?[\d,.]+)% in (\d{4}-\d{2})", tail)
                    if hm and near(hm.group(1), w["max"][1]) and hm.group(2) == w["max"][0][:7]:
                        checked["cut"] += 1
                    else:
                        diffs.append("%s: the high left off the scale is not printed right: '%s', here %.1f%% in %s" % (sid, tail, w["max"][1], w["max"][0][:7]))
                elif "off the scale: high" in tail:
                    diffs.append("%s: says a high is off the scale, here none is" % sid)
                if "lo" in w["cut"]:
                    lm = re.search(r"off the scale: low ([+−]?[\d,.]+)% in (\d{4}-\d{2})", tail)
                    if not (lm and near(lm.group(1), w["min"][1]) and lm.group(2) == w["min"][0][:7]):
                        diffs.append("%s: the low left off the scale is not printed right: '%s'" % (sid, tail))
                if (w["code"] == "below") == ("below zero: by FRED's formula a plus means it grew in size" in tail):
                    checked["below"] += w["code"] == "below"
                else:
                    diffs.append("%s: the below-zero sentence is %s" % (sid, "missing" if w["code"] == "below" else "there by mistake"))
                if x["y"] != y_before.get(sid) or x["bad"]:
                    diffs.append("%s: the Y box moved (%s, was %s) or the card has broken text" % (sid, x["y"], y_before.get(sid)))
            r.log("against this op's own arithmetic, %d lines drawn year over year: latest figure equal on %d; both prints and both dates on %d; number of figures on %d; scale and its edge marks on %d; unbroken runs and single figures on %d" % (
                len(drawn), checked["figure"], checked["prints"], checked["count"], checked["scale"], checked["runs"]))
            r.log("the high left off the scale printed right on %d of %d; the below-zero sentence on %d of %d; left as they are and saying why: %d of %d" % (
                checked["cut"], sum(1 for w in drawn.values() if "hi" in w["cut"]), checked["below"], codes.get("below", 0), checked["kept"], len(mine) - len(drawn)))
            for d_ in diffs[:14]:
                r.log("  differs: " + d_)
            ck(not diffs, "%d differences between the page and this op's figures: %s" % (len(diffs), "; ".join(diffs[:5])))
            heads = page.evaluate("() => [...document.querySelectorAll('#macro-body .macro-item .macro-hist-head')].map(h => { const k = [...h.children].map(e => Math.round(e.getBoundingClientRect().top + e.getBoundingClientRect().height / 2)); return Math.max(...k) - Math.min(...k) })")
            r.log("the head of every strip (range, the two buttons, the legend) stands on one line: %d of %d" % (sum(1 for h in heads if h <= 6), len(heads)))
            ck(len(heads) == 222 and all(h <= 6 for h in heads), "the head of a strip wraps onto a second line on %d cards" % sum(1 for h in heads if h > 6))
            for sid in SAMPLE:
                w, x = mine.get(sid), by.get(sid)
                if not w or not x or w["code"] not in ("ok", "below"):
                    continue
                r.log("  %-14s %s | scale %.1f to %.1f%s | %d figures from %s in %d run(s) | crossed zero %d times, last %s | at the turns: %s" % (
                    sid[5:], x["line"], w["lo"], w["hi"], " (stops short)" if w["cut"] else "", w["n"], w["first"], w["lines"] + w["dots"], len(w["crossings"]), w["crossings"][-1] if w["crossings"] else "-", x["cap"][:110]))
            lead = mine.get("FRED:RAGSOTBO")
            if lead and lead["code"] == "ok":
                r.log("a line with prints before 1996 (RAGSOTBO): its first figure here is %s, the card draws %s figures" % (lead["first"], by["FRED:RAGSOTBO"]["n"]))
                ck(lead["first"] < "1996-02-01", "the first year of the strip has no figures: first %s" % lead["first"])
            page.evaluate("() => { document.getElementById('macro-body').scrollTop = 0 }")
            page.wait_for_timeout(500)
            shots["2_year_over_year"] = page.screenshot(type="jpeg", quality=60)
            page.evaluate("(s) => document.querySelector('#macro-body .macro-item[data-symbol=\"' + s + '\"]').scrollIntoView({ block: 'start' })", "FRED:TREAST")
            page.wait_for_timeout(500)
            shots["3_by_maturity"] = page.screenshot(type="jpeg", quality=60)

            # ---------------------------------------------------------------- kept, one chart alone, a change line
            r.section("4. Kept in the browser; one chart on its own; a change line explains")
            page.reload(wait_until="load")
            page.wait_for_selector("#macro-body .macro-item", timeout=40000)
            page.wait_for_function("() => { const c = document.querySelector('#macro-body .macro-item[data-symbol=\"FRED:WSHOSHO\"]'); return c && c.dataset.metricState === 'ready' && c.dataset.view }", timeout=90000)
            page.wait_for_timeout(800)
            again = [x for x in page.evaluate(CARDS) if x["s"] == "FRED:WSHOSHO"][0]
            r.log("after loading the page again: switch on '%s', the total is drawn as %s: %s" % (page.evaluate(BAR)["on"], again["view"], again["line"]))
            ck(page.evaluate(BAR)["on"] == "yoy" and again["view"] == "yoy" and again["line"] == by["FRED:WSHOSHO"]["line"], "the choice was not kept across a reload")
            page.click((CARD % "FRED:WSHOTSL") + " .macro-view-btn[data-view='level']")
            page.wait_for_function("() => document.querySelector('#macro-body .macro-item[data-symbol=\"FRED:WSHOTSL\"]').dataset.view === 'level'", timeout=20000)
            page.wait_for_timeout(400)
            two = {x["s"]: x for x in page.evaluate(CARDS) if x["s"] in ("FRED:WSHOTSL", "FRED:WSHOSHO")}
            bar, saved = page.evaluate(BAR), page.evaluate(SAVED)
            r.log("Level pressed on the Treasuries chart alone: Treasuries %s [%s], the total still %s | the switch says: '%s' | saved %s | full screen opened by the press: %s" % (
                two["FRED:WSHOTSL"]["view"], two["FRED:WSHOTSL"]["btn"], two["FRED:WSHOSHO"]["view"], bar["own"], saved, page.evaluate("() => document.getElementById('macro-focus').classList.contains('open')")))
            ck(two["FRED:WSHOTSL"]["view"] == "level" and two["FRED:WSHOTSL"]["btn"] == "level*p yoy" and two["FRED:WSHOSHO"]["view"] == "yoy" and saved["own"] == ["FRED:WSHOTSL=level"] and "1 chart is set on its own" in bar["own"]
               and not page.evaluate("() => document.getElementById('macro-focus').classList.contains('open')"), "a chart's own button did not move that chart alone")
            page.click((CARD % "FRED:WSHOTSL") + " .macro-view-btn[data-view='yoy']")
            page.wait_for_function("() => document.querySelector('#macro-body .macro-item[data-symbol=\"FRED:WSHOTSL\"]').dataset.view === 'yoy'", timeout=20000)
            saved = page.evaluate(SAVED)
            ck(not saved["own"] and page.evaluate(BAR)["own"] == "", "back on the desk's setting the chart still counts as set on its own: %s" % saved)
            page.evaluate("(s) => document.querySelector('#macro-body .macro-item[data-symbol=\"' + s + '\"]').scrollIntoView({ block: 'center' })", "FRED:RESPPALGXCH52NWW")
            page.wait_for_function("() => document.querySelector('#macro-body .macro-item[data-symbol=\"FRED:RESPPALGXCH52NWW\"]').dataset.metricState === 'ready'", timeout=60000)
            page.wait_for_timeout(500)
            page.click((CARD % "FRED:RESPPALGXCH52NWW") + " .macro-view-btn[data-view='yoy']", force=True)     # marked aria-disabled: a person can still press it, and is told why
            page.wait_for_timeout(400)
            toast = page.evaluate("() => { const t = document.getElementById('jh-toast'); return t && t.style.display !== 'none' ? t.textContent : '' }")
            chg = [x for x in page.evaluate(CARDS) if x["s"] == "FRED:RESPPALGXCH52NWW"][0]
            r.log("YoY %% pressed on the Fed's own change-from-a-year-ago line: stays %s, the page says '%s'; under the chart: '%s'" % (chg["view"], toast, chg["line"]))
            ck(chg["view"] == "level" and "itself a change" in toast and chg["line"] == WHY["change"] and not page.evaluate(SAVED)["own"], "a change line did not explain itself: %s | %s" % (toast, chg["line"]))
            shots["4_a_change_line"] = page.screenshot(type="jpeg", quality=60)

            # ---------------------------------------------------------------- full screen
            r.section("5. Full screen: the same two buttons, the same figure")
            page.evaluate("(s) => document.querySelector('#macro-body .macro-item[data-symbol=\"' + s + '\"]').scrollIntoView({ block: 'center' })", "FRED:WSHOSHO")
            page.wait_for_timeout(400)
            page.click((CARD % "FRED:WSHOSHO") + " .mi-title")
            page.wait_for_selector("#macro-focus.open .macro-hist-chart.big", timeout=30000)
            page.wait_for_timeout(900)
            foc, w = page.evaluate(FOCUS), mine["FRED:WSHOSHO"]
            r.log("the total, full screen: view %s [%s] | %s | on the chart: %s | axis %s | %s | %d figures in %d run(s) | events %s | chart %d px tall" % (
                foc["view"], foc["btn"], foc["line"], foc["now"], foc["axis"], foc["cut"], foc["n"] or 0, foc["lines"], foc["events"], foc["box"]))
            ck(foc["open"] and foc["view"] == "yoy" and foc["btn"] == "level yoy*" and foc["line"] == by["FRED:WSHOSHO"]["line"] and near(foc["now"].replace("now ", ""), w["last"][1]) and foc["n"] == w["n"] and foc["zero"] == 1
               and foc["lines"] == w["lines"] and foc["legend"].startswith("percent change from a year ago") and len(foc["events"]) == 4 and foc["rows"] == 4 and len(foc["changes"]) == 5 and foc["box"] >= 240 and foc["w"] >= foc["vw"] - 2,
               "the full-screen view in YoY %% is not right: %s" % json.dumps(foc, ensure_ascii=False)[:500])
            ck(bool(foc["cut"]) == ("hi" in w["cut"]), "the big chart's off-the-scale label does not follow the scale: %s vs '%s'" % (foc["cut"], w["cut"]))
            shots["5_full_screen_yoy"] = page.screenshot(type="jpeg", quality=60)
            page.click("#macro-focus .macro-view-btn[data-view='level']")
            page.wait_for_function("() => document.getElementById('macro-focus-chartbox').dataset.view === 'level'", timeout=20000)
            page.wait_for_timeout(500)
            lvl = page.evaluate(FOCUS)
            behind = [x for x in page.evaluate(CARDS) if x["s"] == "FRED:WSHOSHO"][0]
            r.log("Level pressed there: view %s [%s] | %s | on the chart: %s | the card behind it: %s" % (lvl["view"], lvl["btn"], lvl["line"], lvl["now"], behind["view"]))
            ck(lvl["view"] == "level" and lvl["btn"] == "level* yoy" and lvl["zero"] == 0 and lvl["lines"] == 1 and "%" not in lvl["now"] and behind["view"] == "level" and lvl["legend"].startswith("dotted grey"), "Level in the full-screen view: %s" % json.dumps(lvl, ensure_ascii=False)[:300])
            page.click("#macro-focus .macro-view-btn[data-view='yoy']")
            page.wait_for_function("() => document.getElementById('macro-focus-chartbox').dataset.view === 'yoy'", timeout=20000)
            page.keyboard.press("Escape")
            page.wait_for_timeout(400)
            ck(not page.evaluate("() => document.getElementById('macro-focus').classList.contains('open')") and not page.evaluate(SAVED)["own"], "Esc did not close the full-screen view, or a choice was left behind")
            # a line whose scale stops short, and a change line
            page.evaluate("(s) => document.querySelector('#macro-body .macro-item[data-symbol=\"' + s + '\"]').scrollIntoView({ block: 'center' })", "FRED:WSHOMCB")
            page.wait_for_timeout(300)
            page.click((CARD % "FRED:WSHOMCB") + " .mi-title")
            page.wait_for_selector("#macro-focus.open .macro-hist-chart.big", timeout=30000)
            page.wait_for_timeout(700)
            mbs, wm = page.evaluate(FOCUS), mine["FRED:WSHOMCB"]
            r.log("mortgage-backed securities, full screen: %s | %s | on the chart: %s" % (mbs["line"], mbs["cut"], mbs["now"]))
            hm = re.search(r"high ([+−]?[\d,.]+)% in (\d{4}-\d{2})", " ".join(mbs["cut"]))
            ck(mbs["view"] == "yoy" and ("hi" in wm["cut"]) == bool(hm) and (not hm or (near(hm.group(1), wm["max"][1]) and hm.group(2) == wm["max"][0][:7])) and near(mbs["now"].replace("now ", ""), wm["last"][1]), "the mortgage-backed chart full screen: %s" % json.dumps(mbs, ensure_ascii=False)[:300])
            shots["6_full_screen_mbs"] = page.screenshot(type="jpeg", quality=60)
            page.keyboard.press("Escape")
            page.evaluate("(s) => { const c = document.querySelector('#macro-body .macro-item[data-symbol=\"' + s + '\"]'); c.scrollIntoView({ block: 'center' }); }", "FRED:RESPPALGXCH52NWW")
            page.wait_for_timeout(300)
            page.click((CARD % "FRED:RESPPALGXCH52NWW") + " .mi-title")
            page.wait_for_selector("#macro-focus.open .macro-hist-chart.big", timeout=30000)
            page.wait_for_timeout(600)
            cf = page.evaluate(FOCUS)
            r.log("a change line, full screen: view %s [%s] | %s" % (cf["view"], cf["btn"], cf["line"]))
            ck(cf["view"] == "level" and cf["btn"] == "level* yoy-" and cf["line"] == WHY["change"] and cf["zero"] == 0, "a change line full screen: %s" % json.dumps(cf, ensure_ascii=False)[:300])
            page.keyboard.press("Escape")

            # ---------------------------------------------------------------- back to Level
            r.section("6. Back to Level: the desk as it was")
            page.evaluate("() => { document.getElementById('macro-body').scrollTop = 0 }")
            page.wait_for_timeout(300)
            page.click("#macro-viewbar .macro-view-btn[data-allview='level']")
            settle(page, "level")
            hydrate(page)
            rows = page.evaluate(CARDS)
            ready = [x for x in rows if x["state"] == "ready"]
            saved = page.evaluate(SAVED)
            same = {s: page.evaluate(STRIP, s) == level_before[s] for s in SAMPLE}
            r.log("Level pressed on the switch: %d of %d cards as Level, %d with a zero line; saved %s; %d sampled strips are the very picture they were before: %s; page errors %s" % (
                sum(1 for x in ready if x["view"] == "level"), len(ready), sum(1 for x in ready if x["zero"]), saved, sum(same.values()), same, errors[:2]))
            ck(len(ready) == 222 and all(x["view"] == "level" and x["zero"] == 0 and x["hidden"] is True and x["n"] is None for x in ready), "not every card is back as Level")
            ck(saved["chartView"] is None and not saved["own"], "Level left something saved: %s" % saved)
            ck(all(same.values()) and all(level_before.values()), "a Level strip is not the picture it was before the visit to YoY %%: %s" % same)
            ck(all(x["y"] == y_before.get(x["s"]) for x in ready), "a Y box changed")
            ck(not errors, "page errors: %s" % errors[:3])
            ctx.close()

            # ---------------------------------------------------------------- a phone
            r.section("7. On a phone")
            ctx, page, errors = fresh(390, 844)
            opened(page)
            page.wait_for_timeout(800)
            page.click("#macro-viewbar .macro-view-btn[data-allview='yoy']")
            page.wait_for_function("() => { const c = document.querySelector('#macro-body .macro-item[data-symbol=\"FRED:WSHOSHO\"]'); return c && c.dataset.view === 'yoy' }", timeout=60000)
            page.wait_for_timeout(700)
            ph = page.evaluate("""() => { const c = document.querySelector('#macro-body .macro-item[data-symbol="FRED:WSHOSHO"]'), bs = [...c.querySelectorAll('.macro-hist-head .macro-view-btn')].map(b => b.getBoundingClientRect()), bar = [...document.querySelectorAll('#macro-viewbar .macro-view-btn')].map(b => b.getBoundingClientRect());
              return { ox: document.documentElement.scrollWidth - document.documentElement.clientWidth, cards: document.querySelectorAll('#macro-body .macro-item').length, wide: [...document.querySelectorAll('#macro-body .macro-item, #macro-viewbar')].filter(e => e.getBoundingClientRect().right > innerWidth + 1).length,
                card: bs.map(q => [Math.round(q.width), Math.round(q.height)]), bar: bar.map(q => [Math.round(q.width), Math.round(q.height)]), inside: bs.concat(bar).every(q => q.left >= 0 && q.right <= innerWidth + 1), line: c.querySelector('.macro-hist-yoy').textContent, view: c.dataset.view } }""")
            r.log("390 px, YoY %% on: %s" % json.dumps(ph, ensure_ascii=False))
            ck(ph["ox"] <= 0 and ph["wide"] == 0 and ph["cards"] == 222 and ph["view"] == "yoy" and ph["inside"] and all(b[1] >= 24 for b in ph["card"]) and all(b[1] >= 28 for b in ph["bar"]) and ph["line"] == by["FRED:WSHOSHO"]["line"], "on a phone: %s" % ph)
            shots["7_phone"] = page.screenshot(type="jpeg", quality=60)
            page.click((CARD % "FRED:WSHOSHO") + " .mi-title")
            page.wait_for_selector("#macro-focus.open .macro-hist-chart.big", timeout=30000)
            page.wait_for_timeout(700)
            pf = page.evaluate(FOCUS)
            r.log("full screen on the phone: view %s [%s] | %s | chart %d px tall, as wide as the screen %s" % (pf["view"], pf["btn"], pf["line"], pf["box"], pf["w"] >= pf["vw"] - 2))
            ck(pf["view"] == "yoy" and pf["btn"] == "level yoy*" and pf["w"] >= pf["vw"] - 2 and pf["box"] >= 200 and not errors and page.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth") <= 0, "full screen on a phone: %s %s" % (pf, errors[:2]))
            ctx.close()

            # ---------------------------------------------------------------- from the chart, and the other desks
            r.section("8. From the chart's Fed Outright button; the other desks have no such buttons")
            ctx, page, errors = fresh(1440)
            try:
                page.goto("%s/chart.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
                page.wait_for_selector("#btn-soma", state="attached", timeout=60000)
                page.wait_for_timeout(2500)
                page.evaluate("() => document.getElementById('btn-soma').click()")       # the toolbar hides above the viewport at this width; the handler is what is under test
                frame = None
                for _ in range(60):
                    frame = next((fr for fr in page.frames if PAGE in fr.url), None)
                    if frame:
                        break
                    page.wait_for_timeout(500)
                if ck(frame is not None, "the Fed Outright button did not open the desk in the chart's overlay"):
                    frame.wait_for_selector("#macro-viewbar", timeout=40000)
                    frame.wait_for_function("() => { const c = document.querySelector('#macro-body .macro-item[data-symbol=\"FRED:WSHOSHO\"]'); return c && c.dataset.metricState === 'ready' && c.dataset.view }", timeout=90000)
                    before = frame.evaluate("() => document.querySelector('#macro-body .macro-item[data-symbol=\"FRED:WSHOSHO\"]').dataset.view")
                    frame.click("#macro-viewbar .macro-view-btn[data-allview='yoy']")
                    frame.wait_for_function("() => document.querySelector('#macro-body .macro-item[data-symbol=\"FRED:WSHOSHO\"]').dataset.view === 'yoy'", timeout=30000)
                    page.wait_for_timeout(600)
                    line = frame.evaluate("() => document.querySelector('#macro-body .macro-item[data-symbol=\"FRED:WSHOSHO\"] .macro-hist-yoy').textContent")
                    r.log("inside the chart: the desk opened with the total as %s; the switch clicked there: %s" % (before, line))
                    ck(before == "level" and line == by["FRED:WSHOSHO"]["line"], "inside the chart's overlay: %s | %s" % (before, line))
                    shots["8_inside_the_chart"] = page.screenshot(type="jpeg", quality=58)
                    frame.click("#macro-viewbar .macro-view-btn[data-allview='level']")
                    page.wait_for_timeout(500)
                ck(not errors, "chart.html page errors: %s" % errors[:3])
            except Exception as e:  # noqa: BLE001
                ck(False, "chart.html: %s" % str(e)[:200])
            ctx.close()
            for fname, desk in (("dollar-indexes.html", "dollar"), ("ice-bofa-indices.html", "bofa"), ("macro-economic-data.html", "macro")):
                ctx, page, errors = fresh(1440)
                page.goto("%s/%s?nocache=%d" % (BASE, fname, stamp), wait_until="load")
                page.wait_for_selector("#macro-body .macro-item", timeout=40000)
                page.wait_for_function("() => document.querySelectorAll('#macro-body .macro-item[data-metric-state=ready] .macro-hist-line').length >= 3", timeout=90000)
                page.wait_for_timeout(800)
                od = page.evaluate("""() => ({ desk: MacroData.desk, offers: MacroData.yoyDesk(), bar: !!document.getElementById('macro-viewbar'), buttons: document.querySelectorAll('.macro-view-btn').length, cards: document.querySelectorAll('#macro-body .macro-item').length,
                    strips: document.querySelectorAll('#macro-body .macro-item[data-metric-state=ready] .macro-hist-svg svg').length, zero: document.querySelectorAll('.macro-hist-zero').length,
                    head: document.querySelector('#macro-body .macro-hist-head').innerHTML, first: document.getElementById('macro-body').firstElementChild.className })""")
                page.locator("#macro-body .macro-item[data-metric-state=ready]").first.click()
                page.wait_for_selector("#macro-focus.open .macro-hist-chart.big", timeout=30000)
                page.wait_for_timeout(500)
                of = page.evaluate(FOCUS)
                r.log("%s: desk %s | offers year over year %s | switch %s | buttons %d | %d cards | strip head '%s' | full screen: [%s] %d line, %d events" % (fname, od["desk"], od["offers"], od["bar"], od["buttons"], od["cards"], re.sub(r"<[^>]+>", "", od["head"]), of["btn"], of["lines"], len(of["events"])))
                ck(od["desk"] == desk and not od["offers"] and not od["bar"] and od["buttons"] == 0 and od["zero"] == 0 and od["head"].startswith("<span>1996 → today</span>") and od["strips"] >= 3 and of["btn"] == "" and of["lines"] == 1 and of["view"] is None and len(of["events"]) == 4 and not errors,
                   "%s changed: %s %s %s" % (fname, {k: v for k, v in od.items() if k != "head"}, of["btn"], errors[:2]))
                ctx.close()
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            r.log("%s: %d bytes" % (k, len(img)))
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(cards=222, drawn_year_over_year=len(drawn), left_as_they_are=len(mine) - len(drawn), scale_stops_short=cut_n, failed_checks=len(fails))
        if fails:
            for f in fails:
                r.fail(f)
            raise SystemExit(1)
        r.ok("LIVE: the held outright desk opens as Level; the switch and every chart's own button draw percent change from a year ago; %d charts equal this op's own figures, %d say why they stay as they are; full screen, a phone and the chart's overlay hold; the other desks are as they were"
             % (len(drawn), len(mine) - len(drawn)))


if __name__ == "__main__":
    main()
    sys.exit(0)
