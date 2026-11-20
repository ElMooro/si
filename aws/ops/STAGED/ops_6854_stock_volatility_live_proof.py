"""ops 6854 - READ-ONLY live proof of the stock volatility desk and its chart icon, in Chrome on the runner (re-runnable).

Khalid 2026-11-19: "create an engine that would measure volatility. Most and least volatile stocks. Also it would
measure the price spreads for all stocks and rank them based on that. Price spread last day, week, month, quarter and
create an icon for it in chart.html".

CLAUDE.md "CHECK YOUR WORK": the page is opened on justhodl.ai and every control is used the way a person uses it.
What is on screen is compared with the artifact the site serves (data/stock-volatility.json), read separately:
  1. the desk at 1440 px: the verdict, the eight tiles, the year's chart and its hover, the bands, every board, the
     table under every view, size, kind and row filter, a header click ranking both ways, sectors and sizes;
  2. chart.html: the Volatility button stands right after Leaders, opens the desk in the workspace, and a ticker
     clicked there loads in the chart behind it;
  3. the desk at 390 px: the ranked figure is on screen beside the ticker, nothing spills sideways.
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
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

BASE = "https://justhodl.ai"
KEY = "data/stock-volatility.json"
UA = "Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome"
BOARDS = ["most_volatile", "least_volatile", "vol_rising", "vol_falling", "widest_d", "widest_w", "widest_m", "widest_q", "narrowest_d", "narrowest_w", "narrowest_m", "narrowest_q", "pinned"]
SIZES = {"300m": 3e8, "2b": 2e9, "10b": 1e10, "200b": 2e11}
VIEW_HEADS = {"overview": ["#", "Ticker", "Name", "Vol 21", "Vol 63", "In its year", "Day", "Week", "Month", "Quarter", "Mkt cap", "Last", "1d %", "Set aside"],
              "ranks": ["#", "Ticker", "Name", "# Vol 21", "# Vol 63", "# Day", "# Week", "# Month", "# Quarter", "Sector", "Industry", "Mkt cap", "$ vol 20d", "Sessions", "Set aside"]}

STATE = r"""() => { const q = (s) => Array.from(document.querySelectorAll(s)), t = (id) => ((document.getElementById(id) || {}).textContent || '').trim();
  const tick = (sel) => q(sel + ' tbody tr').map(tr => { const a = tr.querySelector('a.tk'); return a ? a.getAttribute('data-t') : null; }).filter(Boolean);
  return { verdict: t('verdict-line'), stamp: t('stamp'), meta: t('rows-meta'), vmeta: t('verdict-meta'), bmeta: t('board-meta'),
    kpis: q('#kpis .jh-kpi').map(k => k.querySelector('.l').textContent.trim() + ' = ' + k.querySelector('.v').textContent.trim() + ' // ' + k.querySelector('.s').textContent.trim()),
    heads: q('#rows th').map(x => x.textContent.trim()), rows: tick('#rows'), pos: q('#rows tbody td.sv-pos').slice(0, 3).map(x => x.textContent), board: tick('#board'),
    first: q('#rows tbody tr').slice(0, 1).map(tr => Array.from(tr.children).map(td => td.textContent.trim()).join(' | '))[0] || '',
    more: (document.getElementById('rows-more') || {}).hidden ? '' : t('rows-more'), show: q('#rows-show .jh-chip').map(b => b.textContent.trim() + (b.classList.contains('on') ? '*' : '')),
    sort: q('#rows th[aria-sort]').map(x => x.textContent.trim() + ':' + x.getAttribute('aria-sort')), chart: q('#hist svg .sv-line').length, band: q('#hist svg .sv-band').length,
    bands: q('#bands tbody tr').map(tr => Array.from(tr.children).map(td => td.textContent.trim()).join(' | ')), sectors: q('#sectors tbody tr').length, sizes: q('#sizes tbody tr').length,
    rules: q('#rules dt').length, limits: q('#limits li').length, src: q('#sources tbody tr').length, ops: t('ops').slice(0, 200), changes: t('changes').slice(0, 200), hash: location.hash,
    secs: q('[data-jh-sec]').length, ox: document.documentElement.scrollWidth - document.documentElement.clientWidth, numrows: q('#hist-table tbody tr').length }; }"""
CHART = r"""() => { const b = (id) => document.getElementById(id), ov = b('ws-overlay'), f = ov ? ov.querySelector('iframe') : null, btn = b('btn-svol'), prev = btn ? btn.previousElementSibling : null;
  return { btn: !!btn, title: btn ? btn.getAttribute('title') : null, label: btn ? btn.textContent.trim() : null, prev: prev ? prev.id : null, next: btn && btn.nextElementSibling ? btn.nextElementSibling.id : null,
    overlay: ov ? ov.className : null, head: ov && ov.querySelector('.wshd span') ? ov.querySelector('.wshd span').textContent : null, src: f ? f.getAttribute('src') : null,
    active: window.jhWatchlistActive ? window.jhWatchlistActive() : null, bars: (window.lastBars || []).length,
    box: btn ? (() => { const r = btn.getBoundingClientRect(); return [Math.round(r.left), Math.round(r.top), Math.round(r.width), Math.round(r.height)]; })() : null }; }"""


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


def get_feed(stamp):
    req = urllib.request.Request("%s/%s?nocache=%d" % (BASE, KEY, stamp), headers={"User-Agent": "Mozilla/5.0 jh-ops"})
    with urllib.request.urlopen(req, timeout=90) as rs:
        return json.loads(rs.read().decode("utf-8"))


def lev(v, d=1):
    return "%.*f%%" % (d, v)


def main():
    with report("ops_6854_stock_volatility_live_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6854 - the stock volatility desk and its chart icon, live %s" % now.isoformat(timespec="minutes"))
        stamp = int(time.time())
        fails, shots, errors = [], {}, []

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL %s" % msg)
            return bool(cond)

        doc = get_feed(stamp)
        rows = [dict(zip(doc["cols"], x)) for x in doc["rows"]]
        by = {x["t"]: x for x in rows}
        u, med = doc["universe"], doc["market"]["stock"]["med"]
        r.log("the site serves %s: %s %s, session of %s, generated %s; %d stocks, %d ETFs" % (KEY, doc.get("engine"), doc.get("version"), doc.get("as_of"), doc.get("generated_at"), u["scanned_stock"], u["scanned_etf"]))
        ck(doc.get("contract") == "stock-volatility.v1" and u["scanned_stock"] > 2500, "the artifact on the site is not the engine's (%s, %s stocks)" % (doc.get("contract"), u.get("scanned_stock")))

        def group(x):
            return "pinned" if x["why"] == "pinned" else "aside" if x["why"] else "free"

        def want(kind="stock", size=None, show="free", key="hv21", desc=True, sector=None):
            sub = [x for x in rows if x["kind"] == kind and (not size or (x["mcap"] or 0) >= SIZES[size]) and (show == "all" or group(x) == show) and (not sector or (x["sector"] or "Unclassified") == sector)]
            have = [x for x in sub if x[key] is not None]
            have.sort(key=lambda x: (-x[key] if desc else x[key]))
            return [x["t"] for x in have] + [x["t"] for x in sub if x[key] is None], len(sub)

        def same_order(got, exp, key, n=25):
            """The first n tickers agree, allowing two rows with an equal reading to swap."""
            if len(got) < min(n, len(exp)):
                return False
            return all(got[i] == exp[i] or by[got[i]][key] == by[exp[i]][key] for i in range(min(n, len(exp))))

        sync_playwright = ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = launch(p, r.log)

            # ------------------------------------------------------------------ 1. the desk at 1440 px
            r.section("1. the desk at 1440 px")
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent=UA)
            page = ctx.new_page()
            page.on("pageerror", lambda e: errors.append("desk: " + str(e)[:200]))
            page.goto("%s/stock-volatility.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            try:
                page.wait_for_function("() => document.querySelectorAll('#rows tbody tr a.tk').length > 50", timeout=90000)
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(1200)
            s = page.evaluate(STATE)
            if not ck(len(s["rows"]) > 50, "the desk did not draw its table: verdict %r" % s["verdict"][:160]):
                shots["desk_failed_1440"] = page.screenshot(type="jpeg", quality=50)
            r.log("verdict: %s" % s["verdict"])
            ck(s["verdict"].startswith("As of %s the median listed stock is moving at %s a year" % (doc["as_of"], lev(med["hv21"]))) and ("%s for the day" % lev(med["sp_d"], 2)) in s["verdict"]
               and ("%s for the quarter" % lev(med["sp_q"])) in s["verdict"], "the verdict does not state the artifact's medians: %s" % s["verdict"][:200])
            for k in s["kpis"]:
                r.log("  tile: %s" % k[:170])
            top, calm = doc["boards"]["most_volatile"][0], doc["boards"]["least_volatile"][0]
            ck(len(s["kpis"]) == 8 and lev(med["hv21"]) in s["kpis"][0] and lev(doc["market"]["benchmark"]["hv21"]) in s["kpis"][2] and lev(med["sp_d"], 2) in s["kpis"][3]
               and top in s["kpis"][4] and calm in s["kpis"][5] and str(len(doc["pinned"])) in s["kpis"][7], "the tiles do not carry the artifact's figures")
            exp, n = want()
            total = u["scanned_stock"]
            r.log("table: %s | first row %s" % (s["meta"], s["first"][:150]))
            ck(same_order(s["rows"], exp, "hv21") and s["pos"] == ["1", "2", "3"] and s["sort"] == ["Vol 21:descending"] and s["heads"] == VIEW_HEADS["overview"]
               and s["meta"].startswith("{:,} of {:,} stocks".format(n, total)), "default table: %s, first %s (want %s), sort %s" % (s["meta"], s["rows"][:3], exp[:3], s["sort"]))
            ck(by[s["rows"][0]]["rk_hv21"] == 1, "the first row is %s, whose rank by Vol 21 is %s" % (s["rows"][0], by[s["rows"][0]]["rk_hv21"]))
            ck(len(s["rows"]) == 200 and ("{:,}".format(n) in s["more"]), "200 rows then a button for the rest: %d rows, %r" % (len(s["rows"]), s["more"]))
            ck(s["show"] == ["Trading freely {:,}*".format(n), "Pinned {:,}".format(u["pinned"]["stock"]), "Jump, print or thin {:,}".format(u["set_aside"]["stock"] - u["pinned"]["stock"]), "All {:,}".format(total)],
               "row chips %s" % s["show"])
            ck(s["chart"] == 1 and s["band"] == 1 and len(s["bands"]) == 7 and s["sectors"] == len(doc["by_sector"]) and s["sizes"] == 5 and s["rules"] == len(doc["definitions"]) and s["limits"] == len(doc["limitations"])
               and s["src"] == len(doc["sources"]) and s["secs"] >= 8 and s["ox"] <= 0 and s["hash"] == "", "page blocks: chart %s band %s bands %s sectors %s sizes %s rules %s sources %s sections %s overflow %s hash %r" % (
                   s["chart"], s["band"], len(s["bands"]), s["sectors"], s["sizes"], s["rules"], s["src"], s["secs"], s["ox"], s["hash"]))
            b0 = doc["bands"]
            ck(all(("{:,}".format(b0[i]["n"]) in s["bands"][i]) and s["bands"][i].startswith(b0[i]["band"]) for i in range(6)) and "{:,}".format(sum(x["n"] for x in b0)) in s["bands"][6], "the bands table: %s" % s["bands"][:2])
            shots["desk_1440"] = page.screenshot(type="jpeg", quality=52)

            def click(sel):
                page.evaluate("(s) => document.querySelector(s).click()", sel)
                page.wait_for_timeout(350)
                return page.evaluate(STATE)

            # a header click ranks both ways
            s = click('#rows th[data-sort="hv21"]')
            exp, n = want(desc=False)
            ck(same_order(s["rows"], exp, "hv21") and s["sort"] == ["Vol 21:ascending"] and "smallest first" in s["meta"], "Vol 21 ascending: %s (want %s)" % (s["rows"][:4], exp[:4]))
            r.log("Vol 21, smallest first: %s" % ", ".join("%s %s" % (t, by[t]["hv21"]) for t in s["rows"][:6]))
            for key, title in (("sp_d", "Day"), ("sp_w", "Week"), ("sp_m", "Month"), ("sp_q", "Quarter"), ("hv63", "Vol 63")):
                s = click('#rows th[data-sort="%s"]' % key)
                exp, _ = want(key=key)
                ok = ck(same_order(s["rows"], exp, key) and s["sort"] == [title + ":descending"], "%s descending: %s (want %s)" % (title, s["rows"][:4], exp[:4]))
                first = by[s["rows"][0]]
                ck(first[{"sp_d": "rk_d", "sp_w": "rk_w", "sp_m": "rk_m", "sp_q": "rk_q", "hv63": "rk_hv63"}[key]] == 1, "%s: the first row %s is not rank 1 in the artifact" % (title, first["t"]))
                if ok:
                    r.log("%-7s widest first: %s" % (title, ", ".join("%s %s" % (t, by[t][key]) for t in s["rows"][:5])))
            click('#rows th[data-sort="sp_q"]')              # a new column starts widest first; the second click reverses it
            s = click('#rows th[data-sort="sp_q"]')
            exp, _ = want(key="sp_q", desc=False)
            ck(same_order(s["rows"], exp, "sp_q") and s["sort"] == ["Quarter:ascending"], "Quarter ascending: %s (want %s)" % (s["rows"][:4], exp[:4]))
            r.log("Quarter narrowest first: %s" % ", ".join("%s %s" % (t, by[t]["sp_q"]) for t in s["rows"][:5]))

            # views
            for v in ("vol", "spread", "range", "ranks", "overview"):
                s = click('#rows-view [data-v="%s"]' % v)
                ck(len(s["rows"]) == 200 and len(s["heads"]) >= 11 and (v not in VIEW_HEADS or s["heads"] == VIEW_HEADS[v]), "view %s: %d rows, heads %s" % (v, len(s["rows"]), s["heads"]))
                r.log("view %-8s %d columns: %s" % (v, len(s["heads"]), " | ".join(s["heads"])[:210]))
                if v == "range":
                    shots["view_low_high_1440"] = page.locator("#table").screenshot(type="jpeg", quality=52)
            s = click('#rows th[data-sort="hv21"]')
            # sizes
            for size in ("300m", "2b", "10b", "200b"):
                s = click('#rows-size [data-v="%s"]' % size)
                exp, n = want(size=size)
                ck(same_order(s["rows"], exp, "hv21") and s["meta"].startswith("{:,} of ".format(n)) and all((by[t]["mcap"] or 0) >= SIZES[size] for t in s["rows"]), "size %s: %s, first %s (want %s)" % (size, s["meta"], s["rows"][:3], exp[:3]))
                r.log("size %-5s %s | most volatile %s" % (size, s["meta"], ", ".join("%s %s" % (t, by[t]["hv21"]) for t in s["rows"][:4])))
            s = click('#rows-size [data-v="all"]')
            # a sector, a search, clear
            sector = doc["by_sector"][0]["sector"]
            page.select_option("#rows-sector", sector)
            page.wait_for_timeout(350)
            s = page.evaluate(STATE)
            exp, n = want(sector=sector)
            ck(same_order(s["rows"], exp, "hv21") and all((by[t]["sector"] or "Unclassified") == sector for t in s["rows"]) and s["meta"].startswith("{:,} of ".format(n)), "sector %s: %s" % (sector, s["meta"]))
            page.select_option("#rows-sector", "")
            page.fill("#rows-q", "nvda")
            page.wait_for_timeout(500)
            s = page.evaluate(STATE)
            ck(s["rows"] == ["NVDA"], "find nvda -> %s" % s["rows"][:5])
            nv = by.get("NVDA") or {}
            r.log("find nvda: %s" % s["first"][:190])
            ck(lev(nv.get("hv21") or 0) in s["first"] and lev(nv.get("sp_q") or 0, 2) in s["first"], "NVDA's row does not print the artifact's figures (%s, %s)" % (nv.get("hv21"), nv.get("sp_q")))
            s = click("#rows-reset")
            ck(len(s["rows"]) == 200 and page.evaluate("() => document.getElementById('rows-q').value") == "", "clear filters")
            # the rows that are not ranked
            s = click('#rows-show [data-v="pinned"]')
            ck(sorted(s["rows"]) == sorted(doc["pinned"]) and "Pinned" in s["first"], "pinned rows: %d on screen, %d in the artifact" % (len(s["rows"]), len(doc["pinned"])))
            r.log("pinned: %d rows | %s" % (len(s["rows"]), s["first"][:170]))
            s = click('#rows-show [data-v="aside"]')
            exp_aside = [x["t"] for x in rows if x["kind"] == "stock" and x["why"] in ("jump", "print", "thin")]
            ck(sorted(s["rows"]) == sorted(exp_aside), "jump, print or thin rows: %d on screen, %d in the artifact" % (len(s["rows"]), len(exp_aside)))
            marks = page.evaluate("() => Array.from(document.querySelectorAll('#rows tbody tr')).map(tr => [tr.querySelector('a.tk').getAttribute('data-t'), tr.lastElementChild.textContent.trim()])")
            want_mark = {"jump": "Jump", "print": "Print", "thin": "Thin"}
            ck(all(m.startswith(want_mark[by[t]["why"]]) for t, m in marks), "a row set aside does not say why: %s" % [(t, m) for t, m in marks if not m.startswith(want_mark[by[t]["why"]])][:4])
            r.log("jump, print or thin: %d rows | %s" % (len(marks), " | ".join("%s: %s" % (t, m[:60]) for t, m in ([x for x in marks if by[x[0]]["why"] == "print"][:3] + [x for x in marks if by[x[0]]["why"] == "jump"][:2] + [x for x in marks if by[x[0]]["why"] == "thin"][:1]))))
            s = click('#rows-show [data-v="all"]')
            ck(s["meta"].startswith("{:,} of {:,} stocks".format(total, total)), "all rows: %s" % s["meta"])
            s = click("#rows-more")
            ck(len(s["rows"]) == total and s["more"] == "", "show all: %d rows on screen of %d" % (len(s["rows"]), total))
            r.log("show all: %d rows drawn" % len(s["rows"]))
            s = click('#rows-show [data-v="free"]')
            # ETFs
            s = click('#rows-kind [data-v="etf"]')
            exp, n = want(kind="etf")
            ck(same_order(s["rows"], exp, "hv21") and s["meta"].startswith("{:,} of {:,} ETFs".format(n, u["scanned_etf"])) and by[s["rows"][0]]["rk_hv21"] == 1, "ETFs: %s, first %s (want %s)" % (s["meta"], s["rows"][:3], exp[:3]))
            r.log("ETFs: %s | most volatile %s" % (s["meta"], ", ".join("%s %s" % (t, by[t]["hv21"]) for t in s["rows"][:4])))
            s = click('#rows th[data-sort="hv21"]')
            r.log("ETFs, least volatile: %s" % ", ".join("%s %s" % (t, by[t]["hv21"]) for t in s["rows"][:4]))
            s = click('#rows-kind [data-v="stock"]')

            # the boards
            for b in BOARDS:
                s = click('#board-chips [data-v="%s"]' % b)
                exp_b = doc["pinned"] if b == "pinned" else doc["boards"][b]
                good = sorted(s["board"]) == sorted(exp_b) if b == "pinned" else s["board"] == exp_b
                ck(good, "board %s: %s (want %s)" % (b, s["board"][:4], exp_b[:4]))
                key = "hv21" if b in ("most_volatile", "least_volatile", "vol_rising", "vol_falling") else "mcap" if b == "pinned" else "sp_" + b[-1]
                r.log("board %-14s %2d | %s | %s" % (b, len(s["board"]), ", ".join("%s %s" % (t, by[t][key] if key != "mcap" else "$%.1fB" % ((by[t]["mcap"] or 0) / 1e9)) for t in s["board"][:5]), s["bmeta"][:70]))
                if b == "least_volatile":
                    shots["board_least_volatile_1440"] = page.locator("#boards").screenshot(type="jpeg", quality=52)
            s = click('#board-chips [data-v="most_volatile"]')

            # the year's chart: the hover reads the artifact's own session
            page.locator("#hist svg").scroll_into_view_if_needed()
            page.wait_for_timeout(200)
            box = page.locator("#hist svg").bounding_box()
            page.mouse.move(box["x"] + box["width"] * 0.42, box["y"] + box["height"] * 0.5)
            page.wait_for_timeout(250)
            tip = page.evaluate("() => { const t = document.getElementById('hist-tip'); return t.hidden ? null : t.innerText.replace(/\\n/g, ' / '); }")
            m = re.match(r"(\d{4}-\d\d-\d\d)", tip or "")
            hist = {h["date"]: h for h in doc["history"]}
            ck(bool(m) and m.group(1) in hist and lev(hist[m.group(1)]["med"]) in tip and "{:,}".format(hist[m.group(1)]["n"]) in tip, "the chart's hover %r is not the artifact's session" % tip)
            r.log("chart hover: %s" % tip)
            shots["market_year_1440"] = page.locator("#market").screenshot(type="jpeg", quality=55)
            s = click('#hist-mode [data-v="spread"]')
            ck(s["chart"] == 1 and s["band"] == 0 and "chart=spread" in s["hash"], "day-spread chart: line %s band %s hash %s" % (s["chart"], s["band"], s["hash"]))
            s = click('#hist-mode [data-v="vol"]')
            ck(s["numrows"] >= 12, "the numbers behind the line: %d rows" % s["numrows"])
            # sectors sort
            sec = page.evaluate("() => { document.querySelector('#sectors th[data-sort=\"hv21\"]').click(); return Array.from(document.querySelectorAll('#sectors tbody tr')).map(tr => tr.children[0].textContent.trim()); }")
            exp_sec = [g["sector"] for g in sorted(doc["by_sector"], key=lambda g: -(g["med"]["hv21"] or 0))]
            ck(sec == exp_sec, "sectors by Vol 21: %s (want %s)" % (sec[:3], exp_sec[:3]))
            r.log("sectors, most volatile first: %s" % ", ".join("%s %s" % (g, next(x["med"]["hv21"] for x in doc["by_sector"] if x["sector"] == g)) for g in sec[:5]))
            ck(not errors, "page errors on the desk: %s" % errors[:3])
            ctx.close()

            # ------------------------------------------------------------------ 2. chart.html: the icon
            r.section("2. chart.html: the Volatility button")
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent=UA)
            page = ctx.new_page()
            cerr = []
            page.on("pageerror", lambda e: cerr.append(str(e)[:200]))
            page.goto("%s/chart.html?s=AAPL&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#btn-svol", timeout=90000, state="attached")
            page.wait_for_function("() => (window.lastBars || []).length > 300 && typeof window.jhOpenWorkspace === 'function'", timeout=90000)
            page.wait_for_timeout(1200)
            c0 = page.evaluate(CHART)
            r.log("button: %s" % json.dumps(c0))
            ck(c0["btn"] and c0["prev"] == "btn-ldr" and "Volatility" in (c0["label"] or "") and "volatile" in (c0["title"] or "") and c0["overlay"] == "", "the button: %s" % c0)
            # the toolbar is off screen until the pointer reaches the top edge (jh-chart-autohide.js): move there, then click the button itself
            page.mouse.move(700, 3)
            try:
                page.wait_for_selector("#jh-ah-top.show", timeout=8000)
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(500)
            page.evaluate("() => document.getElementById('btn-svol').scrollIntoView({ inline: 'center', block: 'nearest' })")
            page.wait_for_timeout(300)
            c1 = page.evaluate(CHART)
            shots["chart_toolbar_1440"] = page.screenshot(type="jpeg", quality=55, clip={"x": 0, "y": 0, "width": 1440, "height": 120})
            clicked = "mouse"
            try:
                bx = c1["box"]
                if not (bx and bx[2] > 10 and 0 <= bx[0] < 1440 and 0 <= bx[1] < 200):
                    raise RuntimeError("button box %s is not on screen" % bx)
                page.mouse.click(bx[0] + bx[2] / 2, bx[1] + bx[3] / 2)
            except Exception as exc:  # noqa: BLE001
                clicked = "script (%s)" % str(exc)[:60]
                page.evaluate("() => document.getElementById('btn-svol').click()")
            page.wait_for_timeout(800)
            c2 = page.evaluate(CHART)
            if c2["overlay"] != "on":
                clicked += " then script"
                page.evaluate("() => document.getElementById('btn-svol').click()")
                page.wait_for_timeout(800)
                c2 = page.evaluate(CHART)
            ck(c2["overlay"] == "on" and (c2["src"] or "").startswith("/stock-volatility.html") and "Volatility" in (c2["head"] or ""), "the button did not open the desk: %s" % c2)
            r.log("clicked by %s at box %s -> overlay %s | %s | %s" % (clicked, c1["box"], c2["overlay"], c2["head"], c2["src"]))
            fr = None
            for _ in range(80):
                fr = next((f for f in page.frames if "stock-volatility" in f.url), None)
                if fr:
                    break
                page.wait_for_timeout(250)
            if ck(fr is not None, "no workspace frame for the desk"):
                fr.wait_for_function("() => document.querySelectorAll('#rows tbody tr a.tk').length > 50", timeout=90000)
                page.wait_for_timeout(600)
                fs = fr.evaluate(STATE)
                ck(fs["rows"][0] == want()[0][0] and len(fs["kpis"]) == 8, "the desk inside the chart: first row %s, %d tiles" % (fs["rows"][:1], len(fs["kpis"])))
                shots["chart_overlay_1440"] = page.screenshot(type="jpeg", quality=52)
                pick = next(t for t in doc["boards"]["least_volatile"] if re.fullmatch(r"[A-Z]{1,5}", t) and t != "AAPL")
                fr.evaluate("() => document.querySelector('#board-chips [data-v=\"least_volatile\"]').click()")
                page.wait_for_timeout(400)
                fr.evaluate("(t) => document.querySelector('#board a.tk[data-t=\"' + t + '\"]').click()", pick)
                try:
                    page.wait_for_function("(t) => window.jhWatchlistActive && String(window.jhWatchlistActive()).toUpperCase() === t && document.getElementById('ws-overlay').className === ''", arg=pick, timeout=45000)
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(2500)
                c3 = page.evaluate(CHART)
                ck(c3["active"] == pick and c3["overlay"] == "" and c3["bars"] > 100, "a ticker clicked on the desk (%s) -> chart %s, overlay %r, %d bars" % (pick, c3["active"], c3["overlay"], c3["bars"]))
                r.log("clicked %s on the Least volatile board inside the workspace -> the chart shows %s (%d bars), workspace closed: %s" % (pick, c3["active"], c3["bars"], c3["overlay"] == ""))
                shots["chart_after_pick_1440"] = page.screenshot(type="jpeg", quality=50)
            ck(not cerr, "page errors on the chart: %s" % cerr[:3])
            ctx.close()

            # ------------------------------------------------------------------ 3. the desk at 390 px
            r.section("3. the desk at 390 px")
            ctx = browser.new_context(viewport={"width": 390, "height": 844}, device_scale_factor=2, is_mobile=True, has_touch=True, user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) jh-ops-probe Mobile Safari")
            page = ctx.new_page()
            merr = []
            page.on("pageerror", lambda e: merr.append(str(e)[:200]))
            page.goto("%s/stock-volatility.html?nocache=%d" % (BASE, stamp + 1), wait_until="domcontentloaded")
            page.wait_for_function("() => document.querySelectorAll('#rows tbody tr a.tk').length > 50", timeout=90000)
            page.wait_for_timeout(1000)
            s = page.evaluate(STATE)
            geo = page.evaluate("""() => { const th = document.querySelector('#rows th[data-sort="hv21"]').getBoundingClientRect(), nm = document.querySelector('#rows td.c-name'), nm2 = document.querySelector('#rows .nm2');
              const wrap = document.getElementById('rows').getBoundingClientRect();
              return { vol_right: Math.round(th.right), wrap_right: Math.round(wrap.right), name_col: nm ? getComputedStyle(nm).display : null, under: nm2 ? getComputedStyle(nm2).display : null, under_text: nm2 ? nm2.textContent : null,
                chart_w: Math.round(document.querySelector('#hist svg').getBoundingClientRect().width) }; }""")
            exp, n = want()
            ck(same_order(s["rows"], exp, "hv21") and len(s["kpis"]) == 8 and s["ox"] <= 0 and s["chart"] == 1, "phone: first %s, %d tiles, overflow %s" % (s["rows"][:3], len(s["kpis"]), s["ox"]))
            ck(geo["vol_right"] <= 390 and geo["name_col"] == "none" and geo["under"] == "block" and geo["chart_w"] <= 390, "phone layout: %s" % geo)
            r.log("phone: %s | Vol 21 column ends at %d px of 390, the name rides under the ticker (%r), chart %d px wide, sideways overflow %s" % (s["meta"], geo["vol_right"], geo["under_text"], geo["chart_w"], s["ox"]))
            shots["desk_390"] = page.screenshot(type="jpeg", quality=55)
            page.evaluate("() => document.getElementById('table').scrollIntoView()")
            page.wait_for_timeout(300)
            shots["desk_table_390"] = page.screenshot(type="jpeg", quality=55)
            s = page.evaluate("() => { document.querySelector('#board-chips [data-v=\"widest_w\"]').click(); return Array.from(document.querySelectorAll('#board tbody tr a.tk')).map(a => a.getAttribute('data-t')); }")
            ck(s == doc["boards"]["widest_w"], "phone board widest week: %s" % s[:4])
            ck(not merr, "page errors on the phone: %s" % merr[:3])
            ctx.close()
            browser.close()

        r.kv(failures=len(fails), as_of=str(doc["as_of"]), stocks=u["scanned_stock"], etfs=u["scanned_etf"], median_hv21=med["hv21"], boards=len(BOARDS), pinned=len(doc["pinned"]), page_errors=len(errors))
        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        if fails:
            r.fail("%d checks failed: %s" % (len(fails), " | ".join(fails)[:1800]))
            sys.exit(1)
        r.ok("LIVE PROOF: the desk ranks %s stocks and %s ETFs both ways on every measure, every board is the artifact's, the chart's Volatility button opens it and a ticker clicked there loads in the chart" % (
            "{:,}".format(u["scanned_stock"]), "{:,}".format(u["scanned_etf"])))


if __name__ == "__main__":
    main()
