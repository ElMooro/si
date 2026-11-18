"""ops 6835 - the three FRED desks beside 13F on the chart: live proof in headless Chrome (read-only, re-runnable).

Khalid 2026-11-17: "add another icon on chart.html next to 13f and it should have all the dxy and dollar index from
fred and it should also show their historical chart: week, month, quarter, yearly percentage change; and another icon
for all the securities held outright index from Fred and another one for all ice boa indices from fred and both
should have historical charts and week, month, quarter, year percentage change."

Drives https://justhodl.ai:
  1. each desk page at 1440 px (dollar-indexes / securities-held-outright / ice-bofa-indices): every card of the
     catalog is scrolled into view and must finish reading - value, as-of date, a history strip since 1996 with the
     famous bear markets, and week / month / quarter / year percentages;
  2. the figures are checked, not just present: this op reads the same series itself through /series and works the
     four percentages out in Python (calendar months, held to the month end) - every card's printed figure must
     agree; the DXY card must equal the ICE formula on FRED's six rates computed here, and is set beside the market's
     own close;
  3. a line that is itself a change says so instead of printing a percentage of a change; a series that ended before
     1996 says so on its strip; full screen opens with the big chart and the four events;
  4. at 390 px: no sideways scroll, cards read, full screen opens;
  5. chart.html: the three buttons sit between CDS and 13F, and each one - clicked - opens its desk in the chart's
     overlay with cards that read;
  6. the Macro desk and the Inflows & outflows desk are as they were.
JPEGs are embedded (base64). Nothing on the site changes.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import base64
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
DESKS = [("dollar", "dollar-indexes.html", "💵 Dollar indexes", 95, 8, "btn-usd"),
         ("soma", "securities-held-outright.html", "🏛 Fed securities held outright", 222, 25, "btn-soma"),
         ("bofa", "ice-bofa-indices.html", "📉 ICE BofA indices", 192, 16, "btn-bofa")]
DXY = {"k": 50.14348112, "legs": [("DEXUSEU", -0.576), ("DEXJPUS", 0.136), ("DEXUSUK", -0.119), ("DEXCAUS", 0.091), ("DEXSDUS", 0.042), ("DEXSZUS", 0.036)]}
TOL = {"W": 5, "M": 16, "Q": 45, "Y": 95}
PRE_1996 = {"FRED:RAGSOCID", "FRED:RAGSOSTC", "FRED:RAGSOUSCID1PA", "FRED:RAGSOUSVN", "FRED:RAGSOUSCIDO"}


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


def series(sid):
    for attempt in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(PROXY + "/series?id=" + urllib.parse.quote(sid), headers=UA), timeout=90) as resp:
                doc = json.loads(resp.read())
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


def shift(day, hz):
    d = datetime.strptime(day, "%Y-%m-%d")
    if hz == "W":
        return d - timedelta(days=7)
    y, m = d.year, d.month - {"M": 1, "Q": 3, "Y": 12}[hz]
    while m <= 0:
        y, m = y - 1, m + 12
    dd = d.day
    while True:
        try:
            return datetime(y, m, dd)
        except ValueError:
            dd -= 1


def changes(obs):
    out = {}
    last_day, last = obs[-1]
    for hz in ("W", "M", "Q", "Y"):
        cut = shift(last_day, hz)
        cut_s = cut.strftime("%Y-%m-%d")
        prior = next((o for o in reversed(obs) if o[0] <= cut_s), None)
        ok = prior and (cut - datetime.strptime(prior[0], "%Y-%m-%d")).days <= TOL[hz] and prior[1] != 0
        out[hz] = (last - prior[1]) / abs(prior[1]) * 100 if ok else None
    return out


def agrees(shown, want):
    if want is None:
        return shown in ("—", "", None)
    try:
        got = float(str(shown).replace("%", "").replace("+", ""))
    except ValueError:
        return False
    return abs(got - want) <= (0.51 if abs(want) >= 100 else 0.0051)


CARDS = """() => { const all = [...document.querySelectorAll('#macro-body .macro-item')]; const st = {}; all.forEach(c => { st[c.dataset.metricState || 'none'] = (st[c.dataset.metricState || 'none'] || 0) + 1 });
  const row = c => { const ch = {}; c.querySelectorAll('.macro-change[data-horizon]').forEach(n => { ch[n.dataset.horizon] = n.textContent.replace(/^[DWMQY]/, '') });
    return { s: c.dataset.symbol, state: c.dataset.metricState, v: c.querySelector('.macro-latest-value').textContent, asof: c.querySelector('.macro-asof').textContent, ch,
      note: !!c.querySelector('.macro-change-note'), strip: c.querySelectorAll('.macro-hist-svg svg').length, line: c.querySelectorAll('.macro-hist-line').length, bears: c.querySelectorAll('.macro-hist-bear').length,
      empty: (c.querySelector('.macro-hist-empty') || {}).textContent || '', bad: /undefined|NaN|\\[object|Infinity/.test(c.innerText), title: c.querySelector('.mi-title').textContent,
      err: c.dataset.metricState === 'error' ? (c.title || c.querySelector('.macro-latest-value').title || '') : '' } };
  const cat = MacroData.catalogItems();
  return { st, rows: all.map(row), cats: document.querySelectorAll('#macro-body .macro-cat').length, title: document.getElementById('macro-modal-title').textContent, desk: MacroData.desk, key: MacroData.STORAGE_KEY,
    catalog: cat.reduce((n, c) => n + c.items.length, 0), families: cat.length, tags: cat.flatMap(c => c.items.filter(i => i[3] === 'C').map(i => 'FRED:' + i[0])),
    ox: document.documentElement.scrollWidth - document.documentElement.clientWidth, sheet: !!document.getElementById('flows-tearsheet') } }"""
FOCUS = """() => { const f = document.getElementById('macro-focus'); return { title: document.getElementById('macro-focus-title').textContent, latest: document.getElementById('macro-focus-latest').innerText.replace(/\\s+/g, ' '),
  ch: [...f.querySelectorAll('.macro-focus-changes .macro-change')].map(e => e.textContent), rows: f.querySelectorAll('.macro-focus-table tbody tr').length, labels: [...f.querySelectorAll('.macro-hist-label')].map(e => e.textContent),
  big: f.querySelectorAll('.macro-hist-chart.big .macro-hist-line').length, w: Math.round(f.getBoundingClientRect().width), vw: innerWidth } }"""


def hydrate(page, limit=900):
    page.evaluate("() => { document.getElementById('macro-body').scrollTop = 0 }")
    for _ in range(limit):
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


def main():
    with report("ops_6835_fred_desks_live_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6835 - dollar indexes, Fed securities held outright, ICE BofA indices: the three chart desks live %s" % now.isoformat(timespec="minutes"))
        stamp = int(time.time())
        js = urllib.request.urlopen(urllib.request.Request("%s/assets/jh-workspaces.js?nocache=%d" % (BASE, stamp), headers=UA), timeout=40).read().decode("utf-8", "replace")
        eng = urllib.request.urlopen(urllib.request.Request("%s/jh-chart-engine.js?nocache=%d" % (BASE, stamp), headers=UA), timeout=40).read().decode("utf-8", "replace")
        new = all(k in js for k in ("DOLLAR_CATALOG", "SOMA_CATALOG", "BOFA_CATALOG", "fetchCalc", "monthsBack")) and "btn-usd" in eng
        r.log("served bundle %d bytes, chart engine %d bytes: the new build is live %s" % (len(js), len(eng), new))
        if not new:
            r.fail("the served site is not the new build yet (Pages deploy pending?)")
            raise SystemExit(1)
        sync_playwright = ensure_playwright(r.log)
        fails, shots = [], {}

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL: " + msg)

        with sync_playwright() as p:
            browser = launch(p, r.log)

            def fresh(width):
                ctx = browser.new_context(viewport={"width": width, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
                page = ctx.new_page()
                errors = []
                page.on("pageerror", lambda e: errors.append("pageerror: %s" % e))
                page.add_init_script("window.open = () => null;")
                return ctx, page, errors

            totals = {}
            for desk, fname, heading, n_series, n_fam, _btn in DESKS:
                r.section("%s - /%s at 1440 px" % (heading, fname))
                ctx, page, errors = fresh(1440)
                t0 = time.time()
                page.goto("%s/%s?nocache=%d" % (BASE, fname, stamp), wait_until="load")
                page.wait_for_selector("#macro-body .macro-item", timeout=40000)
                page.wait_for_function("() => document.querySelectorAll('#macro-body .macro-item[data-metric-state=ready]').length >= 3", timeout=90000)
                if desk == "dollar":
                    page.wait_for_function("() => { const c = document.querySelector('#macro-body .macro-item[data-symbol=\"calc:dxy\"]'); return c && c.dataset.metricState === 'ready' && c.querySelector('.macro-hist-line') }", timeout=90000)
                    page.wait_for_timeout(1500)
                    shots["dollar_1440"] = page.screenshot(type="jpeg", quality=58)
                hydrate(page)
                s = page.evaluate(CARDS)
                took = time.time() - t0
                rows = {x["s"]: x for x in s["rows"]}
                ready = [x for x in s["rows"] if x["state"] == "ready"]
                notready = [x for x in s["rows"] if x["state"] != "ready"]
                r.log("%s | desk %s | %d families, %d cards (catalog %d / %d) | states %s | read in %.0f s | sideways overflow %s | page errors %d" % (s["title"], s["desk"], s["cats"], len(s["rows"]), s["families"], s["catalog"], json.dumps(s["st"]), took, s["ox"], len(errors)))
                ck(s["title"] == heading and s["desk"] == desk and s["key"] == "jh_%s_workspace_v1" % desk and not s["sheet"], "%s: wrong desk %s %s %s" % (desk, s["title"], s["desk"], s["key"]))
                ck(len(s["rows"]) == n_series == s["catalog"] and s["cats"] == n_fam == s["families"], "%s: %d cards / %d families, wanted %d / %d" % (desk, len(s["rows"]), s["cats"], n_series, n_fam))
                ck(not notready, "%s: %d cards did not read: %s" % (desk, len(notready), ", ".join("%s (%s)" % (x["s"], x["state"]) for x in notready[:14])))
                ck(not errors, "%s: page errors %s" % (desk, errors[:3]))
                ck(not any(x["bad"] for x in ready), "%s: broken text on %s" % (desk, [x["s"] for x in ready if x["bad"]][:6]))
                tags = set(s["tags"])
                noted = [x for x in ready if x["note"]]
                ck({x["s"] for x in noted} == (tags & {x["s"] for x in ready}), "%s: the change note is on %d cards, the catalog marks %d" % (desk, len(noted), len(tags)))
                # strips
                no_strip = [x["s"] for x in ready if not (x["strip"] == 1 and (x["line"] == 1 or x["empty"]))]
                ck(not no_strip, "%s: no history strip on %s" % (desk, no_strip[:8]))
                ended_txt = {x["s"]: x["empty"] for x in ready if x["empty"]}
                with_bears = sum(1 for x in ready if x["bears"] >= 3)
                r.log("history strips drawn on %d of %d cards; %d carry three or more bear-market bands; strips that say why they are empty: %s" % (len(ready) - len(no_strip), len(ready), with_bears, json.dumps(ended_txt)[:400]))
                if desk == "soma":
                    ck(set(ended_txt) == PRE_1996 and all(v.startswith("ended ") for v in ended_txt.values()), "soma: the series that ended before 1996 should say so: %s" % ended_txt)
                    ck(len(noted) == 60, "soma: %d change lines carry the note, wanted 60" % len(noted))
                else:
                    ck(not ended_txt and not noted, "%s: unexpected empty strips or change notes %s %d" % (desk, list(ended_txt)[:4], len(noted)))
                # the figures against this op's own arithmetic on the same series
                ids = [x["s"] for x in ready if x["s"].startswith("FRED:") and x["s"] not in tags]
                with ThreadPoolExecutor(max_workers=4) as pool:
                    mine = dict(zip(ids, pool.map(series, ids)))
                wrong, cells, unread = [], 0, []
                for sid in ids:
                    obs = mine.get(sid)
                    if not obs:
                        unread.append(sid)
                        continue
                    card, want = rows[sid], changes(obs)
                    if card["asof"] != obs[-1][0]:
                        wrong.append("%s as of %s vs %s" % (sid, card["asof"], obs[-1][0]))
                        continue
                    for hz in ("W", "M", "Q", "Y"):
                        cells += 1
                        if not agrees(card["ch"].get(hz), want[hz]):
                            wrong.append("%s %s page %s here %s" % (sid, hz, card["ch"].get(hz), None if want[hz] is None else round(want[hz], 3)))
                r.log("week / month / quarter / year: %d cells on %d cards checked against this op's own arithmetic, %d differ; %d series this op could not read itself" % (cells, len(ids) - len(unread), len(wrong), len(unread)))
                ck(not wrong, "%s: figures differ: %s" % (desk, "; ".join(wrong[:8])))
                ck(len(unread) <= 3, "%s: this op could not read %d series itself: %s" % (desk, len(unread), unread[:6]))
                pct_cells = sum(1 for x in ready if x["s"] not in tags for hz in ("W", "M", "Q", "Y") if x["ch"].get(hz, "—") != "—")
                r.log("cards showing a percentage: %d cells filled of %d" % (pct_cells, 4 * len([x for x in ready if x["s"] not in tags])))
                for x in ready[:3]:
                    r.log("  %-24s %-10s %s  W %s  M %s  Q %s  Y %s | %s" % (x["s"], x["v"], x["asof"], x["ch"].get("W"), x["ch"].get("M"), x["ch"].get("Q"), x["ch"].get("Y"), x["title"][:60]))
                if desk == "dollar":
                    legs = {sid: dict(mine.get("FRED:" + sid) or []) for sid, _ in DXY["legs"]}
                    days = sorted(set.intersection(*[set(v) for v in legs.values()])) if all(legs.values()) else []
                    calc = []
                    for d in days:
                        val = DXY["k"]
                        for sid, w in DXY["legs"]:
                            val *= legs[sid][d] ** w
                        calc.append((d, val))
                    card = rows.get("calc:dxy") or {}
                    if calc and card:
                        want = changes(calc)
                        r.log("DXY rebuilt from FRED: the page prints %s as of %s (W %s M %s Q %s Y %s); this op's own figure %.3f on %s over %d days since %s" % (card["v"], card["asof"], card["ch"].get("W"), card["ch"].get("M"), card["ch"].get("Q"), card["ch"].get("Y"), calc[-1][1], calc[-1][0], len(calc), calc[0][0]))
                        ck(card["asof"] == calc[-1][0] and abs(float(card["v"].replace(",", "")) - calc[-1][1]) < 0.006 and all(agrees(card["ch"].get(hz), want[hz]) for hz in ("W", "M", "Q", "Y")), "the DXY card is not the ICE formula on FRED's six rates")
                        try:
                            with urllib.request.urlopen(urllib.request.Request("https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?range=1mo&interval=1d", headers=UA), timeout=40) as resp:
                                res = json.loads(resp.read())["chart"]["result"][0]
                            mkt = {datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d"): c for t, c in zip(res["timestamp"], res["indicators"]["quote"][0]["close"]) if c}
                            both = [(d, v, mkt[d]) for d, v in calc if d in mkt]
                            if both:
                                r.log("beside the market's own DXY close: %s rebuilt %.3f, market %.3f (%.3f%% apart); the market's latest close is %.3f on %s - FRED's rates run to %s" % (both[-1][0], both[-1][1], both[-1][2], abs(both[-1][1] / both[-1][2] - 1) * 100, mkt[max(mkt)], max(mkt), calc[-1][0]))
                                ck(abs(both[-1][1] / both[-1][2] - 1) < 0.01, "the rebuilt DXY is more than 1% from the market's close")
                        except Exception as exc:  # noqa: BLE001
                            r.log("market DXY unavailable to this op: %s" % str(exc)[:100])
                    else:
                        ck(False, "the DXY card or its six rates did not read")
                # full screen
                page.evaluate("() => { document.getElementById('macro-body').scrollTop = 0 }")
                page.locator("#macro-body .macro-item").first.click()
                page.wait_for_selector("#macro-focus.open .macro-hist-chart.big", timeout=30000)
                page.wait_for_timeout(800)
                foc = page.evaluate(FOCUS)
                r.log("full screen: %s | %s | %s | %d events %s | fills the window %s" % (foc["title"][:70], foc["latest"], " ".join(foc["ch"]), foc["rows"], foc["labels"], foc["w"] >= foc["vw"] - 2))
                ck(len(foc["ch"]) == 5 and foc["rows"] == 4 and len(foc["labels"]) == 4 and foc["big"] == 1 and foc["w"] >= foc["vw"] - 2, "%s: full screen %s" % (desk, foc))
                if desk == "bofa":
                    shots["bofa_focus_1440"] = page.screenshot(type="jpeg", quality=58)
                page.keyboard.press("Escape")
                page.wait_for_timeout(300)
                ck(not page.evaluate("() => document.getElementById('macro-focus').classList.contains('open')"), "%s: Esc did not close the full-screen view" % desk)
                if desk == "soma":
                    page.evaluate("() => { const c = [...document.querySelectorAll('#macro-body .macro-item')].find(c => c.dataset.symbol === 'FRED:RESPPALGUOXCH1NWW'); c.scrollIntoView({ block: 'center' }); c.click(); }")
                    page.wait_for_selector("#macro-focus.open .macro-hist-chart.big", timeout=30000)
                    note = page.evaluate("() => [...document.querySelectorAll('#macro-focus .macro-focus-changes .macro-change')].map(e => e.textContent)")
                    r.log("a change line full screen (Treasuries, weekly change): %s | %s" % (page.evaluate("() => document.getElementById('macro-focus-latest').innerText.replace(/\\s+/g, ' ')"), note))
                    ck(len(note) == 1 and "itself a change" in note[0], "soma: the change line's full screen prints percentages: %s" % note)
                    page.keyboard.press("Escape")
                totals[desk] = len(ready)
                ctx.close()

                # the phone
                ctx, page, errors = fresh(390)
                page.goto("%s/%s?nocache=%d" % (BASE, fname, stamp), wait_until="load")
                page.wait_for_selector("#macro-body .macro-item", timeout=40000)
                page.wait_for_function("() => document.querySelectorAll('#macro-body .macro-item[data-metric-state=ready] .macro-hist-line').length >= 2", timeout=90000)
                page.wait_for_timeout(1200)
                ph = page.evaluate("() => ({ ox: document.documentElement.scrollWidth - document.documentElement.clientWidth, cards: document.querySelectorAll('#macro-body .macro-item').length, ready: document.querySelectorAll('#macro-body .macro-item[data-metric-state=ready]').length, wide: [...document.querySelectorAll('#macro-body .macro-item')].filter(c => c.getBoundingClientRect().width > innerWidth).length })")
                if desk == "soma":
                    shots["soma_390"] = page.screenshot(type="jpeg", quality=58)
                page.locator("#macro-body .macro-item").first.click()
                page.wait_for_selector("#macro-focus.open .macro-hist-chart.big", timeout=30000)
                pf = page.evaluate(FOCUS)
                r.log("390 px: %s | full screen %s, %d events" % (json.dumps(ph), pf["title"][:50], pf["rows"]))
                ck(ph["ox"] <= 0 and ph["cards"] == n_series and ph["ready"] >= 2 and ph["wide"] == 0 and pf["rows"] == 4 and not errors, "%s on a phone: %s %s %s" % (desk, ph, pf["rows"], errors[:2]))
                ctx.close()

            r.section("chart.html - the three buttons beside 13F")
            ctx, page, errors = fresh(1440)
            try:
                page.goto("%s/chart.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
                page.wait_for_selector("#btn-13f", timeout=60000)
                page.wait_for_timeout(2500)
                bar = page.evaluate("() => [...document.querySelectorAll('#tfbar .wsdesk')].map(b => ({ id: b.id, label: b.innerText.replace(/\\s+/g, ' ').trim(), title: b.title, shown: b.getBoundingClientRect().width > 0 }))")
                ids = [b["id"] for b in bar]
                at = ids.index("btn-13f") if "btn-13f" in ids else -1
                r.log("desk buttons in order: %s" % " | ".join("%s" % b["label"] for b in bar))
                r.log("around 13F: %s" % json.dumps(bar[max(0, at - 4):at + 2], ensure_ascii=False))
                ck(at >= 4 and ids[at - 3:at + 2] == ["btn-usd", "btn-soma", "btn-bofa", "btn-13f", "btn-ldr"] and ids[at - 4] == "btn-cds", "the three buttons are not between CDS and 13F: %s" % ids)
                ck(all(b["shown"] for b in bar[at - 3:at]), "a new button is not visible at 1440 px")
                for desk, fname, heading, n_series, n_fam, btn in DESKS:
                    page.evaluate("(id) => document.getElementById(id).click()", btn)   # the toolbar sits under the chart's own layers for Playwright's pointer; the handler is what is under test
                    frame = None
                    for _ in range(60):
                        frame = next((fr for fr in page.frames if fname in fr.url), None)
                        if frame:
                            break
                        page.wait_for_timeout(500)
                    if not frame:
                        ck(False, "%s did not open /%s in the chart's overlay" % (btn, fname))
                        continue
                    frame.wait_for_selector("#macro-body .macro-item", timeout=40000)
                    frame.wait_for_function("() => document.querySelectorAll('#macro-body .macro-item[data-metric-state=ready] .macro-hist-line').length >= 3", timeout=90000)
                    page.wait_for_timeout(1200)
                    ov = frame.evaluate("() => ({ title: document.getElementById('macro-modal-title').textContent, cards: document.querySelectorAll('#macro-body .macro-item').length, ready: document.querySelectorAll('#macro-body .macro-item[data-metric-state=ready]').length, strips: document.querySelectorAll('#macro-body .macro-hist-line').length, first: document.querySelector('#macro-body .macro-item').innerText.replace(/\\s+/g, ' ').slice(0, 150) })")
                    head = page.evaluate("() => { const h = document.querySelector('#ws-overlay .wshd'); return h ? h.innerText.replace(/\\s+/g, ' ').trim() : null }")
                    r.log("%s clicked -> overlay \"%s\" -> %s" % (btn, head, json.dumps(ov, ensure_ascii=False)))
                    ck(ov["title"] == heading and ov["cards"] == n_series and ov["ready"] >= 3 and ov["strips"] >= 3, "%s: the overlay desk %s" % (btn, ov))
                    if desk == "bofa":
                        shots["chart_overlay_bofa"] = page.screenshot(type="jpeg", quality=55)
                    page.evaluate("() => { const x = document.getElementById('wsx'); if (x) x.click() }")
                    page.wait_for_timeout(600)
                ck(not errors, "chart.html page errors: %s" % errors[:3])
            except Exception as e:  # noqa: BLE001
                ck(False, "chart.html: %s" % str(e)[:200])
            ctx.close()
            ctx, page, errors = fresh(390)
            try:
                page.goto("%s/chart.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
                page.wait_for_selector("#btn-13f", state="attached", timeout=60000)
                page.wait_for_timeout(2000)
                ph = page.evaluate("() => ['btn-usd', 'btn-soma', 'btn-bofa', 'btn-13f'].map(id => { const b = document.getElementById(id); const q = b && b.getBoundingClientRect(); return { id, there: !!b, w: q ? Math.round(q.width) : 0 } })")
                r.log("chart.html at 390 px: %s" % json.dumps(ph))
                ck(all(b["there"] for b in ph) and len({b["w"] > 0 for b in ph}) == 1, "on a phone the new buttons do not behave like 13F: %s" % ph)
            except Exception as e:  # noqa: BLE001
                ck(False, "chart.html at 390: %s" % str(e)[:200])
            ctx.close()

            r.section("the desks that were already there")
            ctx, page, errors = fresh(1440)
            page.goto("%s/macro-economic-data.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#macro-body .macro-item", timeout=40000)
            page.wait_for_function("() => document.querySelectorAll('#macro-body .macro-hist-svg svg').length >= 3", timeout=60000)
            mc = page.evaluate("() => ({ desk: MacroData.desk, items: document.querySelectorAll('#macro-body .macro-item').length, cats: document.querySelectorAll('#macro-body .macro-cat').length, strips: document.querySelectorAll('#macro-body .macro-hist').length, notes: document.querySelectorAll('.macro-change-note').length, sheet: !!document.getElementById('flows-tearsheet'), title: document.getElementById('macro-modal-title').textContent })")
            r.log("Macro desk: %s" % json.dumps(mc, ensure_ascii=False))
            ck(mc["desk"] == "macro" and mc["items"] >= 177 and mc["strips"] == mc["items"] and mc["notes"] == 0 and not mc["sheet"], "the Macro desk changed: %s" % mc)
            ctx.close()
            ctx, page, errors = fresh(1440)
            page.goto("%s/inflows-outflows.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#flows-tearsheet", timeout=40000)
            page.wait_for_timeout(2500)
            fl = page.evaluate("() => ({ desk: MacroData.desk, items: document.querySelectorAll('#macro-body .macro-item').length, cats: document.querySelectorAll('#macro-body .macro-cat').length, strips: document.querySelectorAll('#macro-body .macro-hist').length, nums: document.querySelectorAll('#macro-body .macro-nums').length, first: document.getElementById('macro-body').firstElementChild.id })")
            r.log("Inflows & outflows desk: %s" % json.dumps(fl))
            ck(fl["desk"] == "flows" and fl["items"] >= 326 and fl["cats"] >= 19 and fl["strips"] == 0 and fl["nums"] == fl["items"] and fl["first"] == "flows-tearsheet", "the flows desk changed: %s" % fl)
            ctx.close()
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            r.log("%s: %d bytes" % (k, len(img)))
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(dollar=str(totals.get("dollar")), held_outright=str(totals.get("soma")), ice_bofa=str(totals.get("bofa")), failures=str(len(fails)))
        if fails:
            for f in fails:
                r.fail(f)
            raise SystemExit(1)
        r.ok("LIVE: the three buttons sit between CDS and 13F and open their desks; %d dollar, %d held-outright and %d ICE BofA cards read with a history strip, and every week / month / quarter / year figure agrees with this op's own arithmetic; 0 failures"
             % (totals.get("dollar", 0), totals.get("soma", 0), totals.get("bofa", 0)))


if __name__ == "__main__":
    main()
    sys.exit(0)
