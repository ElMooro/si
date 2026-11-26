"""ops 6967 - Money flow, live: the documents worked again, the page in a real browser, and the chart's Flow button (read-only).

Khalid 2026-11-25: "Enhance that engine and tracks what etfs and sectors is the money flowing out and flowing into and
covers as many asset classes and etfs as possible. And add a button on charts.html for it call it flow. Check if that
button doesnt exist first before you add it".

Nothing is invoked and nothing is written. On justhodl.ai:

  1. The four documents as the site serves them: the funds are added up again here (this op's code, not the engine's and
     not the page's) and every asset-class row, both totals, the boards and the history's last day are held to them.
  2. global-flow-desk.html in a real browser: the tiles, the bars, the boards and the table show the document's own
     figures; the window buttons, the measure switch, a sort both ways, a sector's sheet and a fund's sheet do what they
     say; the page's own adding-up says the figures hold; the research view that was the page is one click away and loads.
  3. A phone: nothing scrolls sideways.
  4. chart.html: a Flow button beside Flows (which is where and what it was); the pointer clicks it and the money map
     opens in the chart's workspace; a fund's sheet sends the fund to the chart behind and closes the workspace; Flows
     still opens its own desk.
  5. A phone: the button is on the bar and a tap opens the map.

Safe to run again.
"""
from pathlib import Path
import base64
import gzip
import importlib.util
import json
import re
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops")]
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
NAME = "ops_6967_money_flow_live_proof"
MIN_FUNDS = 4500
PAGE_PATH = "/global-flow-desk.html"
TITLE = "Money flow · where money is flowing in and out: every ETF by asset class, sector, country and fund"
HEAD = "Money flow · where money is flowing in and out"
WINS = ("d1", "w1", "m1", "m3", "ytd", "y1")
LEV = "Leveraged & inverse"
PAGE = r"""() => { const q = (s) => document.querySelector(s), all = (s) => Array.from(document.querySelectorAll(s)), txt = (e) => e ? e.textContent.replace(/\s+/g, ' ').trim() : null;
  const body = q('#mm-body'), copy = body ? body.cloneNode(true) : null; if (copy) { const j = copy.querySelector('#mm-json'); if (j) j.remove(); }
  const first = q('#mm-body > .mm-map-only');
  return { asof: txt(q('#mm-asof')), win: txt(q('.mm-bar0 [data-win].on')), measure: txt(q('.mm-bar0 [data-measure].on')),
    tiles: all('.mm-tiles .mm-tile').map(t => [txt(t.querySelector('.mm-k')), txt(t.querySelector('.mm-v')), txt(t.querySelector('.mm-s'))]),
    reading: txt(q('#mm-reading')),
    segs: all('#mm-segments .mm-row').map(r => [r.getAttribute('data-g').split('|')[1], txt(r.querySelectorAll('.mm-num')[0]), txt(r.querySelectorAll('.mm-num')[1])]),
    sectors: all('#mm-sectors .mm-row').map(r => [r.getAttribute('data-g').split('|')[1], txt(r.querySelectorAll('.mm-num')[0])]),
    through: all('#mm-sectors_through .mm-row').length, matrix: all('#mm-matrix td.c').length, cards: all('.mm-card').map(c => c.id),
    board_in: all('#mm-etfs .mm-in > div:first-child .mm-brow').map(r => [r.getAttribute('data-f'), txt(r.querySelectorAll('.mm-num')[0])]),
    board_out: all('#mm-etfs .mm-in > div:last-child .mm-brow').map(r => [r.getAttribute('data-f'), txt(r.querySelectorAll('.mm-num')[0])]),
    table: all('#mm-ftable tbody tr').slice(0, 5).map(r => r.getAttribute('data-f')), table_rows: all('#mm-ftable tbody tr').length, pager: txt(q('.mm-pager span')),
    audit: txt(q('#mm-notes .mm-note')), hist_bars: all('#mm-hchart svg rect:not(.mm-hit)').length, hist_sub: txt(q('#mm-history header p')),
    bad: ((copy ? copy.textContent : '').match(/\bNaN\b|\bundefined\b|\[object|Infinity(?! [A-Z])/g) || []).slice(0, 5),
    research_hidden: document.getElementById('research-view').hidden, map_hidden: first ? getComputedStyle(first).display === 'none' : null,
    embed: document.getElementById('mm-root').className, ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""
CHART = r"""() => { const b = (id) => document.getElementById(id), btn = b('btn-mflow'), ov = b('ws-overlay'), f = ov ? ov.querySelector('iframe') : null, r = btn ? btn.getBoundingClientRect() : null, fl = b('btn-flows');
  return { btn: !!btn, n: document.querySelectorAll('#btn-mflow').length, prev: btn && btn.previousElementSibling ? btn.previousElementSibling.id : null, next: btn && btn.nextElementSibling ? btn.nextElementSibling.id : null,
    glyph: btn ? btn.querySelector('.g').textContent : null, label: btn ? btn.querySelector('.l').textContent : null, title: btn ? btn.title : null,
    flows: fl ? [fl.querySelector('.g').textContent, fl.querySelector('.l').textContent, fl.title] : null,
    box: r ? [Math.round(r.left), Math.round(r.top), Math.round(r.width), Math.round(r.height)] : null, view: [innerWidth, innerHeight],
    overlay: ov ? ov.className : null, src: f ? f.getAttribute('src') : null, head: ov && ov.querySelector('.wshd span') ? ov.querySelector('.wshd span').textContent : null,
    open: ov && ov.querySelector('.wshd a') ? ov.querySelector('.wshd a').getAttribute('href') : null,
    desks: Array.from(document.querySelectorAll('#tfbar .wsdesk')).map(x => x.id), on: (document.querySelector('#tabs .tab.on') || {dataset: {}}).dataset.id || null,
    ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""


def fetch(path, timeout=90):
    req = urllib.request.Request(BASE + path, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def fetch_json(path):
    raw = fetch(path)
    return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)


def money(text):
    """'+$33.5B' / '−$162M' / '$0' -> (dollars, half a unit of the last printed digit); None when it is not money."""
    m = re.match(r"^\s*([+−-]?)\$([0-9][0-9,]*)(?:\.([0-9]+))?([KMBT]?)\s*$", text or "")
    if not m:
        return None
    unit = {"": 1.0, "K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}[m.group(4)]
    v = float(m.group(2).replace(",", "") + ("." + m.group(3) if m.group(3) else "")) * unit
    return (-v if m.group(1) in ("−", "-") else v), 0.5 * unit / (10 ** len(m.group(3) or ""))


def shows(text, value):
    """The page's printed money is the document's figure, to the digits it prints."""
    got = money(text)
    return bool(got) and value is not None and abs(got[0] - value) <= got[1] * 1.02 + 1e-6


def pct_shows(text, value):
    """'+16.9%' is the document's percentage to the digits printed."""
    m = re.match(r"^\s*([+\u2212-]?)([0-9]+)(?:\.([0-9]+))?%\s*$", text or "")
    if not m or value is None:
        return False
    v = float(m.group(2) + ("." + m.group(3) if m.group(3) else "")) * (-1 if m.group(1) in ("\u2212", "-") else 1)
    return abs(v - value) <= 0.5 / (10 ** len(m.group(3) or "")) * 1.02 + 1e-9


def on_screen(c):
    b, v = c.get("box"), c.get("view")
    return bool(b and v and b[2] > 0 and b[3] > 0 and b[0] >= 0 and b[1] >= 0 and b[0] + b[2] <= v[0] and b[1] + b[3] <= v[1])


def main():
    from ops_report import report
    spec = importlib.util.spec_from_file_location("ops_6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    stamp = int(time.time())
    shots, fails = {}, []

    with report(NAME) as r:
        r.heading("ops 6967 - Money flow, live: the documents, the page in a real browser, the chart's Flow button (read-only)")

        def ck(cond, msg):
            if cond:
                return True
            fails.append(msg)
            r.warn("CHECK FAILED: " + msg)
            return False

        # ------------------------------------------------------------------ 0. served
        r.section("0. The live site serves the new files")
        served = {}
        for attempt in range(60):
            try:
                html = fetch("%s?probe=%d" % (PAGE_PATH, stamp + attempt))
                tag = re.search(rb'<script src="(/jh-money-map\.js\?v=[0-9a-f]{8})"', html)        # the build stamps each script with its content hash
                served = {"page": b'<div id="mm-root">' in html and b'<main id="research-view" hidden>' in html,
                          "script": bool(tag) and b"JHMoneyMap" in fetch(tag.group(1).decode() + "&probe=%d" % (stamp + attempt)),
                          "engine": b"id=btn-mflow " in fetch("/jh-chart-engine.js?probe=%d" % (stamp + attempt)),
                          "rail": b'mflow: ["Money flow' in fetch("/jh-chart-tvrail.js?probe=%d" % (stamp + attempt)),
                          "chart": fetch("/chart.html?probe=%d" % (stamp + attempt)).count(b"&mflow=20261126") == 2}
            except Exception as e:  # noqa: BLE001
                served = {"error": str(e)[:80]}
            if served and all(v is True for v in served.values()):
                break
            time.sleep(20)
        r.log("served after %d tries: %s" % (attempt + 1, json.dumps(served)))
        if not (served and all(v is True for v in served.values())):
            r.fail("the live site does not serve Money flow yet; nothing was judged")
            sys.exit(1)

        # ------------------------------------------------------------------ 1. the documents, worked again
        r.section("1. The documents as the site serves them, added up again here")
        doc = fetch_json("/data/global-flow-map.json?probe=%d" % stamp)
        fdoc = fetch_json("/data/global-flow-map-funds.json?probe=%d" % stamp)
        hist = fetch_json("/data/global-flow-map-history.json?probe=%d" % stamp)
        q = doc["quality"]
        col = {c: i for i, c in enumerate(fdoc["cols"])}
        funds = {x[0]: x for x in fdoc["rows"]}
        cur = [x for x in fdoc["rows"] if x[col["behind"]] <= q["late_rule_days"]]
        r.log("main %s %s generated %s, orders through %s | funds document %d rows, generated %s | history %d days to %s, %d rows | current funds %d (document says %d), late %d, assets $%.2fT" % (
            doc["contract"], doc["version"], doc["generated_at"][:16], doc["orders_through"], len(fdoc["rows"]), fdoc["generated_at"][:16], len(hist["dates"]), hist["dates"][-1], len(hist["series"]),
            len(cur), q["funds_current"], q["funds_late"], q["assets_current_usd"] / 1e12))
        ck(doc["contract"] == "global-flow-map.v1" and doc["version"] == fdoc["version"] == hist["version"] and doc["generated_at"] == fdoc["generated_at"] == hist["generated_at"], "the three documents are not one build")
        ck(len(cur) == q["funds_current"] >= MIN_FUNDS and len(fdoc["rows"]) == q["funds_with_rows"], "fund counts: %d current against %s, %d rows against %s" % (len(cur), q["funds_current"], len(fdoc["rows"]), q["funds_with_rows"]))
        ck(hist["dates"][-1] == doc["as_of"] == doc["orders_through"] == fdoc["as_of"], "the documents end on different days")
        wrong = []
        for w in WINS:
            by, every, plain = {}, 0.0, 0.0
            for x in cur:
                v = x[col[w]] or 0
                by[x[col["seg"]]] = by.get(x[col["seg"]], 0.0) + v
                every += v
                plain += 0 if x[col["seg"]] == LEV else v
            for g in doc["groups"]["segments"]:
                if abs(g["w"][w]["net"] - by.get(g["id"], 0.0)) > 5:
                    wrong.append((g["id"], w, g["w"][w]["net"], by.get(g["id"])))
            for k, v in (("all", every), ("plain", plain)):
                if abs(doc["totals"][k]["w"][w]["net"] - v) > 5:
                    wrong.append(("total " + k, w, doc["totals"][k]["w"][w]["net"], v))
            top_in = max(cur, key=lambda x: x[col[w]] or 0)
            top_out = min(cur, key=lambda x: x[col[w]] or 0)
            b = doc["boards"][w]
            if b["in_usd"][0][0] != top_in[0] or b["out_usd"][0][0] != top_out[0]:
                wrong.append(("board", w, b["in_usd"][0][0] + "/" + b["out_usd"][0][0], top_in[0] + "/" + top_out[0]))
        d1 = {g["id"]: g["w"]["d1"]["net"] for g in doc["groups"]["segments"]}
        for k, v in d1.items():
            n = next(g["n"] for g in doc["groups"]["segments"] if g["id"] == k)
            if abs(hist["series"]["segments:" + k][-1] - v) > 0.5 * n + 1:
                wrong.append(("history last day", k, hist["series"]["segments:" + k][-1], v))
        r.log("added up again: %d asset classes x 6 windows, both totals, the top of both boards in every window and the last day of every class in the history: %d wrong %s" % (len(doc["groups"]["segments"]), len(wrong), json.dumps(wrong[:4])))
        ck(not wrong, "the documents do not add up: %s" % wrong[:4])
        seg_count = {}
        for x in cur:
            seg_count[x[col["seg"]]] = seg_count.get(x[col["seg"]], 0) + 1
        ck(all(seg_count.get(g["id"]) == g["n"] for g in doc["groups"]["segments"]), "an asset class does not hold the funds it says")
        pick = "SPY" if "SPY" in funds else cur[0][0]
        shard = fetch_json("/data/global-flow-map/funds/%s.json?probe=%d" % (pick[0], stamp))
        own = shard["funds"][pick]
        ck(abs(sum(v or 0 for v in own["f"][-5:]) - (funds[pick][col["w1"]] or 0)) < 5, "%s's own days do not sum to its week" % pick)
        tot = doc["totals"]["plain"]["w"]
        segs = sorted(doc["groups"]["segments"], key=lambda g: -g["w"]["w1"]["net"])
        sect = sorted(doc["groups"]["sectors"], key=lambda g: -g["w"]["w1"]["net"])
        r.log("WHAT IT SAYS, week to %s: net %+.1fB into %d unleveraged funds (in %.1fB, out %.1fB) | month %+.0fB | YTD %+.0fB" % (
            doc["as_of"], tot["w1"]["net"] / 1e9, doc["totals"]["plain"]["n"], tot["w1"]["in"] / 1e9, tot["w1"]["out"] / 1e9, tot["m1"]["net"] / 1e9, tot["ytd"]["net"] / 1e9))
        r.log("asset classes, week: " + " | ".join("%s %+.2fB" % (g["id"], g["w"]["w1"]["net"] / 1e9) for g in segs))
        r.log("sector funds, week: " + " | ".join("%s %+.2fB" % (g["id"], g["w"]["w1"]["net"] / 1e9) for g in sect))
        r.log("sectors through every equity fund, week: " + " | ".join("%s %+.2fB" % (g["id"], g["w"]["w1"]["net"] / 1e9) for g in sorted(doc["groups"]["sectors_through"], key=lambda g: -g["w"]["w1"]["net"])))
        r.log("ETFs, week: in " + ", ".join("%s %+.2fB" % (t, v / 1e9) for t, v, _ in doc["boards"]["w1"]["in_usd"][:10]) + " | out " + ", ".join("%s %+.2fB" % (t, v / 1e9) for t, v, _ in doc["boards"]["w1"]["out_usd"][:10]))
        r.log("tables: " + ", ".join("%s %d" % (k, len(v)) for k, v in doc["groups"].items()))

        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)

            # ------------------------------------------------------------------ 2. the page
            r.section("2. The page in a real browser")
            ctx = browser.new_context(viewport={"width": 1500, "height": 1000}, user_agent=UA)
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto("%s%s?nocache=%d" % (BASE, PAGE_PATH, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#mm-ftable tbody tr", timeout=90000)
            page.wait_for_function("() => document.querySelectorAll('#mm-hchart svg rect').length > 5 && document.querySelector('#mm-notes .mm-note') && !/loading/i.test(document.querySelector('#mm-notes .mm-note').textContent)", timeout=90000)
            page.wait_for_timeout(600)
            s = page.evaluate(PAGE)
            r.log("as of line: %s" % s["asof"])
            r.log("window %s, measure %s | tiles: %s" % (s["win"], s["measure"], json.dumps(s["tiles"], ensure_ascii=False)))
            r.log("reading: %s" % s["reading"])
            r.log("cards (%d): %s" % (len(s["cards"]), " ".join(s["cards"])))
            ck(s["win"] == "1W" and s["measure"] == "Dollars" and s["map_hidden"] is False, "the page did not open on the money map, one week, dollars: %s %s" % (s["win"], s["measure"]))
            ck(s["research_hidden"] is True, "the research view is showing on first load")
            ck(len(s["tiles"]) == 5 and shows(s["tiles"][0][1], tot["w1"]["net"]) and shows(s["tiles"][1][1], tot["w1"]["in"]) and shows(s["tiles"][2][1], abs(tot["w1"]["out"])),
               "the tiles are not the document's week: %s against net %s in %s out %s" % (s["tiles"][:3], tot["w1"]["net"], tot["w1"]["in"], tot["w1"]["out"]))
            plain_segs = [g for g in segs if g["id"] != LEV]
            ck(s["tiles"][3][1] == plain_segs[0]["id"] and (s["tiles"][4][1] == plain_segs[-1]["id"] or plain_segs[-1]["w"]["w1"]["net"] >= 0), "most in / most out: %s / %s against %s / %s" % (s["tiles"][3][1], s["tiles"][4][1], plain_segs[0]["id"], plain_segs[-1]["id"]))
            ck(doc["orders_through"][-2:].lstrip("0") in s["asof"] and "{:,}".format(q["funds_current"]) in s["asof"], "the as-of line: %s" % s["asof"])
            ck([x[0] for x in s["segs"]] == [g["id"] for g in segs] and all(shows(x[1], g["w"]["w1"]["net"]) for x, g in zip(s["segs"], segs)),
               "the asset-class bars are not the document's rows in order: %s" % s["segs"][:3])
            ck([x[0] for x in s["sectors"]] == [g["id"] for g in sect] and all(shows(x[1], g["w"]["w1"]["net"]) for x, g in zip(s["sectors"], sect)), "the sector bars: %s" % s["sectors"][:3])
            ck(s["through"] == len(doc["groups"]["sectors_through"]) and s["matrix"] == 6 * len(segs), "sectors through %d, matrix cells %d" % (s["through"], s["matrix"]))
            bw = doc["boards"]["w1"]
            ck([x[0] for x in s["board_in"]] == [x[0] for x in bw["in_usd"][:15]] and [x[0] for x in s["board_out"]] == [x[0] for x in bw["out_usd"][:15]]
               and all(shows(x[1], y[1]) for x, y in zip(s["board_in"], bw["in_usd"])) and all(shows(x[1], y[1]) for x, y in zip(s["board_out"], bw["out_usd"])),
               "the boards are not the document's: %s / %s" % (s["board_in"][:3], s["board_out"][:3]))
            r.log("boards: in %s | out %s" % (" ".join("%s %s" % (a, b_) for a, b_ in s["board_in"][:8]), " ".join("%s %s" % (a, b_) for a, b_ in s["board_out"][:8])))
            largest = [x[0] for x in sorted(cur, key=lambda x: -(x[col["assets"]] or 0))[:5]]
            ck(s["table"] == largest and s["table_rows"] == 50, "the table does not open on the largest funds: %s against %s (%d rows)" % (s["table"], largest, s["table_rows"]))
            r.log("every fund: opens on %s; %s" % (" ".join(s["table"]), s["pager"]))
            ck("Added up again on this page" in (s["audit"] or "") and "differ" not in (s["audit"] or ""), "the page's own adding-up: %s" % (s["audit"] or "")[:200])
            r.log("the page's own adding-up: %s" % s["audit"])
            want_bars = sum(1 for v in hist["series"]["total:plain"][-130:] if v)
            ck(s["hist_bars"] == want_bars, "day by day draws %d bars for %d days with money" % (s["hist_bars"], want_bars))
            ck(not s["bad"] and s["ox"] <= 0, "bad text %s, sideways scroll %s px" % (s["bad"], s["ox"]))
            cards = {c[3:] for c in s["cards"]}
            ck({k for k, v in doc["groups"].items() if v} <= cards and {"matrix", "etfs", "history", "funds", "notes"} <= cards, "a table of the document has no card: %s" % sorted({k for k, v in doc["groups"].items() if v} - cards))
            shots["1-the-page"] = page.screenshot(type="jpeg", quality=72)

            # the windows and the measure
            page.click(".mm-bar0 [data-win='m1']")
            page.wait_for_timeout(400)
            s2 = page.evaluate(PAGE)
            segs_m = sorted(doc["groups"]["segments"], key=lambda g: -g["w"]["m1"]["net"])
            ck(s2["win"] == "1M" and shows(s2["tiles"][0][1], tot["m1"]["net"]) and [x[0] for x in s2["segs"]] == [g["id"] for g in segs_m] and s2["board_in"][0][0] == doc["boards"]["m1"]["in_usd"][0][0],
               "1M: tile %s against %s, first row %s against %s" % (s2["tiles"][0][1], tot["m1"]["net"], s2["segs"][0][0], segs_m[0]["id"]))
            page.click(".mm-bar0 [data-measure='pct']")
            page.wait_for_timeout(400)
            s3 = page.evaluate(PAGE)
            by_pct = sorted((g for g in doc["groups"]["segments"] if g["w"]["m1"]["pct"] is not None), key=lambda g: -g["w"]["m1"]["pct"])
            ck(s3["measure"] == "% of assets" and s3["segs"][0][0] == by_pct[0]["id"] and pct_shows(s3["segs"][0][1], by_pct[0]["w"]["m1"]["pct"]),
               "%% of assets: first row %s against %s %.3f" % (s3["segs"][0], by_pct[0]["id"], by_pct[0]["w"]["m1"]["pct"]))
            r.log("1M: net %s, first %s | %% of assets: first %s %s" % (s2["tiles"][0][1], s2["segs"][0][0], s3["segs"][0][0], s3["segs"][0][1]))
            page.click(".mm-bar0 [data-measure='usd']")
            page.click(".mm-bar0 [data-win='w1']")
            page.wait_for_timeout(400)

            # a sector's sheet: the funds behind the bar, sortable
            tech = next((g for g in sect if g["id"] == "Technology"), sect[0])
            page.locator("#mm-sectors .mm-row[data-g='sectors|%s']" % tech["id"]).click()
            page.wait_for_selector("#mm-modal.on #mm-glist table", timeout=15000)
            page.wait_for_timeout(300)
            sheet = page.evaluate("""() => { const rows = Array.from(document.querySelectorAll('#mm-glist tbody tr')); return { title: document.querySelector('#mm-sheet h2').textContent.replace(/\\s+/g, ' ').trim(),
                rows: rows.length, first: rows[0] ? rows[0].getAttribute('data-f') : null, last: rows.length ? rows[rows.length - 1].getAttribute('data-f') : null, facts: document.querySelectorAll('#mm-sheet .mm-facts .mm-tile').length,
                chart: document.querySelectorAll('#mm-gchart svg rect:not(.mm-hit)').length }; }""")
            own_funds = sorted((x for x in cur if x[col["seg"]] != LEV and x[col["sec"]] == tech["id"]), key=lambda x: -(x[col["w1"]] or 0))
            ck(sheet["rows"] == tech["n"] == len(own_funds) and sheet["first"] == own_funds[0][0] and sheet["facts"] == 6 and sheet["chart"] > 0, "the %s sheet: %s against %d funds, first %s" % (tech["id"], sheet, tech["n"], own_funds[0][0]))
            page.locator("#mm-glist th[data-sort='w1']").click()
            page.wait_for_timeout(250)
            turned = page.evaluate("() => document.querySelector('#mm-glist tbody tr').getAttribute('data-f')")
            ck(turned == own_funds[-1][0] or (own_funds[-1][col["w1"]] or 0) == (funds[turned][col["w1"]] or 0), "the sheet's list does not turn round: %s against %s" % (turned, own_funds[-1][0]))
            r.log("%s sheet: %s | %d funds, most in %s, turned round %s, %d bars of its own days" % (tech["id"], sheet["title"], sheet["rows"], sheet["first"], turned, sheet["chart"]))
            shots["2-a-sector"] = page.screenshot(type="jpeg", quality=70)
            page.keyboard.press("Escape")
            page.wait_for_timeout(200)
            ck(page.evaluate("() => !document.getElementById('mm-modal').classList.contains('on')"), "Escape does not close a sheet")

            # the table: sort both ways, search, a fund's sheet
            page.locator("#mm-ftable th[data-sort='w1']").click()
            page.wait_for_timeout(300)
            most_in = page.evaluate("() => document.querySelector('#mm-ftable tbody tr').getAttribute('data-f')")
            page.locator("#mm-ftable th[data-sort='w1']").click()
            page.wait_for_timeout(300)
            most_out = page.evaluate("() => document.querySelector('#mm-ftable tbody tr').getAttribute('data-f')")
            ck(most_in == bw["in_usd"][0][0] and most_out == bw["out_usd"][0][0], "sorting the week both ways gives %s and %s, the document says %s and %s" % (most_in, most_out, bw["in_usd"][0][0], bw["out_usd"][0][0]))
            page.fill("#mm-q", pick)
            page.wait_for_function("(t) => { const r = document.querySelector('#mm-ftable tbody tr'); return r && r.getAttribute('data-f') === t; }", arg=pick, timeout=15000)
            page.locator("#mm-ftable tbody tr[data-f='%s']" % pick).first.click()
            page.wait_for_selector("#mm-fhist svg", timeout=30000)
            page.wait_for_timeout(400)
            fs = page.evaluate("""() => ({ title: document.querySelector('#mm-sheet h2').textContent.replace(/\\s+/g, ' ').trim(), tiles: Array.from(document.querySelectorAll('#mm-sheet .mm-facts .mm-v')).map(e => e.textContent),
                charts: document.querySelectorAll('#mm-fhist svg').length, bars: document.querySelectorAll('#mm-fhist svg:first-of-type rect:not(.mm-hit)').length, kv: document.querySelectorAll('#mm-sheet dl.mm-kv dt').length,
                link: document.querySelector('#mm-sheet a[data-chart]').getAttribute('href') })""")
            ck(fs["charts"] == 2 and len(fs["tiles"]) == 6 and all(shows(t, funds[pick][col[w]]) for t, w in zip(fs["tiles"], WINS)) and fs["bars"] == sum(1 for v in own["f"] if v) and fs["kv"] == len(fdoc["cols"]) - 8
               and fs["link"] == "/chart.html?symbol=" + pick, "%s's sheet: %s" % (pick, fs))
            r.log("sorted both ways: %s then %s | %s's sheet: %s, six windows %s, %d bars of its own days, %d other figures, link %s" % (most_in, most_out, pick, fs["title"], " ".join(fs["tiles"]), fs["bars"], fs["kv"], fs["link"]))
            shots["3-a-fund"] = page.screenshot(type="jpeg", quality=70)
            page.click("#mm-modal [data-close]")
            page.wait_for_timeout(200)

            # the research view that was the page
            page.click(".mm-top [data-view='research']")
            try:
                page.wait_for_function("() => /publication|research/i.test((document.getElementById('state') || {}).textContent || '') && !/Verifying/.test(document.getElementById('state').textContent)", timeout=60000)
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(800)
            rv = page.evaluate("""() => ({ hidden: document.getElementById('research-view').hidden, map: getComputedStyle(document.querySelector('#mm-body > .mm-map-only')).display, state: document.getElementById('state').textContent.slice(0, 120),
                cards: document.querySelectorAll('#cards .metric').length, chart: document.querySelectorAll('#chart svg').length, hash: location.hash, h1: document.querySelector('#research-view h1').textContent })""")
            r.log("research view: %s" % json.dumps(rv))
            ck(rv["hidden"] is False and rv["map"] == "none" and rv["hash"] == "#research" and rv["cards"] > 0 and rv["chart"] == 1 and "Current publication" in rv["state"], "the research view is not one click away and working: %s" % rv)
            shots["4-the-research-view"] = page.screenshot(type="jpeg", quality=65)
            page.click(".mm-top [data-view='map']")
            page.wait_for_timeout(300)
            back = page.evaluate(PAGE)
            ck(back["research_hidden"] is True and back["map_hidden"] is False, "the money map does not come back")
            ck(not errors, "the page threw: %s" % errors[:3])
            r.log("page errors: %d" % len(errors))
            # the research view's own link still lands on the research view
            page.goto("%s%s?nocache=%d#research" % (BASE, PAGE_PATH, stamp + 1), wait_until="domcontentloaded")
            page.wait_for_selector("#mm-root .mm-top", timeout=60000)
            page.wait_for_timeout(1500)
            ck(page.evaluate("() => document.getElementById('research-view').hidden") is False, "#research does not open the research view")
            ctx.close()

            # ------------------------------------------------------------------ 3. a phone
            r.section("3. A phone")
            ctx = browser.new_context(viewport={"width": 390, "height": 844}, user_agent=UA, is_mobile=True, has_touch=True, device_scale_factor=2)
            page = ctx.new_page()
            perr = []
            page.on("pageerror", lambda e: perr.append(str(e)[:200]))
            page.goto("%s%s?nocache=%d" % (BASE, PAGE_PATH, stamp + 2), wait_until="domcontentloaded")
            page.wait_for_selector("#mm-ftable tbody tr", timeout=90000)
            page.wait_for_timeout(800)
            ph = page.evaluate(PAGE)
            ck(ph["ox"] <= 0 and len(ph["tiles"]) == 5 and len(ph["segs"]) == len(segs) and not perr, "the phone: sideways %s px, %d tiles, %d rows, errors %s" % (ph["ox"], len(ph["tiles"]), len(ph["segs"]), perr[:2]))
            shots["5-phone"] = page.screenshot(type="jpeg", quality=65)
            page.evaluate("() => document.getElementById('mm-matrix').scrollIntoView()")
            page.wait_for_timeout(300)
            r.log("phone: %d tiles, %d asset-class rows, sideways scroll %d px, errors %d" % (len(ph["tiles"]), len(ph["segs"]), page.evaluate(PAGE)["ox"], len(perr)))
            ck(page.evaluate(PAGE)["ox"] <= 0, "the rotation matrix makes the phone scroll sideways")
            ctx.close()

            # ------------------------------------------------------------------ 4. the chart's button
            r.section("4. The chart: a Flow button beside Flows")
            ctx = browser.new_context(viewport={"width": 1600, "height": 900}, user_agent=UA)
            page = ctx.new_page()
            cerr = []
            page.on("pageerror", lambda e: cerr.append(str(e)[:200]))
            page.goto("%s/chart.html?s=QQQ&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#btn-mflow", timeout=90000, state="attached")
            page.wait_for_function("() => (window.lastBars || []).length > 300 && typeof window.jhOpenWorkspace === 'function' && typeof window.jhOpenSymbol === 'function'", timeout=90000)
            page.wait_for_timeout(1200)
            c0 = page.evaluate(CHART)
            r.log("button: %s" % json.dumps({k: c0[k] for k in ("btn", "n", "prev", "next", "glyph", "label", "title", "flows", "overlay", "on")}, ensure_ascii=False))
            r.log("desk buttons on the bar (%d): %s" % (len(c0["desks"]), " ".join(c0["desks"])))
            ck(c0["btn"] and c0["n"] == 1 and c0["prev"] == "btn-flows" and c0["next"] == "btn-heat" and c0["glyph"] == "⇅" and c0["label"] == "Flow" and c0["title"] == TITLE, "the button: %s" % {k: c0[k] for k in ("btn", "n", "prev", "next", "glyph", "label", "title")})
            ck(c0["flows"] == ["⇄", "Flows", "Inflows & outflows"], "Flows is not what it was: %s" % c0["flows"])
            ck(c0["desks"][:4] == ["btn-macro", "btn-flows", "btn-mflow", "btn-heat"] and len(set(c0["desks"])) == len(c0["desks"]) and "btn-cbe" in c0["desks"] and "btn-rmap" in c0["desks"], "the bar's desks: %s" % c0["desks"][:6])
            ck(c0["overlay"] == "" and c0["on"] == "QQQ", "the chart did not open on QQQ with the workspace closed: %s %s" % (c0["overlay"], c0["on"]))
            how = None
            for _ in range(3):
                page.mouse.move(700, 3)                     # the toolbar hides until the pointer reaches the top edge
                try:
                    page.wait_for_selector("#jh-ah-top.show", timeout=6000)
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(500)
                page.evaluate("() => document.getElementById('btn-mflow').scrollIntoView({ inline: 'center', block: 'nearest' })")
                page.wait_for_timeout(300)
                seen = page.evaluate(CHART)
                if not on_screen(seen):
                    continue
                shots["6-the-bar"] = page.screenshot(type="jpeg", quality=80, clip={"x": max(0, seen["box"][0] - 180), "y": max(0, seen["box"][1] - 14), "width": 900, "height": seen["box"][3] + 30})
                try:
                    page.click("#btn-mflow", timeout=6000)
                    how = "a real click at %s" % seen["box"][:2]
                    break
                except Exception:  # noqa: BLE001
                    continue
            ck(bool(how), "the pointer could not click the Flow button")
            page.wait_for_selector("#ws-overlay.on iframe", timeout=20000)
            frame = None
            for _ in range(80):
                frame = next((f for f in page.frames if "global-flow-desk" in f.url), None)
                if frame:
                    break
                page.wait_for_timeout(250)
            frame.locator("#mm-ftable tbody tr").first.wait_for(timeout=90000)
            page.wait_for_timeout(800)
            c1 = page.evaluate(CHART)
            inner = frame.evaluate(PAGE)
            r.log("%s: workspace %r on %s, headed %r, Open page -> %s | inside: %s, %d tiles, net %s, %d asset-class rows" % (how, c1["overlay"], c1["src"], c1["head"], c1["open"], inner["embed"], len(inner["tiles"]), inner["tiles"][0][1] if inner["tiles"] else None, len(inner["segs"])))
            ck(c1["overlay"] == "on" and c1["src"] == PAGE_PATH and c1["head"] == HEAD and c1["open"] == PAGE_PATH, "the workspace: %s" % {k: c1[k] for k in ("overlay", "src", "head", "open")})
            ck("mm-embed" in inner["embed"] and len(inner["tiles"]) == 5 and shows(inner["tiles"][0][1], tot["w1"]["net"]) and len(inner["segs"]) == len(segs) and inner["research_hidden"] is True, "the map inside the workspace: %s" % inner["tiles"][:1])
            ck(not cerr, "chart.html threw: %s" % cerr[:3])
            shots["7-in-the-workspace"] = page.screenshot(type="jpeg", quality=70)
            # a fund's sheet hands the fund to the chart behind
            frame.locator("#mm-q").fill(pick)
            frame.locator("#mm-ftable tbody tr[data-f='%s']" % pick).first.wait_for(timeout=15000)
            frame.locator("#mm-ftable tbody tr[data-f='%s']" % pick).first.click()
            frame.locator("#mm-sheet a[data-chart]").wait_for(timeout=15000)
            frame.locator("#mm-sheet a[data-chart]").click()
            try:
                page.wait_for_function("(t) => { const on = document.querySelector('#tabs .tab.on'); return on && on.dataset.id === t && !document.getElementById('ws-overlay').classList.contains('on'); }", arg=pick, timeout=30000)
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(800)
            c2 = page.evaluate(CHART)
            r.log("%s's sheet, Open on the chart: the chart is on %s, workspace %r, address ...%s" % (pick, c2["on"], c2["overlay"], page.url[-34:]))
            ck(c2["on"] == pick and c2["overlay"] == "", "the fund did not go to the chart behind: on %s, workspace %r" % (c2["on"], c2["overlay"]))
            ck(not cerr, "chart.html threw after the hand-off: %s" % cerr[:3])
            # Flows still opens its own desk
            page.evaluate("() => document.getElementById('btn-flows').click()")
            page.wait_for_selector("#ws-overlay.on iframe", timeout=20000)
            page.wait_for_timeout(600)
            c3 = page.evaluate(CHART)
            ck(c3["src"] == "/inflows-outflows.html" and c3["head"] == "Inflows & outflows", "Flows opens %s (%s)" % (c3["src"], c3["head"]))
            r.log("Flows still opens %s (%s)" % (c3["src"], c3["head"]))
            ctx.close()

            # ------------------------------------------------------------------ 5. the chart on a phone
            r.section("5. The chart on a phone")
            ctx = browser.new_context(viewport={"width": 390, "height": 844}, user_agent=UA, is_mobile=True, has_touch=True, device_scale_factor=2)
            page = ctx.new_page()
            merr = []
            page.on("pageerror", lambda e: merr.append(str(e)[:200]))
            page.goto("%s/chart.html?s=QQQ&nocache=%d" % (BASE, stamp + 3), wait_until="domcontentloaded")
            page.wait_for_selector("#btn-mflow", timeout=90000, state="attached")
            page.wait_for_function("() => (window.lastBars || []).length > 300 && typeof window.jhOpenWorkspace === 'function'", timeout=90000)
            page.wait_for_timeout(1200)
            page.evaluate("() => document.getElementById('btn-mflow').scrollIntoView({ inline: 'center', block: 'nearest' })")
            page.wait_for_timeout(400)
            m0 = page.evaluate(CHART)
            tapped = None
            if on_screen(m0):
                try:
                    page.tap("#btn-mflow", timeout=6000)
                    tapped = "a real tap at %s" % m0["box"][:2]
                except Exception:  # noqa: BLE001
                    tapped = None
            if not tapped:
                page.evaluate("() => document.getElementById('btn-mflow').click()")
            page.wait_for_selector("#ws-overlay.on iframe", timeout=20000)
            mframe = None
            for _ in range(80):
                mframe = next((f for f in page.frames if "global-flow-desk" in f.url), None)
                if mframe:
                    break
                page.wait_for_timeout(250)
            mframe.locator("#mm-ftable tbody tr").first.wait_for(timeout=90000)
            page.wait_for_timeout(600)
            m1 = page.evaluate(CHART)
            mi = mframe.evaluate(PAGE)
            r.log("phone: button between %s and %s at %s, on screen %s, %s; workspace %r on %s; %d tiles; sideways scroll %d px on the chart, %d px in the map; page errors %d" % (
                m0["prev"], m0["next"], m0["box"], on_screen(m0), tapped or "pressed by script", m1["overlay"], m1["src"], len(mi["tiles"]), m1["ox"], mi["ox"], len(merr)))
            ck(m0["prev"] == "btn-flows" and m0["next"] == "btn-heat" and on_screen(m0) and bool(tapped), "the phone's button: %s" % {k: m0[k] for k in ("prev", "next", "box", "view")})
            ck(m1["overlay"] == "on" and m1["src"] == PAGE_PATH and len(mi["tiles"]) == 5 and m0["ox"] <= 0 and m1["ox"] <= 0 and mi["ox"] <= 0, "the phone's workspace: %s, %d tiles, sideways %s / %s / %s" % (m1["src"], len(mi["tiles"]), m0["ox"], m1["ox"], mi["ox"]))
            ck(not merr, "chart.html threw on the phone: %s" % merr[:3])
            shots["8-phone-chart"] = page.screenshot(type="jpeg", quality=65)
            ctx.close()
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(button="btn-mflow", beside="Flows | Flow | Heat", funds=q["funds_current"], as_of=doc["as_of"], week_net="%+.1fB" % (tot["w1"]["net"] / 1e9), failed_checks=len(fails), document=str(doc.get("generated_at"))[:16])
        for f in fails[:40]:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("LIVE: Money flow %s shows %s ETFs and $%.2f trillion, orders through %s: the page's figures are the document's and the document adds up; the research view is one click away; "
             "chart.html has a Flow button beside Flows that opens it, and a fund's sheet sends the fund to the chart" % (doc["version"], "{:,}".format(q["funds_current"]), q["assets_current_usd"] / 1e12, doc["as_of"]))


if __name__ == "__main__":
    main()
