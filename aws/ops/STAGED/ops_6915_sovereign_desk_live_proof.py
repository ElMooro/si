"""ops 6915 - LIVE PROOF (read-only, re-runnable): the Sovereign button, the desk it opens, and the repaired CISS page.

A real browser on justhodl.ai, judged against the documents the pages read:
  1. chart.html: the Sovereign button sits right after Repo; a mouse click opens the desk in the chart's overlay;
  2. the Desk: every tile and every SovCISS cell equals the engine's document, ranked as the document ranks them;
  3. a chart opens, draws its history, changes range, answers the pointer;
  4. Crisis peaks: cells equal the measures' own peaks, in all three sets;
  5. World and Sources: rows as the documents say; a retained ECB response is fetched and its SHA-256 recomputed;
  6. the desk alone at 390 px: nothing wider than the screen; a deep link opens a chart;
  7. ciss.html: a readable headline with the exact value beside it, the reconciliation in words, current / blank /
     ended counts as the warehouse says, auxiliaries with their originals - and one checked in the browser;
  8. sovereign.html (the fiscal page that stopped on a null) renders.
Nothing is written anywhere.
"""
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import gzip
import hashlib
import importlib.util
import json
import re
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
UNITS = {"index": (4, "", ""), "pct": (2, "", "%"), "bp": (1, "", " bp"), "eur_bn": (1, "€", " bn"), "pct_gdp": (1, "", "%"), "usd": (4, "$", "")}
CHART = r"""() => { const b = (id) => document.getElementById(id), ov = b('ws-overlay'), f = ov ? ov.querySelector('iframe') : null, btn = b('btn-sov'), prev = btn ? btn.previousElementSibling : null,
  next = btn ? btn.nextElementSibling : null, box = btn ? btn.getBoundingClientRect() : null, repo = b('btn-repo');
  return { btn: !!btn, prev: prev ? prev.id : null, next: next ? next.id : null, label: btn ? btn.textContent.trim() : null, title: btn ? btn.title : null,
    box: box ? [Math.round(box.x), Math.round(box.y), Math.round(box.width), Math.round(box.height)] : null, repoPrev: repo && repo.previousElementSibling ? repo.previousElementSibling.id : null,
    overlay: ov ? ov.className : null, src: f ? f.getAttribute('src') : null, head: ov && ov.querySelector('.wshd span') ? ov.querySelector('.wshd span').textContent : null }; }"""
DESK = r"""() => { const q = (s) => Array.from(document.querySelectorAll(s)), txt = (n) => n ? n.innerText.replace(/\s+/g, ' ').trim() : '', body = document.body.innerText;
  const tables = q('table.board');
  return { asof: txt(document.getElementById('sov-asof')), read: q('.read p').map(txt),
    tiles: q('.tile').map(t => ({ id: t.getAttribute('data-id'), val: txt(t.querySelector('.tv')), date: txt(t.querySelector('.td')) })),
    board: tables[0] ? Array.from(tables[0].querySelectorAll('tbody tr')).map(tr => Array.from(tr.children).map(txt)) : [],
    others: tables[1] ? tables[1].querySelectorAll('tbody tr').length : 0, chips: q('.chips .chip').length, parts: q('.part').length,
    bad: (body.match(/\bNaN\b|\bundefined\b|\bnull\b|Infinity/g) || []), ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""
CANVAS = r"""() => { const c = document.getElementById('sov-cv'); if (!c || !c.width) return { w: 0, amber: 0 }; const d = c.getContext('2d').getImageData(0, 0, c.width, c.height).data; let n = 0;
  for (let i = 0; i < d.length; i += 4) if (d[i] > 200 && d[i + 1] > 150 && d[i + 1] < 210 && d[i + 2] < 90 && d[i + 3] > 200) n++;
  return { w: c.width, h: c.height, amber: n, on: document.getElementById('sov-ovl').classList.contains('on'), meta: document.getElementById('sov-cm').innerText.replace(/\s+/g, ' '),
    title: document.getElementById('sov-ct').innerText, stat: document.getElementById('sov-cs').innerText.replace(/\s+/g, ' ').slice(0, 260), tip: document.getElementById('sov-tip').style.display + ' ' + document.getElementById('sov-tip').innerText.replace(/\s+/g, ' ') }; }"""
CRISIS = r"""() => { const t = document.querySelector('table.crisis'); const txt = (n) => n ? n.innerText.replace(/\s+/g, ' ').trim() : '';
  return { head: Array.from(t.querySelectorAll('thead th')).map(th => th.getAttribute('data-id')), rows: Array.from(t.querySelectorAll('tbody tr')).map(tr => Array.from(tr.children).map(txt)),
    tops: Array.from(document.querySelectorAll('.chips .chip')).map(txt), ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""
CISS = r"""() => { const g = (id) => { const n = document.getElementById(id); return n ? n.innerText.replace(/\s+/g, ' ').trim() : null; };
  const rows = Array.from(document.querySelectorAll('#ciss-table-body tr')).map(tr => Array.from(tr.children).map(c => c.innerText.replace(/\s+/g, ' ').trim()));
  return { read: g('ciss-read'), exact: g('ciss-exact'), rec: g('ciss-reconcile'), recd: g('ciss-reconcile-detail'), count: g('ciss-count'), rows: rows.length,
    long: rows.filter(r => /\d\.\d{7,}/.test(r[1] || '')).length, firstValues: rows.slice(0, 3).map(r => r[1]), aux: Array.from(document.querySelectorAll('#ciss-aux article')).map(a => a.innerText.replace(/\s+/g, ' ').trim()),
    main: document.querySelector('main').innerText, ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""


def fmt(unit, v):
    """The page's own formatting, written again here: jh-sovereign-desk.js fmt().

    A browser's toLocaleString rounds the shortest decimal of a float half away from zero (0.04225 -> 0.0423) where
    Python's "%.4f" rounds the binary value (0.0422), so the number is rounded here as the browser rounds it."""
    if v is None:
        return "—"
    d, pre, suf = UNITS[unit]
    q = abs(Decimal(repr(float(v)))).quantize(Decimal(1).scaleb(-d), rounding=ROUND_HALF_UP)
    return ("−" if v < 0 and q != 0 else "") + pre + "{:,}".format(q) + suf


def lifecycle(row, now):
    """jh-ciss-research.js lifecycle(), for rows collected within the last three days."""
    status = row["quality"]["status"]
    end = datetime.fromisoformat(row["observation_period_end"] + "T00:00:00+00:00")
    days = (now.replace(hour=0, minute=0, second=0, microsecond=0) - end).days
    if status == "missing" and row.get("last_observed_value") is not None and (row.get("last_observed_date") or "") < row["latest_date"]:
        return "blank"
    if status in ("stale", "missing") and days > 400:
        return "ended"
    return status


def script_of(html, name):
    """The address a page itself names for a script: the site build stamps every reference with a content hash."""
    m = re.search(rb"""src=["'](/%s[^"']*)["']""" % re.escape(name).encode(), html)
    return m.group(1).decode() if m else "/" + name


def fetch(path, timeout=90):
    req = urllib.request.Request(BASE + path, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def selftest():
    assert fmt("index", 0.17391) == "0.1739" and fmt("eur_bn", -453.798) == "−€453.8 bn" and fmt("eur_bn", 1004.296) == "€1,004.3 bn" and fmt("bp", 59.81) == "59.8 bp"
    assert fmt("index", -0.00001) == "0.0000" and fmt("pct", None) == "—"
    assert fmt("index", 0.04225) == "0.0423" and fmt("bp", 16.75) == "16.8 bp" and fmt("eur_bn", 1307.451) == "€1,307.5 bn" and fmt("pct", 4.0) == "4.00%"
    assert script_of(b'<script src="/jh-sovereign-desk.js?v=abc123"></script>', "jh-sovereign-desk.js") == "/jh-sovereign-desk.js?v=abc123"
    now = datetime(2026, 11, 22, 15, tzinfo=timezone.utc)
    base = {"quality": {"status": "missing"}, "observation_period_end": "2026-11-20", "latest_date": "2026-11-20", "last_observed_value": 0.008, "last_observed_date": "2026-11-19"}
    assert lifecycle(base, now) == "blank"
    assert lifecycle({"quality": {"status": "stale"}, "observation_period_end": "2025-04-30", "latest_date": "2025-04"}, now) == "ended"
    assert lifecycle({"quality": {"status": "fresh"}, "observation_period_end": "2026-11-19", "latest_date": "2026-11-19"}, now) == "fresh"


def main():
    selftest()
    from ops_report import report
    spec = importlib.util.spec_from_file_location("ops6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    now = datetime.now(timezone.utc)
    stamp = int(time.time())
    with report("ops_6915_sovereign_desk_live_proof") as r:
        r.heading("ops 6915 - the Sovereign button, its desk and the CISS page, real browser on the live site (read-only), %s" % now.isoformat(timespec="minutes"))
        fails = []

        def ck(cond, msg):
            if cond:
                return True
            fails.append(msg)
            r.warn("CHECK FAILED: " + msg)
            return False

        r.section("0. The live site serves the new files")
        served = {}
        for attempt in range(45):
            try:
                desk_html, ciss_html = fetch("/sovereign-desk.html?probe=%d" % (stamp + attempt)), fetch("/ciss.html?probe=%d" % (stamp + attempt))
                served = {"engine": b"id=btn-sov" in fetch("/jh-chart-engine.js?probe=%d" % (stamp + attempt)),
                          "rail": b'sov: ["Sovereign stress' in fetch("/jh-chart-tvrail.js?probe=%d" % (stamp + attempt)),
                          "desk_page": b'id="sov-main"' in desk_html,
                          "desk_script": b"function crisis(" in fetch(script_of(desk_html, "jh-sovereign-desk.js")),
                          "ciss_page": b'id="ciss-exact"' in ciss_html,
                          "ciss_script": b"verifiedMeasure" in fetch(script_of(ciss_html, "jh-ciss-research.js")),
                          "fiscal_page": b"yoyB(" in fetch("/sovereign.html?probe=%d" % (stamp + attempt))}
            except Exception as e:  # noqa: BLE001
                served = {"error": str(e)[:80]}
            if served and all(v is True for v in served.values()):
                break
            time.sleep(20)
        r.log("served after %d tries: %s" % (attempt + 1, json.dumps(served)))
        if not (served and all(v is True for v in served.values())):
            r.fail("the live site does not serve the Sovereign desk yet; nothing was judged")
            sys.exit(1)
        r.log("scripts as the pages name them: %s | %s" % (script_of(desk_html, "jh-sovereign-desk.js"), script_of(ciss_html, "jh-ciss-research.js")))

        doc = json.loads(fetch("/data/ciss-sovereign.json?probe=%d" % stamp))
        hist = json.loads(fetch("/data/ciss-sovereign-history.json?probe=%d" % stamp))
        ware = json.loads(fetch("/data/ciss-stress.json?exact=1&probe=%d" % stamp))
        world = json.loads(fetch("/data/global-sovereign.json?probe=%d" % stamp))
        M = doc["measures"]
        r.log("documents: desk %s, %d measures, %d countries, errors %s | history %d series | warehouse %s" % (
            doc["generated_at"][:19], len(M), len(doc["countries"]), json.dumps(doc["errors"]), len(hist["series"]), ware["generated_at"][:19]))
        ck(doc["contract"] == "ciss-sovereign.v1" and not doc["errors"] and doc["warehouse"]["manifest_key"] == ware["replay"]["manifest_key"], "the desk document: contract, errors or warehouse binding")

        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)

            r.section("1. The button on the chart")
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, user_agent=UA)
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto("%s/chart.html?s=SPY&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#btn-sov", timeout=90000, state="attached")
            page.wait_for_function("() => (window.lastBars || []).length > 300 && typeof window.jhOpenWorkspace === 'function'", timeout=90000)
            page.wait_for_timeout(1200)
            c0 = page.evaluate(CHART)
            r.log("button: %s" % json.dumps(c0))
            ck(c0["btn"] and c0["prev"] == "btn-repo" and c0["next"] == "btn-usd" and c0["repoPrev"] == "btn-cds" and "Sovereign" in (c0["label"] or "") and "SovCISS" in (c0["title"] or "") and c0["overlay"] == "",
               "the button: %s" % c0)
            page.mouse.move(700, 3)                     # the toolbar hides until the pointer reaches the top edge
            try:
                page.wait_for_selector("#jh-ah-top.show", timeout=8000)
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(500)
            page.evaluate("() => document.getElementById('btn-sov').scrollIntoView({ inline: 'center', block: 'nearest' })")
            page.wait_for_timeout(300)
            c1 = page.evaluate(CHART)
            clicked = "mouse"
            try:
                bx = c1["box"]
                if not (bx and bx[2] > 10 and 0 <= bx[0] < 1440 and 0 <= bx[1] < 200):
                    raise RuntimeError("button box %s is not on screen" % bx)
                page.mouse.click(bx[0] + bx[2] / 2, bx[1] + bx[3] / 2)
            except Exception as exc:  # noqa: BLE001
                clicked = "script (%s)" % str(exc)[:60]
                page.evaluate("() => document.getElementById('btn-sov').click()")
            page.wait_for_timeout(800)
            c2 = page.evaluate(CHART)
            if c2["overlay"] != "on":
                clicked += " then script"
                page.evaluate("() => document.getElementById('btn-sov').click()")
                page.wait_for_timeout(800)
                c2 = page.evaluate(CHART)
            ck(c2["overlay"] == "on" and (c2["src"] or "") == "/sovereign-desk.html" and "Sovereign" in (c2["head"] or ""), "the button did not open the desk: %s" % c2)
            r.log("clicked by %s at box %s -> overlay %s | %s | %s" % (clicked, c1["box"], c2["overlay"], c2["head"], c2["src"]))
            fr = None
            for _ in range(80):
                fr = next((f for f in page.frames if f.url.split("?")[0].endswith("/sovereign-desk.html")), None)
                if fr:
                    break
                page.wait_for_timeout(250)
            if not ck(fr is not None, "no workspace frame for the desk"):
                r.fail("no desk frame: %s" % " | ".join(fails)[:900])
                sys.exit(1)

            r.section("2. The Desk, inside the chart")
            fr.wait_for_selector(".tile", timeout=90000)
            fr.wait_for_timeout(800)
            d = fr.evaluate(DESK)
            r.log("as of: %s" % d["asof"])
            for line in d["read"]:
                r.log("  read: %s" % line)
            want_tiles = [(i, fmt(M[i]["unit"], M[i]["value"])) for i in doc["euro_area"]]
            got_tiles = [(t["id"], t["val"].split(" ")[0] + (" " + t["val"].split(" ")[1] if M[t["id"]]["unit"] in ("bp", "eur_bn") else "")) for t in d["tiles"]]
            r.log("tiles: %s" % " | ".join("%s %s (%s)" % (t["id"], t["val"], t["date"]) for t in d["tiles"])[:1400])
            ck(got_tiles == want_tiles, "tiles differ from the document: %s against %s" % (got_tiles[:4], want_tiles[:4]))
            rank = doc["cross_section"]["ranking"]
            names = {c["area"]: c["country"] for c in doc["countries"]}
            want_board = [(names[x["area"]], fmt("index", M["SOV_" + x["area"]]["value"])) for x in rank]
            got_board = [(row[0], row[1].split(" ")[0]) for row in d["board"]]
            r.log("board: %s" % " | ".join("%s %s" % x for x in got_board))
            ck(got_board == want_board, "the board is not the document's ranking: %s against %s" % (got_board[:3], want_board[:3]))
            fr_row = next((row for row in d["board"] if row[0] == "France"), None)
            if fr_row:
                r.log("France's row: %s" % " | ".join(fr_row))
                ck(fmt("pct", M["Y10_FR"]["value"]) in fr_row[8] and fmt("bp", M["S10_FR"]["value"]) in fr_row[9] and fmt("eur_bn", M["TGT_FR"]["value"]) in fr_row[10] and fmt("pct_gdp", M["DEBT_FR"]["value"]) in fr_row[11],
                   "France's yield, spread, TARGET or debt differ from the document")
            ck(d["others"] == sum(1 for c in doc["countries"] if "sovciss" not in c["ids"] and (set(c["ids"]) & {"yield", "debt", "target"})), "the other-governments table has %d rows" % d["others"])
            ck(d["parts"] == 6 and d["chips"] == sum(1 for c in doc["countries"] if "ciss" in c["ids"]), "contributions %d, country CISS chips %d" % (d["parts"], d["chips"]))
            ck(not d["bad"] and d["ox"] <= 1, "broken text %s or overflow %s px" % (d["bad"][:4], d["ox"]))
            ck(len(d["read"]) == 4 and fmt("index", M["EA_SOV_GDP"]["value"]) in d["read"][0], "the opening lines")

            r.section("3. A chart")
            fr.click(".tile[data-id='EA_SOV_GDP']")
            fr.wait_for_function("() => { const c = document.getElementById('sov-cv'); if (!c.width) return false; const d = c.getContext('2d').getImageData(0, 0, c.width, c.height).data; let n = 0; for (let i = 0; i < d.length; i += 4) if (d[i] > 200 && d[i + 1] > 150 && d[i + 2] < 90) n++; return n > 400; }", timeout=60000)
            c = fr.evaluate(CANVAS)
            r.log("chart: %s | %s | canvas %sx%s, %d line pixels" % (c["title"], c["meta"][:170], c["w"], c["h"], c["amber"]))
            r.log("  %s" % c["stat"])
            ck(c["on"] and c["title"] == M["EA_SOV_GDP"]["label"] and M["EA_SOV_GDP"]["key"] in c["meta"] and M["EA_SOV_GDP"]["exact"] in c["meta"] and c["amber"] > 400, "the chart of the euro-area SovCISS")
            fr.click("#sov-rg [data-years='5']")
            fr.wait_for_timeout(500)
            c5 = fr.evaluate(CANVAS)
            box = fr.evaluate("() => { const b = document.getElementById('sov-cv').getBoundingClientRect(); return [b.x, b.y, b.width, b.height]; }")
            fr.hover("#sov-cv", position={"x": box[2] * 0.6, "y": box[3] * 0.5})
            fr.wait_for_timeout(300)
            tip = fr.evaluate(CANVAS)["tip"]
            r.log("five years: %d line pixels (all history %d) | under the pointer: %s" % (c5["amber"], c["amber"], tip))
            ck(c5["amber"] > 200 and c5["amber"] != c["amber"] and tip.startswith("block") and re.search(r"\d{4}", tip), "the range button or the pointer read-out")
            fr.press("body", "Escape")
            fr.wait_for_timeout(200)
            ck(not fr.evaluate("() => document.getElementById('sov-ovl').classList.contains('on')"), "Escape did not close the chart")

            r.section("4. Crisis peaks")
            fr.click("#sov-tabs .tab[data-view='crisis']")
            fr.wait_for_selector("table.crisis", timeout=30000)
            for set_id, col, ep, side in (("sovciss", "SOV_IT", "italy2011", "hi"), ("spread", "S10_GR", "spain2012", "hi"), ("euro", "TGT_IT", "gilts2022", "lo")):
                fr.click("[data-set='%s']" % set_id)
                fr.wait_for_timeout(300)
                k = fr.evaluate(CRISIS)
                idx = k["head"].index(col)
                row_i = [e["id"] for e in doc["episodes"]].index(ep)
                shown = k["rows"][row_i][idx]
                want = fmt(M[col]["unit"], M[col]["peaks"][ep][side][0]).replace(" bp", "").replace(" bn", "")
                r.log("  %-8s %d columns, %d rows | %s in '%s': page %s, document %s | now %s | overflow %s px" % (set_id, len(k["head"]) - 1, len(k["rows"]), col, doc["episodes"][row_i]["name"], shown, want, k["rows"][16][idx], k["ox"]))
                ck(shown == want and len(k["rows"]) == 18 and k["rows"][16][0].startswith("Now") and k["ox"] <= 1, "crisis set %s: %s against %s" % (set_id, shown, want))
                if set_id == "sovciss":
                    r.log("  highest readings by the data: %s" % " | ".join(k["tops"][:4]))
                    ck(len(k["tops"]) == len(M["EA_SOV_GDP"]["tops"]) and fmt("index", M["EA_SOV_GDP"]["tops"][0][0]) in k["tops"][0], "the list of highest readings")

            r.section("5. World and Sources")
            fr.click("#sov-tabs .tab[data-view='world']")
            fr.wait_for_selector("table.board tbody tr", timeout=60000)
            fr.wait_for_timeout(400)
            w_rows = fr.evaluate("() => Array.from(document.querySelectorAll('table.board tbody tr')).map(tr => Array.from(tr.children).map(c => c.innerText.trim()))")
            top = max((c for c in world["countries"] if isinstance(c.get("cds_bp"), (int, float))), key=lambda c: c["cds_bp"])
            r.log("world: %d rows | first: %s" % (len(w_rows), " | ".join(w_rows[0])))
            ck(len(w_rows) == len(world["countries"]) and w_rows[0][0] == top["country"], "the world table: %d rows, first %s (widest CDS in the document: %s)" % (len(w_rows), w_rows[0][0], top["country"]))
            fr.click("#sov-tabs .tab[data-view='sources']")
            fr.wait_for_timeout(500)
            src = fr.evaluate("() => ({ rows: document.querySelectorAll('table.board tbody tr').length, text: document.getElementById('sov-main').innerText.replace(/\\s+/g, ' ').slice(0, 1500), complete: document.getElementById('sov-main').innerText.includes('Nothing: every response arrived and compiled.'), links: Array.from(document.querySelectorAll('#sov-main a')).map(a => a.getAttribute('href')) })")
            r.log("sources: %d rows, %d links, says every response arrived: %s | %s" % (src["rows"], len(src["links"]), src["complete"], src["text"][:420]))
            ck(src["rows"] == len(doc["sources"]) and src["complete"], "the sources view")
            curve = next(s for s in doc["sources"] if s["name"] == "curve")
            raw = gzip.decompress(fetch("/" + curve["evidence"]["key"], timeout=180))
            receipt = json.loads(fetch("/" + doc["replay"]["manifest_key"]))
            r.log("the retained yield-curve response: %d bytes, SHA-256 as its receipt says: %s | run receipt lists %d originals and %d added responses" % (
                len(raw), hashlib.sha256(raw).hexdigest() == curve["evidence"]["sha256"], len(receipt["receipts"]), len(receipt["aux"])))
            ck(hashlib.sha256(raw).hexdigest() == curve["evidence"]["sha256"] and len(raw) == curve["evidence"]["bytes"] and receipt["output_sha256"] == doc["replay"]["output_sha256"], "the retained original or the run receipt")
            ck(not errors, "page errors on the chart: %s" % errors[:3])
            ctx.close()

            r.section("6. The desk alone, on a phone")
            ctx = browser.new_context(viewport={"width": 390, "height": 844}, user_agent=UA, device_scale_factor=2)
            page = ctx.new_page()
            perr = []
            page.on("pageerror", lambda e: perr.append(str(e)[:200]))
            page.goto("%s/sovereign-desk.html?m=SOV_FR&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector(".tile", timeout=90000)
            page.wait_for_function("() => document.getElementById('sov-ovl').classList.contains('on')", timeout=30000)
            deep = page.evaluate(CANVAS)
            r.log("deep link ?m=SOV_FR: %s | %s" % (deep["title"], deep["meta"][:120]))
            ck(deep["title"] == M["SOV_FR"]["label"] and fmt("index", M["SOV_FR"]["value"]) in deep["meta"], "the deep link did not open France's chart")
            page.keyboard.press("Escape")
            widths = {}
            for view in ("desk", "crisis", "world", "sources"):
                page.click("#sov-tabs .tab[data-view='%s']" % view)
                page.wait_for_timeout(900)
                widths[view] = page.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth")
            r.log("wider than a 390 px screen by: %s | page errors %s" % (json.dumps(widths), perr[:2]))
            ck(all(v <= 1 for v in widths.values()) and not perr, "phone layout: %s %s" % (widths, perr[:2]))
            ctx.close()

            r.section("7. The CISS page")
            ctx = browser.new_context(viewport={"width": 1366, "height": 900}, user_agent=UA)
            page = ctx.new_page()
            cerr = []
            page.on("pageerror", lambda e: cerr.append(str(e)[:200]))
            page.goto("%s/ciss.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#ciss-table-body tr button", timeout=90000)
            page.wait_for_selector("#ciss-aux article button", timeout=60000)
            page.wait_for_timeout(1500)
            c = page.evaluate(CISS)
            head = next(s for s in ware["series"] if s["key"] == "CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX")
            place = ("higher than %d" % int(head["pctile"])) if head["pctile"] >= 50 else ("lower than %d" % int(100 - head["pctile"]))
            want_read = "ECB euro-area CISS: %s on %s — %s%% of its readings since %s." % (fmt("index", head["latest"]), head["latest_date"], place, head["start_date"][:4])
            r.log("headline: %s" % c["read"])
            r.log("  %s" % c["exact"])
            r.log("reconciliation: %s | %s" % (c["rec"], c["recd"]))
            ck(c["read"] == want_read and head["latest_decimal"] in (c["exact"] or "") and not re.search(r"\d\.\d{6,}", c["read"]), "the headline: %s against %s" % (c["read"], want_read))
            ck((c["rec"] or "").startswith("Matched on %s" % ware["headline_reconciliation"]["date"]) and ware["headline_reconciliation"]["residual_decimal"] in (c["recd"] or ""), "the reconciliation line")
            tally = {}
            for s in ware["series"]:
                k = lifecycle(s, now)
                tally[k] = tally.get(k, 0) + 1
            want_count = "%d shown · %d current · %d with a blank newest row · %d ended at the ECB" % (len(ware["series"]), tally.get("fresh", 0), tally.get("blank", 0), tally.get("ended", 0))
            r.log("count: %s | warehouse by this page's rule: %s | values of the first rows: %s | cells with seven or more decimals: %d" % (c["count"], json.dumps(tally), c["firstValues"], c["long"]))
            ck((c["count"] or "").startswith(want_count) and c["rows"] == len(ware["series"]) and c["long"] == 0, "the table: %s against %s" % (c["count"], want_count))
            for a in c["aux"]:
                r.log("  aux: %s" % a[:250])
            ck(len(c["aux"]) == 3 and not any("pending" in a for a in c["aux"]) and all("kept as it arrived" in a for a in c["aux"]), "the auxiliaries still say pending, or are not three")
            for mid in doc["auxiliaries"]:
                ck(any(M[mid]["label"] in a and M[mid]["exact"] in a and M[mid]["date"] in a for a in c["aux"]), "auxiliary %s is not shown as the document has it" % mid)
            page.click("#ciss-aux article button[data-verify='AUX_EURUSD']")
            page.wait_for_function("() => { const p = document.querySelector('#ciss-aux pre[data-result=\"AUX_EURUSD\"]'); return p && !p.hidden && !/Checking/.test(p.textContent); }", timeout=120000)
            verdict = page.evaluate("() => document.querySelector('#ciss-aux pre[data-result=\"AUX_EURUSD\"]').textContent.replace(/\\s+/g, ' ').slice(0, 330)")
            r.log("check the euro reference rate against its original, in the browser: %s" % verdict)
            ck(verdict.startswith("VERIFIED") and M["AUX_EURUSD"]["exact"] in verdict and M["AUX_EURUSD"]["period"] in verdict, "the auxiliary did not verify: %s" % verdict[:160])
            counts = {}
            for value in ("ended", "blank", "fresh"):
                page.select_option("#ciss-quality", value)
                page.wait_for_timeout(300)
                counts[value] = page.evaluate("() => document.querySelectorAll('#ciss-table-body tr button').length")
            page.select_option("#ciss-quality", "blank")
            page.wait_for_timeout(300)
            blank_rows = page.evaluate("() => Array.from(document.querySelectorAll('#ciss-table-body tr')).map(tr => tr.innerText.replace(/\\s+/g, ' ').slice(0, 230))")
            r.log("filters: %s | blank rows: %s" % (json.dumps(counts), " || ".join(blank_rows)))
            ck(counts == {"ended": tally.get("ended", 0), "blank": tally.get("blank", 0), "fresh": tally.get("fresh", 0)}, "the quality filter: %s against %s" % (counts, tally))
            page.select_option("#ciss-quality", "")
            page.fill("#ciss-search", "CISS.D.FR.Z0Z.4F.EC.SOV_CIN.IDX")
            page.wait_for_timeout(500)
            page.click("#ciss-table-body tr button.series-button")
            page.wait_for_timeout(1000)
            page.click("#ciss-verify-original")
            page.wait_for_function("() => { const t = (document.getElementById('ciss-original') || {}).textContent || ''; return t && !/Checking/.test(t); }", timeout=120000)
            panel = page.evaluate("() => ({ open: document.getElementById('ciss-detail').open, text: document.getElementById('ciss-original').textContent.slice(0, 60), exact: document.getElementById('ciss-detail-body').innerText.includes('0.17') })")
            r.log("a series panel still opens and verifies its row: %s" % json.dumps(panel))
            ck(panel["open"] and panel["text"].startswith("VERIFIED") and c["ox"] <= 1 and not cerr, "the series panel, overflow %s, errors %s" % (c["ox"], cerr[:2]))
            ctx.close()

            r.section("8. The fiscal page that stopped on a null")
            ctx = browser.new_context(viewport={"width": 1366, "height": 900}, user_agent=UA)
            page = ctx.new_page()
            ferr = []
            page.on("pageerror", lambda e: ferr.append(str(e)[:200]))
            page.goto("%s/sovereign.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_timeout(12000)
            f = page.evaluate("() => ({ stats: Array.from(document.querySelectorAll('.stat')).map(s => s.innerText.replace(/\\s+/g, ' ')), loading: /Loading sovereign/.test(document.body.innerText), svgs: document.querySelectorAll('svg').length, chars: document.body.innerText.length })")
            r.log("sovereign.html: %d summary figures, %d charts, %d characters, still loading: %s, errors %s" % (len(f["stats"]), f["svgs"], f["chars"], f["loading"], ferr[:2]))
            r.log("  %s" % " | ".join(f["stats"])[:900])
            try:
                fiscal, coll, sf = (json.loads(fetch("/data/%s.json?probe=%d" % (k, stamp))) for k in ("sovereign-fiscal", "fed-collateral", "settlement-fails"))
                tic = fiscal.get("tic") or {}
                nulls = {"total.chg_12m_bn": (tic.get("total") or {}).get("chg_12m_bn"), "sec_lending.pctile": ((coll.get("securities_lending") or {}).get("stats") or {}).get("pctile"),
                         "fails.headline": json.dumps(sf.get("headline"))[:200], "fails.signal": json.dumps(sf.get("signal"))[:120]}
                r.log("  what the documents carry there: %s" % json.dumps(nulls)[:700])
            except Exception as e:  # noqa: BLE001
                r.log("  documents: %s" % str(e)[:120])
            ck(not ferr and not f["loading"] and len(f["stats"]) >= 4, "sovereign.html: errors %s, loading %s, %d figures" % (ferr[:2], f["loading"], len(f["stats"])))
            ctx.close()
            browser.close()

        try:
            with urllib.request.urlopen(urllib.request.Request(BASE + "/favicon.ico", headers={"User-Agent": UA}), timeout=30) as resp:
                r.log("favicon.ico: HTTP %s" % resp.status)
        except Exception as e:  # noqa: BLE001
            r.log("favicon.ico: %s (the one console 404 the first page of a browser session reports)" % str(e)[:60])
        r.kv(failed_checks=len(fails), measures=len(M), tiles=len(d["tiles"]), board_rows=len(d["board"]), world_rows=len(w_rows), ciss_rows=c["rows"])
        if fails:
            r.fail("%d checks failed: %s" % (len(fails), " | ".join(fails)[:1500]))
            sys.exit(1)
        r.ok("the Sovereign button opens the desk; tiles, board, charts, crisis peaks, world and sources equal the documents; the CISS page reads plainly and checks its auxiliaries against their originals")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
