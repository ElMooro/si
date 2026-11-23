"""ops 6915 - LIVE PROOF (read-only, re-runnable): the Sovereign button, the desk it opens, and the repaired CISS page.

A real browser on justhodl.ai, judged against the documents the pages read:
  1. chart.html: the Sovereign button sits right after Repo; a mouse click opens the desk in the chart's overlay;
  2. the Desk: every tile and every SovCISS cell equals the engine's document, ranked as the document ranks them;
     the United States, China and the United Kingdom have their CISS; the six ended SovCISS countries are listed;
     colour tags move a country to the top of every table and any column sorts both ways; a country's name opens
     its chart;
  3. a chart opens, draws its history, changes range, answers the pointer;
  4. Crisis peaks: every set draws all its series over their whole history; cells equal the measures' own peaks and
     open that series' chart around the window;
  5. World and Sources: rows as the documents say; the World tab's chart draws every country of each of its four sets,
     "Only tagged" keeps to the tagged countries as they are tagged, a cell opens that country's history; a retained
     ECB response is fetched and its SHA-256 recomputed;
  5c. the Dollar shortage tab: the three ECB measures first, under the ECB's own titles, as tiles and as charts (Khalid
     18:05 ET: "why did you deleted ... add them back"); then every measure of the document in one table, equal to the
     document; each chart draws every observation; the windows table holds the low and high of each of the three and
     each Federal Reserve line a shortage shows in; a tile, a cell, a row and a chart's name each open that measure's
     own chart; the charts of "The price of the dollar" and "Foreign official dollars at the Federal Reserve" open as
     percent change from a year ago, by FRED's formula, recomputed here from the document (Khalid 20:07 ET), the Level
     button brings the level back, and the larger chart opens in the same units;
  6. the desk alone at 390 px: nothing wider than the screen; a deep link opens a chart; tags survive a reload;
  7. ciss.html: a readable headline with the exact value beside it, the reconciliation in words, current / blank /
     ended counts as the warehouse says, auxiliaries with their originals - and one checked in the browser;
  8. sovereign.html (the fiscal page that stopped on a null) renders.
Nothing is written anywhere.
"""
from datetime import date, datetime, timedelta, timezone
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
UNITS = {"index": (4, "", ""), "pct": (2, "", "%"), "bp": (1, "", " bp"), "eur_bn": (1, "€", " bn"), "pct_gdp": (1, "", "%"), "usd": (4, "$", ""), "usd_bn": (1, "$", " bn"), "idx": (2, "", ""), "yoy": (1, "", "%")}
YOY_PARTS = ("price", "official")          # the parts whose charts open as percent change from a year ago
SMALL = {"usd_bn": 3}           # an amount under ten is read to the million, unless it stands on a wide axis
ECB3 = ["AUX_EURUSD", "AUX_FX_EA", "AUX_FX_NONEA"]
DESK_FAMILY = ["AUX_EURUSD", "AUX_FX_EA", "AUX_FX_NONEA", "FED_SWAPS", "FED_FIMA", "FED_CUSTODY_UST", "FED_FOREIGN_RRP", "USD_TED"]      # the Dollar desk's "Dollar shortage" cards, the three ECB measures first
ECB_TITLES = ["EUR/USD reference rate", "Claims on euro-area residents in foreign currency", "Claims on non-euro-area residents in foreign currency"]
CHART = r"""() => { const b = (id) => document.getElementById(id), ov = b('ws-overlay'), f = ov ? ov.querySelector('iframe') : null, btn = b('btn-sov'), prev = btn ? btn.previousElementSibling : null,
  next = btn ? btn.nextElementSibling : null, box = btn ? btn.getBoundingClientRect() : null, repo = b('btn-repo');
  return { btn: !!btn, prev: prev ? prev.id : null, next: next ? next.id : null, label: btn ? btn.textContent.trim() : null, title: btn ? btn.title : null,
    box: box ? [Math.round(box.x), Math.round(box.y), Math.round(box.width), Math.round(box.height)] : null, repoPrev: repo && repo.previousElementSibling ? repo.previousElementSibling.id : null,
    overlay: ov ? ov.className : null, src: f ? f.getAttribute('src') : null, head: ov && ov.querySelector('.wshd span') ? ov.querySelector('.wshd span').textContent : null }; }"""
DESK = r"""() => { const q = (s) => Array.from(document.querySelectorAll(s)), txt = (n) => n ? n.innerText.replace(/\s+/g, ' ').trim() : '', body = document.body.innerText;
  const tiles = (sel) => q(sel).map(t => ({ id: t.getAttribute('data-id'), val: txt(t.querySelector('.tv')), date: txt(t.querySelector('.td')) }));
  const table = (id) => q(`table[data-table='${id}'] tbody tr`).map(tr => Array.from(tr.children).map(txt));
  return { asof: txt(document.getElementById('sov-asof')), read: q('.read p').map(txt), tiles: tiles('.tiles:not(#sov-world-tiles) .tile'), world: tiles('#sov-world-tiles .tile'),
    board: table('sovereign'), others: table('others'), ended: table('ended'), ciss: table('ciss'), heads: q("table[data-table='sovereign'] thead th").map(h => h.getAttribute('data-sort')), parts: q('.part').length,
    bad: (body.match(/\bNaN\b|\bundefined\b|\bnull\b|Infinity/g) || []), ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""
ORDER = r"""(id) => Array.from(document.querySelectorAll(`table[data-table='${id}'] tbody tr`)).map(tr => tr.getAttribute('data-key'))"""
PIXELS = r"""(sel) => { const c = document.querySelector(sel); if (!c || !c.width) return 0; const d = c.getContext('2d').getImageData(0, 0, c.width, c.height).data; let n = 0;
  for (let i = 0; i < d.length; i += 4) if (d[i + 3] > 150 && d[i] + d[i + 1] + d[i + 2] > 330) n++; return n; }"""
CANVAS = r"""() => { const c = document.getElementById('sov-cv'); if (!c || !c.width) return { w: 0, amber: 0 }; const d = c.getContext('2d').getImageData(0, 0, c.width, c.height).data; let n = 0;
  for (let i = 0; i < d.length; i += 4) if (d[i] > 200 && d[i + 1] > 150 && d[i + 1] < 210 && d[i + 2] < 90 && d[i + 3] > 200) n++;
  return { w: c.width, h: c.height, amber: n, on: document.getElementById('sov-ovl').classList.contains('on'), meta: document.getElementById('sov-cm').innerText.replace(/\s+/g, ' '),
    title: document.getElementById('sov-ct').innerText, stat: document.getElementById('sov-cs').innerText.replace(/\s+/g, ' ').slice(0, 260), tip: document.getElementById('sov-tip').style.display + ' ' + document.getElementById('sov-tip').innerText.replace(/\s+/g, ' ') }; }"""
CRISIS = r"""() => { const t = document.querySelector('table.crisis'); const txt = (n) => n ? n.innerText.replace(/\s+/g, ' ').trim() : '';
  const row = (tr) => Array.from(tr.children).map(txt), ov = document.getElementById('sov-ov');
  return { head: Array.from(t.querySelectorAll('thead th')).map(th => th.getAttribute('data-id')), eps: Array.from(t.querySelectorAll('tbody tr[data-ep]')).map(tr => tr.getAttribute('data-ep')),
    rows: Object.fromEntries(Array.from(t.querySelectorAll('tbody tr[data-ep]')).map(tr => [tr.getAttribute('data-ep'), row(tr)])), now: row(t.querySelector('tr.nowrow')), record: row(t.querySelector('tr.recrow')),
    tops: Array.from(document.querySelectorAll('#sov-main .chips.tops .chip')).map(txt), lines: ov ? +ov.getAttribute('data-lines') : null, legend: document.querySelectorAll('#sov-leg .lg').length,
    minis: document.querySelectorAll('.mini').length, ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""
CISS = r"""() => { const g = (id) => { const n = document.getElementById(id); return n ? n.innerText.replace(/\s+/g, ' ').trim() : null; };
  const rows = Array.from(document.querySelectorAll('#ciss-table-body tr')).map(tr => Array.from(tr.children).map(c => c.innerText.replace(/\s+/g, ' ').trim()));
  return { read: g('ciss-read'), exact: g('ciss-exact'), rec: g('ciss-reconcile'), recd: g('ciss-reconcile-detail'), count: g('ciss-count'), rows: rows.length,
    long: rows.filter(r => /\d\.\d{7,}/.test(r[1] || '')).length, firstValues: rows.slice(0, 3).map(r => r[1]), aux: Array.from(document.querySelectorAll('#ciss-aux article')).map(a => a.innerText.replace(/\s+/g, ' ').trim()),
    main: document.querySelector('main').innerText, ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""


def fmt(unit, v, wide=False):
    """The page's own formatting, written again here: jh-sovereign-desk.js fmt().

    A browser's toLocaleString rounds the shortest decimal of a float half away from zero (0.04225 -> 0.0423) where
    Python's "%.4f" rounds the binary value (0.0422), so the number is rounded here as the browser rounds it."""
    if v is None:
        return "—"
    d, pre, suf = UNITS[unit]
    if unit in SMALL and not wide and v != 0 and abs(v) < 10:
        d = SMALL[unit]
    q = abs(Decimal(repr(float(v)))).quantize(Decimal(1).scaleb(-d), rounding=ROUND_HALF_UP)
    return ("−" if v < 0 and q != 0 else "") + pre + "{:,}".format(q) + suf


def yoy(series):
    """FRED's percent change from a year ago, written again here: ((x(t) / x(t - n_obs_per_yr)) - 1) * 100 with 260
    observations a year for a daily series and 52 for a weekly one - on FRED's grid the same day of the week 52 weeks
    (364 days) before.  [(date, percent)]; a day whose year-ago day has no reading has no figure."""
    at = dict(zip(series["d"], series["v"]))
    out = []
    for d, x in zip(series["d"], series["v"]):
        was = at.get((date.fromisoformat(d) - timedelta(days=364)).isoformat())
        if x is not None and was not in (None, 0):
            out.append((d, (x / was - 1) * 100))
    return out


def fmt_yoy(v):
    """jh-sovereign-desk.js fmtChange("yoy", v): a sign, one decimal, a percent sign."""
    q = Decimal(repr(float(v))).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    return ("" if q == 0 else "+" if v > 0 else "−") + "%s%%" % abs(q)


def dside(m):
    """The end of a window a dollar shortage shows a measure in: jh-sovereign-desk.js dollarSide()."""
    return {"higher": "hi", "lower": "lo"}.get(m.get("worse"))


def dunit(m):
    return "index" if m["unit"] == "usd" else m["unit"]


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
    assert fmt("usd_bn", 0.146) == "$0.146 bn" and fmt("usd_bn", 2544.011) == "$2,544.0 bn" and fmt("usd_bn", 0.146, True) == "$0.1 bn" and fmt("usd_bn", 0) == "$0.0 bn" and fmt("usd_bn", -615.423) == "−$615.4 bn"
    assert fmt("idx", 103.3578) == "103.36" and fmt("usd_bn", 60.0) == "$60.0 bn" and fmt("usd_bn", -42.16) == "−$42.2 bn"
    assert yoy({"d": ["2025-11-19", "2025-11-26", "2026-11-11", "2026-11-18"], "v": [2737.528, 2700.0, 2600.0, 2544.011]}) == [("2026-11-18", (2544.011 / 2737.528 - 1) * 100)]
    assert yoy({"d": ["2025-01-01", "2025-12-31"], "v": [0.0, 5.0]}) == [] and yoy({"d": [], "v": []}) == []
    assert fmt_yoy(-7.069) == "−7.1%" and fmt_yoy(4.0742) == "+4.1%" and fmt_yoy(0.04) == "0.0%" and fmt_yoy(12.65) == "+12.7%" and fmt("yoy", -13.6) == "−13.6%"
    assert dside({"worse": "higher"}) == "hi" and dside({"worse": "lower"}) == "lo" and dside({"worse": None}) is None and dunit({"unit": "usd"}) == "index" and dunit({"unit": "usd_bn"}) == "usd_bn"
    assert script_of(b'<script src="/jh-sovereign-desk.js?v=abc123"></script>', "jh-sovereign-desk.js") == "/jh-sovereign-desk.js?v=abc123"
    assert script_of(b'<script src="/assets/jh-workspaces.js?v=9f"></script>', "assets/jh-workspaces.js") == "/assets/jh-workspaces.js?v=9f"      # a script in a folder is named with its folder
    assert script_of(b'<script src="/assets/jh-workspaces.js"></script>', "assets/jh-workspaces.js") == "/assets/jh-workspaces.js"
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
                          "desk_page": b'data-view="dollar"' in desk_html,                             # the page of the fourth round: the Dollar shortage tab
                          "desk_script": all(mark in fetch(script_of(desk_html, "jh-sovereign-desk.js")) for mark in (b"id=sov-dboard", b"id=sov-dtiles", b"DOLLAR_GLOSS", b"data-dunits=yoy")),       # the three ECB measures lead the tab again, above the table of every measure
                          "dollar_desk": "Claims on non-euro-area residents in foreign currency · Eurosystem, weekly".encode() in fetch(script_of(fetch("/dollar-indexes.html?nocache=%d" % (stamp + attempt)), "assets/jh-workspaces.js")),    # the desks' bundle lives under /assets
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
        wh = json.loads(fetch("/data/ciss-sovereign-world.json?probe=%d" % stamp))
        ds = json.loads(fetch("/data/ciss-dollar-shortage.json?probe=%d" % stamp))
        M = doc["measures"]
        ck(ds.get("contract") == "ciss-dollar-shortage.v1" and ds.get("ecb") == ECB3 and ds["desk"]["generated_at"] == doc["generated_at"]
           and all(ds["measures"][k]["value"] == M[k]["value"] and ds["measures"][k]["date"] == M[k]["date"] and len(ds["series"][k]["d"]) == M[k]["n"] for k in ECB3),
           "the dollar-shortage document does not stand on today's desk document")
        fedm = [k for k in ds["order"] if ds["measures"][k].get("source") == "fred"]
        r.log("dollar-shortage document %s, generated %s: %d measures (%d Federal Reserve) in %d parts, %d observations, errors %s" % (
            ds.get("version"), ds["generated_at"][:19], len(ds["measures"]), len(fedm), len(ds.get("parts") or []), ds["coverage"]["observations"], json.dumps(ds.get("errors"))[:200]))
        ck(ds.get("version", "") >= "1.1.0" and len(ds["measures"]) == 15 and len(fedm) == 12 and sorted(ds["order"]) == sorted(ds["measures"]) and ds["order"] == [i for part in ds["parts"] for i in part["ids"]]
           and all(len(ds["series"][k]["d"]) == len(ds["series"][k]["v"]) == ds["measures"][k]["n"] and ds["series"][k]["d"][-1] == ds["measures"][k]["date"] and ds["series"][k]["v"][-1] == ds["measures"][k]["value"] for k in ds["order"])
           and [k for k in ds["order"] if ds["measures"][k]["status"] != "current"] == ["USD_TED"] and all(i in ds["measures"] for i in DESK_FAMILY),
           "the dollar-shortage document is not the fifteen measures it should be: %s" % ds["order"])
        if ds.get("errors"):
            r.warn("the dollar-shortage document names a source it could not read today (the series kept from before is used): %s" % json.dumps(ds["errors"])[:300])
        ck(wh.get("contract") == "sovereign-world.v1" and not wh.get("errors") and len(wh["countries"]) == len(world["countries"]), "the World tab's histories: contract, errors or countries (%s, %s, %d)" % (
            wh.get("contract"), json.dumps(wh.get("errors"))[:120], len(wh.get("countries") or [])))
        r.log("documents: desk %s, %d measures, %d countries, errors %s | history %d series | warehouse %s" % (
            doc["generated_at"][:19], len(M), len(doc["countries"]), json.dumps(doc["errors"]), len(hist["series"]), ware["generated_at"][:19]))
        ck(doc["contract"] == "ciss-sovereign.v1" and not doc["errors"] and doc["warehouse"]["manifest_key"] == ware["replay"]["manifest_key"], "the desk document: contract, errors or warehouse binding")
        ck(doc.get("version", "") >= "1.1.0" and len(doc["episodes"]) == 22 and doc.get("world_ciss") == ["CISS_US", "CISS_CN", "CISS_GB"], "the desk document is not the extended one: version %s" % doc.get("version"))
        names = {c["area"]: c["country"] for c in doc["countries"]}
        key_of = lambda area: names[area].lower()
        wj = {("czechia" if c["country"].lower() == "czech republic" else c["country"].lower()): c for c in world["countries"]}

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
            fr.wait_for_function("() => /bp/.test((document.querySelector(\"table[data-table='sovereign'] tbody tr\") || {}).innerText || '') && document.querySelectorAll(\"table[data-table='sovereign'] tbody tr td\").length > 100", timeout=60000)
            fr.wait_for_timeout(1500)                    # the world document (CDS, ratings) arrives after the first paint
            fr.evaluate("() => { try { localStorage.removeItem('jh.sovereign-desk.v1'); } catch (e) {} }")
            d = fr.evaluate(DESK)
            r.log("as of: %s" % d["asof"])
            for line in d["read"]:
                r.log("  read: %s" % line)
            two = lambda t_: t_["val"].split(" ")[0] + (" " + t_["val"].split(" ")[1] if M[t_["id"]]["unit"] in ("bp", "eur_bn") else "")
            want_tiles = [(i, fmt(M[i]["unit"], M[i]["value"])) for i in doc["euro_area"]]
            got_tiles = [(t_["id"], two(t_)) for t_ in d["tiles"]]
            r.log("tiles: %s" % " | ".join("%s %s (%s)" % (t_["id"], t_["val"], t_["date"]) for t_ in d["tiles"])[:1400])
            ck(got_tiles == want_tiles, "tiles differ from the document: %s against %s" % (got_tiles[:4], want_tiles[:4]))
            r.log("outside the euro area: %s" % " | ".join("%s %s" % (t_["id"], t_["val"]) for t_ in d["world"]))
            ck([(t_["id"], two(t_)) for t_ in d["world"]] == [(i, fmt("index", M[i]["value"])) for i in doc["world_ciss"]], "the United States / China / United Kingdom CISS tiles differ from the document")
            rank = doc["cross_section"]["ranking"]
            want_board = [(names[x["area"]], fmt("index", M["SOV_" + x["area"]]["value"])) for x in rank]
            got_board = [(row[0], row[1].split(" ")[0]) for row in d["board"]]
            r.log("board: %s" % " | ".join("%s %s" % x for x in got_board))
            ck(got_board == want_board, "the board is not the document's ranking: %s against %s" % (got_board[:3], want_board[:3]))
            col = {h: i for i, h in enumerate(d["heads"]) if h}
            fr_row = next((row for row in d["board"] if row[0] == "France"), None)
            if ck(fr_row is not None, "no row for France"):
                r.log("France's row: %s" % " | ".join(fr_row))
                want_fr = {"ciss": fmt("index", M["CISS_FR"]["value"]), "yield": fmt("pct", M["Y10_FR"]["value"]), "spread": fmt("bp", M["S10_FR"]["value"]), "target": fmt("eur_bn", M["TGT_FR"]["value"]),
                           "debt": fmt("pct_gdp", M["DEBT_FR"]["value"]), "deficit": fmt("pct_gdp", M["DEF_FR"]["value"]), "interest": fmt("pct_gdp", M["INT_FR"]["value"]), "clifs": fmt("index", M["CLIFS_FR"]["value"]),
                           "cds": "{:,}".format(Decimal(repr(float(wj["france"]["cds_bp"]))).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)) + " bp", "rating": wj["france"]["rating"]}
                off = {k: (fr_row[col[k]], v) for k, v in want_fr.items() if v not in fr_row[col[k]]}
                ck(not off, "France's row differs from the documents: %s" % off)
            ck(len(d["others"]) == sum(1 for c in doc["countries"] if "sovciss" not in c["ids"] and (set(c["ids"]) & {"yield", "debt", "target"})), "the other-governments table has %d rows" % len(d["others"]))
            ended = sorted((c for c in doc["countries"] if "sovciss_ended" in c["ids"]), key=lambda c: -M[c["ids"]["sovciss_ended"]]["value"])
            r.log("SovCISS the ECB stopped: %s" % " | ".join("%s %s" % (row[0], row[1]) for row in d["ended"]))
            ck([(row[0], row[1]) for row in d["ended"]] == [(c["country"], fmt("index", M[c["ids"]["sovciss_ended"]]["value"])) for c in ended] and len(ended) == 6, "the ended-SovCISS table differs from the document")
            ciss_rows = sorted((c for c in doc["countries"] if "ciss" in c["ids"]), key=lambda c: -M[c["ids"]["ciss"]]["value"])
            r.log("CISS by country: %s" % " | ".join("%s %s (%s)" % (row[0], row[1], row[2]) for row in d["ciss"]))
            ck([(row[0], row[1]) for row in d["ciss"]] == [(c["country"], fmt("index", M[c["ids"]["ciss"]]["value"])) for c in ciss_rows], "the CISS table differs from the document")
            ck({"United States", "China", "United Kingdom"} <= {row[0] for row in d["ciss"]}, "the United States, China or the United Kingdom is missing from the CISS table")
            ck(d["parts"] == 6 and not d["bad"] and d["ox"] <= 1, "contributions %d, broken text %s, overflow %s px" % (d["parts"], d["bad"][:4], d["ox"]))
            ck(len(d["read"]) == 6 and fmt("index", M["EA_SOV_GDP"]["value"]) in d["read"][0] and fmt("index", M["CISS_US"]["value"]) in d["read"][5], "the opening lines")

            r.section("2b. Tags and sorting")
            base = fr.evaluate(ORDER, "sovereign")
            for key, colour in (("italy", "red"), ("greece", "amber")):
                fr.click("table[data-table='sovereign'] .tg[data-tag='%s']" % key)
                fr.wait_for_selector("#sov-pal [data-colour='%s']" % colour, state="visible", timeout=10000)
                fr.click("#sov-pal [data-colour='%s']" % colour)
                fr.wait_for_timeout(300)
            tagged = fr.evaluate(ORDER, "sovereign")
            rest = [k for k in base if k not in ("greece", "italy")]
            r.log("before: %s" % " ".join(base))
            r.log("Italy tagged red, Greece amber: %s | CISS table starts %s" % (" ".join(tagged), fr.evaluate(ORDER, "ciss")[:2]))
            ck(tagged == ["greece", "italy"] + rest and fr.evaluate(ORDER, "ciss")[0] == "italy", "tagged countries did not move to the top: %s" % tagged[:4])
            cval = lambda key: next((M[c["ids"]["ciss"]]["value"] for c in doc["countries"] if c["country"].lower() == key and "ciss" in c["ids"]), None)
            have, none = [k for k in rest if cval(k) is not None], [k for k in rest if cval(k) is None]
            fr.click("table[data-table='sovereign'] th[data-sort='ciss']")
            fr.wait_for_timeout(300)
            down = fr.evaluate(ORDER, "sovereign")
            fr.click("table[data-table='sovereign'] th[data-sort='ciss']")
            fr.wait_for_timeout(300)
            up = fr.evaluate(ORDER, "sovereign")
            r.log("sorted by CISS, high to low: %s" % " ".join(down))
            r.log("sorted by CISS, low to high: %s" % " ".join(up))
            ck(down == ["greece", "italy"] + sorted(have, key=lambda k: -cval(k)) + none and up == ["greece", "italy"] + sorted(have, key=cval) + none, "sorting by CISS: %s / %s" % (down, up))
            kept = fr.evaluate("() => { try { return localStorage.getItem('jh.sovereign-desk.v1'); } catch (e) { return 'no storage: ' + e.message; } }")
            r.log("kept in the browser: %s" % kept)
            ck(kept and '"italy":"red"' in kept and '"greece":"amber"' in kept, "the tags were not kept: %s" % kept)
            fr.click("[data-show='tagged']")
            fr.wait_for_timeout(300)
            only = fr.evaluate(ORDER, "sovereign")
            fr.click("[data-show='all']")
            fr.wait_for_timeout(200)
            ck(only == ["greece", "italy"], "showing only tagged countries: %s" % only)
            fr.click("table[data-table='sovereign'] .cn[data-id='SOV_FR']")
            fr.wait_for_function("(sel) => { const c = document.querySelector(sel); if (!c || !c.width) return false; const d = c.getContext('2d').getImageData(0, 0, c.width, c.height).data; let n = 0; for (let i = 0; i < d.length; i += 4) if (d[i] > 200 && d[i + 1] > 150 && d[i + 2] < 90) n++; return n > 400; }", arg="#sov-cv", timeout=60000)
            named = fr.evaluate("() => document.getElementById('sov-ct').innerText")
            r.log("clicking the country name France opens: %s" % named)
            ck(named == M["SOV_FR"]["label"], "the country name did not open France's SovCISS chart: %s" % named)
            fr.press("body", "Escape")
            fr.click("table[data-table='ciss'] .cn[data-id='CISS_US']")
            fr.wait_for_timeout(900)
            us = fr.evaluate(CANVAS)
            r.log("clicking United States opens: %s | %s" % (us["title"], us["meta"][:110]))
            ck(us["title"] == M["CISS_US"]["label"] and "1980" in us["meta"] and us["amber"] > 400, "the United States CISS chart")
            fr.press("body", "Escape")

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

            r.section("4. Crisis peaks: every set with its history")
            fr.click("#sov-tabs .tab[data-view='crisis']")
            fr.wait_for_selector("table.crisis", timeout=30000)
            for set_id, col_id, ep, side in (("sovciss", "SOV_IT", "italy2011", "hi"), ("spread", "S10_GR", "spain2012", "hi"), ("ciss", "CISS_US", "crash1987", "hi"), ("euro", "TGT_IT", "gilts2022", "lo")):
                fr.click("[data-set='%s']" % set_id)
                fr.wait_for_timeout(1200)
                k = fr.evaluate(CRISIS)
                idx = k["head"].index(col_id)
                shown = k["rows"][ep][idx]
                want = fmt(M[col_id]["unit"], M[col_id]["peaks"][ep][side][0]).replace(" bp", "").replace(" bn", "")
                cols = len(k["head"]) - 1
                if k["lines"] is not None:
                    drawn = fr.evaluate(PIXELS, "#sov-ov")
                    r.log("  %-8s %d series, %d windows | one chart of all %d over their whole history (%d line pixels), legend %d | tagged first: %s" % (set_id, cols, len(k["eps"]), k["lines"], drawn, k["legend"], k["head"][1:4]))
                    ck(k["lines"] == cols and k["legend"] == cols and drawn > 3000, "set %s: the history chart draws %s of %d series (%d pixels)" % (set_id, k["lines"], cols, drawn))
                else:
                    drawn = fr.evaluate("() => Array.from(document.querySelectorAll('canvas[data-mini]')).filter(c => { const d = c.getContext('2d').getImageData(0, 0, c.width, c.height).data; let n = 0; for (let i = 0; i < d.length; i += 4) if (d[i + 3] > 150 && d[i] > 200) n++; return n > 30; }).length")
                    r.log("  %-8s %d series in different units, %d windows | %d small history charts, %d drawn" % (set_id, cols, len(k["eps"]), k["minis"], drawn))
                    ck(k["minis"] == cols and drawn == cols, "set %s: %d of %d small charts drawn" % (set_id, drawn, cols))
                r.log("           %s in '%s': page %s, document %s | now %s | record %s | overflow %s px" % (col_id, ep, shown, want, k["now"][idx], k["record"][idx], k["ox"]))
                ck(shown == want and k["now"][0].startswith("Now") and k["ox"] <= 1, "crisis set %s: %s against %s" % (set_id, shown, want))
                if set_id == "sovciss":
                    ck(k["head"][1:4] == ["EA_SOV_GDP", "SOV_GR", "SOV_IT"], "tagged countries do not lead the columns after the euro area: %s" % k["head"][1:4])
                    ck(set(k["eps"]) == {e["id"] for e in doc["episodes"] if any(e["id"] in M[i]["peaks"] for i in k["head"][1:])} and "crash1987" not in k["eps"], "the windows shown for SovCISS")
                    ck(len(k["tops"]) == len(M["EA_SOV_GDP"]["tops"]) and fmt("index", M["EA_SOV_GDP"]["tops"][0][0]) in k["tops"][0], "the list of highest readings")
                    pl = k["head"].index("SOVM_PL")
                    ck(k["now"][pl] == "—" and k["record"][pl] == fmt("index", M["SOVM_PL"]["high"][0]), "Poland's ended SovCISS: now %s, record %s" % (k["now"][pl], k["record"][pl]))
                    fr.click("[data-olines='tagged']")
                    fr.wait_for_timeout(400)
                    ck(fr.evaluate(CRISIS)["lines"] == 3, "'Only tagged' should leave the euro area and the two tagged countries")
                    fr.click("[data-olines='all']")
                    fr.wait_for_timeout(300)
                    fr.click("table.crisis tr[data-ep='italy2011'] td[data-id='SOV_IT']")
                    fr.wait_for_timeout(1200)
                    cc = fr.evaluate(CANVAS)
                    rg = fr.evaluate("() => Array.from(document.querySelectorAll('#sov-rg .gbtn')).map(b => b.innerText + (b.classList.contains('on') ? '*' : ''))")
                    r.log("           a cell opens %s | ranges %s | %d line pixels" % (cc["title"], rg, cc["amber"]))
                    ck(cc["on"] and cc["title"] == M["SOV_IT"]["label"] and any("Italy and Spain drawn in" in x and x.endswith("*") for x in rg) and cc["amber"] > 300, "a crisis cell did not open its chart around the window")
                    fr.click("#sov-rg [data-years='0']")
                    fr.wait_for_timeout(400)
                    ck(fr.evaluate(CANVAS)["amber"] != cc["amber"], "'All' did not redraw the whole history")
                    fr.press("body", "Escape")
            fr.click("#sov-tabs .tab[data-view='desk']")
            fr.wait_for_selector("[data-cleartags]", timeout=10000)
            fr.click("[data-cleartags]")
            fr.wait_for_timeout(300)
            cleared = fr.evaluate(ORDER, "sovereign")
            r.log("tags cleared: %s" % " ".join(cleared[:4]))
            painted = fr.evaluate("() => Array.from(document.querySelectorAll('#sov-main .tg')).filter(b => b.style.background).length")
            plain = sorted((k for k in base if cval(k) is not None), key=cval) + [k for k in base if cval(k) is None]      # still sorted by CISS, low to high; no CISS goes last
            ck(painted == 0 and cleared == plain, "tags were not cleared: %d dots still coloured, order %s against %s" % (painted, cleared[:4], plain[:4]))
            back = fr.evaluate("() => { try { return localStorage.getItem('jh.sovereign-desk.v1'); } catch (e) { return ''; } }")
            ck('"tags":{}' in (back or ""), "clearing the tags: %s" % back)

            r.section("5. World and Sources")
            fr.click("#sov-tabs .tab[data-view='world']")
            fr.wait_for_selector("table[data-table='world'] tbody tr", timeout=60000)
            fr.wait_for_timeout(400)
            w_rows = fr.evaluate("() => Array.from(document.querySelectorAll(\"table[data-table='world'] tbody tr\")).map(tr => Array.from(tr.children).map(c => c.innerText.trim()))")
            top = max((c for c in world["countries"] if isinstance(c.get("cds_bp"), (int, float))), key=lambda c: c["cds_bp"])
            r.log("world: %d rows | first: %s" % (len(w_rows), " | ".join(w_rows[0])))
            ck(len(w_rows) == len(world["countries"]) and w_rows[0][0] == top["country"], "the world table: %d rows, first %s (widest CDS in the document: %s)" % (len(w_rows), w_rows[0][0], top["country"]))

            r.section("5b. The World tab's chart: all countries, or only the tagged ones")
            fr.wait_for_selector("#sov-ov", timeout=60000)
            fr.wait_for_timeout(900)
            WM = wh["measures"]
            wkey = lambda name: "czechia" if name.lower() == "czech republic" else name.lower()
            held = lambda sid: [c for c in wh["countries"] if sid in c["ids"]]
            many = lambda: fr.evaluate("""() => { const c = document.getElementById('sov-ov'); return { lines: +c.getAttribute('data-lines'), mode: c.getAttribute('data-mode'),
                legend: Array.from(document.querySelectorAll('#sov-leg .lg')).map(b => [b.getAttribute('data-line'), !b.classList.contains('off')]),
                sets: Array.from(document.querySelectorAll('[data-wset]')).map(b => [b.getAttribute('data-wset'), b.innerText.replace(/\\s+/g, ' ').trim(), b.classList.contains('on')]),
                modes: Array.from(document.querySelectorAll('[data-olines]')).filter(b => b.classList.contains('on')).map(b => b.getAttribute('data-olines')),
                miss: (document.getElementById('sov-wmiss') || {}).innerText || '', ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }""")
            k = many()
            r.log("sets on the page: %s" % " | ".join("%s%s" % (s[1], "*" if s[2] else "") for s in k["sets"]))
            ck([(s[0], s[1]) for s in k["sets"]] == [(s["id"], "%s %d" % (s["name"], s["n"])) for s in wh["sets"]] and [s[0] for s in k["sets"] if s[2]] == ["yield"], "the four sets, the long yields first: %s" % k["sets"])
            for s in wh["sets"]:
                fr.click("[data-wset='%s']" % s["id"])
                fr.wait_for_timeout(700)
                k = many()
                drawn = fr.evaluate(PIXELS, "#sov-ov")
                want = [c["ids"][s["id"]] for c in held(s["id"])]
                by = {}
                for mid in want:
                    by[WM[mid]["source"]] = by.get(WM[mid]["source"], 0) + 1
                r.log("  %-15s %d of %d countries drawn (%d line pixels), legend %d, mode %s | sources %s | from %s | overflow %s px" % (
                    s["id"], k["lines"], len(wh["countries"]), drawn, len(k["legend"]), k["mode"], by, min(WM[m]["start"] for m in want), k["ox"]))
                ck(k["lines"] == s["n"] == len(want) and sorted(x[0] for x in k["legend"]) == sorted(want) and all(x[1] for x in k["legend"]) and k["modes"] == ["all"] and drawn > 1500 and k["ox"] <= 1,
                   "set %s: %d lines for %d countries, %d pixels" % (s["id"], k["lines"], s["n"], drawn))
                names_missing = [c["country"] for c in wh["countries"] if s["id"] not in c["ids"]]
                ck(all(n in k["miss"] for n in names_missing) and (bool(k["miss"]) == bool(names_missing)), "set %s: the countries without a history are not all named: %s" % (s["id"], k["miss"][:160]))
            # tags made in the world table; "Only tagged" keeps to them, also when another country is tagged afterwards
            fr.click("[data-wset='yield']")
            fr.wait_for_timeout(500)
            tagged = []

            def tag(country, colour):
                fr.click("table[data-table='world'] .tg[data-tag='%s']" % wkey(country))
                fr.wait_for_selector("#sov-pal [data-colour='%s']" % colour, state="visible", timeout=10000)
                fr.click("#sov-pal [data-colour='%s']" % colour)
                fr.wait_for_timeout(500)
                tagged.append(country)
            expect = lambda sid: sorted(c["ids"][sid] for c in held(sid) if c["country"] in tagged)
            tag("Turkey", "red")
            tag("Japan", "blue")
            fr.click("[data-olines='tagged']")
            fr.wait_for_timeout(500)
            k = many()
            on = sorted(x[0] for x in k["legend"] if x[1])
            r.log("Turkey and Japan tagged, 'Only tagged' on the long yields: %d line(s) %s (Turkey has no monthly yield: %s)" % (k["lines"], on, "Turkey" in wh["missing"]["yield"]))
            ck(on == expect("yield") and k["lines"] == len(on) and k["mode"] == "tagged" and k["modes"] == ["tagged"], "'Only tagged' on the yields: %s against %s" % (on, expect("yield")))
            ck(sorted(x[0] for x in k["legend"][:len(on)]) == on, "tagged countries do not lead the list: %s" % [x[0] for x in k["legend"]][:3])
            tag("United States", "green")
            k = many()
            on = sorted(x[0] for x in k["legend"] if x[1])
            r.log("the United States tagged while 'Only tagged' is on: %d line(s) %s" % (k["lines"], on))
            ck(on == expect("yield") and k["lines"] == len(on) == 2, "'Only tagged' did not follow a new tag: %s" % on)
            box = fr.evaluate("() => { const b = document.getElementById('sov-ov').getBoundingClientRect(); return [b.width, b.height]; }")
            fr.hover("#sov-ov", position={"x": box[0] * 0.7, "y": box[1] * 0.5})
            fr.wait_for_timeout(300)
            tip = fr.evaluate("() => document.getElementById('sov-otip').innerText.replace(/\\s+/g, ' ')")
            r.log("under the pointer: %s" % tip[:150])
            ck("United States" in tip and "Japan" in tip and re.search(r"\d{4}", tip), "the pointer read-out on the World chart: %s" % tip[:120])
            for sid in ("cds", "yield_reported", "cds_reported"):
                fr.click("[data-wset='%s']" % sid)
                fr.wait_for_timeout(600)
                k = many()
                on = sorted(x[0] for x in k["legend"] if x[1])
                r.log("  'Only tagged' carried to %-15s %d line(s) %s" % (sid, k["lines"], on))
                ck(on == expect(sid) and k["lines"] == len(on) and k["mode"] == "tagged", "'Only tagged' on %s: %s against %s" % (sid, on, expect(sid)))
            # a line's name flips it; "Show all" brings every country back
            other = next(c["ids"]["cds_reported"] for c in held("cds_reported") if c["country"] not in tagged)
            fr.click("#sov-leg .lg[data-line='%s']" % other)
            fr.wait_for_timeout(400)
            k = many()
            ck(k["lines"] == len(expect("cds_reported")) + 1 and k["modes"] == [], "a click on one name should add that one line: %d lines, mode buttons %s" % (k["lines"], k["modes"]))
            fr.click("[data-olines='all']")
            fr.wait_for_timeout(400)
            ck(many()["lines"] == len(held("cds_reported")), "'Show all' did not bring every country back")
            # cells of the table open that country's history
            pick = lambda country, kind: next(c["ids"] for c in wh["countries"] if c["country"] == country).get(kind) or next(c["ids"] for c in wh["countries"] if c["country"] == country).get(kind + "_reported")
            fr.click("[data-wset='yield']")
            fr.wait_for_timeout(500)
            for country, kind in (("Turkey", "cds"), ("Japan", "yield")):
                mid = pick(country, kind)
                fr.click("table[data-table='world'] tr[data-key='%s'] td.c[data-id='%s']" % (wkey(country), mid))
                fr.wait_for_timeout(900)
                cc = fr.evaluate(CANVAS)
                r.log("  the %s figure of %s opens: %s | %s | %d line pixels" % (kind, country, cc["title"], cc["meta"][:150], cc["amber"]))
                ck(cc["on"] and cc["title"] == WM[mid]["label"] and WM[mid]["source_label"][:40] in cc["meta"] and cc["amber"] > 300, "the %s cell of %s did not open %s" % (kind, country, mid))
                fr.press("body", "Escape")
            fr.click("table[data-table='world'] tr[data-key='japan'] .cn")
            fr.wait_for_timeout(700)
            ck(fr.evaluate(CANVAS)["title"] == WM[pick("Japan", "yield")]["label"], "the country name in the world table did not open its history")
            fr.press("body", "Escape")
            kept = fr.evaluate("() => { try { return localStorage.getItem('jh.sovereign-desk.v1'); } catch (e) { return ''; } }")
            r.log("kept in the browser: %s" % kept)
            fr.click("[data-olines='tagged']")
            fr.wait_for_timeout(300)
            ck('"lines":"tagged"' in (fr.evaluate("() => { try { return localStorage.getItem('jh.sovereign-desk.v1'); } catch (e) { return ''; } }") or ""), "'Only tagged' is not kept for the next visit")
            fr.click("[data-cleartags]")
            fr.wait_for_timeout(400)
            k = many()
            ck(k["mode"] == "all" and k["lines"] == len(held("yield")), "clearing the tags should show every country again: mode %s, %d lines" % (k["mode"], k["lines"]))
            r.section("5c. The Dollar shortage tab")
            fr.click("#sov-tabs .tab[data-view='dollar']")
            fr.wait_for_selector("canvas[data-dchart]", timeout=60000)
            fr.wait_for_function("() => document.querySelectorAll('canvas[data-dchart]').length >= 15 && document.querySelectorAll('#sov-dtiles .tile').length === 3 && Array.from(document.querySelectorAll('canvas[data-dchart]')).every(c => +c.getAttribute('data-points') > 100) && !!document.querySelector('#sov-dsrc a[href*=\"ciss-dollar-shortage\"]')", timeout=60000)
            DM, DSER = ds["measures"], ds["series"]
            order = ECB3 + [i for i in ds["order"] if i not in ECB3]                                 # the page's order: the three ECB measures, then the Federal Reserve's as the document lists them
            want_parts = [["ecb", 3]] + [[part["id"], len([i for i in part["ids"] if i not in ECB3])] for part in ds["parts"] if any(i not in ECB3 for i in part["ids"])]
            dname = lambda m: m["label"] if m["id"] in ECB3 else m["short"]
            YOY = [i for i in order if i not in ECB3 and DM[i]["part"] in YOY_PARTS and DM[i]["low"][0] > 0]      # a line that has stood at or below zero keeps its own unit
            ysr = {i: yoy(DSER[i]) for i in YOY}
            npts = lambda i: len(ysr[i]) if i in YOY else len(DSER[i]["d"])
            dv = lambda m, v=None, strip=False: fmt(dunit(m), m["value"] if v is None else v).replace(" bn" if strip else "\0", "").replace(" bp" if strip else "\0", "")
            dollar_state = lambda: fr.evaluate("""() => { const q = (s) => Array.from(document.querySelectorAll(s)), txt = (n) => n ? n.innerText.replace(/\\s+/g, ' ').trim() : '';
              const t = document.querySelector('table.dwin'), row = (tr) => tr ? Array.from(tr.children).map(txt) : [];
              const lit = (c) => { if (!c.width) return 0; const d = c.getContext('2d').getImageData(0, 0, c.width, c.height).data; let n = 0; for (let i = 0; i < d.length; i += 4) if (d[i] > 200 && d[i + 1] > 150 && d[i + 2] < 90) n++; return n; };
              const top = (sel) => { const e = document.querySelector(sel); return e ? Math.round(e.getBoundingClientRect().top + window.scrollY) : -1; };
              return { tab: txt(document.querySelector('#sov-tabs .tab.on')), read: q('#sov-dread p').map(txt),
                tiles: q('#sov-dtiles .tile').map(x => [x.getAttribute('data-id'), txt(x.querySelector('.tl')), txt(x.querySelector('.tv'))]),
                titles: q('.dchart .mt').map(e => [e.getAttribute('data-id'), txt(e)]),
                tops: [top('#sov-dtiles'), top(".dpart[data-part='ecb']"), top('#sov-dboard'), top(".dpart[data-part='backstop']"), top('#sov-dwin')],
                board: q('#sov-dboard tr.drow').map(tr => [tr.getAttribute('data-id')].concat(row(tr))), groups: q('#sov-dboard tr.grp').map(tr => tr.getAttribute('data-part')),
                bars: Object.fromEntries(q('#sov-dboard tr.drow').map(tr => { const i = tr.querySelector('.pb i'); return [tr.getAttribute('data-id'), i ? i.className : null]; })),
                parts: q('.dpart').map(d => [d.getAttribute('data-part'), d.querySelectorAll('canvas[data-dchart]').length]),
                charts: q('canvas[data-dchart]').map(c => [c.getAttribute('data-dchart'), +c.getAttribute('data-points'), lit(c), c.getAttribute('data-units')]), heads: q('table.dwin thead tr:last-child th').map(txt), groupheads: q('table.dwin thead th.grp').map(txt),
                punits: q('.dpart').map(d => [d.getAttribute('data-part'), d.getAttribute('data-units'), q(".dpart[data-part='" + d.getAttribute('data-part') + "'] [data-dunits].on").map(b => b.getAttribute('data-dunits')).join('')]),
                cardheads: Object.fromEntries(q('.dchart .mt').map(e => [e.getAttribute('data-id'), txt(e)])),
                rows: Object.fromEntries(q('table.dwin tbody tr[data-ep]').map(tr => [tr.getAttribute('data-ep'), row(tr)])), now: row(t.querySelector('tr.nowrow')), record: row(t.querySelector('tr.recrow')),
                marked: q('table.dwin thead th.mk').map(txt), src: txt(document.getElementById('sov-dsrc')), fred: q('#sov-dsrc a[href*="fred.stlouisfed.org/series/"]').length,
                all: document.getElementById('sov-main').innerText, bad: (document.getElementById('sov-main').innerText.match(/\\bNaN\\b|\\bundefined\\b|\\bnull\\b|Infinity/g) || []),
                ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }""")
            k = dollar_state()
            for line in k["read"]:
                r.log("  read: %s" % line)
            r.log("the three ECB measures, tiles: %s" % " | ".join("%s = %s" % (x[1], x[2]) for x in k["tiles"]))
            r.log("the three ECB measures, charts: %s | from the top of the page: tiles %d px, their charts %d px, the table %d px, the Federal Reserve's charts %d px, the windows table %d px" % (
                " | ".join(x[1][:70] for x in k["titles"][:3]), k["tops"][0], k["tops"][1], k["tops"][2], k["tops"][3], k["tops"][4]))
            for row in k["board"]:
                r.log("  %-16s %s" % (row[0], " | ".join(row[1:])))
            r.log("charts by part %s: %s" % (k["parts"], " | ".join("%s %d points, %d line pixels" % (x[0], x[1], x[2]) for x in k["charts"])))
            # the three Khalid added are there by the names he added them under, first on the tab: tiles, then charts, before anything of the Federal Reserve's
            ck([x[0] for x in k["tiles"]] == ECB3 and [x[1] for x in k["tiles"]] == ECB_TITLES == [DM[i]["label"] for i in ECB3] and all(fmt(DM[i]["unit"], DM[i]["value"]) in v for i, _, v in k["tiles"]),
               "the three ECB tiles: %s" % k["tiles"])
            ck([x[0] for x in k["titles"][:3]] == ECB3 and all(x[1].startswith(t + " ") for x, t in zip(k["titles"][:3], ECB_TITLES)) and [(x[0], x[1]) for x in k["charts"][:3]] == [(i, len(DSER[i]["d"])) for i in ECB3]
               and all(x[2] > 1500 for x in k["charts"][:3]), "the three ECB charts are not the first three charts under the ECB's titles: %s" % k["titles"][:3])
            ck(0 < k["tops"][0] < k["tops"][1] < k["tops"][2] < k["tops"][3] < k["tops"][4], "the three ECB measures should come before the table and the Federal Reserve's charts: %s" % k["tops"])
            ck(k["tab"] == "Dollar shortage" and [row[0] for row in k["board"]] == order and k["groups"] == [x[0] for x in want_parts], "the table of every measure: %s" % [row[0] for row in k["board"]])
            ck(all(row[1].startswith(dname(DM[row[0]])) and row[2] == dv(DM[row[0]]) for row in k["board"]), "a row of the table does not show its measure's name and value: %s" % [(row[0], row[1][:30], row[2]) for row in k["board"] if row[2] != dv(DM[row[0]])][:3])
            ck(k["parts"] == want_parts and [(x[0], x[1]) for x in k["charts"]] == [(i, npts(i)) for i in order] and all(x[2] > 1500 for x in k["charts"]),
               "each chart should draw every observation of its series: %s" % k["charts"])
            # the two parts Khalid named open as percent change from a year ago; every figure is recomputed here from the document by FRED's formula
            for i in YOY:
                lo, hi = min(ysr[i], key=lambda t: t[1]), max(ysr[i], key=lambda t: t[1])
                turns = [d for (d0, a), (d, b) in zip(ysr[i], ysr[i][1:]) if (a > 0) != (b > 0)]
                r.log("  year over year %-16s %s on %s (%d figures from %s) | lowest %s on %s, highest %s on %s | crossed zero %d times, last on %s | the card: %s" % (
                    i, fmt_yoy(ysr[i][-1][1]), ysr[i][-1][0], len(ysr[i]), ysr[i][0][0], fmt_yoy(lo[1]), lo[0], fmt_yoy(hi[1]), hi[0], len(turns), turns[-1] if turns else "never", k["cardheads"][i][:110]))
            ck(YOY == ["USD_DXY", "USD_BROAD", "USD_AFE", "USD_EME", "FED_CUSTODY_UST", "FED_FOREIGN_RRP"] and k["punits"] == [[pid, "yoy" if pid in YOY_PARTS else "level", "yoy" if pid in YOY_PARTS else ""] for pid, _ in want_parts],
               "the price and the foreign official parts should open as percent change from a year ago, the others as they were: %s" % k["punits"])
            ck(all(x[3] == ("yoy" if x[0] in YOY else "level") for x in k["charts"]) and all(ysr[i][-1][0] == DM[i]["date"] and (fmt_yoy(ysr[i][-1][1]) + " from a year ago") in k["cardheads"][i] for i in YOY)
               and dv(DM["FED_CUSTODY_13W"]) in k["cardheads"]["FED_CUSTODY_13W"] and "Drawn as it is" in k["all"],
               "a year-over-year chart does not show FRED's figure for the latest reading: %s" % {i: k["cardheads"][i][:80] for i in YOY})
            swaps, lend, eur, dxy, res, c13 = DM["FED_SWAPS"], DM["AUX_FX_EA"], DM["AUX_EURUSD"], DM["USD_DXY"], DM["AUX_FX_NONEA"], DM["FED_CUSTODY_13W"]
            ck(len(k["read"]) == 8 and k["read"][0].startswith("The EUR/USD reference rate is " + fmt("index", eur["value"])) and k["read"][1].startswith("Claims on euro-area residents in foreign currency") and fmt("eur_bn", lend["value"]) in k["read"][1]
               and fmt("eur_bn", lend["high"][0]) in k["read"][1] and k["read"][2].startswith("Claims on non-euro-area residents in foreign currency are " + fmt("eur_bn", res["value"]))
               and fmt("usd_bn", swaps["value"]) in k["read"][3] and fmt("usd_bn", swaps["high"][0]) in k["read"][3] and fmt("idx", dxy["value"]) in k["read"][4] and fmt("usd_bn", DM["FED_CUSTODY_UST"]["value"]) in k["read"][5]
               and fmt("usd_bn", c13["low"][0]) in k["read"][5] and fmt("bp", DM["USD_SOFR_IOR"]["high"][0]) in k["read"][6] and fmt("usd_bn", DM["H8_NET_DUE"]["low"][0]) in k["read"][6] and k["read"][7].startswith("Not here: a cross-currency basis"),
               "the opening lines of the Dollar shortage tab")
            place = {row[0]: row[8] for row in k["board"]}
            r.log("placed: %s | bars %s" % (json.dumps(place, ensure_ascii=False)[:700], json.dumps(k["bars"])))
            ck(all(k["all"].count(t) >= 3 for t in ECB_TITLES) and "Foreign-currency claims abroad" not in k["all"], "each of the three should be named as the ECB names it in its tile, its chart and its row: %s" % [k["all"].count(t) for t in ECB_TITLES])
            ck(all(place[i].endswith("% of its record") for i in ("FED_SWAPS", "FED_FIMA", "AUX_FX_EA")) and place["USD_DXY"] == "higher than %d%% of its readings" % int(dxy["pctile"]) and place["AUX_EURUSD"] == "lower than %d%% of its readings" % int(100 - eur["pctile"])
               and place["USD_TED"] == "ended at its source" and k["bars"]["FED_SWAPS"] is not None and all(k["bars"][i] is None for i in order if dside(DM[i]) is None or DM[i]["status"] == "ended"),
               "how far each line stands toward a shortage: %s" % json.dumps(place, ensure_ascii=False)[:400])
            cols = [(DM[i], end, end == dside(DM[i])) for i in order for end in (("lo", "hi") if i in ECB3 else ((dside(DM[i]),) if dside(DM[i]) else ()))]
            inside = [i for i in order if i in ECB3 or dside(DM[i])]
            eps = [e["id"] for e in ds["episodes"] if any(e["id"] in m["peaks"] for m, _, _ in cols)]
            g = k["rows"].get("gfc2008") or []
            want_g = [dv(m, m["peaks"]["gfc2008"][end][0], True) if "gfc2008" in m["peaks"] else "—" for m, end, _ in cols]
            r.log("windows: %d rows, %d columns under %s | Lehman row %s | now %s | record %s" % (len(k["rows"]), len(k["heads"]), k["groupheads"], g[1:], k["now"][1:], k["record"][1:]))
            ck(list(k["rows"]) == eps and g[1:] == want_g and len(k["heads"]) == len(cols) == 15 and len(k["marked"]) == sum(1 for c in cols if c[2]) == 11, "the windows table: %s against %s" % (g[1:], want_g))
            ck(k["record"][1:] == [dv(m, (m["high"] if end == "hi" else m["low"])[0], True) for m, end, _ in cols] and len(k["now"]) == 1 + len(inside) and all(k["now"][1 + n].startswith(dv(DM[i], None, True)) for n, i in enumerate(inside))
               and not k["bad"] and k["ox"] <= 1, "record / now rows, broken text %s, overflow %s" % (k["bad"][:3], k["ox"]))
            ck(all(DM[i]["key"] in k["src"] for i in order) and "The document" in k["src"] and k["fred"] == sum(len(DM[i]["series"]) for i in order if DM[i].get("source") == "fred"), "each number's source is not named: %d FRED links | %s" % (k["fred"], k["src"][:200]))
            fr.click("#sov-drg [data-dyears='5']")
            fr.wait_for_timeout(600)
            five = [x[1] for x in dollar_state()["charts"]]
            fr.click("#sov-drg [data-dyears='0']")
            fr.wait_for_timeout(500)
            ck(all(0 < a < b for a, b in zip(five, [x[1] for x in k["charts"]])), "the 5Y range did not shorten the charts: %s" % five)
            box = fr.evaluate("() => { const c = document.querySelector(\"canvas[data-dchart='USD_DXY']\"); c.scrollIntoView({ block: 'center' }); const b = c.getBoundingClientRect(); return [b.width, b.height]; }")
            fr.hover("canvas[data-dchart='USD_DXY']", position={"x": box[0] * 0.6, "y": box[1] * 0.5})
            fr.wait_for_timeout(300)
            ytip = fr.evaluate("() => document.querySelector(\"[data-dtip='USD_DXY']\").innerText.replace(/\\s+/g, ' ')")
            fr.click(".dpart[data-part='price'] [data-dunits='level']")
            fr.wait_for_timeout(900)
            lv = dollar_state()
            fr.click(".dpart[data-part='price'] [data-dunits='yoy']")
            fr.wait_for_timeout(900)
            bk = dollar_state()
            price = [i for i in YOY if DM[i]["part"] == "price"]
            r.log("under the pointer on the DXY chart: %s | Level pressed on the price part: %s, DXY reads %s | pressed back: %s" % (
                ytip, [(x[0], x[3], x[1]) for x in lv["charts"] if x[0] in price], lv["cardheads"]["USD_DXY"][:60], [(x[0], x[3], x[1]) for x in bk["charts"] if x[0] in price]))
            ck("from a year ago" in ytip and re.match(r"^[+−]?\d", ytip), "the pointer read-out on a year-over-year chart: %s" % ytip)
            ck(all(x[3] == "level" and x[1] == len(DSER[x[0]]["d"]) and x[2] > 1500 for x in lv["charts"] if x[0] in price) and dv(DM["USD_DXY"]) in lv["cardheads"]["USD_DXY"] and "from a year ago" not in lv["cardheads"]["USD_DXY"]
               and all(x[3] == "yoy" for x in lv["charts"] if x[0] in YOY and x[0] not in price) and [(x[0], x[1], x[3]) for x in bk["charts"]] == [(x[0], x[1], x[3]) for x in k["charts"]],
               "the Level button should bring the price part's levels back, leave the other part alone, and the first button restore the year-over-year charts")
            fr.click(".dchart .mt[data-id='FED_CUSTODY_UST']")
            fr.wait_for_timeout(1000)
            cc = fr.evaluate(CANVAS)
            big = fr.evaluate("() => { const c = document.getElementById('sov-cv'); return [c.getAttribute('data-units'), +c.getAttribute('data-points'), Array.from(document.querySelectorAll('#sov-rg [data-cunits].on')).map(b => b.getAttribute('data-cunits')).join('')]; }")
            fr.click("#sov-rg [data-cunits='level']")
            fr.wait_for_timeout(700)
            big2 = fr.evaluate("() => { const c = document.getElementById('sov-cv'); return [c.getAttribute('data-units'), +c.getAttribute('data-points'), Array.from(document.querySelectorAll('#sov-rg [data-cunits].on')).map(b => b.getAttribute('data-cunits')).join('')]; }")
            r.log("the larger chart opened from the year-over-year custody chart: %s | %s, %d line pixels | after Level: %s" % (cc["title"][:60], big, cc["amber"], big2))
            ck(cc["on"] and cc["title"] == DM["FED_CUSTODY_UST"]["label"] and big == ["yoy", len(ysr["FED_CUSTODY_UST"]), "yoy"] and cc["amber"] > 1000 and big2 == ["level", len(DSER["FED_CUSTODY_UST"]["d"]), "level"],
               "the larger chart should open in the units of the chart it was opened from and offer the level")
            fr.press("body", "Escape")
            box = fr.evaluate("() => { const c = document.querySelector(\"canvas[data-dchart='FED_SWAPS']\"); c.scrollIntoView({ block: 'center' }); const b = c.getBoundingClientRect(); return [b.width, b.height]; }")
            fr.hover("canvas[data-dchart='FED_SWAPS']", position={"x": box[0] * 0.5, "y": box[1] * 0.5})
            fr.wait_for_timeout(300)
            tip = fr.evaluate("() => document.querySelector(\"[data-dtip='FED_SWAPS']\").innerText.replace(/\\s+/g, ' ')")
            r.log("five years: %s points | under the pointer on the swap lines: %s" % (five, tip))
            ck(tip.startswith("$") and re.search(r"\d{4}$|\d{4} ", tip + " "), "the pointer read-out on a Dollar shortage chart: %s" % tip)
            fr.click("table.dwin tr[data-ep='covid2020'] td.mk[data-id='FED_SWAPS']")
            fr.wait_for_timeout(1000)
            cc = fr.evaluate(CANVAS)
            rg = fr.evaluate("() => Array.from(document.querySelectorAll('#sov-rg .gbtn')).map(b => b.innerText + (b.classList.contains('on') ? '*' : ''))")
            links = fr.evaluate("() => Array.from(document.querySelectorAll('#sov-cm a')).map(a => a.getAttribute('href'))")
            r.log("the swap lines' high in the COVID window opens: %s | ranges %s | %d line pixels | links %s" % (cc["title"], rg, cc["amber"], links))
            ck(cc["on"] and cc["title"] == swaps["label"] and any("COVID" in x and x.endswith("*") for x in rg) and cc["amber"] > 300 and "https://fred.stlouisfed.org/series/SWPT" in links and not any("ecb.europa.eu" in x for x in links),
               "a Dollar shortage cell did not open the Federal Reserve measure's chart around the window")
            fr.press("body", "Escape")
            fr.click("#sov-dtiles .tile[data-id='AUX_FX_NONEA']")
            fr.wait_for_timeout(1000)
            cc = fr.evaluate(CANVAS)
            r.log("the tile of the non-euro-area claims opens: %s | %s | %d line pixels" % (cc["title"], cc["stat"][:110], cc["amber"]))
            ck(cc["on"] and cc["title"] == ECB_TITLES[2] and cc["amber"] > 1000, "the tile of the claims on non-euro-area residents did not open its chart")
            fr.press("body", "Escape")
            fr.click("#sov-dboard tr.drow[data-id='USD_TED'] td.nm")
            fr.wait_for_timeout(1000)
            cc = fr.evaluate(CANVAS)
            r.log("the TED row opens: %s | %s | %d line pixels" % (cc["title"], cc["meta"][:150], cc["amber"]))
            ck(cc["on"] and cc["title"] == DM["USD_TED"]["label"] and "ended at its source" in cc["meta"] and cc["amber"] > 1000, "the ended TED spread's own chart")
            fr.press("body", "Escape")
            fr.click(".dchart .mt[data-id='FED_CUSTODY_13W']")
            fr.wait_for_timeout(1000)
            cc = fr.evaluate(CANVAS)
            r.log("the name over the custody chart opens: %s | %s" % (cc["title"], cc["stat"][:200]))
            ck(cc["on"] and cc["title"] == c13["label"] and "Lowest reading inside each named window".lower() in cc["stat"].lower() and cc["amber"] > 1000, "a chart's name did not open the larger chart, or it reads the wrong end of the windows")
            fr.press("body", "Escape")
            fr.click("#sov-dboard tr.drow[data-id='AUX_EURUSD'] td.nm")
            fr.wait_for_timeout(1000)
            cc = fr.evaluate(CANVAS)
            r.log("the EUR/USD row opens: %s | %s" % (cc["title"], cc["stat"][:150]))
            ck(cc["on"] and cc["title"] == eur["label"] and "last reading of each week" not in cc["stat"] and cc["amber"] > 1000, "the reference rate's own chart should draw every day")
            fr.press("body", "Escape")

            fr.click("#sov-tabs .tab[data-view='sources']")
            fr.wait_for_timeout(500)
            src = fr.evaluate("() => ({ rows: document.querySelectorAll('table.board tbody tr').length, text: document.getElementById('sov-main').innerText.replace(/\\s+/g, ' ').slice(0, 420), complete: document.getElementById('sov-main').innerText.includes('Nothing: every response arrived and compiled.'), links: Array.from(document.querySelectorAll('#sov-main a')).map(a => a.getAttribute('href')) })")
            r.log("sources: %d rows, %d links, says every response arrived: %s | %s" % (src["rows"], len(src["links"]), src["complete"], src["text"]))
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
            page.click("table[data-table='sovereign'] .tg[data-tag='portugal']")
            page.wait_for_selector("#sov-pal [data-colour='green']", state="visible", timeout=10000)
            page.click("#sov-pal [data-colour='green']")
            page.wait_for_timeout(300)
            page.goto("%s/sovereign-desk.html?nocache=%d" % (BASE, stamp + 1), wait_until="domcontentloaded")
            page.wait_for_selector("table[data-table='sovereign'] tbody tr", timeout=90000)
            page.wait_for_timeout(600)
            again = page.evaluate(ORDER, "sovereign")
            r.log("Portugal tagged, page loaded again: %s" % " ".join(again[:4]))
            ck(again[0] == "portugal", "a tag did not survive loading the page again: %s" % again[:3])
            widths = {}
            for view in ("desk", "crisis", "world", "dollar", "sources"):
                page.click("#sov-tabs .tab[data-view='%s']" % view)
                page.wait_for_timeout(1300)
                widths[view] = page.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth")
            page.goto("%s/sovereign-desk.html?view=dollar&m=AUX_FX_EA&nocache=%d" % (BASE, stamp + 2), wait_until="domcontentloaded")
            page.wait_for_selector("canvas[data-dchart]", timeout=90000)
            page.wait_for_function("() => document.getElementById('sov-ovl').classList.contains('on')", timeout=30000)
            linked = [page.evaluate("() => document.querySelector('#sov-tabs .tab.on').innerText"), page.evaluate(CANVAS)["title"]]
            r.log("the link the Dollar desk's cards open (?view=dollar&m=AUX_FX_EA): tab %s, chart %s" % tuple(linked))
            ck(linked == ["Dollar shortage", ds["measures"]["AUX_FX_EA"]["label"]], "the link from the Dollar desk does not open the tab and the chart: %s" % linked)
            page.goto("%s/sovereign-desk.html?view=dollar&m=FED_SWAPS&nocache=%d" % (BASE, stamp + 3), wait_until="domcontentloaded")
            page.wait_for_selector("canvas[data-dchart]", timeout=90000)
            page.wait_for_function("() => document.getElementById('sov-ovl').classList.contains('on')", timeout=30000)
            page.wait_for_timeout(600)
            linked = [page.evaluate("() => document.querySelector('#sov-tabs .tab.on').innerText"), page.evaluate(CANVAS)["title"], page.evaluate(CANVAS)["amber"]]
            r.log("a Federal Reserve card's link (?view=dollar&m=FED_SWAPS): tab %s, chart %s, %d line pixels" % tuple(linked))
            ck(linked[:2] == ["Dollar shortage", ds["measures"]["FED_SWAPS"]["label"]] and linked[2] > 300, "the link to a Federal Reserve measure does not open the tab and its chart: %s" % linked)
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
                sf = json.loads(fetch("/data/settlement-fails.json?probe=%d" % stamp))
                r.log("  what stopped it: the fails document's headline carries %s" % json.dumps({k: (sf.get("headline") or {}).get(k) for k in ("combined_bn", "pctile")}))
            except Exception as e:  # noqa: BLE001
                r.log("  settlement-fails document: %s" % str(e)[:120])
            ck(not ferr and not f["loading"] and len(f["stats"]) >= 4, "sovereign.html: errors %s, loading %s, %d figures" % (ferr[:2], f["loading"], len(f["stats"])))
            ctx.close()

            r.section("9. The Dollar desk carries the lines of the same document")
            spec35 = importlib.util.spec_from_file_location("ops6835", str(sorted(Path(__file__).resolve().parent.glob("ops_6835_*.py"))[0]))
            P35 = importlib.util.module_from_spec(spec35)
            spec35.loader.exec_module(P35)
            ctx = browser.new_context(viewport={"width": 1400, "height": 900}, user_agent=UA)
            ctx.add_init_script("window.open = (u) => { window.__opened = (window.__opened || []).concat([String(u)]); return {}; }")
            page = ctx.new_page()
            derr = []
            page.on("pageerror", lambda e: derr.append(str(e)[:200]))
            page.goto("%s/dollar-indexes.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#macro-body .macro-item", timeout=90000)
            ids = ["flows:ecb-dollar/" + i for i in DESK_FAMILY]
            for sid in ids:
                page.evaluate("(s) => { const c = document.querySelector(`#macro-body .macro-item[data-symbol=\"${s}\"]`); if (c) c.scrollIntoView({ block: 'center' }); }", sid)
                page.wait_for_timeout(400)
            page.wait_for_function("(ids) => ids.every(s => { const c = document.querySelector(`#macro-body .macro-item[data-symbol=\"${s}\"]`); return c && (c.dataset.metricState === 'ready' || c.dataset.metricState === 'error') && c.querySelector('.macro-hist-line, .macro-hist-empty'); })", arg=ids, timeout=120000)
            page.wait_for_timeout(1500)
            s35 = page.evaluate(P35.CARDS)
            by = {row["s"]: row for row in s35["rows"]}
            fam = page.evaluate("() => { const c = document.querySelector('#macro-body .macro-item[data-symbol=\"flows:ecb-dollar/AUX_EURUSD\"]'); const cat = c && c.closest('.macro-cat'); return cat ? cat.innerText.split('\\n').map(x => x.trim()).filter(x => /dollar shortage/i.test(x))[0] || '' : ''; }")
            r.log("%s | %d families, %d cards (catalog %d) | the family: %s" % (s35["title"], s35["cats"], len(s35["rows"]), s35["catalog"], fam))
            ck(s35["catalog"] == 103 and s35["families"] == 9 and len(s35["rows"]) == 103 and "dollar shortage" in fam.lower() and [row["s"] for row in s35["rows"] if row["s"].startswith("flows:")] == ids,
               "the Dollar desk: %d cards in %d families, family '%s'" % (len(s35["rows"]), s35["families"], fam))
            for sid in ids:
                row, mid = by.get(sid), sid.split("/")[1]
                if not ck(row is not None, "no card for %s" % sid):
                    continue
                want = P35.changes(list(zip(ds["series"][mid]["d"], ds["series"][mid]["v"])))
                agree = {hz: P35.agrees(row["ch"].get(hz), want[hz]) for hz in want}
                r.log("  %-13s %s | %s as of %s | W %s M %s Q %s Y %s (own arithmetic %s) | strip %d, bears %d" % (mid, row["title"][:52], row["v"], row["asof"], row["ch"].get("W"), row["ch"].get("M"), row["ch"].get("Q"), row["ch"].get("Y"),
                      " ".join("%+.2f" % want[hz] if want[hz] is not None else "-" for hz in ("W", "M", "Q", "Y")), row["line"], row["bears"]))
                shown = float(str(row["v"]).replace(",", ""))
                ck(row["state"] == "ready" and abs(shown - ds["measures"][mid]["value"]) <= (0.051 if abs(shown) >= 1000 else 0.0051) and row["asof"] == ds["measures"][mid]["date"] and all(agree.values()) and row["line"] >= 1 and not row["bad"],
                   "the %s card: state %s, value %s, changes %s against %s" % (mid, row["state"], row["v"], row["ch"], want))
            ck(all(by.get("flows:ecb-dollar/" + i) and by["flows:ecb-dollar/" + i]["title"].startswith(t) for i, t in zip(ECB3, ECB_TITLES)), "the three ECB cards are not under the ECB's titles: %s" % [(by.get("flows:ecb-dollar/" + i) or {}).get("title") for i in ECB3])
            page.click("#macro-body .macro-item[data-symbol='flows:ecb-dollar/FED_SWAPS'] .mi-title")
            page.wait_for_selector("#macro-focus-chart", state="visible", timeout=30000)
            page.wait_for_timeout(700)
            f35 = page.evaluate(P35.FOCUS)
            page.click("#macro-focus-chart")
            page.wait_for_timeout(500)
            opened = page.evaluate("() => window.__opened || []")
            r.log("full screen: %s | %s | its chart button opens %s | page errors %s" % (f35["title"], f35["latest"][:60], opened, derr[:2]))
            ck(f35["big"] >= 1 and opened == ["/sovereign-desk.html?view=dollar&m=FED_SWAPS"] and not derr, "the card's full screen or its chart link: %s %s %s" % (f35["big"], opened, derr[:2]))
            ctx.close()
            browser.close()

        try:
            with urllib.request.urlopen(urllib.request.Request(BASE + "/favicon.ico", headers={"User-Agent": UA}), timeout=30) as resp:
                r.log("favicon.ico: HTTP %s" % resp.status)
        except Exception as e:  # noqa: BLE001
            r.log("favicon.ico: %s (the one console 404 the first page of a browser session reports)" % str(e)[:60])
        r.kv(failed_checks=len(fails), dollar_observations=ds["coverage"]["observations"], world_series=len(wh["measures"]), measures=len(M), tiles=len(d["tiles"]) + len(d["world"]), board_rows=len(d["board"]), ended_rows=len(d["ended"]), ciss_table_rows=len(d["ciss"]), world_rows=len(w_rows), ciss_rows=c["rows"])
        if fails:
            r.fail("%d checks failed: %s" % (len(fails), " | ".join(fails)[:1500]))
            sys.exit(1)
        r.ok("the Sovereign button opens the desk; tiles, boards, tags, sorting, charts, crisis histories, the World chart (all and only tagged), the Dollar shortage tab and sources hold against the documents; the Dollar desk carries the lines of the same document; the CISS page reads plainly and checks its auxiliaries against their originals")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
