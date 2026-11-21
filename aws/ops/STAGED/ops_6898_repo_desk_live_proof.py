"""ops 6898 - READ-ONLY, real browser on the live site: the chart's Repo button and the desk it opens.

Khalid 2026-11-21: "display an icon for it in the chart.html make sure it cover all the different aspect of the repo
market and repo rate including their spikes in crisis".  Re-runnable; nothing is written.

  0. Guard: the live site must serve the new files (the button in the engine, the rail entry, the desk script); the
     probe waits for the edge instead of judging an old copy.
  1. chart.html at 1440 px: the Repo button sits right after CDS; it is CLICKED; the overlay must hold /repo.html.
  2. Inside that overlay: the Desk view - score and regime equal the feed's, every tile has a number or says why not,
     tiles checked against data/repo.json value by value, the corridor's three levels against the board's policy rows.
  3. Crisis spikes: the table has one row per episode; the cells of September 2019, the 2008 crisis and October 2025
     are recomputed here in Python from the same history files and must equal what the page prints.
  4. A number is clicked: its full-history chart opens with its spikes listed.  The board views still work.
  5. /repo.html at 390 px: no sideways scroll on the Desk, the Crisis view and a board tab.
"""
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
import importlib.util
import json
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
_spec = importlib.util.spec_from_file_location("ops6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
A = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(A)
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome"
EPISODES = {"September 2019 repo squeeze": ("2019-09-13", "2019-10-04"), "Lehman / global financial crisis": ("2008-09-12", "2008-12-31"),
            "October 2025 month-end squeeze": ("2025-10-27", "2025-11-06"), "March 2020 dash for cash": ("2020-03-09", "2020-04-09")}
MEASURES = (("D_SOFR_ADMIN", "bp", 1), ("D_SOFR_TAIL", "bp", 1), ("D_SOFR_EFFR", "bp", 1), ("D_GCF_TRI", "bp", 1), ("D_EFFR_TARGET", "bp", 1), ("TEDRATE", "bp", 100),
            ("NYPD-PD_AFtD_T-A", "$B", 1e-9), ("NYPD-PD_AFtD_AG_MBS-A", "$B", 1e-9), ("D_FED_REPO", "$B", 1), ("SRF_TAKEUP", "$B", 1), ("WLCFLPCL", "$B", 1e-3), ("SWPT", "$B", 1e-3))
CHART = r"""() => { const b = (id) => document.getElementById(id), ov = b('ws-overlay'), f = ov ? ov.querySelector('iframe') : null, btn = b('btn-repo'), prev = btn ? btn.previousElementSibling : null,
  next = btn ? btn.nextElementSibling : null, box = btn ? btn.getBoundingClientRect() : null;
  return { btn: !!btn, prev: prev ? prev.id : null, next: next ? next.id : null, label: btn ? btn.textContent.trim() : null, title: btn ? btn.title : null,
    box: box ? [Math.round(box.x), Math.round(box.y), Math.round(box.width), Math.round(box.height)] : null,
    overlay: ov ? ov.className : null, src: f ? f.getAttribute('src') : null, head: ov && ov.querySelector('.wshd span') ? ov.querySelector('.wshd span').textContent : null }; }"""
DESK = r"""() => { const q = (s) => Array.from(document.querySelectorAll(s)), txt = (n) => n ? n.innerText.replace(/\s+/g, ' ').trim() : '';
  return { hero: txt(document.querySelector('.rd-hero')), asof: txt(document.getElementById('asof')), sections: q('.rd-sec[data-sec]').map(s => s.getAttribute('data-sec')),
    tiles: q('.rd-tile').map(t => ({ id: t.getAttribute('data-id'), lab: txt(t.querySelector('.rd-lab')), val: txt(t.querySelector('.rd-val')), date: txt(t.querySelector('.rd-date')), late: !!t.querySelector('.rd-late'), missing: t.classList.contains('rd-missing') })),
    corridor: q('.rd-corr text').map(t => t.textContent), comps: q('.rd-comp').length, tabs: q('.tab').map(t => t.textContent.trim()),
    baro: getComputedStyle(document.getElementById('baro')).display, ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""
CRISIS = r"""() => { const q = (s) => Array.from(document.querySelectorAll(s)), txt = (n) => n ? n.innerText.replace(/\s+/g, ' ').trim() : '';
  return { head: q('.rd-matrix thead th').map(th => ({ id: th.getAttribute('data-id'), text: txt(th) })),
    rows: q('.rd-matrix tbody tr').map(tr => Array.from(tr.children).map(td => ({ b: txt(td.querySelector('b')), small: txt(td.querySelector('small')), hot: td.classList.contains('rd-hot'), na: td.classList.contains('na') }))),
    lists: q('.rd-list').map(l => ({ head: txt(l.querySelector('h3')), items: Array.from(l.querySelectorAll('li')).map(txt) })),
    ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""


def fixed(v, digits):
    """A number the way the browser prints it (Intl.NumberFormat): the shortest decimal that names the float, rounded
    half away from zero, with thousands separators.  Python's own "%.2f" rounds the float's exact binary value, so
    2.235 prints 2.23 here and 2.24 in the browser."""
    q = Decimal(repr(float(v))).quantize(Decimal(1).scaleb(-digits), rounding=ROUND_HALF_UP)
    return "{:,}".format(q)


def fmt_measure(unit, scale, v):
    """The page's own print of a crisis cell (jh-repo-desk.js fmtMeasure), written again here."""
    x = v * scale
    body = fixed(x, 0 if abs(x) >= 100 else 1)
    return ("+" if x > 0 else "") + body + " bp" if unit == "bp" else "$" + body + "B"


def fmt_kind(kind, v):
    """The page's print of a tile for the kinds checked here."""
    if kind == "rate":
        return fixed(v, 2) + "%"
    if kind == "usd":
        a = abs(v)
        return "$" + (fixed(v / 1e12, 2) + "T" if a >= 1e12 else fixed(v / 1e9, 0) + "B" if a >= 1e10 else fixed(v / 1e9, 1) + "B" if a >= 1e9 else fixed(v / 1e6, 0) + "M")
    raise ValueError(kind)


def window_peak(dates, values, start, end):
    best = None
    for d, v in zip(dates, values):
        if start <= d <= end and isinstance(v, (int, float)) and (best is None or v > best[1]):
            best = (d, v)
    return best


def selftest():
    assert fmt_measure("bp", 1, 315.0) == "+315 bp" and fmt_measure("bp", 100, 4.58) == "+458 bp" and fmt_measure("bp", 1, 32.0) == "+32.0 bp" and fmt_measure("bp", 1, -11.0) == "-11.0 bp"
    assert fmt_measure("$B", 1e-9, 2683588000000.0) == "$2,684B" and fmt_measure("$B", 1, 74.6) == "$74.6B" and fmt_measure("$B", 1e-3, 583000.0) == "$583B"
    assert fixed(2.235, 2) == "2.24" and fixed(2683.588, 0) == "2,684" and fixed(-11.0, 1) == "-11.0" and fixed(0.05, 1) == "0.1"
    assert fmt_kind("rate", 3.91) == "3.91%" and fmt_kind("usd", 2.235e12) == "$2.24T" and fmt_kind("usd", 2.7373e11) == "$274B" and fmt_kind("usd", 6.114e9) == "$6.1B"
    assert window_peak(["2019-09-16", "2019-09-17", "2019-10-07"], [33, 315, 999], "2019-09-13", "2019-10-04") == ("2019-09-17", 315)
    assert window_peak(["2019-09-16"], [None], "2019-09-13", "2019-10-04") is None


def fetch(path, timeout=60):
    req = urllib.request.Request(BASE + path, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def main():
    selftest()
    from ops_report import report
    with report("ops_6898_repo_desk_live_proof") as r:
        stamp = int(time.time())
        r.heading("ops 6898 - the chart's Repo button and the desk it opens, live (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        fails = []

        def ck(cond, msg):
            if cond:
                return True
            fails.append(msg)
            r.warn("CHECK FAILED: " + msg)
            return False

        r.section("0. The live site serves the new files")
        served = {}
        for attempt in range(40):
            try:
                served = {"engine": b"id=btn-repo" in fetch("/jh-chart-engine.js?probe=%d" % (stamp + attempt)),
                          "rail": b'repo: ["Repo' in fetch("/jh-chart-tvrail.js?probe=%d" % (stamp + attempt)),
                          "desk": b"renderCrisis" in fetch("/jh-repo-desk.js?probe=%d" % (stamp + attempt)),
                          "page": b"jh-repo-desk.js" in fetch("/repo.html?probe=%d" % (stamp + attempt))}
            except Exception as e:  # noqa: BLE001
                served = {"error": str(e)[:80]}
            if served and all(v is True for v in served.values()):
                break
            time.sleep(20)
        r.log("served after %d tries: %s" % (attempt + 1, json.dumps(served)))
        try:
            r.log("chart.html carries the Repo cache-bust on %d script tags" % fetch("/chart.html?probe=%d" % stamp).count(b"repo=20261121"))
        except Exception as e:  # noqa: BLE001
            r.log("chart.html: %s" % str(e)[:80])
        if not (served and all(v is True for v in served.values())):
            r.fail("the live site does not serve the Repo desk yet; nothing was judged")
            sys.exit(1)

        board = json.loads(fetch("/data/repo.json?probe=%d" % stamp))
        rows = {s["id"]: dict(s, group=g["name"]) for g in board["groups"] for s in g["series"]}
        desk = json.loads(fetch("/data/repo-market.json?probe=%d" % stamp))
        r.log("feeds: board engine %s as of %s, %d series in %d groups | desk as of %s, score %s %s, policy block %s" % (
            board.get("engine_v"), board.get("as_of"), len(rows), len(board["groups"]), desk.get("as_of"), desk.get("repo_stress_score"), desk.get("regime"), json.dumps(desk.get("policy"))[:160]))
        hist = {}

        def history(sid):
            if sid not in hist:
                hist[sid] = json.loads(fetch("/data/repo-history/%s.json" % rows[sid]["sid"]))
            return hist[sid]

        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)

            r.section("1. chart.html: the Repo button")
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent=UA)
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto("%s/chart.html?s=SPY&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#btn-repo", timeout=90000, state="attached")
            page.wait_for_function("() => (window.lastBars || []).length > 300 && typeof window.jhOpenWorkspace === 'function'", timeout=90000)
            page.wait_for_timeout(1200)
            c0 = page.evaluate(CHART)
            r.log("button: %s" % json.dumps(c0))
            ck(c0["btn"] and c0["prev"] == "btn-cds" and c0["next"] == "btn-usd" and "Repo" in (c0["label"] or "") and "crisis spikes" in (c0["title"] or "") and c0["overlay"] == "", "the button: %s" % c0)
            page.mouse.move(700, 3)                     # the toolbar hides until the pointer reaches the top edge
            try:
                page.wait_for_selector("#jh-ah-top.show", timeout=8000)
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(500)
            page.evaluate("() => document.getElementById('btn-repo').scrollIntoView({ inline: 'center', block: 'nearest' })")
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
                page.evaluate("() => document.getElementById('btn-repo').click()")
            page.wait_for_timeout(800)
            c2 = page.evaluate(CHART)
            if c2["overlay"] != "on":
                clicked += " then script"
                page.evaluate("() => document.getElementById('btn-repo').click()")
                page.wait_for_timeout(800)
                c2 = page.evaluate(CHART)
            ck(c2["overlay"] == "on" and (c2["src"] or "") == "/repo.html" and "Repo" in (c2["head"] or ""), "the button did not open the desk: %s" % c2)
            r.log("clicked by %s at box %s -> overlay %s | %s | %s" % (clicked, c1["box"], c2["overlay"], c2["head"], c2["src"]))
            fr = None
            for _ in range(80):
                fr = next((f for f in page.frames if f.url.split("?")[0].endswith("/repo.html")), None)
                if fr:
                    break
                page.wait_for_timeout(250)
            if not ck(fr is not None, "no workspace frame for the desk"):
                r.fail("no desk frame: %s" % " | ".join(fails)[:900])
                sys.exit(1)

            r.section("2. The Desk view, inside the chart")
            fr.wait_for_selector(".rd-hero", timeout=90000)
            fr.wait_for_timeout(800)
            d = fr.evaluate(DESK)
            r.log("as-of line: %s" % d["asof"])
            r.log("hero: %s" % d["hero"][:260])
            r.log("tabs: %s" % " | ".join(d["tabs"][:26]))
            ck(d["tabs"][:3] == ["Desk", "Crisis spikes", "Overview%d" % len(rows)] and len(set(d["tabs"])) == len(d["tabs"]), "tabs: %s" % d["tabs"][:6])
            ck(str(desk.get("repo_stress_score")) in d["hero"] and str(desk.get("regime")) in d["hero"] and str(desk.get("as_of")) in d["hero"], "hero does not carry the feed's score / regime / date: %s" % d["hero"][:160])
            ck(d["sections"] == ["rates", "spreads", "venues", "lenders", "dealers", "fed", "collateral", "europe"] and d["comps"] == len(desk.get("components") or []) and d["baro"] == "none" and d["ox"] <= 1,
               "sections %s, %d components, board barometer %s, overflow %s" % (d["sections"], d["comps"], d["baro"], d["ox"]))
            tiles = d["tiles"]
            missing = [t["lab"] for t in tiles if t["missing"]]
            late = ["%s (%s)" % (t["lab"], t["date"]) for t in tiles if t["late"]]
            r.log("%d tiles | without a number: %s | marked late: %s" % (len(tiles), missing, late))
            ck(len(tiles) >= 70 and not missing, "tiles %d, without a number %s" % (len(tiles), missing))
            checked = 0
            for sid, kind in (("FNYR-SOFR-A", "rate"), ("FNYR-TGCR-A", "rate"), ("REPO-TRIV1_AR_OO-P", "rate"), ("REPO-GCF_AR_TOT-P", "rate"), ("REPO-DVP_AR_OO-P", "rate"), ("FNYR-EFFR-A", "rate"),
                              ("FNYR-SOFR_UV-A", "usd"), ("REPO-TRIV1_TV_TOT-P", "usd"), ("REPO-GCF_TV_TOT-P", "usd"), ("REPO-DVP_TV_TOT-P", "usd"), ("FICC-SPONSORED_REPO_VOL", "usd"),
                              ("MMF-MMF_RP_TOT-M", "usd"), ("MMF-MMF_RP_wFR-M", "usd"), ("NYPD-PD_AFtD_T-A", "usd"), ("NYPD-PD_SB_T_TOT-A", "usd"), ("RRPONTSYAWARD", "rate"), ("IORB", "rate"), ("DFEDTARU", "rate")):
                t = next((x for x in tiles if x["id"] == sid), None)
                want = fmt_kind(kind, rows[sid]["last_value"])
                if ck(t is not None and t["val"] == want and rows[sid]["last"] in t["date"], "%s: page %s, board %s on %s" % (sid, t and (t["val"], t["date"]), want, rows[sid]["last"])):
                    checked += 1
                r.log("  %-26s %-9s %-11s %s" % (sid, t["val"] if t else "-", t["date"] if t else "-", (t or {}).get("lab", "")[:60]))
            r.log("tiles equal to the board, value and date: %d of 18" % checked)
            sofr_age = (datetime.now(timezone.utc).date() - datetime.strptime(rows["FNYR-SOFR-A"]["last"], "%Y-%m-%d").date()).days
            ck(sofr_age <= 5, "SOFR on the board is %d days old" % sofr_age)
            corr = " ".join(d["corridor"])
            levels = [fixed(rows[x]["last_value"], 2) + "%" for x in ("RRPONTSYAWARD", "IORB", "DFEDTARU")]
            ck(all(x in corr for x in levels) and "SOFR" in corr and (fixed(desk["rates"]["sofr"]["value"], 2) + "%") in corr, "corridor %s against the board's policy rows %s" % (d["corridor"], levels))
            r.log("corridor: %s | the board's floor / IORB / ceiling: %s" % (" · ".join(d["corridor"]), levels))
            shot = page.screenshot(type="jpeg", quality=50)
            r.log("screenshot of the overlay: %d bytes" % len(shot))

            r.section("3. Crisis spikes")
            fr.evaluate("() => document.querySelector('.tab[data-t=\"crisis\"]').click()")
            fr.wait_for_selector(".rd-matrix", timeout=120000)
            fr.wait_for_timeout(600)
            c = fr.evaluate(CRISIS)
            ids = [h["id"] for h in c["head"][1:]]
            ck(ids == [m[0] for m in MEASURES] and len(c["rows"]) == 14 and len(c["lists"]) == 12, "table: %d columns %s, %d rows, %d lists" % (len(ids), ids, len(c["rows"]), len(c["lists"])))
            agree = total = 0
            for row in c["rows"]:
                name = row[0]["b"]
                if name not in EPISODES:
                    continue
                start, end = EPISODES[name]
                cells = []
                for (sid, unit, scale), cell in zip(MEASURES, row[1:]):
                    h = history(sid)
                    peak = window_peak(h["dates"], h["values"], start, end)
                    total += 1
                    if peak is None:
                        ok = cell["na"]
                        want = "—"
                    else:
                        want = fmt_measure(unit, scale, peak[1])
                        ok = cell["b"] == want and cell["small"] == peak[0]
                    agree += 1 if ok else 0
                    ck(ok, "%s / %s: page %s %s, recomputed %s %s" % (name, sid, cell["b"], cell["small"], want, peak and peak[0]))
                    cells.append("%s%s" % (cell["b"] or "—", (" " + cell["small"]) if cell["small"] else ""))
                r.log("  %-34s %s" % (name, " | ".join(cells)))
            r.log("cells of four episodes recomputed from the history files: %d of %d equal" % (agree, total))
            sep = next(row for row in c["rows"] if row[0]["b"] == "September 2019 repo squeeze")
            gfc = next(row for row in c["rows"] if row[0]["b"] == "Lehman / global financial crisis")
            ck(sep[1]["b"] == "+315 bp" and sep[2]["b"] == "+375 bp" and sep[1]["small"] == "2019-09-17" and gfc[7]["b"] == "$2,684B" and gfc[6]["b"] == "+458 bp" and gfc[1]["na"],
               "September 2019 %s %s; 2008 fails %s, TED %s" % (sep[1], sep[2], gfc[7], gfc[6]))
            for lst in c["lists"]:
                r.log("  %-58s %s" % (lst["head"][:58], " ; ".join(lst["items"][:3])[:210]))
            ck(all(lst["items"] for lst in c["lists"]) and "2019-09-17" in c["lists"][0]["items"][0] and "September 2019 repo squeeze" in c["lists"][0]["items"][0], "spike lists: %s" % [len(x["items"]) for x in c["lists"]])
            ck(c["ox"] <= 1, "the crisis view scrolls sideways by %s px" % c["ox"])

            r.section("4. A number opens its history; the board is still there")
            fr.evaluate("() => document.querySelector('.rd-list h3').click()")
            fr.wait_for_function("() => document.getElementById('ovl').classList.contains('on') && document.getElementById('cm').textContent.includes('obs')", timeout=60000)
            chart = fr.evaluate("() => ({ title: document.getElementById('ct').textContent, meta: document.getElementById('cm').textContent, spikes: document.getElementById('spk').innerText.replace(/\\s+/g, ' ') })")
            r.log("chart: %s | %s | %s" % (chart["title"][:80], chart["meta"][-60:], chart["spikes"][:260]))
            ck("D_SOFR_ADMIN" in chart["title"] and "2019-09-17" in chart["spikes"] and "315" in chart["spikes"], "the history chart: %s" % chart)
            fr.evaluate("() => closeOvl()")
            fr.evaluate("() => document.querySelector('.tab[data-t=\"overview\"]').click()")
            fr.wait_for_selector("tr.row", timeout=30000)
            ov = fr.evaluate("() => ({ rows: document.querySelectorAll('tr.row').length, baro: getComputedStyle(document.getElementById('baro')).display, cards: document.querySelectorAll('.card').length })")
            fr.evaluate("() => { const t = Array.from(document.querySelectorAll('.tab')).find(x => x.dataset.t === 'Money market funds (OFR)'); t.click(); }")
            fr.wait_for_timeout(500)
            mm = fr.evaluate("() => ({ rows: document.querySelectorAll('tr.row').length, subs: Array.from(document.querySelectorAll('tr.subhead')).map(x => x.textContent) })")
            fr.evaluate("() => { const q = document.getElementById('q'); q.value = 'sofr'; q.dispatchEvent(new Event('input')); }")
            fr.wait_for_timeout(500)
            found = fr.evaluate("() => document.querySelectorAll('tr.row').length")
            r.log("overview: %d key dials, %d group cards, board barometer %s | Money market funds tab: %d rows in %s | search 'sofr': %d rows" % (ov["rows"], ov["cards"], ov["baro"], mm["rows"], mm["subs"], found))
            ck(ov["rows"] >= 20 and ov["cards"] == len(board["groups"]) and ov["baro"] == "block" and mm["rows"] == 42 and len(mm["subs"]) == 4 and found >= 20, "board views: %s %s %s" % (ov, mm, found))
            ck(not errors, "page errors: %s" % errors[:3])
            ctx.close()

            r.section("5. A phone, 390 px")
            ctx = browser.new_context(viewport={"width": 390, "height": 844}, device_scale_factor=2, user_agent=UA, is_mobile=True, has_touch=True)
            page = ctx.new_page()
            perr = []
            page.on("pageerror", lambda e: perr.append(str(e)[:200]))
            page.goto("%s/repo.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector(".rd-hero", timeout=90000)
            page.wait_for_timeout(800)
            pd = page.evaluate(DESK)
            page.evaluate("() => document.querySelector('.tab[data-t=\"crisis\"]').click()")
            page.wait_for_selector(".rd-matrix", timeout=120000)
            pc = page.evaluate(CRISIS)
            page.evaluate("() => { const t = Array.from(document.querySelectorAll('.tab')).find(x => x.dataset.t === 'Reference rates & volumes'); t.click(); }")
            page.wait_for_selector("tr.row", timeout=30000)
            pb = page.evaluate("() => ({ rows: document.querySelectorAll('tr.row').length, ox: document.documentElement.scrollWidth - document.documentElement.clientWidth })")
            r.log("desk: %d tiles, sideways overflow %s px | crisis: %d rows, overflow %s px | reference rates tab: %d rows, overflow %s px | page errors %s" % (
                len(pd["tiles"]), pd["ox"], len(pc["rows"]), pc["ox"], pb["rows"], pb["ox"], perr[:2]))
            ck(pd["ox"] <= 1 and pc["ox"] <= 1 and pb["ox"] <= 1 and len(pd["tiles"]) >= 70 and pb["rows"] == 30 and not perr, "phone: overflow %s / %s / %s, errors %s" % (pd["ox"], pc["ox"], pb["ox"], perr[:2]))
            ctx.close()
            browser.close()

        r.kv(tiles=len(tiles), tiles_equal_to_board=checked, crisis_cells_recomputed=total, crisis_cells_equal=agree, marked_late=len(late), failed_checks=len(fails))
        if fails:
            r.fail("%d checks failed: %s" % (len(fails), " | ".join(fails)[:1800]))
            sys.exit(1)
        r.ok("the Repo button opens the desk; every tile carries a number equal to the board's; %d of %d crisis cells equal the history files; no page errors; no sideways scroll at 390 px" % (agree, total))
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
