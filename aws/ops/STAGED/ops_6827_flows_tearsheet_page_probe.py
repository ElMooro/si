"""ops 6827 - the flows tearsheet, a chart-free Inflows & outflows desk and its wider coverage: live page proof in
headless Chrome (read-only, re-runnable).

Khalid 2026-11-17: "apply the same thing we did to 13F from this picture to inflows & outflows. Also remove all the
charts from inflows & outflows and see what data you can add to it. Only add, don't remove any data - we are looking
to extend coverage." Drives https://justhodl.ai/inflows-outflows.html at 1440 px and 390 px:
  1. the tearsheet is the first thing on the desk and finishes reading every series: scoreboard, the two notable
     cards, the dollar list and the four lists, one row per family - printed here as the page shows them;
  2. no charts: no svg / canvas and no history strip anywhere on the desk; every card carries its history in
     numbers instead, and the full-screen view is tables;
  3. nothing lost: each of the 169 series the desk had before is still a card and still reads (value, as-of,
     D / W / M / Q / Y); the catalog is now 19 families / 326 series and every one of them resolves live;
  4. a desk saved in the browser before today receives the additions without a reset - note, custom category and
     danger rule intact - and a series the user removed stays removed;
  5. the Macro desk is untouched (strips still drawn, no tearsheet); the chart's Flows button still opens the desk;
  6. no 'undefined' / 'NaN', no horizontal overflow at 390 px, no page errors; how long the tearsheet took to read.
Three JPEGs are embedded (base64). Nothing on the site changes.
"""
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
FAMILIES, SERIES = 19, 326
OLD = """flows:ici/total flows:ici/equity flows:ici/equity_domestic flows:ici/equity_world flows:ici/bond flows:ici/bond_taxable flows:ici/bond_municipal flows:ici/hybrid flows:ici-combined/total flows:ici-combined/equity flows:ici-combined/bond flows:ici-combined/commodity flows:ici-etf/total flows:ici-etf/equity flows:etfc/SPY.flow_5d flows:etfc/IVV.flow_5d flows:etfc/IWM.flow_5d flows:etfc/DIA.flow_5d flows:etfc/IWF.flow_5d flows:etfc/DVY.flow_5d flows:etfc/IGV.flow_5d flows:etfc/ICLN.flow_5d flows:etfc/HACK.flow_5d flows:etfc/TQQQ.flow_5d flows:etfc/SQQQ.flow_5d flows:etfc/SSO.flow_5d flows:etfc/SDS.flow_5d flows:etfc/UPRO.flow_5d flows:etfc/SPXU.flow_5d flows:etfc/UVXY.flow_5d flows:etfc/SVXY.flow_5d flows:etfc/XLK.flow_5d flows:etfc/XLF.flow_5d flows:etfc/XLE.flow_5d flows:etfc/XLV.flow_5d flows:etfc/XLI.flow_5d flows:etfc/XLY.flow_5d flows:etfc/XLP.flow_5d flows:etfc/XLU.flow_5d flows:etfc/XLB.flow_5d flows:etfc/XLRE.flow_5d flows:etfc/XLC.flow_5d flows:etfc/XBI.flow_5d flows:etfc/KRE.flow_5d flows:etfc/ITB.flow_5d flows:etfc/XOP.flow_5d flows:etfc/TLT.flow_5d flows:etfc/IEF.flow_5d flows:etfc/IEI.flow_5d flows:etfc/SHY.flow_5d flows:etfc/GOVT.flow_5d flows:etfc/BIL.flow_5d flows:etfc/SGOV.flow_5d flows:etfc/SHV.flow_5d flows:etfc/TBT.flow_5d flows:etfc/TIP.flow_5d flows:etfc/STIP.flow_5d flows:etfc/LQD.flow_5d flows:etfc/HYG.flow_5d flows:etfc/JNK.flow_5d flows:etfc/USHY.flow_5d flows:etfc/SJNK.flow_5d flows:etfc/SRLN.flow_5d flows:etfc/AGG.flow_5d flows:etfc/EMB.flow_5d flows:etfc/MUB.flow_5d flows:etfc/EFA.flow_5d flows:etfc/EEM.flow_5d flows:etfc/FXI.flow_5d flows:etfc/MCHI.flow_5d flows:etfc/ASHR.flow_5d flows:etfc/EWJ.flow_5d flows:etfc/INDA.flow_5d flows:etfc/EWZ.flow_5d flows:etfc/EWY.flow_5d flows:etfc/EWT.flow_5d flows:etfc/EWG.flow_5d flows:etfc/EWU.flow_5d flows:etfc/EWC.flow_5d flows:etfc/EWW.flow_5d flows:etfc/TUR.flow_5d flows:etfc/EIDO.flow_5d flows:etfc/GLD.flow_5d flows:etfc/SLV.flow_5d flows:etfc/IBIT.flow_5d flows:etfc/ETHA.flow_5d flows:etfc/BITO.flow_5d flows:ici-mmf/total flows:ici-mmf/government flows:ici-mmf/prime flows:ici-mmf/institutional FRED:WRMFNS FRED:RMFSL FRED:MMMFFAQ027S FRED:MMMFTAQ027S ofr:MMF-MMF_RP_OA_TOT-M ofr:MMF-MMF_BRA_TOT-M FRED:DPSACBW027SBOG FRED:TOTBKCR FRED:TOTLL FRED:CCLACBW027SBOG FRED:TOTALSL FRED:M2SL FRED:WALCL FRED:WTREGEN FRED:RRPONTSYD FRED:WRESBAL FRED:BOGMBASE FRED:HNOCESQ027S FRED:HNOMFTQ027S FRED:BOGZ1FA653064100Q FRED:BOGZ1FA563064100Q FRED:BOGZ1FA564090005Q FRED:ROWCEAQ027S FRED:NCBCEBQ027S FRED:BOGZ1FA224090045Q FRED:FORTREASPOS69995 FRED:FORTREASPOS42609 FRED:FORTREASPOS41408 FRED:FORTREASPOS13005 FRED:FORLTTREASNET42609 FRED:FORLTTREASNET42005 FRED:ROWTSAQ027S FRED:FDHBFIN FRED:FORLTTOTALNET99996 FRED:FORLTTREASNET99996 FRED:FORLTEQTYNET99996 FRED:FORLTCORPNET99996 FRED:FORLTAGCYNET99996 FRED:FORLTTOTALNET99990 FRED:FORLTTOTALNET99991 FRED:FORLTTREASNET41408 FRED:FORLTEQTYNET42609 FRED:FORLTEQTYNET41408 flows:crypto-etf/btc_today flows:crypto-etf/btc_30d flows:crypto-etf/eth_today flows:crypto-etf/eth_30d flows:crypto-exchange/btc_netflow_30d flows:crypto-exchange/eth_netflow_30d flows:crypto-exchange/btc_pctile flows:cq/btc_exchange_netflow flows:cq/btc_exchange_inflow flows:cq/btc_exchange_outflow flows:cq/btc_exchange_reserve flows:cq/eth_exchange_netflow flows:cq/eth_exchange_reserve flows:cq/btc_whale_ratio flows:cq/btc_exchange_supply_ratio flows:cq/btc_miner_netflow flows:cq/btc_fund_flow_ratio flows:stablecoins/total flows:stablecoins/usdt flows:stablecoins/usdc flows:apac/tw_foreign_net_twd flows:apac/tw_sector_flows.Semiconductors flows:apac/tw_sector_flows.Electronics/Hardware flows:apac/tw_sector_flows.Financials flows:apac/tw_sector_flows.Materials flows:apac/tw_tracked.2330 flows:apac/kr_foreign_net_krw_bn flows:apac/kr_kospi_foreign_net_krw_bn flows:apac/kr_kospi_institution_net_krw_bn flows:apac/kr_kospi_individual_net_krw_bn flows:apac-jp/foreigners flows:apac-jp/individuals flows:apac-jp/investment_trusts flows:apac-jp/trust_banks flows:apac-jp/proprietary""".split()


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


SHEET = """() => { const s = document.getElementById('flows-tearsheet'); const q = x => [...s.querySelectorAll(x)]; const p = document.getElementById('flows-ts-progress');
  return { state: s.dataset.state, head: (s.querySelector('.macro-ts-h') || {}).textContent, sub: (s.querySelector('.macro-ts-sub') || {}).textContent, done: p ? +p.dataset.done : null, total: p ? +p.dataset.total : null,
    tiles: q('.macro-ts-kpi').map(e => e.innerText.replace(/\\s+/g, ' ').trim()), cards: q('.macro-ts-card').map(e => e.innerText.replace(/\\s+/g, ' ').trim().slice(0, 360)),
    notes: q('.macro-ts-notes p').map(e => e.innerText.slice(0, 280)), cols: q('.macro-ts-col').map(c => ({k: c.dataset.tsList, n: c.querySelector('.macro-ts-n').textContent, rows: c.querySelectorAll('tbody tr').length,
      top: [...c.querySelectorAll('tbody tr')].slice(0, 3).map(r => r.innerText.replace(/\\s+/g, ' ').trim().slice(0, 150))})),
    fams: q('#flows-ts-families tbody tr').map(r => r.innerText.replace(/\\s+/g, ' ').trim().slice(0, 200)), charts: s.querySelectorAll('svg,canvas').length,
    bad: (s.innerText.match(/undefined|NaN|\\[object|null|Infinity/g) || []).length, took: (document.getElementById('flows-ts-took') || {dataset: {}}).dataset.ms || null,
    first: document.getElementById('macro-body').firstElementChild.id, wide: [...s.querySelectorAll('*')].filter(e => !e.closest('.macro-ts-fam') && e.getBoundingClientRect().right > s.getBoundingClientRect().right + 1).length }; }"""

BODY = """() => { const b = document.getElementById('macro-body'); return { items: b.querySelectorAll('.macro-item').length, cats: b.querySelectorAll('.macro-cat').length, strips: b.querySelectorAll('.macro-hist').length,
  charts: b.querySelectorAll('svg,canvas').length, nums: b.querySelectorAll('.macro-nums').length, ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""

CARDS = """(old) => { const all = [...document.querySelectorAll('#macro-body .macro-item')]; const st = {}; all.forEach(c => { st[c.dataset.metricState] = (st[c.dataset.metricState] || 0) + 1; });
  const by = {}; all.forEach(c => { by[c.dataset.symbol] = c; });
  const lost = [], unread = [];
  old.forEach(s => { const c = by[s]; if (!c) { lost.push(s); return; } const v = c.querySelector('.macro-latest-value').textContent.trim();
    if (c.dataset.metricState !== 'ready' || !v || v === 'Unavailable' || !c.querySelector('.macro-asof').textContent.trim() || c.querySelectorAll('.macro-change').length !== 5) unread.push(s + ' [' + c.dataset.metricState + ' ' + v + ']'); });
  const ready = all.filter(c => c.dataset.metricState === 'ready');
  const pick = s => by[s] ? by[s].innerText.replace(/\\s+/g, ' ').trim().slice(0, 620) : null;
  return { st, lost, unread, errors: all.filter(c => c.dataset.metricState === 'error').map(c => c.dataset.symbol + ' (' + (c.querySelector('.macro-latest-value').title || '') + ')'),
    rows: ready.map(c => c.querySelectorAll('.macro-nums-row').length).reduce((a, n) => Math.min(a, n), 99), bad: ready.filter(c => /undefined|NaN|\\[object|Infinity/.test(c.innerText)).map(c => c.dataset.symbol).slice(0, 8),
    turns: ready.filter(c => /top /.test((c.querySelector('.macro-nums') || {innerText: ''}).innerText)).length, samples: [pick('flows:etfc/SPY.flow_5d'), pick('FRED:WALCL'), pick('FRED:FORLTTREASNET13005'), pick('flows:etfc/QQQ.flow'), pick('ofr:FNYR-SOFR_UV-A')] }; }"""

FOCUS = """() => { const f = document.getElementById('macro-focus'); const t = [...f.querySelectorAll('table')];
  return { title: document.getElementById('macro-focus-title').textContent, charts: f.querySelectorAll('svg,canvas').length, facts: f.querySelectorAll('#macro-focus-facts tr').length,
    latest: f.querySelectorAll('#macro-focus-latest-prints tbody tr').length, years: f.querySelectorAll('#macro-focus-years tbody tr').length, turns: t[1] ? t[1].querySelectorAll('tbody tr').length : 0,
    bad: (f.innerText.match(/undefined|NaN|\\[object|Infinity/g) || []).length, year0: f.querySelector('#macro-focus-years tbody tr').innerText.replace(/\\s+/g, ' '),
    yearN: [...f.querySelectorAll('#macro-focus-years tbody tr')].pop().innerText.replace(/\\s+/g, ' '), fact: f.querySelector('#macro-focus-facts').innerText.replace(/\\s+/g, ' ').slice(0, 520),
    turn0: t[1] ? t[1].querySelector('tbody tr').innerText.replace(/\\s+/g, ' ') : null, btns: [...f.querySelectorAll('.macro-focus-actions button')].map(b => b.textContent.trim()) }; }"""


def main():
    with report("ops_6827_flows_tearsheet_page_probe") as r:
        r.heading("ops 6827 - the flows tearsheet + chart-free, wider Inflows & outflows desk, live page proof")
        stamp = int(time.time())
        ua = {"User-Agent": "Mozilla/5.0 jh-ops"}
        js = urllib.request.urlopen(urllib.request.Request("%s/assets/jh-workspaces.js?nocache=%d" % (BASE, stamp), headers=ua), timeout=40).read().decode("utf-8", "replace")
        r.log("assets/jh-workspaces.js: %d bytes, tearsheet code %s, additive catalog %s" % (len(js), "flows-tearsheet" in js, "mergeCatalog" in js))
        if "flows-tearsheet" not in js or "mergeCatalog" not in js:
            r.fail("the served workspace bundle is not the new one yet (Pages deploy pending?)")
            raise SystemExit(1)
        sync_playwright = ensure_playwright(r.log)
        fails, shots = [], {}

        def ck(cond, msg):
            if not cond:
                fails.append(msg)

        with sync_playwright() as p:
            browser = launch(p, r.log)

            def fresh(width):
                ctx = browser.new_context(viewport={"width": width, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
                page = ctx.new_page()
                errors = []
                page.on("pageerror", lambda e: errors.append("pageerror: %s" % e))
                page.add_init_script("window.open = () => null;")
                return ctx, page, errors

            for width in (1440, 390):
                r.section("%d px" % width)
                ctx, page, errors = fresh(width)
                t0 = time.time()
                page.goto("%s/inflows-outflows.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
                page.wait_for_selector("#flows-tearsheet", timeout=40000)
                first_paint = time.time() - t0
                try:
                    page.wait_for_function("() => document.getElementById('flows-tearsheet').dataset.state === 'ready'", timeout=240000)
                except Exception:  # noqa: BLE001
                    ck(False, "%d: the tearsheet did not finish reading within 240 s" % width)
                wall = time.time() - t0
                page.wait_for_timeout(600)
                s = page.evaluate(SHEET)
                r.log("**%s**" % s["head"])
                r.log("    %s" % s["sub"])
                r.log("    tearsheet on screen after %.1f s, every series read after %.1f s (the page's own clock: %s ms)" % (first_paint, wall, s["took"]))
                for t in s["tiles"]:
                    r.log("    tile: %s" % t)
                for c in s["cards"]:
                    r.log("    card: %s" % c)
                for n in s["notes"]:
                    r.log("    note: %s" % n)
                for c in s["cols"]:
                    r.log("    list %-10s %s series, %s shown: %s" % (c["k"], c["n"], c["rows"], " | ".join(c["top"])))
                if width == 1440:
                    for f in s["fams"]:
                        r.log("    family: %s" % f)
                ck(s["state"] == "ready" and s["first"] == "flows-tearsheet", "%d: tearsheet state %s, first in the body %s" % (width, s["state"], s["first"]))
                ck(len(s["tiles"]) == 6 and len(s["cards"]) == 2 and len(s["cols"]) == 5 and len(s["fams"]) == FAMILIES, "%d: tearsheet shape %s tiles %s cards %s lists %s families" % (width, len(s["tiles"]), len(s["cards"]), len(s["cols"]), len(s["fams"])))
                ck(s["charts"] == 0 and s["bad"] == 0 and s["wide"] == 0, "%d: tearsheet charts %s bad literals %s overflowing elements %s" % (width, s["charts"], s["bad"], s["wide"]))
                ck(s["total"] == SERIES and s["done"] == SERIES, "%d: tearsheet read %s of %s (expected %s)" % (width, s["done"], s["total"], SERIES))
                ck("unavailable" not in (s["sub"] or ""), "%d: some series are unavailable: %s" % (width, s["sub"]))
                ck(all(int(c["n"]) > 0 for c in s["cols"]), "%d: an empty list: %s" % (width, [(c["k"], c["n"]) for c in s["cols"]]))
                b = page.evaluate(BODY)
                r.log("desk: %s families, %s cards, %s history strips, %s svg / canvas, %s cards with numbers, page overflow %s px" % (b["cats"], b["items"], b["strips"], b["charts"], b["nums"], b["ox"]))
                ck(b["items"] == SERIES and b["cats"] == FAMILIES and b["nums"] == SERIES, "%d: desk shape %s" % (width, b))
                ck(b["strips"] == 0 and b["charts"] == 0, "%d: a chart is still on the desk: %s strips, %s svg / canvas" % (width, b["strips"], b["charts"]))
                ck(b["ox"] <= 0, "%d: horizontal overflow %s px" % (width, b["ox"]))
                # every card, scrolled into view
                for _ in range(400):
                    done = page.evaluate("() => { const b = document.getElementById('macro-body'); b.scrollTop += b.clientHeight * 0.9; return b.scrollTop + b.clientHeight >= b.scrollHeight - 4; }")
                    page.wait_for_timeout(90)
                    if done:
                        break
                page.wait_for_function("() => ![...document.querySelectorAll('#macro-body .macro-item')].some(c => !c.dataset.metricState || c.dataset.metricState === 'queued' || c.dataset.metricState === 'loading')", timeout=60000)
                page.wait_for_timeout(1500)
                c = page.evaluate(CARDS, OLD)
                r.log("cards after scrolling the whole desk: %s | fewest number rows on a card %s | cards with a value at a famous S&P turn %s | bad literals %s" % (json.dumps(c["st"]), c["rows"], c["turns"], c["bad"]))
                r.log("the 169 series the desk had before: %d missing, %d not reading%s" % (len(c["lost"]), len(c["unread"]), (" - " + "; ".join((c["lost"] + c["unread"])[:12])) if c["lost"] or c["unread"] else ""))
                if width == 1440:
                    for smp in c["samples"]:
                        r.log("    card: %s" % smp)
                ck(not c["lost"] and not c["unread"], "%d: series lost %s, not reading %s" % (width, c["lost"][:6], c["unread"][:6]))
                ck(c["st"].get("ready") == SERIES and not c["errors"], "%d: cards not ready: %s %s" % (width, json.dumps(c["st"]), c["errors"][:8]))
                ck(c["rows"] >= 4 and not c["bad"] and c["turns"] > 40, "%d: card numbers rows %s bad %s with turns %s" % (width, c["rows"], c["bad"], c["turns"]))
                # the full-screen view: a long weekly series, opened from its card
                page.evaluate("() => { const c = document.querySelector('#macro-body .macro-item[data-symbol=\"FRED:WALCL\"]'); c.scrollIntoView({block: 'center'}); c.click(); }")
                page.wait_for_selector("#macro-focus.open #macro-focus-years tbody tr", timeout=20000)
                f = page.evaluate(FOCUS)
                r.log("full-screen view of '%s': %s svg / canvas, %s facts, latest %s prints, %s years (%s ... %s), %s turns (%s), buttons %s" % (
                    f["title"], f["charts"], f["facts"], f["latest"], f["years"], f["year0"], f["yearN"], f["turns"], f["turn0"], f["btns"]))
                r.log("    %s" % f["fact"])
                ck(f["charts"] == 0 and f["facts"] == 9 and f["latest"] == 24 and f["years"] >= 20 and f["turns"] == 4 and f["bad"] == 0 and len(f["btns"]) == 3, "%d: full-screen view %s" % (width, {k: f[k] for k in ("charts", "facts", "latest", "years", "turns", "bad")}))
                page.keyboard.press("Escape")
                page.wait_for_timeout(250)
                ck(page.evaluate("() => !document.getElementById('macro-focus').classList.contains('open')"), "%d: the full-screen view did not close" % width)
                # a row of the tearsheet opens the same view; MORE ROWS widens the lists
                page.evaluate("() => { document.getElementById('macro-body').scrollTop = 0; }")
                page.locator("#flows-tearsheet .macro-ts-col tbody tr").first.click()
                page.wait_for_selector("#macro-focus.open #macro-focus-facts tr", timeout=20000)
                page.keyboard.press("Escape")
                page.wait_for_timeout(250)
                page.click('[data-ts-act="more"]')
                page.wait_for_timeout(250)
                more = page.evaluate("() => [...document.querySelectorAll('#flows-tearsheet .macro-ts-col')].map(c => c.querySelectorAll('tbody tr').length)")
                ck(max(more) > 6 and max(more) <= 15, "%d: MORE ROWS %s" % (width, more))
                page.click('[data-ts-act="more"]')
                page.wait_for_timeout(250)
                r.log("a tearsheet row opens the full-screen view; MORE ROWS shows %s rows per list" % more)
                page.evaluate("() => { document.querySelector('#macro-modal .heatmap-content').style.height = 'auto'; const b = document.getElementById('macro-body'); b.style.overflow = 'visible'; b.scrollTop = 0; }")
                page.wait_for_timeout(300)
                box = page.locator("#flows-tearsheet").bounding_box()
                top = box["y"] + page.evaluate("window.scrollY")
                if width == 1440:
                    shots["tearsheet_desktop"] = page.screenshot(type="jpeg", quality=58, full_page=True, clip={"x": box["x"], "y": top, "width": box["width"], "height": min(box["height"], 1500)})
                    cb = page.locator("#macro-body .macro-cat").nth(1).bounding_box()
                    shots["cards_desktop"] = page.screenshot(type="jpeg", quality=58, full_page=True, clip={"x": cb["x"], "y": cb["y"] + page.evaluate("window.scrollY"), "width": cb["width"], "height": min(cb["height"], 640)})
                else:
                    shots["tearsheet_mobile"] = page.screenshot(type="jpeg", quality=55, full_page=True, clip={"x": 0, "y": top - 4, "width": 390, "height": min(box["height"] + 8, 1900)})
                ck(not errors, "%d: page errors %s" % (width, errors[:3]))
                r.log("page errors %d" % len(errors))
                ctx.close()

            r.section("a desk saved before today / the Macro desk / the chart's Flows button")
            ctx, page, errors = fresh(1440)
            legacy = {"version": 2, "catalogRev": 3, "categories": [
                {"id": "default-0", "cat": "Fund flows · ICI weekly", "flag": "📊", "items": [{"uid": "default-0-0", "symbol": "flows:ici/total", "title": "All long-term funds · weekly net flow", "units": "$M", "kind": "series", "provider": "ICI weekly",
                                                                                         "note": "my own note", "danger": {"type": "none", "threshold": None, "horizon": "D"}}]},
                {"id": "mine-1", "cat": "My own category", "flag": "★", "items": [{"uid": "mine-1-0", "symbol": "FRED:WALCL", "title": "Fed balance sheet (mine)", "units": "$M", "kind": "series", "provider": "fred", "note": "",
                                                                                 "danger": {"type": "value_above", "threshold": 1, "horizon": "D"}}]}]}
            page.add_init_script("try { if (!localStorage.getItem('jh_flows_workspace_v1')) localStorage.setItem('jh_flows_workspace_v1', %s); } catch (e) {}" % json.dumps(json.dumps(legacy)))
            page.goto("%s/inflows-outflows.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#flows-tearsheet", timeout=40000)
            page.wait_for_timeout(1200)
            mg = page.evaluate("""() => { const st = JSON.parse(localStorage.getItem('jh_flows_workspace_v1')); const syms = st.categories.flatMap(c => c.items.map(i => i.symbol)); const mine = st.categories.find(c => c.id === 'mine-1');
              return { families: st.categories.length, series: syms.length, dup: syms.length - new Set(syms).size, note: st.categories[0].items[0].note, mine: mine ? mine.items.length : null, title: mine ? mine.items[0].title : null,
                danger: mine ? mine.items[0].danger.type : null, walcl: syms.filter(s => s === 'FRED:WALCL').length, cards: document.querySelectorAll('#macro-body .macro-item').length }; }""")
            r.log("a desk saved with 2 series (one note, one custom category with a danger rule) after loading today's page: %s" % json.dumps(mg, ensure_ascii=False))
            ck(mg["series"] == SERIES and mg["dup"] == 0 and mg["note"] == "my own note" and mg["mine"] == 1 and mg["title"] == "Fed balance sheet (mine)" and mg["danger"] == "value_above" and mg["walcl"] == 1 and mg["cards"] == SERIES,
               "the saved desk did not merge cleanly: %s" % mg)
            page.evaluate("() => { const st = JSON.parse(localStorage.getItem('jh_flows_workspace_v1')); st.categories.forEach(c => { c.items = c.items.filter(i => i.symbol !== 'flows:etfc/SPY.flow_5d'); }); localStorage.setItem('jh_flows_workspace_v1', JSON.stringify(st)); }")
            page.reload(wait_until="domcontentloaded")
            page.wait_for_selector("#flows-tearsheet", timeout=40000)
            page.wait_for_timeout(800)
            back = page.evaluate("() => JSON.parse(localStorage.getItem('jh_flows_workspace_v1')).categories.flatMap(c => c.items.map(i => i.symbol)).filter(s => s === 'flows:etfc/SPY.flow_5d').length")
            r.log("a series removed by the user and the page reloaded: back on the desk %s times" % back)
            ck(back == 0, "a removed series came back after a reload")
            ctx.close()

            ctx, page, errors = fresh(1440)
            page.goto("%s/macro-economic-data.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#macro-body .macro-item", timeout=40000)
            page.wait_for_function("() => document.querySelectorAll('#macro-body .macro-hist-svg svg').length >= 3", timeout=60000)
            mc = page.evaluate("() => ({ items: document.querySelectorAll('#macro-body .macro-item').length, strips: document.querySelectorAll('#macro-body .macro-hist').length, drawn: document.querySelectorAll('#macro-body .macro-hist-svg svg').length, nums: document.querySelectorAll('.macro-nums').length, sheet: !!document.getElementById('flows-tearsheet') })")
            r.log("Macro desk: %s" % json.dumps(mc))
            ck(mc["items"] > 100 and mc["strips"] == mc["items"] and mc["drawn"] >= 3 and mc["nums"] == 0 and not mc["sheet"] and not errors, "the Macro desk changed: %s %s" % (mc, errors[:2]))
            ctx.close()

            ctx, page, errors = fresh(1440)
            try:
                page.goto("%s/chart.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
                page.wait_for_selector("#btn-flows", timeout=40000)
                page.wait_for_timeout(1500)
                page.evaluate("() => document.getElementById('btn-flows').click()")   # the toolbar button sits under the chart's own layers for Playwright's pointer; its handler is what is under test
                frame = None
                for _ in range(60):
                    frame = next((fr for fr in page.frames if "inflows-outflows.html" in fr.url), None)
                    if frame:
                        break
                    page.wait_for_timeout(500)
                if frame:
                    frame.wait_for_selector("#flows-tearsheet .macro-ts-h", timeout=40000)
                    ov = frame.evaluate("() => ({ sheet: !!document.getElementById('flows-tearsheet'), cards: document.querySelectorAll('#macro-body .macro-item').length, strips: document.querySelectorAll('.macro-hist').length })")
                    r.log("chart.html -> Flows button: the desk opens in the overlay: %s" % json.dumps(ov))
                    ck(ov["sheet"] and ov["cards"] == SERIES and ov["strips"] == 0, "the chart overlay desk: %s" % ov)
                else:
                    ck(False, "chart.html's Flows button did not open the desk in a frame")
            except Exception as e:  # noqa: BLE001
                ck(False, "chart.html overlay: %s" % str(e)[:160])
            ctx.close()
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            r.log("%s: %d bytes" % (k, len(img)))
            r.lines.append("<!-- SHOT_%s_JPEG_BASE64 -->" % k.upper())
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("<!-- /SHOT_%s_JPEG_BASE64 -->" % k.upper())
        r.kv(families=str(FAMILIES), series=str(SERIES), series_before="169", widths="1440,390", failures=str(len(fails)))
        if fails:
            for f in fails:
                r.fail(f)
            raise SystemExit(1)
        r.ok("LIVE PAGE: the flows tearsheet reads all %d series in %d families; no chart left on the desk; all 169 earlier series still read; a saved desk receives the additions; the Macro desk is untouched; 0 failures" % (SERIES, FAMILIES))


if __name__ == "__main__":
    main()
    sys.exit(0)
