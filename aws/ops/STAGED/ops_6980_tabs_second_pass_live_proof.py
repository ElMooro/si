"""ops 6980 - read-only: the second pass on the chart tabs, proven on the live site in a real browser.

Khalid 2026-11-26 11:28 ET: "look into : research. insiders, they seem broken. liquidity and plumbing need better
visuals for easier digestion of data, breadth engine need improvement and better iux, calendar need improvement and a
better iux, crypto need improvement and a better iux, also remove the demand, supply icons from there".

CLAUDE.md rule 5: each thing is clicked and read on justhodl.ai, and the figure on the page is compared with the
document in the bucket (read with boto3, because the site keeps a document for ten minutes).

  1. the site serves the new files
  2. chart.html: the Demand and Supply icons are gone from the desk buttons; the text button still switches the zones
  3. Insiders, opened from its button: the buying total is the document's, nothing floats over it, the bar chart is
     not one bar; the page says once a day
  4. Research, opened from its button on NVDA: the four tiles that were blank are filled
  5. Plumbing, Liquidity, Breadth, Calendar, Crypto, each opened from its button: the "at a glance" picture is drawn
     and its headline figure is the document's; a chart answers the pointer; a tile answers a click
  6. the calendar page: ahead / past switch, search, Eastern time, jump to a day from the grid
  7. a page outside the chart: the tape in the top bar shows its figures with their age
Nothing is written. Safe to run again.
"""
from pathlib import Path
import base64
import importlib.util
import json
import re
import sys
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "aws" / "ops"))
NAME = "ops_6980_tabs_second_pass_live_proof"
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
SYMBOL = "NVDA"
GLANCE = r"""() => { const h = document.querySelector('[data-glance]'); if (!h) return null; const b = h.querySelector('[data-body]');
  return { on: h.getAttribute('data-glance-on'), st: (h.querySelector('[data-st]') || {}).textContent || '', top: Math.round(h.getBoundingClientRect().top + window.pageYOffset),
    cards: h.querySelectorAll('.jgl-card').length, charts: h.querySelectorAll('[data-chart]').length, tiles: h.querySelectorAll('.jgl-tile').length,
    text: b ? b.innerText.replace(/\s+/g, ' ') : '', bad: b ? /NaN|undefined|\[object/.test(b.innerText) : true, none: h.querySelectorAll('.jgl-none').length,
    gauge: (h.querySelector('.jgl-gv') || {}).textContent || null, big: (h.querySelector('.jgl-big') || {}).textContent || null,
    step: (h.querySelector('.jgl-steps li.on b') || {}).textContent || null, first: (h.querySelector('.jgl-tile .jgl-v') || {}).textContent || null,
    ox: document.documentElement.scrollWidth - document.documentElement.clientWidth, inchart: document.documentElement.classList.contains('jh-in-chart') }; }"""


def fetch(url, timeout=120):
    req = urllib.request.Request(url if url.startswith("http") else BASE + url, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def main():
    import boto3
    from ops_report import report
    spec = importlib.util.spec_from_file_location("ops_6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    s3 = boto3.client("s3", region_name="us-east-1")
    stamp = int(time.time())
    shots, fails = {}, []

    def kept(key):
        return json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=key)["Body"].read())

    with report(NAME) as r:
        r.heading("6980 - the second pass on the chart tabs, live (read-only)")

        def ck(cond, msg):
            r.log(("PASS  " if cond else "FAIL  ") + msg)
            if not cond:
                fails.append(msg)
            return cond

        # ------------------------------------------------------------------ 1
        r.section("1. The live site serves the new files")
        served = False
        for attempt in range(80):
            try:
                kit = fetch("/jh-glance.js?probe=%d" % (stamp + attempt))
                sd = fetch("/jh-chart-sd-ui.js?probe=%d" % (stamp + attempt))
                liq = fetch("/liquidity.html?probe=%d" % (stamp + attempt))
                why = fetch("/why.html?probe=%d" % (stamp + attempt))
                jed = fetch("/jh-engine-data.js?probe=%d" % (stamp + attempt))
                if "JHGlance" in kit and "ticks:" in kit and 'mkBtn("btn-demand"' not in sd and "ws-overlay.on" in sd and "allImpactsOn" in fetch("/catalyst-calendar.html?probe=%d" % (stamp + attempt)) and "private-account-status" in jed and 'data-glance="liquidity"' in liq and "total_shareholder_yield_pct" in why and "jh-in-chart" in jed:
                    served = True
                    break
            except Exception as exc:  # noqa: BLE001
                r.log("probe %d: %s" % (attempt, str(exc)[:80]))
            time.sleep(15)
        ck(served, "the glance kit, the zone buttons' script, the liquidity page, the research page and the engine-data kit are the new ones")
        if not served:
            r.kv(failed_checks=len(fails))
            r.fail("the site does not serve the new files yet")
            sys.exit(1)

        docs = {}
        for key in ("data/insider-trades.json", "data/plumbing-stress.json", "data/liquidity-flow.json", "data/breadth-thrust.json", "data/catalyst-calendar.json", "data/onchain-ratios.json", "data/cryptoquant-onchain.json"):
            docs[key] = kept(key)

        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1500, "height": 1000}, user_agent=UA)
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))

            # -------------------------------------------------------------- 2
            r.section("2. chart.html: the Demand and Supply icons")
            page.goto("%s/chart.html?s=%s&nocache=%d" % (BASE, SYMBOL, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#btn-cryp", timeout=90000, state="attached")
            page.wait_for_function("() => (window.lastBars || []).length > 300 && typeof window.jhOpenWorkspace === 'function'", timeout=90000)
            try:
                page.wait_for_selector("#btn-demand2", timeout=60000, state="attached")
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(1500)
            bar = page.evaluate("""() => ({ desks: Array.from(document.querySelectorAll('#tfbar .wsdesk')).map(x => x.id),
              icons: ['btn-demand', 'btn-supply'].filter(i => document.getElementById(i)),
              text: ['btn-demand2', 'btn-supply2', 'btn-demand-rail', 'btn-supply-rail'].filter(i => document.getElementById(i)),
              labels: Array.from(document.querySelectorAll('#tfbar .wsdesk .l')).map(x => x.textContent) })""")
            r.log("desk buttons on the bar (%d): %s" % (len(bar["desks"]), " ".join(bar["desks"])))
            ck(not bar["icons"] and "Demand" not in bar["labels"] and "Supply" not in bar["labels"], "no Demand or Supply icon among the desk buttons (%s)" % (bar["icons"] or "none"))
            ck(all(b in bar["desks"] for b in ("btn-rsch", "btn-ins", "btn-plumb", "btn-liq", "btn-brd", "btn-cal", "btn-cryp", "btn-macro", "btn-mflow", "btn-alrt")) and len(set(bar["desks"])) == len(bar["desks"]), "every other desk button is still there, once each (%d)" % len(bar["desks"]))
            ck(set(bar["text"]) == {"btn-demand2", "btn-supply2", "btn-demand-rail", "btn-supply-rail"}, "the Demand / Supply text buttons and the right-rail pair are still there (%s)" % bar["text"])

            def show_bar():
                page.mouse.move(700, 3)
                try:
                    page.wait_for_selector("#jh-ah-top.show", timeout=6000)
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(400)

            show_bar()
            try:
                before = page.evaluate("() => document.getElementById('btn-demand2').classList.contains('on')")
                page.evaluate("() => document.getElementById('btn-demand2').scrollIntoView({ block: 'nearest' })")
                try:
                    page.click("#btn-demand2", timeout=6000)
                except Exception:  # noqa: BLE001
                    page.evaluate("() => document.getElementById('btn-demand2').click()")
                page.wait_for_timeout(900)
                after = page.evaluate("() => [document.getElementById('btn-demand2').classList.contains('on'), document.getElementById('btn-demand-rail').classList.contains('on')]")
                ck(after[0] != before and after[1] == after[0], "clicking the Demand text button switches the zones (%s -> %s) and the rail button follows" % (before, after[0]))
                page.evaluate("() => document.getElementById('btn-demand2').click()")
                page.wait_for_timeout(500)
                back = page.evaluate("() => document.getElementById('btn-demand2').classList.contains('on')")
                ck(back == before, "a second click puts them back (%s)" % back)
            except Exception as exc:  # noqa: BLE001
                ck(False, "the Demand text button raised %s: %s" % (type(exc).__name__, str(exc)[:200]))
            box = page.evaluate("() => { const b = document.getElementById('tfbar').getBoundingClientRect(); return [Math.round(b.top), Math.round(b.height)]; }")
            show_bar()
            shots["0-the-bar"] = page.screenshot(type="jpeg", quality=78, clip={"x": 0, "y": max(0, box[0] - 4), "width": 1500, "height": min(260, box[1] + 12)})

            def open_tab(bid, path):
                show_bar()
                page.evaluate("(id) => document.getElementById(id).scrollIntoView({ block: 'nearest' })", bid)
                page.wait_for_timeout(200)
                try:
                    page.click("#" + bid, timeout=6000)
                except Exception:  # noqa: BLE001
                    page.evaluate("(id) => document.getElementById(id).click()", bid)
                page.wait_for_selector("#ws-overlay.on iframe", timeout=20000)
                for _ in range(80):
                    f = next((x for x in page.frames if x != page.main_frame and path in x.url), None)
                    if f:
                        return f
                    page.wait_for_timeout(250)
                raise RuntimeError("the page %s did not open" % path)

            def close_tab():
                try:
                    page.click("#wsx", timeout=4000)
                except Exception:  # noqa: BLE001
                    page.evaluate("() => { const o = document.getElementById('ws-overlay'); if (o) o.className = ''; }")
                page.wait_for_timeout(300)

            # -------------------------------------------------------------- 3
            r.section("3. Insiders, from its button")
            try:
                ins = docs["data/insider-trades.json"]
                total = (ins.get("stats") or {}).get("total_value_usd") or 0
                r.log("document: version %s, written %s, %s buys, total $%s, set aside %d" % (ins.get("version"), ins.get("generated_at"), (ins.get("stats") or {}).get("count"), format(total, ",.0f"), len(ins.get("set_aside") or [])))
                f = open_tab("btn-ins", "/insiders.html")
                f.wait_for_selector("[data-jed-on]", timeout=60000)
                f.wait_for_function("() => document.body.innerText.length > 3000", timeout=60000)
                page.wait_for_timeout(3500)
                seen = f.evaluate("""() => { const t = document.body.innerText, vis = (s) => Array.from(document.querySelectorAll(s)).filter(e => { const c = getComputedStyle(e); return c.display !== 'none' && c.visibility !== 'hidden' && e.getBoundingClientRect().width > 0; }).length;
                  const viz = document.getElementById('jhviz'), bars = viz ? Array.from(viz.querySelectorAll('rect')).map(x => +x.getAttribute('height') || +x.getAttribute('width') || 0).filter(v => v > 0) : [];
                  return { inchart: document.documentElement.classList.contains('jh-in-chart'), pills: vis('.jh-tenor-pill, .jh-liq-pill, .jh-lce-pill, #jh-auth-auto-slot, #jh-capital-structure-status, #private-account-status'), money: (t.match(/\\$[\\d.,]+ ?[BMK]\\b/g) || []).slice(0, 12),
                    daily: /once a day/i.test(t), half: /every 30 minutes/i.test(t), lona: (t.match(/LONA/g) || []).length, viz: viz ? viz.innerText.replace(/\\s+/g, ' ').slice(0, 300) : null, bars: bars.length,
                    head: t.replace(/\\s+/g, ' ').slice(0, 500), ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }""")
                r.log("as seen: " + seen["head"][:420])
                r.log("money figures on the page: %s | the page's chart: %s (%d bars)" % (seen["money"], seen["viz"], seen["bars"]))
                want = "$%.1fM" % (total / 1e6) if total < 1e9 else "$%.2fB" % (total / 1e9)
                near = [m for m in seen["money"] if re.sub(r"[ ,]", "", m).startswith(want[:5])]
                ck(total < 2e9 and bool(near), "the buying total on the page is the document's (%s; the page shows %s)" % (want, near[:2] or seen["money"][:4]))
                ck(seen["inchart"] and seen["pills"] == 0, "inside the chart nothing floats over the page (%d floating pills visible)" % seen["pills"])
                ck(seen["daily"] and not seen["half"], "the page says the engine runs once a day")
                ck(seen["ox"] <= 0, "no sideways scroll")
                shots["3-insiders"] = page.screenshot(type="jpeg", quality=55)
                close_tab()
            except Exception as exc:  # noqa: BLE001
                ck(False, "Insiders raised %s: %s" % (type(exc).__name__, str(exc)[:240]))
                close_tab()

            # -------------------------------------------------------------- 4
            r.section("4. Research, from its button on %s" % SYMBOL)
            try:
                f = open_tab("btn-rsch", "/why.html")
                f.wait_for_function("() => document.body.innerText.length > 20000", timeout=120000)
                page.wait_for_timeout(2500)
                tiles = f.evaluate("""() => { const out = {};
                  document.querySelectorAll('.jhv-tile').forEach(t => { const l = (t.querySelector('.jhv-l') || {}).textContent || '', v = (t.querySelector('.jhv-v') || {}).textContent || ''; if (l && !(l in out)) out[l.trim()] = v.trim(); });
                  return { tiles: out, pills: Array.from(document.querySelectorAll('.jh-tenor-pill, .jh-liq-pill, .jh-lce-pill, #jh-auth-auto-slot, #jh-capital-structure-status, #private-account-status')).filter(e => getComputedStyle(e).display !== 'none' && e.getBoundingClientRect().width > 0).length,
                    title: document.title, len: document.body.innerText.length, sym: (document.body.innerText.match(/NVDA|NVIDIA/g) || []).length }; }""")
                r.log("the tiles as read: " + json.dumps(tiles["tiles"], ensure_ascii=False))
                filled = {k: v for k, v in tiles["tiles"].items() if re.search(r"\d", v)}
                ck(all(k in filled for k in ("MKT CAP", "P/B", "FCF YIELD", "SHRHLDR YLD")), "the four tiles that were blank carry a figure (%s)" % {k: tiles["tiles"].get(k) for k in ("MKT CAP", "P/B", "FCF YIELD", "SHRHLDR YLD")})
                own = page.evaluate("() => { const e = document.getElementById('jh-capital-structure-status'); return e ? getComputedStyle(e).display : 'absent'; }")
                ck(own in ("none", "absent"), "while the desk is open the chart's own share-count note does not float over it (%s)" % own)
                ck(tiles["sym"] > 3 and tiles["len"] > 20000 and tiles["pills"] == 0, "the page is %s's research (%s characters) with nothing floating over it" % (SYMBOL, format(tiles["len"], ",")))
                shots["4-research"] = page.screenshot(type="jpeg", quality=55)
                close_tab()
            except Exception as exc:  # noqa: BLE001
                ck(False, "Research raised %s: %s" % (type(exc).__name__, str(exc)[:240]))
                close_tab()

            # -------------------------------------------------------------- 5
            r.section("5. The five pictures, each from its button")
            pl, lf, br, ca, oc, cq = (docs[k] for k in ("data/plumbing-stress.json", "data/liquidity-flow.json", "data/breadth-thrust.json", "data/catalyst-calendar.json", "data/onchain-ratios.json", "data/cryptoquant-onchain.json"))
            today = time.strftime("%Y-%m-%d", time.gmtime())
            n_ahead = sum(1 for e in ca.get("events", []) if e.get("date", "") >= today)
            price = (oc.get("cycle_bands") or {}).get("price")
            plan = [
                ("btn-plumb", "Plumbing", "/plumbing.html", lambda g: g["gauge"] == "%.1f" % pl["composite_score"] and str(pl.get("composite_label")) in g["text"],
                 "the gauge reads %s %s, the document's composite" % ("%.1f" % pl["composite_score"], pl.get("composite_label"))),
                ("btn-liq", "Liquidity", "/liquidity.html", lambda g: (g["big"] or "").replace(" ", "").startswith("$" + format(lf["current"]["net_liquidity_b"], ",.1f")) and "the three legs add to it" in g["text"],
                 "net liquidity reads $%sbn, the document's, and the three legs add to it" % format(lf["current"]["net_liquidity_b"], ",.1f")),
                ("btn-brd", "Breadth", "/breadth-thrust.html", lambda g: (g["step"] or "").upper() == {"NULL": "WAITING", "ARMED": "ARMED", "FIRED": "FIRED"}.get(br.get("state"), (g["step"] or "").upper()) and ("%.4f" % br["current_readings"]["zweig_10d_ema"]) in g["text"] and "the same" in g["text"],
                 "the state is %s and the 10-session average %.4f, recomputed on the page to the same figure" % (br.get("state"), br["current_readings"]["zweig_10d_ema"])),
                ("btn-cal", "Calendar", "/catalyst-calendar.html", lambda g: ("%d events ahead" % n_ahead) in g["text"],
                 "%d events ahead, the document's count from today" % n_ahead),
                ("btn-cryp", "Crypto", "/btc-cycle.html", lambda g: (g["first"] or "").replace(",", "") == "$%d" % round(price) and ("%d kept from CryptoQuant" % len(cq.get("metrics") or {})) in g["text"],
                 "Bitcoin reads $%s, the document's close, with all %d on-chain metrics placed" % (format(round(price), ","), len(cq.get("metrics") or {}))),
            ]
            for bid, label, path, test, said in plan:
                try:
                    f = open_tab(bid, path)
                    f.wait_for_selector("[data-glance-on] [data-body] > *", timeout=60000)
                    page.wait_for_timeout(1200)
                    g = f.evaluate(GLANCE)
                    r.log("%s: %s | %d cards, %d charts, %d tiles | starts %d px down the page" % (label, g["st"], g["cards"], g["charts"], g["tiles"], g["top"]))
                    ck(g["cards"] >= 2 and not g["bad"] and g["none"] == 0 and g["top"] < 420 and g["ox"] <= 0, "%s: the picture is drawn at the top of the page, nothing broken, no sideways scroll" % label)
                    ck(bool(test(g)), "%s: %s (gauge %s, big %s, state %s, first tile %s)" % (label, said, g["gauge"], g["big"], g["step"], g["first"]))
                    if g["charts"]:
                        moved = f.evaluate("""async () => { const c = document.querySelector('[data-glance] [data-chart]'), svg = c.querySelector('svg'), rd = c.querySelector('.jgl-read'), b = svg.getBoundingClientRect(), was = rd.textContent;
                          const at = (fx) => svg.dispatchEvent(new MouseEvent('mousemove', { bubbles: true, clientX: b.left + b.width * fx, clientY: b.top + b.height / 2 }));
                          at(0.35); await new Promise(r => setTimeout(r, 150)); const one = rd.textContent; at(0.8); await new Promise(r => setTimeout(r, 150)); return [was, one, rd.textContent]; }""")
                        ck(moved[1] != moved[0] and moved[2] != moved[1] and re.search(r"\d", moved[2] or "") is not None, "%s: the chart answers the pointer (\"%s\" then \"%s\")" % (label, (moved[1] or "")[:70], (moved[2] or "")[:70]))
                    if bid in ("btn-plumb", "btn-cryp"):
                        sel = "[data-glance] [data-ind]" if bid == "btn-plumb" else "[data-glance] [data-met]"
                        picked = f.evaluate("""async (sel) => { const all = document.querySelectorAll(sel), el = all[Math.min(7, all.length - 1)], d = document.querySelector('[data-glance] .jgl-detail'), was = d ? d.innerText : null;
                          el.click(); await new Promise(r => setTimeout(r, 200)); const now = document.querySelector('[data-glance] .jgl-detail'); return [all.length, was, now ? now.innerText.replace(/\\s+/g, ' ').slice(0, 220) : null]; }""", sel)
                        ck(picked[2] and picked[2] != picked[1] and re.search(r"\d", picked[2]) is not None, "%s: picking one of the %d tiles reads it out (%s)" % (label, picked[0], picked[2][:150] if picked[2] else None))
                    f.evaluate("() => window.scrollTo(0, 0)")
                    page.wait_for_timeout(300)
                    shots["5-%s" % label.lower()] = page.screenshot(type="jpeg", quality=55)
                    if bid == "btn-cal":
                        # ---------------------------------------------------- 6
                        r.section("6. The calendar page's own controls")
                        a = f.evaluate("() => ({ info: document.getElementById('filterInfo').textContent, days: Array.from(document.querySelectorAll('.day-group')).map(d => d.getAttribute('data-date')), when: Array.from(document.querySelectorAll('.chip[data-when]')).map(c => [c.dataset.when, c.classList.contains('active')]), et: (document.body.innerText.match(/\\b\\d{1,2}:\\d{2}\\s?[ap]m ET\\b/gi) || []).slice(0, 3) })")
                        r.log("as loaded: %s | %d days, first %s | switch %s | times %s" % (a["info"], len(a["days"]), a["days"][:1], a["when"], a["et"]))
                        ck(a["days"] and min(a["days"]) >= today and ["ahead", True] in a["when"], "it opens on what is ahead: no day before today (%s)" % min(a["days"] or ["-"]))
                        ck(len(a["et"]) > 0, "times are shown in New York time (%s)" % a["et"][:2])
                        f.click(".chip[data-when=past]")
                        page.wait_for_timeout(400)
                        b = f.evaluate("() => ({ info: document.getElementById('filterInfo').textContent, days: Array.from(document.querySelectorAll('.day-group')).map(d => d.getAttribute('data-date')) })")
                        ck(b["days"] and max(b["days"]) < today, "Past shows only days before today (%d days, newest %s; %s)" % (len(b["days"]), max(b["days"] or ["-"]), b["info"]))
                        f.click(".chip[data-when=ahead]")
                        page.wait_for_timeout(200)
                        f.click(".chip[data-impact=LOW]")
                        page.wait_for_timeout(300)
                        f.fill("#calSearch", "FOMC")
                        page.wait_for_timeout(500)
                        c = f.evaluate("() => ({ info: document.getElementById('filterInfo').textContent, rows: Array.from(document.querySelectorAll('.event')).map(e => e.textContent.replace(/\\s+/g, ' ').trim().slice(0, 90)) })")
                        ck(0 < len(c["rows"]) < 40 and all("fomc" in x.lower() or "fed" in x.lower() for x in c["rows"]), "typing FOMC leaves the Fed's rows (%d: %s)" % (len(c["rows"]), c["rows"][:2]))
                        f.fill("#calSearch", "")
                        page.wait_for_timeout(300)
                        day = f.evaluate("""async () => { const cells = Array.from(document.querySelectorAll('[data-glance] .jgl-day[data-day]:not([disabled])')), el = cells[Math.min(cells.length - 1, 9)], iso = el.getAttribute('data-day');
                          el.click(); await new Promise(r => setTimeout(r, 900)); const g = document.querySelector('.day-group[data-date="' + iso + '"]'); return [iso, g ? Math.round(g.getBoundingClientRect().top) : null, window.innerHeight]; }""")
                        ck(day[1] is not None and -40 <= day[1] <= day[2], "picking %s in the grid brings that day into view (%s px from the top)" % (day[0], day[1]))
                        shots["6-calendar-day"] = page.screenshot(type="jpeg", quality=55)
                    close_tab()
                except Exception as exc:  # noqa: BLE001
                    ck(False, "%s raised %s: %s" % (label, type(exc).__name__, str(exc)[:240]))
                    close_tab()
            ck(not errors, "the chart threw no error while the tabs were opened (%s)" % (errors[:3] or "none"))

            # -------------------------------------------------------------- 7
            r.section("7. A page outside the chart: the tape in the top bar, and a narrow screen")
            try:
                solo = ctx.new_page()
                solo.goto("%s/plumbing.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
                solo.wait_for_selector("[data-glance-on] [data-body] > *", timeout=60000)
                solo.wait_for_timeout(4000)
                tape = solo.evaluate("() => { const t = document.body.innerText; const chip = document.querySelector('.jhc-stale'); return { stale: /Market observations unavailable or stale/.test(t), chip: chip ? chip.textContent : null, title: chip ? chip.title : null, items: document.querySelectorAll('.jhc-chip').length }; }")
                r.log("the top bar: %s" % json.dumps(tape, ensure_ascii=False))
                ck(not tape["stale"] and tape["items"] > 3 and (tape["chip"] is None or "old" in tape["chip"]), "the tape shows its figures%s instead of \"unavailable\"" % (" with their age (%s)" % tape["chip"] if tape["chip"] else " (a fresh packet)"))
                shots["7-top-bar"] = solo.screenshot(type="jpeg", quality=70, clip={"x": 0, "y": 0, "width": 1500, "height": 130})
                solo.close()
                phone = browser.new_context(viewport={"width": 390, "height": 844}, user_agent=UA, device_scale_factor=2).new_page()
                for path in ("/liquidity.html", "/breadth-thrust.html"):
                    phone.goto("%s%s?nocache=%d" % (BASE, path, stamp), wait_until="domcontentloaded")
                    phone.wait_for_selector("[data-glance-on] [data-body] > *", timeout=60000)
                    phone.wait_for_timeout(1500)
                    g = phone.evaluate(GLANCE)
                    wide = phone.evaluate("() => { const h = document.querySelector('[data-glance]'); return Math.round(h.scrollWidth - h.clientWidth); }")
                    ck(not g["bad"] and wide <= 2, "%s on a phone-width screen: the picture fits its column (%d px over)" % (path, wide))
                    phone.evaluate("() => document.querySelector('[data-glance]').scrollIntoView()")
                    shots["7-phone-%s" % path.strip("/").split(".")[0]] = phone.screenshot(type="jpeg", quality=50)
            except Exception as exc:  # noqa: BLE001
                ck(False, "the outside-the-chart look raised %s: %s" % (type(exc).__name__, str(exc)[:240]))
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(failed_checks=len(fails), shots=len(shots))
        if fails:
            r.fail("; ".join(fails)[:900])
            sys.exit(1)
        r.ok("the second pass is live: icons gone, Insiders and Research read right, the five pictures draw the documents' own figures")


if __name__ == "__main__":
    main()
