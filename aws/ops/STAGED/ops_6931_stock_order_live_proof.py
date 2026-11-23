"""ops 6931 - leaders and the largest companies first on every desk the chart opens: live proof in a real browser
(read-only, re-runnable).

Khalid 2026-11-23: "in all the different engines on my chart.html the leaders and largest market caps should show up
first at the top of the enegines they get first priority. everything stay im talking view mode and how different
stocks are arranged in view".

The reading taken: a list of stocks opens with the industry leaders first (the Leaders desk's own set), largest first,
then every other name by market value, largest first; a name with no market value keeps its place after them.  Nothing
leaves a list, no figure changes, the desk's own order is one button away and the choice is kept in the browser.  A
card whose title is itself a ranking (Strongest, Newest, Heaviest ...) keeps its own first fifteen and arranges those.

What has to hold on https://justhodl.ai:
  1. this op reads data/leaders.json itself: its leaders and their market values are the yardstick;
  2. on each desk (Leaders, Strength, Volatility, Capitulation, the eight Wyckoff sheets) every list of names on the
     tearsheet holds exactly the records the desk built, arranged by the rule from the first row to the last; the
     cards on screen are those rows; a list of no stocks is untouched;
  3. the switch: THE DESK'S OWN ORDER puts every list back as the desk built it, is kept across a reload, and LEADERS -
     LARGEST FIRST arranges them again;
  4. the desks' own tables (the leaders, the strength table of names, the volatility rows and board, the capitulation
     prints, the all-stock Acc / Bot / Dist boards) open arranged by the same rule; a click on a column ranks by that
     column and says so; the button returns the arrangement;
  5. opened from the chart's Leaders button the desk is arranged too; on a phone the switch adds no sideways scroll.
JPEGs are embedded (base64).  Nothing on the site changes.
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

BASE = "https://justhodl.ai"
UA = {"User-Agent": "Mozilla/5.0 jh-ops"}
RANKED = re.compile(r"^(newest|latest|strongest|weakest|deepest|heaviest|longest|largest|most|least|widest|narrowest|best|worst|highest|lowest|biggest|top)\b", re.I)
DESKS = [("Leaders", "leaders.html"), ("Strength", "strong.html"), ("Volatility", "stock-volatility.html"), ("Capitulation", "capitulation-desk.html")] + \
        [("Wyckoff " + t, "wyckoff-desk.html?tab=" + t) for t in ("sc", "capit", "bc", "spread", "spring", "accum", "bottom", "dist")]
KNOWN = "() => { const P = window.JHTearsheet && window.JHTearsheet.priority, i = P && P.leaders(); return !!(i && Object.keys(i.cap).length > 100) }"
SHEET = """() => { const el = document.querySelector('[data-jts]'), m = el && el.__model, P = window.JHTearsheet.priority; if (!m) return null;
  const pair = r => [P.ticker(r), P.cap(r)];
  return { state: el.dataset.state, hidden: !!el.hidden, order: m.stockOrder || null, saved: localStorage.getItem('jh-stock-order'), known: Object.keys((P.leaders() || { cap: {} }).cap).length,
    switchOn: [...el.querySelectorAll('[data-jts-order]')].map(b => b.getAttribute('data-jts-order') + '=' + b.getAttribute('aria-pressed')).join(' '),
    lists: (m.lists || []).concat(m.more || []).map(l => ({ key: l.key, title: l.title, arranged: !!(l.full && l.full.own), full: l.full ? l.full.rows.map(pair) : null, own: l.full && l.full.own ? l.full.own.rows.map(pair) : null,
      card: (l.rows || []).map(x => [x.name, x.__r ? P.ticker(x.__r) : null, x.__r ? P.cap(x.__r) : null]), ownCard: l.ownRows ? l.ownRows.map(x => x.name) : null })),
    dom: [...el.querySelectorAll('.jts-col')].map(c => ({ key: c.getAttribute('data-jts-list'), names: [...c.querySelectorAll('.jts-nm')].map(n => n.textContent) })) } }"""
TABLE = """(sel) => [...document.querySelectorAll(sel + ' tbody tr')].map(tr => { const a = tr.querySelector('a[href*="chart.html?s="], a.tk, [data-t]'); const t = tr.getAttribute('data-t') || (a ? (a.getAttribute('data-t') || (a.getAttribute('href') || '').split('s=')[1] || a.textContent) : ''); return decodeURIComponent(String(t || '')).trim().toUpperCase() }).filter(Boolean)"""
BARS = "() => [...document.querySelectorAll('[data-order-for],[data-ld-order-for]')].map(b => (b.getAttribute('data-order-for') || b.getAttribute('data-ld-order-for')) + ': ' + [...b.querySelectorAll('button,span')].map(x => x.textContent + (x.tagName === 'BUTTON' ? '=' + x.getAttribute('aria-pressed') : '')).join(' | '))"


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


def fetch(url, timeout=90):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def rule_breaks(pairs, lead):
    """Rows as [ticker, market value]: where the rule does not hold (empty = arranged).  lead = this op's own leaders."""
    def rank(t, c):
        is_lead = t in lead
        if c is None and is_lead and lead[t] > 0:
            c = lead[t]
        return (0 if is_lead else 1 if c is not None else 2, c or 0)
    out, prev = [], None
    for t, c in pairs:
        cur = rank(t, c)
        if prev is not None and not (prev[1][0] < cur[0] or (prev[1][0] == cur[0] and (cur[0] == 2 or prev[1][1] >= cur[1]))):
            out.append("%s %s before %s %s" % (prev[0], prev[1], t, cur))
        prev = (t, cur)
    return out


def open_desk(ctx, base, path, wait_sheet=True):
    page = ctx.new_page()
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)[:200]))
    page.goto("%s/%s" % (base, path), wait_until="domcontentloaded")
    if wait_sheet:
        page.wait_for_function("() => { const el = document.querySelector('[data-jts]'); return el && (el.dataset.state === 'ready' || el.dataset.state === 'hidden' || el.dataset.state === 'error') }", timeout=120000)
        try:
            page.wait_for_function(KNOWN, timeout=60000)
        except Exception:  # noqa: BLE001
            pass
        page.wait_for_timeout(1500)
    return page, errors


def check_sheet(name, s, lead, ck, log):
    arranged = [l for l in s["lists"] if l["arranged"]]
    bad, cards, kept = [], 0, 0
    dom = {d["key"]: d["names"] for d in s["dom"]}
    for l in arranged:
        if sorted(map(str, l["full"])) != sorted(map(str, l["own"])):
            bad.append("%s: the arranged list is not the desk's own records" % l["key"])
        br = rule_breaks(l["full"], lead)
        if br:
            bad.append("%s: %s" % (l["key"], br[0]))
        if l["card"]:
            cards += 1
            names = [c[0] for c in l["card"]]
            if l["ownCard"] is not None and sorted(names) != sorted(l["ownCard"]):
                bad.append("%s: the card lost or gained a row" % l["key"])
            ranking = bool(RANKED.search(l["title"] or "")) or (l["ownCard"] is not None and names[15:] == l["ownCard"][15:] and names[:15] != l["ownCard"][:15] and sorted(names[:15]) == sorted(l["ownCard"][:15]))
            head = l["card"][:15] if ranking else l["card"]
            if ranking and l["ownCard"] is not None:
                kept += 1
                if sorted(names[:15]) != sorted(l["ownCard"][:15]) or names[15:] != l["ownCard"][15:]:
                    bad.append("%s: a ranking card does not keep its own first fifteen" % l["key"])
            if all(c[1] for c in head):
                br = rule_breaks([(c[1], c[2]) for c in head], lead)
                if br:
                    bad.append("%s card: %s" % (l["key"], br[0]))
            shown = dom.get(l["key"])
            if shown is not None and shown != names[:len(shown)]:
                bad.append("%s: the card on screen is not the arranged rows" % l["key"])
    log("%-16s %2d lists, %2d arranged (%d with a card, %d of them ranking cards kept to their own first fifteen), %s rows arranged; leaders known to the page %d; switch [%s]%s" % (
        name, len(s["lists"]), len(arranged), cards, kept, "{:,}".format(sum(len(l["full"]) for l in arranged)), s["known"], s["switchOn"], "" if not bad else " | " + "; ".join(bad[:3])))
    ck(not bad, "%s: %s" % (name, "; ".join(bad[:4])))
    ck(s["order"] == "priority" if arranged else s["order"] is None, "%s: the sheet's order is %s with %d lists of stocks" % (name, s["order"], len(arranged)))
    return len(arranged), sum(len(l["full"]) for l in arranged)


def prove(r, ck, browser, base, lead, route=None, shots=None, caps=None):
    shots = {} if shots is None else shots
    caps = caps or {}
    ctx = browser.new_context(viewport={"width": 1440, "height": 1000})
    if route:
        ctx.route("**/*", route)

    r.section("2. Every tearsheet the chart opens: each list of names arranged, nothing left out")
    n_lists = n_rows = 0
    for name, path in DESKS:
        page, errors = open_desk(ctx, base, path)
        s = page.evaluate(SHEET)
        if not s or s["hidden"] or s["state"] != "ready":
            r.log("%-16s sheet state %s" % (name, s and s["state"]))
            ck(False, "%s: the sheet is not drawn (%s)" % (name, s and s["state"]))
            page.close()
            continue
        a, b = check_sheet(name, s, lead, ck, r.log)
        n_lists += a
        n_rows += b
        ck(not errors, "%s: page errors %s" % (name, errors[:2]))
        if name == "Leaders":
            shots["1_leaders_sheet"] = page.screenshot(type="jpeg", quality=60)
        page.close()
    r.log("in all: %d lists of names arranged, %s rows, each list the desk's own records in the rule's order" % (n_lists, "{:,}".format(n_rows)))

    r.section("3. The switch: the desk's own order, kept across a reload, and back")
    page, errors = open_desk(ctx, base, "leaders.html")
    s0 = page.evaluate(SHEET)
    page.evaluate("() => document.querySelector('[data-jts-order=engine]').click()")
    page.wait_for_timeout(900)
    s1 = page.evaluate(SHEET)
    same = all(not l["arranged"] for l in s1["lists"])
    own0 = {l["key"]: [c[0] for c in (l["ownCard"] and [[n] for n in l["ownCard"]] or l["card"])] for l in s0["lists"]}
    back = all([c[0] for c in l["card"]] == own0.get(l["key"]) for l in s1["lists"])
    r.log("THE DESK'S OWN ORDER pressed: sheet says %s, kept in the browser as '%s', switch [%s]; every list as the desk built it: %s; the desk's tables: %s" % (s1["order"], s1["saved"], s1["switchOn"], same and back, page.evaluate(BARS)))
    ck(s1["order"] == "engine" and s1["saved"] == "engine" and same and back, "the switch does not return the desk's own order")
    page.reload(wait_until="domcontentloaded")
    page.wait_for_function("() => { const el = document.querySelector('[data-jts]'); return el && el.dataset.state === 'ready' }", timeout=120000)
    page.wait_for_timeout(1500)
    s2 = page.evaluate(SHEET)
    r.log("after loading the page again: %s [%s]" % (s2["order"], s2["switchOn"]))
    ck(s2["order"] == "engine", "the choice is not kept across a reload")
    page.evaluate("() => document.querySelector('[data-jts-order=priority]').click()")
    page.wait_for_timeout(900)
    s3 = page.evaluate(SHEET)
    r.log("LEADERS - LARGEST FIRST pressed: %s, kept as '%s'; first names of the first three lists of stocks: %s" % (s3["order"], s3["saved"], [(l["title"], [c[1] for c in l["card"][:4]]) for l in s3["lists"] if l["arranged"] and l["card"]][:3]))
    ck(s3["order"] == "priority" and s3["saved"] == "priority" and sorted(str(l["card"]) for l in s3["lists"]) == sorted(str(l["card"]) for l in s0["lists"]), "arranged again is not what it was")

    r.section("4. The desks' own tables: arranged, a clicked column wins, the button brings the arrangement back")
    rows = page.evaluate(TABLE, "#lead")
    br = rule_breaks([(t, lead.get(t)) for t in rows], lead)
    r.log("Leaders, the table of leaders: %d rows drawn, first %s; %s; bars: %s" % (len(rows), rows[:8], "largest first throughout" if not br else br[0], page.evaluate(BARS)))
    ck(len(rows) > 50 and not br, "the leaders table is not largest first: %s" % (br[:1],))
    page.evaluate("() => { const th = [...document.querySelectorAll('#lead th[data-sort]')].find(x => /strength|vs|month|rank/i.test(x.getAttribute('data-sort'))) || document.querySelectorAll('#lead th[data-sort]')[3]; th.click() }")
    page.wait_for_timeout(700)
    clicked = page.evaluate(TABLE, "#lead")
    bars = page.evaluate(BARS)
    r.log("a column clicked: first rows now %s; the table says: %s" % (clicked[:6], [b_ for b_ in bars if b_.startswith("lead")]))
    ck(clicked[:20] != rows[:20] and any("Ranked by the column you clicked" in b_ for b_ in bars if b_.startswith("lead")), "a clicked column does not take over")
    page.evaluate("() => document.querySelector('[data-ld-order-for=lead] [data-ld-order=priority]').click()")
    page.wait_for_timeout(700)
    again = page.evaluate(TABLE, "#lead")
    r.log("Largest first pressed: first rows %s" % again[:6])
    ck(again[:40] == rows[:40], "the button does not bring the arrangement back")
    shots["2_leaders_table"] = page.screenshot(type="jpeg", quality=60, clip=page.evaluate("() => { const b = document.getElementById('lead').getBoundingClientRect(); window.scrollTo(0, window.scrollY + b.top - 140); const c = document.getElementById('lead').getBoundingClientRect(); return { x: 0, y: Math.max(0, c.top - 130), width: innerWidth, height: 620 } }"))
    page.close()
    for name, path, sels, doc in (("Volatility", "stock-volatility.html", ["#rows", "#board"], "sheet"), ("Strength", "strong.html", ["#names-tbl"], "sheet"),
                                  ("Capitulation", "capitulation-desk.html", ["#tbl"], None), ("Wyckoff accum", "wyckoff-desk.html?tab=accum", ["#scan-board"], "scan"),
                                  ("Wyckoff bottom", "wyckoff-desk.html?tab=bottom", ["#scan-board"], "scan"), ("Wyckoff dist", "wyckoff-desk.html?tab=dist", ["#scan-board"], "scan")):
        page, errors = open_desk(ctx, base, path)
        page.wait_for_timeout(1500)
        cap = dict(caps) if doc == "scan" else {}
        if doc and doc != "scan":      # the market values this desk itself holds: every record on its own tearsheet
            cap = page.evaluate("() => { const el = document.querySelector('[data-jts]'), m = el && el.__model, P = window.JHTearsheet.priority, out = {}; ((m && m.lists) || []).concat((m && m.more) || []).forEach(l => ((l.full && l.full.rows) || []).forEach(x => { const t = P.ticker(x), c = P.cap(x); if (t && c) out[t] = c })); return out }")
        for sel in sels:
            got = page.evaluate(TABLE, sel)
            br = rule_breaks([(t, cap.get(t)) for t in got], lead) if cap or not doc else ["no market values to check against"]
            if doc is None:      # the capitulation feed carries no market value: leaders first, the rest in the desk's own order
                flags = [t in lead for t in got]
                br = [] if flags == sorted(flags, reverse=True) else ["a leader stands below a name that is not one"]
            r.log("%-15s %-12s %4d rows drawn, first %s; %s" % (name, sel, len(got), got[:7], "arranged by the rule" if not br else br[0]))
            ck(len(got) > 0 and not br, "%s %s is not arranged: %s" % (name, sel, br[:1]))
        ck(not errors, "%s: page errors %s" % (name, errors[:2]))
        if name == "Wyckoff bottom":
            shots["3_wyckoff_bottom_board"] = page.screenshot(type="jpeg", quality=60, clip=page.evaluate("() => { const e = document.getElementById('scan-panel'); e.scrollIntoView({ block: 'start' }); const c = e.getBoundingClientRect(); return { x: 0, y: Math.max(0, c.top), width: innerWidth, height: 640 } }"))
            page.evaluate("() => document.querySelector('#scan-panel [data-sc-order=engine]').click()")
            page.wait_for_timeout(700)
            own = page.evaluate(TABLE, "#scan-board")
            page.evaluate("() => document.querySelector('#scan-panel [data-sc-order=priority]').click()")
            page.wait_for_timeout(700)
            r.log("   the scan's own order pressed: first %s; and back: first %s" % (own[:6], page.evaluate(TABLE, "#scan-board")[:6]))
            ck(own[:30] != got[:30] and page.evaluate(TABLE, "#scan-board")[:30] == got[:30], "the board's own switch does not work")
        page.close()

    r.section("5. From the chart's Leaders button; and a phone")
    page = ctx.new_page()
    page.goto("%s/chart.html?s=NVDA&tf=1d" % base, wait_until="domcontentloaded")
    page.wait_for_function("() => window.lastBars && window.lastBars.length > 100 && document.getElementById('btn-ldr')", timeout=120000)
    page.wait_for_timeout(2500)
    page.evaluate("() => document.getElementById('btn-ldr').click()")
    frame = None
    for _ in range(60):
        frame = next((f for f in page.frames if "leaders.html" in (f.url or "")), None)
        if frame:
            try:
                if frame.evaluate("() => { const el = document.querySelector('[data-jts]'); return !!(el && el.dataset.state === 'ready') }"):
                    break
            except Exception:  # noqa: BLE001
                pass
        page.wait_for_timeout(1000)
    ok_frame = False
    if frame:
        frame.wait_for_timeout(2500)
        fs = frame.evaluate(SHEET)
        ok_frame = bool(fs and fs["order"] == "priority" and any(l["arranged"] for l in fs["lists"]))
        r.log("the Leaders button on the chart opened %s: the sheet there is %s, switch [%s]" % (frame.url.split("/")[-1][:40], fs and fs["order"], fs and fs["switchOn"]))
    ck(ok_frame, "the desk opened from the chart is not arranged")
    page.close()
    ctx.close()
    ctx = browser.new_context(viewport={"width": 390, "height": 844}, device_scale_factor=2, has_touch=True, is_mobile=True)
    if route:
        ctx.route("**/*", route)
    for name, path in (("Leaders", "leaders.html"), ("Wyckoff bottom", "wyckoff-desk.html?tab=bottom")):
        page, errors = open_desk(ctx, base, path)
        ph = page.evaluate("() => { const el = document.querySelector('[data-jts]'), o = el.querySelector('[data-jts-ord]'), r = o ? o.getBoundingClientRect() : null; return { sideways: document.documentElement.scrollWidth - innerWidth, switch: !!o, right: r ? Math.round(r.right) : null, order: (el.__model || {}).stockOrder } }")
        r.log("%-15s on a 390 px phone: order %s, the switch is there %s (its right edge at %s px), sideways scroll %d px" % (name, ph["order"], ph["switch"], ph["right"], ph["sideways"]))
        ck(ph["switch"] and ph["order"] == "priority" and ph["sideways"] <= 0, "%s on a phone: %s" % (name, ph))
        if name == "Leaders":
            page.evaluate("() => document.querySelector('[data-jts]').scrollIntoView({ block: 'start' })")
            page.wait_for_timeout(300)
            shots["4_phone"] = page.screenshot(type="jpeg", quality=55)
        page.close()
    ctx.close()
    return n_lists, n_rows


def main():
    from ops_report import report
    with report("ops_6931_stock_order_live_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6931 - leaders and the largest companies first on every desk the chart opens, real browser on the live site (read-only), %s" % now.isoformat(timespec="minutes"))
        stamp = int(time.time())
        fails, shots = [], {}

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL: " + msg)
            return bool(cond)

        r.section("0. The live site serves the new build")
        want = {"jh-tearsheet.js": "version: '1.8.0'", "jh-leaders.js": "function largestFirst(table)", "jh-strong-today.js": "function largestFirst(table)", "jh-stock-volatility.js": "function largestFirst(table)",
                "jh-wyckoff-scan.js": "data-sc-order", "capitulation-desk.html": "leadersFirst("}
        served, got = False, {}
        for attempt in range(40):
            try:
                got = {f: fetch("%s/%s?nocache=%d" % (BASE, f, stamp + attempt)) for f in want}
            except Exception as exc:  # noqa: BLE001
                r.log("try %d: %s" % (attempt + 1, str(exc)[:90]))
                time.sleep(30)
                continue
            served = all(mark in got[f] for f, mark in want.items())
            if served:
                break
            time.sleep(30)
        r.log("served after %d tries: %s" % (attempt + 1, {f: want[f] in got.get(f, "") for f in want}))
        if not served:
            r.fail("the served site is not the new build yet (Pages deploy pending?)")
            raise SystemExit(1)

        r.section("1. This op's own yardstick: the Leaders desk's feed, read here")
        doc = json.loads(fetch("%s/data/leaders.json?nocache=%d" % (BASE, stamp)))
        lead = {str(x["ticker"]).upper(): (x.get("mcap") or 0) for x in doc.get("rows", []) if x.get("ticker")}
        top = sorted(lead.items(), key=lambda kv: -kv[1])[:6]
        r.log("data/leaders.json as of %s: %d leaders in %d industries; the largest: %s" % (doc.get("as_of"), len(lead), len(doc.get("industries", [])), ", ".join("%s $%.2fT" % (t, c / 1e12) for t, c in top)))
        ck(len(lead) > 400, "the leaders feed holds %d leaders" % len(lead))
        caps = {}
        try:
            scan = json.loads(fetch("%s/data/campaign-scan.json?nocache=%d" % (BASE, stamp)))
            for rows_ in (scan.get("boards") or {}).values():
                for x in rows_:
                    if x.get("ticker") and (x.get("mcap") or 0) > 0:
                        caps[str(x["ticker"]).upper()] = x["mcap"]
            r.log("data/campaign-scan.json as of %s: market values for %d names on its three boards" % (scan.get("as_of"), len(caps)))
        except Exception as exc:  # noqa: BLE001
            r.log("campaign scan not readable here: %s" % str(exc)[:80])

        sync_playwright = ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = launch(p, r.log)
            n_lists, n_rows = prove(r, ck, browser, BASE, lead, shots=shots, caps=caps)
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            r.log("%s: %d bytes" % (k, len(img)))
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(desks=len(DESKS), lists_arranged=n_lists, rows_arranged=n_rows, failed_checks=len(fails))
        if fails:
            for f in fails:
                r.fail(f)
            raise SystemExit(1)
        r.ok("LIVE: %d lists of names (%s rows) on %d sheets open with the leaders and the largest companies first, nothing left out; the desks' own tables too; the desk's own order is one button away and kept" % (n_lists, "{:,}".format(n_rows), len(DESKS)))


if __name__ == "__main__":
    main()
    sys.exit(0)
