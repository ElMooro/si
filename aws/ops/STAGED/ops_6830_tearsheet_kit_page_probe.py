"""ops 6830 - the tearsheet kit on eight desks: live page proof in headless Chrome (read-only, re-runnable).

Khalid 2026-11-17: "apply these concepts from the picture I attached and that donut everywhere in my system where
you see it's necessary." The kit (/jh-tearsheet.js) puts one sheet - headline, six tiles, the two most notable
moves, three notes, a ring beside its list, ranked lists, a source line - at the top of each desk that has both a
whole that adds up and items that can be ranked: ARK holdings, stablecoin supply, the firm book, the paper book, the
S&P 500, insider buying, foreign holders of Treasuries and cross-border transactions. Additive: nothing on those
pages was removed. Drives each page on https://justhodl.ai at 1440 px and 390 px and checks:
  1. the sheet finishes (state ready), has six tiles, its cards, notes, lists and a ring - printed here as shown;
  2. the ring is honest: its slices add up to 100% and its total equals the same sum taken from the page's own feed,
     fetched here separately (ARK: every position's value; stablecoins: total supply; firm book: gross exposure;
     paper book: positions + cash; S&P 500: sector weights; insiders: the scored set, one row per company;
     Treasuries: all foreign holdings; cross-border: the month's gross); a two-sided ring is solid / hatched;
  3. nothing lost and nothing renumbered: the page's own blocks still render, the sheet carries no section number
     and adds no section badge (it is kept out of jh-sections so existing addresses do not move);
  4. no 'undefined' / 'NaN', the sheet adds no horizontal overflow at 390 px, no page errors; the figures column of
     every list is right-aligned (a later style rule had left-aligned it in the first release of the day);
  5. the controls work: MORE ROWS lengthens the lists; the ARK fund buttons narrow the sheet to one fund.
JPEGs are embedded (base64). Nothing on the site changes.
"""
from pathlib import Path
import base64
import json
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

BASE = "https://justhodl.ai"
# page, adapter, feed, a block of the page's own that must still be painted
DESKS = [("ark.html", "ark", "data/ark-holdings.json", "#hero"), ("stablecoin-flow.html", "stablecoin", "data/stablecoin-flow.json", "#root"),
         ("firm-book.html", "firmbook", "data/firm-book.json", "#root"), ("paper-book.html", "paperbook", "data/paper-book.json", "#wrap"),
         ("sp500.html", "sp500", "data/sp500.json", "#cards"), ("insider-clusters.html", "insiders", "data/insider-clusters.json", "#kpi-strip"),
         ("foreign-flows.html", "tic", "data/tic-flows.json", "#cards"), ("capital-inflows.html", "capital", "data/capital-inflows.json", "#cards")]
SHOTS = {("ark", 1440): "ark_desktop", ("capital", 1440): "capital_desktop", ("tic", 1440): "treasury_holders_desktop", ("firmbook", 390): "firm_book_mobile", ("sp500", 390): "sp500_mobile"}


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


def expected(name, d):
    """The whole each ring claims, computed from the feed independently of the page."""
    if name == "ark":
        return sum(h.get("market_value") or 0 for rows in d["holdings_by_fund"].values() for h in rows)
    if name == "stablecoin":
        return d["aggregate"]["total_usd"]
    if name == "firmbook":
        return sum(abs(p["gross_pct"] if p.get("gross_pct") is not None else p["net_pct"]) for p in d["equity_book"] if p.get("net_pct"))
    if name == "paperbook":
        return sum(p["weight_pct"] for p in d["positions"]) + max(d.get("cash_pct") or 0, 0)
    if name == "sp500":
        return sum(s["weight_pct"] for s in d["sectors"].values())
    if name == "insiders":
        best = {}
        for c in sorted(d["clusters"], key=lambda c: -(c.get("score") or 0)):
            best.setdefault(c.get("cik") or c["ticker"], c)
        return sum(c["total_value"] for c in best.values())
    if name == "tic":
        return d["total_foreign_holdings"]["current_b"]
    if name == "capital":
        return sum(abs(c["latest_month_b"]) for c in d["by_asset_class"].values())
    raise KeyError(name)


SHEET = r"""() => { const s = document.querySelector('[data-jts]'); const q = x => [...s.querySelectorAll(x)]; const t = e => e ? (e.innerText || e.textContent || '').replace(/\s+/g, ' ').trim() : '';
  const r = s.getBoundingClientRect(), arcs = q('.jts-arc'), col = s.querySelector('.jts-ringcol');
  const ox = (() => { const f = () => document.documentElement.scrollWidth - document.documentElement.clientWidth; const w = f(); s.style.display = 'none'; const wo = f(); s.style.display = ''; return w - wo; })();
  return { state: s.dataset.state, eye: t(s.querySelector('.jts-eye')), head: t(s.querySelector('.jts-h')), sub: t(s.querySelector('.jts-sub')), tiles: q('.jts-kpi').map(t), cards: q('.jts-card').map(e => t(e).slice(0, 330)),
    notes: q('.jts-notes p').map(e => t(e).slice(0, 260)), ctl: q('[data-jts-ctl]').map(t),
    cols: q('.jts-col').map(c => ({ k: c.dataset.jtsList, head: t(c.querySelector('.jts-ch')), rows: c.querySelectorAll('.jts-t tbody tr').length, first: t(c.querySelector('tbody tr')).slice(0, 170) })),
    arcs: arcs.length, share: arcs.reduce((a, e) => a + parseFloat(e.dataset.share), 0), up: arcs.filter(e => e.classList.contains('up')).length, down: arcs.filter(e => e.classList.contains('down')).length,
    hatched: arcs.filter(e => (e.getAttribute('fill') || '').startsWith('url(#jts-hatch-')).length, hatchDefs: s.querySelectorAll('pattern[id^="jts-hatch-"]').length, total: col ? +col.dataset.total : null,
    center: t(s.querySelector('.jts-rc')) + ' | ' + q('.jts-rs').map(t).join(' | '), labels: q('.jts-rl').map(t), legend: q('.jts-leg tr').map(e => t(e).slice(0, 110)), foot: t(s.querySelector('.jts-foot')).slice(0, 300),
    bad: (s.innerText.match(/undefined|NaN|\[object|\bnull\b|Infinity/g) || []).length, w: Math.round(r.width), h: Math.round(r.height), ox,
    wide: q('*').filter(e => !e.closest('defs') && e.tagName !== 'title' && (e.getBoundingClientRect().right > r.right + 1 || e.getBoundingClientRect().left < r.left - 1)).length,
    sec: s.getAttribute('data-jh-sec'), chrome: s.hasAttribute('data-jh-chrome'), badges: s.querySelectorAll('.jh-secbadge').length, numbered: s.querySelectorAll('[data-jh-sec]').length,
    sections: [...document.querySelectorAll('[data-jh-sec]')].filter(e => !s.contains(e)).length, ink: getComputedStyle(s.querySelector('.jts-h')).color, bg: getComputedStyle(s.querySelector('.jts-in')).backgroundColor,
    outside: document.body.innerText.length - s.innerText.length, version: (window.JHTearsheet || {}).version || null,
    align: (() => { const n = s.querySelector('.jts-t td.n'); return n ? getComputedStyle(n).textAlign : null; })(), scrollMargin: getComputedStyle(s).scrollMarginTop }; }"""


def main():
    with report("ops_6830_tearsheet_kit_page_probe") as r:
        r.heading("ops 6830 - the tearsheet kit on eight desks, live page proof")
        stamp = int(time.time())
        ua = {"User-Agent": "Mozilla/5.0 jh-ops"}

        def get(path):
            return urllib.request.urlopen(urllib.request.Request("%s/%s%snocache=%d" % (BASE, path, "&" if "?" in path else "?", stamp), headers=ua), timeout=60).read()

        kit = get("jh-tearsheet.js").decode("utf-8", "replace")
        ok_kit = "ADAPTERS.capital" in kit and "data-jh-chrome" in kit and "ADAPTERS.insiders" in kit and ".jts .jts-t .n{text-align:right" in kit
        r.log("jh-tearsheet.js: %d bytes, ascii %s, eight adapters %s" % (len(kit), all(ord(c) < 128 for c in kit), ok_kit))
        if not ok_kit:
            r.fail("the served kit is not the new one yet (Pages deploy pending?)")
            raise SystemExit(1)
        feeds = {}
        for _page, name, feed, _own in DESKS:
            feeds[name] = json.loads(get(feed))
        sync_playwright = ensure_playwright(r.log)
        fails, shots, summary = [], {}, []

        def ck(cond, msg):
            if not cond:
                fails.append(msg)

        with sync_playwright() as p:
            browser = launch(p, r.log)
            for page_name, name, feed, own in DESKS:
                r.section("%s (%s)" % (page_name, feed))
                want = expected(name, feeds[name])
                for width in (1440, 390):
                    ctx = browser.new_context(viewport={"width": width, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
                    page = ctx.new_page()
                    errors = []
                    page.on("pageerror", lambda e, errors=errors: errors.append(str(e)[:160]))
                    try:
                        page.goto("%s/%s?probe=%d" % (BASE, page_name, stamp), wait_until="domcontentloaded", timeout=60000)
                        page.wait_for_function("() => { const s = document.querySelector('[data-jts]'); return s && (s.dataset.state === 'ready' || s.dataset.state === 'error'); }", timeout=45000)
                    except Exception as e:  # noqa: BLE001
                        ck(False, "%s @%d: the sheet never finished (%s)" % (page_name, width, str(e)[:100]))
                        ctx.close()
                        continue
                    page.wait_for_timeout(2500)
                    s = page.evaluate(SHEET)
                    tag = "%s @%d" % (page_name, width)
                    if width == 1440:
                        r.log("**%s**" % s["eye"])
                        r.log("head: %s" % s["head"])
                        r.log("sub : %s" % s["sub"])
                        for t in s["tiles"]:
                            r.log("  tile: %s" % t)
                        for c in s["cards"]:
                            r.log("  card: %s" % c)
                        for n in s["notes"]:
                            r.log("  note: %s" % n)
                        r.log("  ring: %d slices (%d solid-up, %d hatched-down), shares sum %.3f%%, centre %s, labels %s" % (s["arcs"], s["up"], s["down"], s["share"], s["center"], s["labels"]))
                        for leg in s["legend"]:
                            r.log("    %s" % leg)
                        for c in s["cols"]:
                            if c["k"] != "ring":
                                r.log("  list %-9s %s | %d rows | %s" % (c["k"], c["head"], c["rows"], c["first"]))
                        r.log("  foot: %s" % s["foot"])
                        tol = max(abs(want) * 2e-6, 1e-6)
                        r.log("  ring total on the page %.6g, the same sum from the feed %.6g (difference %.3g)" % (s["total"] or 0, want, abs((s["total"] or 0) - want)))
                        ck(s["total"] is not None and abs(s["total"] - want) <= tol, "%s: ring total %s is not the feed's %s" % (tag, s["total"], want))
                        if name == "firmbook":
                            firm = feeds[name]["firm"]
                            r.log("  firm.gross_exposure_pct %.3f vs ring %.3f" % (firm["gross_exposure_pct"], s["total"] or 0))
                            ck(abs((s["total"] or 0) - firm["gross_exposure_pct"]) <= 0.2, "%s: ring gross %.3f vs the firm's stated gross %.3f" % (tag, s["total"] or 0, firm["gross_exposure_pct"]))
                        if name == "capital":
                            net = sum(c["latest_month_b"] for c in feeds[name]["by_asset_class"].values())
                            r.log("  security types sum to %.3f, the feed's headline month %.3f" % (net, feeds[name]["headline"]["latest_month_b"]))
                            ck(abs(net - feeds[name]["headline"]["latest_month_b"]) <= 0.002, "%s: the four security types do not sum to the headline" % tag)
                        if name in ("paperbook", "sp500"):
                            ck(abs(want - 100) <= 0.6, "%s: the feed's own parts sum to %.3f, not 100" % (tag, want))
                        own_text = page.evaluate("(sel) => { const e = document.querySelector(sel); return e ? e.innerText.trim().length : -1; }", own)
                        r.log("  the page's own %s: %d characters; text outside the sheet %d; numbered sections on the page %d; sheet numbered: %s, badges inside %d" % (own, own_text, s["outside"], s["sections"], s["sec"], s["badges"]))
                        ck(own_text > 40 and s["outside"] > 1500, "%s: the page's own content is missing (%s %d, outside %d)" % (tag, own, own_text, s["outside"]))
                        more = page.locator('[data-jts-act="more"]')
                        if more.count():
                            before = page.evaluate("() => document.querySelectorAll('[data-jts] .jts-t tbody tr').length")
                            more.click()
                            page.wait_for_timeout(200)
                            after = page.evaluate("() => document.querySelectorAll('[data-jts] .jts-t tbody tr').length")
                            r.log("  MORE ROWS: %d -> %d rows" % (before, after))
                            ck(after > before, "%s: MORE ROWS did nothing" % tag)
                            page.locator('[data-jts-act="more"]').click()
                            page.wait_for_timeout(150)
                        if name == "ark":
                            fund = feeds[name]["funds_tracked"][0]
                            page.locator('[data-jts-ctl="%s"]' % fund).click()
                            page.wait_for_timeout(300)
                            k = page.evaluate(SHEET)
                            fund_total = sum(h.get("market_value") or 0 for h in feeds[name]["holdings_by_fund"][fund])
                            r.log("  %s view: %s | ring total %.6g vs the fund's positions %.6g" % (fund, k["head"], k["total"] or 0, fund_total))
                            ck(k["head"].startswith(fund) and abs((k["total"] or 0) - fund_total) <= max(fund_total * 2e-6, 1e-6) and k["bad"] == 0, "%s: the %s view is wrong" % (tag, fund))
                            page.locator('[data-jts-ctl="ALL"]').click()
                            page.wait_for_timeout(200)
                    r.log("  @%d: sheet %dx%d, state %s, %d tiles, %d cards, %d lists, bad tokens %d, added overflow %d px, wider-than-sheet %d, ink %s on %s, errors %d" % (
                        width, s["w"], s["h"], s["state"], len(s["tiles"]), len(s["cards"]), len(s["cols"]) - 1, s["bad"], s["ox"], s["wide"], s["ink"], s["bg"], len(errors)))
                    ck(s["state"] == "ready", "%s: state %s" % (tag, s["state"]))
                    ck(len(s["tiles"]) == 6 and 1 <= len(s["cards"]) <= 2 and len(s["notes"]) == 3 and len(s["cols"]) >= 4, "%s: anatomy %d tiles %d cards %d notes %d columns" % (tag, len(s["tiles"]), len(s["cards"]), len(s["notes"]), len(s["cols"])))
                    ck(s["arcs"] >= 2 and abs(s["share"] - 100) < 0.05, "%s: ring %d slices sum %.3f" % (tag, s["arcs"], s["share"]))
                    ck(s["hatched"] == s["down"] and (s["down"] == 0 or s["hatchDefs"] >= 1), "%s: %d down slices but %d hatched" % (tag, s["down"], s["hatched"]))
                    if name in ("firmbook", "capital"):
                        ck(s["up"] + s["down"] == s["arcs"], "%s: a two-sided ring has an untyped slice" % tag)
                    ck(s["bad"] == 0, "%s: %d broken values" % (tag, s["bad"]))
                    ck(s["align"] == "right", "%s: the figures column is aligned %s, not right" % (tag, s["align"]))
                    ck(s["ox"] <= 0 and s["wide"] == 0, "%s: the sheet overflows (page +%d px, %d elements wider than it)" % (tag, s["ox"], s["wide"]))
                    ck(s["chrome"] and s["sec"] is None and s["badges"] == 0 and s["numbered"] == 0, "%s: the sheet is numbered as a section (%s, %d badges)" % (tag, s["sec"], s["badges"]))
                    ck(not errors, "%s: page errors %s" % (tag, errors[:2]))
                    key = SHOTS.get((name, width))
                    if key:
                        try:
                            box = page.locator("[data-jts]").bounding_box()
                            top = box["y"] + page.evaluate("window.scrollY")
                            shots[key] = page.screenshot(type="jpeg", quality=56, full_page=True, clip={"x": max(box["x"] - 2, 0), "y": max(top - 4, 0), "width": box["width"] + 4, "height": min(box["height"] + 8, 1700 if width == 1440 else 2100)})
                        except Exception as e:  # noqa: BLE001
                            r.log("  screenshot %s failed: %s" % (key, str(e)[:100]))
                    if width == 1440:
                        summary.append("%s: %s" % (page_name, s["head"]))
                    ctx.close()
            browser.close()
        r.section("what each desk now says at the top")
        for line in summary:
            r.log(line)
        r.section("screenshots (jpeg, base64)")
        for key, img in shots.items():
            r.lines.append("<!-- SHOT %s %d bytes" % (key, len(img)))
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(desks=str(len(DESKS)), checks_failed=str(len(fails)), screenshots=str(len(shots)))
        if fails:
            for f in fails:
                r.fail(f)
            raise SystemExit(1)
        r.ok("LIVE PAGE: the tearsheet and its ring are on %d desks; every ring total equals the same sum taken from the desk's own feed; nothing on those pages was removed or renumbered; no broken value, no overflow, no page error at 1440 or 390 px" % len(DESKS))


if __name__ == "__main__":
    main()
    sys.exit(0)
