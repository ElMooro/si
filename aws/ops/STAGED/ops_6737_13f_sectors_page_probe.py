"""ops 6737 - READ-ONLY browser proof of the 13F page with the sector layer (book 1.2.0; CLAUDE.md rule 5: click it).
Same checks as ops 6728 plus: the "Sectors - where the money went" section renders the money-in / money-out tiles, the
sector table and the industry table from the live book; the sector filter and the BY NET $ / BY VALUE / BY TILT sorts
re-render; stance and contender rows carry industry chips and a net-$ column.

Against https://justhodl.ai/13f.html in headless Chrome, with the live data/13f-book.json (1.1.0 / 13f-book.v2):
the header pill shows the CUSIP-master coverage; the banner tile leads with the net-manager stance; the smart-money
section renders the stance counts and the bullish / bearish / contested tables; the asset-class rotation bars and
table render for the cohort; the contenders tables render; the boards read the ALL universe; the security-master
panel shows coverage, the unclassified-ETF list and the unmapped list; the drill-down carries tickers, kind chips and
the asset mix; switching the universe to ETFs / STOCKS and the scope to CONCENTRATED re-renders every smart-money
section; selecting a manager moves the drill-down and the evidence desk; no [object Object]/undefined/NaN literal is
visible; no horizontal overflow at 390 px. A 0.5x full-page JPEG is embedded (base64). Nothing on the site changes.
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
IDS = ["h-pills", "book-state", "banner-headline", "verdicts", "smart-meta", "stance-counts", "rot-title", "rot-meta", "rot-foot", "sec-title", "sec-meta", "ind-meta", "sec-foot", "cont-title", "cont-meta",
       "boards-title", "boards-meta", "master-meta", "master-kpis", "master-foot", "drill-title", "state", "page-count"]
COUNTS = {"mgr_rows": "#mgr-table tbody tr", "stance_tables": "#stance table", "stance_rows": "#stance tbody tr", "rot_rows": "#rotation tbody tr", "rot_bars": "#rot-bars .stack",
          "sector_rows": "#sectors tbody tr", "industry_rows": "#industries tbody tr", "flow_tiles": "#sec-flows .sub", "flow_lines": "#sec-flows .kv", "ind_filter_buttons": "#ind-filter button",
          "cont_tables": "#contenders table", "cont_rows": "#contenders tbody tr", "boards_tables": "#boards table", "boards_rows": "#boards tbody tr",
          "master_kpis": "#master-kpis .kpi", "master_tables": "#master-lists table", "unclassified_rows": "#master-lists .sub:nth-child(1) tbody tr",
          "drill_tickers": "#drill-body .tk", "drill_kind_chips": "#drill-body .c-st, #drill-body .c-et, #drill-body .c-db", "drill_mix_bars": "#drill-body .stack",
          "conc_cards": "#conc > .sub", "clone_rows": "#clone tbody tr", "engines_rows": "#engines tbody tr", "selection_options": "#selection option", "positions_rows": "#positions tr"}


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


def text(page, sel):
    try:
        return " ".join(page.locator(sel).first.inner_text(timeout=3000).split())
    except Exception as e:  # noqa: BLE001
        return "<%s>" % type(e).__name__


def main():
    with report("ops_6737_13f_sectors_page_probe") as r:
        r.heading("ops 6737 - 13F book: sector layer live page proof in headless Chrome (read-only)")
        r.section("0. served HTML + book over the public route")
        stamp = int(time.time())
        req = urllib.request.Request("%s/13f.html?nocache=%d" % (BASE, stamp), headers={"User-Agent": "Mozilla/5.0 jh-ops"})
        html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "replace")
        new_markup = 'id="stance"' in html and 'id="rotation"' in html and 'id="sectors"' in html and 'id="industries"' in html and 'id="ind-sort-net"' in html and 'id="contenders"' in html
        r.log("13f.html: %d bytes, smart-money markup %s" % (len(html), new_markup))
        req = urllib.request.Request("%s/data/13f-book.json?nocache=%d" % (BASE, stamp), headers={"User-Agent": "Mozilla/5.0 jh-ops"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
            r.log("data/13f-book.json: HTTP %s, %d bytes, cache-control %s" % (resp.status, len(body), resp.headers.get("Cache-Control")))
        book = json.loads(body)
        b = book.get("book") or {}
        cov = (b.get("security_master") or {}).get("coverage") or {}
        r.log("book %s %s generated_at %s | master %s/%s resolved (%s%% of value) | stance cohort/all bullish %s bearish %s | rotation buckets %s" % (
            book.get("version"), book.get("contract"), book.get("generated_at"), cov.get("resolved"), cov.get("identities"), cov.get("resolved_value_pct"),
            (((b.get("stance") or {}).get("cohort") or {}).get("all") or {}).get("n_bullish"), (((b.get("stance") or {}).get("cohort") or {}).get("all") or {}).get("n_bearish"),
            len((((b.get("rotation") or {}).get("cohort") or {}).get("buckets") or []))))
        if not new_markup:
            r.fail("the served 13f.html does not carry the smart-money sections yet (Pages deploy pending?)")
            raise SystemExit(1)
        if book.get("contract") != "13f-book.v2" or not (b.get("sectors") or {}).get("cohort"):
            r.fail("the live book carries no sectors block (%s %s) - run ops 6736 first" % (book.get("version"), book.get("contract")))
            raise SystemExit(1)

        sync_playwright = ensure_playwright(r.log)
        facts = {}
        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
            page = ctx.new_page()
            errors, bad_responses = [], []
            page.on("pageerror", lambda e: errors.append("pageerror: %s" % e))
            page.on("console", lambda m: errors.append("console: %s" % m.text) if m.type == "error" else None)
            page.on("response", lambda resp: bad_responses.append("%s %s" % (resp.status, resp.url)) if resp.status >= 400 else None)
            r.section("1. desktop 1440: load")
            page.goto("%s/13f.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            try:
                page.wait_for_selector("#stance table", timeout=30000)
            except Exception as e:  # noqa: BLE001
                r.fail("stance tables never rendered (%s): book-state = %s" % (type(e).__name__, text(page, "#book-state")))
            try:
                page.wait_for_selector("#positions tr", timeout=45000)
            except Exception as e:  # noqa: BLE001
                r.warn("evidence desk positions table not rendered (%s)" % type(e).__name__)
            page.wait_for_timeout(1500)
            for i in IDS:
                t = text(page, "#" + i)
                facts[i] = t
                r.log("%s: %s" % (i, t[:420]))
            counts = {k: page.locator(sel).count() for k, sel in COUNTS.items()}
            r.log("counts: %s" % json.dumps(counts))
            facts.update(counts)
            r.log("stance bullish table: %s" % text(page, "#stance .sub:nth-child(1)")[:700])
            r.log("stance bearish table: %s" % text(page, "#stance .sub:nth-child(2)")[:700])
            r.log("rotation row 1: %s" % text(page, "#rotation tbody tr")[:400])
            r.log("rotation legend: %s" % text(page, "#rot-bars .leg")[:300])
            r.log("contenders rising: %s" % text(page, "#contenders .sub:nth-child(1)")[:500])
            r.log("boards first table: %s" % text(page, "#boards table")[:400])
            r.log("master lists: %s" % text(page, "#master-lists")[:600])
            r.log("drill mix: %s" % text(page, "#drill-body .sub:has(.stack)")[:400])
            r.log("sector flows: %s" % text(page, "#sec-flows")[:700])
            r.log("sectors row 1: %s" % text(page, "#sectors tbody tr")[:500])
            r.log("industries row 1: %s" % text(page, "#industries tbody tr")[:500])
            r.log("stance row 1 (industry chip + net $): %s" % text(page, "#stance tbody tr")[:300])
            bad = page.evaluate("() => (document.body.innerText.match(/\\[object Object\\]|undefined|NaN/g) || []).length")
            r.log("forbidden literals visible: %s" % bad)
            facts["bad_literals"] = bad

            r.section("2. interactions")
            page.click("#univ-etfs")
            page.wait_for_timeout(500)
            facts["etfs_boards_title"] = text(page, "#boards-title")
            facts["etfs_stance_rows"] = page.locator("#stance tbody tr").count()
            facts["etfs_kind_chips"] = page.locator("#stance .c-et").count()
            facts["etfs_stock_chips"] = page.locator("#stance .c-st").count()
            r.log("after ETFs: boards %s | stance rows %s (etf chips %s, stock chips %s) | contenders %s" % (facts["etfs_boards_title"], facts["etfs_stance_rows"], facts["etfs_kind_chips"], facts["etfs_stock_chips"], text(page, "#cont-meta")))
            r.log("ETF bullish: %s" % text(page, "#stance .sub:nth-child(1)")[:500])
            r.log("ETF widely held: %s" % text(page, "#boards table")[:400])
            page.click("#univ-stocks")
            page.wait_for_timeout(500)
            facts["stocks_boards_title"] = text(page, "#boards-title")
            facts["stocks_etf_chips"] = page.locator("#stance .c-et").count()
            r.log("after STOCKS: boards %s | stance rows %s (etf chips %s) | %s" % (facts["stocks_boards_title"], page.locator("#stance tbody tr").count(), facts["stocks_etf_chips"], text(page, "#stance-counts")))
            page.click("#scope-conc")
            page.wait_for_timeout(500)
            facts["conc_rot_title"] = text(page, "#rot-title")
            r.log("after CONCENTRATED: %s | %s | %s | boards %s" % (facts["conc_rot_title"], text(page, "#smart-meta")[:160], text(page, "#rot-meta")[:160], text(page, "#boards-title")))
            page.click("#univ-all")
            page.click("#scope-cohort")
            page.wait_for_timeout(400)
            page.click("#ind-sort-value")
            page.wait_for_timeout(300)
            facts["ind_sort_value"] = text(page, "#ind-meta")
            page.click("#ind-sort-tilt")
            page.wait_for_timeout(300)
            facts["ind_sort_tilt"] = text(page, "#ind-meta")
            btns = page.locator("#ind-filter button")
            if btns.count() > 1:
                btns.nth(1).click()
                page.wait_for_timeout(300)
            facts["ind_filtered"] = text(page, "#ind-meta")
            facts["ind_filtered_rows"] = page.locator("#industries tbody tr").count()
            r.log("industry sorts/filter: %s | %s | %s (%s rows)" % (facts["ind_sort_value"], facts["ind_sort_tilt"], facts["ind_filtered"], facts["ind_filtered_rows"]))
            btns.nth(0).click()
            page.wait_for_timeout(200)
            page.click("#mgr-table button[data-fund='ELLIOTT']")
            page.wait_for_timeout(2500)
            facts["drill_after_click"] = text(page, "#drill-title")
            facts["selection_after_click"] = page.locator("#selection").input_value()
            facts["drill_tickers_elliott"] = page.locator("#drill-body .tk").count()
            r.log("after selecting Elliott: drill %s | selection=%s | tickers in drill %s | mix %s" % (facts["drill_after_click"], facts["selection_after_click"], facts["drill_tickers_elliott"], text(page, "#drill-body .sub:has(.stack)")[:300]))
            r.log("errors: %s" % (errors[:10] or "none"))
            r.log("responses >= 400: %s" % (bad_responses[:12] or "none"))
            own = [u for u in bad_responses if "/data/13f-book.json" in u or "holdings-research" in u or "/13f.html" in u]
            facts["js_errors"] = len([e for e in errors if not e.startswith("console: Failed to load resource")]) + len(own)
            facts["bad_responses"] = len(bad_responses)

            r.section("3. phone 390")
            page.set_viewport_size({"width": 390, "height": 844})
            page.wait_for_timeout(600)
            overflow = page.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth")
            r.log("horizontal overflow at 390 px: %s px" % overflow)
            wide = page.evaluate("""() => { const cw = document.documentElement.clientWidth; const out = [];
              const clipped = (el) => { for (let a = el.parentElement; a && a !== document.body; a = a.parentElement) { const o = getComputedStyle(a).overflowX;
                if ((o === 'auto' || o === 'scroll' || o === 'hidden' || o === 'clip') && a.getBoundingClientRect().right <= cw + 1) return true; } return false; };
              for (const el of document.querySelectorAll('main *')) { const rct = el.getBoundingClientRect(); const cs = getComputedStyle(el);
                if (rct.right > cw + 1 && rct.width > 0 && cs.position !== 'fixed' && !clipped(el)) {
                  out.push({tag: el.tagName, id: el.id || '', cls: String(el.className || '').slice(0, 60), right: Math.round(rct.right), w: Math.round(rct.width)}); } }
              return out.slice(0, 20); }""")
            r.log("unclipped elements past the right edge at 390 px (inside main): %s" % json.dumps(wide))
            facts["phone_overflow_px"] = overflow
            facts["phone_overflow_in_main"] = len(wide)

            r.section("4. screenshot (0.5x, jpeg, base64)")
            ctx2 = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=0.5)
            p2 = ctx2.new_page()
            p2.goto("%s/13f.html?nocache=%d" % (BASE, stamp + 1), wait_until="domcontentloaded")
            try:
                p2.wait_for_selector("#engines tbody tr", timeout=30000)
                p2.wait_for_selector("#positions tr", timeout=30000)
            except Exception:  # noqa: BLE001
                pass
            p2.wait_for_timeout(1500)
            shot = p2.screenshot(full_page=True, type="jpeg", quality=55)
            r.log("screenshot bytes: %d" % len(shot))
            r.lines.append("")
            r.lines.append("<!-- SHOT_JPEG_BASE64 -->")
            r.lines.append(base64.b64encode(shot).decode("ascii"))
            r.lines.append("<!-- /SHOT_JPEG_BASE64 -->")
            r.lines.append("")
            browser.close()

        ok = (facts.get("mgr_rows", 0) >= 18 and facts.get("stance_tables", 0) == 3 and facts.get("stance_rows", 0) >= 1 and facts.get("rot_rows", 0) >= 3
              and facts.get("sector_rows", 0) >= 8 and facts.get("industry_rows", 0) >= 20 and facts.get("flow_tiles", 0) == 2 and facts.get("flow_lines", 0) >= 6 and facts.get("ind_filter_buttons", 0) >= 6
              and "sorted by reported value" in facts.get("ind_sort_value", "") and "|tilt|" in facts.get("ind_sort_tilt", "") and " in " in facts.get("ind_filtered", "") and 1 <= facts.get("ind_filtered_rows", 0) < facts.get("industry_rows", 0)
              and facts.get("rot_bars", 0) == 2 and facts.get("cont_tables", 0) == 2 and facts.get("boards_tables", 0) == 6 and facts.get("boards_rows", 0) >= 20
              and facts.get("master_kpis", 0) == 4 and facts.get("master_tables", 0) >= 1 and facts.get("drill_tickers", 0) >= 3 and facts.get("drill_mix_bars", 0) == 2
              and facts.get("bad_literals", 1) == 0 and facts.get("js_errors", 1) == 0 and facts.get("phone_overflow_in_main", 1) == 0
              and "ETFs" in facts.get("etfs_boards_title", "") and facts.get("etfs_stock_chips", 1) == 0 and facts.get("stocks_etf_chips", 1) == 0
              and "concentrated" in facts.get("conc_rot_title", "") and "Elliott" in facts.get("drill_after_click", "") and facts.get("selection_after_click") == "ELLIOTT"
              and facts.get("positions_rows", 0) >= 1 and "CUSIP master" in facts.get("h-pills", ""))
        r.kv(**{k: str(v) for k, v in facts.items() if k in ("mgr_rows", "stance_tables", "stance_rows", "rot_rows", "sector_rows", "industry_rows", "flow_lines", "ind_filtered", "ind_filtered_rows", "cont_rows", "boards_tables", "boards_rows", "master_kpis", "unclassified_rows",
                                                              "drill_tickers", "drill_mix_bars", "bad_literals", "js_errors", "bad_responses", "phone_overflow_px", "phone_overflow_in_main",
                                                              "etfs_boards_title", "etfs_stance_rows", "stocks_etf_chips", "conc_rot_title", "drill_after_click", "selection_after_click")})
        if ok:
            r.ok("PROVEN: the sector layer (money in/out tiles, sector + industry tables, filter and sorts) renders from the live book beside stance, rotation and contenders; page clean at 1440 and 390")
        else:
            r.fail("one or more checks failed - see the facts above")
            raise SystemExit(1)


if __name__ == "__main__":
    main()
    sys.exit(0)
