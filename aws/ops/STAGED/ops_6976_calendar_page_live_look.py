"""ops 6976 - read-only: the catalyst calendar page's own view in a real browser after calendar 2.0.

CLAUDE.md rule 5 for the page's own controls (ops 6974 proved the chart tabs and the Engine data section): the type
chips are read with their counts, Economic is clicked, low impact is switched on, a ticker's link is read.
Nothing is written. Safe to run again.
"""
from pathlib import Path
import base64
import importlib.util
import json
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "aws" / "ops"))
NAME = "ops_6976_calendar_page_live_look"
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
READ = r"""() => ({ chips: Array.from(document.querySelectorAll('.chip[data-type]')).map(c => [c.dataset.type, c.textContent.trim(), c.title, c.classList.contains('active')]),
  info: document.getElementById('filterInfo').textContent, days: document.querySelectorAll('.day-group').length, events: document.querySelectorAll('.event').length,
  first: Array.from(document.querySelectorAll('.event')).slice(0, 4).map(e => e.textContent.replace(/\s+/g, ' ').trim()),
  types: Array.from(new Set(Array.from(document.querySelectorAll('.event-type')).map(e => e.textContent.trim()))),
  links: document.querySelectorAll('.event-title a[data-sym]').length, link: (document.querySelector('.event-title a[data-sym]') || {}).href || null,
  labels: Array.from(document.querySelectorAll('.countdown')).slice(0, 3).map(e => e.textContent.trim()), meta: document.getElementById('metaLine').textContent,
  stats: ['nTotal', 'nH7', 'nH30', 'fomcNext', 'nEarnings'].map(i => (document.getElementById(i) || {}).textContent) })"""


def main():
    from ops_report import report
    spec = importlib.util.spec_from_file_location("ops_6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    fails, shots = [], {}
    with report(NAME) as r:
        r.heading("6976 - the calendar page's own view, live (read-only)")

        def ck(cond, msg):
            r.log(("PASS  " if cond else "FAIL  ") + msg)
            if not cond:
                fails.append(msg)

        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)
            page = browser.new_context(viewport={"width": 1400, "height": 1000}, user_agent=UA).new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto("%s/catalyst-calendar.html?nocache=%d" % (BASE, int(time.time())), wait_until="domcontentloaded")
            page.wait_for_selector(".day-group", timeout=90000)
            page.wait_for_selector("#moreTypes .chip", timeout=30000)
            a = page.evaluate(READ)
            r.log("as loaded: %s | %s | %d days, %d events | stats %s" % (a["meta"], a["info"], a["days"], a["events"], a["stats"]))
            r.log("chips: " + json.dumps([[c[1], c[2]] for c in a["chips"]], ensure_ascii=False))
            r.log("first rows: " + json.dumps(a["first"], ensure_ascii=False)[:700])
            got = {c[0] for c in a["chips"]}
            ck({"ECON", "MARKET_HOLIDAY", "OPEX", "QUARTER_END", "VIX_EXPIRY", "EARNINGS_ACTUAL", "GOV_CONTRACT"} <= got, "every type the engine writes has a chip (%d chips)" % len(a["chips"]))
            ck(a["events"] > 60 and "Economic" in a["types"] and "Holiday" in a["types"], "economic releases and market holidays are in the page's own list (%d events, types %s)" % (a["events"], a["types"]))
            ck(a["links"] > 0 and "/chart.html?symbol=" in (a["link"] or ""), "a company's row links to the chart (%d links, e.g. %s)" % (a["links"], a["link"]))
            shots["1-the-calendar"] = page.screenshot(type="jpeg", quality=60)
            page.click(".chip[data-type=ECON]")
            page.wait_for_timeout(300)
            b = page.evaluate(READ)
            r.log("Economic only: %s | first: %s" % (b["info"], json.dumps(b["first"][:3], ensure_ascii=False)[:500]))
            ck(b["types"] == ["Economic"] and 0 < b["events"] < a["events"], "clicking Economic leaves only economic rows (%d)" % b["events"])
            page.click(".chip[data-impact=LOW]")
            page.wait_for_timeout(300)
            c = page.evaluate(READ)
            ck(c["events"] >= b["events"] and c["types"] == ["Economic"], "switching low impact on adds the low-impact releases (%d -> %d)" % (b["events"], c["events"]))
            page.click(".chip[data-type=ALL]")
            page.wait_for_timeout(300)
            d = page.evaluate(READ)
            r.log("everything: %s | first day labels %s" % (d["info"], d["labels"]))
            ck(any("ago" in x for x in d["labels"]) and not any("+-" in x for x in d["labels"]), "a past day reads as days ago")
            ck(not errors, "the page threw no error (%s)" % (errors[:2] or "none"))
            browser.close()
        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(failed_checks=len(fails))
        if fails:
            r.fail("; ".join(fails)[:600])
            sys.exit(1)
        r.ok("the calendar page's own controls work on the live site")


if __name__ == "__main__":
    main()
