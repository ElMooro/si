"""ops 6959 - Credit Before Equity: where the page prints "Infinity" (read-only).

The 22:35 UTC scheduled run of 2026-11-25 published 3.0.0 on its own, and the live proof (ops 6944) failed one check:
with the high-yield book chosen, the board prints "Infinity". This op opens the live page in a real browser, chooses each
book and each colour measure, and reports every element whose own text carries "Infinity", "NaN", "undefined" or "null",
with its place on the page and the text around it - so the cause is read from the page, not guessed.

Nothing is invoked or written.
"""
from pathlib import Path
import importlib.util
import json
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops")]
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
FIND = r"""() => { const out = [], re = /\bNaN\b|\bundefined\b|\bnull\b|Infinity/;
  const walk = document.createTreeWalker(document.getElementById('board'), NodeFilter.SHOW_TEXT);
  let n; while ((n = walk.nextNode())) { if (!re.test(n.nodeValue)) continue; const e = n.parentElement; if (!e || !e.getClientRects().length) continue;
    const path = []; let p = e; for (let i = 0; i < 6 && p && p.id !== 'board'; i++) { path.push(p.tagName.toLowerCase() + (p.id ? '#' + p.id : '') + (typeof p.className === 'string' && p.className ? '.' + p.className.split(' ').join('.') : '')); p = p.parentElement; }
    const sec = e.closest('section, [id^="s-"]'); out.push({ text: n.nodeValue.trim().slice(0, 160), around: (e.parentElement ? e.parentElement.innerText : '').replace(/\s+/g, ' ').slice(0, 260), path: path.join(' < '), section: sec ? (sec.id || '') : '' }); }
  return out.slice(0, 12); }"""


def main():
    from ops_report import report
    spec = importlib.util.spec_from_file_location("ops_6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    stamp = int(time.time())
    with report("ops_6959_credit_board_infinity_probe") as r:
        r.heading("6959 - Credit Before Equity: where the page prints Infinity (read-only)")
        req = urllib.request.Request(BASE + "/data/credit-before-equity.json?probe=%d" % stamp, headers={"User-Agent": UA})
        raw = urllib.request.urlopen(req, timeout=120).read().decode("utf-8")
        doc = json.loads(raw)
        r.log("document %s generated %s | the raw text holds Infinity: %s, NaN: %s" % (doc.get("version"), doc.get("generated_at"), "Infinity" in raw, "NaN" in raw))
        for book in ("all", "ig", "hy", "em"):
            inds = doc["industries"].get(book) or []
            r.log("industries %s: %d, with value %d, with a change %d | first %s" % (book, len(inds), sum(1 for i in inds if (i.get("value") or 0) > 0),
                                                                                  sum(1 for i in inds if isinstance(i.get("d_bp"), (int, float))), json.dumps(inds[0])[:420] if inds else None))
        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)
            page = browser.new_context(viewport={"width": 1500, "height": 950}, user_agent=UA).new_page()
            page.goto(BASE + "/credit-before-equity.html?probe=%d" % stamp, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_selector(".cb-box", timeout=90000)
            page.wait_for_timeout(2500)
            for book in ("all", "hy", "ig", "em"):
                btn = page.locator("#controls button[data-book=%s]" % book)
                if not btn.count() or btn.is_disabled():
                    r.log("book %s: no button or off" % book)
                    continue
                btn.click()
                page.wait_for_timeout(350)
                for measure in ("bp", "pct", "rel", "level", "stress"):
                    mb = page.locator("#controls button[data-measure=%s]" % measure)
                    if not mb.count():
                        continue
                    mb.click()
                    page.wait_for_timeout(250)
                    found = page.evaluate(FIND)
                    r.log("book %s, colour by %s: %d bad texts %s" % (book, measure, len(found), json.dumps(found)[:1500] if found else ""))
                page.locator("#controls button[data-measure=bp]").click()
            browser.close()
        r.ok("PROBE COMPLETE: nothing was invoked or written")


if __name__ == "__main__":
    main()
