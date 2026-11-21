"""ops 6869 - READ-ONLY diagnostic: what makes three desks wider than a 390 px phone, and why a tap on the S&P 500 sheet waits.

ops 6868 (every tearsheet list, live) is green at 1440 px on every desk and at 390 px on all but three, where the PAGE
is wider than the screen: sp500.html by 438 px, ark.html by 61 px, insider-clusters.html by 4 px. Offline, on the
trimmed feeds, none of the three spills, so it is the live content. This reads, on the live pages at 390 px: the
elements that reach past the right edge (inside the tearsheet or not), and for sp500.html what stands over a card's
heading when Chrome tries to click it. Writes nothing.
"""
from datetime import datetime, timezone
from pathlib import Path
import importlib.util
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
_spec = importlib.util.spec_from_file_location("ops6866", str(Path(__file__).resolve().parent / "ops_6866_strength_page_live_proof.py"))
P = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(P)

PAGES = ["/sp500.html", "/ark.html", "/insider-clusters.html", "/firm-book.html", "/stock-volatility.html", "/leaders.html", "/capitulation-desk.html", "/wyckoff-desk.html?tab=bottom", "/paper-book.html",
         "/stablecoin-flow.html", "/foreign-flows.html", "/capital-inflows.html", "/strong.html"]

SPILL = r"""() => { const vw = document.documentElement.clientWidth, out = [], sheet = document.getElementById('jh-tearsheet');
  const label = e => e.tagName.toLowerCase() + (e.id ? '#' + e.id : '') + (typeof e.className === 'string' && e.className ? '.' + e.className.trim().split(/\s+/).slice(0, 3).join('.') : '');
  for (const e of document.querySelectorAll('body *')) { const r = e.getBoundingClientRect(), cs = getComputedStyle(e); if (!r.width || !r.height || cs.position === 'fixed' || cs.display === 'none') continue;
    if (r.right > vw + 1) { let clipped = false; for (let p = e.parentElement; p && p !== document.body; p = p.parentElement) { const o = getComputedStyle(p).overflowX; if (o === 'auto' || o === 'scroll' || o === 'hidden' || o === 'clip') { clipped = true; break; } }
      if (!clipped) out.push({ el: label(e), parent: e.parentElement ? label(e.parentElement) : '', over: Math.round(r.right - vw), w: Math.round(r.width), sheet: !!(sheet && sheet.contains(e)), text: (e.childElementCount ? '' : e.textContent.trim().slice(0, 50)) }); } }
  out.sort((a, b) => b.over - a.over);
  const top = []; for (const o of out) { if (!top.some(t => t.parent === o.parent && t.over === o.over)) top.push(o); if (top.length >= 10) break; }
  const vv = window.visualViewport;
  return { ox: document.documentElement.scrollWidth - vw, vw: vw, inner: window.innerWidth, scale: vv ? vv.scale : null, meta: (document.querySelector('meta[name=viewport]') || { content: '' }).content, n: out.length, in_sheet: out.filter(o => o.sheet).length,
    sheet_w: sheet ? [Math.round(sheet.getBoundingClientRect().width), sheet.scrollWidth, sheet.clientWidth] : null, top: top }; }"""

HIT = r"""(sel) => { const e = document.querySelector(sel); if (!e) return { found: false }; e.scrollIntoView({ block: 'center' }); const r = e.getBoundingClientRect(), x = r.left + r.width / 2, y = r.top + r.height / 2, at = document.elementFromPoint(x, y);
  const label = n => n ? n.tagName.toLowerCase() + (n.id ? '#' + n.id : '') + (typeof n.className === 'string' && n.className ? '.' + n.className.trim().split(/\s+/).slice(0, 3).join('.') : '') : null;
  const cs = getComputedStyle(e); return { found: true, box: [Math.round(r.left), Math.round(r.top), Math.round(r.width), Math.round(r.height)], centre: [Math.round(x), Math.round(y)], inner: [window.innerWidth, window.innerHeight],
    at: label(at), at_in: !!(at && e.contains(at)), at_pos: at ? getComputedStyle(at).position : null, at_z: at ? getComputedStyle(at).zIndex : null, vis: cs.visibility, disp: cs.display, pe: cs.pointerEvents, op: cs.opacity,
    chain: (function () { const c = []; for (let n = at; n && c.length < 6; n = n.parentElement) c.push(label(n)); return c; })() }; }"""


def main():
    from ops_report import report
    with report("ops_6869_phone_spill_diagnostic") as r:
        r.heading("ops 6869 - what spills on a 390 px phone, live %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        sync_playwright = P.ensure_playwright(r.log)
        stamp, facts = int(time.time()), {}
        with sync_playwright() as p:
            browser = P.launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 390, "height": 844}, device_scale_factor=1, is_mobile=True, has_touch=True, user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) jh-ops-probe Mobile Safari")
            for path in PAGES:
                page = ctx.new_page()
                try:
                    page.goto("%s%s%sv=%d" % (P.BASE, path, "&" if "?" in path else "?", stamp), wait_until="domcontentloaded", timeout=60000)
                    page.wait_for_function("() => { const e = document.getElementById('jh-tearsheet'); return e && e.dataset.state === 'ready'; }", timeout=45000)
                    page.wait_for_timeout(2500)
                    s = page.evaluate(SPILL)
                    facts[path] = s["ox"]
                    r.section("%s - the page is %d px wider than the screen (%d elements past the edge, %d of them in the tearsheet)" % (path, s["ox"], s["n"], s["in_sheet"]))
                    r.log("viewport %s, innerWidth %s, scale %s, meta %r, sheet %s" % (s["vw"], s["inner"], s["scale"], s["meta"], s["sheet_w"]))
                    for o in s["top"]:
                        r.log("  +%d px  %s  (w %d, in %s)%s%s" % (o["over"], o["el"], o["w"], o["parent"], "  [TEARSHEET]" if o["sheet"] else "", "  %r" % o["text"] if o["text"] else ""))
                    if path == "/sp500.html":
                        sel = "#jh-tearsheet [data-jts-list='lift'] .jts-ch[data-jts-open]"
                        r.log("card heading: %s" % json.dumps(page.evaluate(HIT, sel)))
                        try:
                            page.click(sel, timeout=6000)
                            r.log("click: went through; open = %r" % page.evaluate("() => document.getElementById('jh-tearsheet').dataset.open"))
                        except Exception as exc:  # noqa: BLE001
                            for line in str(exc).split("\n")[:40]:
                                r.log("click: %s" % line[:300])
                except Exception as exc:  # noqa: BLE001
                    r.log("%s: %s" % (path, str(exc)[:300]))
                page.close()
            browser.close()
        r.kv(**{k.strip("/").split(".")[0].split("?")[0]: v for k, v in facts.items()})
        r.ok("read-only diagnostic: page overflow at 390 px by desk, the elements behind it, and the click on the S&P 500 sheet")


if __name__ == "__main__":
    main()
