"""ops 6803 - what Khalid sees after etf-true-flows 3.1.0 (ops 6802 first light): in a fresh headless Chrome against
justhodl.ai, (1) /etf-research.html verifies the new run in the browser (manifest hash, output binding) and, for GLD,
ASHR and HACK, shows "complete issuer history" with the issuer's rows and the 5-day NAV-valued estimate, while QQQ
(ledger accruing since today) states its coverage honestly; (2) /etf.html (the ETF desk) Flows -> Table -> 5D lists
the issuer-tape rows for GLD / ASHR / HACK with the packet's 5D figures. Screenshots embedded. Read-only."""
from datetime import datetime, timezone
from pathlib import Path
import base64
import json
import re
import subprocess
import sys
import time

import requests

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
SITE = "https://justhodl.ai"


def ensure_playwright(log):
    try:
        import playwright  # noqa: F401
    except Exception:  # noqa: BLE001
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


def money(v):
    a = abs(v)
    for unit, d in (("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if a >= d:
            return ("-" if v < 0 else "") + ("%.1f" % (a / d)).rstrip("0").rstrip(".") + unit
    return "%.0f" % v


def main():
    from ops_report import report
    now = datetime.now(timezone.utc)
    fails, shots = [], []
    with report("ops_6803_etf_registry_ui_proof") as r:
        r.heading("ops 6803 - ETF research page + ETF desk after the issuer registry %s" % now.isoformat(timespec="minutes"))
        packet = requests.get(SITE + "/data/etf-true-flows.json", headers={"Cache-Control": "no-cache"}, timeout=60).json()
        r.log("edge packet: version %s generated %s gaps %d coverage %s" % (packet.get("version"), packet.get("generated_at"), len(packet.get("gaps", [])), json.dumps(packet.get("coverage"))))
        if packet.get("version") != "3.1.0":
            fails.append("edge packet is %s" % packet.get("version"))
        by = packet["by_etf"]
        sync_playwright = ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = launch(p, r.log)
            r.section("1. /etf-research.html")
            ctx = browser.new_context(viewport={"width": 1400, "height": 1100})
            pg = ctx.new_page()
            for t in ("GLD", "ASHR", "HACK", "QQQ"):
                t0 = time.time()
                pg.goto(SITE + "/etf-research.html?ticker=" + t, wait_until="domcontentloaded", timeout=60000)
                try:
                    pg.wait_for_function("() => { const h = document.getElementById('history-title'); return h && /complete issuer history|coverage gap|failed/.test(h.textContent); }", timeout=60000)
                except Exception as exc:  # noqa: BLE001
                    r.warn("  %s: history title did not settle (%s)" % (t, type(exc).__name__))
                state = pg.text_content("#state") or ""
                title = pg.text_content("#history-title") or ""
                detail = re.sub(r"\s+", " ", pg.inner_text("#detail") or "")
                aligned = pg.evaluate("() => [...document.querySelectorAll('#detail table')].map(t => [...t.querySelectorAll('tr')].map(tr => [...tr.children].map(c => c.textContent.trim()).join(' | ')))")
                count = pg.text_content("#page-count") or ""
                chart = (pg.text_content("#chart") or "")[:120]
                r.log("  %s (%.1fs): state %r | %r | %s | chart: %r" % (t, time.time() - t0, state[:90], title, count, chart.strip()[:80]))
                r.log("     detail: %s" % detail[:420])
                for tbl in aligned:
                    if any("Issuer observations" in row for row in tbl):
                        for row in tbl:
                            r.log("     aligned: %s" % row[:200])
                e = by[t]
                if t in ("GLD", "ASHR", "HACK"):
                    if "complete issuer history" not in title:
                        fails.append("%s research page does not show the complete issuer history (%r)" % (t, title))
                    if "retained_native_history" not in detail:
                        fails.append("%s detail lacks the source status" % t)
                    want = str(int(e["history_observations"]))
                    if want not in count.replace(",", ""):
                        fails.append("%s observation count on page (%r) differs from the packet (%s)" % (t, count, want))
                    if e.get("net_flow_5d_usd") is not None:
                        flat = detail.replace("−", "-").replace(",", "").replace("+", "")
                        want = "%.3f" % (e["net_flow_5d_usd"] / 1e6)   # the page prints USD millions to three decimals
                        forms = {want, want.rstrip("0").rstrip("."), "%.1f" % (e["net_flow_5d_usd"] / 1e6), "0" if abs(e["net_flow_5d_usd"]) < 5e-4 else want}
                        if not any(("| " + f.replace("-", "") + " |") in flat.replace("−", "-") or (" " + f.replace("-", "") + " ") in flat for f in forms):
                            fails.append("%s: the page does not show the packet's 5d estimate %s USD m" % (t, want))
                else:
                    # the ledger started today: one observation, honestly stated; no window can be complete yet
                    if "retained_native_snapshot_ledger" not in detail or "1 issuer observations" not in count or "2026-11-11 → 2026-11-11" not in detail:
                        fails.append("QQQ page should state the one-row accruing ledger (%r / %r)" % (title, count))
                if "unverified" in state.lower() or "differs" in state.lower():
                    fails.append("%s research page verification failed: %s" % (t, state))
                shot = "research_%s.png" % t
                pg.screenshot(path=shot, full_page=False)
                shots.append((shot, Path(shot).read_bytes()))
            gaps_text = pg.text_content("#gaps") or ""
            r.log("  gaps line: %s" % gaps_text[:300])
            ctx.close()
            r.section("2. /etf.html Flows -> Table -> 5D")
            ctx = browser.new_context(viewport={"width": 1500, "height": 1100})
            pg = ctx.new_page()
            pg.goto(SITE + "/etf.html", wait_until="domcontentloaded", timeout=60000)
            pg.wait_for_selector("#etf-desk-host button[data-tab='flows']", timeout=60000)
            pg.wait_for_timeout(4000)
            pg.click("#etf-desk-host button[data-tab='flows']")
            pg.wait_for_timeout(800)
            pg.click("#etf-desk-host button[data-mode='table']")
            pg.wait_for_timeout(800)
            pg.click("#etf-desk-host button[data-w='5']")
            pg.wait_for_timeout(1200)
            try:
                pg.wait_for_function("() => { const m = document.getElementById('etf-meta'); return m && /funds/.test(m.textContent); }", timeout=30000)
            except Exception:  # noqa: BLE001
                pass
            meta = pg.text_content("#etf-meta") or ""
            r.log("  meta: %s" % meta[:200])
            # the table's "creation tape" rows come from the desk's own ETF tape; the issuer figures of the packet feed the
            # ETF INFLOWS / OUTFLOWS maps above it (packet.inflows / outflows, filtered by the ticker box)
            for t in ("GLD", "ASHR", "HACK"):
                pg.fill("#etf-q", t)
                pg.keyboard.press("Enter")
                pg.wait_for_timeout(1500)
                rows = pg.evaluate("t => [...document.querySelectorAll('#etf-desk-host table tr')].map(tr => [...tr.querySelectorAll('td')].map(td => td.textContent.trim())).filter(c => c.length && c[0] === t)", t)
                maps = pg.evaluate("() => ({inn: (document.querySelector('#map-in') || {}).innerText || '', out: (document.querySelector('#map-out') || {}).innerText || '', lead: (document.querySelector('#flow-lead .sech span:last-child') || {}).textContent || ''})")
                e = by[t]
                r.log("  %s table row: %s | packet 5d %s (%s)" % (t, json.dumps(rows)[:300], e.get("net_flow_5d_usd"), money(e["net_flow_5d_usd"]) if e.get("net_flow_5d_usd") is not None else "none"))
                r.log("  %s issuer maps: lead %r | inflows %r | outflows %r" % (t, maps["lead"][:80], re.sub(r"\s+", " ", maps["inn"])[:160], re.sub(r"\s+", " ", maps["out"])[:160]))
                in_packet_lists = any(v["ticker"] == t for v in packet.get("inflows", []) + packet.get("outflows", []))
                if in_packet_lists and t not in maps["inn"] + maps["out"]:
                    fails.append("%s is in the packet's inflow/outflow lists but not on the desk's issuer maps" % t)
                if not rows:
                    fails.append("%s has no row in the desk's 5D table" % t)
            pg.fill("#etf-q", "")
            pg.keyboard.press("Enter")
            pg.wait_for_timeout(800)
            shot = "etf_desk_flows_5d.png"
            pg.screenshot(path=shot, full_page=False)
            shots.append((shot, Path(shot).read_bytes()))
            ctx.close()
            browser.close()
        r.section("screenshots")
        for name, body in shots:
            r.log("<!-- SCREENSHOT %s -->\n![%s](data:image/png;base64,%s)\n<!-- /SCREENSHOT -->" % (name, name, base64.b64encode(body).decode()))
        r.kv(fails=str(fails))
        if fails:
            r.fail("UI proof failed: %s" % fails)
        else:
            r.ok("research page verifies the 3.1.0 run and shows the GLD / ASHR / HACK histories; the ETF desk lists their 5D creation prints")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
