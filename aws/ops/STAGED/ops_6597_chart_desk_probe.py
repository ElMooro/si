"""ops 6597 - READ-ONLY browser probe of the served https://justhodl.ai/chart.html: does every desk ("engine") on the
chart actually render, and with what?  (Khalid, 2026-10-21: "bring back chart.html with all the engines inside it";
CLAUDE.md rule 5: click the control yourself and quote the proof - a green deploy is not verification.)

Headless Chrome on the runner (the sandbox cannot reach justhodl.ai).  Loads the chart for AAPL, records page errors,
console errors and failed requests (paths only - never query strings), the toolbar's desk buttons, the chart canvas,
quote HUD, watchlist and dock; then clicks every desk button in turn - the iframe desks (Macro, Heat, Auction, ETF,
Strong, Bonds, CDS, 13F, SC, Capit, BC, Spread, Spring, Pump, Alert), the inline ones (Corr, Khalid sniper, GO) and
the chart-mark toggles (Accum, Bottom, Dist) - and reads what each one shows: text length, a short excerpt, and the
count of "no data / unavailable / failed / loading" markers.  Every dock tab the same way.  Screenshots go to the
private ops bucket (probes/chart/<stamp>/), never to the public bucket.  Nothing on the site is changed.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import re
import subprocess
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
PRIVATE = "justhodl-ops-private-%s" % ACCOUNT
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120)
BASE = "https://justhodl.ai"
SYMBOL = "AAPL"
MARK_RE = re.compile(r"no data|unavailable|not available|could not|did not start|failed|loading|stale|error|offline|n/a", re.I)
TOGGLES = {"btn-acc", "btn-bot", "btn-dist"}
INLINE = {"btn-corm": "#ws-corr", "btn-snip": "#ws-sniper"}
ROUTES = []  # [(url glob, local file)] - empty on the runner; the local rehearsal serves the chart library from disk

SNAP_JS = """() => {
  const q = (s) => document.querySelector(s);
  const txt = (s, n) => { const e = q(s); return e ? (e.innerText || '').replace(/\\s+/g, ' ').trim().slice(0, n || 200) : null; };
  const desks = Array.from(document.querySelectorAll('#tfbar .wsdesk')).map(b => ({id: b.id, label: (b.innerText || '').replace(/\\s+/g, ' ').trim(), title: b.title}));
  return {
    title: document.title,
    desks: desks,
    tfbar_buttons: document.querySelectorAll('#tfbar button').length,
    canvases: document.querySelectorAll('#chart canvas').length,
    quote: txt('#quote', 240),
    stat: txt('#stat', 120), livepill: txt('#livepill', 60), clock: txt('#clock', 40),
    wlist_rows: (q('#wlist') ? q('#wlist').querySelectorAll('tr, .row, li').length : -1),
    wlist_head: txt('#wlist', 300),
    detail: txt('#detail', 300),
    dock_tabs: Array.from(document.querySelectorAll('#dtabs button')).map(b => (b.innerText || '').trim()),
    dockbody_len: (q('#dockbody') ? (q('#dockbody').innerText || '').length : -1),
    flags: {openWorkspace: !!window.jhOpenWorkspace, campToggle: !!window.jhCampToggle, distToggle: !!window.jhDistToggle, sniper: !!window.jhSniperMount, bbgo: !!window.jhBbGo, lwc: !!window.LightweightCharts, tvrail: !!window.__jhTvRail},
    toast: txt('#toast', 200)
  };
}"""

OVERLAY_JS = """() => {
  const ov = document.getElementById('ws-overlay');
  if (!ov) return {present: false};
  const on = ov.className.indexOf('on') >= 0;
  const hd = ov.querySelector('.wshd span');
  const a = ov.querySelector('.wshd a');
  const fr = ov.querySelector('iframe');
  let inner = null;
  if (fr) {
    try {
      const d = fr.contentDocument;
      const body = d && d.body;
      const t = body ? (body.innerText || '') : '';
      inner = {ready: d ? d.readyState : null, title: d ? d.title : null, len: t.length, excerpt: t.replace(/\\s+/g, ' ').trim().slice(0, 400),
               elements: body ? body.querySelectorAll('*').length : 0, canvases: d ? d.querySelectorAll('canvas, svg').length : 0, tables: d ? d.querySelectorAll('table').length : 0,
               rows: d ? d.querySelectorAll('tr').length : 0};
    } catch (e) { inner = {error: String(e).slice(0, 120)}; }
  }
  const corr = ov.querySelector('#ws-corr'); const sn = ov.querySelector('#ws-sniper');
  const inlineEl = corr || sn;
  const inline = inlineEl ? {len: (inlineEl.innerText || '').length, excerpt: (inlineEl.innerText || '').replace(/\\s+/g, ' ').trim().slice(0, 400), elements: inlineEl.querySelectorAll('*').length, canvases: inlineEl.querySelectorAll('canvas, svg').length} : null;
  return {present: true, on: on, title: hd ? hd.innerText : null, href: a ? a.getAttribute('href') : null, iframe_src: fr ? fr.getAttribute('src') : null, inner: inner, inline: inline};
}"""


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


def path_only(url):
    return re.sub(r"\?.*$", "", url or "")[:140]


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    shots = Path("/tmp/chart-probe-%s" % stamp)
    shots.mkdir(parents=True, exist_ok=True)
    with report("ops_6597_chart_desk_probe") as r:
        r.heading("ops 6597 - chart.html desk probe (headless Chrome on the runner, read-only)")
        sync_playwright = ensure_playwright(r.log)
        errors, console, failed = [], [], []
        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1600, "height": 1000}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6597 chart-probe")
            # the auto-hide layout (jh-chart-autohide.js) parks the toolbar off-screen until the mouse reaches the top edge;
            # pin the classic layout for the probe (localStorage jh-autohide=0, the same switch as the page's own pin button)
            ctx.add_init_script("try { localStorage.setItem('jh-autohide', '0'); } catch (e) {}")
            page = ctx.new_page()
            page.set_default_timeout(30000)
            for glob_, local in ROUTES:
                page.route(glob_, (lambda local: (lambda route, *_: route.fulfill(path=local, content_type="application/javascript")))(local))
            page.on("pageerror", lambda e: errors.append(str(e).splitlines()[0][:220]))
            page.on("console", lambda m: console.append((m.type, m.text[:220])) if m.type in ("error", "warning") else None)
            page.on("response", lambda resp: failed.append((resp.status, path_only(resp.url))) if resp.status >= 400 else None)
            page.on("requestfailed", lambda req: failed.append(("FAIL", path_only(req.url) + " " + str(req.failure or "")[:60])))
            t0 = time.time()
            url = "%s/chart.html?s=%s&nocache=%d" % (BASE, SYMBOL, int(t0))
            page.goto(url, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=20000)
            except Exception:  # noqa: BLE001
                r.log("networkidle not reached in 20 s (the chart keeps polling) - continuing")
            page.wait_for_timeout(7000)
            snap = page.evaluate(SNAP_JS)
            r.section("1. the page")
            r.log("loaded %s in %.1fs: title %r; desk buttons %d (%s); tfbar buttons %d; chart canvases %d; flags %s" % (
                path_only(url), time.time() - t0, snap.get("title"), len(snap.get("desks") or []), ",".join(d["label"] for d in snap.get("desks") or []), snap.get("tfbar_buttons"), snap.get("canvases"), json.dumps(snap.get("flags"))))
            r.log("quote HUD: %r | stat %r | live %r | clock %r" % (snap.get("quote"), snap.get("stat"), snap.get("livepill"), snap.get("clock")))
            r.log("watchlist rows %s: %r" % (snap.get("wlist_rows"), (snap.get("wlist_head") or "")[:200]))
            r.log("detail: %r" % (snap.get("detail") or "")[:240])
            r.log("dock tabs %s; dock body chars %s" % (snap.get("dock_tabs"), snap.get("dockbody_len")))
            page.screenshot(path=str(shots / "00-chart.png"), full_page=False)
            r.section("2. desks")
            desks = snap.get("desks") or []
            empty = []
            for d in desks:
                bid, label = d["id"], d["label"]
                t1 = time.time()
                try:
                    page.mouse.move(700, 2)
                    page.click("#" + bid, timeout=8000)
                except Exception as e:  # noqa: BLE001
                    r.warn("%s (%s): click failed %s" % (label, bid, type(e).__name__))
                    r.kv(desk=label, id=bid, kind="?", verdict="CLICK-FAILED", chars="-", markers="-", href="-", excerpt=str(e)[:80].replace("|", "/"))
                    continue
                page.wait_for_timeout(1500)
                ov0 = page.evaluate(OVERLAY_JS)
                if bid in TOGGLES or not (ov0.get("present") and ov0.get("on")):
                    # a chart-mark toggle (Accum / Bottom / Dist / Demand / Supply...): no overlay, the button lights up and a toast may explain
                    page.wait_for_timeout(2500)
                    s2 = page.evaluate(SNAP_JS)
                    cls = page.evaluate("(id) => (document.getElementById(id) || {}).className || ''", bid)
                    toast = s2.get("toast") or ""
                    page.screenshot(path=str(shots / ("%s.png" % bid)))
                    verdict = "TOGGLED" if ("on" in cls.split() or toast) else "NO-VISIBLE-EFFECT"
                    r.log("%-8s %-9s toggle: class %r toast %r (%.1fs)" % (label, bid, cls, toast[:160], time.time() - t1))
                    r.kv(desk=label, id=bid, kind="toggle", verdict=verdict, chars="-", markers="-", href="-", excerpt=(toast or cls)[:160].replace("|", "/"))
                    try:
                        page.click("#" + bid, timeout=4000)  # toggle back
                        page.wait_for_timeout(800)
                    except Exception:  # noqa: BLE001
                        pass
                    continue
                # overlay desks: wait for the iframe (same-origin) or the inline host to settle
                best = None
                for i in range(16):
                    page.wait_for_timeout(1000)
                    ov = page.evaluate(OVERLAY_JS)
                    inner = ov.get("inner") or ov.get("inline") or {}
                    if best is None or (inner.get("len") or 0) > ((best.get("inner") or best.get("inline") or {}).get("len") or 0):
                        best = ov
                    if i >= 5 and (inner.get("len") or 0) > 400 and (inner.get("ready") in (None, "complete")):
                        break
                ov = best or {}
                inner = ov.get("inner") or ov.get("inline") or {}
                kind = "inline" if bid in INLINE else "iframe"
                chars = inner.get("len") if isinstance(inner.get("len"), int) else -1
                excerpt = (inner.get("excerpt") or inner.get("error") or "")
                markers = len(MARK_RE.findall(excerpt))
                page.screenshot(path=str(shots / ("%s.png" % bid)))
                if not ov.get("present") or not ov.get("on"):
                    verdict = "NO-OVERLAY"
                elif chars < 80:
                    verdict = "EMPTY"
                elif re.search(r"did not start|could not load|is not on this page", excerpt, re.I):
                    verdict = "BROKEN"
                else:
                    verdict = "OK"
                if verdict != "OK":
                    empty.append("%s:%s" % (label, verdict))
                r.log("%-8s %-9s %s %-12s chars %6s el %5s canv %3s tbl %3s rows %4s markers %2d (%.1fs) %s | %s" % (
                    label, bid, kind, verdict, chars, inner.get("elements", "-"), inner.get("canvases", "-"), inner.get("tables", "-"), inner.get("rows", "-"), markers, time.time() - t1,
                    path_only(ov.get("iframe_src") or ov.get("href") or ""), excerpt[:300].replace("\n", " ")))
                r.kv(desk=label, id=bid, kind=kind, verdict=verdict, chars=chars, markers=markers, href=path_only(ov.get("iframe_src") or ov.get("href") or ""), excerpt=excerpt[:160].replace("|", "/"))
                try:
                    page.click("#wsx", timeout=4000)
                except Exception:  # noqa: BLE001
                    page.keyboard.press("Escape")
                page.wait_for_timeout(600)
            # GO keyboard
            try:
                page.click("#btn-go", timeout=5000)
                page.wait_for_timeout(3000)
                ov = page.evaluate(OVERLAY_JS)
                inner = ov.get("inner") or ov.get("inline") or {}
                page.screenshot(path=str(shots / "btn-go.png"))
                r.log("GO       btn-go    overlay on=%s title %r chars %s excerpt %r" % (ov.get("on"), ov.get("title"), inner.get("len"), (inner.get("excerpt") or "")[:200]))
                r.kv(desk="GO", id="btn-go", kind="iframe", verdict="OK" if (ov.get("on") and (inner.get("len") or 0) > 80) else "EMPTY", chars=inner.get("len", -1), markers="-", href=path_only(ov.get("iframe_src") or ""), excerpt=(inner.get("excerpt") or "")[:160].replace("|", "/"))
                try:
                    page.click("#wsx", timeout=4000)
                except Exception:  # noqa: BLE001
                    page.keyboard.press("Escape")
            except Exception as e:  # noqa: BLE001
                r.warn("GO: %s" % type(e).__name__)
            r.section("3. dock tabs")
            tabs = page.evaluate("() => Array.from(document.querySelectorAll('#dtabs button')).map((b, i) => ({i, t: (b.innerText || '').trim()}))")
            for tb in tabs:
                try:
                    page.locator("#dtabs button", has_text=tb["t"]).first.click(timeout=5000)
                    page.wait_for_timeout(2500)
                    body = page.evaluate("() => { const e = document.getElementById('dockbody'); const t = e ? (e.innerText || '') : ''; return {len: t.length, excerpt: t.replace(/\\s+/g, ' ').trim().slice(0, 240), canv: e ? e.querySelectorAll('canvas, svg').length : 0}; }")
                    r.log("dock %-14s chars %6d canv %2d | %s" % (tb["t"][:14], body["len"], body["canv"], body["excerpt"][:200]))
                    r.kv(desk="dock:" + tb["t"][:14], id="dtabs#%d" % tb["i"], kind="dock", verdict="OK" if body["len"] > 40 else "EMPTY", chars=body["len"], markers=len(MARK_RE.findall(body["excerpt"])), href="-", excerpt=body["excerpt"][:160].replace("|", "/"))
                except Exception as e:  # noqa: BLE001
                    r.warn("dock %s: %s" % (tb["t"], type(e).__name__))
            page.screenshot(path=str(shots / "99-final.png"))
            ctx.close()
            browser.close()
        r.section("4. errors and failed requests")
        r.log("page errors (%d): %s" % (len(errors), json.dumps(errors[:25])))
        errs = [t for ty, t in console if ty == "error"]
        r.log("console errors (%d, first 25): %s" % (len(errs), json.dumps(errs[:25])))
        r.log("console warnings: %d" % sum(1 for ty, _ in console if ty == "warning"))
        from collections import Counter
        fc = Counter(failed)
        r.log("failed responses/requests (%d distinct): %s" % (len(fc), json.dumps([(str(k[0]), k[1], n) for k, n in fc.most_common(40)])))
        up = 0
        for f in sorted(shots.glob("*.png")):
            try:
                s3.put_object(Bucket=PRIVATE, Key="probes/chart/%s/%s" % (stamp, f.name), Body=f.read_bytes(), ContentType="image/png", ServerSideEncryption="AES256")
                up += 1
            except Exception as e:  # noqa: BLE001
                r.warn("upload %s: %s" % (f.name, type(e).__name__))
        r.log("screenshots: %d uploaded to s3://%s/probes/chart/%s/" % (up, PRIVATE, stamp))
        r.log("SUMMARY: desks %d; not OK: %s; page errors %d; console errors %d; failed requests %d" % (len(snap.get("desks") or []), ", ".join(empty) or "none", len(errors), len(errs), sum(fc.values())))
        r.ok("probe done")


if __name__ == "__main__":
    main()
