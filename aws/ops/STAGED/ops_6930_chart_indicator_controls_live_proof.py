"""ops 6930 - chart.html: every indicator closes, hides and shows - live proof in a real browser (read-only, re-runnable).

Khalid 2026-11-23: "next thing i want you to look at are my indicators in my chart.html the indicators are good but
some of them wont close and remove out of the chart and visible and invisible doesnt always work".

What has to hold on https://justhodl.ai/chart.html (NVDA, daily; a fresh browser with auto refresh off so two pictures
of the same chart are the same picture):
  1. the sweep - every study in the Indicators list is switched on from the list, hidden with its eye, shown again,
     closed with its cross, the way a person does it (pointer over the row, onto the button, down, up).  After each
     step the chart is photographed: hidden must look like the chart before the study (the legend aside), shown must
     be the picture it was, closed must be the chart before, pixel for pixel; adding or closing must not move the zoom;
  2. Volume has a real eye: hides, shows, survives a reload hidden; its cross removes it and it stays removed after a
     reload; the Indicators list brings it back;
  3. indicator windows: the price chart keeps its zoom and at least two fifths of the column; the same bar stands at
     the same x in every window; a hidden window folds to its title row; with seven switched on the area scrolls and
     the one not drawn is named with a cross that closes it; eye and cross are on screen without hovering;
  4. a lit row in the Indicators list switches the study off (a hidden one is shown first); an indicator moved onto the
     price chart has an eye;
  5. a press survives a repaint that lands under it; the legend answers at once while the S&P 500 history downloads;
  6. Supply & Demand: the list switches it on, the cross closes it with the Demand button lit and it stays closed
     after a reload; a desk chip needs one press after a close from the legend; an old studio snapshot in the browser
     no longer undoes the saved layout at a reload;
  7. a phone, touch only: the price chart keeps its height with windows open, the buttons are there and taps work.
JPEGs are embedded (base64).  Nothing on the site changes; everything this op switches on it also closes, in a
private browser profile.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import io
import json
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

BASE = "https://justhodl.ai"
UA = {"User-Agent": "Mozilla/5.0 jh-ops"}
SEED = "try{ if(!localStorage.getItem('jh-chart-v12-tv-layout')) localStorage.setItem('jh-chart-v12-tv-layout', JSON.stringify({kindV:3, liveOn:false})); }catch(e){}" + \
    "document.addEventListener('DOMContentLoaded', function(){ var s = document.createElement('style'); s.textContent = '#toast,#jhwl-toast{visibility:hidden!important}'; document.head.appendChild(s); });" + \
    "(function(){ var f = window.fetch, n = 0; if (!f) return; window.__jhFlight = function(){ return n; }; window.fetch = function(){ n++; var p = f.apply(this, arguments), done = function(){ n--; }; p.then(done, done); return p; }; })();"
# In a browser that has never seen them, the watchlist beside the chart announces each of its signal alerts once, in a
# message at the bottom centre ("SSO: big weekly outflow ...", #jhwl-toast, 2.2 s each) during the first minutes after a
# load; one landing in a picture made 23 steps of the first sweeps differ.  In this op's browser they are not drawn.
# The page's downloads are counted (window.__jhFlight) so a picture of a study that fetches something of its own
# (5-minute bars, the earnings calendar, news, insiders) is taken when the answer has landed.
SAME = 12          # two pictures at most this many pixels apart count as the same; each such pair is counted, located and (the first few) pictured enlarged
READY = "() => window.lastBars && window.lastBars.length > 100 && window.paint && window.INDS && window.OSC && document.querySelector('#legend .leg-sym')"
MAIN = "(window.jhDeskChart || (window.__charts || [])[0])"

STUDIES = """() => { const out = [{ id: 'vol', n: 'Volume', osc: false, cat: 'Volume' }];
  (window.INDS || []).forEach(i => out.push({ id: i.id, n: i.n, osc: false, cat: i.cat || 'Overlay' }));
  (window.OSC || []).forEach(o => out.push({ id: o.id, n: o.n, osc: true, cat: o.cat || 'Oscillator', ext: o.externalPane === true }));
  return out }"""
ADD = """([id, osc]) => { window.jhInduxOpen(); const tab = document.querySelector('#inddlg [data-tab=' + (osc ? 'osc' : 'tech') + ']'); if (tab) tab.click();
  const b = document.querySelector('#indlist [data-add="' + id + '"]'); if (!b) { const x = document.getElementById('indx2'); if (x) x.click(); return false }
  b.click(); const x = document.getElementById('indx2'); if (x) x.click(); return true }"""
STATE = """([id, osc]) => { const list = osc ? window.OSC : window.INDS, it = id === 'vol' ? { on: !!window.volOn, hide: !!(document.querySelector('#legend .leg-row[data-kind=vol].dim')) } : (list || []).find(x => x.id === id) || {};
  const row = id === 'vol' ? document.querySelector('#legend .leg-row[data-kind=vol]') : document.querySelector('#legend .leg-row[data-id="' + id + '"]');
  const pane = document.querySelector('#oscwrap .osc[data-oid="' + id + '"]');
  let place = {}; try { place = JSON.parse(localStorage.getItem('jh-osc-place') || '{}') || {} } catch (e) {}
  return { on: !!it.on, hide: !!it.hide, row: !!row, rowKind: row ? row.getAttribute('data-kind') : '', rowDim: !!(row && row.classList.contains('dim')), rowActs: row ? [...row.querySelectorAll('[data-act]')].map(b => b.getAttribute('data-act')) : [],
    pane: !!pane, paneH: pane ? Math.round(pane.getBoundingClientRect().height) : 0, headH: pane ? Math.round(pane.querySelector('.osc-head').getBoundingClientRect().height) : 0,
    paneActs: pane ? [...pane.querySelectorAll('.osc-head [data-act]')].map(b => b.getAttribute('data-act')) : [], docked: place[id] === 'chart' } }"""
BTN = """([id, osc, act]) => { let b = null;
  if (osc) { const pane = document.querySelector('#oscwrap .osc[data-oid="' + id + '"]'); b = pane && pane.querySelector('.osc-head [data-act=' + act + ']');
    if (!b) { const row = document.querySelector('#legend .leg-row[data-kind=dock][data-id="' + id + '"]'); b = row && row.querySelector('[data-act=' + act + ']') } }
  else { const row = id === 'vol' ? document.querySelector('#legend .leg-row[data-kind=vol]') : document.querySelector('#legend .leg-row[data-kind=ov][data-id="' + id + '"]'); b = row && row.querySelector('[data-act=' + act + ']') }
  if (!b) return null; b.scrollIntoView({ block: 'nearest' }); const r = b.getBoundingClientRect(), q = (b.closest('.leg-row, .osc-head') || b).getBoundingClientRect();
  return { x: r.left + r.width / 2, y: r.top + r.height / 2, rx: q.left + Math.min(q.width / 2, 30), ry: q.top + q.height / 2, seen: getComputedStyle(b.parentElement).opacity !== '0' && getComputedStyle(b).opacity !== '0' } }"""
CLIP = """() => { const a = document.getElementById('stage').getBoundingClientRect(), b = document.getElementById('oscwrap').getBoundingClientRect(), v = document.getElementById('volhud').getBoundingClientRect();
  const top = Math.max(0, Math.min(a.top, v.height ? v.top : a.top)), bottom = Math.min(innerHeight, Math.max(a.bottom, b.bottom)); return { x: Math.max(0, a.left), y: top, width: Math.min(innerWidth, a.right) - Math.max(0, a.left), height: bottom - top } }"""
LEGEND = "() => { const r = document.getElementById('chart-readouts').getBoundingClientRect(), c = (" + CLIP + ")(); return [Math.floor(r.left - c.x), Math.floor(r.top - c.y), Math.ceil(r.right - c.x) + 2, Math.ceil(r.bottom - c.y) + 2] }"
# the selected-bar readouts follow the pointer, and the three desk chips stay lit while their study is on but hidden: neither is a leftover
READOUTS = "() => { const c = (" + CLIP + ")(); return ['ohlc', 'hud', 'toast', 'jhwl-toast', '__chips'].map(id => { const e = id === '__chips' ? (document.querySelector('.jh-desk [data-k]') || {}).parentElement : document.getElementById(id); if (!e) return null; let l = 1e9, t = 1e9, r = -1e9, b = -1e9; " + \
    "[e, ...e.querySelectorAll('*')].forEach(n => { const q = n.getBoundingClientRect(); if (!q.width || !q.height) return; l = Math.min(l, q.left); t = Math.min(t, q.top); r = Math.max(r, q.left + Math.max(q.width, n.scrollWidth)); b = Math.max(b, q.bottom) }); " + \
    "return r > l ? [Math.floor(l - c.x) - 4, Math.floor(t - c.y) - 4, Math.ceil(r - c.x) + 24, Math.ceil(b - c.y) + 4] : null }).filter(Boolean) }"
RANGE = "() => { const c = " + MAIN + "; const r = c && c.timeScale().getVisibleLogicalRange(); return r ? [r.from, r.to] : null }"
HOLD = "(v) => { const c = " + MAIN + "; if (c && v) c.timeScale().setVisibleLogicalRange({ from: v[0], to: v[1] }) }"
LAYOUT = """() => { const main = """ + MAIN + """, bars = window.lastBars, t = bars[bars.length - 8].time, r = main.timeScale().getVisibleLogicalRange(), w = document.getElementById('oscwrap'), st = document.getElementById('stage'), col = st.parentNode;
  let used = 0; [...col.children].forEach(k => { if (k !== st && k !== w && getComputedStyle(k).display !== 'none' && !/absolute|fixed/.test(getComputedStyle(k).position)) used += k.offsetHeight });
  const panes = [...document.querySelectorAll('#oscwrap .osc')].map(p => { const h = p.querySelector('.osc-host'), c = h && h.__jhChart; return { id: p.getAttribute('data-oid'), h: Math.round(p.getBoundingClientRect().height), x: c && p.dataset.hid !== '1' ? Math.round(c.timeScale().timeToCoordinate(t) * 10) / 10 : null, hid: p.dataset.hid === '1', ops: getComputedStyle(p.querySelector('.osc-head .leg-ops') || p).opacity } });
  return { range: r ? [Math.round(r.from * 10) / 10, Math.round(r.to * 10) / 10] : null, mainX: Math.round(main.timeScale().timeToCoordinate(t) * 10) / 10, chart: Math.round(st.getBoundingClientRect().height), column: col.clientHeight - used, area: Math.round(w.getBoundingClientRect().height), areaInside: w.scrollHeight,
    on: window.OSC.filter(o => o.on).map(o => o.id), note: (document.querySelector('.osc-more') || {}).textContent || '', panes } }"""


def ensure_playwright(log):
    try:
        import playwright  # noqa: F401
    except Exception:
        log("installing playwright")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
    try:
        import PIL  # noqa: F401
    except Exception:
        log("installing pillow")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "pillow"], check=True)
    from playwright.sync_api import sync_playwright
    return sync_playwright


def launch(p, log):
    try:
        return p.chromium.launch(channel="chrome", headless=True)
    except Exception as e:  # noqa: BLE001
        log("chrome channel unavailable (%s); installing chromium" % type(e).__name__)
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
        return p.chromium.launch(headless=True)


def fetch(url, timeout=60):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


class Cam:
    """Photographs the chart column once it stops changing and counts the pixels that differ between two pictures."""

    def __init__(self, page):
        from PIL import Image, ImageChops
        self.page, self.Image, self.Chops, self.view, self.near, self.near_pics, self.capped = page, Image, ImageChops, None, [], [], 0

    def quiet(self, cap=6000):
        """Wait until the page has no download out (two looks 150 ms apart), at most `cap` ms."""
        waited, calm = 0, 0
        while waited < cap:
            n = self.page.evaluate("() => (window.__jhFlight ? window.__jhFlight() : 0)")
            calm = calm + 1 if n == 0 else 0
            if calm >= 2:
                return waited
            self.page.wait_for_timeout(150)
            waited += 150
        self.capped += 1
        return waited

    def park(self):
        vp = self.page.viewport_size
        self.page.mouse.move(vp["width"] + 40, vp["height"] + 40)

    def grab(self):
        im = self.Image.open(io.BytesIO(self.page.screenshot(clip=self.page.evaluate(CLIP)))).convert("RGB")
        im.info["readouts"] = [tuple(max(0, v) for v in box) for box in self.page.evaluate(READOUTS)]
        return im

    def settled(self, tries=14, gap=320):
        self.park()
        self.page.wait_for_timeout(gap)
        if self.view:
            self.page.evaluate(HOLD, self.view)
            self.page.wait_for_timeout(120)
        prev = self.grab()
        for _ in range(tries):
            self.page.wait_for_timeout(gap)
            if self.view:
                self.page.evaluate(HOLD, self.view)
                self.page.wait_for_timeout(100)
            cur = self.grab()
            if cur.size == prev.size and self.diff(cur, prev, exact=True) is None:
                return cur
            prev = cur
        return prev

    def diff(self, a, b, mask=None, exact=False):
        if a.size != b.size:
            return {"px": -1, "box": None, "size": [list(a.size), list(b.size)]}
        d = self.Chops.difference(a, b)
        for box in a.info.get("readouts", []) + b.info.get("readouts", []):
            d.paste((0, 0, 0), box)
        if mask:
            d.paste((0, 0, 0), tuple(mask))
        box = d.getbbox()                       # any channel, any amount
        if box is None:
            return None
        red, green, blue = d.split()
        any_ = self.Chops.lighter(self.Chops.lighter(red, green), blue).point(lambda v: 255 if v else 0)
        px = any_.histogram()[255]
        if px <= SAME and not exact:
            self.near.append({"px": px, "box": list(box)})
            if len(self.near_pics) < 4:          # the few pixels, enlarged twelve times, the two pictures side by side
                x0, y0, x1, y1 = box
                crop = (max(0, x0 - 14), max(0, y0 - 14), min(a.size[0], x1 + 14), min(a.size[1], y1 + 14))
                if (crop[2] - crop[0]) * (crop[3] - crop[1]) <= 120 * 120:
                    left, right = (im.crop(crop).resize(((crop[2] - crop[0]) * 12, (crop[3] - crop[1]) * 12), self.Image.NEAREST) for im in (a, b))
                    both = self.Image.new("RGB", (left.size[0] * 2 + 12, left.size[1]), "white")
                    both.paste(left, (0, 0)); both.paste(right, (left.size[0] + 12, 0))
                    buf = io.BytesIO(); both.save(buf, "PNG")
                    self.near_pics.append(("near_%d_%dpx_at_%d_%d" % (len(self.near_pics) + 1, px, x0, y0), buf.getvalue()))
            return None
        return {"px": px, "box": list(box)}

    def press(self, sid, osc, act, touch=False):
        page = self.page
        hit = page.evaluate(BTN, [sid, osc, act])
        if not hit:
            return None
        if touch:
            page.touchscreen.tap(hit["x"], hit["y"])
            return hit
        page.mouse.move(hit["rx"], hit["ry"])
        page.wait_for_timeout(60)
        hit = page.evaluate(BTN, [sid, osc, act]) or hit
        page.mouse.move(hit["x"], hit["y"])
        page.wait_for_timeout(40)
        page.mouse.down()
        page.wait_for_timeout(30)
        page.mouse.up()
        return hit

    def held(self, sid, osc, act, during, ms=900):
        """Button down, something happens on the page, button up."""
        page = self.page
        hit = page.evaluate(BTN, [sid, osc, act])
        page.mouse.move(hit["rx"], hit["ry"])
        page.wait_for_timeout(60)
        page.mouse.move(hit["x"], hit["y"])
        page.wait_for_timeout(40)
        page.mouse.down()
        page.evaluate(during)
        page.wait_for_timeout(ms)
        page.mouse.up()


def sweep_one(cam, s, base):
    page, sid, osc = cam.page, s["id"], s["osc"]
    r = {"id": sid, "n": s["n"], "osc": osc}
    if sid == "vol":
        page.evaluate("() => window.jhSetVol(false)")
        base = cam.settled()
    pics = [base]
    r["view_before"] = page.evaluate(RANGE)
    if not page.evaluate(ADD, [sid, osc]):
        r["in_list"] = False
        return r, base
    r["in_list"] = True
    page.wait_for_timeout(500)
    r["view_after_add"] = page.evaluate(RANGE)          # half a second after switching on, whether its own download has answered or not
    r["row_at_once"] = bool(page.evaluate(BTN, [sid, osc, "x"]))
    cam.quiet()
    on = cam.settled()
    r["on"] = page.evaluate(STATE, [sid, osc])
    r["drawn"] = cam.diff(base, on)
    hit = cam.press(sid, osc, "eye")
    r["eye"] = bool(hit)
    r["eye_seen"] = bool(hit and hit.get("seen"))
    if hit:
        legend = page.evaluate(LEGEND)
        hid = cam.settled()
        r["hidden"] = page.evaluate(STATE, [sid, osc])
        r["hidden_vs_before"] = cam.diff(base, hid, legend)
        r["hidden_vs_on"] = cam.diff(on, hid)
        hit2 = cam.press(sid, osc, "eye")
        r["eye_again"] = bool(hit2)
        if hit2:
            shown = cam.settled()
            r["shown_vs_on"] = cam.diff(on, shown)
    hit3 = cam.press(sid, osc, "x")
    r["x"] = bool(hit3)
    page.wait_for_timeout(500)
    r["view_after_close"] = page.evaluate(RANGE)
    off = cam.settled()
    r["off"] = page.evaluate(STATE, [sid, osc])
    r["off_vs_before"] = cam.diff(base, off)
    if r["off_vs_before"] is not None or r["off"]["on"]:
        page.evaluate("""([id, osc]) => { const it = (osc ? window.OSC : window.INDS).find(x => x.id === id); if (it) { it.on = false; it.hide = false }
          try { const m = JSON.parse(localStorage.getItem('jh-osc-place') || '{}') || {}; delete m[id]; localStorage.setItem('jh-osc-place', JSON.stringify(m)) } catch (e) {}
          if (id === 'vol') window.jhSetVol(false); else window.paint(window.lastBars) }""", [sid, osc])
        base = cam.settled()
        r["rebased"] = True
    r["_pics"] = {"before": pics[0], "on": locals().get("on"), "hidden": locals().get("hid"), "shown": locals().get("shown"), "closed": off}
    if sid == "vol":
        page.evaluate("() => window.jhSetVol(true)")
        base = cam.settled()
    return r, base


def evidence(cam, row, shots, limit=8):
    """Where a step left a difference, the two pictures side by side around it (what it should be | what it was)."""
    pics = row.pop("_pics", None) or {}
    if not row.get("verdict"):
        return
    for key, a, b in (("shown_vs_on", "on", "shown"), ("off_vs_before", "before", "closed"), ("hidden_vs_before", "before", "hidden")):
        d = row.get(key)
        if not d or not d.get("box") or not pics.get(a) or not pics.get(b) or pics[a].size != pics[b].size or len([k for k in shots if k.startswith("differs_")]) >= limit:
            continue
        if key == "hidden_vs_before" and row.get("osc"):      # a folded window is meant to differ from no window: its title row is there
            continue
        x0, y0, x1, y1 = d["box"]
        box = (max(0, x0 - 60), max(0, y0 - 60), min(pics[a].size[0], x1 + 60), min(pics[a].size[1], y1 + 60))
        left, right = pics[a].crop(box), pics[b].crop(box)
        both = cam.Image.new("RGB", (left.size[0] * 2 + 12, left.size[1]), "white")
        both.paste(left, (0, 0))
        both.paste(right, (left.size[0] + 12, 0))
        buf = io.BytesIO()
        both.save(buf, "JPEG", quality=70)
        shots["differs_%s_%s" % (row["id"], key)] = buf.getvalue()


def verdict(r):
    """What a person would notice.  Empty = works."""
    out = []
    if not r.get("in_list"):
        return ["not in the Indicators list"]
    drawn = r.get("drawn") is not None
    if r["id"] == "buyback":          # a text section under the chart with its own Close button; checked on its own below
        return out
    if not r.get("eye"):
        out.append("no eye")
    else:
        if not r.get("eye_seen"):
            out.append("the eye is not on screen until the pointer is over the row")
        if drawn:
            if r.get("hidden_vs_on") is None:
                out.append("the eye does nothing")
            elif r.get("hidden_vs_before") is not None and not r["osc"]:
                out.append("hidden, something stays on the chart (%s px in %s)" % (r["hidden_vs_before"]["px"], r["hidden_vs_before"]["box"]))
            if r["osc"] and r.get("hidden", {}).get("pane") and r["hidden"].get("paneH", 0) > r["hidden"].get("headH", 22) + 4:
                out.append("hidden, the window stays on screen (%s px tall)" % r["hidden"]["paneH"])
            if not r.get("eye_again"):
                out.append("once hidden there is no eye to show it again")
            elif r.get("shown_vs_on") is not None:
                out.append("shown again it is not the picture it was (%s px)" % r["shown_vs_on"]["px"])
    if not r.get("x"):
        out.append("no cross")
    if r.get("off", {}).get("on"):
        out.append("still on after the cross")
    for key, what in (("view_after_add", "adding"), ("view_after_close", "closing")):
        v0, v1 = r.get("view_before"), r.get(key)
        if v0 and v1 and (abs(v1[0] - v0[0]) > 0.6 or abs(v1[1] - v0[1]) > 0.6):
            out.append("%s it moves the chart (%s to %s)" % (what, [round(x, 1) for x in v0], [round(x, 1) for x in v1]))
    if r.get("in_list") and r.get("row_at_once") is False and r["id"] != "buyback":
        out.append("half a second after switching it on there is no row or window to close it by")
    if r.get("off_vs_before") is not None:
        d = r["off_vs_before"]
        out.append("closed, something stays on the chart (%s px in %s)" % (d["px"], d.get("box") or d.get("size")))
    return out


def open_chart(browser, base, route=None, phone=False, extra=""):
    kw = dict(viewport={"width": 390, "height": 844}, device_scale_factor=2, has_touch=True, is_mobile=True) if phone else dict(viewport={"width": 1500, "height": 950}, device_scale_factor=1)
    ctx = browser.new_context(**kw)
    if route:
        ctx.route("**/*", route)
    ctx.add_init_script(SEED)
    page = ctx.new_page()
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)[:200]))
    for attempt in range(5):               # the edge was slow to answer for two minutes after a deploy: ask again
        try:
            page.goto("%s/chart.html?s=NVDA&tf=1d%s" % (base, extra), wait_until="domcontentloaded", timeout=60000)
            settle(page)
            break
        except Exception:  # noqa: BLE001
            if attempt == 4:
                raise
            page.wait_for_timeout(20000)
    return ctx, page, errors


def settle(page):
    """Bars drawn, every script in, and the late starters done (the studio file adds its studies and repaints 1.6 s after load)."""
    page.wait_for_function(READY, timeout=120000)
    try:
        page.wait_for_load_state("load", timeout=60000)
    except Exception:  # noqa: BLE001
        pass
    page.wait_for_timeout(3500)


def reload(page):
    for attempt in range(4):
        try:
            page.reload(wait_until="domcontentloaded", timeout=60000)
            settle(page)
            return
        except Exception:  # noqa: BLE001
            if attempt == 3:
                raise
            page.wait_for_timeout(15000)


def prove(r, ck, browser, base, route=None, shots=None, only=None, cache_wait=62000, budget=2400, part="chart"):
    """Every numbered part of the docstring.  `route` and `only` exist for the rehearsal against a local copy.
    The sweep is run in two halves, each inside one run's hour: part "chart" = Volume and the studies drawn on the
    price chart, then everything else here; part "windows" (ops 6932) = the oscillators and Buyback, and nothing else."""
    shots = {} if shots is None else shots
    ctx, page, errors = open_chart(browser, base, route)
    cam = Cam(page)

    r.section("1. The sweep: %s - on, hidden, shown, closed" % ("every oscillator in the Indicators list" if part == "windows" else "Volume and every study drawn on the price chart"))
    studies = page.evaluate(STUDIES)
    cam.view = page.evaluate(RANGE)
    a, b = cam.settled(), cam.settled()
    r.log("NVDA daily, %d bars, auto refresh off; %d studies in the list (%d on the price chart, %d oscillators, Volume); the chart shows bars %s; two pictures of the untouched chart are identical: %s" % (
        page.evaluate("() => window.lastBars.length"), len(studies), sum(1 for s in studies if not s["osc"]) - 1, sum(1 for s in studies if s["osc"]), [round(x, 1) for x in cam.view], cam.diff(a, b) is None))
    ck(cam.diff(a, b) is None, "the untouched chart is not a steady picture: %s" % cam.diff(a, b))
    base_pic, rows, t0, unswept = a, [], time.time(), []
    for s in studies:
        if only and s["id"] not in only:
            continue
        if not only and s["osc"] != (part == "windows"):
            continue
        if not only and part == "sweep":
            continue
        if time.time() - t0 > budget:          # the run has an hour in all: what is left is named, not silently dropped
            unswept.append(s["id"])
            continue
        try:
            row, base_pic = sweep_one(cam, s, base_pic)
        except Exception as exc:  # noqa: BLE001
            row = {"id": s["id"], "n": s["n"], "osc": s["osc"], "in_list": True, "error": str(exc)[:160]}
            try:
                reload(page)
                cam.view = cam.view and page.evaluate(RANGE) and cam.view
                base_pic = cam.settled()
            except Exception:  # noqa: BLE001
                pass
        row["verdict"] = ["the page failed: " + row["error"]] if row.get("error") else verdict(row)
        evidence(cam, row, shots)
        rows.append(row)
    good = [x for x in rows if not x["verdict"]]
    drew = [x for x in rows if x.get("drawn") is not None]
    if unswept:
        r.log("NOT SWEPT in this run (time): %d studies - %s" % (len(unswept), ", ".join(unswept)))
    ck(not unswept, "%d studies were not swept in the time this run has" % len(unswept))
    r.log("%d studies taken through the four steps in %.0f s: %d work at every step; %d of them drew something on this chart (the rest had nothing to draw on a daily stock chart and still switched on, hid, showed and closed by their row)" % (
        len(rows), time.time() - t0, len(good), len(drew)))
    r.log("the eye was on screen without hovering on %d of %d; hidden then shown gave back the same picture on %d of %d that drew; closed gave back the chart before on %d of %d" % (
        sum(1 for x in rows if x.get("eye_seen")), sum(1 for x in rows if x.get("eye")), sum(1 for x in drew if x.get("eye_again") and x.get("shown_vs_on") is None), len(drew),
        sum(1 for x in rows if x.get("x") and x.get("off_vs_before") is None and not x.get("off", {}).get("on")), len(rows)))
    for x in rows:
        if x["verdict"]:
            r.log("  %-12s %-26s %s" % (x["id"], x["n"][:26], " | ".join(x["verdict"])))
    r.log("half a second after switching on, the study's row or window (with its cross) was there for %d of %d; a download still out after 6 s when its picture was due: %d times" % (
        sum(1 for x in rows if x.get("row_at_once")), sum(1 for x in rows if x.get("in_list")), cam.capped))
    r.log("pictures counted as the same although a few pixels apart (at most %d): %d comparisons%s" % (SAME, len(cam.near), "" if not cam.near else ", largest %d px; where: %s" % (
        max(n["px"] for n in cam.near), sorted({(n["px"], tuple(n["box"])) for n in cam.near})[:8])))
    for name, png in cam.near_pics:
        shots[name] = png
    ck(len(good) == len(rows), "%d of %d studies do not close, hide or show cleanly: %s" % (len(rows) - len(good), len(rows), ", ".join(x["id"] for x in rows if x["verdict"])[:400]))
    for sid in ("vwap", "pat", "avwap", "vprof", "reflevels", "sdmd", "rsi", "macd", "effspread"):
        x = next((y for y in rows if y["id"] == sid), None)
        if x:
            r.log("  %-10s drew %s px | hidden: %s | shown again: %s | closed: %s" % (sid, (x.get("drawn") or {}).get("px"), "nothing left" if x.get("hidden_vs_before") is None or x["osc"] else x["hidden_vs_before"],
                                                                                  "same picture" if x.get("shown_vs_on") is None else x["shown_vs_on"], "chart as before" if x.get("off_vs_before") is None else x["off_vs_before"]))
    if part != "windows":
        r.log("the %d oscillators and Buyback are swept the same way by ops 6932 (one run has an hour)" % sum(1 for s in studies if s["osc"]))
    # Buyback: a text section with its own Close
    bb = "() => { const o = window.OSC.find(x => x.id === 'buyback') || {}; const el = document.getElementById('jh-buyback-pane'); return { on: !!o.on, shown: !!(el && el.style.display !== 'none' && el.children.length) } }"
    if part == "windows" and any(s["id"] == "buyback" for s in studies):
        page.evaluate(ADD, ["buyback", True]); page.wait_for_timeout(2500); b1 = page.evaluate(bb)
        page.evaluate("() => { const c = document.querySelector('#jh-buyback-pane button[aria-label=Close]'); if (c) c.click() }"); page.wait_for_timeout(1500); b2 = page.evaluate(bb)
        r.log("Buyback (a section under the chart with its own Close): on %s -> its Close -> %s" % (b1, b2))
        ck(b1["on"] and b1["shown"] and not b2["on"] and not b2["shown"], "Buyback does not close: %s %s" % (b1, b2))
    if part in ("windows", "sweep"):
        ctx.close()
        return rows
    with_vol = cam.settled()

    r.section("2. Volume: the eye hides and shows, the cross removes, each survives a reload")
    VOL = "() => { let lay = {}; try { lay = JSON.parse(localStorage.getItem('jh-chart-v12-tv-layout')) || {} } catch (e) {} const row = document.querySelector('#legend .leg-row[data-kind=vol]'); return { on: !!window.volOn, row: !!row, dim: !!(row && row.classList.contains('dim')), eye: row ? (row.querySelector('[data-act=eye]') || {}).textContent : '', saved: { volOff: lay.volOff, volHide: lay.volHide } } }"
    cam.press("vol", False, "eye"); hid = cam.settled(); v1 = page.evaluate(VOL)
    d1 = cam.diff(with_vol, hid)
    r.log("eye: %s; %s px of the picture changed (the volume bars)" % (v1, d1 and d1["px"]))
    ck(v1["row"] and v1["dim"] and v1["eye"] == "○" and v1["on"] and d1 and d1["px"] > 5000, "the eye on Volume does not hide it and keep its row: %s %s" % (v1, d1))
    cam.press("vol", False, "eye"); shown = cam.settled(); v2 = page.evaluate(VOL)
    r.log("eye again: %s; against the chart before: %s" % (v2, cam.diff(with_vol, shown) or "the same picture"))
    ck(v2["row"] and not v2["dim"] and cam.diff(with_vol, shown) is None, "Volume shown again is not the picture it was: %s" % cam.diff(with_vol, shown))
    cam.press("vol", False, "eye"); page.wait_for_timeout(600)
    reload(page); v1r = page.evaluate(VOL)
    r.log("hidden, then the page loaded again: %s (pictures are compared within one load only: the live page reads fresh bars each time)" % v1r)
    ck(v1r["row"] and v1r["dim"], "a hidden Volume is not hidden after a reload: %s" % v1r)
    hid2 = cam.settled()
    legend = page.evaluate(LEGEND)
    cam.press("vol", False, "x"); gone = cam.settled(); v3 = page.evaluate(VOL)
    r.log("cross: %s; against the hidden picture, legend aside: %s" % (v3, cam.diff(hid2, gone, legend) or "the same picture"))
    ck((not v3["row"]) and not v3["on"] and cam.diff(hid2, gone, legend) is None, "the cross on Volume does not remove it: %s %s" % (v3, cam.diff(hid2, gone, legend)))
    reload(page); v3r = page.evaluate(VOL)
    r.log("after loading the page again: %s" % v3r)
    ck((not v3r["row"]) and not v3r["on"], "a removed Volume comes back after a reload: %s" % v3r)
    none = cam.settled()
    page.evaluate(ADD, ["vol", False]); back = cam.settled(); v4 = page.evaluate(VOL)
    d4 = cam.diff(none, back)
    r.log("Volume switched on from the Indicators list: %s; %s px of the picture changed (the bars are back)" % (v4, d4 and d4["px"]))
    ck(v4["row"] and not v4["dim"] and d4 and d4["px"] > 5000, "the list does not bring Volume back: %s" % d4)
    with_vol = cam.settled()             # the chart of this load, for what follows

    r.section("3. Indicator windows: the zoom, the room, bars in line, a hidden window, seven of them")
    L0 = page.evaluate(LAYOUT)
    for sid in ("rsi", "macd", "stoch"):
        page.evaluate(ADD, [sid, True]); page.wait_for_timeout(900)
    cam.park(); page.wait_for_timeout(400)
    L3 = page.evaluate(LAYOUT)
    r.log("before: bars %s, price chart %d px of a %d px column" % (L0["range"], L0["chart"], L0["column"]))
    r.log("RSI, MACD, Stochastic on: bars %s; price chart %d px (%.0f%% of the column), window area %d px, nothing to scroll: %s; windows %s" % (
        L3["range"], L3["chart"], 100.0 * L3["chart"] / max(1, L3["column"]), L3["area"], L3["areaInside"] <= L3["area"] + 1, [(p["id"], p["h"]) for p in L3["panes"]]))
    r.log("the eighth bar from the right stands at x = %s on the price chart and at %s in the windows; eye and cross of each window drawn at opacity %s" % (L3["mainX"], [p["x"] for p in L3["panes"]], [p["ops"] for p in L3["panes"]]))
    ck(L3["range"] == L0["range"], "three windows moved the zoom: %s to %s" % (L0["range"], L3["range"]))
    ck(L3["chart"] >= 0.4 * L3["column"] - 2 and L3["chart"] >= 218, "the price chart is squeezed: %d of %d px" % (L3["chart"], L3["column"]))
    ck(len(L3["panes"]) == 3 and all(p["x"] is not None and abs(p["x"] - L3["mainX"]) <= 1 for p in L3["panes"]), "a window is out of line with the price chart: %s vs %s" % ([p["x"] for p in L3["panes"]], L3["mainX"]))
    ck(all(p["ops"] == "1" for p in L3["panes"]), "a window's eye and cross are not on screen")
    shots["1_three_windows"] = page.screenshot(type="jpeg", quality=62)
    cam.press("macd", True, "eye"); page.wait_for_timeout(900)
    LH = page.evaluate(LAYOUT); mh = next(p for p in LH["panes"] if p["id"] == "macd")
    r.log("MACD hidden: its window is %d px (its title row); the others %s; bars %s" % (mh["h"], [(p["id"], p["h"]) for p in LH["panes"] if p["id"] != "macd"], LH["range"]))
    ck(mh["hid"] and mh["h"] <= 40 and LH["range"] == L0["range"], "a hidden window does not fold away: %s" % mh)
    cam.press("macd", True, "eye"); page.wait_for_timeout(900)
    ck(not next(p for p in page.evaluate(LAYOUT)["panes"] if p["id"] == "macd")["hid"], "the eye does not bring the MACD window back")
    for sid in ("cci", "mfi", "obv", "atr"):
        page.evaluate(ADD, [sid, True]); page.wait_for_timeout(900)
    cam.park(); page.wait_for_timeout(400)
    L7 = page.evaluate(LAYOUT)
    r.log("seven oscillators on: %d windows of %s px in an area %d px tall holding %d px (it scrolls); price chart %d px; bars %s; the page says: '%s'" % (
        len(L7["panes"]), sorted(set(p["h"] for p in L7["panes"])), L7["area"], L7["areaInside"], L7["chart"], L7["range"], L7["note"]))
    ck(len(L7["on"]) == 7 and len(L7["panes"]) == 6 and L7["areaInside"] > L7["area"] and L7["chart"] >= 0.4 * L7["column"] - 2 and L7["range"] == L0["range"], "seven windows: %s" % {k: L7[k] for k in ("chart", "column", "area", "areaInside", "range")})
    ck("not shown" in L7["note"] and "×" in L7["note"], "the seventh oscillator is on and nowhere: '%s'" % L7["note"])
    last = L7["panes"][-1]["id"]
    hitl = page.evaluate(BTN, [last, True, "x"])
    r.log("the last window (%s), scrolled to: its cross is at y = %.0f of a %d px screen" % (last, hitl["y"], page.viewport_size["height"]))
    ck(hitl and 0 < hitl["y"] < page.viewport_size["height"], "the last window's cross cannot be reached")
    shots["2_seven_scrolled_to_the_end"] = page.screenshot(type="jpeg", quality=62)
    page.evaluate("() => { const b = document.querySelector('.osc-more [data-more]'); if (b) b.click() }"); page.wait_for_timeout(900)
    L6 = page.evaluate(LAYOUT)
    ck(len(L6["on"]) == 6 and not L6["note"], "the cross in the note does not close the seventh: %s" % L6["on"])
    for sid in list(L6["on"]):
        cam.press(sid, True, "x"); page.wait_for_timeout(700)
    clean = cam.settled()
    Lc = page.evaluate(LAYOUT)
    r.log("all closed with their crosses: %d on, bars %s, price chart %d px; against the chart before: %s" % (len(Lc["on"]), Lc["range"], Lc["chart"], cam.diff(with_vol, clean) or "the same picture"))
    ck(not Lc["on"] and Lc["range"] == L0["range"] and cam.diff(with_vol, clean) is None, "after closing every window the chart is not as before: %s" % cam.diff(with_vol, clean))

    r.section("4. The Indicators list switches a lit study off; an indicator on the price chart has an eye")
    page.evaluate(ADD, ["sma50", False]); page.wait_for_timeout(700); s1 = page.evaluate(STATE, ["sma50", False])
    page.evaluate(ADD, ["sma50", False]); page.wait_for_timeout(700); s2 = page.evaluate(STATE, ["sma50", False])
    ck(s1["on"] and s1["row"] and not s2["on"] and not s2["row"], "a lit row in the list does not switch SMA 50 off")
    page.evaluate(ADD, ["sma50", False]); page.wait_for_timeout(600); cam.press("sma50", False, "eye"); page.wait_for_timeout(700); s3 = page.evaluate(STATE, ["sma50", False])
    page.evaluate(ADD, ["sma50", False]); page.wait_for_timeout(700); s4 = page.evaluate(STATE, ["sma50", False])
    ck(s3["hide"] and s4["on"] and not s4["hide"], "a hidden study pressed in the list is not shown")
    page.evaluate(ADD, ["sma50", False]); page.wait_for_timeout(700)
    page.evaluate(ADD, ["rsi", True]); page.wait_for_timeout(800); o1 = page.evaluate(STATE, ["rsi", True])
    page.evaluate(ADD, ["rsi", True]); page.wait_for_timeout(800); o2 = page.evaluate(STATE, ["rsi", True])
    ck(o1["on"] and o1["pane"] and not o2["on"] and not o2["pane"], "a lit oscillator row in the list does not switch it off")
    r.log("SMA 50: list on %s -> same row again %s; hidden then pressed in the list -> shown %s; RSI: list on %s -> same row again %s" % (s1["on"], s2["on"], s4["on"] and not s4["hide"], o1["on"], o2["on"]))
    page.evaluate(ADD, ["rsi", True]); page.wait_for_timeout(800)
    cam.press("rsi", True, "up"); page.wait_for_timeout(900)
    d1 = page.evaluate(STATE, ["rsi", True]); on_pic = cam.settled()
    legend = page.evaluate(LEGEND)
    cam.press("rsi", True, "eye"); hid_pic = cam.settled(); d2 = page.evaluate(STATE, ["rsi", True])
    cam.press("rsi", True, "eye"); sh_pic = cam.settled(); d3 = page.evaluate(STATE, ["rsi", True])
    cam.press("rsi", True, "x"); off_pic = cam.settled(); d4 = page.evaluate(STATE, ["rsi", True])
    r.log("RSI moved onto the price chart: its legend row has %s; eye -> hidden %s, chart as before (legend aside): %s; eye -> %s; cross -> on %s, chart as before: %s" % (
        d1["rowActs"], d2["hide"], cam.diff(with_vol, hid_pic, legend) is None, "the picture it was" if cam.diff(on_pic, sh_pic) is None else cam.diff(on_pic, sh_pic), d4["on"], cam.diff(with_vol, off_pic) is None))
    ck(d1["docked"] and "eye" in d1["rowActs"] and d2["hide"] and d2["rowDim"] and cam.diff(with_vol, hid_pic, legend) is None and not d3["hide"] and cam.diff(on_pic, sh_pic) is None and not d4["on"] and cam.diff(with_vol, off_pic) is None,
       "an indicator on the price chart does not hide, show and close: %s" % [d1["rowActs"], d2["hide"], d3["hide"], d4["on"]])

    r.section("5. A press is not lost to a repaint; the legend answers while the S&P 500 history downloads")
    page.evaluate(ADD, ["ema9", False]); page.wait_for_timeout(800)
    cam.held("ema9", False, "eye", "() => { window.paint(window.lastBars) }"); page.wait_for_timeout(1200)
    e1 = page.evaluate(STATE, ["ema9", False])
    page.evaluate(ADD, ["cci", True]); page.wait_for_timeout(900)
    cam.held("cci", True, "x", "() => { window.paint(window.lastBars) }"); page.wait_for_timeout(1500)
    c1 = page.evaluate(STATE, ["cci", True])
    r.log("eye of EMA 9 held down while the chart repainted under it, then released: hidden %s; cross of the CCI window the same way: still on %s" % (e1["hide"], c1["on"]))
    ck(e1["hide"] and e1["rowDim"], "a repaint under a held legend button eats the press")
    ck(not c1["on"] and not c1["pane"], "a repaint under a held window button eats the press")
    bench = []
    page.on("requestfinished", lambda q: bench.append((q.url, q.timing)) if ("GSPC" in q.url or "symbol=SPX" in q.url or "ticker=SPX" in q.url) else None)
    r.log("waiting %d s so the chart's 60 s copy of the S&P 500 history is no longer fresh" % (cache_wait / 1000))
    page.wait_for_timeout(cache_wait)
    t = page.evaluate("""async () => { const row = document.querySelector('#legend .leg-row[data-id=ema9]'); const eye = row.querySelector('[data-act=eye]'); const was = row.classList.contains('dim'); const t0 = performance.now(); eye.click();
        for (let i = 0; i < 1500; i++) { await new Promise(r => setTimeout(r, 10)); const r2 = document.querySelector('#legend .leg-row[data-id=ema9]'); if (r2 && r2.classList.contains('dim') !== was) return Math.round(performance.now() - t0); } return -1; }""")
    page.wait_for_timeout(6000)
    took = [round(tm.get("responseEnd", -1)) for _, tm in bench if tm]
    r.log("eye of EMA 9 pressed: its row changed after %d ms; the page asked for the S&P 500 history %d time(s) for that repaint, answered in %s ms" % (t, len(bench), took or "-"))
    ck(0 <= t <= 600, "the legend waits on the benchmark download: %d ms" % t)
    cam.press("ema9", False, "x"); page.wait_for_timeout(800)

    r.section("6. Supply & Demand, the three desk chips, an old studio snapshot")
    ST = "(id) => { const i = window.INDS.find(x => x.id === id) || {}; return { on: !!i.on, row: !!document.querySelector('#legend .leg-row[data-id=\"' + id + '\"]') } }"
    SD = "() => ({ demand: (document.getElementById('btn-demand-rail') || { classList: { contains: () => null } }).classList.contains('on'), supply: (document.getElementById('btn-supply-rail') || { classList: { contains: () => null } }).classList.contains('on') })"
    page.evaluate(ADD, ["sdmd", False]); page.wait_for_timeout(1300); q1, b1 = page.evaluate(ST, "sdmd"), page.evaluate(SD)
    r.log("Supply & Demand switched on from the list: %s, buttons %s" % (q1, b1))
    ck(q1["on"] and q1["row"] and b1["demand"] and b1["supply"], "the list cannot switch Supply & Demand on: %s %s" % (q1, b1))
    cam.press("sdmd", False, "x"); page.wait_for_timeout(1400)
    page.evaluate("() => document.getElementById('btn-demand-rail').click()"); page.wait_for_timeout(1300); q2, b2 = page.evaluate(ST, "sdmd"), page.evaluate(SD)
    cam.press("sdmd", False, "x"); page.wait_for_timeout(1600); q3, b3 = page.evaluate(ST, "sdmd"), page.evaluate(SD)
    reload(page); q4, b4 = page.evaluate(ST, "sdmd"), page.evaluate(SD)
    r.log("Demand button pressed: %s %s; the cross on its legend row: %s %s; after loading the page again: %s %s" % (q2, b2, q3, b3, q4, b4))
    ck(q2["on"] and b2["demand"] and not q3["on"] and not q3["row"] and not b3["demand"] and not q4["on"] and not b4["demand"], "the cross does not close Supply & Demand while Demand is lit: %s %s %s" % (q2, q3, q4))
    page.evaluate("() => document.querySelector('.jh-desk [data-k=vp]').click()"); page.wait_for_timeout(1400); k1 = page.evaluate(ST, "vprof")
    cam.press("vprof", False, "x"); page.wait_for_timeout(1400); k2 = page.evaluate(ST, "vprof")
    page.evaluate("() => document.querySelector('.jh-desk [data-k=vp]').click()"); page.wait_for_timeout(1400); k3 = page.evaluate(ST, "vprof")
    r.log("VP chip: on %s; cross on the legend row: on %s; the chip pressed once more: on %s" % (k1["on"], k2["on"], k3["on"]))
    ck(k1["on"] and not k2["on"] and k3["on"], "the VP chip needs two presses after a close from the legend")
    cam.press("vprof", False, "x"); page.wait_for_timeout(900)
    page.evaluate(ADD, ["ema9", False]); page.wait_for_timeout(900)
    page.evaluate("() => { const st = {}; window.INDS.forEach(i => st[i.id] = { on: false, c: i.c }); st.sma200 = { on: true, c: '#f23645' }; localStorage.setItem('jh-chart-ind-layout-v1', JSON.stringify(st)) }")
    reload(page); page.wait_for_timeout(1500)
    m1, m2 = page.evaluate(ST, "ema9"), page.evaluate(ST, "sma200")
    r.log("an old studio snapshot planted in the browser (EMA 9 off, SMA 200 on) while the saved layout says EMA 9 on, SMA 200 off; after loading the page again: EMA 9 %s, SMA 200 %s" % (m1, m2))
    ck(m1["on"] and not m2["on"], "an old studio snapshot still overrides the saved layout at a reload: EMA 9 %s, SMA 200 %s" % (m1["on"], m2["on"]))
    r.log("page errors through all of this: %d %s" % (len(errors), errors[:3]))
    ck(not errors, "page errors: %s" % errors[:3])
    ctx.close()

    r.section("7. A phone, touch only (390 x 844)")
    ctx, page, errors = open_chart(browser, base, route, phone=True)
    cam = Cam(page)
    PH = "() => ({ chart: Math.round(document.getElementById('host').getBoundingClientRect().height), sideways: document.documentElement.scrollWidth - innerWidth })"
    p0 = page.evaluate(PH)
    page.evaluate(ADD, ["sma20", False]); page.wait_for_timeout(900)
    page.evaluate(ADD, ["rsi", True]); page.wait_for_timeout(900)
    page.evaluate(ADD, ["macd", True]); page.wait_for_timeout(900)
    p1 = page.evaluate(PH)
    hit = cam.press("rsi", True, "eye", touch=True); page.wait_for_timeout(900); t1 = page.evaluate(STATE, ["rsi", True])
    hit2 = cam.press("sma20", False, "eye", touch=True); page.wait_for_timeout(900); t2 = page.evaluate(STATE, ["sma20", False])
    cam.press("macd", True, "x", touch=True); page.wait_for_timeout(900); t3 = page.evaluate(STATE, ["macd", True])
    r.log("price chart %d px with no window, %d px with RSI and MACD open (it had none left before this change); nothing scrolls sideways: %s; the eye of a window is drawn: %s" % (p0["chart"], p1["chart"], p1["sideways"] <= 0, bool(hit and hit["seen"])))
    r.log("tap the eye of RSI: hidden %s (window %d px); tap the eye of SMA 20 on the legend: hidden %s; tap the cross of MACD: on %s" % (t1["hide"], t1["paneH"], t2["hide"], t3["on"]))
    ck(p1["chart"] >= 200 and p1["sideways"] <= 0, "on a phone the price chart has no room with windows open: %s" % p1)
    ck(hit and hit["seen"] and t1["hide"] and t1["paneH"] <= 60 and hit2 and t2["hide"] and not t3["on"], "on a phone taps do not hide and close: %s %s %s" % (t1["hide"], t2["hide"], t3["on"]))
    page.evaluate("() => { const w = document.getElementById('stage'); w.scrollIntoView({ block: 'start' }) }"); page.wait_for_timeout(500)
    shots["3_phone"] = page.screenshot(type="jpeg", quality=60)
    ctx.close()
    return rows


def main():
    from ops_report import report
    with report("ops_6930_chart_indicator_controls_live_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6930 - chart.html: every indicator closes, hides and shows; real browser on the live site (read-only), %s" % now.isoformat(timespec="minutes"))
        stamp = int(time.time())
        fails, shots = [], {}

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL: " + msg)
            return bool(cond)

        r.section("0. The live site serves the new build")
        served, got = False, {}
        want = {"chart.html": "&ind=20261123c&ldr=20261105", "jh-chart-engine.js": "function legendFirst(){", "jh-chart-auction.js": "function studyShown(", "jh-chart-sd-ui.js": "function follow()", "jh-chart-evr.js": "data-act=eye title='Visibility'"}
        for attempt in range(40):
            try:
                got = {f: fetch("%s/%s?nocache=%d" % (BASE, f, stamp + attempt)) for f in want}
            except Exception as exc:  # noqa: BLE001  (a read cut short while the site is being replaced: ask again)
                r.log("try %d: %s" % (attempt + 1, str(exc)[:90]))
                time.sleep(30)
                continue
            served = all(mark in got[f] for f, mark in want.items())
            if served:
                break
            time.sleep(30)
        r.log("served after %d tries: %s" % (attempt + 1, {f: (len(got.get(f, "")), want[f] in got.get(f, "")) for f in want}))
        if not served:
            r.fail("the served site is not the new build yet (Pages deploy pending?)")
            raise SystemExit(1)

        sync_playwright = ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = launch(p, r.log)
            rows = prove(r, ck, browser, BASE, shots=shots)
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            r.log("%s: %d bytes" % (k, len(img)))
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        good = sum(1 for x in rows if not x["verdict"])
        r.kv(studies=len(rows), work_at_every_step=good, failed_checks=len(fails))
        if fails:
            for f in fails:
                r.fail(f)
            raise SystemExit(1)
        r.ok("LIVE: %d of %d studies switch on, hide, show and close with the chart returning to the same picture; Volume has a real eye; windows keep the zoom, stay in line and in reach; presses and reloads hold" % (good, len(rows)))


if __name__ == "__main__":
    main()
    sys.exit(0)
