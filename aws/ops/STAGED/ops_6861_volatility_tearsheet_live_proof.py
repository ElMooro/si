"""ops 6861 - READ-ONLY live proof of the tearsheet with the donut on the Volatility desk, in Chrome on the runner (re-runnable).

Khalid 2026-11-20: "fix these: ... This desk has no tearsheet with the donut yet."

CLAUDE.md "CHECK YOUR WORK": stock-volatility.html is opened on justhodl.ai at 1440 and 390 px and the sheet is read
back against the artifact the site serves (data/stock-volatility.json), fetched separately:
  - the ring's total is the market value of the ranked stocks summed over the feed's own bands, one slice a band;
  - the tiles carry the feed's medians, SPY and the count set aside;
  - every list is the engine's board, name for name, at 6 rows, 15 after MORE ROWS, 3 on a phone;
  - 1 DAY / 1 WEEK / 1 MONTH / 1 QUARTER move the two spread lists and the spread tile, nothing else;
  - the desk's own sections keep their numbers and the desk under the sheet still draws; nothing spills sideways;
  - the sheet is there when the desk is opened from the chart's Volatility button.
Writes nothing.
"""
from datetime import datetime, timezone
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
KEY = "data/stock-volatility.json"
UA = "Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome"
LISTS = {"most-volatile": "most_volatile", "least-volatile": "least_volatile", "rising": "vol_rising", "falling": "vol_falling"}
SECTIONS = ["verdict", "market", "boards", "table", "groups", "rules-sec", "sources-sec"]

STATE = r"""() => { const el = document.getElementById('jh-tearsheet'); if (!el) return null; const q = (s) => Array.from(el.querySelectorAll(s));
  const ring = el.querySelector('[data-jts-list="ring"]'), lists = {};
  q('[data-jts-list]').forEach(c => { const k = c.getAttribute('data-jts-list'); if (k !== 'ring') lists[k] = { n: (c.querySelector('.jts-n') || {}).textContent,
    rows: Array.from(c.querySelectorAll('tbody tr')).map(tr => [tr.querySelector('.jts-nm').textContent.split(' \u00b7 ')[0], tr.querySelector('.jts-big').textContent, tr.querySelector('.jts-big').className]) }; });
  return { state: el.dataset.state, chrome: el.hasAttribute('data-jh-chrome'), sec: el.getAttribute('data-jh-sec'), h: Math.round(el.getBoundingClientRect().height), top: Math.round(el.getBoundingClientRect().top + scrollY),
    eye: (el.querySelector('.jts-eye') || {}).textContent, title: (el.querySelector('.jts-h') || {}).textContent, foot: (el.querySelector('.jts-foot') || {}).textContent,
    tiles: q('.jts-kpi').map(k => [k.querySelector('.k').textContent, k.querySelector('.v').textContent, k.querySelector('.s').textContent]),
    cards: q('.jts-card').map(c => [c.querySelector('.big').textContent, c.querySelector('.t').textContent]), notes: q('.jts-notes p').length,
    ctl: q('[data-jts-ctl]').map(b => [b.getAttribute('data-jts-ctl'), b.textContent, b.getAttribute('aria-pressed')]), acts: q('[data-jts-act]').map(b => b.textContent),
    total: ring ? Number(ring.getAttribute('data-total')) : null, center: (el.querySelector('.jts-rc') || {}).textContent, arcs: q('.jts-arc').map(a => [a.getAttribute('class'), Number(a.getAttribute('data-share')), Number(a.getAttribute('fill-opacity'))]),
    legend: q('.jts-leg tr').map(tr => tr.textContent.trim().slice(0, 60)), lists: lists, coloured: q('.jts-big.up, .jts-big.down, .jts-col.up, .jts-col.down, .jts-card.up, .jts-card.down').length,
    secs: Array.from(document.querySelectorAll('[data-jh-sec]')).map(e => e.getAttribute('data-jh-sec') + ' ' + e.id), desk_rows: document.querySelectorAll('#rows tbody tr a.tk').length,
    verdict_top: Math.round(document.getElementById('verdict').getBoundingClientRect().top + scrollY), ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""


def ensure_playwright(log):
    try:
        import playwright  # noqa: F401
    except ImportError:
        log("installing playwright")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
    from playwright.sync_api import sync_playwright
    return sync_playwright


def launch(p, log):
    try:
        return p.chromium.launch(channel="chrome", headless=True)
    except Exception as exc:  # noqa: BLE001
        log("system Chrome not usable (%s): installing chromium" % str(exc)[:80])
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
        return p.chromium.launch(headless=True)


def get_feed(stamp):
    req = urllib.request.Request("%s/%s?nocache=%d" % (BASE, KEY, stamp), headers={"User-Agent": "Mozilla/5.0 jh-ops"})
    with urllib.request.urlopen(req, timeout=90) as rs:
        return json.loads(rs.read().decode("utf-8"))


def vp(v):
    return "%s%%" % format(v, ",.1f")


def pp(v):
    return "%s%%" % format(v, ",.2f" if abs(v) < 10 else ",.1f")


def same(text, v, dp=None):
    """The printed percentage is v rounded to the digits it shows (the page rounds a tie its own way)."""
    try:
        got = float(str(text).replace("%", "").replace(",", "").replace("\u2212", "-"))
    except ValueError:
        return False
    dp = (2 if abs(v) < 10 else 1) if dp is None else dp
    return str(text).endswith("%") and abs(got - v) <= 0.5 * 10 ** -dp + 1e-9


def mains(secs):
    return [x for x in secs if x.split(" ")[1] in SECTIONS]


def main():
    with report("ops_6861_volatility_tearsheet_live_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6861 - the Volatility tearsheet, live %s" % now.isoformat(timespec="minutes"))
        stamp = int(time.time())
        fails, shots = [], {}

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL %s" % msg)
            return bool(cond)

        doc = get_feed(stamp)
        rows = {x[0]: dict(zip(doc["cols"], x)) for x in doc["rows"]}
        med, bench, u, boards = doc["market"]["stock"]["med"], doc["market"]["benchmark"], doc["universe"], doc["boards"]
        bands = [b for b in doc["bands"] if b.get("mcap")]
        total = sum(b["mcap"] for b in bands)
        hot = sum(b["mcap"] for b in bands if b["from"] >= 40)
        r.log("the site serves %s: %s %s, session of %s, generated %s; ranked market value $%.2fT in %d bands, %d stocks ranked" % (
            KEY, doc.get("engine"), doc.get("version"), doc.get("as_of"), doc.get("generated_at"), total / 1e12, len(bands), sum(b["n"] for b in bands)))

        def check_sheet(s, limit, k, where):
            """One sheet state against the feed: `limit` rows a list, spread window k."""
            ok = True
            ok &= ck(s and s["state"] == "ready" and s["chrome"] and s["sec"] is None, "%s: the sheet is not ready or is numbered as a section (%s)" % (where, s and (s["state"], s["sec"])))
            ok &= ck(abs(s["total"] - total) <= 1e-6 * total and len(s["arcs"]) == len(bands), "%s: ring total %s vs the feed's bands %s, %d slices of %d" % (where, s["total"], total, len(s["arcs"]), len(bands)))
            ok &= ck(all(abs(a[1] - b["mcap"] / total * 100) < 0.002 for a, b in zip(s["arcs"], bands)), "%s: a slice's share is not its band's" % where)
            ok &= ck(all("pos" in a[0] for a in s["arcs"]) and all(s["arcs"][i][2] > s["arcs"][i - 1][2] for i in range(1, len(s["arcs"]))), "%s: the ring is not one hue stepped by band: %s" % (where, [a[2] for a in s["arcs"]]))
            t = s["tiles"]
            ok &= ck(len(t) == 6 and same(t[0][1], med["hv21"], 1) and same(t[2][1], bench["hv21"], 1) and same(t[3][1], med["sp_" + k]) and t[4][1] == "%d%%" % round(hot / total * 100) and t[5][1] == format(u["set_aside"]["stock"], ",")
                     and t[1][2].startswith("%s rising" % format(doc["market"]["stock"]["rising"], ",")), "%s: tiles %s" % (where, [x[:2] for x in t]))
            ok &= ck(len(s["cards"]) == 2 and s["cards"][0][1].startswith(boards["most_volatile"][0]) and same(s["cards"][0][0], rows[boards["most_volatile"][0]]["hv21"], 1)
                     and s["cards"][1][1].startswith(boards["least_volatile"][0]) and s["notes"] == 3, "%s: cards %s" % (where, s["cards"]))
            want = dict(LISTS, widest="widest_" + k, narrowest="narrowest_" + k)
            for key, board in want.items():
                got = [x[0] for x in s["lists"].get(key, {}).get("rows", [])]
                ok &= ck(got == boards[board][:limit], "%s: list %s shows %s, the board says %s" % (where, key, got[:4], boards[board][:4]))
            got = s["lists"]["widest"]["rows"][0]
            ok &= ck(same(got[1], rows[got[0]]["sp_" + k]), "%s: %s prints %s for the %s spread, the feed has %s" % (where, got[0], got[1], k, rows[got[0]]["sp_" + k]))
            got = s["lists"]["most-volatile"]["rows"][0]
            ok &= ck(same(got[1], rows[got[0]]["hv21"], 1), "%s: %s prints %s, the feed has %s" % (where, got[0], got[1], rows[got[0]]["hv21"]))
            pin = [x[0] for x in s["lists"]["pinned"]["rows"]]
            exp = sorted(doc["pinned"], key=lambda t_: -(rows[t_]["mcap"] or 0))[:limit]
            ok &= ck(pin == exp and s["lists"]["pinned"]["n"] == str(u["pinned"]["stock"]), "%s: pinned %s (want %s), count %s" % (where, pin[:4], exp[:4], s["lists"]["pinned"]["n"]))
            ok &= ck(len(s["lists"]["sectors"]["rows"]) == min(limit, len(doc["by_sector"])) and s["lists"]["sectors"]["rows"][0][0] == max(doc["by_sector"], key=lambda g: g["med"]["hv21"])["sector"], "%s: sectors %s" % (where, s["lists"]["sectors"]["rows"][:2]))
            ok &= ck(s["coloured"] == 0, "%s: %d figures or lists are coloured up / down - volatility is neither" % (where, s["coloured"]))
            ok &= ck([x[0] for x in s["ctl"] if x[2] == "true"] == [k] and [x[1] for x in s["ctl"]] == ["1 DAY", "1 WEEK", "1 MONTH", "1 QUARTER"], "%s: buttons %s" % (where, s["ctl"]))
            return ok

        sync_playwright = ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = launch(p, r.log)
            for width, height, mobile, limit, tag in ((1440, 900, False, 6, "1440"), (390, 844, True, 3, "390")):
                r.section("the desk at %s px" % tag)
                ctx = browser.new_context(viewport={"width": width, "height": height}, device_scale_factor=1, is_mobile=mobile, has_touch=mobile,
                                          user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) jh-ops-probe Mobile Safari" if mobile else UA)
                page = ctx.new_page()
                errors, feed_reads = [], []
                page.on("pageerror", lambda e, errors=errors: errors.append(str(e)[:200]))
                page.on("request", lambda q, feed_reads=feed_reads: feed_reads.append(q.url) if "stock-volatility.json" in q.url else None)
                page.goto("%s/stock-volatility.html?nocache=%d" % (BASE, stamp + width), wait_until="domcontentloaded")
                try:
                    page.wait_for_function("() => (document.getElementById('jh-tearsheet') || {dataset: {}}).dataset.state === 'ready' && document.querySelectorAll('#rows tbody tr a.tk').length > 50", timeout=90000)
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(1200)
                s = page.evaluate(STATE)
                if not ck(bool(s) and s["state"] == "ready", "%s px: the sheet did not come up: %s" % (tag, s and (s["state"], s.get("title")))):
                    shots["failed_" + tag] = page.screenshot(type="jpeg", quality=50)
                    ctx.close()
                    continue
                r.log("eyebrow: %s" % s["eye"])
                r.log("title: %s" % s["title"])
                for t in s["tiles"]:
                    r.log("  tile: %s = %s // %s" % tuple(t))
                r.log("ring: centre %s, total %.0f (feed %.0f), slices %s" % (s["center"], s["total"], total, " | ".join("%.1f%% @%.2f" % (a[1], a[2]) for a in s["arcs"])))
                for line in s["legend"]:
                    r.log("  legend: %s" % line)
                check_sheet(s, limit, "m", "%s px, month" % tag)
                ck(s["eye"].startswith("Volatility tearsheet") and doc["as_of"] in s["eye"] and "The median stock is moving at" in s["title"] and ("justhodl-stock-volatility %s" % doc["version"]) in s["foot"], "%s px: eyebrow / title / source line" % tag)
                ck([x.split(" ")[1] for x in s["secs"] if x.split(" ")[1] in SECTIONS] == SECTIONS and [x.split(" ")[0] for x in s["secs"] if x.split(" ")[1] in SECTIONS] == [str(i) for i in range(2, 9)],
                   "%s px: the desk's sections were renumbered: %s" % (tag, s["secs"]))
                ck(s["top"] < s["verdict_top"] and s["desk_rows"] >= 200 and s["ox"] <= 0 and "COPY LINK" in s["acts"] and any("PRINT" in a for a in s["acts"]),
                   "%s px: sheet at %s, verdict at %s, %d desk rows, overflow %s, actions %s" % (tag, s["top"], s["verdict_top"], s["desk_rows"], s["ox"], s["acts"]))
                r.log("sections %s | sheet %d px tall above the verdict | desk rows %d | sideways overflow %s | feed requests %d" % (
                    ", ".join(s["secs"]), s["h"], s["desk_rows"], s["ox"], len(feed_reads)))
                for key in ("most-volatile", "least-volatile", "widest", "narrowest", "rising", "falling", "sectors", "pinned"):
                    r.log("  %-15s %5s | %s" % (key, s["lists"][key]["n"], ", ".join("%s %s" % (x[0], x[1]) for x in s["lists"][key]["rows"][:6])))
                shots["sheet_" + tag] = page.screenshot(type="jpeg", quality=55 if not mobile else 50, clip={"x": 0, "y": max(0, s["top"] - 8), "width": width, "height": min(s["h"] + 16, 1500 if not mobile else 1400)})
                # the four windows
                for k in ("d", "w", "q", "m"):
                    page.evaluate("(k) => document.querySelector('#jh-tearsheet [data-jts-ctl=\"' + k + '\"]').click()", k)
                    page.wait_for_timeout(250)
                    sk = page.evaluate(STATE)
                    if check_sheet(sk, limit, k, "%s px, window %s" % (tag, k)):
                        r.log("window %s: median spread %s | widest %s | narrowest %s" % (k, sk["tiles"][3][1], ", ".join("%s %s" % (x[0], x[1]) for x in sk["lists"]["widest"]["rows"][:4]),
                                                                                      ", ".join("%s %s" % (x[0], x[1]) for x in sk["lists"]["narrowest"]["rows"][:4])))
                # MORE ROWS
                page.evaluate("() => document.querySelector('#jh-tearsheet [data-jts-act=\"more\"]').click()")
                page.wait_for_timeout(300)
                sm = page.evaluate(STATE)
                check_sheet(sm, 15, "m", "%s px, more rows" % tag)
                r.log("MORE ROWS: %s" % ", ".join("%s %d" % (key, len(v["rows"])) for key, v in sm["lists"].items()))
                ck(mains(sm["secs"]) == mains(s["secs"]) and sm["ox"] <= 0, "%s px: more rows moved the desk's sections (%s) or spilled sideways (%s)" % (tag, mains(sm["secs"]), sm["ox"]))
                ck(not errors, "%s px: page errors %s" % (tag, errors[:3]))
                ctx.close()

            # ------------------------------------------------------------------ from the chart's Volatility button
            r.section("chart.html: the Volatility button opens the desk with its sheet")
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent=UA)
            page = ctx.new_page()
            cerr = []
            page.on("pageerror", lambda e: cerr.append(str(e)[:200]))
            page.goto("%s/chart.html?s=AAPL&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#btn-svol", timeout=90000, state="attached")
            page.wait_for_function("() => typeof window.jhOpenWorkspace === 'function'", timeout=90000)
            page.wait_for_timeout(1000)
            page.evaluate("() => document.getElementById('btn-svol').click()")
            fr = None
            for _ in range(80):
                fr = next((f for f in page.frames if "stock-volatility" in f.url), None)
                if fr:
                    break
                page.wait_for_timeout(250)
            if ck(fr is not None, "no workspace frame for the desk"):
                try:
                    fr.wait_for_function("() => (document.getElementById('jh-tearsheet') || {dataset: {}}).dataset.state === 'ready'", timeout=90000)
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(700)
                fs = fr.evaluate(STATE)
                check_sheet(fs, 6, "m", "inside the chart")
                r.log("inside the chart's workspace: %s | ring %s | %d desk rows" % (fs and fs["title"], fs and fs["center"], fs and fs["desk_rows"]))
                shots["chart_workspace_1440"] = page.screenshot(type="jpeg", quality=50)
            ck(not cerr, "page errors on the chart: %s" % cerr[:3])
            ctx.close()
            browser.close()

        r.kv(failures=len(fails), as_of=str(doc["as_of"]), ring_total_usd=round(total), bands=len(bands), median_hv21=med["hv21"], set_aside=u["set_aside"]["stock"])
        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        if fails:
            r.fail("%d checks failed: %s" % (len(fails), " | ".join(fails)[:1800]))
            sys.exit(1)
        r.ok("LIVE PROOF: the Volatility tearsheet's ring is the feed's ranked market value ($%.2fT) by band, its lists are the engine's boards at every window, and the desk beneath it is unchanged" % (total / 1e12))


if __name__ == "__main__":
    main()
