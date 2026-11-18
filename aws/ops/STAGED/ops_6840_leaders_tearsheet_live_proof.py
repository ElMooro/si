"""ops 6840 - the Leaders tearsheet with its donut: live proof in headless Chrome (read-only, re-runnable).

Khalid 2026-11-18: "now the next one we are going to work on is leaders: apply the same concept to leaders" - asked
which, he chose "Tearsheet with the donut".

Drives https://justhodl.ai/leaders.html at 1440 and 390 px and reads data/leaders.json itself:
  1. the sheet is the first block of the desk, ready, with the picture's anatomy (headline, six tiles, two notable
     cards, three notes, the ring, eleven ranked lists) and no broken value;
  2. the figures are the feed's: the ring's total equals the ranked leaders' market cap summed here, its five slices
     the caps and counts by tape label; the tiles equal the engine's own summary (ahead of the S&P 500, accumulation /
     distribution, smart money, signed dollar volume); the first row of every list is the row this op ranks first;
  3. a fund whose shares split inside the window is set aside (named, with the reason) and never ranked - checked
     against the splits the feed itself records;
  4. the window buttons (week / month / quarter) move the headline and the lists; MORE ROWS opens fifteen;
  5. the desk below is untouched (verdict, boards, industries, leaders, funds render; the sheet is not a numbered
     section; no sideways scroll is added) and the 3 MB feed is not downloaded once more for the sheet;
  6. chart.html: the Leaders button opens the desk in the overlay with the sheet at the top.
JPEGs are embedded (base64). Nothing on the site changes.
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
UA = {"User-Agent": "Mozilla/5.0 jh-ops"}
WIN = {"w": ("the week", "w5", 5, 9), "m": ("the month", "w21", 21, 33), "q": ("the quarter", "w63", 63, 95)}
GRADES = ["STRONG ACCUMULATION", "ACCUMULATION", "NEUTRAL", "DISTRIBUTION", "STRONG DISTRIBUTION"]
LISTS = ["ring", "strongest", "weakest", "printed", "accumulation", "distribution", "smart-buying", "smart-selling", "industries-leading", "industries-lagging", "fund-inflows", "fund-outflows"]


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


def money(v, signed=False):
    if v is None:
        return "—"
    a = abs(v)
    s = "−" if v < 0 else ("+" if signed and v > 0 else "")
    if a >= 1e12:
        return "%s$%.2fT" % (s, a / 1e12)
    if a >= 1e9:
        return "%s$%.2fB" % (s, a / 1e9)
    if a >= 1e6:
        return "%s$%.1fM" % (s, a / 1e6)
    return "%s$%s" % (s, format(int(round(a)), ","))


def ms(day):
    return datetime.strptime(day[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()


def expected(doc, k):
    """What the sheet must show for window k, worked out here from the feed."""
    word, flow, obs, span = WIN[k]
    live = [r for r in doc["rows"] if r.get("vs") and not r.get("stale")]
    lab = lambda r: (r.get("ad") or {}).get("label") or "NEUTRAL"                 # noqa: E731
    read = [r for r in live if isinstance(r["vs"].get(k), (int, float))]
    out = {"n": len(live), "cap": sum(r.get("mcap") or 0 for r in live), "beat": sum(1 for r in read if r["vs"][k] > 0), "read": len(read)}
    out["tiers"] = {g: (sum(r.get("mcap") or 0 for r in live if lab(r) == g), sum(1 for r in live if lab(r) == g)) for g in GRADES}
    out["strongest"] = max(read, key=lambda r: r["vs"][k])["ticker"]
    out["weakest"] = min(read, key=lambda r: r["vs"][k])["ticker"]
    scored = [r for r in live if isinstance((r.get("ad") or {}).get("score"), (int, float))]
    top, low = max(r["ad"]["score"] for r in scored), min(r["ad"]["score"] for r in scored)
    out["accumulation"] = {r["ticker"] for r in scored if r["ad"]["score"] == top}
    out["distribution"] = {r["ticker"] for r in scored if r["ad"]["score"] == low}
    net13 = lambda r: (r.get("f13") or {}).get("net_usd")                         # noqa: E731
    buy = [r for r in live if (r.get("smart") or {}).get("verdict") == "BUYING" and net13(r) is not None]
    sell = [r for r in live if (r.get("smart") or {}).get("verdict") == "SELLING" and net13(r) is not None]
    out["smart-buying"] = max(buy, key=net13)["ticker"] if buy else None
    out["smart-selling"] = min(sell, key=net13)["ticker"] if sell else None
    out["n_buying"] = sum(1 for r in live if (r.get("smart") or {}).get("verdict") == "BUYING")
    out["n_selling"] = sum(1 for r in live if (r.get("smart") or {}).get("verdict") == "SELLING")
    inds = [x for x in doc["industries"] if isinstance((x.get("vs") or {}).get(k), (int, float))]
    out["industries-leading"] = max(inds, key=lambda x: x["vs"][k])["industry"]
    out["industries-lagging"] = min(inds, key=lambda x: x["vs"][k])["industry"]
    mf = [((r.get("mf") or {}).get(k) or {}).get("net") for r in live]
    out["mf_net"] = sum(v for v in mf if isinstance(v, (int, float)))
    kept, aside = [], []
    for f in doc["etfs"]:
        fl = f.get("flow") or {}
        w = fl.get(flow) or {}
        if f.get("stale") or not fl.get("current") or not isinstance(w.get("usd"), (int, float)):
            continue
        end = ms(w.get("end") or fl.get("effective"))
        start = ms(w["start"]) if w.get("start") else end - span * 86400
        split = next((s for s in (f.get("splits") or []) if start <= ms(s["date"]) <= end + 86400), None)
        if split or (w["usd"] > 0 and (w.get("pct_aum") or 0) > 100):
            aside.append(f["ticker"])
        else:
            kept.append((f["ticker"], w["usd"]))
    out["aside"] = sorted(aside)
    ins, outs = [x for x in kept if x[1] > 0], [x for x in kept if x[1] < 0]
    out["fund-inflows"] = max(ins, key=lambda x: x[1])[0] if ins else None
    out["fund-outflows"] = min(outs, key=lambda x: x[1])[0] if outs else None
    out["fund_net"] = sum(x[1] for x in kept)
    out["n_in"], out["n_out"] = len(ins), len(outs)
    return out


SHEET = r"""() => { const s = document.querySelector('[data-jts]'); if (!s) return null; const q = (x) => [...s.querySelectorAll(x)]; const t = (e) => e ? (e.innerText || e.textContent || '').replace(/\s+/g, ' ').trim() : '';
  const r = s.getBoundingClientRect(); const arcs = q('.jts-arc');
  return { state: s.dataset.state, head: t(s.querySelector('.jts-h')), eye: t(s.querySelector('.jts-eye')), sub: t(s.querySelector('.jts-sub')), tiles: q('.jts-kpi').map(t), cards: q('.jts-card').map(e => t(e).slice(0, 300)), notes: q('.jts-notes p').length,
    ctl: q('[data-jts-ctl]').map(e => t(e) + (e.getAttribute('aria-pressed') === 'true' ? '*' : '')),
    cols: q('.jts-col').map(c => ({ k: c.dataset.jtsList, head: t(c.querySelector('.jts-ch')), rows: c.querySelectorAll('.jts-t tbody tr').length, first: t(c.querySelector('tbody tr')).slice(0, 170), name: t(c.querySelector('tbody tr .jts-nm')) })),
    arcs: arcs.length, share: arcs.reduce((a, e) => a + parseFloat(e.dataset.share), 0), hatched: arcs.filter(e => (e.getAttribute('fill') || '').startsWith('url(')).length, labels: q('.jts-rl').map(t), center: t(s.querySelector('.jts-rc')), subs: q('.jts-rs').map(t),
    total: parseFloat((s.querySelector('.jts-ringcol') || { dataset: {} }).dataset.total), legend: q('.jts-leg tr').map(e => t(e).slice(0, 140)), rests: q('.jts-rest').map(e => t(e).slice(0, 330)), foot: t(s.querySelector('.jts-foot')).slice(0, 300),
    sec: s.getAttribute('data-jh-sec'), badges: s.querySelectorAll('.jh-secbadge').length, bad: (s.innerText.match(/undefined|NaN|\[object|\bnull\b|Infinity/g) || []).length,
    align: [...new Set(q('.jts-t:not(.jts-leg) td.n').map(e => getComputedStyle(e).textAlign))],
    wide: q('*').filter(e => !e.closest('defs') && e.tagName !== 'title').filter(e => e.getBoundingClientRect().right > r.right + 1 || e.getBoundingClientRect().left < r.left - 1).length,
    ox: (() => { const f = () => document.documentElement.scrollWidth - document.documentElement.clientWidth; const w = f(); s.style.display = 'none'; const wo = f(); s.style.display = ''; return [w, wo]; })(),
    rect: [Math.round(r.left), Math.round(r.top + scrollY), Math.round(r.width), Math.round(r.height)], next: (s.nextElementSibling || {}).id, version: (window.JHTearsheet || {}).version,
    desk: { verdict: t(document.getElementById('verdict-line')).slice(0, 160), kpis: document.querySelectorAll('#kpis > *').length, tables: ['board', 'ind', 'lead', 'etf'].map(id => (document.getElementById(id) || { querySelectorAll: () => [] }).querySelectorAll('tbody tr').length),
      secs: [...document.querySelectorAll('main [data-jh-key][data-jh-sec]')].map(e => e.getAttribute('data-jh-sec') + ' ' + e.getAttribute('data-jh-key')) } } }"""


def main():
    with report("ops_6840_leaders_tearsheet_live_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6840 - the Leaders tearsheet live %s" % now.isoformat(timespec="minutes"))
        stamp = int(time.time())
        html = urllib.request.urlopen(urllib.request.Request("%s/leaders.html?nocache=%d" % (BASE, stamp), headers=UA), timeout=40).read().decode("utf-8", "replace")
        kit = urllib.request.urlopen(urllib.request.Request("%s/jh-tearsheet.js?nocache=%d" % (BASE, stamp), headers=UA), timeout=40).read().decode("utf-8", "replace")
        new = 'data-jts="leaders"' in html and "ADAPTERS.leaders" in kit and "version: '1.2.0'" in kit
        r.log("served leaders.html %d bytes, kit %d bytes: the new build is live %s" % (len(html), len(kit), new))
        if not new:
            r.fail("the served site is not the new build yet (Pages deploy pending?)")
            raise SystemExit(1)
        doc = json.loads(urllib.request.urlopen(urllib.request.Request("%s/data/leaders.json?nocache=%d" % (BASE, stamp), headers=UA), timeout=60).read())
        S = doc.get("summary") or {}
        r.log("feed: engine %s, bars as of %s, published %s, %d leaders / %d industries / %d funds; summary n %s" % (doc.get("version"), doc.get("as_of"), doc.get("generated_at"), len(doc["rows"]), len(doc["industries"]), len(doc["etfs"]), S.get("n")))
        want = {k: expected(doc, k) for k in WIN}
        E = want["m"]
        r.log("worked out here for the month: ranked %d, market cap %s, ahead %d of %d, tape %s, first rows %s, funds set aside %s" % (E["n"], money(E["cap"]), E["beat"], E["read"], {g: (money(c), n) for g, (c, n) in E["tiers"].items()},
              {k: (sorted(E[k]) if isinstance(E[k], set) else E[k]) for k in LISTS[1:] if k in E}, E["aside"]))
        sync_playwright = ensure_playwright(r.log)
        fails, shots = [], {}

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL: " + msg)

        def judge(s, E, k, width):
            word = WIN[k][0]
            tiles = " || ".join(s["tiles"])
            ck(s["state"] == "ready" and s["bad"] == 0, "%d/%s: state %s, %d broken values" % (width, k, s["state"], s["bad"]))
            ck(abs(s["total"] - E["cap"]) <= 1e-6 * E["cap"] and abs(s["share"] - 100) < 0.01 and s["center"] == money(E["cap"]), "%d/%s: ring total %s / centre %s against the feed's %s" % (width, k, s["total"], s["center"], money(E["cap"])))
            ck(s["arcs"] == 5 and s["hatched"] == 2, "%d/%s: ring %d slices, %d hatched" % (width, k, s["arcs"], s["hatched"]))
            for g, line in zip(GRADES, s["legend"]):
                cap, n = E["tiers"][g]
                ck(money(cap) in line and ("%d leaders" % n) in line, "%d/%s: ring legend %r is not %s / %d leaders" % (width, k, line[:70], money(cap), n))
            ck(("%d of %d over %s" % (E["beat"], E["read"], word)) in tiles and word in s["head"], "%d/%s: ahead-of-the-S&P tile or headline: %s | %s" % (width, k, s["tiles"][0], s["head"][:80]))
            acc = E["tiers"]["STRONG ACCUMULATION"][1] + E["tiers"]["ACCUMULATION"][1]
            dist = E["tiers"]["STRONG DISTRIBUTION"][1] + E["tiers"]["DISTRIBUTION"][1]
            ck(("%d / %d" % (acc, dist)) in s["tiles"][2] and ("%d / %d" % (E["n_buying"], E["n_selling"])) in s["tiles"][4], "%d/%s: tape or smart-money tile: %s | %s" % (width, k, s["tiles"][2], s["tiles"][4]))
            ck(money(E["mf_net"], True) in s["tiles"][3] and money(E["fund_net"], True) in s["tiles"][5] and ("%d in" % E["n_in"]) in s["tiles"][5] and ("%d out" % E["n_out"]) in s["tiles"][5],
               "%d/%s: dollar-volume or funds tile: %s | %s (here %s, %s, %d in, %d out)" % (width, k, s["tiles"][3], s["tiles"][5], money(E["mf_net"], True), money(E["fund_net"], True), E["n_in"], E["n_out"]))
            cols = {c["k"]: c for c in s["cols"]}
            ck([c["k"] for c in s["cols"]] == LISTS, "%d/%s: columns %s" % (width, k, [c["k"] for c in s["cols"]]))
            for key in LISTS[1:]:
                if key == "printed" or key not in cols:
                    continue
                first = cols[key]["name"].split(" · ")[0]
                exp = E[key]
                ok = first in exp if isinstance(exp, set) else first == exp
                ck(ok, "%d/%s: %s starts with %r, the feed ranks %s first" % (width, k, key, first, sorted(exp) if isinstance(exp, set) else exp))
            note = " ".join(x for x in s["rests"] if "Set aside" in x)
            ck(bool(note) == bool(E["aside"]) and all(t in note for t in E["aside"]) and not any(cols[c]["name"] in E["aside"] for c in ("fund-inflows", "fund-outflows")), "%d/%s: set aside %s, the sheet says %r" % (width, k, E["aside"], note[:120]))
            if S.get("n") == E["n"] and k == "m":
                ck(E["beat"] == S.get("beating_m") and acc == S.get("accumulation") and dist == S.get("distribution") and E["n_buying"] == S.get("smart_buying") and E["n_selling"] == S.get("smart_selling") and abs(E["mf_net"] - (S.get("mf21_net_usd") or 0)) < 1,
                   "the engine's own summary differs from the rows: %s" % json.dumps({x: S.get(x) for x in ("beating_m", "accumulation", "distribution", "smart_buying", "smart_selling", "mf21_net_usd")}))

        with sync_playwright() as p:
            browser = launch(p, r.log)
            for width in (1440, 390):
                r.section("leaders.html at %d px" % width)
                ctx = browser.new_context(viewport={"width": width, "height": 900}, device_scale_factor=1 if width == 1440 else 2, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
                page = ctx.new_page()
                errors, reads = [], []
                page.on("pageerror", lambda e: errors.append("pageerror: %s" % e))
                page.on("request", lambda q: reads.append(q.url) if "/data/leaders.json" in q.url else None)
                page.goto("%s/leaders.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
                page.wait_for_function("() => (document.querySelector('[data-jts]') || { dataset: {} }).dataset.state === 'ready'", timeout=90000)
                page.wait_for_function("() => document.querySelectorAll('#lead tbody tr').length > 5", timeout=60000)
                page.wait_for_timeout(2500)
                s = page.evaluate(SHEET)
                r.log("kit %s | sheet at %s, followed by #%s | feed requests by the page %d | sideways overflow with / without the sheet %s | page errors %d" % (s["version"], s["rect"], s["next"], len(reads), s["ox"], len(errors)))
                r.log(s["eye"])
                r.log("**%s**" % s["head"])
                r.log(s["sub"])
                r.log("tiles: %s" % " || ".join(s["tiles"]))
                for c in s["cards"]:
                    r.log("card: %s" % c[:280])
                r.log("ring: centre %s %s | labels %s | total %s" % (s["center"], s["subs"], s["labels"], s["total"]))
                for line in s["legend"]:
                    r.log("  %s" % line)
                for c in s["cols"][1:]:
                    r.log("  [%s] %s | %d rows | %s" % (c["k"], c["head"], c["rows"], c["first"][:130]))
                for x in s["rests"]:
                    if "Set aside" in x or "went away" in x:
                        r.log("  note: %s" % x)
                judge(s, want["m"], "m", width)
                ck(len(s["tiles"]) == 6 and len(s["cards"]) == 2 and s["notes"] == 3 and len(s["cols"]) == 12, "%d: anatomy %d tiles, %d cards, %d notes, %d columns" % (width, len(s["tiles"]), len(s["cards"]), s["notes"], len(s["cols"])))
                ck(s["ctl"] == ["1 WEEK", "1 MONTH*", "1 QUARTER"], "%d: window buttons %s" % (width, s["ctl"]))
                ck(not errors, "%d: page errors %s" % (width, errors[:3]))
                ck(len(reads) <= 2, "%d: the feed was requested %d times (the desk alone asks twice)" % (width, len(reads)))
                ck(s["align"] == ["right"] and s["wide"] == 0 and s["ox"][0] == s["ox"][1], "%d: layout - alignment %s, %d elements wider than the sheet, overflow %s" % (width, s["align"], s["wide"], s["ox"]))
                ck(s["sec"] is None and s["badges"] == 0 and s["next"] == "verdict", "%d: the sheet is numbered (%s, %d badges) or is not right above the verdict (%s)" % (width, s["sec"], s["badges"], s["next"]))
                d = s["desk"]
                r.log("the desk below: %s | %d verdict tiles | rows printing / industries / leaders / funds %s | sections %s" % (d["verdict"][:90], d["kpis"], d["tables"], [x for x in d["secs"] if "/" not in x]))
                keys = ["verdict", "printing-now", "industries", "leaders", "industry-funds", "reading-rules", "sources"]
                top = [x.split(" ", 1) for x in d["secs"] if x.split(" ", 1)[1] in keys]
                ck(d["kpis"] >= 6 and all(n > 0 for n in d["tables"]) and [x[1] for x in top] == keys and [x[0] for x in top] == ["2", "3", "4", "5", "6", "7", "8"],
                   "%d: the desk below the sheet changed (its sections must still be 2..8): %s" % (width, d))
                ck(all(c["rows"] == (6 if width == 1440 else 3) for c in s["cols"] if c["k"] not in ("ring", "printed") and c["rows"]), "%d: rows per list %s" % (width, [(c["k"], c["rows"]) for c in s["cols"]]))
                shots["leaders_%d" % width] = page.screenshot(type="jpeg", quality=58, full_page=False) if width == 390 else page.locator("[data-jts]").screenshot(type="jpeg", quality=52)
                for k in ("w", "q", "m"):
                    page.evaluate("(k) => document.querySelector('[data-jts-ctl=\"' + k + '\"]').click()", k)
                    page.wait_for_timeout(200)
                    t = page.evaluate(SHEET)
                    cols = {c["k"]: c for c in t["cols"]}
                    r.log("window %s: %s | strongest %s | funds in %s | %s" % (k, t["head"][:78], cols["strongest"]["name"][:26], cols["fund-inflows"]["name"], t["tiles"][5][:70]))
                    judge(t, want[k], k, width)
                if width == 1440:
                    page.evaluate("() => document.querySelector('[data-jts-act=\"more\"]').click()")
                    page.wait_for_timeout(250)
                    t = page.evaluate(SHEET)
                    ck(all(c["rows"] == 15 for c in t["cols"] if c["k"] in ("strongest", "weakest", "accumulation", "distribution", "smart-buying", "fund-inflows")), "MORE ROWS: %s" % [(c["k"], c["rows"]) for c in t["cols"]])
                    r.log("MORE ROWS: %s" % [(c["k"], c["rows"]) for c in t["cols"][1:]])
                ctx.close()

            r.section("chart.html - the Leaders button")
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1, user_agent="Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome")
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append("pageerror: %s" % e))
            try:
                page.goto("%s/chart.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
                page.wait_for_selector("#btn-ldr", state="attached", timeout=60000)
                page.wait_for_timeout(2500)
                page.evaluate("() => document.getElementById('btn-ldr').click()")
                frame = None
                for _ in range(60):
                    frame = next((fr for fr in page.frames if "leaders.html" in fr.url), None)
                    if frame:
                        break
                    page.wait_for_timeout(500)
                if not frame:
                    ck(False, "the Leaders button did not open the desk in the chart's overlay")
                else:
                    frame.wait_for_function("() => (document.querySelector('[data-jts]') || { dataset: {} }).dataset.state === 'ready'", timeout=90000)
                    page.wait_for_timeout(1500)
                    s = frame.evaluate(SHEET)
                    head = page.evaluate("() => { const h = document.querySelector('#ws-overlay .wshd'); return h ? h.innerText.replace(/\\s+/g, ' ').trim() : null }")
                    r.log("overlay \"%s\": sheet %s at %s | %s | ring %s" % (head, s["state"], s["rect"], s["head"][:90], s["center"]))
                    judge(s, want["m"], "m", 1440)
                    ck(s["rect"][1] < 900 and s["next"] == "verdict", "in the overlay the sheet is not at the top of the desk: %s" % s["rect"])
                    shots["chart_overlay_leaders"] = page.screenshot(type="jpeg", quality=55)
                ck(not errors, "chart.html page errors: %s" % errors[:3])
            except Exception as e:  # noqa: BLE001
                ck(False, "chart.html: %s" % str(e)[:200])
            ctx.close()
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            r.log("%s: %d bytes" % (k, len(img)))
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(as_of=str(doc.get("as_of")), ranked=str(E["n"]), market_cap=money(E["cap"]), set_aside=",".join(E["aside"]) or "none", failures=str(len(fails)))
        if fails:
            for f in fails:
                r.fail(f)
            raise SystemExit(1)
        r.ok("LIVE: the Leaders tearsheet is the first block of the desk and in the chart's overlay; ring %s across %d leaders = the feed; every tile and the first row of every list agree with this op's own ranking on all three windows; 0 failures" % (money(E["cap"]), E["n"]))


if __name__ == "__main__":
    main()
    sys.exit(0)
