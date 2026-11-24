"""ops 6938 - the Risk Map heat map and its button on the chart: the warehouse's own bars, then a real browser on the live site.

Khalid 2026-11-24: "improve this engine so that it covers all asset classes ... then add a tab for it on chart.html make sure you
double check your work to make sure whats booming is actually booming and whats destroyed is actually destroyed and make it in a
big heatmap visual like tradingview heatmap".

Read-only: no engine is invoked and nothing is written. Four parts, each with arithmetic written here, not the engine's:

  1. The published document against itself (ops 6937's checks): every change is the close over its anchor close, every state is
     the rule's, both lists hold only what is in that state.
  2. The published document against the warehouse's raw bars, read here file by file: every anchor is a real bar of that date
     with that close, it is the last bar on or before the calendar day the window starts, and the last close is the warehouse's
     (a fund's forming bar: inside that session's range). So "up 12% in a month" is two closes anyone can look up.
  3. The page in a real browser on justhodl.ai: one tile per instrument, none over another; each tile's number is the
     document's and its colour is its state's (booming tiles are the brightest greens, destroyed the brightest reds); the lists
     are the document's; the 1D view shows the document's change, and with live prices on, the session price the page was
     given over the previous close; the previous view is one click away; a phone.
  4. chart.html: the Risk Map button stands between Heat and Auction, opens the map inside the chart, and a tile opens that
     instrument's chart.

Safe to run again.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import base64
import gzip
import importlib.util
import json
import math
import re
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "aws" / "lambdas" / "justhodl-regime-map" / "source"
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(SRC)]
BASE = "https://justhodl.ai"
LIVE = "justhodl-dashboard-live"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
MINUS = "−"
PLAIN = {"BOOMING": "Booming", "RISK-ON": "Rising", "NEUTRAL": "Flat", "RISK-OFF": "Falling", "DESTROYED": "Destroyed"}
WINDOWS = ("d", "w", "m", "q", "h", "ytd", "y")

TILES = r"""() => { const map = document.getElementById('rm-map'), mb = map.getBoundingClientRect();
  const out = Array.from(map.querySelectorAll('.rm-t')).map(e => { const b = e.getBoundingClientRect(), s = e.querySelector('span'), n = e.querySelector('b');
    return { t: e.dataset.t, x: b.left - mb.left, y: b.top - mb.top, w: b.width, h: b.height, bg: getComputedStyle(e).backgroundColor, name: n ? n.textContent : null, val: s ? s.textContent : null,
      haven: e.classList.contains('rm-haven'), late: !!e.querySelector('u') }; });
  let over = 0, out_of = 0;
  for (let i = 0; i < out.length; i++) { const a = out[i]; if (a.x < -0.6 || a.y < -0.6 || a.x + a.w > mb.width + 0.6 || a.y + a.h > mb.height + 0.6) out_of++;
    for (let j = i + 1; j < out.length; j++) { const b = out[j]; if (Math.min(a.x + a.w, b.x + b.w) - Math.max(a.x, b.x) > 0.6 && Math.min(a.y + a.h, b.y + b.h) - Math.max(a.y, b.y) > 0.6) over++; } }
  return { tiles: out, over: over, out_of: out_of, map: [Math.round(mb.width), Math.round(mb.height)], vw: innerWidth, ox: document.documentElement.scrollWidth - document.documentElement.clientWidth,
    classes: Array.from(map.querySelectorAll('.rm-ch b')).map(e => e.textContent), groups: map.querySelectorAll('.rm-gh').length,
    bad: (document.getElementById('rm-app').innerText.match(/\bNaN\b|\bundefined\b|\bnull\b|Infinity/g) || []) }; }"""
ROWS = r"""(sel) => Array.from(document.querySelectorAll(sel)).map(tr => Array.from(tr.children).map(td => td.innerText.replace(/\s+/g, ' ').trim()))"""
CHART = r"""() => { const b = (id) => document.getElementById(id), btn = b('btn-rmap'), ov = b('ws-overlay'), f = ov ? ov.querySelector('iframe') : null;
  return { btn: !!btn, prev: btn && btn.previousElementSibling ? btn.previousElementSibling.id : null, next: btn && btn.nextElementSibling ? btn.nextElementSibling.id : null,
    label: btn ? btn.querySelector('.l').textContent : null, title: btn ? btn.title : null, overlay: ov ? ov.className : null, src: f ? f.getAttribute('src') : null,
    head: ov && ov.querySelector('.wshd span') ? ov.querySelector('.wshd span').textContent : null,
    desks: Array.from(document.querySelectorAll('#tfbar .wsdesk')).slice(0, 6).map(x => x.id), on: (document.querySelector('#tabs .tab.on') || {dataset: {}}).dataset.id || null }; }"""


def sgn(v):
    return "+" if v > 0 else MINUS if v < 0 else ""


def fixed(a, d):
    """What JavaScript's toFixed prints: the exact binary value, halves up."""
    return str(Decimal(a).quantize(Decimal(1).scaleb(-d), rounding=ROUND_HALF_UP))


def fmt_pct(v, digits=None):
    if v is None:
        return "–"
    a = abs(v)
    d = digits if digits is not None else 0 if a >= 100 else 1 if a >= 10 else 2
    if float(fixed(a, d)) == 0:
        return fixed(0.0, d) + "%"
    return sgn(v) + fixed(a, d) + "%"


def fmt_score(v):
    n = int(math.floor(v + 0.5))                      # JavaScript's Math.round
    return sgn(n) + str(abs(n))


def state_label(t):
    return ("▲ " if t["state"] == "BOOMING" else "▼ " if t["state"] == "DESTROYED" else "") + fmt_score(t["score"])


def rgb(s):
    m = re.match(r"rgba?\((\d+),\s*(\d+),\s*(\d+)", s or "")
    return tuple(int(x) for x in m.groups()) if m else None


def months_back(d, n):
    y, m = d.year, d.month - n
    while m <= 0:
        y, m = y - 1, m + 12
    for day in range(d.day, 0, -1):
        try:
            return date(y, m, day)
        except ValueError:
            continue


def window_start(last_iso, window):
    last = date.fromisoformat(last_iso)
    if window == "w":
        return (last - timedelta(days=7)).isoformat()
    if window == "ytd":
        return date(last.year - 1, 12, 31).isoformat()
    return months_back(last, {"m": 1, "q": 3, "h": 6, "y": 12}[window]).isoformat()


def against_bars(t, closes, session=None):
    """One tile against the warehouse's raw bars of its instrument. closes: {date: close} in the tile's own terms (already
    rebased for its recorded splits, already the currency against the dollar). session: (date, low, high) of the forming bar.
    Returns the list of what does not agree, and how many anchors were looked up. Pure."""
    bad, n = [], 0
    days = sorted(closes)
    near = lambda a, b: abs(a - b) <= 2e-6 * max(abs(a), abs(b)) + 1e-12
    for k, (ad, ac, _) in t["a"].items():
        n += 1
        if ad not in closes:
            bad.append("%s %s: the warehouse has no bar on %s" % (t["t"], k, ad))
            continue
        if not near(closes[ad], ac):
            bad.append("%s %s: the document's close of %s is %s, the warehouse's %s" % (t["t"], k, ad, ac, closes[ad]))
        want = t["pd"] if k == "d" else window_start(t["d"], k)
        between = [x for x in days if ad < x <= want and x < t["d"]]
        if k != "d" and (ad > want or between):
            bad.append("%s %s: measured from %s, but the last bar on or before %s is %s" % (t["t"], k, ad, want, between[-1] if between else "earlier"))
        if k == "d" and (ad != t["pd"] or [x for x in days if ad < x < t["d"]]):
            bad.append("%s: the bar before %s is not %s" % (t["t"], t["d"], ad))
    if t.get("f"):
        lo, hi = (session[1], session[2]) if session and session[0] == t["d"] else (None, None)
        if lo is None:
            bad.append("%s: its last bar (%s) was still forming and the warehouse holds no range for that session" % (t["t"], t["d"]))
        elif not (lo * (1 - 1e-6) <= t["c"] <= hi * (1 + 1e-6)):
            bad.append("%s: the last price %s is outside that session's range %s .. %s" % (t["t"], t["c"], lo, hi))
    elif t["d"] not in closes or not near(closes[t["d"]], t["c"]):
        bad.append("%s: the last close of %s is %s, the warehouse's %s" % (t["t"], t["d"], t["c"], closes.get(t["d"])))
    return bad, n


def raw_closes(t, gz, today, L):
    """The warehouse's own bars of one instrument, in the tile's terms: ({date: close}, (session, low, high) or None). gz reads a key."""
    if t["k"] == "fund":
        rows = gz(L.bank_key(t["sym"]))["rows"]
        day = lambda x: datetime.fromtimestamp(x[0] / 1000.0, tz=timezone.utc).date().isoformat()
        out = {day(x): float(x[4]) for x in rows}
        rng = {day(x): (float(x[3]), float(x[2])) for x in rows}
        for sp in t.get("splits") or []:                      # the bank keeps the bars before a split it has not re-pulled as they traded
            out = {d: (c * sp["factor"] if d < sp["date"] else c) for d, c in out.items()}
            rng = {d: ((lo * sp["factor"], hi * sp["factor"]) if d < sp["date"] else (lo, hi)) for d, (lo, hi) in rng.items()}
        row = (today.get("rows") or {}).get(t["sym"])
        sess = (today["session"], float(row[2]), float(row[1])) if row and today.get("session") else None
        if t.get("f") and t["d"] in rng:                          # the session has since closed: its final bar's range
            sess = (t["d"], rng[t["d"]][0], rng[t["d"]][1])
        return out, sess
    if t["k"] == "coin":
        return {x[0]: float(x[4]) for x in gz(L.COIN_ROOT + t["t"] + ".json.gz")["rows"]}, None
    rows = gz(L.fmp_key(t["sym"]))
    if isinstance(rows, dict):
        rows = rows.get("rows") or rows.get("historical") or rows.get("data") or []
    out = {str(x["date"])[:10]: float(x["close"]) for x in rows if x.get("close")}
    return ({d: 1.0 / c for d, c in out.items()} if t.get("inv") else out), None


def selftest():
    assert fmt_pct(1.234) == "+1.23%" and fmt_pct(-12.25) == MINUS + "12.3%" and fmt_pct(-123.4) == MINUS + "123%" and fmt_pct(0.0) == "0.00%" and fmt_pct(-0.004) == "0.00%" and fmt_pct(4.7, 1) == "+4.7%"
    assert fmt_score(-21) == MINUS + "21" and fmt_score(0) == "0" and fmt_score(-0.4) == "0" and fmt_score(36.5) == "+37" and fmt_score(-57.5) == MINUS + "57" and state_label({"state": "BOOMING", "score": 64}) == "▲ +64" and state_label({"state": "RISK-OFF", "score": -21}) == MINUS + "21"
    assert rgb("rgb(8, 153, 129)") == (8, 153, 129) and rgb("rgba(54, 58, 69, 1)") == (54, 58, 69) and rgb("") is None
    assert window_start("2026-11-24", "q") == "2026-08-24" and window_start("2026-03-31", "m") == "2026-02-28" and window_start("2026-01-05", "ytd") == "2025-12-31"
    closes = {"2026-10-23": 100.0, "2026-10-26": 101.0, "2026-11-20": 109.0, "2026-11-23": 110.0}
    t = {"t": "X", "d": "2026-11-23", "c": 110.0, "pd": "2026-11-20", "p": 109.0, "a": {"d": ["2026-11-20", 109.0, 1], "m": ["2026-10-23", 100.0, 21]}}
    assert against_bars(t, closes) == ([], 2)
    assert any("warehouse's 101.0" in b for b in against_bars(dict(t, a={"d": ["2026-11-20", 109.0, 1], "m": ["2026-10-26", 100.0, 20]}), closes)[0])
    assert any("the last bar on or before 2026-10-23" in b for b in against_bars(dict(t, a={"m": ["2026-10-22", 99.0, 22]}), dict(closes, **{"2026-10-22": 99.0}))[0])
    assert any("the last close" in b for b in against_bars(dict(t, c=111.0), closes)[0])
    f = dict(t, f=True, d="2026-11-24", pd="2026-11-23", p=110.0, c=111.0, a={"d": ["2026-11-23", 110.0, 1], "m": ["2026-10-23", 100.0, 22]})
    assert against_bars(f, closes, ("2026-11-24", 109.5, 111.5)) == ([], 2)
    assert any("outside that session's range" in b for b in against_bars(f, closes, ("2026-11-24", 109.5, 110.5))[0])
    assert any("no range for that session" in b for b in against_bars(f, closes, ("2026-11-21", 1, 2))[0])
    assert any("the bar before" in b for b in against_bars(dict(t, pd="2026-10-26", a={"d": ["2026-10-26", 101.0, 1]}), closes)[0])


def fetch(path, timeout=90):
    req = urllib.request.Request(BASE + path, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def main():
    selftest()
    import boto3
    from ops_report import report
    import lambda_function as L

    def load(pat):
        spec = importlib.util.spec_from_file_location(pat, str(sorted(Path(__file__).resolve().parent.glob(pat + "_*.py"))[0]))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    A, first = load("ops_6846"), load("ops_6937")
    first.selftest()
    s3 = boto3.client("s3", region_name="us-east-1")
    now, stamp = datetime.now(timezone.utc), int(time.time())
    shots, fails = {}, []

    with report("ops_6938_risk_map_live_proof") as r:
        r.heading("ops 6938 - the Risk Map heat map and its chart button, the warehouse's bars and a real browser on the live site (read-only), %s" % now.isoformat(timespec="minutes"))

        def ck(cond, msg):
            if cond:
                return True
            fails.append(msg)
            r.warn("CHECK FAILED: " + msg)
            return False

        def gz(key):
            return json.loads(gzip.decompress(s3.get_object(Bucket=LIVE, Key=key)["Body"].read()))

        # ------------------------------------------------------------------ 0. served
        r.section("0. The live site serves the new files")
        served = {}
        for attempt in range(60):
            try:
                served = {"page": b'id="rm-v-classic"' in fetch("/regime-map.html?probe=%d" % (stamp + attempt)),
                          "script": b"JHRiskMap" in fetch("/jh-risk-map.js?v=20261124a&probe=%d" % (stamp + attempt)),
                          "engine": b"id=btn-rmap" in fetch("/jh-chart-engine.js?probe=%d" % (stamp + attempt)),
                          "rail": b'riskmap: ["Risk Map' in fetch("/jh-chart-tvrail.js?probe=%d" % (stamp + attempt)),
                          "chart": fetch("/chart.html?probe=%d" % (stamp + attempt)).count(b"&rmap=20261124") == 2}
            except Exception as e:  # noqa: BLE001
                served = {"error": str(e)[:80]}
            if served and all(v is True for v in served.values()):
                break
            time.sleep(20)
        r.log("served after %d tries: %s" % (attempt + 1, json.dumps(served)))
        if not (served and all(v is True for v in served.values())):
            r.fail("the live site does not serve the Risk Map heat map yet; nothing was judged")
            sys.exit(1)

        # ------------------------------------------------------------------ 1. the document against itself
        r.section("1. The published document against itself")
        doc = json.loads(fetch("/data/regime-map.json?probe=%d" % stamp))
        held = json.loads(s3.get_object(Bucket=LIVE, Key=L.OUT_KEY)["Body"].read())
        m = doc["map"]
        tiles = m["tiles"]
        by_t = {t["t"]: t for t in tiles}
        r.log("document %s %s, generated %s, turns measured from %s | %d of %d instruments %s | dates %s" % (
            doc.get("contract"), doc.get("version"), doc.get("generated_at"), doc.get("turns_since"), m["n"], m["n_universe"], m["counts"], doc["as_of"]))
        ck(held.get("generated_at") == doc.get("generated_at"), "the site serves a document other than the warehouse's: %s vs %s" % (doc.get("generated_at"), held.get("generated_at")))
        found = first.problems(doc)
        r.log("ops 6937's checks on it: %d problems %s" % (len(found), found[:5]))
        fails.extend(found)
        states = {s: sum(1 for t in tiles if t["state"] == s) for s in PLAIN}
        r.log("states: %s | regime %s | cross-asset %s" % (states, doc["regime"]["label"], doc["cross_asset"]["label"]))
        r.log("booming (%d): %s" % (len(m["booming"]), ", ".join("%s %+d (1M %+.1f%%, 3M %+.1f%%, vs 50d %+.1f%%, vs 200d %+.1f%%)" % (
            k, by_t[k]["score"], by_t[k]["r"]["m"], by_t[k]["r"]["q"], by_t[k]["v50"], by_t[k]["v200"]) for k in m["booming"])))
        r.log("destroyed (%d), the first twelve: %s" % (len(m["destroyed"]), ", ".join("%s %+d (1M %+.1f%%, 3M %+.1f%%)" % (k, by_t[k]["score"], by_t[k]["r"]["m"], by_t[k]["r"]["q"]) for k in m["destroyed"][:12])))
        r.log("scored that far, not called it: %s" % ([(t["t"], t["held"]) for t in tiles if t.get("held")] or "none"))
        ck(m["n"] >= 0.9 * m["n_universe"] and len([c for c in m["classes"] if any(t["cls"] == c["id"] for t in tiles)]) == 9 and min(m["counts"].values()) >= 3, "not every asset class or kind is on the map: %s" % m["counts"])
        age = (now - datetime.fromisoformat(doc["generated_at"])).total_seconds() / 3600.0
        ck(age < 30, "the document is %.1f hours old" % age)

        # ------------------------------------------------------------------ 2. the document against the warehouse's raw bars
        r.section("2. Every anchor and every last close against the warehouse's raw bars, read here")
        today = gz(L.TODAY_KEY)
        r.log("forming bars: session %s as of %s, %d tickers" % (today.get("session"), today.get("as_of"), len(today.get("rows") or {})))

        def one(t):
            try:
                closes, sess = raw_closes(t, gz, today, L)
                return (t["t"],) + against_bars(t, closes, sess)
            except Exception as e:  # noqa: BLE001
                return t["t"], ["%s: its warehouse file could not be read (%s: %s)" % (t["t"], type(e).__name__, str(e)[:80])], 0
        with ThreadPoolExecutor(16) as pool:
            res = list(pool.map(one, tiles))
        looked = sum(x[2] for x in res)
        wrong = [b for x in res for b in x[1]]
        r.log("%d anchors on %d instruments looked up in the warehouse's own files: %d do not agree %s" % (looked, len(res), len(wrong), wrong[:8]))
        by_kind = {k: sum(1 for t in tiles if t["k"] == k and not [b for x in res if x[0] == t["t"] for b in x[1]]) for k in m["counts"]}
        r.log("instruments whose every anchor and last close is the warehouse's: %s of %s" % (by_kind, m["counts"]))
        r.log("funds on a forming bar: %d (inside that session's range); rebased for a recorded split: %s" % (sum(1 for t in tiles if t.get("f")), [t["t"] for t in tiles if t.get("splits")]))
        fails.extend(wrong)
        ck(looked >= 6 * 0.95 * len(tiles), "too few anchors were looked up: %d" % looked)
        for k in m["booming"] + m["destroyed"][:8]:
            t = by_t[k]
            r.log("  %-6s %-9s close %s on %s | a month ago %s on %s (%+.2f%%) | three months ago %s on %s (%+.2f%%)" % (
                k, t["state"], t["c"], t["d"], t["a"]["m"][1], t["a"]["m"][0], t["r"]["m"], t["a"]["q"][1], t["a"]["q"][0], t["r"]["q"]))

        # ------------------------------------------------------------------ 3. the page
        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)

            r.section("3. The heat map in a real browser")
            ctx = browser.new_context(viewport={"width": 1600, "height": 900}, user_agent=UA)
            page = ctx.new_page()
            errors, quotes = [], {}
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))

            def on_response(resp):
                if "/quotes?tickers=" in resp.url and resp.status == 200:
                    try:
                        quotes.update((resp.json() or {}).get("tickers") or {})
                    except Exception:  # noqa: BLE001
                        pass
            page.on("response", on_response)
            page.goto("%s/regime-map.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#rm-app[data-ready='1']", timeout=90000)
            page.wait_for_function("() => window.__riskMap && window.__riskMapDoc", timeout=30000)
            pdoc = page.evaluate("() => ({g: window.__riskMapDoc.generated_at, n: window.__riskMapDoc.map.n, audit: window.__riskMap.audit})")
            ck(pdoc["g"] == doc["generated_at"] and pdoc["n"] == m["n"], "the page drew another document: %s" % pdoc["g"])
            r.log("the page's own check: %d changes on %d instruments, %d do not agree" % (pdoc["audit"]["changes"], pdoc["audit"]["tiles"], len(pdoc["audit"]["bad"])))
            ck(pdoc["audit"]["bad"] == [] and pdoc["audit"]["changes"] == sum(len(t["a"]) for t in tiles), "the page's own check: %s" % pdoc["audit"]["bad"][:4])
            ck("all agree" in page.inner_text("#rm-check"), "the page does not say its check passed: %s" % page.inner_text("#rm-check")[:200])
            page.evaluate("() => { const b = document.getElementById('rm-livebox'); if (b.checked) b.click(); }")          # the document's own figures first
            page.wait_for_timeout(500)
            g = page.evaluate(TILES)
            drawn = {x["t"]: x for x in g["tiles"]}
            r.log("map %s px in a %d px window: %d tiles, %d over another, %d outside the map, %d class headings %s, %d group headings, %d with their name on, %d with a number" % (
                g["map"], g["vw"], len(g["tiles"]), g["over"], g["out_of"], len(g["classes"]), g["classes"], g["groups"], sum(1 for x in g["tiles"] if x["name"]), sum(1 for x in g["tiles"] if x["val"])))
            ck(sorted(drawn) == sorted(by_t) and len(g["tiles"]) == m["n"], "the tiles are not the document's instruments, one each")
            ck(g["over"] == 0 and g["out_of"] == 0 and g["ox"] <= 1 and g["bad"] == [], "tiles overlap, leave the map, the page scrolls sideways or prints a non-number: %s" % {k: g[k] for k in ("over", "out_of", "ox", "bad")})
            ck(g["map"][0] >= 0.95 * g["vw"] and g["map"][1] >= 420 and len(g["classes"]) == 9 and sum(1 for x in g["tiles"] if x["name"]) >= 0.8 * m["n"], "the map is not a big one: %s" % g["map"])
            off = [x["t"] for x in g["tiles"] if (x["name"] is not None and x["name"] != x["t"]) or (x["val"] is not None and x["val"] != state_label(by_t[x["t"]]))]
            ck(not off, "tiles whose name or number is not the document's: %s" % off[:8])
            # colour: green for rising and booming, red for falling and destroyed, grey for flat; booming and destroyed are the brightest
            shade = {s: [] for s in PLAIN}
            wrong_colour = []
            for x in g["tiles"]:
                c, st = rgb(x["bg"]), by_t[x["t"]]["state"]
                tilt = c[1] - c[0]
                shade[st].append(tilt)
                if (st in ("BOOMING", "RISK-ON") and tilt < 12) or (st in ("DESTROYED", "RISK-OFF") and tilt > -12) or (st == "NEUTRAL" and abs(tilt) > 10):
                    wrong_colour.append((x["t"], st, x["bg"]))
            r.log("colour (green minus red) by state: %s" % {PLAIN[s]: (min(v), max(v)) for s, v in shade.items() if v})
            ck(not wrong_colour, "tiles drawn in the wrong colour for their state: %s" % wrong_colour[:6])
            ck((not shade["BOOMING"] or not shade["RISK-ON"] or min(shade["BOOMING"]) > max(shade["RISK-ON"])) and (not shade["DESTROYED"] or not shade["RISK-OFF"] or max(shade["DESTROYED"]) < min(shade["RISK-OFF"])),
               "a rising tile is as bright as a booming one, or a falling one as a destroyed one")
            ck(sorted(x["t"] for x in g["tiles"] if x["haven"]) == sorted(t["t"] for t in tiles if t["role"] == "haven") and sorted(x["t"] for x in g["tiles"] if x["late"]) == sorted(t["t"] for t in tiles if t.get("late")),
               "the safe-haven edge or the late mark is on the wrong tiles")
            shots["desk-state"] = page.screenshot(type="jpeg", quality=74)

            # the lists
            page.evaluate("() => document.querySelectorAll('.rm-more').forEach(b => b.click())")
            boom, bust = page.evaluate(ROWS, ".rm-l-up tbody tr"), page.evaluate(ROWS, ".rm-l-dn tbody tr")
            want_boom = sorted((t for t in tiles if t["state"] == "BOOMING"), key=lambda t: -t["score"])
            want_bust = sorted((t for t in tiles if t["state"] == "DESTROYED"), key=lambda t: t["score"])
            row_ok = lambda row, t: row[0] == t["t"] and row[3] == fmt_score(t["score"]) and row[4] == fmt_pct(t["r"]["m"], 1) and row[5] == fmt_pct(t["r"]["q"], 1)
            r.log("lists on the page: booming %s | getting destroyed %d rows, first %s" % ([x[0] for x in boom], len(bust), [x[0] for x in bust[:6]]))
            ck(sorted(x[0] for x in boom) == sorted(m["booming"]) and [by_t[x[0]]["score"] for x in boom] == [t["score"] for t in want_boom] and all(row_ok(x, by_t[x[0]]) for x in boom), "the booming list is not the document's")
            ck(sorted(x[0] for x in bust) == sorted(m["destroyed"]) and [by_t[x[0]]["score"] for x in bust] == [t["score"] for t in want_bust] and all(row_ok(x, by_t[x[0]]) for x in bust), "the destroyed list is not the document's")
            heldp = page.evaluate("() => Array.from(document.querySelectorAll('.rm-held li')).map(li => li.dataset.t)")
            ck(sorted(heldp) == sorted(t["t"] for t in tiles if t.get("held")), "the held-back list: %s" % heldp)
            cls = page.evaluate(ROWS, ".rm-classes tbody tr")
            ck(len(cls) == 9 and all(int(row[1]) == m["stats"]["classes"][c["id"]]["n"] and row[2] == fmt_score(m["stats"]["classes"][c["id"]]["score"]) for row, c in zip(cls, m["classes"])), "the class table: %s" % cls[:2])

            # one day, the document's own figure
            page.click(".rm-seg [data-mode='d']")
            page.wait_for_timeout(400)
            g1 = page.evaluate(TILES)
            off1 = [(x["t"], x["val"], fmt_pct(by_t[x["t"]]["r"].get("d"))) for x in g1["tiles"] if x["val"] is not None and x["val"] != fmt_pct(by_t[x["t"]]["r"].get("d"))]
            day = lambda k: by_t[k]["r"].get("d") or 0.0
            sign1 = [x["t"] for x in g1["tiles"] if (day(x["t"]) > 0.2 and rgb(x["bg"])[1] <= rgb(x["bg"])[0]) or (day(x["t"]) < -0.2 and rgb(x["bg"])[0] <= rgb(x["bg"])[1])]
            r.log("1D view, live prices off: %d tiles carry a number, %d differ from the document, %d coloured against their sign" % (sum(1 for x in g1["tiles"] if x["val"]), len(off1), len(sign1)))
            ck(not off1 and not sign1, "1D view: %s %s" % (off1[:5], sign1[:5]))
            # live prices on: the session price the page was handed, over the same closes
            page.evaluate("() => { const b = document.getElementById('rm-livebox'); if (!b.checked) b.click(); }")
            page.wait_for_function("() => document.getElementById('rm-app').dataset.live !== undefined", timeout=30000)
            page.wait_for_timeout(2500)
            lv = page.evaluate("() => { const o = {}; window.__riskMapDoc.map.tiles.forEach(t => { const l = window.__riskMap.liveOf(t.t); if (l) o[t.t] = l; }); return o; }")
            g2 = {x["t"]: x for x in page.evaluate(TILES)["tiles"]}
            badlive, matched = [], 0
            for k, l in lv.items():
                q, t = quotes.get(k), by_t[k]
                if t["k"] != "fund" or not q:
                    badlive.append(k + " (no quote)")
                elif q.get("updatedTs") != l["at"]:
                    continue                                  # the page has refreshed since: a later quote than the one captured here
                elif abs(l["c"] - q["dayClose"]) > 1e-9 or abs(l["r"]["d"] - round((q["dayClose"] / q["prevClose"] - 1) * 100, 2)) > 0.011 or abs(l["r"]["m"] - round((q["dayClose"] / t["a"]["m"][1] - 1) * 100, 2)) > 0.011:
                    badlive.append(k)
                else:
                    matched += 1
                if g2[k]["val"] is not None and g2[k]["val"] != fmt_pct(l["r"]["d"]):
                    badlive.append(k + " (tile)")
            sample = [(k, by_t[k]["c"], lv[k]["c"], by_t[k]["r"]["d"], lv[k]["r"]["d"]) for k in ("SPY", "QQQ", "IWM", "TLT", "GLD") if k in lv]
            r.log("live prices: %d of %d funds (the Worker answered for %d, %d held to the quote captured here); ticker, engine close, session price, engine 1D, live 1D: %s | badge: %s" % (
                len(lv), m["counts"]["fund"], len(quotes), matched, sample, page.inner_text("#rm-live-badge")[:120]))
            ck(not badlive and (not lv or matched >= 0.5 * len(lv)), "live figures that are not the quote over the close: %s (%d of %d held)" % (badlive[:8], matched, len(lv)))
            if not lv:
                r.warn("no fund had a usable live quote at this hour: the live arithmetic was not exercised on the site (it is in the page's tests)")
            notlive = [k for k, x in g2.items() if k not in lv and x["val"] is not None and x["val"] != fmt_pct(by_t[k]["r"].get("d"))]
            ck(not notlive, "a tile without a live quote does not show the document's figure: %s" % notlive[:6])
            shots["desk-1d"] = page.screenshot(type="jpeg", quality=74)
            page.click(".rm-seg [data-mode='state']")
            page.wait_for_timeout(300)
            back = [x["t"] for x in page.evaluate(TILES)["tiles"] if x["val"] is not None and x["val"] != state_label(by_t[x["t"]])]
            ck(not back, "back on Boom / Destroyed a live price moved a state's number: %s" % back[:5])

            # a class, a card
            page.click(".rm-chips [data-cls='crypto']")
            page.wait_for_timeout(400)
            gc = page.evaluate(TILES)
            ck(sorted(x["t"] for x in gc["tiles"]) == sorted(t["t"] for t in tiles if t["cls"] == "crypto") and gc["over"] == 0 and gc["classes"] == ["Crypto"], "the Crypto view: %d tiles" % len(gc["tiles"]))
            page.click(".rm-chips [data-cls='']")
            page.wait_for_timeout(300)
            pick = by_t[(m["booming"] or m["strongest"])[0]]
            page.click(".rm-t[data-t='%s']" % pick["t"])
            card = page.inner_text("#rm-card")
            r.log("card of %s: %s" % (pick["t"], card.replace("\n", " | ")[:520]))
            ck(pick["t"] in card and PLAIN[pick["state"]] in card and all(pick["a"][k][0] in card for k in pick["a"]) and fmt_pct(pick["r"]["m"]) in card and fmt_pct(pick["r"]["q"]) in card and ("score " + fmt_score(pick["score"])) in card,
               "the card of %s does not carry its proof" % pick["t"])
            page.keyboard.press("Escape")

            # the second palette, then the previous view
            page.click(".rm-seg [data-pal='cb']")
            page.wait_for_timeout(300)
            cb = {x["t"]: rgb(x["bg"]) for x in page.evaluate(TILES)["tiles"]}
            ck(all(cb[t["t"]][2] > cb[t["t"]][0] for t in tiles if t["state"] in ("BOOMING", "RISK-ON")) and all(cb[t["t"]][0] > cb[t["t"]][2] for t in tiles if t["state"] in ("DESTROYED", "RISK-OFF")), "the orange and blue palette")
            page.click(".rm-seg [data-pal='rg']")
            page.click("#rm-v-classic")
            page.wait_for_selector("#wrap .cell", timeout=15000)
            old = page.evaluate("""() => ({ cells: document.querySelectorAll('#wrap .cell').length, sleeves: Array.from(document.querySelectorAll('#wrap .sleeve .st')).map(e => e.textContent), app: document.getElementById('rm-app').hidden,
              label: (document.querySelector('#wrap .banner .lab') || {}).textContent, boom: Array.from(document.querySelectorAll('#wrap .col.b .row .tk')).map(e => e.textContent),
              bust: Array.from(document.querySelectorAll('#wrap .col.d .row .tk')).map(e => e.textContent), url: location.search })""")
            r.log("previous view: %s" % json.dumps(old))
            ck(old["cells"] == len(doc["ranked"]) == 38 and old["sleeves"] == list(doc["sleeves"]) and old["app"] is True and old["label"] == doc["regime"]["label"] and old["boom"] == [x["ticker"] for x in doc["booming"]]
               and old["bust"] == [x["ticker"] for x in doc["destroyed"]] and "view=classic" in old["url"], "the previous view is not the first map, one click away")
            shots["previous-view"] = page.screenshot(type="jpeg", quality=70)
            page.click("#rm-v-map")
            page.wait_for_timeout(400)
            ck(page.evaluate("() => !document.getElementById('rm-app').hidden && document.getElementById('wrap').hidden && document.querySelectorAll('.rm-t').length") == m["n"], "back from the previous view")
            r.log("page errors: %d %s" % (len(errors), errors[:3]))
            ck(not errors, "the page threw: %s" % errors[:3])
            ctx.close()

            # a phone
            ctx = browser.new_context(viewport={"width": 390, "height": 844}, user_agent=UA, device_scale_factor=2, is_mobile=True, has_touch=True)
            page = ctx.new_page()
            page.goto("%s/regime-map.html?nocache=%d" % (BASE, stamp + 1), wait_until="domcontentloaded")
            page.wait_for_selector("#rm-app[data-ready='1']", timeout=90000)
            page.wait_for_timeout(800)
            gp = page.evaluate(TILES)
            r.log("phone: map %s in a %d px window, %d tiles, %d over another, sideways scroll %d px, %d with their name on" % (gp["map"], gp["vw"], len(gp["tiles"]), gp["over"], gp["ox"], sum(1 for x in gp["tiles"] if x["name"])))
            ck(len(gp["tiles"]) == m["n"] and gp["over"] == 0 and gp["out_of"] == 0 and gp["ox"] <= 1 and sum(1 for x in gp["tiles"] if x["name"]) >= 0.75 * m["n"], "the phone view")
            page.evaluate("() => document.getElementById('rm-map').scrollIntoView()")
            page.wait_for_timeout(300)
            page.tap(".rm-t[data-t='SPY']")
            page.wait_for_timeout(300)
            box = page.evaluate("() => { const b = document.getElementById('rm-card').getBoundingClientRect(); return [Math.round(b.left), Math.round(b.top), Math.round(b.right), Math.round(b.bottom), innerWidth, innerHeight]; }")
            ck(box[0] >= 0 and box[2] <= box[4] + 1 and box[3] <= box[5] + 1 and box[1] >= 0 and "SPY" in page.inner_text("#rm-card"), "the card on a phone: %s" % box)
            shots["phone"] = page.screenshot(type="jpeg", quality=62)
            ctx.close()

            # ------------------------------------------------------------------ 4. the chart
            r.section("4. The Risk Map button on chart.html")
            ctx = browser.new_context(viewport={"width": 1600, "height": 900}, user_agent=UA)
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto("%s/chart.html?s=SPY&nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#btn-rmap", timeout=90000, state="attached")
            page.wait_for_function("() => (window.lastBars || []).length > 300 && typeof window.jhOpenWorkspace === 'function' && typeof window.jhOpenSymbol === 'function'", timeout=90000)
            page.wait_for_timeout(1200)
            c0 = page.evaluate(CHART)
            r.log("button: %s" % json.dumps(c0))
            ck(c0["btn"] and c0["prev"] == "btn-heat" and c0["next"] == "btn-auc" and c0["label"] == "Risk Map" and c0["desks"][:5] == ["btn-macro", "btn-flows", "btn-heat", "btn-rmap", "btn-auc"] and c0["overlay"] == "" and c0["on"] == "SPY",
               "the button: %s" % c0)
            page.mouse.move(700, 3)                     # the toolbar hides until the pointer reaches the top edge
            try:
                page.wait_for_selector("#jh-ah-top.show", timeout=8000)
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(500)
            page.evaluate("() => document.getElementById('btn-rmap').scrollIntoView({ inline: 'center', block: 'nearest' })")
            try:
                page.click("#btn-rmap", timeout=6000)
                how = "a real click"
            except Exception:  # noqa: BLE001
                page.evaluate("() => document.getElementById('btn-rmap').click()")
                how = "the button's own click (the toolbar was not on screen)"
            page.wait_for_selector("#ws-overlay.on iframe", timeout=15000)
            fr = page.frame_locator("#ws-overlay iframe")
            fr.locator("#rm-app[data-ready='1']").wait_for(timeout=90000)
            page.wait_for_timeout(1200)
            c1 = page.evaluate(CHART)
            n_in = fr.locator(".rm-t").count()
            r.log("opened by %s: %s | %d tiles inside" % (how, json.dumps({k: c1[k] for k in ("overlay", "src", "head")}), n_in))
            ck(c1["overlay"] == "on" and (c1["src"] or "").startswith("/regime-map.html") and "Risk Map" in (c1["head"] or "") and n_in == m["n"], "the map inside the chart: %s" % c1)
            shots["chart-risk-map"] = page.screenshot(type="jpeg", quality=74)
            goto = max((t for t in tiles if t["k"] == "fund" and t["t"] != "SPY" and t["state"] in ("DESTROYED", "RISK-OFF")), key=lambda t: t.get("dv") or 0)
            fr.locator(".rm-t[data-t='%s']" % goto["t"]).click()
            fr.locator(".rm-open").click()
            page.wait_for_function("(s) => { const t = document.querySelector('#tabs .tab.on'); return t && t.dataset.id === s && document.getElementById('ws-overlay').className === ''; }", arg=goto["chart"], timeout=30000)
            page.wait_for_function("() => (window.lastBars || []).length > 100", timeout=60000)
            page.wait_for_timeout(1500)
            c2 = page.evaluate(CHART)
            r.log("a tile's 'Open %s in the chart': overlay %r, the chart is on %s" % (goto["t"], c2["overlay"], c2["on"]))
            ck(c2["overlay"] == "" and c2["on"] == goto["chart"], "a tile did not open its chart: %s" % c2)
            shots["chart-after-open"] = page.screenshot(type="jpeg", quality=70)
            r.log("page errors: %d %s" % (len(errors), errors[:3]))
            ck(not errors, "chart.html threw: %s" % errors[:3])
            ctx.close()
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(tiles=m["n"], booming=len(m["booming"]), destroyed=len(m["destroyed"]), anchors_looked_up=looked, anchors_wrong=len(wrong), live_funds=len(lv), failed_checks=len(fails), document=doc["generated_at"][:16])
        for f in fails[:40]:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("LIVE: %d instruments in nine asset classes on one heat map; %d anchors and every last close are the warehouse's own bars; %d booming and %d destroyed, each by the rule and drawn as such; the chart's Risk Map button opens it" % (
            m["n"], looked, len(m["booming"]), len(m["destroyed"])))


if __name__ == "__main__":
    main()
