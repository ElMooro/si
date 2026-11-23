"""ops 6866 - READ-ONLY live proof of the rebuilt Strength desk (strong.html), in Chrome on the runner (re-runnable).

Khalid 2026-11-20: "Strength vs S&P 500 needs improvement like today everything is dumping hard howver i cant see which
stocks and etfs are showing strength and dumping less than sp500 and also i want ui/ux redesign just like we did on the
other. i like what it already have ... i want to enhance it further"

CLAUDE.md "CHECK YOUR WORK": the page is opened on justhodl.ai at 1440 and 390 px and read back against the Python
reference (aws/lambdas/justhodl-strength/source/wide.py) run over THE SAME BYTES the page fetched - the whole-market
table (data/strength-wide.json) and today's forming bars (data/warm/polygon-full/today.json.gz), taken off the page's
own responses, so a refresh between the page's read and this one cannot hide a difference:
  - the page's read of the session (groups, sectors, industries, ETF classes, rank tenths, dump-day record, boards)
    equals wide.live_read / wide.last_read, value for value;
  - the sentence, the six tiles, the bars, the carried-in panel and the group table print those figures;
  - "Every stock and ETF" lists exactly the names the reference lists for every chip pressed (kind, size, outcome,
    rank, set aside), in the reference's order for every header clicked, finds by ticker and name, restores its state
    from the address, shows all rows on demand and downloads a CSV of the same rows;
  - the tearsheet's ring is the scope's market value by outcome and its lists are the reference's, for each of the
    four scopes, at 6 rows, 15 after MORE ROWS, 3 on a phone;
  - every section the page had before is still there, drawn, under the number the registry gave it; the two new
    sections carry the numbers registered for them; nothing spills sideways; no page error.
Writes nothing.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import json
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-strength" / "source"))
import wide as W  # noqa: E402

BASE = "https://justhodl.ai"
DESK_KEY, WIDE_KEY, TODAY_KEY = "data/strength.json", "data/strength-wide.json", "data/warm/polygon-full/today.json.gz"
UA = "Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome"
READ_KEYS = ("regime", "n_counted", "n_ranked_1b", "mcap_1b", "rank_col", "record_cols", "groups", "by_sector", "by_class", "industries", "by_rank", "by_record", "boards")
VIEW_TODAY = ["t", "name", "where", "ind", "mcap", "px", "ret", "x", "out", "rank", "dump", "fo", "pos", "vwx", "rv", "ticks"]
VIEW_PHONE = ["t", "name", "ret", "x", "out", "mcap", "where", "ind", "rank", "dump", "px", "fo", "pos", "vwx", "rv", "ticks"]
KIT_MIN = (1, 6, 0)
VIEW_RECORD = ["t", "name", "ret", "x", "out", "rank", "dump", "ds", "dx", "rally", "rx", "dnx", "upx", "both"]
VIEW_TREND = ["t", "name", "ret", "x", "out", "rank", "x5", "x21", "x63", "x126", "p52", "ma", "atr", "mcap"]
PAGE = 200
MINUS = "−"

STATE = r"""() => { const T = (id) => document.getElementById(id), v = window.JHStrengthDoc, el = T('jh-tearsheet');
  const q = (root, s) => root ? Array.from(root.querySelectorAll(s)) : [], txt = (e) => e ? e.textContent.replace(/\s+/g, ' ').trim() : null;
  const chips = (id) => { const on = q(T(id), 'button.jh-chip').filter(b => b.getAttribute('aria-pressed') === 'true').map(b => b.getAttribute('data-v')); return on.length === 1 ? on[0] : on; };
  const tbl = T('names-tbl'), lists = {};
  q(el, '[data-jts-list]').forEach(c => { const k = c.getAttribute('data-jts-list'); if (k !== 'ring') lists[k] = { n: txt(c.querySelector('.jts-n')), title: txt(c.querySelector('.jts-ch b')),
    rows: q(c, 'tbody tr').map(tr => [tr.querySelector('.jts-nm').textContent.split(' · ')[0], tr.querySelector('.jts-big').textContent]) }; });
  const ring = el ? el.querySelector('[data-jts-list="ring"]') : null, count = (s) => document.querySelectorAll(s).length;
  return {
    view: v ? { contract: v.contract, mode: v.mode, session: v.session, rank_as_of: v.rank_as_of, table_as_of: v.table_as_of, bars_as_of: v.bars_as_of, table_generated: (v.table || {}).generated_at, version: (v.table || {}).version,
                n: v.rows.length, n_no_print: v.n_no_print, unconfirmed: v.unconfirmed.map(r => r.t), splits: v.splits_today.map(r => r.t), spy: v.spy.ret, read: v.read } : null,
    verdict: txt(T('verdict-line')), stamp: txt(T('stamp')),
    today: { h: txt(T('today-h')), meta: txt(T('today-meta')), kpis: q(T('today-kpis'), '.jh-kpi').map(k => [txt(k.querySelector('.l')), txt(k.querySelector('.v')), txt(k.querySelector('.s'))]),
             bars: txt(T('today-bars')), bars_h: txt(T('bars-h')), carried_h: txt(T('carried-h')), carried: q(T('carried'), '.sx-crow').map(c => [txt(c.querySelector('.sx-cl b')), txt(c.querySelector('.sx-cl span')), txt(c.lastElementChild.querySelector('b')), txt(c.lastElementChild.querySelector('span'))]), carried_mode: chips('carried-mode'),
             group: chips('today-group'), groups_head: q(T('today-groups'), 'th').map(th => th.getAttribute('data-sort')), groups: q(T('today-groups'), 'tbody tr').map(tr => q(tr, 'td').map(txt)) },
    names: { meta: txt(T('names-meta')), head: q(tbl, 'th').map(th => th.getAttribute('data-sort') || '#'), sorted: q(tbl, 'th[aria-sort]').map(th => [th.getAttribute('data-sort'), th.getAttribute('aria-sort')]),
             rows: q(tbl, 'tbody tr').map(tr => { const a = tr.querySelector('a.tk'); return a ? a.textContent.trim() : null; }), first: q(q(tbl, 'tbody tr')[0], 'td').map(txt), empty: txt(tbl.querySelector('td.jh-load')),
             more: T('names-more').hidden ? null : txt(T('names-more')), aside: txt(T('names-aside')), q: T('names-q').value, sector: T('names-sector').value,
             kind: chips('names-kind'), scope: chips('names-scope'), out: chips('names-out'), rank: chips('names-rank'), show: chips('names-show'), viewc: chips('names-view'),
             fit: tbl.scrollWidth - tbl.clientWidth },
    sheet: el ? { state: el.dataset.state, chrome: el.hasAttribute('data-jh-chrome'), sec: el.getAttribute('data-jh-sec'), h: Math.round(el.getBoundingClientRect().height), top: Math.round(el.getBoundingClientRect().top + scrollY),
             eye: txt(el.querySelector('.jts-eye')), title: txt(el.querySelector('.jts-h')), foot: txt(el.querySelector('.jts-foot')),
             tiles: q(el, '.jts-kpi').map(k => [txt(k.querySelector('.k')), txt(k.querySelector('.v')), txt(k.querySelector('.s'))]), cards: q(el, '.jts-card').map(c => [txt(c.querySelector('.big')), txt(c.querySelector('.t'))]),
             notes: q(el, '.jts-notes p').length, ctl: q(el, '[data-jts-ctl]').map(b => [b.getAttribute('data-jts-ctl'), b.getAttribute('aria-pressed')]), acts: q(el, '[data-jts-act]').map(b => b.getAttribute('data-jts-act')),
             total: ring ? Number(ring.getAttribute('data-total')) : null, center: txt(el.querySelector('.jts-rc')), arcs: q(el, '.jts-arc').map(a => [a.getAttribute('class'), Number(a.getAttribute('data-share'))]), lists: lists } : null,
    kit: (window.JHTearsheet || {}).version || null, hash: location.hash,
    secs: q(document, '[data-jh-sec]').map(e => [e.getAttribute('data-jh-sec'), e.getAttribute('data-jh-key') || '', e.id || '']),
    old: { verdict_kpis: count('#kpis .jh-kpi'), thesis_kpis: count('#thesis-kpis .jh-kpi'), dumps: count('#dumps tbody tr'), rallies: count('#rallies tbody tr'), classes: count('#classes tbody tr'), coil: count('#coil tbody tr'),
           leaders: count('#leaders tbody tr'), laggards: count('#laggards tbody tr'), focus: (T('focus') || {textContent: ''}).textContent.trim().length, desk: count('#tbl tbody tr a.tk'), chips: count('#chips .jh-chip'),
           defs: (T('defs') || {textContent: ''}).textContent.trim().length, sources: (T('sources') || {textContent: ''}).textContent.trim().length, lastnames: count('#lastnames tbody tr') },
    scripts: q(document, 'script[src]').map(s => s.getAttribute('src')).filter(s => /strong|tearsheet/.test(s)),
    bad: (document.querySelector('main') || document.body).innerText.match(/undefined|NaN|\[object Object\]/g) || [],
    ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""


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


# ------------------------------------------------------------------------------------------------- the reference
def reference(doc, today, labels):
    """What the page must show, from the same two documents: mode, the read, and every row with the words the page
    filters on. Mirrors jh-strong-engine.js model() through wide.py's own functions."""
    rules = doc["rules"]
    j = W.live_join(doc, today) if today else None
    if j:
        mode, rows, spy, rank_col, rec, session, carried = "live", j["rows"], j["spy"]["ret"], "rs", ("dn", "dh"), j["session"], j["as_of"]
        read = W.live_read(doc, today, class_labels=labels)
    else:
        mode, rank_col, rec, session, carried = "last", "rsp", ("dnp", "dhp"), str(doc["as_of"]), str(doc["prior_session"])
        rows = [dict(zip(doc["cols"], raw)) for raw in doc["rows"]]
        for r in rows:
            r["ret"] = (r["c"] / r["pc"] - 1.0) * 100.0 if r.get("pc") else None
            r["bar"], r["unconfirmed"] = True, False
        bm = doc["benchmark"]
        spy = (bm["c"] / bm["pc"] - 1.0) * 100.0
        read = W.last_read(doc, class_labels=labels)
    flat = rules["flat_pct"]
    for r in rows:
        r["counted"] = r.get("ret") is not None and not r.get("why")
        r["out"] = W.outcome(r["ret"], spy, flat) if r.get("ret") is not None else None
        r["x"] = r["ret"] - spy if r.get("ret") is not None else None
        r["rank"] = r.get(rank_col)
        r["bucket"] = W.record_bucket(r.get(rec[0]), r.get(rec[1]))
        r["where"] = (labels.get(r.get("cls")) or r.get("cls") or "ETF") if r["kind"] == "etf" else (r.get("sector") or "Unclassified")
    return {"mode": mode, "session": session, "carried": carried, "rows": rows, "spy": spy, "read": read, "rules": rules, "red": read["regime"] in ("dump", "down")}


def own_order(browser):
    """This proof compares lists with the desk's own ranking.  Since 2026-11-23 (kit 1.8.0) a list of stocks opens with
    the leaders and the largest companies first; the desk's own order is the other position of the same switch, and
    this proof selects it."""
    make = browser.new_context

    def new_context(*a, **k):
        ctx = make(*a, **k)
        ctx.add_init_script("try{ localStorage.setItem('jh-stock-order', 'engine') }catch(e){}")
        return ctx
    browser.new_context = new_context
    return browser


def scope_rows(ref, kind="stock", scope="b1", out="auto", rank="all", show="counted", q="", sector=""):
    """jh-strong-today.js rowFilter, in Python."""
    R = ref["rules"]
    of = ("held" if ref["red"] else "all") if out == "auto" else out
    q = q.strip().upper()
    keep = []
    for r in ref["rows"]:
        if kind != "all" and r["kind"] != kind:
            continue
        if r["kind"] == "stock":
            if scope == "b1" and not (r.get("mcap") or 0) >= R["board_min_mcap"]:
                continue
            if scope == "b10" and not (r.get("mcap") or 0) >= R["big_mcap"]:
                continue
            if scope == "sp" and not r.get("sp"):
                continue
        elif scope != "all" and not (r.get("adv") or 0) >= R["etf_min_adv"]:
            continue
        if show == "counted" and not r["counted"]:
            continue
        if show == "aside" and (r["counted"] or not r.get("bar")):
            continue
        if of == "held" and not (r["out"] and r["out"] != "down_more"):
            continue
        if of not in ("all", "held") and r["out"] != of:
            continue
        if rank == "strong" and not (r["rank"] is not None and r["rank"] >= 80):
            continue
        if rank == "weak" and not (r["rank"] is not None and r["rank"] <= 20):
            continue
        if rank == "record" and r["bucket"] != "all":
            continue
        if sector and r["where"] != sector:
            continue
        if q and not (str(r["t"]).upper().startswith(q) or q in str(r.get("name") or "").upper() or q in str(r.get("industry") or "").upper()):
            continue
        keep.append(r)
    return keep


def ordered(rows, key, desc=True):
    """jh-strong-today.js sortRows: blanks last either way; equal values by the move against SPY (best first), then the ticker."""
    tie = lambda r: (r.get("x") is None, -(r.get("x") or 0.0), str(r["t"]))
    have = sorted((r for r in rows if r.get(key) is not None), key=tie)
    have.sort(key=lambda r: r[key], reverse=desc)
    return have + sorted((r for r in rows if r.get(key) is None), key=tie)


def semver(v):
    try:
        return tuple(int(x) for x in str(v).split(".")[:3])
    except ValueError:
        return (0, 0, 0)


def number(text):
    try:
        return float(re.sub(r"[^0-9.\-]", "", str(text).replace(MINUS, "-").replace(",", "")))
    except ValueError:
        return None


def near(text, v, dp):
    """The printed figure is v at the digits it shows (the page rounds a tie its own way)."""
    got = number(text)
    return got is not None and v is not None and abs(got - v) <= 0.5 * 10 ** -dp + 1e-9


def diff(a, b, path=""):
    if isinstance(a, dict) and isinstance(b, dict):
        out = []
        for k in sorted(set(a) | set(b)):
            out += [(path + "/" + k, "only one side")] if (k not in a or k not in b) else diff(a[k], b[k], path + "/" + k)
        return out
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        if len(a) != len(b):
            return [(path, "length %d vs %d" % (len(a), len(b)))]
        out = []
        for i, (x, y) in enumerate(zip(a, b)):
            out += diff(x, y, "%s/%d" % (path, i))
        return out
    return [] if a == b else [(path, a, b)]


def fint(n):
    return format(int(n), ",")


# ------------------------------------------------------------------------------------------------------ the proof
def prove(browser, r, ck, shots, base=BASE, stamp=0, route=None, facts=None):
    """Open the page at two widths and hold everything on it against the reference. `route` (tests only) answers the
    page's requests from files; `facts` collects the figures for the report's summary."""
    facts = {} if facts is None else facts
    registry = json.loads((ROOT / "config" / "section-registry.json").read_text(encoding="utf-8"))["pages"]["strong"]["sections"]
    reg = {}
    for s in registry:
        reg[s["key"]] = s["n"]
        for x in s.get("sub", []):
            reg[x["key"]] = x["n"]

    for width, height, mobile, limit, tag in ((1440, 900, False, 6, "1440"), (390, 844, True, 3, "390")):
        r.section("strong.html at %s px" % tag)
        ctx = browser.new_context(viewport={"width": width, "height": height}, device_scale_factor=1, is_mobile=mobile, has_touch=mobile, accept_downloads=True,
                                  user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) jh-ops-probe Mobile Safari" if mobile else UA)
        if route:
            ctx.route("**/*", route)
        page = ctx.new_page()
        errors, got = [], {DESK_KEY: [], WIDE_KEY: [], TODAY_KEY: []}
        page.on("pageerror", lambda e, errors=errors: errors.append(str(e)[:200]))

        def on_response(resp, got=got):
            for key in got:
                if key in resp.url and resp.status == 200:
                    got[key].append(resp)
        page.on("response", on_response)
        parsed = {}

        def body(resp):
            if id(resp) not in parsed:
                try:
                    parsed[id(resp)] = json.loads(resp.body().decode("utf-8"))
                except Exception as exc:  # noqa: BLE001
                    parsed[id(resp)] = {"_error": str(exc)[:120]}
            return parsed[id(resp)]

        refs = {}

        def ref_for(s):
            """The reference over the very documents the page drew this state from."""
            v = s["view"]
            wide = next((d for d in map(body, reversed(got[WIDE_KEY])) if d.get("generated_at") == v["table_generated"]), None)
            desk = next((d for d in map(body, reversed(got[DESK_KEY])) if d.get("contract") == "strength.v1"), None)
            if v["mode"] == "live":
                today = next((d for d in map(body, reversed(got[TODAY_KEY])) if d.get("as_of") == v["bars_as_of"] and d.get("session") == v["session"]), None)
            else:
                today = next((d for d in map(body, reversed(got[TODAY_KEY])) if d.get("rows")), None)
            key = (v["table_generated"], v["bars_as_of"], v["mode"])
            if key not in refs:
                if wide is None or desk is None or (v["mode"] == "live" and today is None):
                    return None
                refs[key] = reference(wide, today, (desk.get("classes") or {}).get("labels") or {})
                refs[key]["doc"], refs[key]["today"] = wide, today
            return refs[key]

        def state(wait=350):
            page.wait_for_timeout(wait)
            return page.evaluate(STATE)

        def press(host, value):
            page.evaluate("([h, v]) => document.querySelector('#' + h + ' button.jh-chip[data-v=\"' + v + '\"]').click()", [host, value])

        def names_ok(s, where, kind="stock", scope="b1", out="auto", rank="all", show="counted", q="", sort=("x", True), view=None, shown_all=False, sector=""):
            """The names table against the reference under one set of filters."""
            ref = ref_for(s)
            if not ck(ref is not None, "%s: the page's own feeds were not captured" % where):
                return None
            exp = ordered(scope_rows(ref, kind, scope, out, rank, show, q, sector), *sort)
            n = len(exp)
            want = [x["t"] for x in (exp if shown_all else exp[:PAGE])]
            got_rows = [t for t in s["names"]["rows"] if t]
            ok = ck(s["names"]["meta"].startswith("%s of %s names" % (fint(n), fint(len(ref["rows"])))), "%s: the table says '%s', the reference lists %s of %s" % (where, s["names"]["meta"][:40], fint(n), fint(len(ref["rows"]))))
            ok &= ck(got_rows == want, "%s: %d rows on screen, first %s; the reference has %d, first %s" % (where, len(got_rows), got_rows[:5], len(want), want[:5]))
            ok &= ck((s["names"]["more"] is None) == (shown_all or n <= PAGE) and (s["names"]["more"] is None or s["names"]["more"] == "Show all %s rows" % fint(n)), "%s: the show-all button reads %s for %d rows" % (where, s["names"]["more"], n))
            if view:
                ok &= ck(s["names"]["head"] == ["#"] + view, "%s: columns %s" % (where, s["names"]["head"]))
            if n and "ret" in s["names"]["head"]:
                top, cells = exp[0], s["names"]["first"]
                ok &= ck(near(cells[s["names"]["head"].index("ret")], top["ret"], 2), "%s: %s prints a move of %s, the reference has %.4f" % (where, top["t"], cells[s["names"]["head"].index("ret")], top["ret"]))
                if "x" in s["names"]["head"]:
                    ok &= ck(near(cells[s["names"]["head"].index("x")], top["x"], 2), "%s: %s prints %s against SPY, the reference has %.4f" % (where, top["t"], cells[s["names"]["head"].index("x")], top["x"]))
            if not n:
                ok &= ck(bool(s["names"]["empty"]), "%s: an empty list shows no message" % where)
            return exp if ok else None

        def sheet_ok(s, where, fund, rows_cap):
            """The tearsheet against the reference for one scope."""
            ref, sh = ref_for(s), s["sheet"]
            if not ck(ref is not None and sh and sh["state"] == "ready", "%s: the sheet is not ready (%s)" % (where, sh and sh["state"])):
                return False
            R = ref["rules"]
            counted = [x for x in ref["rows"] if x["counted"]]
            stocks = [x for x in counted if x["kind"] == "stock"]
            rows = {"b1": [x for x in stocks if (x.get("mcap") or 0) >= R["board_min_mcap"]], "sp": [x for x in stocks if x.get("sp")], "all": stocks,
                    "etf": [x for x in counted if x["kind"] == "etf" and (x.get("adv") or 0) >= R["etf_min_adv"]]}[fund]
            word = {"b1": "stocks worth $1B or more", "sp": "S&P 500 members", "all": "listed stocks", "etf": "ETFs trading $5M or more a day"}[fund]
            total = sum(x.get("mcap") or 0 for x in rows)
            held = [x for x in rows if x["out"] != "down_more"]
            beat = [x for x in rows if x["ret"] > ref["spy"]]
            share = 100.0 * (len(held) if ref["red"] else len(beat)) / len(rows) if rows else None
            ok = ck(sh["chrome"] and sh["sec"] is None, "%s: the sheet is numbered as a section" % where)
            ok &= ck([c[0] for c in sh["ctl"]] == ["b1", "sp", "all", "etf"] and [c[0] for c in sh["ctl"] if c[1] == "true"] == [fund], "%s: scope buttons %s" % (where, sh["ctl"]))
            by_out = [v_ for v_ in (sum(x.get("mcap") or 0 for x in rows if x["out"] == o) for o in ("up", "flat", "down_less", "down_more")) if v_ > 0]
            ok &= ck(sh["total"] is not None and abs(sh["total"] - total) <= 1e-6 * max(total, 1) and len(sh["arcs"]) == len(by_out), "%s: the ring holds %s in %d slices; the scope's market value is %s in %d" % (where, sh["total"], len(sh["arcs"]), total, len(by_out)))
            ok &= ck(len(sh["arcs"]) == len(by_out) and all(abs(a[1] - 100.0 * v_ / total) < 0.002 for a, v_ in zip(sh["arcs"], by_out)), "%s: slices %s, the reference %s" % (where, [a[1] for a in sh["arcs"]], [round(100.0 * v_ / total, 3) for v_ in by_out]))
            ok &= ck(word in sh["title"] and near(sh["title"].split(" of " + word)[0].split(" ")[-1], share, 0), "%s: the title '%s' against %.2f%% of %s" % (where, sh["title"], share or -1, word))
            t = sh["tiles"]
            ok &= ck(len(t) == 6 and near(t[0][1], ref["spy"], 2) and near(t[1][1], share, 0) and t[1][2].startswith("%s " % fint(len(rows))) and near(t[2][1], 100.0 * (len(rows) - len(held)) / len(rows), 0)
                     and t[2][2].startswith("%s of %s" % (fint(len(rows) - len(held)), fint(len(rows)))), "%s: tiles %s" % (where, [x[:2] for x in t]))
            lists = sh["lists"]
            # funds that hold volatility itself are counted above and left off the ETF lists (the engine's rules.board_skip_cls)
            named = [x for x in rows if not (fund == "etf" and x.get("cls") in (R.get("board_skip_cls") or []))]
            up = sorted(sorted((x for x in named if x["out"] == "up"), key=lambda x: x["t"]), key=lambda x: -x["ret"])
            ok &= ck(number(lists["up"]["n"]) == len(up) and [x[0] for x in lists["up"]["rows"]] == [x["t"] for x in up[:rows_cap]], "%s: the up list is %s (%s), the reference %s (%d)" % (
                where, [x[0] for x in lists["up"]["rows"]][:4], lists["up"]["n"], [x["t"] for x in up[:4]], len(up)))
            if up:
                ok &= ck(near(lists["up"]["rows"][0][1], up[0]["ret"], 2), "%s: %s prints %s, the reference %.4f" % (where, up[0]["t"], lists["up"]["rows"][0][1], up[0]["ret"]))
            strong = [x for x in named if (x["rank"] or 0) >= 80]
            sb = sorted(sorted((x for x in strong if x["out"] == "down_more"), key=lambda x: x["t"]), key=lambda x: x["x"])
            shold = sorted(sorted((x for x in strong if x["out"] != "down_more"), key=lambda x: x["t"]), key=lambda x: -x["x"])
            rep = sorted(sorted((x for x in named if x["bucket"] == "all" and x["out"] != "down_more"), key=lambda x: x["t"]), key=lambda x: -x["x"])
            if fund == "etf":
                ok &= ck(not any(x[0] in {y["t"] for y in rows if y not in named} for lst in lists.values() for x in lst["rows"]), "%s: a fund that holds volatility is on a list" % where)
            for key, exp in (("strong-breaking", sb), ("strong-holding", shold), ("repeat", rep)):
                ok &= ck(number(lists[key]["n"]) == len(exp) and [x[0] for x in lists[key]["rows"]] == [x["t"] for x in exp[:rows_cap]], "%s: list %s is %s (%s), the reference %s (%d)" % (
                    where, key, [x[0] for x in lists[key]["rows"]][:4], lists[key]["n"], [x["t"] for x in exp[:4]], len(exp)))
            ok &= ck(len(sh["cards"]) == 2 and sh["notes"] == 3 and "link" in sh["acts"] and "print" in sh["acts"], "%s: cards %d, notes %d, actions %s" % (where, len(sh["cards"]), sh["notes"], sh["acts"]))
            return ok

        page.goto("%s/strong.html?nocache=%d" % (base, stamp + width), wait_until="domcontentloaded")
        try:
            page.wait_for_function("() => document.querySelector('#names-tbl tbody tr td a.tk') && (document.getElementById('jh-tearsheet') || {dataset: {}}).dataset.state === 'ready' && document.querySelector('#tbl tbody tr td a.tk')", timeout=90000)
        except Exception:  # noqa: BLE001
            pass
        s = state(1500)
        if not ck(bool(s["view"]) and s["sheet"] and s["sheet"]["state"] == "ready" and len(s["names"]["rows"]) > 0, "%s px: the page did not come up: view %s, sheet %s, %d names; scripts %s; errors %s" % (
                tag, bool(s["view"]), s["sheet"] and s["sheet"]["state"], len(s["names"]["rows"]), s["scripts"], errors[:2])):
            shots["failed_" + tag] = page.screenshot(type="jpeg", quality=45)
            ctx.close()
            continue
        ref, v = ref_for(s), s["view"]
        if not ck(ref is not None, "%s px: the page's feeds were not seen on the wire (%s)" % (tag, {k: len(x) for k, x in got.items()})):
            ctx.close()
            continue
        live, red, read, G = ref["mode"] == "live", ref["red"], ref["read"], ref["read"]["groups"]
        key = "held_pct" if red else "beat_pct"
        r.log("served: %s | kit %s | table %s of %s (generated %s) | bars as of %s | %s names" % (", ".join(s["scripts"]), s["kit"], v["version"], v["table_as_of"], v["table_generated"], v["bars_as_of"], fint(v["n"])))
        r.log("sentence: %s" % s["verdict"])

        # -------------------------------------------------------------------------------- the read, value for value
        ck(v["contract"] == "strength-view.v1" and v["mode"] == ref["mode"] and v["session"] == ref["session"] and v["rank_as_of"] == ref["carried"] and v["n"] == len(ref["rows"]),
           "%s px: view %s %s carried %s with %s rows; the reference %s %s carried %s with %d" % (tag, v["mode"], v["session"], v["rank_as_of"], v["n"], ref["mode"], ref["session"], ref["carried"], len(ref["rows"])))
        d = []
        for k in READ_KEYS:
            d += diff(read.get(k), v["read"].get(k), k)
        ck(not d, "%s px: the page's read differs from wide.py's in %d places: %s" % (tag, len(d), d[:4]))
        ck(abs(v["spy"] - ref["spy"]) < 1e-9 and (not live or (v["n_no_print"] == read["n_no_print"] and v["unconfirmed"] == read["unconfirmed"])),
           "%s px: SPY %s vs %s, no print %s vs %s, unconfirmed %s vs %s" % (tag, v["spy"], ref["spy"], v["n_no_print"], read.get("n_no_print"), v["unconfirmed"], read.get("unconfirmed")))
        r.log("the read (%s, %s, regime %s, SPY %+.2f%%): %d keys equal wide.py's on the page's own bytes - %s names counted, %d sectors, %d ETF classes, %d industries, %d boards" % (
            ref["mode"], ref["session"], read["regime"], ref["spy"], len(READ_KEYS), fint(read["n_counted"]), len(read["by_sector"]), len(read["by_class"]), read["industries"]["n"], len(read["boards"])))

        # ------------------------------------------------------------------------- the sentence, tiles, bars, panels
        b1, spx = G["stocks_1b"], G["sp500"]
        ck(s["verdict"].startswith("Today, as of " if live else "Last session (%s)" % ref["session"]) and near(s["verdict"].split("SPY ")[1].split("%")[0], ref["spy"], 2)
           and ("%d%% of stocks worth $1B or more" % number(s["verdict"].split(". ")[1].split("%")[0])) in s["verdict"] and near(s["verdict"].split(". ")[1].split("%")[0], b1[key], 0),
           "%s px: the sentence against SPY %.2f and %s %.1f: %s" % (tag, ref["spy"], key, b1[key], s["verdict"][:200]))
        if red and len(read["by_sector"]) > 1:
            ck(read["by_sector"][0]["sector"] in s["verdict"] and read["by_sector"][-1]["sector"] in s["verdict"], "%s px: the sentence does not name %s and %s" % (tag, read["by_sector"][0]["sector"], read["by_sector"][-1]["sector"]))
        k = s["today"]["kpis"]
        tile_groups = (("S&P 500 members", spx), ("Stocks worth $1B or more", b1), ("Every stock", G["stocks"]), ("ETFs trading $5M+ a day", G["etfs_liquid"]))
        ck(len(k) == 6 and near(k[0][1], ref["spy"], 2) and all(k[i + 1][0] == name and near(k[i + 1][1].split("%")[0], g[key], 0)
                                                                    and ("up %s · flat %s · down less %s · down more %s" % (fint(g["up"]), fint(g["flat"]), fint(g["down_less"]), fint(g["down_more"]))) in k[i + 1][2]
                                                                    for i, (name, g) in enumerate(tile_groups)) and near(k[5][1].split("%")[0], read["by_rank"][9][key], 0), "%s px: the six tiles: %s" % (tag, k))
        for t in k:
            r.log("  tile: %s = %s // %s" % tuple(t))
        ck(all(("%s counted" % fint(g["n"])) in s["today"]["bars"] for g in (spx, G["stocks_10b"], b1, G["stocks"], G["etfs_liquid"], G["etfs"])) and ("Who is holding up" if live else "Who held up") in s["today"]["bars_h"],
           "%s px: the bars: %s | %s" % (tag, s["today"]["bars_h"], s["today"]["bars"][:200]))
        ck(s["today"]["carried_mode"] == "record" and len(s["today"]["carried"]) == len(read["by_record"]) and all(line[1] == ("%s stocks" % fint(b["n"]) if b["n"] else "none") and (not b["n"] or near(line[2], b[key], 0)) for line, b in zip(s["today"]["carried"], read["by_record"])),
           "%s px: by dump-day record: %s vs %s" % (tag, s["today"]["carried"], [(b["n"], b.get(key)) for b in read["by_record"]]))
        press("carried-mode", "rank")
        s2 = state()
        ck(s2["today"]["carried_mode"] == "rank" and len(s2["today"]["carried"]) == 10 and all(line[1] == (fint(b["n"]) if b["n"] else "") and (not b["n"] or near(line[2], b[key], 0)) for line, b in zip(s2["today"]["carried"], ref_for(s2)["read"]["by_rank"])),
           "%s px: by rank: %s vs %s" % (tag, s2["today"]["carried"][:3], [(b["n"], b[key]) for b in read["by_rank"]][:3]))
        r.log("carried in, by dump-day record: %s" % " | ".join("%s (%s) %s, %s" % tuple(x) for x in s["today"]["carried"]))
        r.log("carried in, by rank: %s" % " | ".join("%s (%s) %s, %s" % tuple(x) for x in s2["today"]["carried"]))
        press("carried-mode", "record")
        gs = s["today"]["groups"]
        ck(s["today"]["group"] == "sector" and [g[0] for g in gs] == [g["sector"] for g in ordered([dict(g, t=g["sector"]) for g in read["by_sector"]], key)] and all(g[1] == fint(x["n"]) for g, x in zip(gs, ordered([dict(g, t=g["sector"]) for g in read["by_sector"]], key))),
           "%s px: the sector table %s vs %s" % (tag, [g[:2] for g in gs][:4], [(g["sector"], g["n"]) for g in read["by_sector"]][:4]))
        press("today-group", "class")
        s2 = state()
        ck(s2["today"]["group"] == "class" and sorted(g[0] for g in s2["today"]["groups"]) == sorted(g["label"] for g in ref_for(s2)["read"]["by_class"]), "%s px: ETF classes %s" % (tag, [g[0] for g in s2["today"]["groups"]]))
        r.log("ETF classes (median move, best first): %s" % ", ".join("%s %s" % (g[0], g[s2["today"]["groups_head"].index("med")]) for g in s2["today"]["groups"]))
        press("today-group", "industry")
        s2 = state()
        ind = ref_for(s2)["read"]["industries"]
        shown_ind = [g[0] for g in s2["today"]["groups"]]
        ck(s2["today"]["group"] == "industry" and len(shown_ind) == ind["n"] and all(any(x == g["industry"] or x.startswith(g["industry"] + " ") for x in shown_ind) for g in ind["holding"] + ind["breaking"]),
           "%s px: the industry table has %d rows, first %s; the reference has %d industries, best %s" % (tag, len(shown_ind), shown_ind[:3], ind["n"], [g["industry"] for g in ind["holding"][:3]]))
        press("today-group", "sector")

        # ------------------------------------------------------------------------------------ every stock and ETF
        s = state()
        view0 = VIEW_PHONE if mobile else VIEW_TODAY
        base_rows = names_ok(s, "%s px, as it opens" % tag, view=view0)
        ck(s["names"]["kind"] == "stock" and s["names"]["scope"] == "b1" and s["names"]["out"] == ("held" if red else "all") and s["names"]["rank"] == "all" and s["names"]["show"] == "counted" and s["names"]["viewc"] == "today"
           and s["names"]["sorted"] == [["x", "descending"]], "%s px: the table opens on %s" % (tag, {q_: s["names"][q_] for q_ in ("kind", "scope", "out", "rank", "show", "viewc", "sorted")}))
        if not mobile:
            ck(s["names"]["fit"] <= 0, "1440 px: the names table is %s px wider than its box" % s["names"]["fit"])
        if base_rows:
            r.log("opens on: %s" % s["names"]["meta"])
            for x in base_rows[:12]:
                r.log("  %-6s %-34s %-22s move %+7.2f%%  vs SPY %+7.2f  rank in %s  dump days %s" % (x["t"], str(x.get("name"))[:34], x["where"][:22], x["ret"], x["x"], "-" if x["rank"] is None else "%.0f" % x["rank"],
                                                                                                    "-" if not x.get("dn" if live else "dnp") else "%s/%s" % (x["dh" if live else "dhp"], x["dn" if live else "dnp"])))
        steps = [("names-kind", "etf", dict(kind="etf")), ("names-scope", "all", dict(kind="etf", scope="all")), ("names-kind", "all", dict(kind="all", scope="all")), ("names-kind", "stock", dict(scope="all")),
                 ("names-scope", "sp", dict(scope="sp")), ("names-scope", "b10", dict(scope="b10")), ("names-scope", "b1", dict()), ("names-out", "up", dict(out="up")), ("names-out", "flat", dict(out="flat")),
                 ("names-out", "down_less", dict(out="down_less")), ("names-out", "down_more", dict(out="down_more")), ("names-out", "all", dict(out="all")), ("names-rank", "strong", dict(out="all", rank="strong")),
                 ("names-rank", "weak", dict(out="all", rank="weak")), ("names-rank", "record", dict(out="all", rank="record")), ("names-rank", "all", dict(out="all")), ("names-show", "aside", dict(out="all", show="aside")),
                 ("names-show", "all", dict(out="all", show="all")), ("names-show", "counted", dict(out="all")), ("names-out", "held", dict(out="held"))]
        counts = []
        for host, value, f in steps:
            press(host, value)
            sx = state(220)
            rows = names_ok(sx, "%s px, %s=%s" % (tag, host, value), **f)
            counts.append("%s=%s %s" % (host.replace("names-", ""), value, "FAIL" if rows is None else fint(len(rows))))
        r.log("chips, the list after each (reference = page): %s" % " | ".join(counts))
        # the three column sets
        for value, cols in (("record", VIEW_RECORD), ("trend", VIEW_TREND), ("today", view0)):
            press("names-view", value)
            names_ok(state(220), "%s px, columns %s" % (tag, value), out="held", view=cols, sort=("x", True) if value == "today" else ("x", True))
        # headers: rank by the move, both ways; by market value; back to the move against SPY
        for col, key_, desc in (("ret", "ret", True), ("ret", "ret", False), ("mcap", "mcap", True), ("rank", "rank", True), ("x", "x", True)):
            page.evaluate("(c) => document.querySelector('#names-tbl th[data-sort=\"' + c + '\"]').click()", col)
            sx = state(220)
            rows = names_ok(sx, "%s px, header %s %s" % (tag, col, "down" if desc else "up"), out="held", sort=(key_, desc))
            ck(sx["names"]["sorted"] == [[col, "descending" if desc else "ascending"]], "%s px: header %s shows %s" % (tag, col, sx["names"]["sorted"]))
            if rows and col == "ret":
                r.log("ranked by the move, %s: %s" % ("highest first" if desc else "lowest first", ", ".join("%s %+.2f%%" % (x["t"], x["ret"]) for x in rows[:8])))
        # find
        for term in ("NV", "bank"):
            page.fill("#names-q", term)
            sx = state(450)
            rows = names_ok(sx, "%s px, find '%s'" % (tag, term), out="held", q=term)
            if rows is not None:
                r.log("find '%s': %d names (%s)" % (term, len(rows), ", ".join(x["t"] for x in rows[:8])))
        page.fill("#names-q", "")
        state(450)
        # show all, then the CSV of exactly those rows
        press("names-out", "all")
        sx = state(220)
        allrows = names_ok(sx, "%s px, every counted $1B+ stock" % tag, out="all")
        if allrows is not None and len(allrows) > PAGE:
            page.evaluate("() => document.getElementById('names-more').click()")
            sx = state(500)
            names_ok(sx, "%s px, show all" % tag, out="all", shown_all=True)
            r.log("show all: %d rows on screen" % len(sx["names"]["rows"]))
        if not mobile and allrows is not None:
            try:
                with page.expect_download(timeout=20000) as dl:
                    page.evaluate("() => document.getElementById('names-csv').click()")
                text = Path(dl.value.path()).read_text(encoding="utf-8")
                lines = text.split("\n")
                ck(len(lines) == len(allrows) + 1 and lines[0].startswith("t,name,") and [ln.split(",")[0] for ln in lines[1:6]] == [x["t"] for x in allrows[:5]] and dl.value.suggested_filename.startswith("strength-vs-spy-%s" % ref["session"]),
                   "the CSV %s holds %d lines for %d rows, first %s" % (dl.value.suggested_filename, len(lines), len(allrows), [ln.split(",")[0] for ln in lines[1:4]]))
                r.log("CSV: %s, %d rows + header, %d bytes; header %s" % (dl.value.suggested_filename, len(lines) - 1, len(text), lines[0][:150]))
            except Exception as exc:  # noqa: BLE001
                ck(False, "the CSV did not download: %s" % str(exc)[:120])
        # the address carries the state; a fresh page restores it
        press("names-kind", "etf")
        press("names-out", "down_more")
        sx = state(250)
        ck("kind=etf" in sx["hash"] and "out=down_more" in sx["hash"], "%s px: the address does not carry the filters: %s" % (tag, sx["hash"]))
        if not mobile:
            p2 = ctx.new_page()
            p2.on("response", on_response)
            p2.goto("%s/strong.html?nocache=%d%s" % (base, stamp + 7, sx["hash"]), wait_until="domcontentloaded")
            try:
                p2.wait_for_function("() => document.querySelector('#names-tbl tbody tr td') && window.JHStrengthDoc", timeout=90000)
            except Exception:  # noqa: BLE001
                pass
            p2.wait_for_timeout(1200)
            keep, page = page, p2
            s3 = page.evaluate(STATE)
            rows = names_ok(s3, "a fresh page at %s" % sx["hash"], kind="etf", out="down_more")
            ck(s3["names"]["kind"] == "etf" and s3["names"]["out"] == "down_more", "a fresh page at %s opens on %s / %s" % (sx["hash"], s3["names"]["kind"], s3["names"]["out"]))
            if rows is not None:
                r.log("a fresh page at %s: %d ETFs down more than SPY (%s)" % (sx["hash"], len(rows), ", ".join("%s %+.2f%%" % (x["t"], x["ret"]) for x in rows[-6:])))
            page = keep
            p2.close()
        page.evaluate("() => document.getElementById('names-reset').click()")
        s = state(400)
        names_ok(s, "%s px, after Clear filters" % tag, view=view0)

        # ------------------------------------------------------------------------------------------ the tearsheet
        ck(s["sheet"]["eye"].startswith("Strength tearsheet") and (("today %s" % ref["session"]) if live else ("last session %s" % ref["session"])) in s["sheet"]["eye"] and ("justhodl-strength %s" % v["version"]) in s["sheet"]["foot"]
           and semver(s["kit"]) >= KIT_MIN, "%s px: eyebrow '%s', source '%s', kit %s" % (tag, s["sheet"]["eye"], s["sheet"]["foot"][:80], s["kit"]))
        r.log("sheet: %s" % s["sheet"]["eye"])
        r.log("sheet: %s" % s["sheet"]["title"])
        for t in s["sheet"]["tiles"]:
            r.log("  sheet tile: %s = %s // %s" % tuple(t))
        sheet_ok(s, "%s px, $1B+ stocks" % tag, "b1", limit)
        top_y, sheet_h = s["sheet"]["top"], s["sheet"]["h"]
        if not mobile:
            page.evaluate("() => window.scrollTo(0, 0)")
            shots["sheet_" + tag] = page.screenshot(type="jpeg", quality=52, full_page=True, clip={"x": 0, "y": max(0, top_y - 8), "width": width, "height": min(sheet_h + 16, 1350)})
        for fund in ("sp", "all", "etf", "b1"):
            page.evaluate("(k) => document.querySelector('#jh-tearsheet [data-jts-ctl=\"' + k + '\"]').click()", fund)
            sx = state(300)
            if sheet_ok(sx, "%s px, scope %s" % (tag, fund), fund, limit):
                r.log("scope %-3s: %s | ring %s | up: %s" % (fund, sx["sheet"]["title"], sx["sheet"]["center"], ", ".join("%s %s" % (x[0], x[1]) for x in sx["sheet"]["lists"]["up"]["rows"][:5])))
        if "more" in s["sheet"]["acts"]:
            page.evaluate("() => document.querySelector('#jh-tearsheet [data-jts-act=\"more\"]').click()")
            sx = state(350)
            sheet_ok(sx, "%s px, more rows" % tag, "b1", 15)
            r.log("MORE ROWS: %s" % ", ".join("%s %d" % (k_, len(x["rows"])) for k_, x in sx["sheet"]["lists"].items()))
            page.evaluate("() => document.querySelector('#jh-tearsheet [data-jts-act=\"more\"]').click()")
        s = state(350)
        for name in [x for x in ("up", "down-less", "down-most", "strong-holding", "strong-breaking", "weak-bid", "repeat", "groups", "industries-holding", "industries-breaking") if x in s["sheet"]["lists"]]:
            lst = s["sheet"]["lists"][name]
            r.log("  %-20s %5s | %s" % (lst["title"][:20], lst["n"], ", ".join("%s %s" % (x[0], x[1]) for x in lst["rows"][:6])))

        # ---------------------------------------------------------------- what the page had before, and the numbers
        o = s["old"]
        ck(o["verdict_kpis"] >= 4 and o["thesis_kpis"] >= 3 and o["dumps"] >= 1 and o["rallies"] >= 1 and o["classes"] >= 8 and o["coil"] >= 1 and o["leaders"] >= 5 and o["laggards"] >= 5 and o["focus"] > 20
           and o["desk"] >= 100 and o["chips"] >= 5 and o["defs"] > 200 and o["sources"] > 100, "%s px: a section the page had before is not drawn: %s" % (tag, o))
        r.log("what the page had before, drawn: %s" % ", ".join("%s %s" % kv for kv in o.items()))
        dom = {key_: n for n, key_, _ in s["secs"]}
        moved = [(key_, n, dom.get(key_)) for key_, n in reg.items() if dom.get(key_) != n and (not mobile or "/" not in key_ or key_ in dom)]
        ck(not moved and dom.get("today") == "13" and dom.get("names") == "14", "%s px: section numbers differ from the registry: %s" % (tag, moved[:6]))
        tops = [key_ for n, key_, _ in s["secs"] if "/" not in key_]
        ck(tops[:4] == ["strength-vs-s-p", "today", "names", "verdict-index-regime-breadth-and-bases"] and len(tops) == 12, "%s px: the page's order is %s" % (tag, tops))
        r.log("sections, top to bottom: %s" % ", ".join("§%s %s" % (n, key_) for n, key_, _ in s["secs"] if "/" not in key_))
        ck(s["ox"] <= 0 and not s["bad"] and not errors, "%s px: sideways overflow %s, bad text %s, page errors %s" % (tag, s["ox"], s["bad"][:3], errors[:3]))
        r.log("sideways overflow %s px | 'undefined' / NaN on the page: %d | page errors: %d | feed responses seen: %s" % (s["ox"], len(s["bad"]), len(errors), {k_.split("/")[-1]: len(x) for k_, x in got.items()}))
        # screenshots
        page.evaluate("() => window.scrollTo(0, 0)")
        for sel, name, cap in (("#today", "today", 1250), ("#names", "names", 1150)):
            box = page.evaluate("(q) => { const b = document.querySelector(q).getBoundingClientRect(); return [Math.round(b.top + scrollY), Math.round(b.height)]; }", sel)
            shots["%s_%s" % (name, tag)] = page.screenshot(type="jpeg", quality=50, full_page=True, clip={"x": 0, "y": max(0, box[0] - 6), "width": width, "height": min(box[1] + 12, cap if not mobile else 1500)})
        if not facts:
            facts.update(mode=ref["mode"], session=ref["session"], spy=round(ref["spy"], 2), regime=read["regime"], counted=read["n_counted"], held_1b=b1["held_pct"], up_1b=b1["up_pct"], held_sp500=spx["held_pct"],
                         held_etf_liquid=G["etfs_liquid"]["held_pct"], bars_as_of=v["bars_as_of"], table=v["table_generated"],
                         by_record=[(b["n"], b["held_pct"], b.get("median_ret")) for b in read["by_record"]], by_rank=[b["held_pct"] for b in read["by_rank"]],
                         sectors=[(g["sector"], g["n"], g["held_pct"], g["median_ret"]) for g in read["by_sector"]], classes=[(g["label"], g["n"], g["held_pct"], g["median_ret"]) for g in read["by_class"]])
        ctx.close()
    return facts


def main():
    from ops_report import report
    with report("ops_6866_strength_page_live_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6866 - the Strength desk, rebuilt, live %s" % now.isoformat(timespec="minutes"))
        fails, shots, facts = [], {}, {}

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL %s" % msg)
            return bool(cond)

        sync_playwright = ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = own_order(launch(p, r.log))
            prove(browser, r, ck, shots, BASE, int(time.time()), None, facts)
            browser.close()
        if facts:
            r.section("the session as the page shows it")
            r.log("%s %s: SPY %+.2f%% (%s), %s names counted; $1B+ stocks holding up %.1f%% (up %.1f%%), S&P 500 members %.1f%%, ETFs $5M+ %.1f%%" % (
                facts["mode"], facts["session"], facts["spy"], facts["regime"], fint(facts["counted"]), facts["held_1b"], facts["up_1b"], facts["held_sp500"], facts["held_etf_liquid"]))
            r.log("by dump-day record carried in (n, held up %%, median move): %s" % facts["by_record"])
            r.log("by rank tenth, weakest to strongest, held up %%: %s" % facts["by_rank"])
            for g in facts["sectors"]:
                r.log("  sector %-26s n %4d  held up %5.1f%%  median %+.2f%%" % tuple(g))
            for g in facts["classes"]:
                r.log("  ETF    %-26s n %4d  held up %5.1f%%  median %+.2f%%" % tuple(g))
            r.kv(failures=len(fails), mode=facts["mode"], session=facts["session"], spy=facts["spy"], held_up_1b=facts["held_1b"], held_up_sp500=facts["held_sp500"], bars_as_of=str(facts["bars_as_of"]))
        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        if fails:
            r.fail("%d checks failed: %s" % (len(fails), " | ".join(fails)[:1800]))
            sys.exit(1)
        r.ok("LIVE PROOF: strong.html's read of the session equals wide.py's on the page's own bytes at both widths; every chip, header, find, the CSV and the four tearsheet scopes list the reference's names; the page's earlier sections are drawn under their registered numbers")


if __name__ == "__main__":
    main()
