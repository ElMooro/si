"""ops 6867 - READ-ONLY live proof: every category of the Strength desk opens as a whole list that ranks by any column.

Khalid 2026-11-20 17:46 ET: "this is a good start but you are only displaying the first six of each category - when i
click on that category i should be able to view the entire list and sort them by market cap and by industry and sort
them ascending and descending etc"

CLAUDE.md "CHECK YOUR WORK": Chrome on the runner opens justhodl.ai/strong.html at 1440 and 390 px and does what he
described, and every list on screen is held against the Python reference (wide.py) run over the bytes the page itself
fetched (the method of ops 6866, whose helpers this imports):
  - every card of the tearsheet, each ring slice and the ring itself is clicked: the whole list opens (its count, its
    first 200 rows in order, SHOW ALL for the rest), for each of the four scopes;
  - the open list is ranked by Mkt value, Industry, Sector, Move, Rank in and Dump days, each both ways: the rows on
    screen are the reference's in that order, the heading carries the arrow, blanks are last;
  - find narrows it, DOWNLOAD CSV saves exactly the rows on screen in their order, a fresh feed from the desk leaves the
    open list, its ranking and the find text in place, BACK and Escape return to the sheet;
  - sectors and industries open in full (every industry, not fifteen), rank by any column, and a click on one shows
    its names in "Every stock and ETF" below;
  - that table has an Industry column that ranks both ways and an Industry menu; a row of the sector / industry /
    ETF-class table above it filters it the same way;
  - no page error, nothing spills sideways, nothing prints "undefined".
Writes nothing.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import importlib.util
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
_spec = importlib.util.spec_from_file_location("ops6866", str(Path(__file__).resolve().parent / "ops_6866_strength_page_live_proof.py"))
P = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(P)

BASE = P.BASE
PAGE = 200
OUT_WORD = {"up": "Up", "flat": "Flat", "down_less": "Down less", "down_more": "Down more"}
OUT_N = {"up": 3, "flat": 2, "down_less": 1, "down_more": 0}

FULL = r"""() => { const el = document.getElementById('jh-tearsheet'), f = el.querySelector('.jts-full'), q = (r, s) => r ? Array.from(r.querySelectorAll(s)) : [], txt = (e) => e ? e.textContent.replace(/\s+/g, ' ').trim() : null;
  const v = window.JHStrengthDoc, head = q(f, '.jts-ft thead th[data-jts-sort]').map(th => th.getAttribute('data-jts-sort'));
  return { open: el.dataset.open || '', state: el.dataset.state, view: v ? { mode: v.mode, session: v.session, bars_as_of: v.bars_as_of, table_generated: (v.table || {}).generated_at } : null,
    key: f ? f.getAttribute('data-jts-full') : null, title: txt(f && f.querySelector('.jts-ft1')), count: txt(f && f.querySelector('[data-jts-count]')),
    chips: q(f, '.jts-fnav [data-jts-open]').map(b => [b.getAttribute('data-jts-open'), b.getAttribute('aria-pressed'), txt(b.querySelector('i'))]),
    head: head, heads: q(f, '.jts-ft thead th[data-jts-sort]').map(th => txt(th).replace(/ [▾▴]$/, '')), sorted: q(f, '.jts-ft thead th[aria-sort]').map(th => [th.getAttribute('data-jts-sort'), th.getAttribute('aria-sort')]),
    rows: q(f, '.jts-ft tbody tr').map(tr => txt(tr.cells[1])), n_rows: q(f, '.jts-ft tbody tr').length,
    first: q(q(f, '.jts-ft tbody tr')[0], 'td').map(txt),
    cells: (function () { const i = head.indexOf('industry'), s = head.indexOf('sector'), m = head.indexOf('mcap'); return q(f, '.jts-ft tbody tr').slice(0, 400).map(tr => [i < 0 ? null : txt(tr.cells[i + 1]), s < 0 ? null : txt(tr.cells[s + 1]), m < 0 ? null : txt(tr.cells[m + 1])]); })(),
    more: txt(f && f.querySelector('[data-jts-act="all"]')), find: f && f.querySelector('[data-jts-find]') ? f.querySelector('[data-jts-find]').value : null,
    focus: !!(document.activeElement && document.activeElement.hasAttribute && document.activeElement.hasAttribute('data-jts-find')),
    cards: q(el, '.jts-cols [data-jts-list]').map(c => [c.getAttribute('data-jts-list'), !!c.querySelector('.jts-ch[data-jts-open]'), txt(c.querySelector('.jts-n')), txt(c.querySelector('.jts-go'))]),
    legend: q(el, '.jts-leg tr').map(tr => tr.getAttribute('data-jts-open')), ctl: q(el, '[data-jts-ctl]').filter(b => b.getAttribute('aria-pressed') === 'true').map(b => b.getAttribute('data-jts-ctl'))[0],
    sheet_top: Math.round(el.getBoundingClientRect().top), sheet_h: Math.round(el.getBoundingClientRect().height), page_y: Math.round(el.getBoundingClientRect().top + scrollY),
    wrap: f && f.querySelector('.jts-fwrap') ? [f.querySelector('.jts-fwrap').scrollWidth, f.querySelector('.jts-fwrap').clientWidth, Math.round(f.querySelector('.jts-fwrap').getBoundingClientRect().height)] : null,
    bad: (el.innerText.match(/undefined|NaN|\[object Object\]/g) || []).length, ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""

NAMES = r"""() => { const T = (id) => document.getElementById(id), tbl = T('names-tbl'), q = (r, s) => r ? Array.from(r.querySelectorAll(s)) : [], txt = (e) => e ? e.textContent.replace(/\s+/g, ' ').trim() : null;
  const chips = (id) => q(T(id), 'button.jh-chip').filter(b => b.getAttribute('aria-pressed') === 'true').map(b => b.getAttribute('data-v'))[0];
  const head = q(tbl, 'th').map(th => th.getAttribute('data-sort') || '#'), v = window.JHStrengthDoc, box = T('names').getBoundingClientRect(), i = head.indexOf('ind');
  return { meta: txt(T('names-meta')), head: head, sorted: q(tbl, 'th[aria-sort]').map(th => [th.getAttribute('data-sort'), th.getAttribute('aria-sort')]),
    rows: q(tbl, 'tbody tr').map(tr => { const a = tr.querySelector('a.tk'); return a ? a.textContent.trim() : null; }), ind: i < 0 ? [] : q(tbl, 'tbody tr').map(tr => txt(tr.cells[i])),
    kind: chips('names-kind'), scope: chips('names-scope'), out: chips('names-out'), rank: chips('names-rank'), show: chips('names-show'),
    sector: T('names-sector').value, industry: T('names-industry').value, industry_hidden: T('names-industry').parentNode.hidden, n_industries: T('names-industry').options.length - 1,
    more: T('names-more').hidden ? null : txt(T('names-more')), top: Math.round(box.top), vh: innerHeight, hash: location.hash, fit: tbl.scrollWidth - tbl.clientWidth,
    group: chips('today-group'), groups: q(T('today-groups'), 'tbody tr').map(tr => txt(tr.querySelector('[data-pick]'))),
    view: v ? { mode: v.mode, session: v.session, bars_as_of: v.bars_as_of, table_generated: (v.table || {}).generated_at } : null,
    ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""


def text_key(v):
    return None if v in (None, "") else str(v).lower()


def full_sorted(rows, get, kind, desc):
    """kit fullRows: blanks last either way, equal values in the list's own order."""
    dec = [(i, r, (text_key(get(r)) if kind == "s" else get(r))) for i, r in enumerate(rows)]
    have = [x for x in dec if x[2] is not None]
    none = [x for x in dec if x[2] is None]
    have.sort(key=lambda x: x[2], reverse=desc)          # stable: ties keep the list's own order, both ways
    return [x[1] for x in have + none]


def sheet_lists(ref, fund):
    """The strength adapter's lists of names for one scope, in each list's own order (jh-tearsheet.js ADAPTERS.strength)."""
    R = ref["rules"]
    counted = [x for x in ref["rows"] if x["counted"]]
    stocks = [x for x in counted if x["kind"] == "stock"]
    rows = {"b1": [x for x in stocks if (x.get("mcap") or 0) >= R["board_min_mcap"]], "sp": [x for x in stocks if x.get("sp")], "all": stocks,
            "etf": [x for x in counted if x["kind"] == "etf" and (x.get("adv") or 0) >= R["etf_min_adv"]]}[fund]
    skip = (R.get("board_skip_cls") or []) if fund == "etf" else []
    named = [x for x in rows if x.get("cls") not in skip]
    top = lambda lst, key, desc=True: sorted(sorted(lst, key=lambda x: x["t"]), key=lambda x: x[key], reverse=desc)
    of = lambda o: [x for x in named if x["out"] == o]
    strong = [x for x in named if (x["rank"] or 0) >= 80]
    weak = [x for x in named if x["rank"] is not None and x["rank"] <= 20]
    red = ref["red"]
    more = top(of("down_more"), "ret", False)
    lists = {"all": top(rows, "x"), "up": top(of("up"), "ret"), "flat": top(of("flat"), "x"), "down-less": top(of("down_less"), "x"),
             "strong-holding": top([x for x in strong if x["out"] != "down_more"], "x"), "strong-breaking": top([x for x in strong if x["out"] == "down_more"], "x", False),
             "weak-bid": top([x for x in weak if x["out"] == "up"], "ret"), "repeat": top([x for x in named if x["bucket"] == "all" and x["out"] != "down_more"], "x")}
    lists["down-most-etf" if (red and fund == "etf") else "down-more" if red else "down-most"] = more
    slices = {"up": "up", "flat": "flat", "down_less": "down-less", "down_more": "down-most-etf" if (red and fund == "etf") else "down-more" if red else "down-most"}
    cards = ["up", "down-less" if red else "down-most", "strong-holding", "strong-breaking", "weak-bid", "repeat", "groups"] + (["industries-holding", "industries-breaking"] if fund != "etf" else (["down-most-etf"] if red else []))
    return {"rows": rows, "lists": lists, "slices": slices, "cards": cards, "unit": {"b1": "stock", "sp": "member", "all": "stock", "etf": "fund"}[fund]}


def industries(ref, rows, minimum):
    """Every industry of the scope with enough names: count, share holding up, market value (the adapter's grp())."""
    by = {}
    for x in rows:
        if x.get("industry"):
            by.setdefault(x["industry"], []).append(x)
    out = []
    for name, members in by.items():
        if len(members) < minimum:
            continue
        held = sum(1 for x in members if x["out"] != "down_more")
        beat = sum(1 for x in members if x["ret"] > ref["spy"])
        rets = sorted(x["ret"] for x in members)
        med = rets[len(rets) // 2] if len(rets) % 2 else (rets[len(rets) // 2 - 1] + rets[len(rets) // 2]) / 2.0
        out.append({"name": name, "n": len(members), "held_pct": held / len(members) * 100.0, "beat_pct": beat / len(members) * 100.0, "median_ret": med, "mcap": sum(x.get("mcap") or 0 for x in members), "members": members})
    key = "held_pct" if ref["red"] else "beat_pct"
    out.sort(key=lambda g: (-g[key], -g["median_ret"], g["name"]))
    return out


def prove(browser, r, ck, shots, base=BASE, stamp=0, route=None, facts=None):
    facts = {} if facts is None else facts
    for width, height, mobile, tag in ((1440, 900, False, "1440"), (390, 844, True, "390")):
        r.section("strong.html at %s px" % tag)
        ctx = browser.new_context(viewport={"width": width, "height": height}, device_scale_factor=1, is_mobile=mobile, has_touch=mobile, accept_downloads=True,
                                  user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) jh-ops-probe Mobile Safari" if mobile else P.UA)
        if route:
            ctx.route("**/*", route)
        page = ctx.new_page()
        errors, got, parsed, refs = [], {P.DESK_KEY: [], P.WIDE_KEY: [], P.TODAY_KEY: []}, {}, {}
        page.on("pageerror", lambda e, errors=errors: errors.append(str(e)[:200]))

        def on_response(resp, got=got):
            for key in got:
                if key in resp.url and resp.status == 200:
                    got[key].append(resp)
        page.on("response", on_response)

        def body(resp):
            if id(resp) not in parsed:
                try:
                    parsed[id(resp)] = json.loads(resp.body().decode("utf-8"))
                except Exception as exc:  # noqa: BLE001
                    parsed[id(resp)] = {"_error": str(exc)[:120]}
            return parsed[id(resp)]

        def ref_for(v):
            """The reference over the very documents the page drew this state from (ops 6866's method)."""
            key = (v["table_generated"], v["bars_as_of"], v["mode"])
            if key not in refs:
                wide = next((d for d in map(body, reversed(got[P.WIDE_KEY])) if d.get("generated_at") == v["table_generated"]), None)
                desk = next((d for d in map(body, reversed(got[P.DESK_KEY])) if d.get("contract") == "strength.v1"), None)
                today = next((d for d in map(body, reversed(got[P.TODAY_KEY])) if (d.get("as_of") == v["bars_as_of"] and d.get("session") == v["session"]) or (v["mode"] != "live" and d.get("rows"))), None)
                if wide is None or desk is None or (v["mode"] == "live" and today is None):
                    return None
                ref = P.reference(wide, today, (desk.get("classes") or {}).get("labels") or {})
                for x in ref["rows"]:
                    x["ind"] = None if x["kind"] == "etf" else (x.get("industry") or None)
                    n_, h_ = (x.get("dn"), x.get("dh")) if ref["mode"] == "live" else (x.get("dnp"), x.get("dhp"))     # the record carried INTO the session
                    x["dumpkey"] = (h_ / n_ + n_ / 1e4) if n_ else None
                refs[key] = ref
            return refs[key]

        def full(wait=260):
            page.wait_for_timeout(wait)
            return page.evaluate(FULL)

        def names(wait=450):
            page.wait_for_timeout(wait)
            return page.evaluate(NAMES)

        def js(code, arg=None):
            return page.evaluate(code, arg)

        def click_sheet(sel):
            js("(s) => document.querySelector('#jh-tearsheet ' + s).click()", sel)

        def list_ok(s, where, want, unit, sort=None, shown_all=False, q=None, total=None):
            """An open list against the reference rows in the order they should be on screen."""
            n = len(want)
            show = want if shown_all else want[:PAGE]
            ok = ck([x for x in s["rows"]] == [x["t"] for x in show], "%s: %d rows on screen, first %s; the reference has %d, first %s" % (where, s["n_rows"], s["rows"][:5], len(show), [x["t"] for x in show[:5]]))
            head = "%s%s %s" % (P.fint(n), (" of %s" % P.fint(total)) if (total is not None and total != n) else "", unit if (total if total is not None else n) == 1 else unit + "s")
            ok &= ck((s["count"] or "").startswith(head + " · "), "%s: the count line reads '%s', expected to start '%s'" % (where, (s["count"] or "")[:70], head))
            ok &= ck((s["more"] is None) == (shown_all or n <= PAGE) and (s["more"] is None or s["more"] == "SHOW ALL %s ROWS" % P.fint(n)), "%s: the show-all button reads %s for %d rows" % (where, s["more"], n))
            if sort:
                ok &= ck(s["sorted"] == [list(sort)], "%s: the heading arrow is on %s, expected %s" % (where, s["sorted"], sort))
            return ok

        page.goto("%s/strong.html?nocache=%d" % (base, stamp + width), wait_until="domcontentloaded")
        try:
            page.wait_for_function("() => document.querySelector('#names-tbl tbody tr td a.tk') && (document.getElementById('jh-tearsheet') || {dataset: {}}).dataset.state === 'ready' && document.querySelector('#jh-tearsheet .jts-ch[data-jts-open]')", timeout=90000)
        except Exception:  # noqa: BLE001
            pass
        s = full(1200)
        kit = js("() => (window.JHTearsheet || {}).version || null")
        if not ck(s["state"] == "ready" and s["view"] and P.semver(kit) >= (1, 6, 0) and any(c[1] for c in s["cards"]), "%s px: the sheet is not the one that opens lists: state %s, kit %s, cards %s, errors %s" % (tag, s["state"], kit, s["cards"][:3], errors[:2])):
            shots["failed_" + tag] = page.screenshot(type="jpeg", quality=45)
            ctx.close()
            continue
        ref = ref_for(s["view"])
        if not ck(ref is not None, "%s px: the page's feeds were not seen on the wire (%s)" % (tag, {k: len(x) for k, x in got.items()})):
            ctx.close()
            continue
        red, live = ref["red"], ref["mode"] == "live"
        r.log("kit %s | %s %s, SPY %+.2f%% | table generated %s | bars as of %s" % (kit, ref["mode"], ref["session"], ref["spy"], s["view"]["table_generated"], s["view"]["bars_as_of"]))

        # ------------------------------------------------- every card, every slice and the ring open the whole list
        opened = []
        for fund in ("b1", "sp", "all", "etf"):
            click_sheet('[data-jts-ctl="%s"]' % fund)
            s = full()
            L = sheet_lists(ref_for(s["view"]), fund)
            ck(s["ctl"] == fund and [c[0] for c in s["cards"]] == ["ring"] + L["cards"] and all(c[1] and c[3] and c[3].startswith("VIEW ALL") for c in s["cards"]),
               "%s px, scope %s: the cards are %s; each should open (reference cards %s)" % (tag, fund, s["cards"], L["cards"]))
            ck(s["legend"] == [L["slices"][o] for o in ("up", "flat", "down_less", "down_more") if any(x["out"] == o for x in L["rows"])], "%s px, scope %s: the ring's rows open %s" % (tag, fund, s["legend"]))
            line = []
            targets = [("ring", ".jts-ringcol .jts-ch", "all")] + [(o, '.jts-leg tr[data-jts-open="%s"]' % L["slices"][o], L["slices"][o]) for o in ("up", "flat", "down_less", "down_more") if any(x["out"] == o for x in L["rows"])] \
                + [(k, '[data-jts-list="%s"] .jts-ch' % k, k) for k in L["cards"] if k in L["lists"]]
            for name, sel, key in targets:
                click_sheet(sel)
                s = full()
                L = sheet_lists(ref_for(s["view"]), fund)
                want = L["lists"][key]
                ok = ck(s["open"] == key and s["key"] == key, "%s px, scope %s: a click on %s opened '%s', expected '%s'" % (tag, fund, name, s["open"], key))
                ok &= list_ok(s, "%s px, scope %s, %s" % (tag, fund, key), want, L["unit"])
                ok &= ck([c[0] for c in s["chips"] if c[1] == "true"] == [key] and dict((c[0], c[2]) for c in s["chips"]).get(key) == P.fint(len(want)), "%s px, scope %s, %s: the category buttons %s" % (tag, fund, key, s["chips"]))
                ok &= ck(s["bad"] == 0 and s["ox"] <= 0, "%s px, scope %s, %s: bad text %s, sideways overflow %s" % (tag, fund, key, s["bad"], s["ox"]))
                line.append("%s %s%s" % (key, P.fint(len(want)), "" if ok else " FAIL"))
                click_sheet('[data-jts-act="close"]')
                s = full(160)
                ck(s["open"] == "" and s["key"] is None and len(s["cards"]) > 3, "%s px, scope %s: BACK did not return to the sheet after %s" % (tag, fund, key))
            opened.append("%s: %s" % (fund, ", ".join(line)))
            r.log("scope %s - each opened to its whole list (rows on screen = the reference, in order): %s" % (fund, ", ".join(line)))
        click_sheet('[data-jts-ctl="b1"]')

        # ------------------------------------------------------- one list, ranked by any column, both ways (b1, up)
        click_sheet('[data-jts-list="up"] .jts-ch')
        s = full()
        L = sheet_lists(ref_for(s["view"]), "b1")
        up = L["lists"]["up"]
        desk_cols = ["t", "name", "sector", "industry", "mcap", "px", "ret", "x", "out", "rank", "dump", "fo", "pos", "vwx", "rv"]
        phone_cols = ["t", "ret", "x", "mcap", "industry", "sector", "out", "rank", "dump", "name", "px", "fo", "pos", "vwx", "rv"]     # the answer beside the ticker, the long text last
        ck(s["head"] == (phone_cols if mobile else desk_cols) and "Industry" in s["heads"] and "Mkt value" in s["heads"] and s["sorted"] == [["ret", "descending"]],
           "%s px: the open list's columns %s (%s), ranked %s" % (tag, s["head"], s["heads"], s["sorted"]))
        if not mobile:
            ck(s["wrap"] and s["wrap"][0] <= s["wrap"][1], "1440 px: the open list is %s px wide in a %s px box" % tuple(s["wrap"][:2] if s["wrap"] else (None, None)))
        ck(0 <= js("() => Math.round(document.querySelector('#jh-tearsheet .jts-full').getBoundingClientRect().top)") <= height * 0.56, "%s px: the opened list is not where the reader is looking" % tag)
        r.log("'%s' opened: %s" % (s["title"], s["count"]))
        r.log("  columns: %s" % ", ".join(s["heads"]))
        COLS = (("mcap", "n", lambda x: x.get("mcap")), ("industry", "s", lambda x: x.get("industry")), ("sector", "s", lambda x: x.get("sector") or "Unclassified"), ("ret", "n", lambda x: x["ret"]),
                ("rank", "n", lambda x: x["rank"]), ("dump", "n", lambda x: x["dumpkey"]), ("name", "s", lambda x: x.get("name")), ("t", "s", lambda x: x["t"]))
        for cid, kind, get in COLS:
            for turn in (0, 1):
                js("(c) => document.querySelector('#jh-tearsheet .jts-ft th[data-jts-sort=\"' + c + '\"]').click()", cid)
                s = full(200)
                up = sheet_lists(ref_for(s["view"]), "b1")["lists"]["up"]
                first_desc = kind != "s"                 # the first click on a column: text A to Z, numbers highest first; the second reverses it
                desc = first_desc if turn == 0 else not first_desc
                want = full_sorted(up, get, kind, desc)
                ok = list_ok(s, "%s px, ranked by %s %s" % (tag, cid, "down" if desc else "up"), want, "stock", sort=(cid, "descending" if desc else "ascending"))
                if ok and cid in ("mcap", "industry", "sector") and not mobile:
                    i = {"industry": 0, "sector": 1, "mcap": 2}[cid]
                    r.log("  ranked by %s, %s: %s" % ({"mcap": "Mkt value", "industry": "Industry", "sector": "Sector"}[cid], ("highest first" if desc else "lowest first") if kind == "n" else ("Z to A" if desc else "A to Z"),
                                                    " | ".join("%s %s" % (t, c[i]) for t, c in list(zip(s["rows"], s["cells"]))[:7])))
        # market value, largest first: the screenshot he asked for
        js("(c) => document.querySelector('#jh-tearsheet .jts-ft th[data-jts-sort=\"' + c + '\"]').click()", "mcap")
        s = full(220)
        up = sheet_lists(ref_for(s["view"]), "b1")["lists"]["up"]
        by_cap = full_sorted(up, lambda x: x.get("mcap"), "n", True)
        list_ok(s, "%s px, ranked by market value again" % tag, by_cap, "stock", sort=("mcap", "descending"))
        ck(P.near(s["first"][s["head"].index("ret") + 1], by_cap[0]["ret"], 2) and s["first"][s["head"].index("industry") + 1] == (by_cap[0].get("industry") or "")
           and s["first"][s["head"].index("out") + 1] == OUT_WORD[by_cap[0]["out"]], "%s px: the first row prints %s; the reference is %s %s %.4f" % (tag, s["first"], by_cap[0]["t"], by_cap[0].get("industry"), by_cap[0]["ret"]))
        page.evaluate("() => window.scrollTo(0, 0)")
        shots["full_by_mcap_" + tag] = page.screenshot(type="jpeg", quality=50, full_page=True, clip={"x": 0, "y": max(0, s["page_y"] - 8), "width": width, "height": min(s["sheet_h"] + 16, 1250 if not mobile else 1500)})
        # show all, find, a fresh feed, CSV
        if len(by_cap) > PAGE:
            click_sheet('[data-jts-act="all"]')
            s = full(400)
            list_ok(s, "%s px, show all" % tag, by_cap, "stock", sort=("mcap", "descending"), shown_all=True)
            r.log("  SHOW ALL: %d rows on screen" % s["n_rows"])
        page.fill("#jh-tearsheet [data-jts-find]", "bank")
        s = full(500)
        found = [x for x in by_cap if "bank" in (" ".join(str(x.get(k) or "") for k in ("name", "sector", "industry"))).lower() or x["t"].lower().startswith("bank")]
        list_ok(s, "%s px, find 'bank'" % tag, found, "stock", sort=("mcap", "descending"), total=len(by_cap))
        r.log("  find 'bank': %s (%s)" % (s["count"].split(" · ")[0], ", ".join(s["rows"][:8])))
        # the desk hands the sheet a fresh feed every minute: the open list, its ranking and the text being typed stay
        page.focus("#jh-tearsheet [data-jts-find]")
        js("() => window.dispatchEvent(new CustomEvent('jh:strength-doc', { detail: window.JHStrengthDoc }))")
        s2 = full(300)
        ck(s2["open"] == "up" and s2["find"] == "bank" and s2["focus"] and s2["sorted"] == [["mcap", "descending"]] and s2["rows"] == s["rows"],
           "%s px: a fresh feed moved the open list: open %s, find %s, focus %s, ranked %s, %d rows" % (tag, s2["open"], s2["find"], s2["focus"], s2["sorted"], s2["n_rows"]))
        if not mobile:
            try:
                with page.expect_download(timeout=20000) as dl:
                    click_sheet('[data-jts-act="csv"]')
                lines = Path(dl.value.path()).read_text(encoding="utf-8").split("\n")
                ck(len(lines) == len(found) + 1 and lines[0].startswith("Ticker,Name,Sector,Industry,Mkt value,Last,Move,vs SPY,Against SPY,") and [ln.split(",")[0] for ln in lines[1:6]] == [x["t"] for x in found[:5]]
                   and dl.value.suggested_filename == "strength-up-b1-%s%s.csv" % (ref["session"], "-live" if live else ""), "the CSV %s holds %d lines for %d rows, first %s" % (dl.value.suggested_filename, len(lines), len(found), lines[:2]))
                r.log("  DOWNLOAD CSV: %s, %d rows + heading, in the order on screen (%s)" % (dl.value.suggested_filename, len(lines) - 1, ", ".join(ln.split(",")[0] for ln in lines[1:6])))
            except Exception as exc:  # noqa: BLE001
                ck(False, "the CSV of the open list did not download: %s" % str(exc)[:120])
        page.fill("#jh-tearsheet [data-jts-find]", "")
        full(400)
        # a category button moves to another list without going back; Escape closes
        click_sheet('.jts-fnav [data-jts-open="repeat"]')
        s = full()
        list_ok(s, "%s px, the HELD EVERY DUMP DAY button" % tag, sheet_lists(ref_for(s["view"]), "b1")["lists"]["repeat"], "stock")
        ck(s["open"] == "repeat" and s["sorted"] == [["x", "descending"]], "%s px: the category button opened %s ranked %s" % (tag, s["open"], s["sorted"]))
        page.focus("#jh-tearsheet [data-jts-find]")
        page.keyboard.press("Escape")
        s = full(200)
        ck(s["open"] == "" and len(s["cards"]) > 3, "%s px: Escape did not return to the sheet" % tag)

        # ---------------------------------------------- industries in full, and one of them handed to the table below
        click_sheet('[data-jts-list="industries-holding"] .jts-ch')
        s = full()
        refv = ref_for(s["view"])
        L = sheet_lists(refv, "b1")
        inds = industries(refv, L["rows"], refv["rules"].get("industry_min") or 5)
        ck(s["open"] == "industries-holding" and s["head"] == (["name", "held", "n", "med", "mcap", "up", "beat", "more", "sector"] if mobile else ["name", "sector", "n", "held", "up", "beat", "more", "med", "mcap"]) and s["n_rows"] == len(inds) and s["rows"] == [g["name"] for g in inds]
           and (s["count"] or "").startswith("%d industries · " % len(inds)), "%s px: industries in full: %d rows, first %s, '%s'; the reference has %d, first %s" % (tag, s["n_rows"], s["rows"][:3], (s["count"] or "")[:50], len(inds), [g["name"] for g in inds[:3]]))
        r.log("industries in full: %s" % s["count"])
        js("(c) => document.querySelector('#jh-tearsheet .jts-ft th[data-jts-sort=\"' + c + '\"]').click()", "mcap")
        s = full(220)
        by_cap_i = full_sorted(inds, lambda g: g["mcap"], "n", True)
        ck(s["rows"] == [g["name"] for g in by_cap_i] and s["sorted"] == [["mcap", "descending"]], "%s px: industries by market value: %s; the reference %s" % (tag, s["rows"][:4], [g["name"] for g in by_cap_i[:4]]))
        r.log("  ranked by Mkt value, highest first: %s" % " | ".join("%s %s" % (a, c[2]) for a, c in list(zip(s["rows"], s["cells"]))[:6]))
        js("(c) => document.querySelector('#jh-tearsheet .jts-ft th[data-jts-sort=\"' + c + '\"]').click()", "name")
        s = full(220)
        ck(s["rows"] == sorted((g["name"] for g in inds), key=lambda x: x.lower()) and s["sorted"] == [["name", "ascending"]], "%s px: industries A to Z: %s" % (tag, s["rows"][:4]))
        if not mobile:
            page.evaluate("() => window.scrollTo(0, 0)")
            shots["industries_" + tag] = page.screenshot(type="jpeg", quality=48, full_page=True, clip={"x": 0, "y": max(0, s["page_y"] - 8), "width": width, "height": min(s["sheet_h"] + 16, 900)})
        pick = next((g for g in by_cap_i if g["name"] == "Semiconductors"), by_cap_i[0])
        js("(n) => Array.from(document.querySelectorAll('#jh-tearsheet [data-jts-pick]')).find(b => b.textContent.trim() === n).click()", pick["name"])
        t = names(1300)
        want = P.ordered(pick["members"], "x")
        ck(t["industry"] == pick["name"] and t["kind"] == "stock" and t["scope"] == "b1" and t["out"] == "all" and [x for x in t["rows"] if x] == [x["t"] for x in want[:PAGE]]
           and t["meta"].startswith("%s of " % P.fint(len(want))) and -4 <= t["top"] < t["vh"] * 0.6, "%s px: a click on %s in the sheet shows %s in the table (industry '%s', %s, top %s px); the reference lists %s" % (
               tag, pick["name"], t["rows"][:5], t["industry"], t["meta"][:30], t["top"], [x["t"] for x in want[:5]]))
        ck(set(x for x in t["ind"] if x) == {pick["name"]}, "%s px: the table's Industry column under that filter reads %s" % (tag, sorted(set(t["ind"]))[:3]))
        r.log("a click on '%s' in the sheet -> the table below: %s | %s" % (pick["name"], t["meta"].split(" · ")[0], ", ".join(t["rows"][:10])))
        if not mobile:
            shots["picked_" + tag] = page.screenshot(type="jpeg", quality=48)
        # the Industry menu and the Industry column of the table itself
        ck(not t["industry_hidden"] and t["n_industries"] >= 30 and "industry=" in t["hash"], "%s px: the Industry menu: hidden %s, %d industries, address %s" % (tag, t["industry_hidden"], t["n_industries"], t["hash"]))
        page.select_option("#names-industry", "")
        t = names()
        base_rows = P.scope_rows(ref_for(t["view"]), "stock", "b1", "all", "all", "counted")
        for turn, desc in ((0, False), (1, True)):
            js("() => document.querySelector('#names-tbl th[data-sort=\"ind\"]').click()")
            t = names(350)
            base_rows = P.scope_rows(ref_for(t["view"]), "stock", "b1", "all", "all", "counted")
            want = P.ordered(base_rows, "ind", desc)
            ck([x for x in t["rows"] if x] == [x["t"] for x in want[:PAGE]] and t["sorted"] == [["ind", "descending" if desc else "ascending"]],
               "%s px: the table ranked by Industry %s shows %s; the reference %s" % (tag, "Z to A" if desc else "A to Z", t["rows"][:5], [x["t"] for x in want[:5]]))
            r.log("the table ranked by Industry, %s: %s" % ("Z to A" if desc else "A to Z", " | ".join("%s %s" % (a, b) for a, b in list(zip(t["rows"], t["ind"]))[:6])))
        js("() => document.querySelector('#names-tbl th[data-sort=\"mcap\"]').click()")
        t = names(350)
        want = P.ordered(P.scope_rows(ref_for(t["view"]), "stock", "b1", "all", "all", "counted"), "mcap", True)
        ck([x for x in t["rows"] if x] == [x["t"] for x in want[:PAGE]] and t["sorted"] == [["mcap", "descending"]], "%s px: the table ranked by market value shows %s; the reference %s" % (tag, t["rows"][:5], [x["t"] for x in want[:5]]))
        if not mobile:
            ck(t["fit"] <= 0, "1440 px: the table with its Industry column is %s px wider than its box" % t["fit"])
        # a row of the group table above the names filters them the same way: a sector, an industry, an ETF class
        for group, field, kind in (("sector", "sector", "stock"), ("industry", "industry", "stock"), ("class", "sector", "etf")):
            js("(g) => document.querySelector('#today-group button.jh-chip[data-v=\"' + g + '\"]').click()", group)
            t = names(300)
            ck(t["group"] == group and len(t["groups"]) >= (8 if group != "industry" else 30), "%s px: the %s table has %d rows" % (tag, group, len(t["groups"])))
            name = t["groups"][1]
            js("(n) => Array.from(document.querySelectorAll('#today-groups [data-pick]')).find(b => b.getAttribute('data-pick') === n).click()", name)
            t = names(1300)
            refv = ref_for(t["view"])
            want = P.ordered(P.scope_rows(refv, kind, "b1", "all", "all", "counted", **({"sector": name} if field == "sector" else {})), "x")
            if field == "industry":
                want = [x for x in want if x.get("industry") == name]
            ck(t["kind"] == kind and (t["sector"] if field == "sector" else t["industry"]) == name and [x for x in t["rows"] if x] == [x["t"] for x in want[:PAGE]] and t["meta"].startswith("%s of " % P.fint(len(want))) and -4 <= t["top"] < t["vh"] * 0.6,
               "%s px: a click on the %s '%s' shows %s (%s, top %s); the reference lists %d, first %s" % (tag, group, name, t["rows"][:4], t["meta"][:28], t["top"], len(want), [x["t"] for x in want[:4]]))
            r.log("a click on the %s row '%s' -> %s | %s" % (group, name, t["meta"].split(" · ")[0], ", ".join(t["rows"][:8])))
            if group == "class":
                ck(t["industry_hidden"], "%s px: funds have no industry, and the Industry menu is still shown" % tag)
        js("() => document.getElementById('names-reset').click()")
        js("() => document.querySelector('#today-group button.jh-chip[data-v=\"sector\"]').click()")
        t = names(400)
        ck(t["ox"] <= 0 and not errors, "%s px: sideways overflow %s, page errors %s" % (tag, t["ox"], errors[:3]))
        r.log("sideways overflow %s px | page errors %d | feed responses seen %s" % (t["ox"], len(errors), {k.split("/")[-1]: len(x) for k, x in got.items()}))
        if not facts:
            facts.update(mode=ref["mode"], session=ref["session"], spy=round(ref["spy"], 2), opened=opened, n_industries=len(inds), kit=kit)
        ctx.close()
    return facts


def main():
    from ops_report import report
    with report("ops_6867_strength_full_lists_live_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6867 - every category of the Strength desk opens in full and ranks by any column, live %s" % now.isoformat(timespec="minutes"))
        fails, shots, facts = [], {}, {}

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL %s" % msg)
            return bool(cond)

        sync_playwright = P.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = P.launch(p, r.log)
            prove(browser, r, ck, shots, BASE, int(time.time()), None, facts)
            browser.close()
        if facts:
            r.kv(failures=len(fails), mode=facts["mode"], session=facts["session"], spy=facts["spy"], kit=facts["kit"], industries=facts["n_industries"])
        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        if fails:
            r.fail("%d checks failed: %s" % (len(fails), " | ".join(fails)[:1800]))
            sys.exit(1)
        r.ok("LIVE PROOF: every card, ring slice and the ring open their whole list in all four scopes; an open list ranks by market value, industry and every other column both ways, is found, shown in full and saved as CSV; industries open in full and hand their names to the table, which has its own Industry column and menu")


if __name__ == "__main__":
    main()
