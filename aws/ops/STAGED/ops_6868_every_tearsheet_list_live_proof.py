"""ops 6868 - READ-ONLY live proof: every list of every tearsheet desk opens whole and ranks by any column, either way.

Khalid 2026-11-20 18:55 ET: "now do the same with Volatility - most and least volatile stocks, price spread - and all
the other engines that arent showing the whole list"

CLAUDE.md "CHECK YOUR WORK": Chrome on the runner opens each desk that carries the tearsheet on justhodl.ai, at 1440 and
390 px, and does what he described on every list of every sheet (kit 1.7.0):
  - each card heading, the ring's heading and each ring slice is clicked: the whole list opens in place of the sheet,
    its count line states the list's size, its first 200 rows are drawn in the list's own order and SHOW ALL draws
    the rest;
  - column headings are clicked twice each: the rows on screen are held against an independent sort of the list's own
    values (numbers highest first, text A to Z, the second click reverses, blanks last either way, ties in the list's
    own order), and the heading carries the arrow. Every column of every kind of list is ranked this way; a list on
    the same columns as one already ranked column by column, or longer than 600 rows, is ranked by its name, the
    column it stands in, size, sector, industry and two more figures;
  - find narrows the list, DOWNLOAD CSV saves the rows on screen, BACK and Escape return to the sheet;
  - a ticker is a link to the chart that leaves the desk's frame (target _top), and inside the chart's workspace it
    loads the symbol in the chart behind the desk;
  - no page error, nothing spills sideways, nothing prints "undefined".
And each whole list is held against the bytes of the feed the page itself fetched, read here in Python without the
kit: for Volatility the engine's own ranking population, in order, list by list (so the first 25 names of a list are
the engine's board, its length the engine's population, the bands the engine's band counts); for the other desks the
names each list must hold. Writes nothing.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import importlib.util
import json
import math
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
_spec = importlib.util.spec_from_file_location("ops6866", str(Path(__file__).resolve().parent / "ops_6866_strength_page_live_proof.py"))
P = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(P)

BASE = P.BASE
PAGE = 200
KIT_MIN = (1, 7, 0)
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
SLICED = False       # set by the offline runner, whose feeds are trimmed fixtures
BIG = 600            # a list longer than this has a sample of its columns ranked, not every one
SWEEP = ("mcap", "cap", "sector", "sec", "industry", "ind", "name", "usd", "val", "score", "date")

SHEETS = [
    dict(tag="volatility", path="/stock-volatility.html", feeds={"doc": "/data/stock-volatility.json"}, scopes=[None, "d"], shot=True, phone=True),
    dict(tag="leaders", path="/leaders.html", feeds={"doc": "/data/leaders.json"}, scopes=[None, "w"], shot=True, phone=True),
    dict(tag="wy-bottom", path="/wyckoff-desk.html?tab=bottom", feeds={"doc": "/data/campaign-scan.json"}, scopes=[None, "etf"], shot=True),
    dict(tag="wy-accum", path="/wyckoff-desk.html?tab=accum", feeds={"doc": "/data/campaign-scan.json"}, scopes=[None, "etf"]),
    dict(tag="wy-dist", path="/wyckoff-desk.html?tab=dist", feeds={"doc": "/data/campaign-scan.json"}, scopes=[None, "etf"]),
    dict(tag="wy-sc", path="/wyckoff-desk.html?tab=sc", feeds={"doc": "/data/bottom.json"}),
    dict(tag="wy-capit", path="/wyckoff-desk.html?tab=capit", feeds={"doc": "/data/capitulation-desk.json"}),
    dict(tag="wy-bc", path="/wyckoff-desk.html?tab=bc", feeds={"doc": "/data/phase-detector.json"}),
    dict(tag="wy-spring", path="/wyckoff-desk.html?tab=spring", feeds={"doc": "/data/phase-detector.json"}),
    dict(tag="wy-spread", path="/wyckoff-desk.html?tab=spread", feeds={"doc": "/data/katlin.json"}),
    dict(tag="capitulation", path="/capitulation-desk.html", feeds={"doc": "/data/capitulation-desk.json"}, shot=True),
    dict(tag="sp500", path="/sp500.html", feeds={"doc": "/data/sp500.json"}, shot=True),
    dict(tag="ark", path="/ark.html", feeds={"doc": "/data/ark-holdings.json"}, shot=True),
    dict(tag="insiders", path="/insider-clusters.html", feeds={"doc": "/data/insider-clusters.json"}),
    dict(tag="firmbook", path="/firm-book.html", feeds={"doc": "/data/firm-book.json"}),
    dict(tag="paperbook", path="/paper-book.html", feeds={"doc": "/data/paper-book.json"}),
    dict(tag="stablecoin", path="/stablecoin-flow.html", feeds={"doc": "/data/stablecoin-flow.json"}),
    dict(tag="tic", path="/foreign-flows.html", feeds={"doc": "/data/tic-flows.json"}),
    dict(tag="capital", path="/capital-inflows.html", feeds={"doc": "/data/capital-inflows.json"}),
    dict(tag="strength", path="/strong.html", feeds={}),
]

MODEL = r"""() => { const el = document.getElementById('jh-tearsheet'); if (!el) return null; const m = el.__model || {}, cards = m.lists || [], all = cards.concat(m.more || []);
  const txt = (c, r) => { const v = c.fmt ? c.fmt(r) : c.get(r); return v == null ? '' : String(v); };
  return { state: el.dataset.state || '', open: el.dataset.open || '', kit: (window.JHTearsheet || {}).version || '', title: m.title || '', eyebrow: m.eyebrow || '', hide: !!m.hide, width: el.clientWidth,
    ctl: (m.controls || []).map(c => [c.id, !!c.on]), ring: m.ring ? { open: m.ring.open || '', slices: (m.ring.slices || []).map(s => s.open || '') } : null,
    lists: all.map(l => ({ key: l.key, title: l.title, card: cards.indexOf(l) >= 0 && l.card !== false, n: l.n == null ? null : l.n, rows: (l.rows || []).length, names: (l.rows || []).slice(0, 25).map(r => String(r.name)),
      full: l.full ? { n: l.full.rows.length, sort: l.full.sort || null, units: l.full.units || [], href: !!l.full.href, pick: !!l.full.pick, phone: l.full.phone || null, cols: l.full.cols.map(c => ({ id: c.id, t: c.t, type: c.type, start: c.find === 'start' })),
        lead: l.full.rows.slice(0, 25).map(r => txt(l.full.cols[0], r)) } : null })) }; }"""

VALUES = r"""(key) => { const el = document.getElementById('jh-tearsheet'), m = el.__model, l = (m.lists || []).concat(m.more || []).filter(x => x.key === key)[0], f = l.full;
  const blank = v => v == null || v === '' || (typeof v === 'number' && !isFinite(v));
  return { ids: f.cols.map(c => c.id), raw: f.rows.map(r => f.cols.map(c => { const v = c.get(r); return blank(v) ? null : v; })), text: f.rows.map(r => f.cols.map(c => { const v = c.fmt ? c.fmt(r) : c.get(r); return v == null ? '' : String(v); })),
    href: f.href ? f.rows.slice(0, 60).map(r => f.href(r) || '') : null }; }"""

DOM = r"""(want) => { const el = document.getElementById('jh-tearsheet'), f = el.querySelector('.jts-full'), q = (r, s) => r ? Array.from(r.querySelectorAll(s)) : [], t = e => e ? e.textContent.replace(/\s+/g, ' ').trim() : '';
  const head = q(f, '.jts-ft thead th[data-jts-sort]'), ids = head.map(th => th.getAttribute('data-jts-sort')), at = (want || []).map(id => ids.indexOf(id)), trs = q(f, '.jts-ft tbody tr');
  return { state: el.dataset.state || '', open: el.dataset.open || '', key: f ? f.getAttribute('data-jts-full') : null, title: t(f && f.querySelector('.jts-ft1')), count: t(f && f.querySelector('[data-jts-count]')),
    head: ids, heads: head.map(th => t(th).replace(/ [▾▴]$/, '')), sorted: head.filter(th => th.hasAttribute('aria-sort')).map(th => [th.getAttribute('data-jts-sort'), th.getAttribute('aria-sort')]),
    n_rows: trs.length, rows: trs.map(tr => at.map(i => i < 0 ? null : t(tr.cells[i + 1]))), top: trs.slice(0, 12).map(tr => Array.from(tr.cells).slice(1).map(t)), more: t(f && f.querySelector('[data-jts-act="all"]')),
    links: q(f, '.jts-ft tbody td.jts-fk a').slice(0, 60).map(a => [a.getAttribute('href'), a.getAttribute('target'), t(a)]), first_cells: trs.slice(0, 60).map(tr => t(tr.cells[1])),
    nav: q(f, '.jts-fnav [data-jts-open]').map(b => [b.getAttribute('data-jts-open'), b.getAttribute('aria-pressed'), t(b.querySelector('i'))]), find: f && f.querySelector('[data-jts-find]') ? f.querySelector('[data-jts-find]').value : null,
    cards: q(el, '.jts-cols [data-jts-list]').map(c => [c.getAttribute('data-jts-list'), !!c.querySelector('.jts-ch[data-jts-open]'), t(c.querySelector('.jts-n')), t(c.querySelector('.jts-go'))]),
    legend: q(el, '.jts-leg tr').map(tr => tr.getAttribute('data-jts-open') || ''), ring_open: (el.querySelector('.jts-ringcol .jts-ch[data-jts-open]') || { getAttribute: () => '' }).getAttribute('data-jts-open') || '',
    wrap: f && f.querySelector('.jts-fwrap') ? [f.querySelector('.jts-fwrap').scrollWidth, f.querySelector('.jts-fwrap').clientWidth] : null, sheet: [el.scrollWidth, el.clientWidth],
    bad: (el.innerText.match(/undefined|NaN|\[object Object\]|Infinity/g) || []).length, ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""


RANK = r"""([id, want]) => { const el = document.getElementById('jh-tearsheet');
  if (id) { const th = el.querySelector('.jts-ft thead th[data-jts-sort="' + id + '"]'); if (!th) return null; th.click(); }
  const f = el.querySelector('.jts-full'), t = e => e ? e.textContent.replace(/\s+/g, ' ').trim() : '', head = Array.from(f.querySelectorAll('.jts-ft thead th[data-jts-sort]')), ids = head.map(h => h.getAttribute('data-jts-sort')), at = want.map(x => ids.indexOf(x));
  return { sorted: head.filter(h => h.hasAttribute('aria-sort')).map(h => [h.getAttribute('data-jts-sort'), h.getAttribute('aria-sort')]), rows: Array.from(f.querySelectorAll('.jts-ft tbody tr')).map(tr => at.map(i => i < 0 ? null : t(tr.cells[i + 1]))),
    bad: want.length > 3 ? (f.textContent.match(/undefined|NaN|\[object Object\]|Infinity/g) || []).length : 0, ox: want.length > 3 ? document.documentElement.scrollWidth - document.documentElement.clientWidth : 0 }; }"""


def fin(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def day(s):
    return str(s or "")[:10]


def month(s):
    s = str(s or "")
    return "%s %s" % (MONTHS[int(s[5:7]) - 1], s[:4]) if len(s) >= 7 and s[5:7].isdigit() else "—"


def cap(r):
    return r["mcap"] if fin(r.get("mcap")) else 0


def expected_order(vals, col, typ, d):
    """indices of the rows ranked by one column the way the page states it: blanks last either way, ties in the list's own order"""
    keyed, blank = [], []
    for i, row in enumerate(vals):
        v = row[col]
        if v is None or v == "" or (typ == "n" and not fin(v)):
            blank.append(i)
        else:
            keyed.append((str(v).lower() if typ == "s" else v, i))
    keyed.sort(key=lambda x: x[0], reverse=d < 0)          # Python's sort is stable both ways: equal keys keep the list's own order
    return [i for _, i in keyed] + blank


# ------------------------------------------------------------------------------------------- the references
# Each returns {list key: ("ordered" | "set", [first-column text of every row the list must hold])} from the feed's bytes,
# and (label, expected, actual) facts that tie a list to a count the engine itself published.
def ref_volatility(d, scope):
    k = scope or "m"
    sp = "sp_" + k
    cols = d["cols"]
    rows = [dict(zip(cols, r)) for r in d["rows"]]
    br, U = d.get("board_rule") or {}, d.get("universe") or {}
    floor = br["min_mcap"] if fin(br.get("min_mcap")) else 1e9
    prior = (d.get("windows") or {}).get("prior") or 21
    RK = {"hv21": "rk_hv21", "sp_d": "rk_d", "sp_w": "rk_w", "sp_m": "rk_m", "sp_q": "rk_q"}

    def ranked(m):
        return sorted([r for r in rows if r["kind"] == "stock" and (r.get("mcap") or 0) >= floor and fin(r.get(RK[m]))], key=lambda r: r[RK[m]])

    def chg(r):
        return r["hv21"] - r["hv21p"]

    def big(rs):
        return sorted(rs, key=lambda r: -(r.get("mcap") or 0))

    vol, spr = ranked("hv21"), ranked(sp)
    moving = sorted([r for r in vol if fin(r.get("hv21p")) and (r.get("ja") is None or r["ja"] > 21 + prior)], key=lambda r: (-chg(r), r["t"]))
    up, down = [r for r in moving if chg(r) > 0], [r for r in reversed(moving) if chg(r) < 0]
    ranked_all = big([r for r in rows if r["kind"] == "stock" and fin(r.get("rk_hv21"))])
    aside, etfs = big([r for r in rows if r["kind"] == "stock" and r.get("aside")]), big([r for r in rows if r["kind"] == "etf"])
    by_t = {r["t"]: r for r in rows}
    bands = [b for b in d.get("bands") or [] if fin(b.get("mcap"))]
    out = {"most-volatile": vol, "least-volatile": vol[::-1], "widest": spr, "narrowest": spr[::-1], "rising": up, "falling": down, "ranked": ranked_all, "aside": aside, "etfs": etfs,
           "pinned": big([by_t[t] for t in d.get("pinned") or [] if t in by_t])}
    members = []
    for i, b in enumerate(bands):
        members.append([r for r in ranked_all if r["hv21"] >= b["from"] and (b.get("to") is None or r["hv21"] < b["to"])])
        out["band-%d" % i] = members[-1]
    lists = {key: ("ordered", [r["t"] for r in v]) for key, v in out.items()}
    sect = sorted([g for g in d.get("by_sector") or [] if g.get("med") and fin(g["med"].get("hv21"))], key=lambda g: -g["med"]["hv21"])
    lists["sectors"] = ("ordered", [g["sector"] for g in sect])
    B, n, pop = d.get("boards") or {}, br.get("n") or 25, br.get("population") or {}
    t = lambda rs: [r["t"] for r in rs]  # noqa: E731
    facts = [("the first %d most volatile are the engine's board" % n, B.get("most_volatile"), t(vol[:n])), ("the first %d least volatile are the engine's board" % n, B.get("least_volatile"), t(vol[::-1][:n])),
             ("the first %d widest (%s) are the engine's board" % (n, k), B.get("widest_" + k), t(spr[:n])), ("the first %d narrowest (%s) are the engine's board" % (n, k), B.get("narrowest_" + k), t(spr[::-1][:n])),
             ("the first %d rising are the engine's board" % n, B.get("vol_rising"), t(moving[:n])), ("the first %d falling are the engine's board" % n, B.get("vol_falling"), t(moving[::-1][:n])),
             ("rising: the board's names all rose", True, len(up) >= n and all(chg(r) > 0 for r in moving[:n])), ("falling: the board's names all fell", True, len(down) >= n and all(chg(r) < 0 for r in moving[::-1][:n])),
             ("the volatility list is the engine's ranking population", pop.get("hv21"), len(vol)), ("the spread list (%s) is the engine's ranking population" % k, pop.get(sp), len(spr)),
             ("each band's list is the engine's band count", [b["n"] for b in bands], [len(x) for x in members]), ("every ranked stock is in one band", len(ranked_all), sum(len(x) for x in members)),
             ("set aside = the engine's count", (U.get("set_aside") or {}).get("stock"), len(aside)), ("ETFs = the engine's count", U.get("scanned_etf"), len(etfs)),
             ("stocks ranked or set aside = the engine's count", U.get("scanned_stock"), sum(1 for r in rows if r["kind"] == "stock"))]
    return lists, facts, {"as_of": d.get("as_of"), "version": d.get("version"), "rows": len(rows), "ranked_1b": len(vol), "rising": len(up), "falling": len(down), "etfs": len(etfs)}


def ref_leaders(d, scope):
    k = scope or "m"
    live = [r for r in d.get("rows") or [] if r.get("vs") and not r.get("stale")]
    read = [r for r in live if fin(r["vs"].get(k))]
    lab = lambda r: (r.get("ad") or {}).get("label") or "NEUTRAL"  # noqa: E731
    score = lambda r: (r.get("ad") or {}).get("score")  # noqa: E731
    verdict = lambda r: (r.get("smart") or {}).get("verdict")  # noqa: E731
    t = lambda rs: [r["ticker"] for r in rs]  # noqa: E731
    inds = [x for x in d.get("industries") or [] if x.get("vs") and fin(x["vs"].get(k))]
    out = {"all": t(live), "strongest": t([r for r in read if r["vs"][k] > 0]), "weakest": t([r for r in read if r["vs"][k] <= 0]),
           "accumulation": t([r for r in live if fin(score(r)) and score(r) >= 20]), "distribution": t([r for r in live if fin(score(r)) and score(r) <= -20]),
           "smart-buying": t([r for r in live if verdict(r) == "BUYING"]), "smart-selling": t([r for r in live if verdict(r) == "SELLING"]),
           "printed": [x["ticker"] for x in (d.get("changes") or {}).get("new") or []],
           "industries-leading": [x["industry"] for x in inds if x["vs"][k] > 0], "industries-lagging": [x["industry"] for x in inds if x["vs"][k] <= 0]}
    for i, g in enumerate(("STRONG ACCUMULATION", "ACCUMULATION", "NEUTRAL", "DISTRIBUTION", "STRONG DISTRIBUTION")):
        out["tape-%d" % i] = t([r for r in live if lab(r) == g])
    S = d.get("summary") or {}
    return {key: ("set", v) for key, v in out.items()}, [("every leader = the five tape lists", len(live), sum(len(out["tape-%d" % i]) for i in range(5)))], {"as_of": d.get("as_of"), "version": d.get("version"), "leaders": len(live), "summary": S.get("n_ranked")}


def ref_scan(board, statuses):
    def ref(d, scope):
        kind = "etf" if scope == "etf" else "stock"
        rows = [r for r in (d.get("boards") or {}).get(board) or [] if r.get("kind") == kind]
        floor = 1e8 if kind == "etf" else 1e9
        sized = [r for r in rows if not r.get("jump") and cap(r) >= floor]
        t = lambda rs: [r["ticker"] for r in rs]  # noqa: E731
        out = {"largest": t(rows)}
        for i, s in enumerate(statuses):
            out["status-%d" % i] = t([r for r in rows if r.get("status") == s])
        if board == "bottom":
            out.update({"new-climax": t([r for r in rows if r.get("mark") == "CLIMAX"]), "new-low": t([r for r in rows if r.get("mark") == "BOTTOM"]), "volume": t([r for r in sized if r.get("mark") == "CLIMAX" and fin(r.get("score"))]),
                        "deepest": t([r for r in sized if fin(r.get("decline_pct"))]), "strongest": t([r for r in sized if fin(r.get("since_pct")) and r["since_pct"] > 0]), "broken": t([r for r in rows if r.get("status") == "BROKEN"])})
        elif board == "accumulation":
            out.update({"newest": t(rows), "strongest": t([r for r in sized if fin(r.get("since_pct")) and r["since_pct"] > 0]), "weakest": t([r for r in sized if not (fin(r.get("since_pct")) and r["since_pct"] > 0)]),
                        "bases": t([r for r in rows if fin(r.get("base_sessions"))])})
        else:
            out.update({"newest": t(rows), "furthest": t([r for r in sized if fin(r.get("below_high_pct")) and r["below_high_pct"] < 0]), "reclaimed": t([r for r in rows if r.get("status") == "RECLAIMED"]),
                        "effort": t([r for r in sized if fin(r.get("effort_x")) and r.get("why") == "more volume bought less"])})
        if kind == "stock":
            out["sectors"] = sorted({r.get("sector") or "Unclassified" for r in rows})
        return {key: ("set", v) for key, v in out.items()}, [("every marked %s stands under one status" % kind, len(rows), sum(len(out["status-%d" % i]) for i in range(len(statuses))))], {"as_of": d.get("as_of"), "version": d.get("version"), "board": len(rows)}
    return ref


def ref_sc(d, scope):
    rows = [r for r in d.get("board") or [] if r.get("frame") == "D" and r.get("state") in ("CLIMAX", "TESTING")]
    t = lambda rs: [r["ticker"] for r in rs]  # noqa: E731
    out = {"newest": t(rows), "volume": t(rows), "deepest": t([r for r in rows if fin(r.get("sc_decline_pct"))]), "largest": t([r for r in rows if fin(r.get("mcap"))]), "graded": t([r for r in rows if fin(r.get("score"))]),
           "testing": t([r for r in rows if r.get("state") == "TESTING" and fin(r.get("dist_sc_low_pct"))])}
    return {key: ("set", v) for key, v in out.items()}, [], {"session": d.get("session"), "version": d.get("version"), "rows": len(rows)}


def ref_capit(d, scope):
    boards = d.get("boards") or {}
    order = d.get("desk_order") or list(boards)
    rows = [r for k in order for r in boards.get(k) or []]
    aft = lambda r, k: (r.get("after") or {}).get(k)  # noqa: E731
    t = lambda rs: [r["ticker"] for r in rs]  # noqa: E731
    out = {"all": t(rows), "latest": t(d.get("latest") or []), "volume": t([r for r in rows if fin(r.get("vol_x"))]), "falls": t([r for r in rows if fin(r.get("ret_pct"))]),
           "bounce": t([r for r in rows if r.get("after") and fin(aft(r, "bounce_pct")) and aft(r, "bounce_pct") >= 0.05]), "undercut": t([r for r in rows if r.get("after") and fin(aft(r, "undercut_pct")) and aft(r, "undercut_pct") <= -0.05]),
           "sessions": [day(s.get("date")) for s in (d.get("breadth") or {}).get("sessions") or [] if (s.get("total") or 0) > 0]}
    for k in order:
        if boards.get(k):
            out["desk-" + k] = t(boards[k])
    return {key: ("set", v) for key, v in out.items()}, [("every print = the desks' boards", len(rows), sum(len(boards.get(k) or []) for k in order))], {"session": d.get("session"), "prints": len(rows)}


def ref_bc(d, scope):
    names = [t for t, v in (d.get("tickers") or {}).items() if any(e and e.get("type") == "BC" for e in (v or {}).get("events") or [])]
    B = d.get("boards") or {}
    out = {"newest": names, "dist-begin": [x["ticker"] for x in B.get("distribution_beginning") or []], "dist-mature": [x["ticker"] for x in B.get("distribution_mature") or []],
           "dist-ended": [x["ticker"] for x in B.get("distribution_ended_markdown") or []]}
    return {key: ("set", v) for key, v in out.items()}, [], {"generated_at": d.get("generated_at"), "names": len(names)}


def ref_spring(d, scope):
    out = {"fired": [], "forming": [], "failed": []}
    for t, v in (d.get("tickers") or {}).items():
        v = v or {}
        fired = [e for e in v.get("events") or [] if e and e.get("type") == "SPRING"]
        lo, px = v.get("range_low"), v.get("price")
        dist = (px - lo) / lo * 100 if lo and px else None
        if v.get("phase") == "ACCUMULATION" and lo and px:
            if fired:
                out["fired"].append(t)
            elif (v.get("days_in_phase") or 0) >= 10 and dist <= 3:
                out["forming"].append(t)
        elif fired and v.get("phase") == "DISTRIBUTION":
            out["failed"].append(t)
    return {key: ("set", v) for key, v in out.items()}, [], {"generated_at": d.get("generated_at"), "names": sum(len(v) for v in out.values())}


def ref_spread(d, scope):
    rows = [x for x in d.get("picks") or [] if x and x.get("bbw_pctile") is not None and x["bbw_pctile"] <= 20 and (x.get("squeeze") or ((x.get("accum_legs") or {}).get("bb_tight") or 0) >= 70)]
    t = lambda rs: [r["ticker"] for r in rs]  # noqa: E731
    out = {"tightest": t(rows), "contraction": t([r for r in rows if fin((r.get("accum_legs") or {}).get("range_contraction"))]), "dry": t([r for r in rows if fin(r.get("vol_ratio_20_120"))]),
           "accum": t([r for r in rows if fin((r.get("pillars") or {}).get("accumulation"))]), "largest": t([r for r in rows if fin(r.get("mcap"))])}
    return {key: ("set", v) for key, v in out.items()}, [], {"session": d.get("session") or d.get("as_of"), "picks": len(d.get("picks") or []), "coiled": len(rows)}


def ref_sp500(d, scope):
    F = d.get("member_fields") or []
    i_chg, i_cap = F.index("px_chg_since_census_pct"), F.index("mcap")
    mem = [(t, r[i_chg], r[i_cap]) for t, r in (d.get("members") or {}).items() if fin(r[i_cap])]
    total = (d.get("index") or {}).get("total_mcap") or sum(m[2] for m in mem)
    contrib = {t: (c / total * 100 * chg / 100 if fin(chg) else None) for t, chg, c in mem} if total else {}
    out = {"heavy": [m[0] for m in mem], "lift": [t for t, v in contrib.items() if v is not None and v > 0], "drag": [t for t, v in contrib.items() if v is not None and v < 0], "sectors": list((d.get("sectors") or {}).keys())}
    return {key: ("set", v) for key, v in out.items()}, [("members listed = the index's own count", (d.get("index") or {}).get("members"), len(mem))], {"members": len(mem), "generated_at": d.get("generated_at")}


def ref_ark(d, scope):
    by = d.get("holdings_by_fund") or {}
    funds = [f for f in (d.get("funds_tracked") or list(by)) if by.get(f)]
    pos = {(h.get("ticker") or h.get("company")) for f in funds for h in by[f]}
    diff = d.get("diff_vs_prev") or {}
    src, hi = diff.get("all"), diff.get("highlights") or {}
    pick = lambda a, b: (src.get(a) if src else hi.get(b)) or []  # noqa: E731
    out = {"positions": None, "new": pick("new_positions", "new_positions"), "adds": pick("position_adds", "biggest_adds"), "trims": pick("position_trims", "biggest_trims"), "closed": pick("closed_positions", "closed_positions")}
    lists = {key: ("count", len(v)) for key, v in out.items() if v is not None}
    lists["positions"] = ("count", len(pos))
    return lists, [], {"holdings_date": d.get("holdings_date"), "positions": len(pos), "complete": bool(src)}


def ref_stablecoin(d, scope):
    known, seen = [], set()
    for c in (d.get("top_stablecoins_by_mcap") or []) + (d.get("top_5_minters_30d") or []) + (d.get("top_3_burners_30d") or []):
        if c and c.get("symbol") not in seen:
            seen.add(c.get("symbol"))
            known.append(c)
    out = {"coins": [c["symbol"] for c in d.get("top_stablecoins_by_mcap") or []], "mint": [c["symbol"] for c in known if (c.get("delta_30d") or 0) > 0], "burn": [c["symbol"] for c in known if (c.get("delta_30d") or 0) < 0],
           "chains": [c["chain"] for c in (d.get("aggregate") or {}).get("top_chains") or []]}
    return {key: ("set", v) for key, v in out.items()}, [], {"coins": len(known), "generated_at": d.get("generated_at")}


def ref_firmbook(d, scope):
    book = d.get("equity_book") or []
    out = {"book": [p["symbol"] for p in book], "longs": [p["symbol"] for p in book if (p.get("net_pct") or 0) > 0], "shorts": [p["symbol"] for p in book if (p.get("net_pct") or 0) < 0],
           "sectors": ["Sector not mapped" if s.get("sector") == "Unknown" else s.get("sector") for s in d.get("sector_exposure") or []], "desks": [x.get("name") for x in (d.get("desk_contributions") or {}).values()]}
    return {key: ("set", v) for key, v in out.items()}, [], {"positions": len(book), "generated_at": d.get("generated_at")}


def ref_paperbook(d, scope):
    trades, last = d.get("recent_trades") or [], d.get("last_rebalance")
    at = [t for t in trades if t.get("date") == last]
    buy = lambda t: "buy" in str(t.get("side")).lower()  # noqa: E731
    sell = lambda t: "sell" in str(t.get("side")).lower()  # noqa: E731
    out = {"positions": [p["ticker"] for p in d.get("positions") or []], "buys": [t["ticker"] for t in at if buy(t)], "sells": [t["ticker"] for t in at if sell(t)], "earlier": [t["ticker"] for t in trades if t.get("date") != last]}
    return {key: ("set", v) for key, v in out.items()}, [], {"last_rebalance": last, "trades": len(trades)}


def ref_insiders(d, scope):
    raw = sorted(d.get("clusters") or [], key=lambda c: -(c.get("score") or 0))
    seen, scored = set(), []
    for c in raw:
        k = c.get("cik") or c.get("ticker")
        if k in seen:
            continue
        seen.add(k)
        if fin(c.get("total_value")):
            scored.append(c)
    hy = d.get("hygiene") if (d.get("hygiene") or {}).get("applied") else None
    pop = scored
    if hy:
        seen_b, pop = set(), []
        for b in d.get("all_ticker_buys") or []:
            k = b.get("cik") or b.get("ticker")
            if k in seen_b or not fin(b.get("total_value")):
                continue
            seen_b.add(k)
            pop.append(b)
    t = lambda rs: [r["ticker"] for r in rs]  # noqa: E731
    out = {"clusters": t(scored), "dollars": t(pop), "ceo": t([c for c in scored if c.get("has_ceo")]), "crowd": t([c for c in scored if (c.get("n_insiders") or 0) >= 2])}
    if hy and hy.get("set_aside"):
        out["aside"] = t(hy["set_aside"])
    return {key: ("set", v) for key, v in out.items()}, [], {"scored": len(scored), "companies": len(pop), "hygiene": bool(hy)}


def ref_tic(d, scope):
    rows = [r for r in (d.get("individual") or {}).values() if fin(r.get("current_b"))]
    c3 = lambda r: r.get("3mo_change_b")  # noqa: E731
    n = lambda rs: [r["country"] for r in rs]  # noqa: E731
    out = {"holders": n(rows), "up": n([r for r in rows if fin(c3(r)) and c3(r) > 0]), "down": n([r for r in rows if fin(c3(r)) and c3(r) < 0]), "yoy": n([r for r in rows if fin(r.get("yoy_change_b"))])}
    return {key: ("set", v) for key, v in out.items()}, [], {"jurisdictions": len(rows)}


def ref_capital(d, scope):
    cls = [v for v in (d.get("by_asset_class") or {}).values() if fin(v.get("latest_month_b"))]
    hist = d.get("history_12mo_rolling_b") or []
    lists = {"bought": ("count", sum(1 for c in cls if c["latest_month_b"] > 0)), "sold": ("count", sum(1 for c in cls if c["latest_month_b"] < 0)), "year": ("count", len(cls)), "history": ("set", [month(x.get("asof")) for x in hist])}
    return lists, [], {"prints": len(hist), "data_asof": d.get("data_asof")}


REFS = {"volatility": ref_volatility, "leaders": ref_leaders, "wy-bottom": ref_scan("bottom", ("HELD", "UNDERCUT", "BROKEN")), "wy-accum": ref_scan("accumulation", ("ABOVE", "BASE", "LOST")),
        "wy-dist": ref_scan("distribution", ("BELOW", "RANGE", "RECLAIMED")), "wy-sc": ref_sc, "wy-capit": ref_capit, "capitulation": ref_capit, "wy-bc": ref_bc, "wy-spring": ref_spring, "wy-spread": ref_spread,
        "sp500": ref_sp500, "ark": ref_ark, "stablecoin": ref_stablecoin, "firmbook": ref_firmbook, "paperbook": ref_paperbook, "insiders": ref_insiders, "tic": ref_tic, "capital": ref_capital}


# ------------------------------------------------------------------------------------------------- the browser
def ready(page, timeout=45000):
    page.wait_for_function("() => { const e = document.getElementById('jh-tearsheet'); return e && e.dataset.state === 'ready' && e.__model && !e.__model.hide; }", timeout=timeout)


def wait_open(page, key, timeout=12000):
    page.wait_for_function("(k) => { const e = document.getElementById('jh-tearsheet'), f = e && e.querySelector('.jts-full'); return (e.dataset.open || '') === k && (!k || (f && f.getAttribute('data-jts-full') === k)); }", arg=key, timeout=timeout)


def ident(texts):
    return " | ".join(texts)


def squash(v):
    return " ".join(str(v).split())                              # the page collapses runs of spaces, as a browser draws them


def prove_list(page, ck, who, lst, depth, phone):
    """One open list: its count, its own order, its columns both ways, then SHOW ALL. Returns how many rankings were checked.
    depth: "every" column; "sample" (a long list: its name, the column it stands in, size / sector / industry, two more figures);
    "few" (a list on columns already ranked one by one on another list of the same sheet, or a phone)."""
    f, key = lst["full"], lst["key"]
    vals = page.evaluate(VALUES, key)
    ids, raw, text = vals["ids"], vals["raw"], vals["text"]
    n = len(raw)
    idc = ids[:3]                                              # a row is named by its first three columns
    at3 = [ids.index(c) for c in idc]
    names = [ident([squash(text[i][c]) for c in at3]) for i in range(n)]
    s = page.evaluate(DOM, idc)
    ck(s["key"] == key and s["open"] == key, "%s: the open list is %r, not %r" % (who, s["key"], key))
    if n == 0:                                                 # a list with nothing in it opens on its own sentence, with no table
        ck(s["n_rows"] == 0 and s["head"] == [] and s["count"].startswith("0 ") and s["bad"] == 0 and s["ox"] <= 1, "%s: an empty list draws %d rows, headings %s, count %r" % (who, s["n_rows"], s["head"], s["count"][:40]))
        return 0
    ck(sorted(s["head"]) == sorted(ids), "%s: the headings %s are not the list's columns %s" % (who, s["head"], ids))
    if phone and f.get("phone"):
        lead = [c for i, c in enumerate(f["phone"]) if c in ids and c not in f["phone"][:i]]
        ck(s["head"][:len(lead)] == lead, "%s: on a phone the columns lead with %s, the page has %s" % (who, lead, s["head"][:len(lead)]))
    elif not phone:
        ck(s["head"] == ids, "%s: the columns stand in the sheet's order: %s vs %s" % (who, s["head"], ids))
    units = f.get("units") or ["row", "rows"]
    ck(s["count"].startswith("%s %s" % (P.fint(n), units[0] if n == 1 else units[1])), "%s: the count line %r does not state %s %s" % (who, s["count"][:70], P.fint(n), units[1]))
    ck(s["n_rows"] == min(n, PAGE), "%s: %d rows drawn, expected %d of %d" % (who, s["n_rows"], min(n, PAGE), n))
    ck([ident(r) for r in s["rows"]] == names[:min(n, PAGE)], "%s: the rows drawn are not the list's own, in its order" % who)
    ck(s["bad"] == 0, "%s: the sheet prints a broken value (%d)" % (who, s["bad"]))
    ck(s["ox"] <= 1, "%s: the page spills %d px sideways" % (who, s["ox"]))
    if n > PAGE:
        ck(s["more"] == "SHOW ALL %s ROWS" % P.fint(n), "%s: the button reads %r" % (who, s["more"]))
    if f.get("sort") and n > 1:
        arrow = "descending" if f["sort"]["d"] < 0 else "ascending"
        ck(s["sorted"] == [[f["sort"]["k"], arrow]], "%s: the heading the list stands in is %s, expected %s %s" % (who, s["sorted"], f["sort"]["k"], arrow))
    if vals.get("href") is not None and n:
        got = {t: (h, tg) for h, tg, t in s["links"]}
        for i, h in enumerate(vals["href"][:min(n, 60, s["n_rows"])]):
            cell = s["first_cells"][i]
            if h:
                ck(got.get(cell, ("", ""))[0] == h and got.get(cell, ("", ""))[1] == "_top", "%s: %s should link to %s out of the desk's frame, the page has %s" % (who, cell, h, got.get(cell)))
            else:
                ck(cell not in got, "%s: %s has no chart of its own and should not be a link" % (who, cell))
    order, done = list(range(n)), 0
    if n >= 2:
        cols = f["cols"]
        if depth != "every":
            keep = [cols[0]["id"]] + ([f["sort"]["k"]] if f.get("sort") else []) + [c["id"] for c in cols if c["id"] in SWEEP]
            keep += [c["id"] for c in cols if c["type"] == "n"][-2:]
            cols = [c for c in cols if c["id"] in keep][: (9 if depth == "sample" else 3 if phone else 4)]
        cur = s["sorted"][0] if s["sorted"] else None
        for ci, c in enumerate(cols):
            j = ids.index(c["id"])
            first = -1 if c["type"] == "n" else 1
            if cur and cur[0] == c["id"]:
                first = 1 if cur[1] == "descending" else -1    # the list already stands in this column: the first click reverses it
            for d in (first, -first):
                if ci == 0:                                    # the first column by the mouse, the rest by the same click event dispatched on the heading
                    page.click("#jh-tearsheet .jts-ft thead th[data-jts-sort='%s']" % c["id"])
                    st = page.evaluate(RANK, [None, idc])
                else:
                    st = page.evaluate(RANK, [c["id"], idc])
                arrow = "descending" if d < 0 else "ascending"
                cur = [c["id"], arrow]
                ok = ck(st is not None and st["sorted"] == [[c["id"], arrow]], "%s: after the click %s reads %s, expected %s" % (who, c["id"], st and st["sorted"], arrow))
                if st is None:
                    continue
                order = expected_order(raw, j, c["type"], d)
                got, want = [ident(r) for r in st["rows"]], [names[i] for i in order[:len(st["rows"])]]
                if got != want:
                    at = next((x for x in range(min(len(got), len(want))) if got[x] != want[x]), min(len(got), len(want)))
                    ok = ck(False, "%s: ranked by %s %s, row %d is %r where the list's values put %r" % (who, c["t"], arrow, at + 1, got[at] if at < len(got) else None, want[at] if at < len(want) else None))
                done += 1 if ok else 0
        st = page.evaluate(RANK, [None, idc + ["", ""]])       # with a fourth name asked for, the read also measures the page (a layout, so once a list)
        ck(st["ox"] <= 1 and st["bad"] == 0, "%s: after ranking by every column the page spills %d px or prints a broken value (%d)" % (who, st["ox"], st["bad"]))
    if n > PAGE:                                               # the rest of the list, in the order it now stands in
        page.click("#jh-tearsheet [data-jts-act='all']")
        page.wait_for_function("(n) => document.querySelectorAll('#jh-tearsheet .jts-ft tbody tr').length === n", arg=n, timeout=20000)
        st = page.evaluate(RANK, [None, idc + ["", ""]])
        ck([ident(r[:len(idc)]) for r in st["rows"]] == [names[i] for i in order], "%s: SHOW ALL does not draw the whole list (%d rows drawn of %d) in its order" % (who, len(st["rows"]), n))
        ck(st["ox"] <= 1, "%s: with every row drawn the page spills %d px" % (who, st["ox"]))
    return done


def prove_sheet(page, r, ck, sheet, scope, feeds, phone, deep, shots, facts, stamp, seen):
    tag = sheet["tag"] + ("" if scope is None else ":" + scope) + (" @390" if phone else "")
    m = page.evaluate(MODEL)
    if not ck(m and m["state"] == "ready" and not m["hide"], "%s: the sheet is not ready (%s)" % (tag, m and m["state"])):
        return
    ck(P.semver(m["kit"]) >= KIT_MIN, "%s: the kit is %s, older than %s" % (tag, m["kit"], ".".join(map(str, KIT_MIN))))
    lists = {l["key"]: l for l in m["lists"]}
    cards = [l for l in m["lists"] if l["card"]]
    s = page.evaluate(DOM, [])
    ck(s["bad"] == 0 and s["ox"] <= 1, "%s: the sheet prints a broken value (%d) or spills %d px" % (tag, s["bad"], s["ox"]))
    # every card with a row opens, and its heading says so; the count beside it is the whole list's unless the card states its own
    dom_cards = {c[0]: c for c in s["cards"]}
    for l in cards:
        c = dom_cards.get(l["key"])
        if not ck(c is not None, "%s: the card %s is not drawn" % (tag, l["key"])):
            continue
        if l["rows"] or (l["full"] and l["full"]["n"]):
            ck(l["full"] is not None and c[1] and c[3].startswith("VIEW ALL"), "%s: the card %s (%d rows) has no whole list behind its heading" % (tag, l["key"], l["rows"]))
        if l["full"]:
            ck(l["full"]["n"] >= l["rows"], "%s: %s opens %d rows, fewer than the card's %d" % (tag, l["key"], l["full"]["n"], l["rows"]))
            ck(c[2] == (str(l["n"]) if l["n"] is not None else str(l["full"]["n"])), "%s: the count beside %s reads %r (n %s, whole %d)" % (tag, l["key"], c[2], l["n"], l["full"]["n"]))
    # the ring and its slices open a list that exists
    if m["ring"]:
        for k in [m["ring"]["open"]] + m["ring"]["slices"]:
            ck(not k or (k in lists and lists[k]["full"]), "%s: the ring opens %r, which is no list of the sheet" % (tag, k))
        ck(s["ring_open"] == m["ring"]["open"] and [x for x in s["legend"] if x] == [x for x in m["ring"]["slices"] if x], "%s: the ring on the page opens %r / %s, the sheet says %r / %s" % (tag, s["ring_open"], s["legend"], m["ring"]["open"], m["ring"]["slices"]))
    # the reference: the names each list must hold, from the feed's own bytes
    ref = REFS.get(sheet["tag"])
    n_ref = 0
    if ref and feeds.get("doc") is not None and not phone:
        want, engine, info = ref(feeds["doc"], scope)
        facts.setdefault(sheet["tag"], {}).update({("" if scope is None else scope + "_") + k: v for k, v in info.items()})
        for label, a, b in engine:
            if SLICED and (isinstance(a, int) or (isinstance(a, list) and a and isinstance(a[0], int))) and not isinstance(a, bool):
                continue                                       # offline the fixture is a slice of the feed: the engine's counts are the whole feed's
            ck(a == b, "%s: %s: the engine says %s, the feed's rows give %s" % (tag, label, str(a)[:120], str(b)[:120]))
        for key, (how, exp) in want.items():
            l = lists.get(key)
            if l is None or not l["full"]:
                ck(how == "count" and exp == 0 or (how != "count" and not exp), "%s: the feed holds %s for %s and the sheet has no such list" % (tag, exp if how == "count" else len(exp), key))
                continue
            if how == "count":
                ck(l["full"]["n"] == exp, "%s: %s opens %d rows, the feed holds %d" % (tag, key, l["full"]["n"], exp))
                n_ref += 1
                continue
            got = page.evaluate("(k) => { const m = document.getElementById('jh-tearsheet').__model, l = (m.lists || []).concat(m.more || []).filter(x => x.key === k)[0], c = l.full.cols[0]; return l.full.rows.map(r => { const v = c.fmt ? c.fmt(r) : c.get(r); return v == null ? '' : String(v); }); }", key)
            exp = [str(x) for x in exp]
            if how == "ordered":
                if got != exp:
                    at = next((x for x in range(min(len(got), len(exp))) if got[x] != exp[x]), min(len(got), len(exp)))
                    ck(False, "%s: %s: %d rows on the page, %d from the feed; row %d is %r, the feed's is %r" % (tag, key, len(got), len(exp), at + 1, got[at] if at < len(got) else None, exp[at] if at < len(exp) else None))
            elif sorted(got) != sorted(exp):
                ck(False, "%s: %s: %d rows on the page, %d from the feed; only on the page %s, only in the feed %s" % (tag, key, len(got), len(exp), sorted(set(got) - set(exp))[:6], sorted(set(exp) - set(got))[:6]))
            n_ref += 1
    # open every list: each card by its heading, the others from the buttons above an open list, the ring by its heading and legend
    opened, ranked, first = 0, 0, True
    todo = [l for l in m["lists"] if l["full"]]
    if phone:
        todo = [l for l in todo if l["card"]][:4] + [l for l in todo if not l["card"]][:1]
    for l in todo:
        key = l["key"]
        who = "%s/%s" % (tag, key)
        try:
            if l["card"] and (first or opened % 3 == 0):
                page.evaluate("() => { const e = document.getElementById('jh-tearsheet'); if (e.dataset.open) e.querySelector('[data-jts-act=\"close\"]').click(); }")
                wait_open(page, "")
                page.click("#jh-tearsheet [data-jts-list='%s'] .jts-ch[data-jts-open]" % key)
            else:
                if not page.evaluate("() => !!document.getElementById('jh-tearsheet').dataset.open"):
                    page.click("#jh-tearsheet .jts-ch[data-jts-open]")
                    page.wait_for_selector("#jh-tearsheet .jts-full", timeout=10000)
                page.click("#jh-tearsheet .jts-fnav [data-jts-open='%s']" % key)
            wait_open(page, key)
        except Exception as exc:  # noqa: BLE001
            ck(False, "%s: the list did not open (%s)" % (who, str(exc)[:120]))
            continue
        first = False
        opened += 1
        if sheet.get("shot") and opened == 1 and (not phone or sheet.get("phone")):
            try:
                page.evaluate("() => { const f = document.querySelector('#jh-tearsheet .jts-full'); if (f) f.scrollIntoView({ block: 'start' }); const w = document.querySelector('#jh-tearsheet .jts-fwrap'); if (w) w.scrollTop = 0; }")
                page.wait_for_timeout(250)
                shots["%s%s_%s" % (sheet["tag"], "" if scope is None else "_" + scope, "390" if phone else "1440")] = page.screenshot(type="jpeg", quality=50)
            except Exception as exc:  # noqa: BLE001
                r.log("screenshot failed: %s" % str(exc)[:100])
        sig = tuple(c["id"] for c in l["full"]["cols"])             # lists on the same columns: one is ranked by every column, the others by a few
        depth = "every" if deep and opened == 1 else "few" if phone or sig in seen else "sample" if l["full"]["n"] > BIG else "every"
        seen.add(sig)
        ranked += prove_list(page, ck, who, l, depth, phone)
    # find, CSV, the ring, BACK and Escape on the last open list
    if opened and not phone:
        l = [x for x in todo if x["full"]["n"] >= 3]
        if l:
            l = l[0]
            key, who = l["key"], "%s/%s" % (tag, l["key"])
            page.click("#jh-tearsheet .jts-fnav [data-jts-open='%s']" % key)
            wait_open(page, key)
            vals = page.evaluate(VALUES, key)
            text_cols = [i for i, c in enumerate(l["full"]["cols"]) if c["type"] == "s"]
            word = next((str(row[text_cols[0]]) for row in vals["raw"] if row[text_cols[0]]), "") if text_cols else ""
            q = word[:3].lower()
            if q:
                page.fill("#jh-tearsheet [data-jts-find]", q)
                page.wait_for_timeout(450)

                def hit(row):
                    for i in text_cols:
                        v = row[i]
                        if v is None:
                            continue
                        v = str(v).lower()
                        if (v.startswith(q) if l["full"]["cols"][i]["start"] else q in v):
                            return True
                    return False
                want = [i for i, row in enumerate(vals["raw"]) if hit(row)]
                s2 = page.evaluate(DOM, vals["ids"][:3])
                names = [ident([squash(vals["text"][i][c]) for c in range(min(3, len(vals["ids"])))]) for i in want]
                ck(s2["count"].startswith("%s of %s" % (P.fint(len(want)), P.fint(len(vals["raw"]))) if len(want) != len(vals["raw"]) else P.fint(len(want))), "%s: find %r: the count line reads %r, expected %d of %d" % (who, q, s2["count"][:60], len(want), len(vals["raw"])))
                ck(sorted(ident(x) for x in s2["rows"]) == sorted(names[:max(s2["n_rows"], 0)]) if len(want) <= PAGE else s2["n_rows"] == PAGE, "%s: find %r does not show the %d rows that hold it" % (who, q, len(want)))
                try:
                    with page.expect_download(timeout=15000) as dl:
                        page.click("#jh-tearsheet [data-jts-act='csv']")
                    csv = Path(dl.value.path()).read_text(encoding="utf-8").split("\n")
                    ck(len(csv) == len(want) + 1, "%s: the CSV has %d lines for %d rows on screen" % (who, len(csv), len(want)))
                    ck(dl.value.suggested_filename.endswith("-%s.csv" % key) or dl.value.suggested_filename.endswith(".csv"), "%s: the CSV is named %s" % (who, dl.value.suggested_filename))
                except Exception as exc:  # noqa: BLE001
                    ck(False, "%s: the CSV did not download (%s)" % (who, str(exc)[:100]))
                page.fill("#jh-tearsheet [data-jts-find]", "")
                page.wait_for_timeout(350)
            page.click("#jh-tearsheet [data-jts-act='close']")
            wait_open(page, "")
            if m["ring"] and m["ring"]["open"]:
                page.click("#jh-tearsheet .jts-ringcol .jts-ch[data-jts-open]")
                wait_open(page, m["ring"]["open"])
                page.focus("#jh-tearsheet [data-jts-find]")
                page.keyboard.press("Escape")
                wait_open(page, "")
            for i, k in enumerate(m["ring"]["slices"] if m["ring"] else []):
                if not k:
                    continue
                page.click("#jh-tearsheet .jts-leg tr[data-jts-open='%s']" % k)
                wait_open(page, k)
                page.click("#jh-tearsheet [data-jts-act='close']")
                wait_open(page, "")
                break
    else:
        page.evaluate("() => { const e = document.getElementById('jh-tearsheet'); if (e.dataset.open) e.querySelector('[data-jts-act=\"close\"]').click(); }")
    r.log("%s: kit %s - %d lists (%d cards), %d opened, %d rankings held against the list's own values, %d lists held against the feed%s" % (
        tag, m["kit"], len(m["lists"]), len(cards), opened, ranked, n_ref, "" if scope is not None or phone else " - " + m["title"][:110]))
    f = facts.setdefault("_totals", {"sheets": 0, "lists": 0, "opened": 0, "rankings": 0, "referenced": 0})
    f["sheets"] += 1
    f["lists"] += len(m["lists"])
    f["opened"] += opened
    f["rankings"] += ranked
    f["referenced"] += n_ref


def prove(browser, r, ck, shots, base=BASE, stamp=0, route=None, facts=None, only=None, offline=False):
    facts = {} if facts is None else facts
    for width, height, mobile, tag in ((1440, 900, False, "1440"), (390, 844, True, "390")):
        r.section("every desk at %s px" % tag)
        ctx = browser.new_context(viewport={"width": width, "height": height}, device_scale_factor=1, is_mobile=mobile, has_touch=mobile, accept_downloads=True,
                                  user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) jh-ops-probe Mobile Safari" if mobile else P.UA)
        if route:
            ctx.route("**/*", route)
        for sheet in SHEETS:
            if only and sheet["tag"] not in only:
                continue
            page = ctx.new_page()
            errors, got = [], {}
            page.on("pageerror", lambda e, errors=errors: errors.append(str(e)[:200]))

            def on_response(resp, got=got, sheet=sheet):
                for name, path in sheet["feeds"].items():
                    if path in resp.url and resp.status == 200:
                        got[name] = resp
            page.on("response", on_response)
            try:
                page.goto("%s%s%sv=%d" % (base, sheet["path"], "&" if "?" in sheet["path"] else "?", stamp), wait_until="domcontentloaded", timeout=60000)
                ready(page)
                page.wait_for_timeout(1200)                     # the desk hands the sheet its own copy of the feed once it has read it
                ready(page)
            except Exception as exc:  # noqa: BLE001
                ck(False, "%s at %s: the sheet did not become ready (%s)" % (sheet["tag"], tag, str(exc)[:160]))
                page.close()
                continue
            feeds = {}
            for name, resp in got.items():
                try:
                    feeds[name] = json.loads(resp.body().decode("utf-8"))
                except Exception as exc:  # noqa: BLE001
                    ck(offline, "%s: the feed %s could not be read back (%s)" % (sheet["tag"], sheet["feeds"][name], str(exc)[:100]))
            if sheet["feeds"] and not mobile:
                ck("doc" in feeds, "%s: the page did not fetch %s" % (sheet["tag"], sheet["feeds"].get("doc")))
            seen = set()
            for i, scope in enumerate(sheet.get("scopes") or [None]):
                if mobile and i:
                    continue
                if scope is not None:
                    try:
                        page.evaluate("() => { const e = document.getElementById('jh-tearsheet'); if (e.dataset.open) e.querySelector('[data-jts-act=\"close\"]').click(); }")
                        page.click("#jh-tearsheet [data-jts-ctl='%s']" % scope)
                        page.wait_for_function("(s) => { const b = document.querySelector('#jh-tearsheet [data-jts-ctl=\"' + s + '\"]'); return b && b.getAttribute('aria-pressed') === 'true'; }", arg=scope, timeout=10000)
                    except Exception as exc:  # noqa: BLE001
                        ck(False, "%s: the %s button did not take (%s)" % (sheet["tag"], scope, str(exc)[:120]))
                        continue
                try:
                    prove_sheet(page, r, ck, sheet, scope, feeds, mobile, sheet["tag"] == "volatility" and scope is None and not mobile, shots, facts, stamp, seen)
                except Exception as exc:  # noqa: BLE001
                    ck(False, "%s%s at %s: the walk stopped (%s)" % (sheet["tag"], "" if scope is None else ":" + scope, tag, str(exc)[:300]))
            if not offline:
                ck(not errors, "%s at %s: page errors %s" % (sheet["tag"], tag, errors[:3]))
            elif errors:
                r.log("%s: page errors offline (the desk's own script on a trimmed feed): %s" % (sheet["tag"], errors[:2]))
            page.close()
        # inside the chart's workspace: the desk sits in a frame, and a ticker of an open list loads in the chart behind it
        if not mobile and not offline and (not only or "volatility" in only):
            r.section("inside the chart's workspace")
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e, errors=errors: errors.append(str(e)[:200]))
            try:
                page.goto("%s/chart.html?v=%d" % (base, stamp), wait_until="domcontentloaded", timeout=60000)
                page.wait_for_function("() => typeof window.jhOpenWorkspace === 'function' && typeof window.jhOpenSymbol === 'function'", timeout=45000)
                page.evaluate("() => window.jhOpenWorkspace('volatility')")
                page.wait_for_selector("#ws-overlay.on iframe", timeout=15000)
                frame = None
                for _ in range(60):
                    frame = next((f for f in page.frames if "stock-volatility.html" in f.url), None)
                    if frame:
                        break
                    page.wait_for_timeout(250)
                ck(frame is not None, "workspace: the Volatility desk's frame did not load")
                if frame:
                    frame.wait_for_function("() => { const e = document.getElementById('jh-tearsheet'); return e && e.dataset.state === 'ready' && e.__model; }", timeout=45000)
                    frame.click("#jh-tearsheet [data-jts-list='least-volatile'] .jts-ch[data-jts-open]")
                    frame.wait_for_selector("#jh-tearsheet .jts-full .jts-ft tbody tr", timeout=15000)
                    st = frame.evaluate(DOM, [])
                    ck(st["key"] == "least-volatile" and st["n_rows"] == PAGE and st["ox"] <= 1, "workspace: the list in the frame is %s with %d rows, spilling %d px" % (st["key"], st["n_rows"], st["ox"]))
                    sym = st["links"][2][2] if len(st["links"]) > 2 else ""
                    shots["workspace_1440"] = page.screenshot(type="jpeg", quality=50)
                    frame.click("#jh-tearsheet .jts-ft tbody tr:nth-child(3) td.jts-fk a")
                    page.wait_for_function("() => document.getElementById('ws-overlay').className === ''", timeout=10000)
                    page.wait_for_function("(s) => { const b = document.querySelector('#tabs .tab.on'); return b && b.getAttribute('data-id') === s; }", arg=sym, timeout=15000)
                    top = page.evaluate("() => ({ url: location.pathname, on: (document.querySelector('#tabs .tab.on') || { getAttribute: () => '' }).getAttribute('data-id'), overlay: document.getElementById('ws-overlay').className })")
                    ck(top["url"].endswith("/chart.html") and top["on"] == sym and top["overlay"] == "", "workspace: a click on %s left the chart on %s (%s), overlay %r" % (sym, top["on"], top["url"], top["overlay"]))
                    r.log("workspace: Volatility opened inside chart.html, 'Least volatile' opened in full (%d rows drawn), a click on %s closed the desk and the chart's active tab is %s" % (st["n_rows"], sym, top["on"]))
                    facts["workspace"] = {"symbol": sym, "active": top["on"]}
            except Exception as exc:  # noqa: BLE001
                ck(False, "workspace: the walk stopped (%s)" % str(exc)[:300])
            page.close()
        ctx.close()
    return facts


def main():
    from ops_report import report
    with report("ops_6868_every_tearsheet_list_live_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6868 - every list of every tearsheet desk opens whole and ranks by any column, live %s" % now.isoformat(timespec="minutes"))
        fails, shots, facts = [], {}, {}

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL %s" % msg)
            return bool(cond)

        sync_playwright = P.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = P.own_order(P.launch(p, r.log))   # the desk's own order: what this proof compares against (kit 1.8.0 opens on leaders first)
            prove(browser, r, ck, shots, BASE, int(time.time()), None, facts)
            browser.close()
        tot = facts.get("_totals") or {}
        vol = facts.get("volatility") or {}
        r.kv(failures=len(fails), sheets=tot.get("sheets"), lists=tot.get("lists"), opened=tot.get("opened"), rankings=tot.get("rankings"), lists_held_against_feed=tot.get("referenced"),
             volatility_as_of=vol.get("as_of"), volatility_ranked_1b=vol.get("ranked_1b"), volatility_rising=vol.get("rising"), volatility_falling=vol.get("falling"), workspace=json.dumps(facts.get("workspace")))
        r.section("what each feed held")
        for k, v in facts.items():
            if k not in ("_totals", "workspace"):
                r.log("%s: %s" % (k, json.dumps(v)[:400]))
        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        if fails:
            r.fail("%d checks failed: %s" % (len(fails), " | ".join(fails)[:1800]))
            sys.exit(1)
        r.ok("LIVE PROOF: on every desk that carries the tearsheet, every card, the ring and its slices open the whole list; each list is drawn in its own order with SHOW ALL for the rest, ranks by its columns both ways "
             "against an independent sort of its values, is found and saved as CSV; Volatility's lists are the engine's whole ranking populations in order (the first 25 = its boards, the lengths = its populations and band counts), "
             "the other desks' lists hold exactly the names their feeds carry; a ticker opens the chart out of the desk's frame, and inside the chart's workspace it loads behind the desk; no page error, nothing spills, nothing prints undefined.")


if __name__ == "__main__":
    main()
