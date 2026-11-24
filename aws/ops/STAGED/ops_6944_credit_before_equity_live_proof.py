"""ops 6944 - Credit Before Equity 2.0: the warehouse's own bonds and bars, then a real browser on the live site.

Khalid 2026-11-24: "i want you to expand it to cover as many companies as possible and it should have a heatmap that shows
industries with biggest corp spread widening which signal trouble and ones with lowes which signals safety also you i should be
able to sort them by industry and sort them ascending and descending and also add companies CDS credit default swaps".

Read-only: no engine is invoked and nothing is written. Three parts, each with arithmetic written here, not the engine's:

  1. The published document against itself and against the warehouse: the counts are the rows; five companies' spreads are
     worked again from the corporate bonds desk's raw bonds and the Treasury curve; a company's one-week change is worked
     again from the board's own ledger day; every industry's change is the middle company by value; every lead's stock
     change is two closes of the price bank; the traded CDS levels are the CDS desk's.
  2. The page in a real browser on justhodl.ai: the evidence status, one tile per industry with the document's figure and a
     colour that means what it says (widening red, tightening green - not the site's amber), the two ranked lists, the
     one-week / book / measure controls, an industry's detail, the table sorted by every means the page offers (header,
     the Sort by list, the direction button, by industry both ways, grouped by industry), the search, the company CDS table
     sorted both ways, and the previous view one click away and back.
  3. A phone.

Safe to run again.
"""
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
SRC = ROOT / "aws" / "lambdas" / "justhodl-credit-before-equity" / "source"
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared"), str(SRC)]
BASE = "https://justhodl.ai"
LIVE = "justhodl-dashboard-live"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
MINUS = "−"
TEN = {"1M": 1 / 12, "2M": 2 / 12, "3M": 0.25, "4M": 4 / 12, "6M": 0.5, "1Y": 1.0, "2Y": 2.0, "3Y": 3.0, "5Y": 5.0, "7Y": 7.0, "10Y": 10.0, "20Y": 20.0, "30Y": 30.0}
LEADS = ("CREDIT_LEADS_DOWN", "CREDIT_LEADS_UP")

BOXES = r"""() => { const map = document.querySelector('.cb-map'); if (!map) return null; const mb = map.getBoundingClientRect();
  const out = Array.from(map.querySelectorAll('.cb-box')).map(e => { const b = e.getBoundingClientRect(), n = e.querySelector('.cb-box-n'), v = e.querySelector('.cb-box-v');
    return { ind: e.dataset.ind, x: b.left - mb.left, y: b.top - mb.top, w: b.width, h: b.height, bg: getComputedStyle(e).backgroundColor, name: n ? n.textContent : null, val: v ? v.textContent : null, title: e.title, tag: e.tagName }; });
  let over = 0, out_of = 0;
  for (let i = 0; i < out.length; i++) { const a = out[i]; if (a.x < -1.6 || a.y < -1.6 || a.x + a.w > mb.width + 1.6 || a.y + a.h > mb.height + 1.6) out_of++;
    for (let j = i + 1; j < out.length; j++) { const b = out[j]; if (Math.min(a.x + a.w, b.x + b.w) - Math.max(a.x, b.x) > 1.2 && Math.min(a.y + a.h, b.y + b.h) - Math.max(a.y, b.y) > 1.2) over++; } }
  return { boxes: out, over: over, out_of: out_of, map: [Math.round(mb.width), Math.round(mb.height)], ox: document.documentElement.scrollWidth - document.documentElement.clientWidth,
    badges_in_map: map.querySelectorAll('.jh-secbadge').length, floating: document.querySelectorAll('#board .jh-secbadge.float').length, bad: (document.getElementById('board').innerText.match(/\bNaN\b|\bundefined\b|\bnull\b|Infinity/g) || []) }; }"""
COL = r"""([sel, n]) => Array.from(document.querySelectorAll(sel + ' tbody tr:not(.cb-grp)')).map(tr => tr.children[n] ? tr.children[n].innerText.replace(/\s+/g, ' ').trim() : null)"""


def fixed(a, d):
    """What JavaScript's toFixed prints: the exact binary value, halves away from zero."""
    return str(Decimal(a).quantize(Decimal(1).scaleb(-d), rounding=ROUND_HALF_UP))


def figure(text):
    """The number a cell or a tile shows, or None for a dash."""
    if text is None:
        return None
    t = text.replace(MINUS, "-").replace(",", "")
    m = re.search(r"[-+]?\d+(?:\.\d+)?", t)
    return float(m.group(0)) if m else None


def shown(v, text):
    """True when `text` is the figure v as the page writes basis points: one decimal under 100, none from 100."""
    f = figure(text)
    if v is None:
        return f is None
    return f is not None and abs(f - v) <= (0.5001 if abs(v) >= 99.95 else 0.0501)


def rgb(s):
    m = re.match(r"rgba?\((\d+),\s*(\d+),\s*(\d+)", s or "")
    return tuple(int(x) for x in m.groups()) if m else None


def par_at(row, years):
    pts = sorted((TEN[k], float(v)) for k, v in row.items() if k in TEN and isinstance(v, (int, float)))
    if years <= pts[0][0]:
        return pts[0][1]
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        if years <= x1:
            return y0 + (y1 - y0) * (years - x0) / (x1 - x0)
    return pts[-1][1]


def own_spreads(bonds, curve_row, as_of):
    """{bond key: (spread bp, market value held)} for the bonds a board would read, by this script's own arithmetic."""
    out = {}
    for b in bonds:
        y, dur, v = b.get("rate"), b.get("dur"), float(b.get("demand") or 0.0)
        if not isinstance(y, (int, float)) or not isinstance(dur, (int, float)) or not 0 < y < 49.99 or dur < 1.0:
            continue
        yy = max(y, 0.5) / 100.0
        x = dur * yy
        t = max(30.0 if x >= 0.97 else -math.log(1.0 - x) / math.log(1.0 + yy), dur)
        try:
            left = (date.fromisoformat(str(b.get("maturity"))[:10]) - date.fromisoformat(as_of)).days / 365.25
            if left > 0:
                t = min(t, max(left, dur))
        except (TypeError, ValueError):
            pass
        out[b.get("k")] = (round((y - par_at(curve_row, min(t, 30.0))) * 100.0, 1), v)
    return out


def middle(pairs):
    pairs = sorted(pairs)
    total, run = sum(v for _, v in pairs), 0.0
    for d, v in pairs:
        run += v
        if run >= total / 2.0:
            return d
    return None


def is_sorted(vals, desc):
    have = [v for v in vals if v is not None]
    return all((a >= b) if desc else (a <= b) for a, b in zip(have, have[1:])) and all(v is None for v in vals[len(have):])


def browse(new_page, url, doc, ck, shots, log, phone_page=None):
    """Parts 2 and 3: the page in a real browser. new_page() gives a desktop page; phone_page() a phone's."""
    cols = doc["issuers"]["cols"]
    table = [dict(zip(cols, r)) for r in doc["issuers"]["rows"]]
    names = doc["names"]
    inds = {b: {i["industry"]: i for i in doc["industries"][b]} for b in ("all", "ig", "hy", "em")}
    page, errors = new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=90000)
    page.wait_for_selector(".cb-box", timeout=90000)
    page.wait_for_timeout(2500)

    ev = page.locator("#evidence").inner_text()
    log("evidence on the page: %s" % " | ".join(x.strip() for x in ev.splitlines() if x.strip())[:420])
    ck("RECONCILED" in ev and "counts reconcile with the rows (8 checks)" in ev, "the evidence status is not RECONCILED: %s" % ev[:200])
    ck("does not verify that the bond, CDS and price sources are fresh" in ev, "the evidence status no longer says what it does not verify")
    hero = page.locator("#hero").inner_text()
    cov = doc["coverage"]
    for what, n in (("companies", cov["companies"]), ("listed", cov["listed"]), ("bonds with a spread", cov["bonds_with_spread"]), ("CDS levels", cov["cds_priced"])):
        ck("{:,}".format(n) in hero, "the page's %s is not the document's %s" % (what, n))
    n_down, n_up = sum(1 for x in names if x["signal"] == "CREDIT_LEADS_DOWN"), sum(1 for x in names if x["signal"] == "CREDIT_LEADS_UP")
    ck("%d credit worse" % n_down in hero and "%d credit better" % n_up in hero, "the page's lead counts are not %d down / %d up" % (n_down, n_up))
    log("hero: %s" % " | ".join(x.strip() for x in hero.splitlines() if x.strip())[:500])

    def tiles(book, win, what):
        g = page.evaluate(BOXES)
        want = {k: v for k, v in inds[book].items() if (v.get("value") or 0) > 0}
        ck(g is not None and len(g["boxes"]) == len(want), "%s: %s tiles for %d industries" % (what, g and len(g["boxes"]), len(want)))
        ck(g["over"] == 0 and g["out_of"] == 0, "%s: %d tiles overlap, %d leave the map" % (what, g["over"], g["out_of"]))
        ck(not g["bad"], "%s: the page prints %s" % (what, g["bad"][:3]))
        ck(g["badges_in_map"] == 0 and g["floating"] == 0, "%s: %d section badges on the map, %d floating with no heading" % (what, g["badges_in_map"], g["floating"]))
        wrong, n = [], 0
        for b in g["boxes"]:
            i = want.get(b["ind"])
            if i is None:
                wrong.append("%s is not an industry of the document" % b["ind"])
                continue
            d = (i["win"].get(win) or {}).get("d_bp")
            f = figure(b["title"].split(":", 1)[1].split("·")[0]) if d is not None else None
            n += 1
            if d is not None and not shown(d, b["title"].split(":", 1)[1].split("·")[0]):
                wrong.append("%s shows %s, the document says %s" % (b["ind"], f, d))
            c = rgb(b["bg"])
            if d is not None and c and abs(d) >= 3:
                if d > 0 and not (c[0] > c[1] + 15 and c[0] > c[2] + 10):
                    wrong.append("%s widened %s and is drawn %s" % (b["ind"], d, c))
                if d < 0 and not (c[1] > c[0] + 8):
                    wrong.append("%s tightened %s and is drawn %s" % (b["ind"], d, c))
        ck(not wrong, "%s: %s" % (what, wrong[:4]))
        return g, n

    g, n = tiles("all", doc["primary"], "the heat map")
    ck(all(b["tag"] == "BUTTON" for b in g["boxes"]), "a tile is not a button")
    have = [(i["win"][doc["primary"]]["d_bp"], k) for k, i in inds["all"].items() if (i["win"].get(doc["primary"]) or {}).get("d_bp") is not None]
    widest, calmest = max(have), min(have)
    by_ind = {b["ind"]: b for b in g["boxes"]}
    cw, cc = rgb(by_ind[widest[1]]["bg"]), rgb(by_ind[calmest[1]]["bg"])
    ck(cw[0] - max(cw[1], cw[2]) >= cc[0] - max(cc[1], cc[2]), "the industry widening most (%s) is not the reddest tile: %s vs %s %s" % (widest[1], cw, calmest[1], cc))
    log("heat map: %d tiles, map %s px; widening most %s %+.1f bp drawn rgb%s; widening least %s %+.1f bp drawn rgb%s" % (len(g["boxes"]), g["map"], widest[1], widest[0], cw, calmest[1], calmest[0], cc))
    def bring(sel):
        """Bring an element under the site's top bar, so a picture shows it whole."""
        page.evaluate("(s) => { const e = document.querySelector(s); window.scrollTo(0, e.getBoundingClientRect().top + window.scrollY - 56); }", sel)
        page.wait_for_timeout(350)
    shots["1-top"] = page.screenshot(type="jpeg", quality=72)
    bring("#s-industries")
    shots["2-heat-map"] = page.screenshot(type="jpeg", quality=72)

    rank = page.evaluate("() => Array.from(document.querySelectorAll('#rank .cb-rank')).map(r => Array.from(r.querySelectorAll('.cb-rank-row')).map(x => [x.dataset.ind, x.querySelector('.cb-rank-v').textContent]))")
    ck(len(rank) == 2 and rank[0][0][0] == widest[1] and shown(widest[0], rank[0][0][1]), "the trouble list does not begin with %s %s: %s" % (widest[1], widest[0], rank[:1]))
    ck(len(rank) == 2 and shown(calmest[0], rank[1][0][1]), "the safety list does not begin with the smallest change %s: %s" % (calmest[0], rank[1][:1] if len(rank) > 1 else None))
    heads = page.evaluate("() => Array.from(document.querySelectorAll('#rank h3')).map(h => h.textContent)")
    ck(any("trouble" in h for h in heads) and any("safety" in h for h in heads), "the two lists are not named trouble and safety: %s" % heads)
    log("lists: %s | trouble %s | safety %s" % (heads, rank[0][:3], rank[1][:3] if len(rank) > 1 else None))

    # the controls: a window, a book, a measure
    wins = {k: v for k, v in doc["windows"].items()}
    for wkey in ("w1", "m3", "ytd"):
        btn = page.locator("#controls button[data-win=%s]" % wkey)
        if wins[wkey].get("ref"):
            btn.click()
            page.wait_for_timeout(350)
            tiles("all", wkey, "the heat map, %s" % wins[wkey]["words"])
            head = page.locator("#table th").nth(6).inner_text()
            ck({"w1": "1W", "m3": "3M", "ytd": "YTD"}[wkey] in head, "the table's change column did not follow the window: %s" % head)
            if wkey == "w1":
                bring("#s-industries")
                shots["3-one-week"] = page.screenshot(type="jpeg", quality=72)
        else:
            ck(btn.is_disabled() and bool(btn.get_attribute("title")), "%s has no reference day and is not offered as unavailable with its reason" % wkey)
            log("%s: not available, the button is off and says: %s" % (wins[wkey]["words"], (btn.get_attribute("title") or "")[:110]))
    page.click("#controls button[data-win=%s]" % doc["primary"])
    for book in ("hy", "ig"):
        page.click("#controls button[data-book=%s]" % book)
        page.wait_for_timeout(300)
        tiles(book, doc["primary"], "the heat map, %s" % book)
    page.click("#controls button[data-book=all]")
    page.click("#controls button[data-measure=pct]")
    page.wait_for_timeout(250)
    vals = page.evaluate("() => Array.from(document.querySelectorAll('.cb-box-v')).map(e => e.textContent)")
    ck(vals and all(v.endswith("%") for v in vals), "colour by change in %% does not write percentages: %s" % vals[:3])
    page.click("#controls button[data-measure=level]")
    page.wait_for_timeout(250)
    lv = page.evaluate(BOXES)
    ck(all(shown(inds["all"][b["ind"]]["spread_bp"], b["title"].split(":", 1)[1].split("·")[0]) for b in lv["boxes"]), "colour by spread level does not show each industry's spread")
    page.click("#controls button[data-measure=bp]")
    page.wait_for_timeout(250)

    # an industry's detail, and its companies in the table
    page.click(".cb-box[data-ind='%s']" % widest[1])
    page.wait_for_timeout(300)
    detail = page.locator("#mapdetail").inner_text()
    n_ind = sum(1 for r in table if r["industry"] == widest[1])
    ck("Show its %d companies in the table" % n_ind in detail and "Widened most" in detail, "the detail of %s does not offer its %d companies: %s" % (widest[1], n_ind, detail[:160]))
    bring("#rank")
    shots["4-industry-detail"] = page.screenshot(type="jpeg", quality=72)
    page.click("#mapdetail button[data-show-ind]")
    page.wait_for_timeout(400)
    ck(page.locator("#table select[data-industry]").input_value() == widest[1], "the table was not narrowed to %s" % widest[1])
    col = page.evaluate(COL, ["#table", 1])
    ck(col and set(col) == {widest[1]}, "the narrowed table holds other industries: %s" % sorted(set(col))[:4])
    page.select_option("#table select[data-industry]", "")
    page.wait_for_timeout(300)

    # the table: sorted by every means the page offers
    key = doc["primary"]
    d_col = "d_" + key
    d = [figure(x) for x in page.evaluate(COL, ["#table", 6])]
    top = max(r[d_col] for r in table if r[d_col] is not None)
    ck(len(d) == 100 and is_sorted(d, True) and shown(top, str(d[0])), "the table does not open on the widest change first (%s): %s" % (top, d[:4]))
    page.click("#table th button[data-sort=d]")
    page.wait_for_timeout(300)
    d = [figure(x) for x in page.evaluate(COL, ["#table", 6])]
    low = min(r[d_col] for r in table if r[d_col] is not None)
    ck(is_sorted(d, False) and shown(low, str(d[0])) and page.locator("#table th[aria-sort=ascending]").count() == 1, "a click on the change header does not sort ascending from %s: %s" % (low, d[:4]))
    page.click("#table button[data-dir]")
    page.wait_for_timeout(300)
    d = [figure(x) for x in page.evaluate(COL, ["#table", 6])]
    ck(is_sorted(d, True) and "Descending" in page.locator("#table button[data-dir]").inner_text(), "the direction button does not turn the order round: %s" % d[:4])
    page.select_option("#table select[data-sortsel]", "spread_bp")
    page.wait_for_timeout(300)
    s = [figure(x) for x in page.evaluate(COL, ["#table", 5])]
    ck(is_sorted(s, True) and shown(max(r["spread_bp"] for r in table if r["spread_bp"] is not None), str(s[0])), "Sort by Spread does not put the widest spread first: %s" % s[:4])
    page.click("#table th button[data-sort=industry]")
    page.wait_for_timeout(300)
    a = page.evaluate(COL, ["#table", 1])
    first_ind, last_ind = min(r["industry"] for r in table), max(r["industry"] for r in table)
    ck(a == sorted(a) and a[0] == first_ind, "sorting by industry ascending does not begin with %s: %s" % (first_ind, a[:3]))
    shots["5-table-by-industry"] = page.screenshot(type="jpeg", quality=72)
    page.click("#table th button[data-sort=industry]")
    page.wait_for_timeout(300)
    z = page.evaluate(COL, ["#table", 1])
    ck(z == sorted(z, reverse=True) and z[0] == last_ind, "sorting by industry descending does not begin with %s: %s" % (last_ind, z[:3]))
    log("table: widest change %s first, then ascending from %s, the direction button, Sort by Spread (%s first), by industry %s .. and back from %s" % (top, low, s[0], a[0], z[0]))
    page.check("#table input[data-group]")
    page.wait_for_timeout(400)
    groups = page.evaluate("() => Array.from(document.querySelectorAll('#table tr.cb-grp b')).map(b => b.textContent)")
    ck(sorted(groups) == sorted({r["industry"] for r in table}), "grouped by industry: %d groups for %d industries" % (len(groups), len({r["industry"] for r in table})))
    shots["6-grouped-by-industry"] = page.screenshot(type="jpeg", quality=72)
    page.uncheck("#table input[data-group]")
    page.select_option("#table select[data-sortsel]", "d")
    page.fill("#table input[data-q]", "JPM")
    page.wait_for_timeout(700)
    found = page.evaluate(COL, ["#table", 0])
    jpm = next((r for r in table if r["ticker"] == "JPM"), None)
    ck(jpm is not None and len(found) >= 1 and found[0].startswith("JPM") and shown(jpm["spread_bp"], page.evaluate(COL, ["#table", 5])[0]), "the search does not find JPM with its spread: %s" % found[:3])
    page.fill("#table input[data-q]", "")
    page.wait_for_timeout(700)
    page.click("#table button[data-more=all]")
    page.wait_for_timeout(900)
    n_rows = page.locator("#table tbody tr").count()
    ck(n_rows == len(table), "Show all gives %d rows for %d companies" % (n_rows, len(table)))
    log("table: grouped into %d industries; the search finds JPM at %s bp; Show all lists %d companies" % (len(groups), jpm and jpm["spread_bp"], n_rows))

    # leads
    n_cards = page.locator(".cb-lead").count()
    ck(n_cards == min(8, n_down) + min(8, n_up), "the leads show %d cards for %d down / %d up" % (n_cards, n_down, n_up))
    bring("#leads")
    shots["7-leads"] = page.screenshot(type="jpeg", quality=72)

    # company CDS
    priced = doc["cds"]["priced"]
    bring("#cds")
    m = [figure(x) for x in page.evaluate(COL, ["#cds", 7])]
    ck(len(m) == min(40, len(priced)) and is_sorted(m, True), "the CDS table does not open on the widest one-month change: %s" % m[:4])
    shots["8-company-cds"] = page.screenshot(type="jpeg", quality=72)
    page.click("#cds button[data-cds-all]")
    page.wait_for_timeout(400)
    ck(page.locator("#cds tbody tr").count() == len(priced), "Show all gives %d CDS rows for %d levels" % (page.locator("#cds tbody tr").count(), len(priced)))
    page.select_option("#cds select[data-cds-sortsel]", "spread_bp")
    page.wait_for_timeout(300)
    lv_txt = page.evaluate(COL, ["#cds", 4])
    lv = [figure(x) for x in lv_txt if x.endswith("bp")]
    ck(is_sorted(lv, True), "Sort by Level does not sort the CDS levels: %s" % lv[:4])
    odd = [c for c in priced if c.get("believable") is False]
    ck(all(any("pts upfront" in x for x in lv_txt) for _ in odd), "a points-upfront quote is not shown in points")
    page.click("#cds button[data-cds-dir]")
    page.wait_for_timeout(300)
    lv2 = [figure(x) for x in page.evaluate(COL, ["#cds", 4]) if x.endswith("bp")]
    ck(is_sorted(lv2, False) and shown(min(c["spread_bp"] for c in priced if c.get("believable") is not False), str(lv2[0])), "the CDS direction button does not sort ascending: %s" % lv2[:4])
    names_cds = page.evaluate(COL, ["#cds", 0])
    ck(len(names_cds) == len(priced), "the CDS table lost rows when sorted")
    bare = page.locator("#cds details summary").inner_text()
    ck(str(len(doc["cds"]["no_level"])) in bare, "the names with no level are not counted: %s" % bare)
    log("company CDS: %d levels, widest month first (%s), by level both ways (%s .. %s); %s" % (len(priced), m[:3], lv[:2], lv2[:2], bare))

    # the previous view, one click away, and back
    page.evaluate("() => window.scrollTo(0, 0)")
    page.click("#viewtoggle")
    page.wait_for_timeout(400)
    model = [x for x in names if isinstance(x.get("distance_to_default"), (int, float)) or isinstance(x.get("synthetic_cds_bp"), (int, float))]
    prev_rows = page.locator("#prev tbody tr").count()
    ck(page.locator("#prevview").is_visible() and not page.locator("#newview").is_visible() and prev_rows == len(model), "the previous view shows %d rows for the model's %d names" % (prev_rows, len(model)))
    heads = page.evaluate("() => Array.from(document.querySelectorAll('#prev thead th')).map(t => t.textContent.trim())")
    ck([h.replace(" ▲", "").replace(" ▼", "") for h in heads][:9] == ["Ticker", "Name", "DD", "ΔDD", "Model CDS bp", "ΔCDS", "ΔPrice", "Credit", "Signal"], "the previous view's columns changed: %s" % heads)
    shots["9-previous-view"] = page.screenshot(type="jpeg", quality=72)
    ck(page.locator("#viewtoggle").inner_text() == "Back to the full board", "the way back is not offered")
    page.click("#viewtoggle")
    page.wait_for_timeout(400)
    ck(page.locator("#newview").is_visible() and page.locator(".cb-box").count() == len(g["boxes"]), "the board did not come back from the previous view")
    log("previous view: %d model names, columns %s; and back" % (prev_rows, heads))
    log("page errors: %d %s" % (len(errors), errors[:3]))
    ck(not errors, "the page threw: %s" % errors[:3])

    if phone_page:
        ph, perr = phone_page()
        ph.goto(url, wait_until="domcontentloaded", timeout=90000)
        ph.wait_for_selector(".cb-box", timeout=90000)
        ph.wait_for_timeout(2500)
        ph.locator("#controls").scroll_into_view_if_needed()
        ph.wait_for_timeout(400)
        pg = ph.evaluate(BOXES)
        ck(pg["ox"] <= 1 and pg["over"] == 0 and len(pg["boxes"]) == len(g["boxes"]), "on a phone: %d px of sideways scroll, %d overlaps, %d tiles" % (pg["ox"], pg["over"], len(pg["boxes"])))
        shots["10-phone"] = ph.screenshot(type="jpeg", quality=72)
        log("phone: map %s px, %d tiles, sideways scroll %d px, errors %d" % (pg["map"], len(pg["boxes"]), pg["ox"], len(perr)))
        ck(not perr, "the page threw on a phone: %s" % perr[:3])


def fetch(path, timeout=90):
    req = urllib.request.Request(BASE + path, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def main():
    import boto3
    from ops_report import report
    import cbe_names as N
    import lambda_function as L
    import bank_enrich

    def load(pat):
        spec = importlib.util.spec_from_file_location(pat, str(sorted(Path(__file__).resolve().parent.glob(pat + "_*.py"))[0]))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    A = load("ops_6846")
    s3 = boto3.client("s3", region_name="us-east-1")
    now, stamp = datetime.now(timezone.utc), int(time.time())
    shots, fails = {}, []

    def parse(raw):
        return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)

    def get(key):
        return parse(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())

    with report("ops_6944_credit_before_equity_live_proof") as r:
        r.heading("ops 6944 - Credit Before Equity 2.0: the warehouse's bonds and bars, and a real browser on the live site (read-only), %s" % now.isoformat(timespec="minutes"))

        def ck(cond, msg):
            if cond:
                return True
            fails.append(msg)
            r.warn("CHECK FAILED: " + msg)
            return False

        r.section("0. The live site serves the new files and the engine's 2.0 document")
        served = {}
        for attempt in range(60):
            try:
                html = fetch("/credit-before-equity.html?probe=%d" % (stamp + attempt))
                tag = re.search(rb'<script src="(/jh-credit-board\.js\?v=[0-9a-f]{8})"', html)          # the build stamps each script with its content hash
                served = {"page": b'id="newview"' in html and b'id="viewtoggle"' in html and b'id="s-industries"' in html and bool(tag),
                          "script": bool(tag) and all(mark in fetch(tag.group(1).decode() + "&probe=%d" % (stamp + attempt)) for mark in (b"jh-reskin-skip", b"JHCreditBoard", b"data-jh-nosort", b"cb-cap")),
                          "document": json.loads(fetch("/data/credit-before-equity.json?probe=%d" % (stamp + attempt))).get("contract") == "credit-before-equity.v2"}
            except Exception as e:  # noqa: BLE001
                served = {"error": str(e)[:80]}
            if served and all(v is True for v in served.values()):
                break
            time.sleep(20)
        r.log("served after %d tries: %s" % (attempt + 1, json.dumps(served)))
        if not (served and all(v is True for v in served.values())):
            r.fail("the live site does not serve Credit Before Equity 2.0 yet; nothing was judged")
            sys.exit(1)

        r.section("1. The published document against itself and against the warehouse")
        doc = json.loads(fetch("/data/credit-before-equity.json?probe=%d" % stamp))
        held = get(L.OUT_KEY)
        ck(held.get("generated_at") == doc.get("generated_at"), "the site serves a document other than the warehouse's: %s vs %s" % (doc.get("generated_at"), held.get("generated_at")))
        cov, names = doc["coverage"], doc["names"]
        cols = doc["issuers"]["cols"]
        table = [dict(zip(cols, row)) for row in doc["issuers"]["rows"]]
        leads = [x for x in names if x["signal"] in LEADS]
        r.log("document %s %s, generated %s: bonds of %s, stocks to %s, CDS of %s | %d companies (%d listed), %d leads, %d CDS levels | windows %s" % (
            doc.get("contract"), doc.get("version"), doc.get("generated_at"), doc.get("as_of"), doc.get("stock_as_of"), doc["cds"].get("as_of"), cov["companies"], cov["listed"], len(leads), cov["cds_priced"],
            {k: v.get("ref") for k, v in doc["windows"].items()}))
        r.log("degraded: %s" % doc["degraded"])
        ck(doc["version"] == "2.0.0" and not doc["degraded"], "the document is not a clean 2.0.0 run: %s %s" % (doc["version"], doc["degraded"]))
        ck(doc["n_names"] == len(names) and doc["n_leads"] == len(leads) and doc["leads"] == leads[:20] and doc["n_awaiting_history"] == sum(1 for x in names if x["signal"] == "INSUFFICIENT_HISTORY"),
           "the document's counts are not its rows")
        ck(cov["companies"] == len(table) and cov["listed"] == sum(1 for x in table if x["ticker"]) and all(len(row) == len(cols) for row in doc["issuers"]["rows"]), "the coverage counts are not the company rows")
        ck(len(names) >= 800 and len(table) >= 2000 and cov["cds_priced"] >= 100, "the board is smaller than the dry run's: %d listed, %d companies, %d CDS" % (len(names), len(table), cov["cds_priced"]))
        ck(sum(1 for x in names if isinstance(x.get("distance_to_default"), (int, float))) >= 18, "the first engine's model names are gone")

        bonds_doc = get(L.BONDS_KEY)
        issues = bonds_doc["issues"]
        ck(issues["as_of"] == doc["as_of"], "the board is of %s, the bond desk's holdings are of %s" % (doc["as_of"], issues["as_of"]))
        par_rows = get(L.PAR_KEY)["rows"]
        curve = par_rows.get(doc["as_of"]) or par_rows[max(d for d in par_rows if d <= doc["as_of"])]
        listed_rows = L.listed_companies(bank_enrich.rows(fields=list(L.BOOK_FIELDS)) or [])
        matcher = N.Matcher(listed_rows + [x for x in N.EXTRA_LISTED if x["symbol"] in cov["extra_listed"]])
        usd = [b for b in issues["bonds"] if b.get("ccy") == "USD" and b.get("book") in ("US IG", "US HY", "EM corporate", "EM HY") and not b.get("left")]
        key_ticker = {}
        for b in usd:
            key, cut = N.issuer_key(b.get("issuer") or "")
            if key not in key_ticker:
                co, _ = matcher.match(key, cut, b.get("sector"), foreign=(b.get("country") or "") not in ("", "United States"))
                key_ticker[key] = co["symbol"] if co else None
        by_ticker = {x["ticker"]: x for x in table if x["ticker"]}
        w1 = doc["windows"]["w1"].get("ref")
        week = (get(L.LEDGER_ROOT + w1 + ".json.gz").get("bonds") or {}) if w1 else {}
        for t in ("AAPL", "JPM", "CHTR", "WHR", "XOM"):
            mine = own_spreads([b for b in usd if key_ticker.get(N.issuer_key(b.get("issuer") or "")[0]) == t], curve, issues["as_of"])
            den = sum(v for _, v in mine.values())
            spread = sum(s * v for s, v in mine.values()) / den if den else None
            theirs = by_ticker.get(t) or {}
            ok = spread is not None and theirs.get("spread_bp") is not None and abs(spread - theirs["spread_bp"]) <= 0.6
            line = "%-5s spread by own arithmetic %s bp over %d bonds, the document %s bp" % (t, None if spread is None else round(spread, 1), len(mine), theirs.get("spread_bp"))
            if w1:
                both = [(s - week[k], v) for k, (s, v) in mine.items() if k in week]
                d = sum(x * v for x, v in both) / sum(v for _, v in both) if both else None
                ok = ok and d is not None and theirs.get("d_w1") is not None and abs(d - theirs["d_w1"]) <= 0.6
                line += " | one-week change from the ledger day %s: own %s bp over %d bonds, the document %s bp" % (w1, None if d is None else round(d, 1), len(both), theirs.get("d_w1"))
            r.log(line + (" -> agree" if ok else " -> DIFFER"))
            ck(ok, "the document's %s is not the warehouse's bonds over the curve" % t)

        n_ind = 0
        for book in ("all", "ig", "hy", "em"):
            for ind in doc["industries"][book]:
                for wkey, w in ind["win"].items():
                    col = "d_" + wkey
                    mid = middle([(x[col], x["value"]) for x in table if x["industry"] == ind["industry"] and (book == "all" or x["book"] == book) and x[col] is not None])
                    n_ind += 1
                    ck(mid is not None and abs(mid - w["d_bp"]) < 0.06, "%s (%s, %s): the middle company by value is %s, the document says %s" % (ind["industry"], book, wkey, mid, w["d_bp"]))
        r.log("industries: %d industry-book-window changes, each the middle company by market value held of the company rows" % n_ind)
        month = doc["windows"][doc["primary"]]
        ranked = sorted(doc["industries"]["all"], key=lambda i: -((i["win"].get(doc["primary"]) or {}).get("d_bp") or -1e9))
        r.log("since %s: %s" % (month.get("ref"), "; ".join("%s %+.1f bp (spread %s)" % (i["industry"], i["win"][doc["primary"]]["d_bp"], i["spread_bp"]) for i in ranked if doc["primary"] in i["win"])))

        ref = month.get("ref")
        n_px = 0
        for x in leads[:12] + [y for y in names if y["ticker"] in ("AAPL", "JPM", "XOM")]:
            if x.get("signal_basis") == "traded CDS" or x.get("d_price_pct") is None:
                continue
            raw = None
            for name in L.bank_names(x["ticker"]):
                try:
                    raw = get(L.bank_key(name))
                    break
                except Exception:  # noqa: BLE001
                    continue
            bars = {datetime.fromtimestamp(row[0] / 1000.0, tz=timezone.utc).date().isoformat(): row[4] for row in (raw or {}).get("rows") or [] if row and row[0] and row[4]}
            before = [d for d in sorted(bars) if d <= ref]
            if not before:
                continue
            a, b = bars[before[-1]], bars[max(bars)]
            mine = 100.0 * (b / a - 1.0)
            n_px += 1
            ok = abs(mine - x["d_price_pct"]) <= 0.02 or x["ticker"] in (cov.get("stocks_no_file") or [])
            if not ok:
                r.log("  %s: closes %s on %s and %s on %s give %+.2f%%, the document says %+.2f%% (a split between them would explain it)" % (x["ticker"], a, before[-1], b, max(bars), mine, x["d_price_pct"]))
            if x["signal"] in LEADS:
                down = x["signal"] == "CREDIT_LEADS_DOWN"
                ck((x["d_price_pct"] > -3.0) if down else (x["d_price_pct"] < 3.0), "%s is a lead and its stock already followed (%s%%)" % (x["ticker"], x["d_price_pct"]))
                if x.get("signal_basis") == "bond spreads":
                    ck(abs(x["x_spread_bp"]) >= x["bar_bp"], "%s is a lead and its credit is not past its bar" % x["ticker"])
            ck(ok or abs(mine - x["d_price_pct"]) > 30, "%s: the stock change %s%% is not the price bank's two closes (%+.2f%%)" % (x["ticker"], x["d_price_pct"], mine))
        r.log("stocks: %d changes are the price bank's close of %s over its newest close" % (n_px, ref))
        for x in leads[:10]:
            r.log("  lead %-6s %-32s %-20s spread %s bp, %+.1f bp since %s (%+.1f past its book, bar %s), stock %+.2f%% -> %s" % (
                x["ticker"], str(x["name"])[:32], str(x.get("industry"))[:20], x.get("spread_bp"), x.get("d_spread_bp") or 0, ref, x.get("x_spread_bp") or 0, x.get("bar_bp"), x.get("d_price_pct") or 0, x["signal"]))

        desk = get(L.CDS_KEY)
        desk_rows = {x["name"]: x for g in ("us_corp", "global_corp") for x in ((desk.get("groups") or {}).get(g) or {}).get("rows") or []}
        wrong = [c["name"] for c in doc["cds"]["priced"] if c["name"] not in desk_rows or abs((desk_rows[c["name"]].get("spread_bp") or 0) - c["spread_bp"]) > 0.06]
        ck(not wrong and len(doc["cds"]["priced"]) == sum(1 for x in desk_rows.values() if isinstance(x.get("spread_bp"), (int, float))), "the CDS levels are not the CDS desk's: %s" % wrong[:4])
        r.log("CDS: %d levels, each the CDS desk's own (%s); %d tied to a listed stock; %d trade with no level" % (len(doc["cds"]["priced"]), desk.get("as_of"), cov["cds_priced_tied"], len(doc["cds"]["no_level"])))

        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)
            r.section("2. The page in a real browser")

            def new_page(phone=False):
                ctx = browser.new_context(viewport={"width": 390, "height": 844}, user_agent=UA, device_scale_factor=2, is_mobile=True, has_touch=True) if phone else browser.new_context(viewport={"width": 1500, "height": 950}, user_agent=UA)
                page, errors = ctx.new_page(), []
                page.on("pageerror", lambda e: errors.append(str(e)[:200]))
                return page, errors
            browse(new_page, BASE + "/credit-before-equity.html?probe=%d" % stamp, doc, ck, shots, r.log, phone_page=lambda: new_page(True))
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(companies=cov["companies"], listed=cov["listed"], leads=len(leads), cds_levels=cov["cds_priced"], industries=len(doc["industries"]["all"]), failed_checks=len(fails), document=doc["generated_at"][:16])
        for f in fails[:40]:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("LIVE: %d companies (%d with a listed stock) from the bond desk's own bonds, %d industries on one heat map with the document's figures and colours, the table sorted every way the page offers, %d company CDS levels, the previous view one click away" % (
            cov["companies"], cov["listed"], len(doc["industries"]["all"]), cov["cds_priced"]))


if __name__ == "__main__":
    main()
