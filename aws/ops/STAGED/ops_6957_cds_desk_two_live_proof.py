"""ops 6957 - CDS desk 2.0: the published documents worked again, then a real browser on the live site.

Khalid 2026-11-25: "audit and improve the CDS engine next and improve it also to include more companies and all asset
classes and even sovereign CDS and public companies worldwide. Make sure you use beautiful visuals for easy disgesting
the data and make sure all the engine data is visible on the page".

Read-only: no engine is invoked and nothing is written. Arithmetic here is written here, not the engine's.

  1. The documents as the site serves them: the counts are the rows; nobody is on the desk twice; the audit's names are
     no longer at single digits; a level lies inside what that day's own prints (the tape document) can mean; every
     product class adds to the window's total; every drawn option strike is near its index; a listing is in the part of
     the world the swap's currency says; no level is past the ceiling; nothing shaped like an ISIN is published.
  2. The page in a real browser on justhodl.ai: one card an index with the document's level; the heat map has a tile a
     sector with the document's middle figure and a colour that means what it says; the group, window and measure
     controls change it; a tile opens its names; the movers are the document's; a name opens everything the desk has on
     it with the prints behind the level; products, options, mortgage indices, quiet names, repairs and the whole
     document draw from the document; the subordinated table and the map's reference quotes; every 1.6 section is still
     there, where it was.
  3. A phone.

Safe to run again.
"""
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import base64
import importlib.util
import json
import re
import statistics
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops")]
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
MINUS = "−"
PRICED = ("liquid", "thin", "aged", "bracketed")
EUROPE = set("GB FR DE IT ES NL BE LU IE PT AT FI SE DK NO CH GR PL CZ HU JE GG IM MT CY IS LI MC".split())
HOME = {"EUR": EUROPE, "GBP": EUROPE, "CHF": EUROPE, "JPY": {"JP"}}
GROUP_WORDS = {"all": "All corporates", "us_corp": "U.S.", "global_corp": "Global", "sub": "Subordinated", "sovereign": "Sovereigns"}
OLD_SECTIONS = ("Index strip", "World sovereign map", "Sovereign 5Y CDS", "Bank–sovereign nexus", "U.S. corporate 5Y CDS", "Global (non-U.S.) corporate 5Y CDS", "Active but unpriced", "Biggest 1-day moves",
                "Tape activity", "Data sources")
NEW_SECTIONS = ("The credit market at a glance", "Subordinated bank and corporate 5Y CDS", "Every product of the credit file", "Options on the indices", "Commercial-mortgage indices (CMBX)", "Quiet names",
                "What the desk repaired, and what it holds back", "Everything the engine published")
TEXT = r"""(sel) => { const e = document.querySelector(sel); return e ? e.innerText : null; }"""
COUNT = r"""(sel) => document.querySelectorAll(sel).length"""
TILES = r"""() => Array.from(document.querySelectorAll('#cds2-heat .cv-tile')).map(g => { const r = g.querySelector('rect'), v = g.querySelector('.cv-tile-val'), t = g.querySelector('title');
  return { key: g.getAttribute('data-v'), fill: r.getAttribute('fill'), val: v ? v.textContent : null, title: t ? t.textContent : '' }; })"""
BAD = r"""(sel) => { const e = document.querySelector(sel); if (!e) return ['missing ' + sel]; return (e.innerText.match(/\bNaN\b|\bundefined\b|\[object Object\]|Infinity(?! [A-Z])/g) || []).slice(0, 5); }"""
OVER = r"""() => document.documentElement.scrollWidth - document.documentElement.clientWidth"""


def fetch(path, timeout=120):
    req = urllib.request.Request(BASE + path, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def med(xs):
    xs = [x for x in xs if isinstance(x, (int, float))]
    return statistics.median(xs) if xs else None


def rows_of(doc, grp):
    out = []
    for g in (("us_corp", "global_corp", "sub") if grp == "all" else (grp,)):
        for x in (doc["groups"].get(g) or {}).get("rows") or []:
            out.append(dict(x, group=g))
    return out


def tiles_of(doc, grp, col):
    """One tile a sector (a region for sovereigns): the middle name's change, written here."""
    by = {}
    for x in rows_of(doc, grp):
        if x["status"] in PRICED:
            by.setdefault((x.get("region") if grp == "sovereign" else x.get("sector")) or "Other", []).append(x)
    return {k: {"n": len(v), "v": med([x.get(col) for x in v]), "level": med([x.get("spread_bp") for x in v]), "keys": [x["key"] for x in v]} for k, v in by.items()}


def fx(v, dp=1):
    """A figure as the browser writes it: the shortest decimal that is this float, halves away from zero (Python's own
    format rounds 117.25 to 117.2 and a middle of -9.35 to -9.3; the page writes 117.3 and -9.4)."""
    q = Decimal(repr(abs(float(v)))).quantize(Decimal(1).scaleb(-dp), rounding=ROUND_HALF_UP)
    return "{:,.{dp}f}".format(q, dp=dp)


def signed(v, dp=1):
    if v is None:
        return "—"
    return ("+" if v > 0 else MINUS if v < 0 else "") + fx(v, dp)


def rgb(hexa):
    return tuple(int(hexa[i:i + 2], 16) for i in (1, 3, 5))


def check_documents(doc, tape, hist, ck, log):
    groups = doc["groups"]
    ck(doc.get("version") == "2.0.0" and sorted(groups) == ["global_corp", "sovereign", "sub", "us_corp"], "the document is not 2.0 with its four groups: %s %s" % (doc.get("version"), sorted(groups)))
    every = [(g, kind, x) for g, blk in groups.items() for kind in ("rows", "unpriced", "dormant", "quiet") for x in blk.get(kind) or []]
    for g, blk in groups.items():
        st = [x["status"] for x in blk["rows"]]
        ck(blk["n_priced"] == len(blk["rows"]) == sum(1 for s in st if s in PRICED), "%s: the count of names with a level is not its rows (%s, %d rows)" % (g, blk["n_priced"], len(blk["rows"])))
        ck(blk["n_unpriced"] == len(blk["unpriced"]) and blk["n_quiet"] == len(blk["quiet"]) and blk["n_aged"] == st.count("aged") and blk["n_bracketed"] == sum(1 for x in blk["rows"] if x.get("spread_basis") == "bracketed"),
           "%s: the declared counts are not the lists" % g)
        names = [x["name"] for x in blk["rows"]]
        twice = sorted({n for n in names if names.count(n) > 1})
        ck(not twice, "%s carries a name twice: %s" % (g, twice[:5]))
    keys = [x["key"] for g, kind, x in every]
    ck(len(set(keys)) == len(keys), "a key is on the desk in two lists: %s" % sorted({k for k in keys if keys.count(k) > 1})[:5])
    by_key = {x["key"]: x for g, kind, x in every if kind == "rows"}
    att, exp = by_key.get("AT & T"), by_key.get("EXPEDIA")
    ck(att is not None and 40 < att["spread_bp"] < 400 and "AT&T" not in by_key, "AT&T (7 bp and listed twice in 1.6): %s" % (att and att["spread_bp"]))
    ck(exp is not None and 40 < exp["spread_bp"] < 400, "Expedia (18 bp in 1.6): %s" % (exp and exp["spread_bp"]))
    low = sorted((x["spread_bp"], x["name"]) for g, kind, x in every if kind == "rows" and g != "sovereign" and x["spread_bp"] < 8)
    ck(not low, "a company under 8 bp is the misread coupon again: %s" % low[:6])
    ck(all(x["spread_bp"] <= 10000 for x in by_key.values()), "a level is past the 10,000 bp ceiling")
    # a level lies inside what that day's own 5-year prints can mean (the tape document), worked here from the tape
    cols = {c: i for i, c in enumerate(tape["cols"])}
    inside, tried, outside = 0, 0, []
    for key, x in by_key.items():
        prints = [p for p in (tape["names"].get(key) or []) if p[cols["date"]] == x["last_date"] and 4.0 <= p[cols["tenor"]] < 6.0]
        cands = [v for p in prints for v in (p[cols["quoted_bp"]], p[cols["above_bp"]], p[cols["below_bp"]]) if isinstance(v, (int, float))]
        if not cands:
            continue
        tried += 1
        if min(cands) * 0.97 - 0.5 <= x["spread_bp"] <= max(cands) * 1.03 + 0.5:
            inside += 1
        else:
            outside.append((x["name"], x["spread_bp"], round(min(cands), 1), round(max(cands), 1)))
    ck(tried >= 150 and len(outside) <= max(2, tried // 100), "levels outside what their own day's prints can mean: %d of %d %s" % (len(outside), tried, outside[:6]))
    log("levels against their own day's prints in the tape document: %d of %d inside the prints' range%s" % (inside, tried, (" | outside: %s" % outside[:4]) if outside else ""))
    ck(not re.search(r"\b[A-Z]{2}[A-Z0-9]{9}[0-9]\b", json.dumps(tape["names"])) and not [c for c in tape["cols"] if re.search(r"isin|cusip|underlier", c, re.I)], "the tape document carries something shaped like an identifier")
    # products, options, listings
    for w, win in doc["products"]["windows"].items():
        ck(sum(c["n"] for c in win["classes"]) == win["n"], "products %s: the classes do not add to the window (%s)" % (w, win["n"]))
        ck(all(c["usd_mm"] < 5e6 for c in win["classes"]), "products %s: a class worth trillions is a mis-filed notional" % w)
    for f in doc["options"]["families"]:
        lvl = f["index_level"]
        if f["strike_kind"] == "spread" and lvl:
            ck(all(0.5 < r["strike"] / lvl < 2.7 for r in f["ladder"]), "options %s: a drawn strike is far off the index" % f["family"])
        ck(all(e["expiry"] < "2029" for e in f["expiries"]), "options %s: an expiry years out is the index's maturity" % f["family"])
    away = [(x["name"], x.get("ccy"), x["listing"]["symbol"], x["listing"]["country"]) for g, kind, x in every if x.get("listing") and x.get("ccy") in HOME and x["listing"]["country"] not in HOME[x["ccy"]]]
    ck(not away, "a listing outside the part of the world the swap's currency says: %s" % away[:5])
    cov = groups["sovereign"]["coverage"]
    uni = groups["sovereign"]["universe"]
    ck(cov["known"] == len(uni) and cov["priced"] == sum(1 for u in uni if u["status"] in PRICED) and cov["reference_only"] == sum(1 for u in uni if u["status"] not in PRICED and u.get("ref_bp") is not None),
       "sovereign coverage is not the known-issuer list: %s" % cov)
    ck(len(doc["indices"]) >= 12 and all(i.get("status") in ("current", "aged") for i in doc["indices"]), "indices: %d, statuses %s" % (len(doc["indices"]), sorted({str(i.get("status")) for i in doc["indices"]})))
    ck(set(hist["names"]) >= {"AT & T", "EXPEDIA"} and max(p[1] for p in hist["names"]["AT & T"]["points"]) < 400 and min(p[1] for p in hist["names"]["AT & T"]["points"][-200:]) > 30,
       "AT&T's history still carries the misread days: %s" % sorted(p[1] for p in hist["names"]["AT & T"]["points"])[:3])
    tied = sum(1 for g, kind, x in every if x.get("ticker"))
    listed = sum(1 for g, kind, x in every if x.get("listing"))
    log("document %s as of %s, generated %s: with a level %s | trade with no level %s | quiet %s | indices %d (%s aged) | tied to the credit board %d, with a listing %d on %d exchanges | sovereign %s" % (
        doc["version"], doc["as_of"], doc["generated_at"], {g: b["n_priced"] for g, b in groups.items()}, {g: b["n_unpriced"] for g, b in groups.items()}, {g: b["n_quiet"] for g, b in groups.items()},
        len(doc["indices"]), [i["name"] for i in doc["indices"] if i.get("status") == "aged"], tied, listed, len({x["listing"]["exchange"] for g, kind, x in every if x.get("listing")}), cov))
    log("AT&T %s bp (%s, %s; bonds %s) | Expedia %s bp (%s) | products 30d %s | options %s | tape %s names, %s prints" % (
        att and att["spread_bp"], att and att["status"], att and att.get("anchor_source"), att and att.get("bond_bp"), exp and exp["spread_bp"], exp and exp["status"],
        {c["cls"]: c["n"] for c in doc["products"]["windows"]["30d"]["classes"]}, {f["label"]: f["n"] for f in doc["options"]["families"]}, tape.get("n_names"), tape.get("n_prints")))
    return by_key


def browse(new_page, url, doc, tape, ck, shots, log, phone_page=None):
    page, errors = new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_selector("#cds2-glance .cv-card", timeout=120000)
    page.wait_for_selector("#cds2-data .cv-node", timeout=60000)
    page.wait_for_timeout(2500)
    groups = doc["groups"]
    ck((page.evaluate(TEXT, "#cds-badge") or "").strip() == "v2.0" and "v2.0.0" in (page.evaluate(TEXT, "#ts") or ""), "the header does not say 2.0: %r %r" % (page.evaluate(TEXT, "#cds-badge"), page.evaluate(TEXT, "#ts")))
    heads = page.evaluate("() => Array.from(document.querySelectorAll('#cdsdesk-root h3')).map(h => h.innerText.replace(/\\s+/g, ' ').trim())")
    gone = [s for s in OLD_SECTIONS if not any(s in h for h in heads)]
    new = [s for s in NEW_SECTIONS if not any(s in h for h in heads)]
    ck(not gone, "a 1.6 section is gone from the page: %s" % gone)
    ck(not new, "a 2.0 section is not on the page: %s" % new)
    order = [next((i for i, h in enumerate(heads) if s in h), -1) for s in ("The credit market at a glance", "Index strip", "World sovereign map", "U.S. corporate 5Y CDS", "Subordinated bank", "Every product", "Data sources", "Everything the engine published")]
    ck(order == sorted(order) and -1 not in order, "the sections are not in the order the page was built in: %s" % order)

    # ---- index cards
    cards = page.evaluate("() => Array.from(document.querySelectorAll('#cds2-glance .cv-card')).map(c => ({ key: c.getAttribute('data-key'), text: c.innerText.replace(/\\s+/g, ' '), aged: c.classList.contains('cv-aged') }))")
    ck(len(cards) == len(doc["indices"]), "index cards: %d for %d indices" % (len(cards), len(doc["indices"])))
    wrong = []
    for i in doc["indices"]:
        c = next((c for c in cards if c["key"] == "IDX:" + i["key"]), None)
        want = fx(i["price"], 2) if (i["quote"] == "price" and i.get("price") is not None) else fx(i["spread_bp"], 1)
        if c is None or want not in c["text"] or c["aged"] != (i.get("status") == "aged") or (c["aged"] and ("last printed " + i["last_date"]) not in c["text"]):
            wrong.append((i["key"], want, c and c["text"][:60]))
    ck(not wrong, "an index card does not show the document's level or age: %s" % wrong[:3])
    page.evaluate("document.getElementById('cds2-glance').scrollIntoView({block:'start'})")
    page.wait_for_timeout(400)
    shots["01-at-a-glance"] = page.screenshot(type="jpeg", quality=74)

    # ---- heat map: the document's figure, a colour that means what it says, and controls that change it
    def tiles_match(grp, col, label):
        want, got = tiles_of(doc, grp, col), page.evaluate(TILES)
        ck(len(got) == len(want) and {t["key"] for t in got} == set(want), "heat map %s: %d tiles for %d %s" % (label, len(got), len(want), "regions" if grp == "sovereign" else "sectors"))
        off, hues = [], []
        for t in got:
            w = want.get(t["key"])
            if not w:
                continue
            if t["val"] is not None and w["v"] is not None and t["val"] != signed(w["v"]) + " bp":
                off.append((t["key"], t["val"], signed(w["v"])))
            if ("%d names" % w["n"]) not in t["title"]:
                off.append((t["key"], "names", w["n"]))
            if w["v"] is not None and abs(w["v"]) > 0.05:
                r_, g_, b_ = rgb(t["fill"])
                hues.append((w["v"] > 0) == (r_ > g_))        # wider leans red, tighter leans green
        ck(not off, "heat map %s: a tile's figure is not the middle name's, worked here: %s" % (label, off[:4]))
        ck(hues and all(hues), "heat map %s: a tile's colour does not mean what it says (wider red, tighter green)" % label)
        return want, got
    want, got = tiles_match("all", "chg_1m_bp", "all corporates, one month")
    log("heat map: %d sector tiles, each the middle name's one-month change worked here (e.g. %s)" % (len(got), [(t["key"], t["val"]) for t in got[:4]]))
    page.click("#cds2-glance [data-cv='win'][data-v='1w']")
    page.wait_for_timeout(300)
    tiles_match("all", "chg_1w_bp", "all corporates, one week")
    page.click("#cds2-glance [data-cv='grp'][data-v='sovereign']")
    page.wait_for_timeout(300)
    w_sov, g_sov = tiles_match("sovereign", "chg_1w_bp", "sovereigns, one week")
    ck(page.evaluate("() => document.querySelector(\"#cds2-glance [data-cv='grp'][data-v='sovereign']\").classList.contains('on') && !document.querySelector(\"#cds2-glance [data-cv='grp'][data-v='all']\").classList.contains('on')"),
       "the group control does not show which group is drawn")
    shots["02-heat-sovereigns"] = page.screenshot(type="jpeg", quality=74)
    page.click("#cds2-glance [data-cv='measure'][data-v='level']")
    page.wait_for_timeout(300)
    lv = page.evaluate(TILES)
    ck(all(t["val"] is None or t["val"] == fx(w_sov[t["key"]]["level"], 0) + " bp" for t in lv), "coloured by level, a tile does not show the middle level: %s" % [(t["key"], t["val"]) for t in lv[:3]])
    page.click("#cds2-glance [data-cv='grp'][data-v='all']")
    page.click("#cds2-glance [data-cv='win'][data-v='1m']")
    page.click("#cds2-glance [data-cv='measure'][data-v='chg']")
    page.wait_for_timeout(300)
    big = max(want, key=lambda k: want[k]["n"])
    page.evaluate("(k) => Array.from(document.querySelectorAll('#cds2-heat .cv-tile')).find(g => g.getAttribute('data-v') === k).dispatchEvent(new MouseEvent('click', { bubbles: true }))", big)
    page.wait_for_timeout(400)
    listed = page.evaluate("() => Array.from(document.querySelectorAll('#cds2-pick tbody tr .cv-name')).map(e => e.getAttribute('data-key'))")
    ck(sorted(listed) == sorted(want[big]["keys"]), "the %s tile opens %d names for the %d it counts" % (big, len(listed), want[big]["n"]))
    log("the %s tile opens its %d names" % (big, len(listed)))
    page.click("#cds2-pick [data-cv='pick']")
    page.wait_for_timeout(200)
    ck(page.evaluate(COUNT, "#cds2-pick tbody tr") == 0, "a tile's list does not close")

    # ---- movers: the document's
    liquid = [x for x in rows_of(doc, "all") if x.get("liquid") and isinstance(x.get("chg_1m_bp"), (int, float)) and x.get("points_upfront") is None and (x["spread_bp"] or 0) < 1000]
    up = sorted((x for x in liquid if x["chg_1m_bp"] > 0), key=lambda x: -x["chg_1m_bp"])[:10]
    seen = page.evaluate("() => Array.from(document.querySelectorAll('#cds2-movers .cv-two > div:first-child .cv-bar-row')).map(r => [r.querySelector('.cv-name').getAttribute('data-key'), r.querySelector('.cv-bar-val').innerText.trim()])")
    ck([s[0] for s in seen] == [x["key"] for x in up] and all(s[1] == signed(x["chg_1m_bp"]) for s, x in zip(seen, up)), "the widened-most list is not the document's: %s vs %s" % (seen[:3], [(x["key"], x["chg_1m_bp"]) for x in up[:3]]))
    log("widened most in a month: %s" % [(x["name"], x["chg_1m_bp"], x["spread_bp"]) for x in up[:5]])

    # ---- a name's sheet: AT&T
    att = next(x for x in groups["us_corp"]["rows"] if x["key"] == "AT & T")
    page.evaluate("() => { const e = Array.from(document.querySelectorAll('#cdsdesk-us .cds-clickable, #cdsdesk-root .cds-clickable')).find(e => e.getAttribute('data-key') === 'AT & T'); e.scrollIntoView({block:'center'}); e.click(); }")
    page.wait_for_selector("#cds2-sheet", timeout=30000)
    page.wait_for_selector("#cds2-tape table tbody tr", timeout=60000)
    sheet = page.evaluate(TEXT, "#cds2-sheet")
    ck("Everything the desk has on AT&T" in sheet and fx(att["spread_bp"], 1) in sheet and (att.get("bond_bp") is None or fx(att["bond_bp"], 0) in sheet) and "T · AT&T Inc." in sheet,
       "AT&T's sheet does not carry its level, its bonds and its stock: %s" % sheet[:200].replace("\n", " | "))
    n_tape = page.evaluate(COUNT, "#cds2-tape table tbody tr")
    ck(n_tape == len(tape["names"]["AT & T"]), "AT&T's sheet lists %d prints for the %d in the tape document" % (n_tape, len(tape["names"]["AT & T"])))
    figs = page.evaluate(COUNT, "#cds2-sheet .cv-fig")
    ck(figs >= 40 and not page.evaluate(BAD, "#cds2-sheet"), "the sheet's figures: %d, bad text %s" % (figs, page.evaluate(BAD, "#cds2-sheet")))
    ck(page.evaluate("() => { const m = document.querySelector('#cdsdesk-modal'); return !!m && getComputedStyle(m).display !== 'none' && !!m.querySelector('svg'); }"), "the name's window lost its history chart")
    page.evaluate("document.getElementById('cds2-sheet').scrollIntoView({block:'start'})")
    page.wait_for_timeout(300)
    shots["03-name-sheet"] = page.screenshot(type="jpeg", quality=74)
    page.evaluate("document.getElementById('cds2-tape').scrollIntoView({block:'center'})")
    page.wait_for_timeout(300)
    # the prints sort both ways on a column
    first = page.evaluate("() => { const th = document.querySelectorAll('#cds2-tape table thead th')[3]; th.click(); return Array.from(document.querySelectorAll('#cds2-tape table tbody tr')).map(tr => parseFloat(tr.children[3].innerText)); }")
    second = page.evaluate("() => { const th = document.querySelectorAll('#cds2-tape table thead th')[3]; th.click(); return Array.from(document.querySelectorAll('#cds2-tape table tbody tr')).map(tr => parseFloat(tr.children[3].innerText)); }")
    ck(first != second and sorted(first) in (first, second) and sorted(first, reverse=True) in (first, second), "the prints do not sort both ways by size")
    shots["04-prints-behind-the-level"] = page.screenshot(type="jpeg", quality=74)
    log("AT&T's sheet: level %s bp beside its bonds at %s bp, %d figures, %d prints listed and sorted both ways" % (att["spread_bp"], att.get("bond_bp"), figs, n_tape))
    page.keyboard.press("Escape")
    page.evaluate("() => { const b = document.querySelector('#cdsdesk-modal .close, #cdsdesk-modal [data-close], #cdsdesk-modal button'); const m = document.querySelector('#cdsdesk-modal'); if (m && getComputedStyle(m).display !== 'none' && b) b.click(); }")
    page.wait_for_timeout(300)

    # ---- products
    pr = doc["products"]
    for w in ("1d", "30d", "5d"):
        page.click("#cds2-products [data-cv='prod'][data-v='%s']" % w)
        page.wait_for_timeout(250)
        txt = page.evaluate(TEXT, "#cds2-products")
        ck("{:,}".format(pr["windows"][w]["n"]) + " new trades" in txt and all("{:,}".format(c["n"]) in txt for c in pr["windows"][w]["classes"]), "products %s: the page does not show the window's counts" % w)
    ck(page.evaluate(COUNT, "#cds2-products .cv-prod-row") == len(pr["windows"]["5d"]["classes"]), "products: a class has no bar")
    page.evaluate("document.getElementById('cds2-products').scrollIntoView({block:'start'})")
    page.wait_for_timeout(300)
    shots["05-every-product"] = page.screenshot(type="jpeg", quality=74)
    # ---- options
    lad = page.evaluate("() => Array.from(document.querySelectorAll('#cds2-options .cv-opt-card')).map(c => ({ name: c.querySelector('b').innerText, bars: c.querySelectorAll('svg rect').length, exp: c.querySelectorAll('.cv-exp-row').length, text: c.innerText }))")
    fams = doc["options"]["families"]
    ck(len(lad) == len(fams) and all(l["bars"] == len(f["ladder"]) and l["exp"] == len(f["expiries"]) and l["name"] == f["label"] for l, f in zip(lad, fams)), "options: a ladder or an expiry list is not the document's: %s" % [(l["name"], l["bars"], l["exp"]) for l in lad])
    ck(all(("%d%%" % round(f["share_struck_wider_pct"])) in l["text"] for l, f in zip(lad, fams) if f.get("share_struck_wider_pct") is not None), "options: the struck-wider share is not the document's")
    page.evaluate("document.getElementById('cds2-options').scrollIntoView({block:'start'})")
    page.wait_for_timeout(300)
    shots["06-options"] = page.screenshot(type="jpeg", quality=74)
    # ---- mortgage indices, quiet names, repairs
    st_rows = doc["structured"]["rows"]
    ck(page.evaluate(COUNT, "#cds2-structured details tbody tr") == len(st_rows), "mortgage indices: %d rows for %d series" % (page.evaluate(COUNT, "#cds2-structured details tbody tr"), len(st_rows)))
    quiet_n = sum(b["n_quiet"] for b in groups.values())
    page.click("#cds2-quiet [data-cv='quiet']")
    page.wait_for_timeout(400)
    ck(page.evaluate(COUNT, "#cds2-quiet tbody tr") == quiet_n, "quiet names: the whole list is %d rows for %d names" % (page.evaluate(COUNT, "#cds2-quiet tbody tr"), quiet_n))
    page.click("#cds2-quiet [data-cv='quiet']")
    page.wait_for_timeout(300)
    ck(page.evaluate(COUNT, "#cds2-quiet tbody tr") == min(12, quiet_n), "quiet names: the short view does not come back")
    q = doc["quality"]
    qt = page.evaluate(TEXT, "#cds2-quality")
    ck("{:,}".format(q["coupon_written_as"].get("percent", 0)) in qt and "{:,}".format(q["n_aged"]) in qt and "{:,}".format(q["n_bracketed"]) in qt, "the repairs panel does not show the document's counts")
    page.evaluate("document.getElementById('cds2-structured').scrollIntoView({block:'start'})")
    page.wait_for_timeout(300)
    shots["07-mortgage-quiet-repairs"] = page.screenshot(type="jpeg", quality=74)
    # ---- the whole document
    top = page.evaluate("() => Array.from(document.querySelectorAll('#cds2-data > .panel > .cv-tree > .cv-node > summary > b, #cds2-data > .panel > .cv-tree > .cv-leaf > .cv-key')).map(e => e.innerText.trim())")
    ck(sorted(top) == sorted(doc.keys()), "the whole-document section does not list every key: missing %s" % sorted(set(doc) - set(top)))
    page.evaluate("""() => { const open = (n) => { n.open = true; n.dispatchEvent(new Event('toggle')); };
        const g = Array.from(document.querySelectorAll('#cds2-data .cv-node')).find(n => n.querySelector('summary b').innerText.trim() === 'groups'); open(g); }""")
    page.wait_for_timeout(400)
    kids = page.evaluate("() => { const g = Array.from(document.querySelectorAll('#cds2-data .cv-node')).find(n => n.querySelector('summary b').innerText.trim() === 'groups'); return Array.from(g.querySelectorAll(':scope > .cv-node-body > .cv-tree > .cv-node > summary > b')).map(e => e.innerText.trim()); }")
    ck(sorted(kids) == sorted(groups), "the groups branch does not open to its four groups: %s" % kids)
    page.evaluate("document.getElementById('cds2-data').scrollIntoView({block:'start'})")
    page.wait_for_timeout(300)
    shots["08-everything-published"] = page.screenshot(type="jpeg", quality=74)
    # ---- subordinated table and the map
    sub = groups["sub"]
    n_sub = page.evaluate(COUNT, "#cdsdesk-sb tbody tr")
    ck(n_sub == len(sub["rows"]) + len(sub["unpriced"]) + len(sub["dormant"]), "subordinated table: %d rows for %d names" % (n_sub, len(sub["rows"]) + len(sub["unpriced"]) + len(sub["dormant"])))
    page.evaluate("document.getElementById('cds2-sub-panel').scrollIntoView({block:'start'})")
    page.wait_for_timeout(300)
    shots["09-subordinated"] = page.screenshot(type="jpeg", quality=74)
    approx = page.evaluate("() => document.querySelectorAll('#cdsdesk-map path[stroke-dasharray]').length")
    labels = page.evaluate("() => Array.from(document.querySelectorAll('#cdsdesk-map text')).filter(t => t.textContent.indexOf('≈') === 0).length")
    ref_only = groups["sovereign"]["coverage"]["reference_only"]
    ck(approx >= max(3, ref_only // 2) and labels >= 1 and "reference quote, not a print" in page.evaluate("() => document.querySelector('#cdsdesk-map').parentElement.innerText"),
       "the map does not mark the reference quotes: %d countries drawn with a broken outline, %d labelled, for %d in the document" % (approx, labels, ref_only))
    page.evaluate("document.getElementById('cdsdesk-map').scrollIntoView({block:'center'})")
    page.wait_for_timeout(400)
    shots["10-world-map"] = page.screenshot(type="jpeg", quality=74)
    us_rows = page.evaluate(COUNT, "#cdsdesk-us tbody tr")
    usb = groups["us_corp"]
    ck(us_rows == len(usb["rows"]) + len(usb["unpriced"]) + len(usb["dormant"]), "U.S. table: %d rows for %d names" % (us_rows, len(usb["rows"]) + len(usb["unpriced"]) + len(usb["dormant"])))
    # the listed stock beside each company: the credit board's tie, else its home listing anywhere in the world
    heads_gl = page.evaluate("() => Array.from(document.querySelectorAll('#cdsdesk-gl thead th')).map(t => t.innerText.replace(/[^A-Za-z0-9 −]/g, '').trim().toLowerCase())")
    ck("stock" in heads_gl and "its bonds" in heads_gl, "the global table has no stock or bond column: %s" % heads_gl)
    si = heads_gl.index("stock") if "stock" in heads_gl else 2
    gl = page.evaluate("(i) => Array.from(document.querySelectorAll('#cdsdesk-gl tbody tr')).map(tr => [tr.children[0].getAttribute('data-key'), tr.children[i].innerText.replace(/\\s+/g, ' ').trim()])", si)
    glb = groups["global_corp"]
    want_stock = {x["key"]: (x.get("ticker") or (x.get("listing") or {}).get("symbol") or "—") for kind in ("rows", "unpriced", "dormant") for x in glb[kind]}
    off = [(k, cell, want_stock.get(k)) for k, cell in gl if k in want_stock and not cell.startswith(want_stock[k])]
    listed_gl = sum(1 for k, cell in gl if cell != "—")
    ck(not off and listed_gl >= 60, "the global table's stock column is not the document's: %d listed, off %s" % (listed_gl, off[:4]))
    exchanges = sorted({x["listing"]["exchange"] for kind in ("rows", "unpriced", "dormant") for x in glb[kind] if x.get("listing")})
    log("global corporates: %d of %d rows show a listed stock, on %d exchanges (%s)" % (listed_gl, len(gl), len(exchanges), ", ".join(exchanges[:14])))
    bad = page.evaluate(BAD, "#cdsdesk-root")
    ck(not bad and page.evaluate(OVER) <= 2 and not errors, "bad text %s, sideways overflow %s, page errors %s" % (bad, page.evaluate(OVER), errors[:3]))
    log("page: %d index cards, %d ladders, %d mortgage series, %d quiet names opened whole, %d keys of the document listed, %d subordinated rows, %d reference quotes on the map, %d U.S. rows, no bad text, no error" % (
        len(cards), len(lad), len(st_rows), quiet_n, len(top), n_sub, approx, us_rows))

    if phone_page:
        ph, ph_errors = phone_page()
        ph.goto(url, wait_until="domcontentloaded", timeout=120000)
        ph.wait_for_selector("#cds2-glance .cv-card", timeout=120000)
        ph.wait_for_timeout(2500)
        vb = ph.evaluate("() => document.querySelector('#cds2-heat svg').getAttribute('viewBox')")
        colsn = ph.evaluate("() => getComputedStyle(document.querySelector('#cds2-glance .cv-cards')).gridTemplateColumns.split(' ').length")
        ck(vb == "0 0 380 540" and colsn == 2 and ph.evaluate(OVER) <= 2, "on a phone: heat map %s, index cards %s across, sideways overflow %s" % (vb, colsn, ph.evaluate(OVER)))
        ph.evaluate("document.querySelector('#cds2-heat').scrollIntoView({block:'center'})")
        ph.wait_for_timeout(300)
        shots["11-phone-heat-map"] = ph.screenshot(type="jpeg", quality=72)
        ph.evaluate("() => { const e = Array.from(document.querySelectorAll('#cdsdesk-root .cds-clickable')).find(e => e.getAttribute('data-key') === 'AT & T'); e.scrollIntoView({block:'center'}); e.click(); }")
        ph.wait_for_selector("#cds2-sheet", timeout=30000)
        ph.wait_for_timeout(1200)
        w = ph.evaluate("() => { const p = document.querySelector('#cdsdesk-modal .panel') || document.querySelector('#cds2-sheet'); return [Math.round(p.getBoundingClientRect().width), window.innerWidth, document.documentElement.scrollWidth - document.documentElement.clientWidth]; }")
        ck(w[0] <= w[1] + 2 and w[2] <= 2, "on a phone the name's window is wider than the screen: %s" % w)
        ph.evaluate("document.getElementById('cds2-sheet').scrollIntoView({block:'start'})")
        ph.wait_for_timeout(300)
        shots["12-phone-name-sheet"] = ph.screenshot(type="jpeg", quality=72)
        ck(not ph_errors, "the page threw on a phone: %s" % ph_errors[:3])
        log("phone: the heat map is drawn tall at true text size, the index cards two across, a name's sheet fits the screen, no sideways overflow")


def main():
    from ops_report import report
    spec = importlib.util.spec_from_file_location("ops_6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    stamp = int(time.time())
    with report("ops_6957_cds_desk_two_live_proof") as r:
        r.heading("6957 - CDS desk 2.0 live: the documents worked again, then a real browser on %s" % BASE)
        fails, shots = [], {}

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAILED: %s" % msg)
            return bool(cond)

        r.section("1. The documents as the site serves them")
        doc = json.loads(fetch("/data/cds-desk.json?probe=%d" % stamp))
        tape = json.loads(fetch("/data/cds-desk-tape.json?probe=%d" % stamp))
        hist = json.loads(fetch("/data/cds-desk-history.json?probe=%d" % stamp))
        html = fetch("/cds.html?probe=%d" % stamp).decode("utf-8", "replace")
        views = fetch("/jh-cds-views.js?probe=%d" % stamp).decode("utf-8", "replace")
        ck("/jh-cds-views.js" in html and html.index("/jh-cds-views.js") < html.index("/jh-cds-desk.js") and 'id="cds-badge">v2.0<' in html, "the served page does not load the 2.0 views before the desk script")
        ck("#f23645" in views and "#089981" in views and "#4C8DF6" in views, "the site build re-coloured the views' measured colours")
        r.log("served: cds.html %d bytes, jh-cds-views.js %d bytes with its measured colours" % (len(html), len(views)))
        check_documents(doc, tape, hist, ck, r.log)

        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)
            r.section("2. The page in a real browser, then a phone")

            def new_page(phone=False):
                ctx = browser.new_context(viewport={"width": 390, "height": 844}, user_agent=UA, device_scale_factor=2, is_mobile=True, has_touch=True) if phone else browser.new_context(viewport={"width": 1500, "height": 950}, user_agent=UA)
                page, errors = ctx.new_page(), []
                page.on("pageerror", lambda e: errors.append(str(e)[:200]))
                return page, errors
            browse(new_page, BASE + "/cds.html?probe=%d" % stamp, doc, tape, ck, shots, r.log, phone_page=lambda: new_page(True))
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        g = doc["groups"]
        r.kv(version=doc["version"], with_a_level=sum(b["n_priced"] for b in g.values()), indices=len(doc["indices"]), failed_checks=len(fails), document=doc["generated_at"][:16])
        for f in fails[:40]:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("LIVE CDS desk %s: %d names with a level (%d sovereign, %d U.S., %d global, %d subordinated) and %d indices from every product of the public tape; the heat map, the movers, a name's sheet with its prints, "
             "the products, the option ladders, the mortgage indices, the quiet names, the repairs and every key of the document are on the page with the document's own figures; every 1.6 section is where it was" % (
                 doc["version"], sum(b["n_priced"] for b in g.values()), g["sovereign"]["n_priced"], g["us_corp"]["n_priced"], g["global_corp"]["n_priced"], g["sub"]["n_priced"], len(doc["indices"])))


if __name__ == "__main__":
    main()
