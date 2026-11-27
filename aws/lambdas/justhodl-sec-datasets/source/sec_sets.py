"""The SEC data sets this bank keeps, and the arithmetic of what to fetch next. Pure: no network, no AWS.

A data set is a page on sec.gov that lists zip files (one per quarter or month). `advertised()` reads such a page,
`period_of()` dates a file from its name, `plan()` chooses what one run banks inside its byte budget: first the newest
file of every set, then the history, newest first.
"""
from html import unescape
from urllib.parse import urljoin
import re

BASE = "https://www.sec.gov/data-research/sec-markets-data/"

# name -> page, the file names that belong to it, what it is, and whether this engine also reads it into tables
SETS = {
    "nport": {"page": BASE + "form-n-port-data-sets", "match": r"_nport\.zip$", "derive": "nport",
              "what": "Form N-PORT: every registered fund's holdings, quarterly"},
    "nmfp": {"page": BASE + "dera-form-n-mfp-data-sets", "match": r"_nmfp\.zip$", "derive": "nmfp",
             "what": "Form N-MFP: every money market fund's portfolio, monthly"},
    "bdc": {"page": BASE + "bdc-data-sets", "match": r"_bdc\.zip$", "derive": None,
            "what": "Business development companies: financial data and the schedule of investments"},
    "form13f": {"page": BASE + "form-13f-data-sets", "match": r"form13f\.zip$", "derive": None,
                "what": "Form 13F: every institutional manager's reported holdings, quarterly"},
    "insiders": {"page": BASE + "insider-transactions-data-sets", "match": r"_form345\.zip$", "derive": None,
                 "what": "Forms 3, 4 and 5: every insider transaction, quarterly since 2006"},
    "formd": {"page": BASE + "form-d-data-sets", "match": r"_d\.zip$", "derive": None,
              "what": "Form D: notices of private offerings, quarterly since 2008"},
    "ncen": {"page": BASE + "form-n-cen-data-sets", "match": r"_ncen\.zip$", "derive": None,
             "what": "Form N-CEN: the annual census of registered funds"},
}
MONTHS = {m: i + 1 for i, m in enumerate("jan feb mar apr may jun jul aug sep oct nov dec".split())}
UNITS = {"KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3}
SAFE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{3,80}\.zip")


def period_of(name):
    """The last day a file covers, as (year, month, day), read from its name; None when the name says nothing.

    2026q3_nport.zip -> (2026, 9, 30); 2026_10_bdc.zip -> (2026, 10, 31); 20261008-20261106_nmfp.zip -> (2026, 11, 6);
    01jun2026-31aug2026_form13f.zip -> (2026, 8, 31).
    """
    low = name.lower()
    m = re.search(r"(\d{8})-(\d{8})", low)
    if m:
        e = m.group(2)
        return int(e[:4]), int(e[4:6]), int(e[6:])
    m = re.search(r"\d{2}[a-z]{3}\d{4}-(\d{2})([a-z]{3})(\d{4})", low)
    if m and m.group(2) in MONTHS:
        return int(m.group(3)), MONTHS[m.group(2)], int(m.group(1))
    m = re.search(r"(?<!\d)(\d{4})q([1-4])(?!\d)", low)
    if m:
        q = int(m.group(2))
        return int(m.group(1)), q * 3, 31 if q in (1, 4) else 30
    m = re.search(r"(?<!\d)(\d{4})_(\d{2})(?!\d)", low)
    if m and 1 <= int(m.group(2)) <= 12:
        return int(m.group(1)), int(m.group(2)), 31
    return None


def size_of(text):
    """'440.95 MB' -> bytes; None when the page shows no size."""
    m = re.search(r"([\d.,]+)\s*(KB|MB|GB)", text or "", flags=re.I)
    if not m:
        return None
    try:
        return int(float(m.group(1).replace(",", "")) * UNITS[m.group(2).upper()])
    except ValueError:
        return None


def advertised(page_html, page_url, match):
    """The zip files a data-set page lists: [{name, url, period, size_shown}], newest first.

    Only links whose file name fits the set's own pattern and is a plain file name are taken: a page that changes shape
    yields fewer files, never a stranger's link.
    """
    text = page_html if isinstance(page_html, str) else page_html.decode("utf-8", "replace")
    rx, out, seen = re.compile(match, re.I), [], set()
    rows = re.findall(r"<tr.*?</tr>", text, flags=re.S | re.I) or [text]
    for row in rows:
        for href in re.findall(r'href="([^"]+\.zip)"', row, flags=re.I):
            url = urljoin(page_url, unescape(href))
            name = url.rsplit("/", 1)[-1]
            if name in seen or not rx.search(name) or not SAFE_NAME.fullmatch(name) or "sec.gov/" not in url:
                continue
            seen.add(name)
            out.append({"name": name, "url": url, "period": period_of(name),
                        "size_shown": size_of(re.sub(r"<[^>]+>", " ", row)) if len(rows) > 1 else None})
    out.sort(key=lambda f: (f["period"] or (0, 0, 0), f["name"]), reverse=True)
    return out


def plan(library, banked, byte_budget, max_files=40, assume=60 * 1024 ** 2):
    """What one run banks: [(set, file, tier)] in order; tier 0 = the newest file of a set, tier 1 = history.

    library: {set: [file, ...]} newest first (advertised()); banked: {set: {file name: record}}.
    First the newest file of each set that is not banked yet (every set gets its latest before any history), then the
    rest newest first across all sets. A file is taken while the budget lasts; the first file is always taken, so one
    file larger than the budget still moves.
    """
    first, rest = [], []
    for name, files in library.items():
        have = banked.get(name) or {}
        missing = [f for f in files if f["name"] not in have]
        if not missing:
            continue
        if files and files[0]["name"] not in have:
            first.append((name, missing[0]))
            missing = missing[1:]
        rest.extend((name, f) for f in missing)
    first.sort(key=lambda sf: sf[1]["period"] or (0, 0, 0), reverse=True)
    rest.sort(key=lambda sf: (sf[1]["period"] or (0, 0, 0), sf[0]), reverse=True)
    chosen, spent = [], 0
    for tier, group in ((0, first), (1, rest)):
        for name, f in group:
            cost = f.get("size_shown") or assume
            if chosen and (spent + cost > byte_budget or len(chosen) >= max_files):
                continue
            chosen.append((name, f, tier))
            spent += cost
    return chosen


def iso(period):
    return "%04d-%02d-%02d" % period if period else None
