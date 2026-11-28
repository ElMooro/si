"""Damodaran Online, read into tables. Pure: bytes in, plain documents out; no network, no clock, no S3.

The source is Aswath Damodaran's site at NYU Stern (pages.stern.nyu.edu/~adamodar). He shares it with "no strings
attached" and asks for nothing but, if one cares to, an acknowledgement; every document built here names him and
his file. His own cautions travel with the data: the industry averages are made once a year (early January) and are
not a real-time source for valuing one company; the monthly implied premium is made at the start of each month.

What is read:
  * ERPbymonth.xlsx   - the implied equity risk premium for the S&P 500 at the start of every month since Sept 2008
  * histimpl.xls      - the same premium at each year end since 1960
  * histretSP.xls     - annual returns on stocks, bills, bonds, real estate and gold since 1928
  * ctryprem*.xlsx    - equity and country risk premiums by country (the newest of the dated files)
  * every file with an "industry averages" table (cost of capital, betas, margins, multiples, ... by region)

Workbooks made on a Mac count days from 1904, the others from 1900: the workbook says which, and it is obeyed.
"""
from datetime import date, timedelta
import io
import re
import zipfile
import xml.etree.ElementTree as ET

VERSION = "1.0.0"
SOURCE = "Aswath Damodaran, Damodaran Online (NYU Stern)"
SITE = "https://pages.stern.nyu.edu/~adamodar/"
ATTRIBUTION = "Data: Aswath Damodaran, Damodaran Online, Stern School of Business at New York University (pages.stern.nyu.edu/~adamodar)."
CAUTION = ("Industry averages are computed once a year (early January) from his own sample of companies and are not a "
           "real-time source for valuing one company. The implied premium is computed at the start of each month.")
REGIONS = [("Global", "Global"), ("Europe", "Europe"), ("Japan", "Japan"), ("emerg", "Emerging markets"),
           ("Rest", "Australia, NZ and Canada"), ("China", "China"), ("India", "India")]
NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
      "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
      "p": "http://schemas.openxmlformats.org/package/2006/relationships"}
# The five premiums the home page states each month, in its order, and the monthly file's column for each.
MEASURES = [
    ("sustainable_payout", "Trailing 12 months, payout adjusted to what is sustainable", r"sustain"),
    ("cash_yield", "Trailing 12 months of cash returned", r"^erp \(t12 ?m\)$"),
    ("average_cf", "Average cash yield of the last ten years", r"smooth|average|10"),
    ("net_cash_yield", "Net cash yield", r"net"),
    ("normalized", "Normalized earnings and payout", r"normal"),
]
HEADLINE = "sustainable_payout"


# ----------------------------------------------------------------------------------------------------- workbooks
def _col(ref):
    n = 0
    for ch in ref:
        if ch.isalpha():
            n = n * 26 + (ord(ch.upper()) - 64)
    return n - 1


def xlsx_book(raw):
    z = zipfile.ZipFile(io.BytesIO(raw))
    names = set(z.namelist())
    shared = []
    if "xl/sharedStrings.xml" in names:
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall("m:si", NS):
            shared.append("".join(t.text or "" for t in si.iter("{%s}t" % NS["m"])))
    wb = ET.fromstring(z.read("xl/workbook.xml"))
    pr = wb.find("m:workbookPr", NS)
    d1904 = pr is not None and str(pr.get("date1904", "")).lower() in ("1", "true")
    rels = {x.get("Id"): x.get("Target") for x in ET.fromstring(z.read("xl/_rels/workbook.xml.rels")).findall("p:Relationship", NS)}
    sheets = {}
    for sh in wb.find("m:sheets", NS).findall("m:sheet", NS):
        target = rels.get(sh.get("{%s}id" % NS["r"]), "")
        path = target.lstrip("/") if target.startswith("/") else "xl/" + target
        if path not in names:
            continue
        rows = []
        for row in ET.fromstring(z.read(path)).iter("{%s}row" % NS["m"]):
            cells = {}
            for c in row.findall("m:c", NS):
                v, kind = c.find("m:v", NS), c.get("t")
                if kind == "inlineStr":
                    val = "".join(t.text or "" for t in c.iter("{%s}t" % NS["m"]))
                elif v is None or v.text is None:
                    continue
                elif kind == "s":
                    val = shared[int(v.text)]
                elif kind in ("str", "e"):
                    val = v.text
                elif kind == "b":
                    val = v.text == "1"
                else:
                    try:
                        val = float(v.text)
                    except ValueError:
                        val = v.text
                cells[_col(c.get("r") or "A")] = val
            idx = int(row.get("r") or len(rows) + 1) - 1
            while len(rows) < idx:
                rows.append([])
            rows.append([cells.get(i) for i in range(max(cells) + 1)] if cells else [])
        sheets[sh.get("name")] = rows
    return {"date1904": d1904, "sheets": sheets}


def xls_book(raw):
    import xlrd
    wb = xlrd.open_workbook(file_contents=raw)
    sheets = {}
    for sh in wb.sheets():
        rows = []
        for i in range(sh.nrows):
            row = []
            for j in range(sh.ncols):
                c = sh.cell(i, j)
                if c.ctype == 3:
                    try:
                        y, m, d = xlrd.xldate_as_tuple(c.value, wb.datemode)[:3]
                        row.append("%04d-%02d-%02d" % (y, m, d) if y else c.value)
                    except Exception:
                        row.append(c.value)
                elif c.ctype in (0, 6):
                    row.append(None)
                elif c.ctype == 5:                                    # an error cell (#N/A, #DIV/0!)
                    row.append(None)
                else:
                    row.append(c.value)
            rows.append(row)
        sheets[sh.name] = rows
    return {"date1904": bool(wb.datemode), "sheets": sheets}


def book(raw):
    return xlsx_book(raw) if raw[:2] == b"PK" else xls_book(raw)


def excel_date(value, date1904=False):
    """An Excel day count, or a date already written out, as YYYY-MM-DD; None when it is neither."""
    if isinstance(value, str):
        m = re.match(r"^\s*(\d{4})-(\d{2})-(\d{2})", value)
        if m:
            return "%s-%s-%s" % m.groups()
        m = re.match(r"^\s*(\d{1,2})/(\d{1,2})/(\d{2,4})\s*$", value)
        if m:
            y = int(m.group(3))
            y += 2000 if y < 70 else 1900 if y < 100 else 0
            return "%04d-%02d-%02d" % (y, int(m.group(1)), int(m.group(2)))
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 1 <= value < 120000:
        return None
    base = date(1904, 1, 1) if date1904 else date(1899, 12, 30)
    return (base + timedelta(days=int(value))).isoformat()


# -------------------------------------------------------------------------------------------------------- helpers
def text(v):
    return re.sub(r"\s+", " ", str(v)).strip() if isinstance(v, str) else ""


def num(v):
    """A number, or None: 'NA', blanks, errors and booleans are not numbers."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v) if v == v and abs(v) != float("inf") else None
    s = str(v).strip().replace(",", "")
    if not s or s.upper() in ("NA", "N/A", "NM", "-", "#N/A"):
        return None
    pct = s.endswith("%")
    try:
        x = float(s.rstrip("%"))
    except ValueError:
        return None
    return x / 100.0 if pct else x


def tidy(x, places=6):
    if x is None:
        return None
    r = round(x, places)
    return int(r) if r == int(r) and abs(r) >= 1000 else r


def slug(label):
    s = re.sub(r"[^a-z0-9]+", "_", label.lower().replace("&", " and ").replace("%", " pct ").replace("$", " usd ")).strip("_")
    return s or "col"


def unique_keys(labels):
    seen, out = {}, []
    for lab in labels:
        k = slug(lab)
        seen[k] = seen.get(k, 0) + 1
        out.append(k if seen[k] == 1 else "%s_%d" % (k, seen[k]))
    return out


def find_row(rows, first, limit=60):
    """Index of the first row whose first cell reads `first` (a pattern), within the top of the sheet."""
    pat = re.compile(first, re.I)
    for i, row in enumerate(rows[:limit]):
        if row and pat.search(text(row[0])):
            return i
    return None


def header_note(rows, label, limit=12):
    """The value written beside a label in a sheet's preamble ("Date updated:", "What is this data?")."""
    pat = re.compile(label, re.I)
    for row in rows[:limit]:
        if row and pat.search(text(row[0])):
            rest = [c for c in row[1:] if c not in (None, "")]
            return rest[0] if rest else None
    return None


# ---------------------------------------------------------------------------------------- the implied premium
def erp_monthly(bk):
    """The start-of-month implied premium, September 2008 on. Returns {cols, rows, measures, first, last}."""
    sheet = next((rows for name, rows in bk["sheets"].items() if rows and re.search(r"start of month", text(rows[0][0] if rows[0] else ""), re.I)), None)
    if sheet is None:
        raise ValueError("no sheet opens with 'Start of month'")
    labels = [text(c) for c in sheet[0]]
    while labels and not labels[-1]:
        labels.pop()
    keys = ["date"] + unique_keys(labels[1:])
    rows = []
    for row in sheet[1:]:
        day = excel_date(row[0] if row else None, bk["date1904"])
        if not day:
            continue
        vals = [tidy(num(row[j]) if j < len(row) else None) for j in range(1, len(labels))]
        if all(v is None for v in vals):
            continue
        rows.append([day] + vals)
    rows.sort(key=lambda r: r[0])
    measures = {}
    for key, _label, pat in MEASURES:
        hit = [i for i, lab in enumerate(labels) if i and re.search(r"erp|premium", lab, re.I) and re.search(pat, lab.lower()) and i not in measures.values()]
        if hit:
            measures[key] = hit[0]
    have, gaps = set(r[0][:7] for r in rows), []
    if rows:
        y, m = int(rows[0][0][:4]), int(rows[0][0][5:7])
        while "%04d-%02d" % (y, m) < rows[-1][0][:7]:
            if "%04d-%02d" % (y, m) not in have:
                gaps.append("%04d-%02d" % (y, m))
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return {"cols": [{"key": k, "label": lab or k} for k, lab in zip(keys, ["Start of month"] + labels[1:])], "rows": rows,
            "measures": measures, "first": rows[0][0] if rows else None, "last": rows[-1][0] if rows else None, "gaps": gaps}


def table_by_year(rows, first=r"^year$", low=1900, high=2200, groups=True):
    """A sheet whose table is keyed by a year in its first column: {cols, rows, header_row}."""
    h = find_row(rows, first, 80)
    if h is None:
        raise ValueError("no header row opening with a year column")
    labels = [text(c) for c in rows[h]]
    while labels and not labels[-1]:
        labels.pop()
    above = rows[h - 1] if groups and h else []
    group, tags = "", []
    for j in range(len(labels)):
        g = text(above[j]) if j < len(above) else ""
        if g:
            group = g
        tags.append(group)
    full = [labels[0]] + [("%s: %s" % (tags[j], labels[j]) if tags[j] and labels[j] else labels[j] or tags[j]) for j in range(1, len(labels))]
    keep = [j for j in range(len(labels)) if labels[j]]
    out = []
    for row in rows[h + 1:]:
        y = num(row[0]) if row else None
        if y is None or y != int(y) or not low <= y <= high:
            if out and (not row or row[0] in (None, "")):
                break
            continue
        out.append([int(y)] + [tidy(num(row[j]) if j < len(row) else None) for j in keep[1:]])
    keys = ["year"] + unique_keys([full[j] for j in keep[1:]])
    return {"cols": [{"key": k, "label": full[j]} for k, j in zip(keys, keep)], "rows": out, "header_row": h + 1}


def erp_annual(bk):
    name = next((n for n in bk["sheets"] if re.search(r"impl", n, re.I)), next(iter(bk["sheets"])))
    t = table_by_year(bk["sheets"][name], low=1950, groups=False)
    t["updated"] = _as_date(header_note(bk["sheets"][name], r"date updated"), bk)
    return t


def returns_by_year(bk):
    name = next((n for n in bk["sheets"] if re.search(r"returns by year", n, re.I)), None)
    if name is None:
        raise ValueError("no 'Returns by year' sheet")
    t = table_by_year(bk["sheets"][name], low=1900)
    t["updated"] = _as_date(header_note(bk["sheets"][name], r"date updated"), bk)
    return t


def _as_date(v, bk):
    return excel_date(v, bk["date1904"]) if v is not None else None


def premiums_from_returns(tbl):
    """Stocks over bills and over bonds, arithmetic and compounded, over the whole record and its last 50 and 10 years:
    worked here from the annual returns, so the reader can see how each is made."""
    idx = {}
    for j, c in enumerate(tbl["cols"]):
        lab = c["label"].lower()
        if "value of" in lab or "real" in lab or "premium" in lab:
            continue
        if "s&p 500" in lab and "stocks" not in idx:
            idx["stocks"] = j
        elif "t.bill" in lab and "bills" not in idx:
            idx["bills"] = j
        elif "t. bond" in lab or "t.bond" in lab:
            idx.setdefault("bonds", j)
    if set(idx) != {"stocks", "bills", "bonds"}:
        return None
    rows = [r for r in tbl["rows"] if all(r[idx[k]] is not None for k in idx)]
    if len(rows) < 20:
        return None

    def span(part):
        n = len(part)
        ar = {k: sum(r[idx[k]] for r in part) / n for k in idx}
        geo = {}
        for k in idx:
            v = 1.0
            for r in part:
                v *= 1.0 + r[idx[k]]
            geo[k] = v ** (1.0 / n) - 1.0
        return {"from": part[0][0], "to": part[-1][0], "years": n,
                "stocks_minus_bills": {"arithmetic": tidy(ar["stocks"] - ar["bills"], 4), "compounded": tidy(geo["stocks"] - geo["bills"], 4)},
                "stocks_minus_bonds": {"arithmetic": tidy(ar["stocks"] - ar["bonds"], 4), "compounded": tidy(geo["stocks"] - geo["bonds"], 4)},
                "stocks": {"arithmetic": tidy(ar["stocks"], 4), "compounded": tidy(geo["stocks"], 4)}}
    return [span(rows), span(rows[-50:]), span(rows[-10:])]


# ------------------------------------------------------------------------------------------ country premiums
COUNTRY_COLS = [("country", "Country"), ("region", "Region"), ("moodys_rating", "Moody's rating"),
                ("default_spread", "Default spread from the rating"), ("equity_risk_premium", "Total equity risk premium"),
                ("country_risk_premium", "Country risk premium"), ("cds_spread", "Sovereign CDS, net of the US"),
                ("equity_risk_premium_cds", "Total equity risk premium, from the CDS"), ("country_risk_premium_cds", "Country risk premium, from the CDS")]


def country_premiums(bk):
    name = next((n for n in bk["sheets"] if re.search(r"erps? by country", n, re.I)), None)
    if name is None:
        raise ValueError("no 'ERPs by country' sheet")
    rows = bk["sheets"][name]
    h = find_row(rows, r"^country$", 40)
    if h is None:
        raise ValueError("no header row opening with 'Country'")
    out, end = [], len(rows)
    for i in range(h + 1, len(rows)):
        row = rows[i]
        c = text(row[0]) if row else ""
        if not c or re.match(r"frontier markets", c, re.I):
            end = i
            break
        cells = list(row) + [None] * 9
        out.append([c, text(cells[1]) or None, text(cells[2]) or None] + [tidy(num(cells[j])) for j in range(3, 9)])
    # Below the rated countries he lists the frontier markets that carry no sovereign rating, under headings of their own.
    frontier = None
    f = next((i for i in range(end, min(end + 6, len(rows))) if rows[i] and re.match(r"frontier markets", text(rows[i][0]), re.I)), None)
    if f is not None:
        fh = next((i for i in range(f + 1, min(f + 5, len(rows))) if rows[i] and re.match(r"^country$", text(rows[i][0]), re.I)), None)
        if fh is not None:
            labels = [text(c) for c in rows[fh][:9]]
            keep = [j for j, lab in enumerate(labels) if lab]
            frows = []
            for row in rows[fh + 1:]:
                c = text(row[0]) if row else ""
                if not c:
                    break
                frows.append([c] + [(tidy(num(row[j])) if j < len(row) and not isinstance(row[j], str) else (text(row[j]) or None) if j < len(row) else None) for j in keep[1:]])
            keys = ["country"] + unique_keys([labels[j] for j in keep[1:]])
            frontier = {"title": text(rows[f][0]), "cols": [{"key": k, "label": labels[j]} for k, j in zip(keys, keep)], "rows": frows}
    notes = []
    for row in rows[:h]:
        nums = [(j, c) for j, c in enumerate(row[1:], 1) if isinstance(c, (int, float)) and not isinstance(c, bool)]
        words = [text(c) for c in row if text(c)]
        if nums and words:
            notes.append({"label": words[0], "value": tidy(float(nums[0][1])), "note": words[1] if len(words) > 1 else None})
    updated = None
    for n in notes:
        if re.search(r"date of update", n["label"], re.I):
            updated = excel_date(n["value"], bk["date1904"])
    prem = [n for n in notes if isinstance(n["value"], float) and 0.005 < n["value"] < 0.2 and re.search(r"premium", n["label"] + " " + (n["note"] or ""), re.I)]
    mult = next((n["value"] for n in notes if re.search(r"multipl|volatil", n["label"] + " " + (n["note"] or ""), re.I) and isinstance(n["value"], float) and 0.5 < n["value"] < 5), None)
    return {"cols": [{"key": k, "label": lab} for k, lab in COUNTRY_COLS], "rows": out, "updated": updated,
            "mature_market_premium": prem[0]["value"] if prem else None, "us_premium": prem[1]["value"] if len(prem) > 1 else None,
            "equity_to_bond_volatility": mult, "inputs": [n for n in notes if not re.search(r"date of update", n["label"], re.I)], "frontier": frontier}


# ------------------------------------------------------------------------------------------ industry averages
def region_of(stem):
    """('wacc', 'US', 'United States') from 'wacc'; ('wacc', 'Global', 'Global') from 'waccGlobal'."""
    for tag, label in REGIONS:
        if stem.endswith(tag) and len(stem) > len(tag):
            return stem[:-len(tag)], tag if tag != "emerg" else "Emerging", label
    return stem, "US", "United States"


def topic_of(stem):
    base = region_of(stem)[0]
    base = re.sub(r"data$", "", base)                    # pedata / peGlobal, pbvdata / pbvGlobal
    base = {"betas": "beta", "vebitda": "vebitda"}.get(base, base)
    return re.sub(r"[^a-z0-9]+", "", base.lower()) or "table"


def industry_table(bk):
    """The 'industry averages' table of a workbook, or None when it has none. All his columns, under his own headings."""
    for name, rows in bk["sheets"].items():
        h = find_row(rows, r"^industry( name)?$", 60)
        if h is None:
            continue
        labels = [text(c) for c in rows[h]]
        while labels and not labels[-1]:
            labels.pop()
        keep = [j for j in range(len(labels)) if labels[j]]
        if len(keep) < 2:
            continue
        out, blanks = [], 0
        for row in rows[h + 1:]:
            nm = text(row[0]) if row else ""
            if not nm:
                blanks += 1
                if blanks >= 2 and out:
                    break
                continue
            blanks = 0
            vals = [tidy(num(row[j]) if j < len(row) else None) for j in keep[1:]]
            if all(v is None for v in vals):
                continue
            out.append([nm] + vals)
        if len(out) < 5:
            continue
        keys = ["industry"] + unique_keys([labels[j] for j in keep[1:]])
        what = header_note(rows, r"what is this data")
        return {"sheet": name, "title": text(what) if isinstance(what, str) else None, "updated": _as_date(header_note(rows, r"date updated"), bk),
                "cols": [{"key": k, "label": labels[j]} for k, j in zip(keys, keep)], "rows": out, "header_row": h + 1}
    return None


# The figures a valuation engine or a reader reaches for first, by the file that carries each and its heading there.
JOIN = [
    ("wacc", r"^number of firms$", "firms", "Companies"),
    ("wacc", r"^beta$", "beta", "Beta"),
    ("wacc", r"^cost of equity$", "cost_of_equity", "Cost of equity"),
    ("wacc", r"^e/\(d\+e\)$", "equity_weight", "Equity share of capital"),
    ("wacc", r"^cost of debt$", "cost_of_debt", "Cost of debt, before tax"),
    ("wacc", r"^tax rate$", "tax_rate", "Tax rate"),
    ("wacc", r"^cost of capital$", "cost_of_capital", "Cost of capital"),
    ("margin", r"^gross margin$", "gross_margin", "Gross margin"),
    ("margin", r"pre-tax unadjusted operating margin", "operating_margin", "Operating margin, before tax"),
    ("margin", r"^net margin$", "net_margin", "Net margin"),
    ("roe", r"^roe \(unadjusted\)$|^roe$", "roe", "Return on equity"),
    ("eva", r"^roc$|^roic$|return on (invested )?capital", "roic", "Return on capital"),
    ("pe", r"^trailing pe$", "pe_trailing", "PE, trailing"),
    ("pe", r"^forward pe$", "pe_forward", "PE, forward"),
    ("pbv", r"^pbv$", "pbv", "Price to book"),
    ("ps", r"^price/sales$", "ps", "Price to sales"),
    ("ps", r"^ev/sales$", "ev_sales", "EV to sales"),
    ("vebitda", r"^ev/ebitda$", "ev_ebitda", "EV to EBITDA"),
    ("vebitda", r"^ev/ebit$", "ev_ebit", "EV to EBIT"),
]


def join_industries(tables):
    """One row an industry from the tables of one region: {cols, rows, missing}. `tables` is {topic: industry_table}."""
    names, seen = [], set()
    for topic in ("wacc", "margin", "pe", "vebitda"):
        for row in (tables.get(topic) or {}).get("rows", []):
            if row[0] not in seen:
                seen.add(row[0])
                names.append(row[0])
    cols, picks, missing = [{"key": "industry", "label": "Industry"}], [], []
    for topic, pat, key, label in JOIN:
        t = tables.get(topic)
        j = None
        if t:
            j = next((i for i, c in enumerate(t["cols"]) if i and re.search(pat, c["label"].strip().lower())), None)
        if j is None:
            missing.append("%s: %s" % (topic, label))
            continue
        cols.append({"key": key, "label": label, "from": topic})
        picks.append(({r[0]: r[j] for r in t["rows"]}))
    rows = [[nm] + [p.get(nm) for p in picks] for nm in names]
    return {"cols": cols, "rows": rows, "missing": missing}
