"""Individual corporate bonds from public daily fund holdings.

The rows are the bonds inside named iShares and SPDR books, not a TRACE
CUSIP blotter and not every bond outstanding. A yield the file does not
print stays blank. Demand is market value held by the scanned funds.
CUSIPs are used only as a join key inside this process and are not returned.
"""
from __future__ import annotations

import csv
import hashlib
import io
import math
import re
import zipfile
from collections import defaultdict
from datetime import date
from xml.etree import ElementTree as ET

FUNDS = (
    {"id": "USIG", "ticker": "USIG", "name": "iShares Broad USD Investment Grade Corporate", "book": "US IG", "kind": "ishares", "url": "https://www.ishares.com/us/products/239460/x/latest-holdings.csv"},
    {"id": "LQD", "ticker": "LQD", "name": "iShares iBoxx $ Investment Grade Corporate", "book": "US IG", "kind": "ishares", "url": "https://www.ishares.com/us/products/239566/x/latest-holdings.csv"},
    {"id": "USHY", "ticker": "USHY", "name": "iShares Broad USD High Yield Corporate", "book": "US HY", "kind": "ishares", "url": "https://www.ishares.com/us/products/291299/x/latest-holdings.csv"},
    {"id": "HYG", "ticker": "HYG", "name": "iShares iBoxx $ High Yield Corporate", "book": "US HY", "kind": "ishares", "url": "https://www.ishares.com/us/products/239565/x/latest-holdings.csv"},
    {"id": "SHYG", "ticker": "SHYG", "name": "iShares 0-5 Year High Yield Corporate", "book": "US HY", "kind": "ishares", "url": "https://www.ishares.com/us/products/258100/x/latest-holdings.csv"},
    {"id": "FALN", "ticker": "FALN", "name": "iShares Fallen Angels USD Bond", "book": "US HY", "kind": "ishares", "url": "https://www.ishares.com/us/products/283855/x/latest-holdings.csv"},
    {"id": "CEMB", "ticker": "CEMB", "name": "iShares J.P. Morgan EM Corporate", "book": "EM corporate", "kind": "ishares", "url": "https://www.ishares.com/us/products/239525/x/latest-holdings.csv"},
    {"id": "EMHY", "ticker": "EMHY", "name": "iShares J.P. Morgan EM High Yield", "book": "EM HY", "kind": "ishares", "url": "https://www.ishares.com/us/products/239527/x/latest-holdings.csv"},
    {"id": "EUIG", "ticker": "EUIG", "name": "iShares Euro Investment Grade Corporate USD Hedged", "book": "Euro IG", "kind": "ishares", "url": "https://www.ishares.com/us/products/346691/x/latest-holdings.csv"},
    {"id": "EUHY", "ticker": "EUHY", "name": "iShares Euro High Yield Corporate USD Hedged", "book": "Euro HY", "kind": "ishares", "url": "https://www.ishares.com/us/products/239550/x/latest-holdings.csv"},
    {"id": "IGIL", "ticker": "IGIL", "name": "iShares US & Intl High Yield Corporate", "book": "US + intl HY", "kind": "ishares", "url": "https://www.ishares.com/us/products/239551/x/latest-holdings.csv"},
    {"id": "ICVT", "ticker": "ICVT", "name": "iShares Convertible Bond", "book": "Convertible", "kind": "ishares", "url": "https://www.ishares.com/us/products/272819/x/latest-holdings.csv"},
    {"id": "FLOT", "ticker": "FLOT", "name": "iShares Floating Rate Bond", "book": "Floating", "kind": "ishares", "url": "https://www.ishares.com/us/products/239534/x/latest-holdings.csv"},
    {"id": "IEAC", "ticker": "IEAC", "name": "iShares Core Euro Corporate Bond UCITS", "book": "Euro IG", "kind": "ishares", "url": "https://www.ishares.com/uk/individual/en/products/251726/x/latest-holdings.csv"},
    {"id": "IAGG", "ticker": "IAGG", "name": "iShares Core International Aggregate", "book": "International", "kind": "ishares", "url": "https://www.ishares.com/us/products/279626/x/latest-holdings.csv"},
    {"id": "IBND", "ticker": "IBND", "name": "SPDR Bloomberg International Corporate", "book": "International", "kind": "ssga", "url": "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-ibnd.xlsx"},
)

_SKIP_SECTOR = {
    "treasury", "government", "sovereign", "cash and/or derivatives", "cash",
    "agency", "municipal", "government related", "securitized", "supranational",
    "money market",
}
_SKIP_ASSET = {"equity", "money market", "cash", "futures", "options", "fx", "currency"}
_COARSE = {"corporates", "corporate", "fixed income"}
_DROP = {
    "MTN", "REGS", "REG", "144A", "SR", "UNSECURED", "GUAR", "GUARANTEED", "THE",
    "INC", "LLC", "PLC", "NV", "SA", "AG", "CO", "CORP", "CORPORATION", "LTD",
    "LIMITED", "BV", "SE", "SPA", "COMPANY", "HOLDINGS", "HOLDING",
}
_MON = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}
_NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def _num(raw):
    if type(raw) in (int, float) and not isinstance(raw, bool):
        number = float(raw)
        return number if math.isfinite(number) else None
    text = str(raw or "").strip().replace(",", "").replace("%", "")
    if text in ("", "-", "—", "N/A", "n/a", "#N/A"):
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _round(value, places):
    number = _num(value)
    if number is None:
        return None
    return round(number, places)


def _date(raw):
    text = str(raw or "").strip()
    if not text or text in ("-", "—"):
        return None
    match = re.search(r"([A-Za-z]{3})\s+(\d{1,2}),\s*(\d{4})", text)
    if match and _MON.get(match.group(1).title()):
        return date(int(match.group(3)), _MON[match.group(1).title()], int(match.group(2))).isoformat()
    match = re.search(r"(\d{1,2})[/-]([A-Za-z]{3})[/-](\d{4})", text)
    if match and _MON.get(match.group(2).title()):
        return date(int(match.group(3)), _MON[match.group(2).title()], int(match.group(1))).isoformat()
    match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if match:
        try:
            return date(int(match.group(3)), int(match.group(1)), int(match.group(2))).isoformat()
        except ValueError:
            return None
    match = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if match:
        return match.group(0)
    return None


def _as_of(text):
    for line in (text or "").splitlines()[:8]:
        if "as of" not in line.lower():
            continue
        found = _date(line)
        if found:
            return found
    return None


def norm_issuer(name):
    cleaned = re.sub(r"[^A-Z0-9 ]", " ", (name or "").upper())
    tokens = [tok for tok in cleaned.split() if tok not in _DROP and not tok.isdigit() and len(tok) > 1]
    return " ".join(tokens[:6])


def _cell(row, *names):
    for name in names:
        if name in row and row[name] not in (None, ""):
            return row[name]
    return None


def _keep_sector(sector):
    return (sector or "").strip().lower() not in _SKIP_SECTOR


def _keep_asset(asset):
    text = (asset or "").strip().lower()
    if not text:
        return True
    return text not in _SKIP_ASSET and "equity" not in text


def _isin(raw):
    text = re.sub(r"[^A-Za-z0-9]", "", str(raw or "")).upper()
    if len(text) == 12 and text.isalnum() and not text.isdigit():
        return text
    return None


def _row_from(mapped, fund):
    issuer = str(_cell(mapped, "issuer") or "").strip()
    if not issuer or issuer in ("-", "—"):
        return None
    sector = str(_cell(mapped, "sector") or "").strip()
    if sector in ("-", "—"):
        sector = ""
    if not _keep_sector(sector):
        return None
    asset = str(_cell(mapped, "asset") or "")
    if not _keep_asset(asset):
        return None
    maturity = _date(_cell(mapped, "maturity"))
    coupon = _round(_cell(mapped, "coupon"), 3)
    ytm = _round(_cell(mapped, "ytm"), 3)
    ytw = _round(_cell(mapped, "ytw"), 3)
    demand = _num(_cell(mapped, "demand"))
    if demand is not None and demand < 0:
        demand = None
    isin = _isin(_cell(mapped, "isin"))
    if maturity is None and isin is None:
        return None
    if ytm is None and ytw is None and (demand is None or demand == 0) and isin is None:
        return None
    ccy = str(_cell(mapped, "ccy") or "").strip().upper()
    if ccy in ("", "-", "—"):
        ccy = ""
    country = str(_cell(mapped, "country") or "").strip()
    if country in ("-", "—"):
        country = ""
    return {
        "issuer": issuer,
        "sector": sector,
        "country": country,
        "ccy": ccy[:3] if len(ccy) >= 3 else ccy,
        "coupon": coupon,
        "maturity": maturity,
        "ytm": ytm,
        "ytw": ytw,
        "price": _round(_cell(mapped, "price"), 3),
        "demand": demand,
        "dur": _round(_cell(mapped, "dur"), 2),
        "isin": isin,
        "book": fund["book"],
        "fund": fund["id"],
    }


def parse_ishares(text, fund):
    if not text or text.lstrip()[:1] == "<":
        return [], None
    lines = text.splitlines()
    header = None
    for index, line in enumerate(lines[:30]):
        if "Name" in line and "Market Value" in line and "," in line:
            header = index
            break
    if header is None:
        return [], _as_of(text)
    out = []
    for raw in csv.DictReader(lines[header:]):
        mapped = {
            "issuer": _cell(raw, "Name"),
            "sector": _cell(raw, "Sector"),
            "asset": _cell(raw, "Asset Class"),
            "maturity": _cell(raw, "Maturity"),
            "coupon": _cell(raw, "Coupon (%)"),
            "ytm": _cell(raw, "YTM (%)"),
            "ytw": _cell(raw, "Yield to Worst (%)"),
            "demand": _cell(raw, "Market Value"),
            "price": _cell(raw, "Price"),
            "dur": _cell(raw, "Duration"),
            "isin": _cell(raw, "ISIN"),
            "ccy": _cell(raw, "Market Currency") or _cell(raw, "Currency"),
            "country": _cell(raw, "Location"),
        }
        row = _row_from(mapped, fund)
        if row:
            out.append(row)
    return out, _as_of(text)


def _xlsx_strings(blob):
    try:
        archive = zipfile.ZipFile(io.BytesIO(blob))
    except zipfile.BadZipFile:
        return None
    if "xl/sharedStrings.xml" not in archive.namelist() or "xl/worksheets/sheet1.xml" not in archive.namelist():
        return None
    shared = []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    tag = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t"
    for item in root.findall("m:si", _NS):
        shared.append("".join(node.text or "" for node in item.iter(tag)))
    sheet = ET.fromstring(archive.read("xl/worksheets/sheet1.xml"))
    rows = []
    for row in sheet.findall("m:sheetData/m:row", _NS):
        values = []
        for cell in row.findall("m:c", _NS):
            node = cell.find("m:v", _NS)
            if node is None or node.text is None:
                values.append("")
                continue
            if cell.get("t") == "s":
                try:
                    values.append(shared[int(node.text)])
                except (ValueError, IndexError):
                    values.append("")
            else:
                values.append(node.text)
        rows.append(values)
    return rows


def parse_ssga(blob, fund):
    rows = _xlsx_strings(blob)
    if not rows:
        return [], None
    as_of = None
    header = None
    for index, row in enumerate(rows[:12]):
        joined = " ".join(row)
        if as_of is None and "as of" in joined.lower():
            as_of = _date(joined)
        if row and str(row[0]).strip() == "Name" and "Market Value" in row:
            header = index
    if header is None:
        return [], as_of
    titles = [str(cell).strip() for cell in rows[header]]
    out = []
    for raw in rows[header + 1:]:
        if not raw or not str(raw[0]).strip():
            continue
        mapped_row = {titles[i]: raw[i] for i in range(min(len(titles), len(raw)))}
        row = _row_from({
            "issuer": mapped_row.get("Name"),
            "sector": "",
            "asset": "Fixed Income",
            "maturity": mapped_row.get("Maturity"),
            "coupon": mapped_row.get("Coupon"),
            "ytm": None,
            "ytw": None,
            "demand": mapped_row.get("Market Value"),
            "price": None,
            "dur": None,
            "isin": mapped_row.get("ISIN") or mapped_row.get("Identifier"),
            "ccy": mapped_row.get("Local Currency"),
            "country": "",
        }, fund)
        if row:
            out.append(row)
    return out, as_of


def _name_key(row):
    coupon = "" if row.get("coupon") is None else ("%.3f" % row["coupon"])
    return "|".join([norm_issuer(row.get("issuer")), row.get("maturity") or "", coupon, row.get("ccy") or ""])


def _public_key(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]


def _fine(sector):
    return bool(sector) and sector.strip().lower() not in _COARSE


def _pick_rate(parts):
    rated = [part for part in parts if part.get("ytw") is not None or part.get("ytm") is not None]
    pool = rated or parts
    pool = sorted(pool, key=lambda part: -(part.get("demand") or 0))
    best = pool[0]
    rate = best.get("ytw") if best.get("ytw") is not None else best.get("ytm")
    return rate


def _wavg(pairs):
    total = weight = 0.0
    for value, size in pairs:
        if value is None or size is None or size <= 0:
            continue
        total += value * size
        weight += size
    if weight <= 0:
        return None
    return round(total / weight, 3)


def assemble(fund_results, prior_bonds):
    """fund_results: iterable of (fund, rows, status, as_of). prior_bonds: previous packet rows."""
    prior = {}
    for old in prior_bonds or []:
        if isinstance(old, dict) and old.get("k") and old.get("rate") is not None:
            prior[old["k"]] = old["rate"]
    by_isin = defaultdict(list)
    loose = []
    funds_out = []
    for fund, rows, status, as_of in fund_results:
        funds_out.append({
            "id": fund["id"], "ticker": fund["ticker"], "name": fund["name"],
            "book": fund["book"], "as_of": as_of, "n": len(rows or []), "status": status,
        })
        for row in rows or []:
            if row.get("isin"):
                by_isin[row["isin"]].append(row)
            else:
                loose.append(row)
    groups = []
    name_index = defaultdict(list)
    for isin, parts in by_isin.items():
        group = {"token": "i:" + isin, "parts": parts}
        groups.append(group)
        name_index[_name_key(parts[0])].append(group)
    for row in loose:
        hits = name_index.get(_name_key(row)) or []
        if len(hits) == 1:
            hits[0]["parts"].append(row)
        else:
            groups.append({"token": "n:" + _name_key(row), "parts": [row]})

    bonds = []
    for group in groups:
        parts = group["parts"]
        demand = sum(part["demand"] or 0 for part in parts if part.get("demand"))
        demand = round(demand, 0) if demand else None
        rate = _pick_rate(parts)
        fine = next((part["sector"] for part in parts if _fine(part.get("sector"))), "")
        sector = fine or next((part["sector"] for part in parts if part.get("sector")), "") or ""
        display = sorted(parts, key=lambda part: (-(part.get("demand") or 0), len(part.get("issuer") or "")))[0]
        country = next((part["country"] for part in parts if part.get("country")), "") or display.get("country") or ""
        ccy = next((part["ccy"] for part in parts if part.get("ccy")), "") or ""
        maturity = next((part["maturity"] for part in parts if part.get("maturity")), None)
        coupon = next((part["coupon"] for part in parts if part.get("coupon") is not None), None)
        price = next((part["price"] for part in sorted(parts, key=lambda part: -(part.get("demand") or 0)) if part.get("price") is not None), None)
        dur = next((part["dur"] for part in parts if part.get("dur") is not None), None)
        book_demand = defaultdict(float)
        for part in parts:
            if part.get("demand"):
                book_demand[part["book"]] += part["demand"]
        book = max(book_demand, key=book_demand.get) if book_demand else parts[0]["book"]
        key = _public_key(group["token"])
        chg = None
        if rate is not None and key in prior and prior[key] is not None:
            chg = round(rate - prior[key], 3)
        item = {
            "k": key,
            "issuer": display["issuer"],
            "sector": sector or None,
            "country": country or None,
            "ccy": ccy or None,
            "coupon": coupon,
            "maturity": maturity,
            "rate": rate,
            "chg": chg,
            "price": price,
            "demand": demand,
            "dur": dur,
            "book": book,
            "_norm": norm_issuer(display["issuer"]),
        }
        if item["_norm"]:
            bonds.append(item)

    issuers = {}
    for bond in bonds:
        bucket = issuers.setdefault(bond["_norm"], {
            "issuer": bond["issuer"], "sector": bond["sector"], "country": bond["country"],
            "n": 0, "demand": 0.0, "rates": [], "chgs": [], "books": set(), "countries": set(),
        })
        bucket["n"] += 1
        if bond.get("demand"):
            bucket["demand"] += bond["demand"]
        if bond.get("rate") is not None:
            bucket["rates"].append((bond["rate"], bond.get("demand") or 0))
        if bond.get("chg") is not None:
            bucket["chgs"].append((bond["chg"], bond.get("demand") or 0))
        if bond.get("book"):
            bucket["books"].add(bond["book"])
        if bond.get("country"):
            bucket["countries"].add(bond["country"])
        if _fine(bond.get("sector")) and not _fine(bucket.get("sector")):
            bucket["sector"] = bond["sector"]
        if bond.get("demand") and len(bond["issuer"]) < len(bucket["issuer"]):
            bucket["issuer"] = bond["issuer"]
    issuer_rows = []
    for bucket in issuers.values():
        issuer_rows.append({
            "issuer": bucket["issuer"],
            "sector": bucket["sector"],
            "country": bucket["country"] if len(bucket["countries"]) <= 1 else "multi",
            "n": bucket["n"],
            "demand": round(bucket["demand"], 0) if bucket["demand"] else None,
            "rate": _wavg(bucket["rates"]),
            "chg": _wavg(bucket["chgs"]),
            "books": sorted(bucket["books"]),
        })
    issuer_rows.sort(key=lambda row: (-(row["demand"] or 0), row["issuer"]))

    sectors = {}
    for bond in bonds:
        label = bond.get("sector") or ""
        bucket = sectors.setdefault(label, {"sector": label or None, "n": 0, "issuers": set(), "demand": 0.0, "rates": [], "chgs": []})
        bucket["n"] += 1
        bucket["issuers"].add(bond["_norm"])
        if bond.get("demand"):
            bucket["demand"] += bond["demand"]
        if bond.get("rate") is not None:
            bucket["rates"].append((bond["rate"], bond.get("demand") or 0))
        if bond.get("chg") is not None:
            bucket["chgs"].append((bond["chg"], bond.get("demand") or 0))
    sector_rows = []
    for bucket in sectors.values():
        sector_rows.append({
            "sector": bucket["sector"],
            "n": bucket["n"],
            "n_issuers": len(bucket["issuers"]),
            "demand": round(bucket["demand"], 0) if bucket["demand"] else None,
            "rate": _wavg(bucket["rates"]),
            "chg": _wavg(bucket["chgs"]),
        })
    sector_rows.sort(key=lambda row: (-(row["demand"] or 0), row["sector"] or ""))

    for bond in bonds:
        bond.pop("_norm", None)
        for key in list(bond):
            if bond[key] is None or bond[key] == "":
                bond.pop(key)
    bonds.sort(key=lambda row: (-(row.get("demand") or 0), row["issuer"], row.get("maturity") or ""))
    live = [fund for fund in funds_out if fund["n"] > 0]
    return {
        "n_bonds": len(bonds),
        "n_issuers": len(issuer_rows),
        "n_sectors": sum(1 for row in sector_rows if row.get("sector")),
        "n_with_rate": sum(1 for row in bonds if row.get("rate") is not None),
        "n_with_change": sum(1 for row in bonds if row.get("chg") is not None),
        "demand": round(sum(row.get("demand") or 0 for row in bonds), 0) if bonds else None,
        "funds": funds_out,
        "sectors": sector_rows,
        "issuers": issuer_rows,
        "bonds": bonds,
        "as_of": max((fund["as_of"] for fund in live if fund.get("as_of")), default=None),
        "status": "live" if live else "unavailable",
        "note": (
            "Individual bonds held by the scanned iShares and SPDR books. "
            "Industry is the fund sector. Yield is yield-to-worst when the file prints it, else yield-to-maturity. "
            "Demand is market value inside those funds, not TRACE volume and not the amount outstanding. "
            "A blank yield was not printed. Yield change is versus the previous desk snapshot of the same bond. "
            "CUSIPs are not republished. This is not every corporate bond outstanding."
        ),
    }
