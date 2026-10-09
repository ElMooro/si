"""justhodl-nyfed-hhdc — NY Fed Quarterly Report on Household Debt and Credit (Consumer Credit Panel / Equifax).

The report's data workbook is public (no key) at a quarter-stamped URL:
  https://www.newyorkfed.org/medialibrary/interactives/householdcredit/data/xls/HHD_C_Report_<YYYY>Q<n>.xlsx
Every "Page N Data" sheet has the same shape: title (row 1), unit (row 2), a header row, then one row per quarter
labelled "YY:Qn". All such sheets are parsed generically, so each chart's columns become quarterly series
(total balances by product, delinquency status, 90+ day delinquency by product, flows into delinquency, foreclosures,
bankruptcies, credit scores at origination, by age and by state). Descriptive only.
"""
from __future__ import annotations

import re
import datetime as dt

import risk_sources as RS

SLUG = "nyfed-hhdc"
URL = "https://www.newyorkfed.org/medialibrary/interactives/householdcredit/data/xls/HHD_C_Report_{y}Q{q}.xlsx"
PAGE = "https://www.newyorkfed.org/microeconomics/hhdc"
QRE = re.compile(r"^(\d{2}):Q([1-4])$")


def recent_quarters(today=None, n=6):
    today = today or dt.date.today()
    y, q = today.year, (today.month - 1) // 3 + 1
    out = []
    for _ in range(n):
        out.append((y, q))
        q -= 1
        if q == 0:
            y, q = y - 1, 4
    return out


def _year(yy):
    """'99' -> 1999, '03' -> 2003 (the panel starts 1999)."""
    y = int(yy)
    return 1900 + y if y >= 90 else 2000 + y


def _sid(s):
    return re.sub(r"[^A-Za-z0-9]+", "_", str(s)).strip("_").upper()


def parse_sheet(rows):
    """-> (title, unit, {column_label: [[date, v], ...]}) or None if the sheet is not a quarterly table."""
    if len(rows) < 5 or not rows[0] or not rows[0][0]:
        return None
    title = str(rows[0][0]).strip()
    unit = str(rows[1][0]).strip() if len(rows) > 1 and rows[1] and rows[1][0] else ""
    if unit.lower().startswith("return to"):
        unit = ""
    header_i = None
    for i, r in enumerate(rows[1:12], 1):
        if r and (r[0] is None or str(r[0]).strip() == "") and sum(1 for c in r[1:] if c not in (None, "")) >= 2:
            header_i = i
            break
    if header_i is None:
        return None
    header = [str(c).strip() if c is not None else "" for c in rows[header_i]]
    if sum(1 for h in header[1:] if QRE.match(h)) >= 8:          # transposed layout: quarters across, places down (state pages)
        cols = {}
        for r in rows[header_i + 1:]:
            if not r or not r[0] or str(r[0]).strip() == "":
                continue
            place = str(r[0]).strip()
            pts = []
            for i, h in enumerate(header[1:], 1):
                m = QRE.match(h)
                if m and i < len(r):
                    v = RS.parse_num(r[i])
                    if v is not None:
                        pts.append([RS.quarter_end(_year(m.group(1)), int(m.group(2))), v])
            if len(pts) >= 8:
                cols[place] = pts
        return (title, unit, cols) if cols else None
    cols = {h: [] for h in header[1:] if h}
    n = 0
    for r in rows[header_i + 1:]:
        if not r or r[0] is None:
            continue
        m = QRE.match(str(r[0]).strip())
        if not m:
            continue
        d = RS.quarter_end(_year(m.group(1)), int(m.group(2)))
        n += 1
        for i, h in enumerate(header[1:], 1):
            if h and i < len(r):
                v = RS.parse_num(r[i])
                if v is not None:
                    cols[h].append([d, v])
    if n < 8:
        return None
    return title, unit, {h: p for h, p in cols.items() if p}


def build(event):
    pk = RS.Packet(
        SLUG, "NY Fed Household Debt & Credit", "NY Fed Quarterly Report on Household Debt and Credit",
        "The New York Fed's quarterly household balance-sheet report from the Consumer Credit Panel (a 5% sample of "
        "Equifax credit files): total debt by product, delinquency status, 90+ day delinquency by product, transitions "
        "into delinquency, foreclosures and bankruptcies, origination credit scores, and breakdowns by age and state. "
        "Household credit stress is the US consumer-side counterpart to the FDIC bank-side view. Read from the report's public workbook.",
        {"provider": "Federal Reserve Bank of New York — Center for Microeconomic Data", "url": PAGE, "docs": PAGE,
         "license": "Public (NY Fed terms of use)", "cadence": "quarterly (early Feb / May / Aug / Nov)"},
        cadence="daily")
    blob, used = None, None
    tried = []
    for y, q in recent_quarters():
        u = URL.format(y=y, q=q)
        try:
            blob = RS.http_get(u, timeout=60, retries=0)
            if not blob.startswith(b"PK"):          # not-yet-published quarters answer 200 with an HTML page
                tried.append(f"{y}Q{q}: not a workbook ({len(blob)} bytes)")
                blob = None
                continue
            used = (y, q, u)
            break
        except Exception as exc:  # noqa: BLE001
            tried.append(f"{y}Q{q}: {str(exc)[:60]}")
    if blob is None:
        raise RuntimeError("no HHDC workbook found for the last six quarters: " + "; ".join(tried))
    y, q, u = used
    pk.raw(f"HHD_C_Report_{y}Q{q}.xlsx", blob, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", url=u)
    x = RS.Xlsx(blob)
    pages = []
    for sh in x.sheet_names():
        m = re.match(r"^Page (\d+) Data$", sh)
        if not m:
            continue
        parsed = parse_sheet(x.rows(sh))
        if not parsed:
            continue
        title, unit, cols = parsed
        pno = int(m.group(1))
        for label, pts in cols.items():
            pk.add_series(f"P{pno}_{_sid(label)}", f"{title} — {label}", pts, unit=unit, freq="quarterly", group=f"page{pno}", page_title=title)
        pages.append([pno, title, unit, len(cols), max(p[-1][0] for p in cols.values())])
    if not pages:
        raise RuntimeError("HHDC workbook parsed no quarterly tables (layout changed?)")
    pk.add_table("pages", ["Page", "Chart", "Unit", "Columns", "Last quarter"], pages, title=f"Charts parsed from the {y}Q{q} report workbook")
    pk.extra["report_quarter"] = f"{y}Q{q}"

    def find(title_sub, label):
        for s in pk.series.values():
            if title_sub.lower() in (s.get("page_title") or "").lower() and s["label"].lower().endswith("— " + label.lower()):
                return s
        return None
    tot = find("Total Debt Balance", "Total")
    if tot:
        yo = next((v for d, v in tot["_points"] if d == f"{int(tot['date'][:4]) - 1}{tot['date'][4:]}"), None)
        pk.kpi("Total household debt", f"${tot['latest']:.2f}tn", f"{tot['date']} · Δ ${tot['chg']:+.2f}tn q/q" + (f" · {100 * (tot['latest'] / yo - 1):+.1f}% y/y" if yo else ""), "info")
    d90 = find("90+ Days Delinquent by Loan Type", "ALL")
    if d90:
        pk.kpi("Balance 90+ days delinquent", f"{d90['latest']:.2f}%", f"all products · {RS.ordinal(d90['pct_rank'])} pct since 2003", "warning" if d90["latest"] > 3 else "info")
    cc = find("90+ Days Delinquent by Loan Type", "CC")
    if cc:
        pk.kpi("Credit card 90+ delinquent", f"{cc['latest']:.2f}%", f"peak {cc['max']:.2f}% · auto {find('90+ Days Delinquent by Loan Type', 'AUTO')['latest']:.2f}%" if find("90+ Days Delinquent by Loan Type", "AUTO") else f"peak {cc['max']:.2f}%", "warning" if cc["latest"] > 10 else "info")
    cur = find("Total Balance by Delinquency Status", "Current")
    if cur:
        pk.kpi("Balance current", f"{cur['latest']:.1f}%", f"{100 - cur['latest']:.1f}% in some stage of delinquency", "info")
    pk.kpi("Report", f"{y}Q{q}", f"{len(pages)} chart tables · {pk.series and len(pk.series)} series", "mute")
    pk.note("Balances are from a 5% anonymised sample of Equifax credit files, scaled up; the Fed revises history as the sample is refreshed. "
            "Student-loan delinquency is distorted by the 2020–2024 payment pause and the 2025 resumption of reporting.")
    return pk


def lambda_handler(event, context=None):
    return RS.run(SLUG, build, event)
