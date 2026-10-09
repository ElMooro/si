"""justhodl-fragile-states-index — Fund for Peace Fragile States Index, annual 2006→ (public Excel, no key).

The /excel/ page lists one workbook per edition (fsi-2006.xlsx … FSI-2023-DOWNLOAD.xlsx), each a flat sheet:
  Country, Year, Rank, Total, then the twelve indicators (C1 Security Apparatus … X1 External Intervention).
All editions are harvested, so each country gets an annual Total series plus the latest-edition indicator grid.
Descriptive only.
"""
from __future__ import annotations

import re

import risk_sources as RS

SLUG = "fragile-states-index"
PAGE = "https://fragilestatesindex.org/excel/"
HOME = "https://fragilestatesindex.org/"


def find_workbooks(html):
    links = set(re.findall(r'href="\s*(https?://fragilestatesindex\.org/[^"]*?\.xlsx)\s*"', html, re.I))
    out = {}
    for u in links:
        m = re.search(r"fsi-?(\d{4})", u, re.I)
        if m:
            out.setdefault(int(m.group(1)), u)
    return dict(sorted(out.items()))


def parse(rows):
    """-> (indicator_names, [{country, year, rank, total, ind: {name: v}}])"""
    header = [str(h).strip() if h is not None else "" for h in rows[0]]
    col = {h: i for i, h in enumerate(header)}
    for need in ("Country", "Year", "Total"):
        if need not in col:
            raise RuntimeError(f"FSI header changed: {header}")
    inds = [h for h in header if re.match(r"^[A-Z]\d:", h)]
    out = []
    for r in rows[1:]:
        if not r or len(r) <= col["Total"] or not r[col["Country"]]:
            continue
        try:
            y = int(float(r[col["Year"]]))
        except (TypeError, ValueError):
            continue
        if y > 3000:                      # 2016+ workbooks store Year as an Excel date serial (44562 = 2022-01-01)
            d = RS.excel_date(y)
            if not d:
                continue
            y = int(d[:4])
        tot = RS.parse_num(r[col["Total"]])
        if tot is None:
            continue
        rank = r[col["Rank"]] if "Rank" in col and col["Rank"] < len(r) else None
        out.append({"country": str(r[col["Country"]]).strip(), "year": y, "rank": str(rank) if rank is not None else None,
                    "total": tot, "ind": {h: RS.parse_num(r[col[h]]) for h in inds if col[h] < len(r)}})
    return inds, out


def _sid(name):
    return re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").upper()


def build(event):
    pk = RS.Packet(
        SLUG, "Fragile States Index", "Fragile States Index (Fund for Peace)",
        "Annual 0–120 fragility score for ~179 countries built from twelve cohesion, economic, political and social "
        "indicators (security apparatus, factionalised elites, group grievance, economic decline, uneven development, "
        "human flight, state legitimacy, public services, human rights, demographic pressures, refugees/IDPs, external "
        "intervention). Higher = more fragile. Every public edition since 2006 is harvested from the FFP download page.",
        {"provider": "The Fund for Peace", "url": HOME, "docs": "https://fragilestatesindex.org/methodology/",
         "license": "Public (Fund for Peace terms)", "cadence": "annual"},
        cadence="weekly")
    html = RS.http_text(PAGE, timeout=60)
    books = find_workbooks(html)
    if not books:
        raise RuntimeError("FSI download page lists no workbooks")
    by_country = {}
    latest = {"year": None, "inds": [], "rows": []}
    for year, url in books.items():
        try:
            blob = RS.http_get(url, timeout=60)
        except Exception as exc:  # noqa: BLE001
            pk.file_status(url.rsplit("/", 1)[-1], url, f"unavailable: {exc}")
            continue
        pk.raw(f"fsi-{year}.xlsx", blob, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", url=url)
        inds, rows = parse(RS.Xlsx(blob).rows(0))
        for r in rows:
            by_country.setdefault(r["country"], []).append([f"{r['year']}-12-31", r["total"]])
        if latest["year"] is None or year > latest["year"]:
            latest = {"year": year, "inds": inds, "rows": rows}
    for country, pts in sorted(by_country.items()):
        pk.add_series("TOTAL_" + _sid(country), f"{country} — FSI total", pts, unit="score 0–120", freq="annual", group="total", country=country)
    grid = []
    for r in latest["rows"]:
        s = pk.series.get("TOTAL_" + _sid(r["country"]))
        prev = next((v for d, v in s["_points"] if d[:4] == str(r["year"] - 1)), None) if s else None
        p5 = next((v for d, v in s["_points"] if d[:4] == str(r["year"] - 5)), None) if s else None
        grid.append([r["country"], r["rank"], round(r["total"], 1), round(r["total"] - prev, 1) if prev is not None else None,
                     round(r["total"] - p5, 1) if p5 is not None else None] + [r["ind"].get(h) for h in latest["inds"]])
    grid.sort(key=lambda g: -g[2])
    pk.add_table("grid", ["Country", "Rank", "Total", "Δ 1y", "Δ 5y"] + latest["inds"], grid,
                 title=f"Fragile States Index {latest['year']} — all indicators", note="Higher = more fragile. Δ columns are score changes against the earlier editions.")
    pk.extra["latest_edition"] = latest["year"]
    pk.extra["editions"] = sorted(books)
    if grid:
        pk.kpi("Latest edition", str(latest["year"]), f"{len(grid)} countries · editions {min(books)}–{max(books)}", "info")
        pk.kpi("Most fragile", grid[0][0], f"{grid[0][2]} / 120", "neg")
        worsening = sorted([g for g in grid if g[3] is not None], key=lambda g: -g[3])[:3]
        if worsening:
            pk.kpi("Largest 1y deterioration", f"{worsening[0][0]} {worsening[0][3]:+.1f}", ", ".join(f"{g[0]} {g[3]:+.1f}" for g in worsening[1:]), "warning")
        alert = sum(1 for g in grid if g[2] >= 90)
        pk.kpi("Alert and above (≥90)", str(alert), f"{sum(1 for g in grid if g[2] >= 100)} at high alert (≥100)", "info")
    pk.note("The Fund for Peace publishes one edition a year; the public download page is the source of truth for which editions exist "
            "(2023 was the last workbook listed at the time of writing — if a newer edition appears it is picked up automatically). "
            "Scores are expert-coded with content analysis; year-on-year moves below ~1 point are noise.")
    return pk


def lambda_handler(event, context=None):
    return RS.run(SLUG, build, event)
