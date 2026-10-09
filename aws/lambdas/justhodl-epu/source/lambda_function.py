"""justhodl-epu — Economic Policy Uncertainty indices (Baker, Bloom & Davis), policyuncertainty.com.

Two public files, no key:
  All_Country_Data.xlsx   monthly Global EPU (current-GDP and PPP weights) + ~25 country indices, 1985/1997→
  All_Daily_Policy_Data.csv   daily US news-based EPU, 1985→
Both are mirrored verbatim; series are normalised to month-end / ISO dates. Descriptive only.
"""
from __future__ import annotations

import re

import risk_sources as RS

SLUG = "epu"
XLSX = "https://www.policyuncertainty.com/media/All_Country_Data.xlsx"
DAILY = "https://www.policyuncertainty.com/media/All_Daily_Policy_Data.csv"
PAGE = "https://www.policyuncertainty.com/"


def _sid(name):
    return re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").upper()


def parse_global(rows):
    """rows of All_Country_Data: Year, Month, GEPU_current, GEPU_ppp, <countries...> -> {col: [[date, v], ...]}"""
    header = [str(h).strip() if h is not None else "" for h in rows[0]]
    out = {h: [] for h in header[2:] if h}
    for r in rows[1:]:
        if not r or len(r) < 3 or r[0] is None or r[1] is None:
            continue
        try:
            y, m = int(float(r[0])), int(float(r[1]))
        except (TypeError, ValueError):
            continue
        if not (1900 < y < 2100 and 1 <= m <= 12):
            continue
        d = RS.month_end(y, m)
        for i, h in enumerate(header[2:], 2):
            if h and i < len(r):
                v = RS.parse_num(r[i])
                if v is not None:
                    out[h].append([d, v])
    return out


def parse_daily(text):
    pts = []
    for row in RS.csv_rows(text):
        if len(row) < 4 or not row[0].strip().isdigit():
            continue
        try:
            d = f"{int(row[2]):04d}-{int(row[1]):02d}-{int(row[0]):02d}"
        except ValueError:
            continue
        v = RS.parse_num(row[3])
        if v is not None:
            pts.append([d, v])
    return pts


def rolling_mean(pts, n):
    out, buf, s = [], [], 0.0
    for d, v in pts:
        buf.append(v)
        s += v
        if len(buf) > n:
            s -= buf.pop(0)
        if len(buf) == n:
            out.append([d, round(s / n, 2)])
    return out


def build(event):
    pk = RS.Packet(
        SLUG, "Economic Policy Uncertainty", "Economic Policy Uncertainty (EPU) indices — Baker, Bloom & Davis",
        "Newspaper-based indices of economic policy uncertainty: the monthly Global EPU (GDP-weighted average of national "
        "indices, current and PPP weights), ~25 national monthly indices, and the daily US news-based index since 1985. "
        "Read directly from policyuncertainty.com's public files; values are index levels normalised by the authors.",
        {"provider": "Baker, Bloom & Davis — policyuncertainty.com", "url": PAGE, "docs": PAGE + "methodology.html",
         "license": "Public academic data (cite Baker, Bloom & Davis 2016)", "cadence": "monthly; US daily"},
        cadence="daily")
    blob = RS.http_get(XLSX, timeout=90)
    pk.raw("All_Country_Data.xlsx", blob, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", url=XLSX)
    cols = parse_global(RS.Xlsx(blob).rows(0))
    if "GEPU_current" not in cols:
        raise RuntimeError(f"EPU workbook header changed: {list(cols)[:6]}")
    pk.add_series("GEPU_CURRENT", "Global EPU (current-GDP weights)", cols.pop("GEPU_current"), unit="index", freq="monthly", group="global")
    pk.add_series("GEPU_PPP", "Global EPU (PPP-GDP weights)", cols.pop("GEPU_ppp", []), unit="index", freq="monthly", group="global")
    grid = []
    for name, pts in cols.items():
        s = pk.add_series("CTY_" + _sid(name), f"{name} — EPU", pts, unit="index", freq="monthly", group="country")
        if s:
            vals = [v for _, v in s["_points"]]
            avg12 = round(sum(vals[-12:]) / min(12, len(vals)), 1)
            yoy = next((v for d, v in s["_points"] if d[:7] == f"{int(s['date'][:4]) - 1}-{s['date'][5:7]}"), None)
            grid.append([name, round(s["latest"], 1), s["date"], avg12, s["pct_rank"],
                         round(100.0 * (s["latest"] / yoy - 1), 1) if yoy else None])
    grid.sort(key=lambda r: -(r[4] or 0))
    pk.add_table("countries", ["Country", "Latest", "Month", "12m avg", "Pct rank (own history)", "Δ y/y %"], grid,
                 title="National EPU indices", note="Percentile rank is against each country's own full history; indices are not comparable in level across countries.")
    try:
        txt = RS.http_text(DAILY, timeout=90)
        pk.raw("All_Daily_Policy_Data.csv", txt.encode("utf-8"), "text/csv", url=DAILY)
        daily = parse_daily(txt)
        pk.add_series("US_DAILY", "US daily news-based EPU", daily, unit="index", freq="daily", group="us")
        pk.add_series("US_DAILY_30D", "US daily EPU — 30-day mean", rolling_mean(daily, 30), unit="index", freq="daily", group="us")
    except Exception as exc:  # noqa: BLE001 — the monthly packet still stands without the daily file
        pk.file_status("All_Daily_Policy_Data.csv", DAILY, f"unavailable: {exc}")
    g = pk.series.get("GEPU_CURRENT")
    if g:
        pk.kpi("Global EPU", f"{g['latest']:.0f}", f"{g['date'][:7]} · {RS.ordinal(g['pct_rank'])} pct since {g['first'][:4]}",
               "warning" if g["pct_rank"] and g["pct_rank"] > 85 else "info")
        peak = max(g["_points"], key=lambda p: p[1])
        pk.kpi("Global EPU peak", f"{peak[1]:.0f}", f"{peak[0][:7]}", "mute")
    u = pk.series.get("US_DAILY_30D")
    if u:
        pk.kpi("US EPU, 30-day mean", f"{u['latest']:.0f}", f"to {u['date']} · {RS.ordinal(u['pct_rank'])} pct since 1985", "info")
    if grid:
        pk.kpi("Countries tracked", str(len(grid)), f"highest own-history rank: {grid[0][0]} ({grid[0][4]} pct)", "info")
    pk.note("EPU counts newspaper articles containing terms about the economy, policy and uncertainty, scaled to a mean of 100 over a "
            "base period that differs by country — compare each series with its own history, not across countries. Global EPU "
            "is a GDP-weighted average of the national indices.")
    return pk


def lambda_handler(event, context=None):
    return RS.run(SLUG, build, event)
