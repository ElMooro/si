"""justhodl-fhfa-hpi — FHFA House Price Index, from the agency's single master CSV (no key).

  https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv
  columns: hpi_type, hpi_flavor, frequency, level, place_name, place_id, yr, period, index_nsa, index_sa

We keep the parts that describe housing-collateral risk: the monthly purchase-only index for the US and the nine
Census divisions (SA and NSA), the quarterly purchase-only index for the 50 states + DC, and the quarterly
all-transactions index for the 100 largest metros by latest coverage. The 17 MB CSV is mirrored verbatim. Descriptive only.
"""
from __future__ import annotations

import csv
import io

import risk_sources as RS

SLUG = "fhfa-hpi"
CSV = "https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv"
PAGE = "https://www.fhfa.gov/data/hpi"
N_MSA = 100


def _date(freq, yr, period):
    y, p = int(yr), int(period)
    return RS.month_end(y, p) if freq == "monthly" else RS.quarter_end(y, p)


def parse(text):
    """-> {(flavor, freq, level, place_name, place_id): {'sa': [[d,v]], 'nsa': [[d,v]]}} for the kept slices."""
    keep = {}
    rd = csv.DictReader(io.StringIO(text))
    for r in rd:
        if r["hpi_type"] != "traditional":
            continue
        fl, fq, lv = r["hpi_flavor"], r["frequency"], r["level"]
        if (fl, fq, lv) not in (("purchase-only", "monthly", "USA or Census Division"),
                                ("purchase-only", "quarterly", "State"),
                                ("all-transactions", "quarterly", "MSA")):
            continue
        try:
            d = _date(fq, r["yr"], r["period"])
        except (TypeError, ValueError):
            continue
        k = (fl, fq, lv, r["place_name"], r["place_id"])
        slot = keep.setdefault(k, {"sa": [], "nsa": []})
        v = RS.parse_num(r.get("index_sa"))
        if v is not None:
            slot["sa"].append([d, v])
        v = RS.parse_num(r.get("index_nsa"))
        if v is not None:
            slot["nsa"].append([d, v])
    return keep


def _yoy(pts, date, step):
    """value `step` observations back (12 monthly / 4 quarterly) by date lookup."""
    y = int(date[:4]) - 1
    tgt = f"{y}{date[4:]}"
    return next((v for d, v in pts if d == tgt), None)


def build(event):
    pk = RS.Packet(
        SLUG, "FHFA House Price Index", "FHFA House Price Index — US, Census divisions, states, metros",
        "The Federal Housing Finance Agency's repeat-sales house price indices built from Fannie Mae / Freddie Mac "
        "mortgage data: the monthly purchase-only index (US and nine Census divisions), quarterly state purchase-only "
        "indices and quarterly all-transactions metro indices. Housing collateral is the largest asset on bank and "
        "household balance sheets; the turn in these indices led 2007. Read from FHFA's master CSV.",
        {"provider": "Federal Housing Finance Agency", "url": PAGE, "docs": "https://www.fhfa.gov/data/hpi/datasets",
         "license": "US government public data", "cadence": "monthly (last Tuesday) and quarterly"},
        cadence="daily")
    text = RS.http_text(CSV, timeout=180)
    pk.raw("hpi_master.csv", text.encode("utf-8"), "text/csv", url=CSV)
    keep = parse(text)
    if not keep:
        raise RuntimeError("FHFA master CSV: no rows matched the kept slices (schema changed?)")
    state_rows, msa_rows, div_rows = [], [], []
    msa_candidates = []
    for (fl, fq, lv, name, pid), pts in keep.items():
        if lv == "USA or Census Division":
            sid = "US" if name == "United States" else "DIV_" + pid.replace("DV_", "")
            s = pk.add_series(sid + "_SA", f"{name} — purchase-only HPI (SA)", pts["sa"], unit="index 1991-01=100", freq="monthly", group="national")
            pk.add_series(sid + "_NSA", f"{name} — purchase-only HPI (NSA)", pts["nsa"], unit="index 1991-01=100", freq="monthly", group="national")
            if s:
                yo = _yoy(s["_points"], s["date"], 12)
                peak = max(s["_points"], key=lambda p: p[1])
                div_rows.append([name, s["date"], round(s["latest"], 1), round(s["chg_pct"], 2) if s["chg_pct"] is not None else None,
                                 round(100 * (s["latest"] / yo - 1), 1) if yo else None, round(100 * (s["latest"] / peak[1] - 1), 1), peak[0]])
        elif lv == "State":
            s = pk.add_series("ST_" + pid, f"{name} — purchase-only HPI (SA, quarterly)", pts["sa"] or pts["nsa"], unit="index 1991-Q1=100", freq="quarterly", group="state")
            if s:
                yo = _yoy(s["_points"], s["date"], 4)
                peak = max(s["_points"], key=lambda p: p[1])
                state_rows.append([pid, name, s["date"], round(s["latest"], 1), round(s["chg_pct"], 2) if s["chg_pct"] is not None else None,
                                   round(100 * (s["latest"] / yo - 1), 1) if yo else None, round(100 * (s["latest"] / peak[1] - 1), 1)])
        else:
            msa_candidates.append((name, pid, pts))
    # metros: keep the N_MSA with the longest current history (proxy for the biggest / best covered)
    msa_candidates.sort(key=lambda t: (-len(t[2]["nsa"]), t[0]))
    for name, pid, pts in msa_candidates[:N_MSA]:
        s = pk.add_series("MSA_" + pid, f"{name} — all-transactions HPI (NSA, quarterly)", pts["nsa"], unit="index 1995-Q1=100", freq="quarterly", group="msa")
        if s:
            yo = _yoy(s["_points"], s["date"], 4)
            peak = max(s["_points"], key=lambda p: p[1])
            msa_rows.append([pid, name, s["date"], round(s["latest"], 1), round(100 * (s["latest"] / yo - 1), 1) if yo else None,
                             round(100 * (s["latest"] / peak[1] - 1), 1)])
    div_rows.sort(key=lambda r: (r[0] != "United States", r[0]))
    state_rows.sort(key=lambda r: (r[5] if r[5] is not None else 999))
    msa_rows.sort(key=lambda r: (r[4] if r[4] is not None else 999))
    pk.add_table("national", ["Area", "Month", "Index (SA)", "Δ m/m %", "Δ y/y %", "From peak %", "Peak month"], div_rows,
                 title="US and Census divisions — monthly purchase-only index (SA)")
    pk.add_table("states", ["State", "Name", "Quarter", "Index", "Δ q/q %", "Δ y/y %", "From peak %"], state_rows,
                 title="States — quarterly purchase-only index (SA)", note="Sorted weakest year-on-year first.")
    pk.add_table("metros", ["MSA", "Name", "Quarter", "Index", "Δ y/y %", "From peak %"], msa_rows,
                 title=f"Metros — quarterly all-transactions index ({len(msa_rows)} with the longest coverage)", note="Sorted weakest year-on-year first.")
    us = pk.series.get("US_SA")
    if us:
        yo = _yoy(us["_points"], us["date"], 12)
        peak = max(us["_points"], key=lambda p: p[1])
        pk.kpi("US purchase-only HPI (SA)", f"{us['latest']:.1f}", f"{us['date'][:7]} · Δ {us['chg_pct']:+.2f}% m/m" if us["chg_pct"] is not None else us["date"][:7], "info")
        if yo:
            yy = 100 * (us["latest"] / yo - 1)
            pk.kpi("US year on year", f"{yy:+.1f}%", "nominal, seasonally adjusted", "neg" if yy < 0 else "info")
        pk.kpi("From peak", f"{100 * (us['latest'] / peak[1] - 1):+.1f}%", f"peak {peak[0][:7]}", "warning" if us["latest"] < peak[1] * 0.97 else "mute")
    neg_states = [r for r in state_rows if r[5] is not None and r[5] < 0]
    if state_rows:
        pk.kpi("States falling y/y", str(len(neg_states)), f"of {len(state_rows)} · weakest {state_rows[0][0]} {state_rows[0][5]:+.1f}%" if state_rows[0][5] is not None else f"of {len(state_rows)}",
               "warning" if len(neg_states) >= 10 else "info")
    if msa_rows:
        neg_msa = sum(1 for r in msa_rows if r[4] is not None and r[4] < 0)
        pk.kpi("Metros falling y/y", str(neg_msa), f"of {len(msa_rows)} tracked", "warning" if neg_msa >= 25 else "info")
    pk.note("Repeat-sales indices from conforming mortgages (Fannie/Freddie), so jumbo and cash markets are under-represented. The monthly "
            "purchase-only index is 1991-01 = 100; state and metro indices are quarterly. 'From peak' is nominal.")
    return pk


def lambda_handler(event, context=None):
    return RS.run(SLUG, build, event)
