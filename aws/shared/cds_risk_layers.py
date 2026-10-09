"""cds_risk_layers — v1.6.0 risk layers for the CDS desk (sovereign + U.S. corporate).

Five free, keyless layers are attached to the desk packet on every run:

  imf_ara_gdd   IMF DataMapper — reserve adequacy (ARA metric, months of imports,
                short-term-debt cover) and private debt (total / household / NFC,
                % of GDP, Global Debt Database) per sovereign
  ecb_sup       ECB Supervisory Banking Statistics (SUP) — significant + less
                significant euro-area banks' exposures to each euro-area general
                government (S13, EUR mn, half-yearly since 2021-S2): the bank side
                of the bank–sovereign nexus; also written to data/warm/ecb-sup/
  eba           EBA Risk Dashboard (via data/eba-risk-dashboard.json) — share of
                each banking system's sovereign book held in its own government
  esma          ESMA European Rating Platform (via data/esma-ratings.json) — current
                S&P / Moody's / Fitch / DBRS / Scope ratings, outlooks, consensus notch
  ofr_form_pf   OFR Hedge Fund Monitor Form PF aggregates (warm data/warm/ofr-hfm/) —
                qualifying hedge funds' gross notional exposure to sovereign debt
                and to credit, repo borrowing, leverage, and the Form PF credit-spread
                stress test (effect of +250 bp)
  us_credit     NY Fed CMDI (data/nyfed-cmdi.json) and FDIC BankFind aggregates
                (data/fdic-bankfind.json) for the U.S. corporate panel

Everything is descriptive (no call, no sizing). Each layer is optional: a failing
source leaves a status string, never blocks the desk.
"""
from __future__ import annotations

import csv
import gzip
import io
import json

VERSION = "1.6.0"

IMF_ARA = {"Reserves_ARA": ("ara", "reserves / IMF ARA metric (1.0–1.5 = adequate)"),
           "Reserves_M": ("res_months_imports", "reserves, months of imports"),
           "Reserves_STD": ("res_std_cover", "reserves / short-term external debt"),
           "Reserves_M2": ("res_m2_pct", "reserves, % of broad money")}
IMF_GDD = {"Privatedebt_all": ("private_debt_gdp", "private debt (loans + debt securities), % of GDP"),
           "HH_ALL": ("hh_debt_gdp", "household debt, % of GDP"),
           "NFC_ALL": ("nfc_debt_gdp", "non-financial corporate debt, % of GDP")}
IMF_URL = "https://www.imf.org/external/datamapper/api/v1/%s"
ECB_SUP_URL = ("https://data-api.ecb.europa.eu/service/data/SUP/H...S13.E0010._T.ALL._Z.ALL.LE.E.C"
               "?format=csvdata&detail=dataonly")
ISO2_3 = {"AT": "AUT", "BE": "BEL", "BG": "BGR", "CY": "CYP", "DE": "DEU", "EE": "EST", "ES": "ESP", "FI": "FIN", "FR": "FRA",
          "GR": "GRC", "HR": "HRV", "IE": "IRL", "IT": "ITA", "LT": "LTU", "LU": "LUX", "LV": "LVA", "MT": "MLT", "NL": "NLD",
          "PT": "PRT", "SI": "SVN", "SK": "SVK", "CZ": "CZE", "DK": "DNK", "HU": "HUN", "PL": "POL", "RO": "ROU", "SE": "SWE",
          "GB": "GBR", "IS": "ISL", "NO": "NOR", "LI": "LIE", "US": "USA", "EU": "EU"}
SUP_AGG = {"W0": "all counterparties", "W1": "rest of the world", "G00": "euro area (unallocated)", "E10": "EU institutions", "_X": "other"}
OFR_FPF = {"FPF-ASSETCLASS_SOVEREIGN_GNE_SUM": ("sov_gne", "QHF gross notional exposure to sovereign debt", "$"),
           "FPF-ASSETCLASS_CREDIT_GNE_SUM": ("credit_gne", "QHF gross notional exposure to credit (bonds, loans, CDS, structured)", "$"),
           "FPF-ASSETCLASS_SOVEREIGN_LGNE_SUM": ("sov_long", "QHF long notional, sovereign debt", "$"),
           "FPF-ASSETCLASS_SOVEREIGN_SGNE_SUM": ("sov_short", "QHF short notional, sovereign debt", "$"),
           "FPF-ASSETCLASS_CREDIT_LGNE_SUM": ("credit_long", "QHF long notional, credit", "$"),
           "FPF-ASSETCLASS_CREDIT_SGNE_SUM": ("credit_short", "QHF short notional, credit", "$"),
           "FPF-BORROW_REPO_SUM": ("repo_borrow", "QHF repo borrowing", "$"),
           "FPF-ALLQHF_CDSUP250BPS_P50": ("cds_up250_p50", "median effect on NAV of +250 bp credit spreads (%)", "%"),
           "FPF-ALLQHF_CDSDOWN250BPS_P50": ("cds_dn250_p50", "median effect on NAV of −250 bp credit spreads (%)", "%"),
           "FPF-ALLQHF_CDSUP250BPS_P5": ("cds_up250_p5", "5th-percentile effect on NAV of +250 bp credit spreads (%)", "%"),
           "FPF-STRATEGY_CREDIT_LEVERAGERATIO_NAVWMEAN": ("credit_leverage", "credit-strategy funds' leverage (GAV/NAV, NAV-weighted)", "x"),
           "FPF-ALLQHF_GAVN10_LEVERAGERATIO_AVERAGE": ("top10_leverage", "ten largest QHFs' leverage (GAV/NAV, average)", "x"),
           "FPF-STRATEGY_CREDIT_GAV_SUM": ("credit_gav", "credit-strategy funds' gross assets", "$")}


def _f(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v == v else None


# ───────────────────────────────────────────────── IMF ARA + GDD ──
def parse_imf_json(text, indicator):
    try:
        d = json.loads(text)
    except ValueError:
        return {}
    vals = ((d.get("values") or {}).get(indicator)) or {}
    out = {}
    for iso3, years in vals.items():
        if isinstance(years, dict) and len(iso3) == 3:
            clean = {str(y): _f(v) for y, v in years.items() if _f(v) is not None}
            if clean:
                out[iso3] = clean
    return out


def _latest(series, max_year):
    ys = [y for y in series if y.isdigit() and int(y) <= max_year]
    if not ys:
        return None, None
    y = max(ys)
    return series[y], y


def attach_imf_layers(packet, imf, as_of_iso):
    """imf: {indicator: {iso3: {year: value}}} -> e['ara'], e['pdebt'] on sovereign universe / rows."""
    yr = int(as_of_iso[:4])
    sov = (packet.get("groups") or {}).get("sovereign") or {}
    by_key, n_ara, n_debt = {}, 0, 0
    for e in sov.get("universe") or []:
        iso3 = e.get("iso3")
        if not iso3:
            continue
        ara, debt = {}, {}
        for ind, (alias, _l) in IMF_ARA.items():
            v, y = _latest((imf.get(ind) or {}).get(iso3) or {}, yr)
            if v is not None:
                ara[alias] = round(v, 2)
                ara["year"] = max(ara.get("year", "0"), y)
        for ind, (alias, _l) in IMF_GDD.items():
            v, y = _latest((imf.get(ind) or {}).get(iso3) or {}, yr)
            if v is not None:
                debt[alias] = round(v, 1)
                debt["year"] = max(debt.get("year", "0"), y)
        if ara:
            e["ara"] = ara
            n_ara += 1
        if debt:
            e["pdebt"] = debt
            n_debt += 1
        if e.get("key") and (ara or debt):
            by_key[e["key"]] = (ara or None, debt or None)
    for bucket in ("rows", "unpriced", "dormant"):
        for r in sov.get(bucket) or []:
            if r.get("key") in by_key:
                a, d = by_key[r["key"]]
                if a:
                    r["ara"] = a
                if d:
                    r["pdebt"] = d
    packet.setdefault("layers", {})["imf_ara_gdd"] = {
        "source": "IMF DataMapper — Assessing Reserve Adequacy (ARA) and Global Debt Database (GDD), keyless JSON",
        "indicators": {**{a: l for _i, (a, l) in IMF_ARA.items()}, **{a: l for _i, (a, l) in IMF_GDD.items()}},
        "n_reserve_adequacy": n_ara, "n_private_debt": n_debt,
        "read": "ARA below 1.0 = reserves below the IMF's adequacy range (emerging markets only; reserve-currency issuers are not assessed). "
                "Private debt is the household + corporate leverage behind a sovereign's contingent liabilities."}
    return n_ara, n_debt


# ───────────────────────────────────────────────────── ECB SUP ──
def parse_ecb_sup(text):
    """csvdata -> {(ref_area, count_area): {period: value}}  (EUR mn, S13 total exposures)."""
    out = {}
    for r in csv.DictReader(io.StringIO(text)):
        v = _f(r.get("OBS_VALUE"))
        if v is None:
            continue
        out.setdefault((r.get("REF_AREA"), r.get("COUNT_AREA")), {})[r.get("TIME_PERIOD")] = v
    return out


def _period_iso(p):
    # 2025-S2 -> 2025-12-31 ; 2025-S1 -> 2025-06-30
    y, s = p.split("-S")
    return f"{y}-12-31" if s == "2" else f"{y}-06-30"


def attach_ecb_sup(packet, sup):
    """Euro-area banks' exposure to each euro-area government; per-sovereign e['banks'] + packet['bank_sovereign']."""
    if not sup:
        return 0
    periods = sorted({p for m in sup.values() for p in m})
    last, prev_y = periods[-1], (periods[-3] if len(periods) >= 3 else None)
    ea_total = (sup.get(("B01", "W0")) or {}).get(last)
    rows, by_iso3 = [], {}
    for (ref, cnt), m in sup.items():
        if ref != "B01" or cnt not in ISO2_3:
            continue
        iso3 = ISO2_3[cnt]
        v = m.get(last)
        if v is None:
            continue
        home = (sup.get((cnt, cnt)) or {}).get(last)
        rec = {"ea_banks_eur_bn": round(v / 1000.0, 1), "period": last, "period_end": _period_iso(last),
               "share_of_ea_sov_book_pct": round(100.0 * v / ea_total, 1) if ea_total else None,
               "home_banks_eur_bn": round(home / 1000.0, 1) if home is not None else None,
               "home_banks_share_pct": round(100.0 * home / v, 1) if (home is not None and v) else None,
               "chg_yoy_pct": round(100.0 * (v / m[prev_y] - 1), 1) if (prev_y and m.get(prev_y)) else None,
               "history": [[_period_iso(p), round(m[p] / 1000.0, 1)] for p in periods if p in m]}
        by_iso3[iso3] = rec
        rows.append([iso3, rec["ea_banks_eur_bn"], rec["share_of_ea_sov_book_pct"], rec["home_banks_eur_bn"], rec["home_banks_share_pct"], rec["chg_yoy_pct"]])
    rows.sort(key=lambda r: -(r[1] or 0))
    sov = (packet.get("groups") or {}).get("sovereign") or {}
    n = 0
    by_key = {}
    for e in sov.get("universe") or []:
        rec = by_iso3.get(e.get("iso3"))
        if rec:
            e["banks"] = dict(rec, history=None)
            n += 1
            if e.get("key"):
                by_key[e["key"]] = e["banks"]
    for bucket in ("rows", "unpriced", "dormant"):
        for r in sov.get(bucket) or []:
            if r.get("key") in by_key:
                r["banks"] = by_key[r["key"]]
    agg = {k: {"eur_bn": round(((sup.get(("B01", k)) or {}).get(last) or 0) / 1000.0, 1), "label": lab} for k, lab in SUP_AGG.items() if (sup.get(("B01", k)) or {}).get(last)}
    packet["bank_sovereign"] = {
        "source": "ECB Supervisory Banking Statistics (SUP), dataset key SUP.H.<bank home>.<government>.S13.E0010 — total exposures of significant and less significant institutions to general government, EUR mn, half-yearly",
        "url": ECB_SUP_URL, "period": last, "period_end": _period_iso(last), "periods": [_period_iso(p) for p in periods],
        "ea_banks_total_sov_eur_bn": round(ea_total / 1000.0, 1) if ea_total else None,
        "aggregates": agg,
        "columns": ["ISO3", "Euro-area banks' exposure €bn", "% of EA banks' sovereign book", "Own banks' exposure €bn", "Own banks' share %", "Δ y/y %"],
        "rows": rows, "by_iso3": by_iso3, "n_sovereigns": n,
        "read": "How much of each euro-area government's debt sits on euro-area bank balance sheets, and how much of that is held by the country's own banks — the doom-loop channel of 2011-12. Counterparts are euro-area governments only."}
    return n


# ────────────────────────────────────────────── EBA home bias ──
def attach_eba(packet, eba_packet):
    if not eba_packet:
        return 0
    t = (eba_packet.get("tables") or {}).get("sovereign_grid") or {}
    cols, rows = t.get("columns") or [], t.get("rows") or []
    if not rows:
        return 0
    ci = {c: i for i, c in enumerate(cols)}
    by_iso3 = {}
    for r in rows:
        cc = r[0]
        iso3 = ISO2_3.get(cc)
        if not iso3:
            continue
        by_iso3[iso3] = {"home_bias_pct": r[ci.get("Home sovereign %", 1)], "other_eu_pct": r[ci.get("Other EU/EEA %", 2)],
                         "amortised_cost_pct": r[ci.get("Amortised cost %", 3)], "long_10y_pct": r[ci.get("10Y+ %", 5)],
                         "total_eur_bn": r[ci.get("Total €bn", 6)], "period": eba_packet.get("latest_period") or (t.get("title") or "")[-10:]}
    sov = (packet.get("groups") or {}).get("sovereign") or {}
    n, by_key = 0, {}
    for e in sov.get("universe") or []:
        rec = by_iso3.get(e.get("iso3"))
        if rec:
            e["eba"] = rec
            n += 1
            if e.get("key"):
                by_key[e["key"]] = rec
    for bucket in ("rows", "unpriced", "dormant"):
        for r in sov.get(bucket) or []:
            if r.get("key") in by_key:
                r["eba"] = by_key[r["key"]]
    packet.setdefault("layers", {})["eba"] = {"source": "EBA Risk Dashboard, Sovereigns sheet (via the EBA engine, data/eba-risk-dashboard.json)",
                                               "page": "/eba-risk-dashboard.html", "n_sovereigns": n, "eu_home_bias_pct": by_iso3.get("EU", {}).get("home_bias_pct"),
                                               "period": (by_iso3.get("EU") or {}).get("period"),
                                               "read": "Share of each country's banks' sovereign portfolio invested in their own government; amortised-cost share = the part not marked to market."}
    return n


# ─────────────────────────────────────────────── ESMA ratings ──
def attach_esma(packet, esma_packet):
    cons = (esma_packet or {}).get("consensus") or {}
    if not cons:
        return 0
    sov = (packet.get("groups") or {}).get("sovereign") or {}
    n, by_key = 0, {}
    for e in sov.get("universe") or []:
        c = cons.get(e.get("iso3"))
        if c:
            rec = {"consensus": c.get("consensus_label"), "notch": c.get("consensus_notch"), "n_agencies": c.get("n_agencies"),
                   "neg": c.get("negative_outlooks"), "pos": c.get("positive_outlooks"), "last_action": c.get("last_action_date"),
                   "agencies": {a: {"rating": v.get("rating"), "outlook": v.get("outlook"), "date": v.get("date"), "last_action": v.get("last_action")}
                                for a, v in (c.get("ratings") or {}).items()}}
            e["rating"] = rec
            n += 1
            if e.get("key"):
                by_key[e["key"]] = rec
    for bucket in ("rows", "unpriced", "dormant"):
        for r in sov.get(bucket) or []:
            if r.get("key") in by_key:
                r["rating"] = by_key[r["key"]]
    acts = ((esma_packet.get("tables") or {}).get("actions") or {}).get("rows") or []
    packet.setdefault("layers", {})["esma"] = {"source": "ESMA European Rating Platform (via the ESMA engine, data/esma-ratings.json)", "page": "/esma-ratings.html",
                                                "n_sovereigns": n, "as_of": esma_packet.get("as_of"), "recent_actions": acts[:40],
                                                "read": "Current long-term foreign-currency sovereign ratings of the main agencies and the latest upgrade / downgrade tape, timestamped by ESMA."}
    return n


# ───────────────────────────────────────────── OFR Form PF ──
def _ofr_points(blob):
    """warm ofr-hfm series file (possibly gzip) -> [[date, value]]"""
    try:
        blob = gzip.decompress(blob)
    except (OSError, EOFError):
        pass
    d = json.loads(blob)
    if isinstance(d, dict) and "points" in d:
        return d["points"]
    for v in d.values():
        if isinstance(v, dict) and "timeseries" in v:
            agg = (v["timeseries"] or {}).get("aggregation") or []
            return [[p[0], p[1]] for p in agg if p and p[1] is not None]
    return []


def attach_ofr_form_pf(packet, reader):
    """reader(mnemonic) -> bytes or None. Builds packet['hedge_funds']."""
    series, status = {}, {}
    for mn, (alias, label, unit) in OFR_FPF.items():
        try:
            blob = reader(mn)
            pts = _ofr_points(blob) if blob else []
        except Exception as exc:  # noqa: BLE001
            pts, status[mn] = [], "%s: %s" % (type(exc).__name__, exc)
        if not pts:
            status.setdefault(mn, "not in warm store")
            continue
        scale = 1e-9 if unit == "$" else 1.0
        pts = [[p[0], round(p[1] * scale, 2 if unit == "$" else 2)] for p in pts]
        last, prev = pts[-1], (pts[-5] if len(pts) >= 5 else None)
        series[alias] = {"mnemonic": mn, "label": label, "unit": "$bn" if unit == "$" else unit, "points": pts,
                         "last": {"date": last[0], "value": last[1]},
                         "chg_yoy_pct": round(100.0 * (last[1] / prev[1] - 1), 1) if (prev and prev[1]) else None,
                         "max": max(p[1] for p in pts), "max_date": max(pts, key=lambda p: p[1])[0]}
        status[mn] = "ok"
    sg, cg = series.get("sov_gne"), series.get("credit_gne")
    packet["hedge_funds"] = {
        "source": "OFR Hedge Fund Monitor — SEC Form PF aggregates for qualifying hedge funds (data.financialresearch.gov/hf/v1), mirrored by the OFR engine under data/warm/ofr-hfm/",
        "page": "/ofr.html", "series": series, "status": status, "n_series": len(series),
        "as_of": sg["last"]["date"] if sg else (cg["last"]["date"] if cg else None),
        "read": "Who is on the other side of the sovereign and credit books: hedge funds' gross notional in sovereign debt and in credit (which includes CDS), their repo funding, and the Form PF stress test — the median fund's NAV change for +250 bp of credit spreads."}
    return len(series)


# ────────────────────────────────────── U.S. credit: CMDI + FDIC ──
def attach_us_credit(packet, cmdi_packet, fdic_packet):
    out = {}
    if cmdi_packet and cmdi_packet.get("series"):
        s = cmdi_packet["series"]
        pick = lambda sid: {"value": s[sid]["latest"], "date": s[sid]["date"], "pct_rank_since_2005": s[sid].get("pct_rank"),  # noqa: E731
                            "chg_w": s[sid].get("chg"), "max": s[sid].get("max"), "tail": (s[sid].get("tail") or [])[-260:]} if sid in s else None
        out["cmdi"] = {"market": pick("MARKET"), "ig": pick("IG"), "hy": pick("HY"), "page": "/nyfed-cmdi.html", "as_of": cmdi_packet.get("as_of"),
                       "source": "NY Fed Corporate Bond Market Distress Index (via data/nyfed-cmdi.json)",
                       "read": "The Fed's own 0–1 gauge of U.S. corporate bond market functioning (primary issuance, secondary spreads and liquidity), weekly since 2005. Pair with CDX IG / HY: distress in the cash market with calm CDS, or the reverse, is a basis signal."}
    if fdic_packet:
        a = ((fdic_packet.get("aggregates") or {}).get(fdic_packet.get("latest_quarter")) or {})
        s = fdic_packet.get("series") or {}
        out["fdic"] = {"quarter": fdic_packet.get("latest_quarter"), "aggregates": a, "page": "/fdic-bankfind.html",
                       "series": {k: {"label": s[k]["label"], "tail": s[k].get("tail")} for k in ("SYS_HTM_LOSS_TO_EQUITY", "SYS_UNINSURED_SHARE", "SYS_CRE_TO_TIER1", "SYS_N_HTM_LOSS_GT50_EQUITY") if k in s},
                       "screen": ((fdic_packet.get("tables") or {}).get("fragility_screen") or {}).get("rows", [])[:15],
                       "screen_columns": ((fdic_packet.get("tables") or {}).get("fragility_screen") or {}).get("columns"),
                       "source": "FDIC BankFind Suite API, all insured institutions' Call Reports (via data/fdic-bankfind.json)",
                       "read": "Balance-sheet fragility of the U.S. banking system behind the financial names in the corporate CDS panel: uninsured deposits, held-to-maturity losses against equity, CRE concentration."}
    if out:
        packet["us_credit"] = out
    return len(out)
