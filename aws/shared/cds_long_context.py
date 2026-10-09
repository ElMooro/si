"""cds_long_context.py — where today's CDS tape sits against two decades of free, non-truncated credit history.

The DTCC public tape only reaches back to 2024-09 and FRED's ICE BofA OAS series were cut to a rolling three
years in April 2026, so neither can show 2008.  Three free government sources still carry the full record:

  * Moody's Baa minus 10Y Treasury (FRED BAA10Y) — daily since 1986.  The closest investment-grade corporate
    spread to CDX IG that is still published in full.  Dec-2008 peak ~616bp.
  * OFR Financial Stress Index (financialresearch.gov) — daily since 2000, with a Credit category built from
    corporate and CDS spreads, and an Emerging-markets regional category.
  * Gilchrist–Zakrajšek credit spread and excess bond premium (Federal Reserve FEDS notes) — monthly since 1973.
  * ECB SovCISS (Composite Indicator of Sovereign Stress) — daily since 2000 for eleven euro-area states and the euro
    area, built from 2Y/10Y yield spreads to swaps, realised yield volatility and bid-ask spreads (ECB Data Portal,
    dataset CISS, keyless CSV).  The only free, country-specific sovereign-market stress record that reaches 2008.
  * IMF World Economic Outlook via the DataMapper API (keyless JSON): gross debt, fiscal balance, current account,
    growth and inflation for every sovereign in the universe, including the ones with no public CDS print.

The live CDX IG level is mapped onto Baa–10Y with a least-squares fit over the overlap window (the days both
series exist), and the mapped level is placed on the 2006→today distribution.  The fit statistics (n, r², slope)
are published with the mapping; a weak fit is reported, not hidden.  ICE HY/IG OAS (three years) are kept only
for the CDS–bond basis.

Pure functions over CSV text; no network.  Descriptive measurements only.
"""
from datetime import date, timedelta
import calendar
import csv
import io
import math
import statistics

LONG_START = "2006-01-01"
CRISIS_WINDOWS = (("GFC 2008-09", "2008-01-01", "2009-12-31"), ("Euro crisis 2011-12", "2011-01-01", "2012-12-31"),
                  ("Energy 2015-16", "2015-06-01", "2016-06-30"), ("COVID 2020", "2020-01-01", "2020-12-31"),
                  ("Hiking cycle 2022-23", "2022-01-01", "2023-12-31"))
FRED_SERIES = {"BAA10Y": "Moody's Baa − 10Y Treasury", "AAA10Y": "Moody's Aaa − 10Y Treasury",
               "BAMLH0A0HYM2": "ICE BofA US HY OAS", "BAMLC0A0CM": "ICE BofA US Corp (IG) OAS"}
ECB_ISO2_TO_ISO3 = {"AT": "AUT", "BE": "BEL", "DE": "DEU", "ES": "ESP", "FI": "FIN", "FR": "FRA", "GR": "GRC", "IE": "IRL", "IT": "ITA",
                    "NL": "NLD", "PT": "PRT", "U2": "EA", "GB": "GBR", "US": "USA", "CN": "CHN",
                    # CLIFS (monthly country-level financial stress) adds the rest of the EU
                    "BG": "BGR", "CY": "CYP", "CZ": "CZE", "DK": "DNK", "EE": "EST", "HR": "HRV", "HU": "HUN", "LT": "LTU", "LU": "LUX",
                    "LV": "LVA", "MT": "MLT", "PL": "POL", "RO": "ROU", "SE": "SWE", "SI": "SVN", "SK": "SVK"}
ECB_NAMES = {"AUT": "Austria", "BEL": "Belgium", "DEU": "Germany", "ESP": "Spain", "FIN": "Finland", "FRA": "France", "GRC": "Greece",
             "IRL": "Ireland", "ITA": "Italy", "NLD": "Netherlands", "PRT": "Portugal", "EA": "euro area", "GBR": "United Kingdom",
             "USA": "United States", "CHN": "China", "BGR": "Bulgaria", "CYP": "Cyprus", "CZE": "Czech Republic", "DNK": "Denmark",
             "EST": "Estonia", "HRV": "Croatia", "HUN": "Hungary", "LTU": "Lithuania", "LUX": "Luxembourg", "LVA": "Latvia", "MLT": "Malta",
             "POL": "Poland", "ROU": "Romania", "SWE": "Sweden", "SVN": "Slovenia", "SVK": "Slovakia"}
# three ECB stress families, in the order a sovereign's own 2006 -> record is chosen (most sovereign-specific first)
ECB_FAMILIES = (
    ("sovciss", "ECB SovCISS", "index (0 calm → 1 extreme sovereign stress)",
     "ECB Data Portal CISS dataset, daily SovCISS since 2000 (2Y/10Y spread to swaps, yield volatility, bid-ask)", "sovereign-market stress"),
    ("ciss", "ECB CISS", "index (0 calm → 1 extreme systemic financial stress)",
     "ECB Data Portal CISS dataset, daily new CISS since 2000 (money, bond, equity and FX markets, financial intermediaries, and their cross-correlation)",
     "systemic financial stress"),
    ("clifs", "ECB CLIFS", "index (0 calm → 1 extreme country financial stress)",
     "ECB Data Portal CLIFS dataset, monthly Country-Level Index of Financial Stress since 1990 (equity, bond and FX market stress, correlation-weighted)",
     "country financial stress"),
)
STALE_ECB_MONTHS = 18  # a CLIFS country whose last print is older than this is a discontinued series (Estonia stops 2010)
IMF_INDICATORS = {"GGXWDG_NGDP": ("debt_gdp", "general government gross debt, % of GDP"),
                  "GGXCNL_NGDP": ("fiscal_bal_gdp", "general government net lending/borrowing, % of GDP"),
                  "BCA_NGDPD": ("cab_gdp", "current account balance, % of GDP"),
                  "NGDP_RPCH": ("gdp_growth", "real GDP growth, %"),
                  "PCPIPCH": ("inflation", "inflation, average consumer prices, %")}
OFR_COLUMNS = {"OFR FSI": "ofr_fsi", "Credit": "ofr_credit", "Emerging markets": "ofr_em", "Funding": "ofr_funding", "Volatility": "ofr_vol"}


def parse_fred_csv(text):
    """fredgraph.csv (observation_date,<id>) -> {date: value}; '.' = missing."""
    out = {}
    rdr = csv.reader(io.StringIO(text))
    header = next(rdr, None)
    if not header or len(header) < 2:
        return out
    for row in rdr:
        if len(row) < 2 or not row[0] or row[1] in ("", "."):
            continue
        try:
            out[row[0]] = float(row[1])
        except ValueError:
            continue
    return out


def parse_ofr_csv(text):
    """OFR fsi.csv -> {column_alias: {date: value}}."""
    rdr = csv.DictReader(io.StringIO(text))
    out = {alias: {} for alias in OFR_COLUMNS.values()}
    for row in rdr:
        d = (row.get("Date") or "").strip()
        if not d:
            continue
        for col, alias in OFR_COLUMNS.items():
            v = (row.get(col) or "").strip()
            if v:
                try:
                    out[alias][d] = float(v)
                except ValueError:
                    pass
    return out


def parse_ebp_csv(text):
    """Fed ebp_csv.csv (date,gz_spread,ebp,est_prob) -> {"gz_spread": {date: pct}, "ebp": {date: pct}}."""
    rdr = csv.DictReader(io.StringIO(text))
    out = {"gz_spread": {}, "ebp": {}}
    for row in rdr:
        d = (row.get("date") or "").strip()
        for k in out:
            v = (row.get(k) or "").strip()
            if d and v:
                try:
                    out[k][d] = float(v)
                except ValueError:
                    pass
    return out


def parse_ecb_csv(text):
    """ECB Data Portal csvdata (KEY,FREQ,REF_AREA,...,TIME_PERIOD,OBS_VALUE,...) -> {iso3: {date: value}}.
    Accepts any CISS SOV_* daily key; the reference area decides the bucket (U2 = euro area -> 'EA')."""
    out = {}
    rdr = csv.DictReader(io.StringIO(text))
    for row in rdr:
        area = (row.get("REF_AREA") or "").strip()
        d = (row.get("TIME_PERIOD") or "").strip()
        v = (row.get("OBS_VALUE") or "").strip()
        if not area or not v or len(d) not in (7, 10):
            continue
        if len(d) == 7:  # monthly period -> month-end date so the series shares the daily date axis
            y, m = int(d[:4]), int(d[5:7])
            d = "%04d-%02d-%02d" % (y, m, calendar.monthrange(y, m)[1])
        try:
            out.setdefault(ECB_ISO2_TO_ISO3.get(area, area), {})[d] = float(v)
        except ValueError:
            continue
    return out


def parse_imf_json(text, indicator):
    """IMF DataMapper /api/v1/<indicator> -> {iso3: {year(str): value}}."""
    import json
    try:
        d = json.loads(text)
    except ValueError:
        return {}
    vals = ((d.get("values") or {}).get(indicator)) or {}
    out = {}
    for iso3, years in vals.items():
        if not isinstance(years, dict) or len(iso3) != 3:
            continue
        clean = {}
        for y, v in years.items():
            try:
                clean[str(y)] = float(v)
            except (TypeError, ValueError):
                continue
        if clean:
            out[iso3] = clean
    return out


def fundamentals_for(imf, iso3, as_of_iso):
    """Latest WEO outturn (previous calendar year) and the current-year WEO figure for one sovereign.
    imf: {indicator: {iso3: {year: value}}}.  Returns None when the IMF has nothing for the code."""
    if not imf or not iso3:
        return None
    yr = int(as_of_iso[:4])
    out, hit = {"year": str(yr - 1), "weo_year": str(yr)}, False
    for ind, (alias, _label) in IMF_INDICATORS.items():
        series = (imf.get(ind) or {}).get(iso3) or {}
        v_last = series.get(str(yr - 1))
        v_weo = series.get(str(yr))
        if v_last is None:
            # fall back to the latest year the IMF has at or before last year
            prior = [y for y in series if y.isdigit() and int(y) <= yr - 1]
            if prior:
                y = max(prior)
                v_last, out["year"] = series[y], y
        if v_last is not None or v_weo is not None:
            hit = True
        out[alias] = round(v_last, 1) if v_last is not None else None
        out[alias + "_weo"] = round(v_weo, 1) if v_weo is not None else None
    return out if hit else None


def attach_fundamentals(packet, imf, as_of_iso):
    """Add IMF WEO fundamentals to every sovereign universe entry, row, unpriced and dormant record (by iso3).
    Returns the number of sovereigns that received data; writes packet['fundamentals'] with the source note."""
    groups = (packet.get("groups") or {})
    sov = groups.get("sovereign") or {}
    by_key = {}
    n = 0
    for e in sov.get("universe") or []:
        f = fundamentals_for(imf, e.get("iso3"), as_of_iso)
        if f:
            e["fund"] = f
            n += 1
            if e.get("key"):
                by_key[e["key"]] = f
    for bucket in ("rows", "unpriced", "dormant"):
        for r in sov.get(bucket) or []:
            if r.get("key") in by_key:
                r["fund"] = by_key[r["key"]]
    packet["fundamentals"] = {"source": "IMF World Economic Outlook via DataMapper API (imf.org/external/datamapper/api/v1), keyless",
                              "indicators": {alias: label for _ind, (alias, label) in IMF_INDICATORS.items()},
                              "year": str(int(as_of_iso[:4]) - 1), "weo_year": as_of_iso[:4], "n_sovereigns": n,
                              "read": "annual fiscal and external balances for the issuer behind each CDS; WEO current-year figures are IMF staff estimates, not outturns"}
    return n


def weekly(series, start_iso=LONG_START, scale=1.0, nd=1):
    """Last observation of each ISO week from start_iso -> [[date, value]] (keeps ~52 points/year)."""
    out, last_week = [], None
    for d in sorted(series):
        if d < start_iso:
            continue
        wk = date.fromisoformat(d).isocalendar()[:2]
        pt = [d, round(series[d] * scale, nd)]
        if wk == last_week:
            out[-1] = pt
        else:
            out.append(pt)
            last_week = wk
    return out


def pct_rank(values, v):
    if not values or v is None:
        return None
    return round(100.0 * sum(1 for x in values if x <= v) / len(values), 1)


def crisis_peaks(series, scale=1.0, nd=0):
    peaks = []
    for label, a, b in CRISIS_WINDOWS:
        window = [(v, d) for d, v in series.items() if a <= d <= b]
        if window:
            v, d = max(window)
            peaks.append({"episode": label, "date": d, "value": round(v * scale, nd)})
    return peaks


def fit_map(xs, ys):
    """Least-squares y = a + b*x with r²; None when the overlap is too thin to mean anything."""
    n = len(xs)
    if n < 20:
        return None
    mx, my = statistics.mean(xs), statistics.mean(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    if sxx <= 1e-12:
        return None
    b = sxy / sxx
    a = my - b * mx
    ss_tot = sum((y - my) ** 2 for y in ys)
    ss_res = sum((y - (a + b * x)) ** 2 for x, y in zip(xs, ys))
    r2 = 1 - ss_res / ss_tot if ss_tot > 1e-12 else None
    return {"a": round(a, 3), "b": round(b, 4), "r2": round(r2, 3) if r2 is not None else None, "n": n}


def _daily_series(bank_series):
    return {d: r["s"] for d, r in (bank_series or {}).items() if r.get("s") is not None}


def _months_before(iso, n):
    y, m = int(iso[:4]), int(iso[5:7])
    m -= n
    while m <= 0:
        y -= 1
        m += 12
    return "%04d-%02d-01" % (y, m)


def ecb_stress_for(series_out, iso3):
    """The most sovereign-specific ECB stress record published for a country: SovCISS, else CISS, else CLIFS."""
    for fam, label0, _unit, _source, kind in ECB_FAMILIES:
        s = series_out.get(fam + "_" + iso3)
        if s:
            euro = next((p for p in s.get("peaks") or [] if str(p.get("episode", "")).startswith("Euro")), None)
            gfc = next((p for p in s.get("peaks") or [] if str(p.get("episode", "")).startswith("GFC")), None)
            return {"family": fam, "label": label0, "kind": kind, "frequency": s.get("frequency", "daily"), "series": fam + "_" + iso3,
                    "value": s["last"]["value"], "date": s["last"]["date"], "pct_rank_since_2006": s.get("pct_rank_since_2006"),
                    "gfc_peak": gfc and gfc.get("value"), "euro_peak": euro and euro.get("value"), "n_points": len(s.get("points") or [])}
    return None


def attach_ecb_stress(packet, series_out):
    """Put the ECB record on every sovereign universe entry / row / unpriced / dormant record (`stress`) so the table and
    the map have it without waiting for the history file.  Returns the number of universe entries that got one."""
    sov = (packet.get("groups") or {}).get("sovereign") or {}
    n = 0
    by_key = {}
    for e in sov.get("universe") or []:
        st = ecb_stress_for(series_out, e.get("iso3") or "")
        if st:
            e["stress"] = st
            n += 1
            if e.get("key"):
                by_key[e["key"]] = st
    for coll in ("rows", "unpriced", "dormant"):
        for r in sov.get(coll) or []:
            st = by_key.get(r.get("key")) or ecb_stress_for(series_out, r.get("iso3") or "")
            if st:
                r["stress"] = st
    fams = {}
    for k in series_out:
        fam = k.split("_", 1)[0]
        if fam in ("sovciss", "ciss", "clifs"):
            fams[fam] = fams.get(fam, 0) + 1
    packet["ecb_stress"] = {"source": "ECB Data Portal, datasets CISS (daily SovCISS + new CISS) and CLIFS (monthly), keyless csvdata",
                            "n_sovereigns": n, "series_by_family": fams,
                            "rule": "a sovereign is drawn against its own ECB record when one exists: SovCISS (sovereign-market stress) first, then CISS (systemic financial stress: US, UK, China and eight euro states), then CLIFS (monthly country financial stress: the rest of the EU); non-European sovereigns outside those lists keep the OFR proxy",
                            "read": "0 = calm, 1 = extreme; percentile is of every observation since 2006 of that country's own record. Descriptive context, not a price."}
    return n


def reconcile_with_systemic_stress(packet, systemic):
    """Cross-check today's ECB levels against the sibling engine justhodl-systemic-stress (data/systemic-stress.json), which
    reads the same ECB series with lastNObservations.  Same date -> values must agree to 0.002."""
    if not systemic:
        return None
    iso2 = {v: k for k, v in ECB_ISO2_TO_ISO3.items()}
    checks = []
    sov = (packet.get("groups") or {}).get("sovereign") or {}
    stress_by_iso = {e.get("iso3"): e.get("stress") for e in sov.get("universe") or [] if e.get("stress")}
    for fam, block_key in (("sovciss", "sovereign_stress"), ("ciss", "systemic_stress")):
        other = ((systemic.get(block_key) or {}).get("countries") or {})
        for iso3, st in stress_by_iso.items():
            if st.get("family") != fam:
                continue
            o = other.get(iso2.get(iso3, ""))
            if not o:
                continue
            same_day = o.get("as_of") == st.get("date")
            diff = abs(float(o.get("value")) - float(st["value"])) if o.get("value") is not None else None
            checks.append({"iso3": iso3, "family": fam, "cds_desk": st["value"], "cds_desk_date": st["date"], "systemic_stress": o.get("value"),
                           "systemic_stress_date": o.get("as_of"), "same_day": same_day,
                           "match": bool(same_day and diff is not None and diff <= 0.002)})
    out = {"engine": "justhodl-systemic-stress", "key": "data/systemic-stress.json", "generated_at": systemic.get("generated_at"),
           "n_checked": len(checks), "n_match": sum(1 for c in checks if c["match"]), "n_same_day": sum(1 for c in checks if c["same_day"]),
           "checks": checks, "read": "two independent pulls of the same ECB series must agree on the same day; a same-day mismatch is a parsing bug, a different-day pair is just publication lag"}
    packet.setdefault("cross_reference", {})["systemic_stress"] = out
    return out


def build_long_context(fred, ofr, ebp, bank, as_of_iso, ecb=None, ciss=None, clifs=None):
    """fred: {id: {date: pct}}, ofr: {alias: {date: v}}, ebp: {gz_spread/ebp: {date: pct}}, bank: cds bank.

    Returns the long block for data/cds-desk-history.json: weekly series since 2006 with crisis peaks and today's
    percentile, the CDX IG -> Baa-10Y mapping, and the CDS-bond basis over the ICE overlap."""
    series_out, notes = {}, []
    baa = fred.get("BAA10Y") or {}
    cdx_ig = _daily_series((bank.get("series") or {}).get("IDX:CDX.NA.IG"))
    cdx_hy = _daily_series((bank.get("series") or {}).get("IDX:CDX.NA.HY"))
    # 1. Moody's Baa - 10Y in bp
    if baa:
        long_vals = [v * 100 for d, v in baa.items() if d >= LONG_START and d <= as_of_iso]
        last_d = max(d for d in baa if d <= as_of_iso)
        series_out["baa10y"] = {"name": FRED_SERIES["BAA10Y"], "unit": "bp", "source": "FRED BAA10Y (Moody's, daily since 1986)",
                                "points": weekly(baa, scale=100, nd=0), "last": {"date": last_d, "value": round(baa[last_d] * 100)},
                                "pct_rank_since_2006": pct_rank(long_vals, baa[last_d] * 100), "peaks": crisis_peaks(baa, 100)}
        # map CDX IG onto Baa-10Y over the overlap window
        xs, ys = [], []
        for d, s in cdx_ig.items():
            if d in baa:
                xs.append(s)
                ys.append(baa[d] * 100)
        fit = fit_map(xs, ys)
        if fit and cdx_ig:
            last_cdx_d = max(cdx_ig)
            mapped = fit["a"] + fit["b"] * cdx_ig[last_cdx_d]
            peak08 = next((p for p in series_out["baa10y"]["peaks"] if p["episode"].startswith("GFC")), None)
            series_out["baa10y"]["cdx_ig_map"] = {
                "cdx_ig_bp": cdx_ig[last_cdx_d], "cdx_ig_date": last_cdx_d, "mapped_baa10y_bp": round(mapped),
                "pct_rank_since_2006": pct_rank(long_vals, mapped), "share_of_gfc_peak": round(mapped / peak08["value"], 3) if peak08 else None,
                "fit": fit, "usable": bool(fit["b"] > 0 and (fit["r2"] or 0) >= 0.3), "method": "least squares of Baa-10Y on CDX IG over the days both exist; extrapolation outside the overlap range is a stretch, read the r² first"}
    if fred.get("AAA10Y"):
        aaa = fred["AAA10Y"]
        last_d = max(d for d in aaa if d <= as_of_iso)
        series_out["aaa10y"] = {"name": FRED_SERIES["AAA10Y"], "unit": "bp", "source": "FRED AAA10Y (Moody's, daily since 1983)",
                                "points": weekly(aaa, scale=100, nd=0), "last": {"date": last_d, "value": round(aaa[last_d] * 100)},
                                "pct_rank_since_2006": pct_rank([v * 100 for d, v in aaa.items() if d >= LONG_START and d <= as_of_iso], aaa[last_d] * 100),
                                "peaks": crisis_peaks(aaa, 100)}
    # 2. OFR FSI categories
    for alias, label in (("ofr_credit", "OFR FSI · Credit category"), ("ofr_em", "OFR FSI · Emerging markets"), ("ofr_fsi", "OFR Financial Stress Index"),
                         ("ofr_funding", "OFR FSI · Funding"), ("ofr_vol", "OFR FSI · Volatility")):
        s = (ofr or {}).get(alias) or {}
        if not s:
            continue
        last_d = max(d for d in s if d <= as_of_iso) if any(d <= as_of_iso for d in s) else max(s)
        series_out[alias] = {"name": label, "unit": "index (0 = average stress)", "source": "OFR Financial Stress Index, daily since 2000 (financialresearch.gov)",
                             "points": weekly(s, nd=2), "last": {"date": last_d, "value": round(s[last_d], 2)},
                             "pct_rank_since_2006": pct_rank([v for d, v in s.items() if d >= LONG_START and d <= as_of_iso], s[last_d]),
                             "peaks": crisis_peaks(s, nd=2)}
    # 2b. ECB stress families — SovCISS (sovereign, daily, euro area + 11 states), CISS (systemic, daily, + US / UK / China),
    #     CLIFS (country financial stress, monthly, every EU state + UK).  Each becomes `<family>_<ISO3>`.
    stale_cut = _months_before(as_of_iso, STALE_ECB_MONTHS)
    for (fam, label0, unit, source, _kind), data in zip(ECB_FAMILIES, (ecb, ciss, clifs)):
        for iso3, s in sorted((data or {}).items()):
            if not s or not any(d <= as_of_iso for d in s):
                continue
            last_d = max(d for d in s if d <= as_of_iso)
            if last_d < stale_cut:
                notes.append("%s %s discontinued (last %s), skipped" % (label0, iso3, last_d))
                continue
            pts = weekly(s, nd=3) if fam != "clifs" else [[d, round(v, 3)] for d, v in sorted(s.items()) if d >= LONG_START and d <= as_of_iso]
            series_out[fam + "_" + iso3] = {"name": "%s · %s" % (label0, ECB_NAMES.get(iso3, iso3)), "unit": unit, "source": source,
                                            "family": fam, "frequency": "monthly" if fam == "clifs" else "daily",
                                            "iso3": iso3, "points": pts, "last": {"date": last_d, "value": round(s[last_d], 3)},
                                            "pct_rank_since_2006": pct_rank([v for d, v in s.items() if d >= LONG_START and d <= as_of_iso], s[last_d]),
                                            "peaks": crisis_peaks(s, nd=3)}
    # 3. GZ spread / EBP (monthly)
    for k, label in (("gz_spread", "Gilchrist–Zakrajšek credit spread"), ("ebp", "Excess bond premium")):
        s = (ebp or {}).get(k) or {}
        if not s:
            continue
        last_d = max(s)
        series_out[k] = {"name": label, "unit": "bp" if k == "gz_spread" else "bp (premium over default risk)",
                         "source": "Federal Reserve FEDS Notes ebp_csv.csv, monthly since 1973",
                         "points": [[d, round(v * 100)] for d, v in sorted(s.items()) if d >= LONG_START], "last": {"date": last_d, "value": round(s[last_d] * 100)},
                         "pct_rank_since_2006": pct_rank([v * 100 for d, v in s.items() if d >= LONG_START], s[last_d] * 100), "peaks": crisis_peaks(s, 100)}
    # 4. CDS-bond basis over the ICE overlap (three years max)
    basis = {}
    for key, fred_id, cds in (("hy", "BAMLH0A0HYM2", cdx_hy), ("ig", "BAMLC0A0CM", cdx_ig)):
        oas = fred.get(fred_id) or {}
        pts = [[d, round(cds[d] - oas[d] * 100, 1), round(cds[d], 1), round(oas[d] * 100, 1)] for d in sorted(cds) if d in oas]
        if pts:
            vals = [p[1] for p in pts]
            basis[key] = {"cds": "CDX " + key.upper(), "bond": FRED_SERIES[fred_id], "columns": ["date", "basis_bp", "cds_bp", "oas_bp"], "points": pts[-260:],
                          "last": pts[-1], "mean_bp": round(statistics.mean(vals), 1), "pct_rank": pct_rank(vals, vals[-1]),
                          "read": "CDS minus cash-bond spread; a basis moving sharply negative (CDS cheap to bonds) has historically marked funding stress, a positive basis means protection is bid ahead of bonds"}
    if not series_out:
        notes.append("no long-history source was reachable; block empty")
    return {"start": LONG_START, "as_of": as_of_iso, "series": series_out, "basis": basis,
            "limits": ["DTCC public tape begins 2024-09; FRED ICE BofA OAS series carry only three years since April 2026 — neither shows 2008",
                       "sovereign CDS history before 2024-09 is not available from any free source; ECB SovCISS gives a country-specific sovereign-stress record since 2000 for eleven euro-area states, OFR's emerging-markets category is the only free long EM stress record",
                       "the CDX IG → Baa-10Y mapping is a straight-line fit over the overlap window; it places today's level on the long distribution, it does not reconstruct historical CDX"],
            "notes": notes}


def build_history(bank, as_of_iso, long_block, keys, branch_keys=()):
    """Full daily series for every listed name and index (the main packet only carries 120-day sparklines).
    branch_keys: unpriced names also get both feasible branches per coupon per day ([date, above_coupon, below_coupon])."""
    series = bank.get("series") or {}
    meta = bank.get("meta") or {}
    out = {}
    for key in keys:
        s = series.get(key)
        if not s:
            continue
        pts = [[d, r["s"]] for d, r in sorted(s.items()) if r.get("s") is not None and d <= as_of_iso]
        branches = {}
        if key in branch_keys:
            for d, r in sorted(s.items()):
                for c, pair in (r.get("cd") or {}).items():
                    branches.setdefault(c, []).append([d, pair[0], pair[1]])
        if pts or branches:
            out[key] = {"points": pts, "n": sum(r.get("n") or 0 for r in s.values()), "first": pts[0][0] if pts else None, "last": pts[-1][0] if pts else None,
                        "raw": (meta.get(key) or {}).get("raw")}
            if branches:
                out[key]["branches"] = branches
    return {"engine": "justhodl-cds-desk", "as_of": as_of_iso, "names": out, "long": long_block}
