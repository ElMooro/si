"""
bls-labor-agent — Bureau of Labor Statistics desk.

TWO outputs, one daily run (14:00 UTC):
  1. data/bls-labor.json      — legacy CPI/PPI/ECI/productivity + labor summary
                                 (consumed by us-data-desk.html — UNCHANGED contract)
  2. data/bls-employment.json — comprehensive Employment Crisis Detection dataset
                                 (consumed by bls.html): core CPS, U-1..U-6, demographics,
                                 51 state LAUS rates, JOLTS, 17 CES industries, hours &
                                 earnings — full history + MoM/YoY + per-indicator crisis
                                 thresholds + composite crisis engine (Sahm rule et al).

Uses BLS v2 with a registered key (env BLS_API_KEY): 50 series/query, 20y windows.
If the key is missing/invalid falls back to keyless v1 (25 series, 10y) — shorter
history, still 100% real data. All series IDs verified against BLS/FRED catalogs.
"""
import os
import json
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/bls-labor.json"
EMP_KEY = "data/bls-employment.json"
API_KEY = os.environ.get("BLS_API_KEY", "")
V2 = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
V1 = "https://api.bls.gov/publicAPI/v1/timeseries/data/"

# ------------------------------------------------------------------ legacy maps
LABOR = {
    "unemployment_rate": "LNS14000000",
    "u6_underemployment": "LNS13327709",
    "nonfarm_payrolls_level_k": "CES0000000001",
    "avg_hourly_earnings": "CES0500000003",
    "labor_force_participation": "LNS11300000",
    "emp_population_ratio": "LNS12300000",
    "job_openings_k": "JTS000000000000000JOL",
    "quits_rate": "JTS000000000000000QUR",
    "hires_rate": "JTS000000000000000HIR",
}
INFLATION = {
    "cpi_all_items": "CUUR0000SA0",
    "cpi_core": "CUUR0000SA0L1E",
    "cpi_shelter": "CUUR0000SAH1",
    "cpi_core_services_sa": "CUSR0000SASLE",
    "cpi_supercore": "CUSR0000SASL2RS",
    "ppi_final_demand": "WPUFD4",
    "ppi_core": "WPUFD49104",
    "eci_compensation": "CIU1010000000000A",
}
PRODUCTIVITY = {
    "nonfarm_productivity": "PRS85006092",
    "unit_labor_costs": "PRS85006112",
    "real_hourly_comp": "PRS85006152",
}

# --------------------------------------------------- employment desk catalog
# unit: pct | k (thousands) | hrs | usd | wks ; on: value|mom_chg|yoy_pct
# dir: above = higher is worse ; below = lower is worse ; soft: verify may WARN not FAIL


def C(sid, label, cat, unit="pct", dec=1, warn=None, crisis=None,
      dirn="above", on="value", soft=False):
    return {"sid": sid, "label": label, "cat": cat, "unit": unit, "dec": dec,
            "warn": warn, "crisis": crisis, "dir": dirn, "on": on, "soft": soft}


NATIONAL = {
    # ---- core CPS / CES headline
    "unemployment_rate": C("LNS14000000", "Unemployment Rate (U-3)", "core",
                           warn=4.6, crisis=5.5),
    "nonfarm_payrolls": C("CES0000000001", "Total Nonfarm Payrolls", "core",
                          unit="k", dec=0, warn=60, crisis=0, dirn="below",
                          on="mom_chg"),
    "employment_level": C("LNS12000000", "Employment Level (Household)", "core",
                          unit="k", dec=0),
    "civilian_labor_force": C("LNS11000000", "Civilian Labor Force", "core",
                              unit="k", dec=0),
    "unemployed_level": C("LNS13000000", "Unemployed Persons", "core",
                          unit="k", dec=0, warn=7600, crisis=9000),
    "lfpr": C("LNS11300000", "Labor Force Participation Rate", "core",
              warn=62.0, crisis=61.0, dirn="below"),
    "epop": C("LNS12300000", "Employment-Population Ratio", "core",
              warn=59.5, crisis=58.5, dirn="below"),
    "long_term_unemployed": C("LNS13008636", "Long-Term Unemployed (27+ wks)",
                              "core", unit="k", dec=0, warn=1900, crisis=2600),
    "avg_duration_weeks": C("LNS13008275", "Avg Duration of Unemployment", "core",
                            unit="wks", warn=24, crisis=30, soft=True),
    "median_duration_weeks": C("LNS13008276", "Median Duration of Unemployment",
                               "core", unit="wks", warn=11, crisis=15, soft=True),
    "part_time_econ": C("LNS12032194", "Part-Time for Economic Reasons", "core",
                        unit="k", dec=0, warn=5200, crisis=6500),
    "nilf_want_job": C("LNS15026639", "Not in Labor Force, Want a Job", "core",
                       unit="k", dec=0, warn=6200, crisis=7200, soft=True),
    # ---- alternative measures U-1..U-6 (A-15, SA) — IDs verified vs FRED
    "u1_rate": C("LNS13025670", "U-1 · Unemployed 15+ Weeks", "alt",
                 warn=2.0, crisis=3.0),
    "u2_rate": C("LNS14023621", "U-2 · Job Losers & Temp Completed", "alt",
                 warn=2.5, crisis=3.5),
    "u4_rate": C("LNS13327707", "U-4 · Unemployed + Discouraged", "alt",
                 warn=4.9, crisis=5.9),
    "u5_rate": C("LNS13327708", "U-5 · + All Marginally Attached", "alt",
                 warn=5.6, crisis=6.8),
    "u6_rate": C("LNS13327709", "U-6 · Broadest Underemployment", "alt",
                 warn=8.5, crisis=10.0),
    # ---- demographics (CPS, SA)
    "ur_men_20plus": C("LNS14000025", "Adult Men (20+)", "demo",
                       warn=4.4, crisis=5.4),
    "ur_women_20plus": C("LNS14000026", "Adult Women (20+)", "demo",
                         warn=4.4, crisis=5.4),
    "ur_teen": C("LNS14000012", "Teenagers (16-19)", "demo",
                 warn=14.0, crisis=18.0),
    "ur_white": C("LNS14000003", "White", "demo", warn=4.2, crisis=5.2),
    "ur_black": C("LNS14000006", "Black or African American", "demo",
                  warn=7.0, crisis=9.0),
    "ur_hispanic": C("LNS14000009", "Hispanic or Latino", "demo",
                     warn=5.6, crisis=7.0),
    "ur_asian": C("LNS14032183", "Asian", "demo", warn=4.5, crisis=6.0),
    "ur_bachelors": C("LNS14027662", "Bachelor's Degree+ (25+)", "demo",
                      warn=3.0, crisis=4.0, soft=True),
    "ur_less_hs": C("LNS14027659", "Less than HS Diploma (25+)", "demo",
                    warn=7.0, crisis=9.0, soft=True),
    # ---- JOLTS (total nonfarm, SA)
    "jolts_openings": C("JTS000000000000000JOL", "Job Openings", "jolts",
                        unit="k", dec=0, warn=6800, crisis=5500, dirn="below"),
    "jolts_openings_rate": C("JTS000000000000000JOR", "Job Openings Rate",
                             "jolts", warn=4.0, crisis=3.4, dirn="below"),
    "jolts_hires": C("JTS000000000000000HIL", "Hires", "jolts",
                     unit="k", dec=0),
    "jolts_hires_rate": C("JTS000000000000000HIR", "Hires Rate", "jolts",
                          warn=3.3, crisis=3.0, dirn="below"),
    "jolts_quits": C("JTS000000000000000QUL", "Quits", "jolts",
                     unit="k", dec=0),
    "jolts_quits_rate": C("JTS000000000000000QUR", "Quits Rate", "jolts",
                          warn=1.9, crisis=1.7, dirn="below"),
    "jolts_layoffs": C("JTS000000000000000LDL", "Layoffs & Discharges", "jolts",
                       unit="k", dec=0),
    "jolts_layoffs_rate": C("JTS000000000000000LDR", "Layoffs Rate", "jolts",
                            warn=1.3, crisis=1.6),
    "jolts_seps": C("JTS000000000000000TSL", "Total Separations", "jolts",
                    unit="k", dec=0),
    "jolts_seps_rate": C("JTS000000000000000TSR", "Total Separations Rate",
                         "jolts"),
    # ---- industry payrolls (CES, SA, thousands) — YoY contraction = stress
    "ind_private": C("CES0500000001", "Total Private", "industry", unit="k",
                     dec=0, warn=0.0, crisis=-1.0, dirn="below", on="yoy_pct"),
    "ind_goods": C("CES0600000001", "Goods-Producing", "industry", unit="k",
                   dec=0, warn=0.0, crisis=-2.0, dirn="below", on="yoy_pct"),
    "ind_services": C("CES0800000001", "Private Service-Providing", "industry",
                      unit="k", dec=0, warn=0.0, crisis=-1.0, dirn="below",
                      on="yoy_pct", soft=True),
    "ind_mining": C("CES1000000001", "Mining & Logging", "industry", unit="k",
                    dec=0, warn=-2.0, crisis=-8.0, dirn="below", on="yoy_pct"),
    "ind_construction": C("CES2000000001", "Construction", "industry", unit="k",
                          dec=0, warn=0.0, crisis=-3.0, dirn="below",
                          on="yoy_pct"),
    "ind_manufacturing": C("CES3000000001", "Manufacturing", "industry",
                           unit="k", dec=0, warn=0.0, crisis=-2.0, dirn="below",
                           on="yoy_pct"),
    "ind_ttu": C("CES4000000001", "Trade, Transport & Utilities", "industry",
                 unit="k", dec=0, warn=0.0, crisis=-1.5, dirn="below",
                 on="yoy_pct"),
    "ind_retail": C("CES4200000001", "Retail Trade", "industry", unit="k",
                    dec=0, warn=0.0, crisis=-2.0, dirn="below", on="yoy_pct"),
    "ind_transport": C("CES4300000001", "Transportation & Warehousing",
                       "industry", unit="k", dec=0, warn=0.0, crisis=-2.5,
                       dirn="below", on="yoy_pct", soft=True),
    "ind_information": C("CES5000000001", "Information", "industry", unit="k",
                         dec=0, warn=0.0, crisis=-3.0, dirn="below",
                         on="yoy_pct"),
    "ind_financial": C("CES5500000001", "Financial Activities", "industry",
                       unit="k", dec=0, warn=0.0, crisis=-1.5, dirn="below",
                       on="yoy_pct"),
    "ind_prof_biz": C("CES6000000001", "Professional & Business Svcs",
                      "industry", unit="k", dec=0, warn=0.0, crisis=-1.5,
                      dirn="below", on="yoy_pct"),
    "ind_temp_help": C("CES6056132001", "Temporary Help Services ★leading",
                       "industry", unit="k", dec=0, warn=-1.0, crisis=-6.0,
                       dirn="below", on="yoy_pct"),
    "ind_edu_health": C("CES6500000001", "Education & Health", "industry",
                        unit="k", dec=0, warn=0.5, crisis=-0.5, dirn="below",
                        on="yoy_pct"),
    "ind_leisure": C("CES7000000001", "Leisure & Hospitality", "industry",
                     unit="k", dec=0, warn=0.0, crisis=-3.0, dirn="below",
                     on="yoy_pct"),
    "ind_other": C("CES8000000001", "Other Services", "industry", unit="k",
                   dec=0, warn=0.0, crisis=-2.0, dirn="below", on="yoy_pct",
                   soft=True),
    "ind_government": C("CES9000000001", "Government", "industry", unit="k",
                        dec=0, warn=0.0, crisis=-1.0, dirn="below",
                        on="yoy_pct"),
    # ---- hours & earnings (CES, SA)
    "ahe_private": C("CES0500000003", "Avg Hourly Earnings · Private", "hours",
                     unit="usd", dec=2, warn=3.2, crisis=2.5, dirn="below",
                     on="yoy_pct"),
    "ahe_production": C("CES0500000008", "Avg Hourly Earnings · Production",
                        "hours", unit="usd", dec=2, soft=True),
    "awh_private": C("CES0500000002", "Avg Weekly Hours · Private", "hours",
                     unit="hrs", warn=34.2, crisis=33.9, dirn="below"),
    "awh_manufacturing": C("CES3000000002", "Avg Weekly Hours · Manufacturing",
                           "hours", unit="hrs", warn=40.0, crisis=39.4,
                           dirn="below"),
    "awe_private": C("CES0500000011", "Avg Weekly Earnings · Private", "hours",
                     unit="usd", dec=2, soft=True),
}

STATES = [
    ("01", "AL", "Alabama"), ("02", "AK", "Alaska"), ("04", "AZ", "Arizona"),
    ("05", "AR", "Arkansas"), ("06", "CA", "California"),
    ("08", "CO", "Colorado"), ("09", "CT", "Connecticut"),
    ("10", "DE", "Delaware"), ("11", "DC", "District of Columbia"),
    ("12", "FL", "Florida"), ("13", "GA", "Georgia"), ("15", "HI", "Hawaii"),
    ("16", "ID", "Idaho"), ("17", "IL", "Illinois"), ("18", "IN", "Indiana"),
    ("19", "IA", "Iowa"), ("20", "KS", "Kansas"), ("21", "KY", "Kentucky"),
    ("22", "LA", "Louisiana"), ("23", "ME", "Maine"), ("24", "MD", "Maryland"),
    ("25", "MA", "Massachusetts"), ("26", "MI", "Michigan"),
    ("27", "MN", "Minnesota"), ("28", "MS", "Mississippi"),
    ("29", "MO", "Missouri"), ("30", "MT", "Montana"), ("31", "NE", "Nebraska"),
    ("32", "NV", "Nevada"), ("33", "NH", "New Hampshire"),
    ("34", "NJ", "New Jersey"), ("35", "NM", "New Mexico"),
    ("36", "NY", "New York"), ("37", "NC", "North Carolina"),
    ("38", "ND", "North Dakota"), ("39", "OH", "Ohio"),
    ("40", "OK", "Oklahoma"), ("41", "OR", "Oregon"),
    ("42", "PA", "Pennsylvania"), ("44", "RI", "Rhode Island"),
    ("45", "SC", "South Carolina"), ("46", "SD", "South Dakota"),
    ("47", "TN", "Tennessee"), ("48", "TX", "Texas"), ("49", "UT", "Utah"),
    ("50", "VT", "Vermont"), ("51", "VA", "Virginia"),
    ("53", "WA", "Washington"), ("54", "WV", "West Virginia"),
    ("55", "WI", "Wisconsin"), ("56", "WY", "Wyoming"),
]
STATE_SID = {"LASST%s0000000000003" % f: (a, n) for f, a, n in STATES}


# ------------------------------------------------------------------ HTTP core
def _call(url, payload):
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=50) as r:
        return json.loads(r.read())


def _post_batch(sids, y0, y1, use_key):
    base = {"seriesid": sids, "startyear": str(y0), "endyear": str(y1)}
    if use_key:
        base["registrationkey"] = API_KEY
    for attempt in range(3):
        try:
            r = _call(V2 if use_key else V1, base)
            if r.get("status") == "REQUEST_SUCCEEDED":
                return (r.get("Results") or {}).get("series") or []
        except Exception:
            pass
        time.sleep(1.2 * (attempt + 1))
    return []


def fetch_history(sid_list, start_year, end_year):
    """Return ({sid: {(year,'MM'): value}}, {sid: latest_period_name}, api)."""
    use_key = bool(API_KEY)
    if use_key:  # validate key once on a tiny probe
        probe = _post_batch(["LNS14000000"], end_year - 1, end_year, True)
        if not probe:
            use_key = False
    batch_n, win = (50, 20) if use_key else (25, 10)
    if not use_key:
        start_year = max(start_year, end_year - 9)
    data, pnames = {}, {}
    windows = []
    y = start_year
    while y <= end_year:
        windows.append((y, min(y + win - 1, end_year)))
        y += win
    for i in range(0, len(sid_list), batch_n):
        chunk = sid_list[i:i + batch_n]
        for (y0, y1) in windows:
            for s in _post_batch(chunk, y0, y1, use_key):
                sid = s.get("seriesID")
                if not sid:
                    continue
                pm = data.setdefault(sid, {})
                for d in s.get("data", []):
                    per = d.get("period", "")
                    if not per.startswith("M") or per == "M13":
                        continue
                    try:
                        pm[(d["year"], per[1:])] = float(d["value"])
                    except Exception:
                        continue
                    if sid not in pnames and d.get("latest") == "true":
                        pnames[sid] = "%s %s" % (d.get("periodName", ""),
                                                 d.get("year", ""))
            time.sleep(0.35)
    return data, pnames, ("v2" if use_key else "v1")


# ------------------------------------------------------------- series builder
def _round(v, unit, dec):
    if v is None:
        return None
    if unit == "k":
        return int(round(v))
    return round(v, dec if dec else 1)


def build_series(meta, pm, pname):
    hist = sorted(pm.items())  # [((y,'MM'),v)] ascending
    if not hist:
        return {"sid": meta["sid"], "label": meta["label"], "cat": meta["cat"],
                "unit": meta["unit"], "error": "no data"}
    keys = [k for k, _ in hist]
    vals = [v for _, v in hist]
    latest, prev = vals[-1], (vals[-2] if len(vals) > 1 else None)
    ly, lm = keys[-1]
    mom_pct = mom_chg = yoy_pct = yoy_chg = None
    if prev not in (None, 0):
        mom_pct = round((latest / prev - 1) * 100, 2)
    if prev is not None:
        mom_chg = _round(latest - prev, meta["unit"], meta["dec"])
    py = pm.get((str(int(ly) - 1), lm))
    if py not in (None, 0):
        yoy_pct = round((latest / py - 1) * 100, 2)
    if py is not None:
        yoy_chg = _round(latest - py, meta["unit"], meta["dec"])
    s = {"sid": meta["sid"], "label": meta["label"], "cat": meta["cat"],
         "unit": meta["unit"], "dec": meta["dec"],
         "value": _round(latest, meta["unit"], meta["dec"]),
         "period": "%s-%s" % (ly, lm),
         "period_name": pname or ("%s-%s" % (ly, lm)),
         "mom_pct": mom_pct, "mom_chg": mom_chg,
         "yoy_pct": yoy_pct, "yoy_chg": yoy_chg,
         "warn": meta["warn"], "crisis": meta["crisis"], "dir": meta["dir"],
         "on": meta["on"],
         "history": [["%s-%s" % (y, m), _round(v, meta["unit"], meta["dec"])]
                     for (y, m), v in hist]}
    st, dist = status_for(meta, s)
    s["status"], s["distance"] = st, dist
    return s


def status_for(meta, s):
    basis = s.get("value") if meta["on"] == "value" else s.get(meta["on"])
    w, c = meta["warn"], meta["crisis"]
    if basis is None or w is None or c is None:
        return None, None
    if meta["dir"] == "above":
        return ("ALERT" if basis >= c else "WATCH" if basis >= w else "OK",
                round(c - basis, 2))
    return ("ALERT" if basis <= c else "WATCH" if basis <= w else "OK",
            round(basis - c, 2))


# ------------------------------------------------------------- crisis engine
def _sahm(hist):
    vals = [v for _, v in hist if v is not None]
    if len(vals) < 15:
        return None
    ma3 = [sum(vals[i - 2:i + 1]) / 3.0 for i in range(2, len(vals))]
    prior = ma3[-13:-1]
    if len(prior) < 12:
        return None
    return round(ma3[-1] - min(prior), 2)


def _grade(v, w, c, dirn):
    if v is None or w is None or c is None:
        return None
    if dirn == "below":
        v, w, c = -v, -w, -c
    band = (c - w) or 1e-9
    calm = w - band
    if v >= c:
        return 100
    if v >= w:
        return int(round(55 + 45 * (v - w) / band))
    if v <= calm:
        return 0
    return int(round(55 * (v - calm) / band))


def crisis_engine(S):
    def g(k, f="value"):
        d = S.get(k) or {}
        return d.get(f)

    def last3_avg_chg(k):
        h = (S.get(k) or {}).get("history") or []
        v = [x[1] for x in h if x[1] is not None]
        if len(v) < 4:
            return None
        d = [v[i] - v[i - 1] for i in range(len(v) - 3, len(v))]
        return round(sum(d) / 3.0, 1)

    ur_hist = (S.get("unemployment_rate") or {}).get("history") or []
    sahm = _sahm(ur_hist)
    pay3 = last3_avg_chg("nonfarm_payrolls")
    temp_yoy = g("ind_temp_help", "yoy_pct")
    jol, unem = g("jolts_openings"), g("unemployed_level")
    opu = round(jol / unem, 2) if (jol and unem) else None
    quits = g("jolts_quits_rate")
    u6, u3 = g("u6_rate"), g("unemployment_rate")
    spread = round(u6 - u3, 2) if (u6 is not None and u3 is not None) else None
    u6h = {p: v for p, v in ((S.get("u6_rate") or {}).get("history") or [])}
    u3h = {p: v for p, v in ur_hist}
    sp_series = [u6h[p] - u3h[p] for p in sorted(u3h)
                 if p in u6h and u6h[p] is not None and u3h[p] is not None]
    spread_delta = (round(sp_series[-1] - sum(sp_series[-13:-1]) / 12.0, 2)
                    if len(sp_series) >= 13 else None)
    ltu, tot = g("long_term_unemployed"), g("unemployed_level")
    ltu_share = round(ltu / tot * 100, 1) if (ltu and tot) else None
    layoffs = g("jolts_layoffs_rate")

    comps = [
        ("sahm_rule", "Sahm Rule (3mo UR vs 12mo low)", sahm, 0.30, 0.50,
         "above", 0.22, "pp"),
        ("payroll_3mo", "Payroll 3-Mo Avg Change", pay3, 60, 0, "below",
         0.18, "k/mo"),
        ("temp_help_yoy", "Temp-Help Employment YoY", temp_yoy, 0.0, -6.0,
         "below", 0.10, "%"),
        ("openings_per_unemployed", "Openings per Unemployed", opu, 1.05, 0.90,
         "below", 0.12, "x"),
        ("quits_rate", "Quits Rate", quits, 1.9, 1.7, "below", 0.10, "%"),
        ("u6_u3_spread_delta", "U6-U3 Spread vs 12mo Avg", spread_delta, 0.30,
         0.70, "above", 0.08, "pp"),
        ("ltu_share", "Long-Term Share of Unemployed", ltu_share, 25.0, 32.0,
         "above", 0.10, "%"),
        ("layoffs_rate", "Layoffs & Discharges Rate", layoffs, 1.3, 1.6,
         "above", 0.10, "%"),
    ]
    rows, wsum, acc = [], 0.0, 0.0
    for key, label, val, w, c, dirn, wt, unit in comps:
        gr = _grade(val, w, c, dirn)
        st = None
        if gr is not None:
            st = "ALERT" if gr >= 100 else "WATCH" if gr >= 55 else "OK"
            wsum += wt
            acc += gr * wt
        rows.append({"key": key, "label": label, "value": val, "unit": unit,
                     "warn": w, "crisis": c, "dir": dirn, "weight": wt,
                     "grade": gr, "status": st})
    score = int(round(acc / wsum)) if wsum else None
    level = (None if score is None else
             "STABLE" if score < 20 else "WATCH" if score < 40 else
             "WARNING" if score < 65 else "CRISIS")
    return {"score": score, "level": level, "sahm": sahm,
            "u6_u3_spread": spread, "components": rows,
            "methodology": "Weighted composite of 8 recession-onset signals; "
                           "Sahm rule per Claudia Sahm (2019). 0-19 STABLE, "
                           "20-39 WATCH, 40-64 WARNING, 65+ CRISIS."}


# ---------------------------------------------------------------- legacy path
def fetch_bls(series_map):
    now = datetime.now(timezone.utc)
    base = {"seriesid": list(series_map.values()),
            "startyear": str(now.year - 2), "endyear": str(now.year)}
    resp, api = None, None
    if API_KEY:
        try:
            p = dict(base)
            p["registrationkey"] = API_KEY
            p["calculations"] = True
            r = _call(V2, p)
            if r.get("status") == "REQUEST_SUCCEEDED" and \
                    (r.get("Results") or {}).get("series"):
                resp, api = r, "v2"
        except Exception:
            pass
    if resp is None:
        r = _call(V1, base)
        resp, api = r, "v1"
    by_id = {s.get("seriesID"): s.get("data", [])
             for s in (resp.get("Results", {}) or {}).get("series", [])}
    inv = {v: k for k, v in series_map.items()}
    out = {}
    for sid, rows in by_id.items():
        name = inv.get(sid, sid)
        if not rows:
            out[name] = {"series_id": sid, "error": "no data"}
            continue
        pm = {}
        for d in rows:
            try:
                pm[(d["year"], d["period"])] = float(d["value"])
            except Exception:
                pass
        latest = rows[0]
        try:
            val = float(latest["value"])
        except Exception:
            val = None
        ly, lp = latest["year"], latest["period"]
        mom = yoy = None
        if val is not None and len(rows) > 1:
            try:
                pv = float(rows[1]["value"])
                mom = round((val / pv - 1) * 100, 2) if pv else None
            except Exception:
                pass
        pk = (str(int(ly) - 1), lp)
        if val is not None and pm.get(pk):
            yoy = round((val / pm[pk] - 1) * 100, 2)
        calc = (latest.get("calculations") or {}).get("pct_changes") or {}
        try:
            if calc.get("1") is not None:
                mom = float(calc["1"])
            if calc.get("12") is not None:
                yoy = float(calc["12"])
        except Exception:
            pass
        out[name] = {"series_id": sid, "value": val,
                     "period": "%s-%s" % (ly, lp[-2:]),
                     "period_name": latest.get("periodName"),
                     "mom_pct": mom, "yoy_pct": yoy}
    return out, api


def publish_legacy(now):
    labor = inflation = {}
    api = None
    errs = []
    try:
        labor, api = fetch_bls(LABOR)
    except Exception as e:
        errs.append("labor:" + str(e)[:100])
        labor = {}
    try:
        inflation, api2 = fetch_bls(INFLATION)
        api = api or api2
    except Exception as e:
        errs.append("inflation:" + str(e)[:100])
        inflation = {}
    productivity = {}
    try:
        productivity, api3 = fetch_bls(PRODUCTIVITY)
        api = api or api3
    except Exception as e:
        errs.append("productivity:" + str(e)[:100])
        productivity = {}

    def g(d, k, f="value"):
        return (d.get(k) or {}).get(f)

    ur = g(labor, "unemployment_rate")
    core_yoy = g(inflation, "cpi_core", "yoy_pct")
    summary = {
        "unemployment_rate": ur,
        "cpi_yoy_pct": g(inflation, "cpi_all_items", "yoy_pct"),
        "core_cpi_yoy_pct": core_yoy,
        "shelter_yoy_pct": g(inflation, "cpi_shelter", "yoy_pct"),
        "core_services_yoy_pct": g(inflation, "cpi_core_services_sa",
                                   "yoy_pct"),
        "supercore_yoy_pct": g(inflation, "cpi_supercore", "yoy_pct"),
        "wage_growth_yoy_pct": g(labor, "avg_hourly_earnings", "yoy_pct"),
        "job_openings_k": g(labor, "job_openings_k"),
        "quits_rate": g(labor, "quits_rate"),
        "unit_labor_costs_qoq_pct": g(productivity, "unit_labor_costs"),
        "productivity_qoq_pct": g(productivity, "nonfarm_productivity"),
        "real_hourly_comp_qoq_pct": g(productivity, "real_hourly_comp"),
        "labor_read": ("TIGHT" if (ur is not None and ur < 4.2)
                       else "LOOSENING" if ur is not None else None),
        "inflation_read": ("ABOVE TARGET" if (core_yoy is not None and
                                              core_yoy > 2.5)
                           else "NEAR TARGET" if core_yoy is not None
                           else None),
    }
    n_live = sum(1 for d in (labor, inflation, productivity)
                 for v in d.values()
                 if isinstance(v, dict) and v.get("value") is not None)
    out = {"generated_at": now.isoformat(),
           "source": "U.S. Bureau of Labor Statistics API (api.bls.gov) — "
                     "direct, free",
           "api_version": api, "key_valid": (api == "v2"),
           "summary": summary, "labor_market": labor, "inflation": inflation,
           "productivity": productivity,
           "_series_live": n_live, "_error": "; ".join(errs) or None}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=3600")
    return n_live


# ------------------------------------------------------------ employment path
def publish_employment(now):
    nat_sids = [m["sid"] for m in NATIONAL.values()]
    nat_pm, nat_names, api = fetch_history(nat_sids, 2000, now.year)
    st_pm, st_names, _ = fetch_history(list(STATE_SID.keys()),
                                       2015 if api == "v2" else now.year - 9,
                                       now.year)
    series = {}
    for key, meta in NATIONAL.items():
        series[key] = build_series(meta, nat_pm.get(meta["sid"], {}),
                                   nat_names.get(meta["sid"]))
    nat_ur = (series.get("unemployment_rate") or {}).get("value")
    states = {}
    st_meta = {"unit": "pct", "dec": 1, "warn": 5.2, "crisis": 6.5,
               "dir": "above", "on": "value", "cat": "state"}
    for sid, (abbr, name) in STATE_SID.items():
        m = dict(st_meta)
        m.update({"sid": sid, "label": name})
        s = build_series(m, st_pm.get(sid, {}), st_names.get(sid))
        s["abbr"] = abbr
        if s.get("value") is not None and nat_ur is not None:
            s["vs_national"] = round(s["value"] - nat_ur, 1)
        states[abbr] = s

    engine = crisis_engine(series)
    live_nat = sum(1 for s in series.values() if s.get("value") is not None)
    live_st = sum(1 for s in states.values() if s.get("value") is not None)
    out = {"generated_at": now.isoformat(),
           "source": "U.S. Bureau of Labor Statistics API (api.bls.gov) — "
                     "CPS, CES, JOLTS, LAUS. 100% real federal data.",
           "api_version": api, "key_valid": (api == "v2"),
           "refresh": "Auto-updates daily 14:00 UTC via AWS Lambda "
                      "bls-labor-agent",
           "history_start": "2000-01" if api == "v2" else None,
           "crisis": engine,
           "national": series, "states": states,
           "_series_live": {"national": live_nat, "states": live_st}}
    S3.put_object(Bucket=BUCKET, Key=EMP_KEY,
                  Body=json.dumps(out, separators=(",", ":"),
                                  default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=1800")
    return live_nat, live_st, api


# -------------------------------------------------------------------- handler
def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    legacy_live = 0
    err = None
    try:
        legacy_live = publish_legacy(now)
    except Exception as e:
        err = "legacy:" + str(e)[:140]
    emp_nat = emp_st = 0
    emp_api = None
    try:
        emp_nat, emp_st, emp_api = publish_employment(now)
    except Exception as e:
        err = (err + " | " if err else "") + "employment:" + str(e)[:140]
    return {"ok": err is None, "legacy_series": legacy_live,
            "employment_national": emp_nat, "employment_states": emp_st,
            "api": emp_api, "error": err}
