"""justhodl-eba-risk-dashboard — European Banking Authority Risk Dashboard (Data Annex workbook).

The EBA publishes a quarterly Risk Dashboard built from supervisory reporting of
~160 EU/EEA banks. Its "Data Annex InteractiveRiskDashboard" workbook (free, no
key) is tidy long-format data:

  KRIs by country and EU   20 key risk indicators × 32 countries, quarterly since 2014Q4
                           (NPL ratio, CET1, leverage, LCR, NSFR, RoE, cost of risk, ...)
  Sovereigns               bank sovereign exposures: share to home country, to other
                           EU/EEA countries, accounting portfolio and maturity buckets,
                           plus total gross carrying amount, by country since 2018Q2.

The workbook URL changes every quarter, so we harvest the current link from the
dashboard page, mirror the file verbatim, and parse: EU-wide series for every KRI,
per-country series for the headline KRIs, the sovereign home-bias series per
banking system, and a latest-quarter country grid. Descriptive only.
"""
from __future__ import annotations

import re
import urllib.parse

import risk_sources as RS

SLUG = "eba-risk-dashboard"
PAGE = "https://www.eba.europa.eu/risk-and-data-analysis/risk-analysis/risk-monitoring/risk-dashboard"
HEADLINE = {"AQT_3.2": ("NPL", "NPL ratio"), "SVC_3": ("CET1", "CET1 capital ratio"), "SVC_13": ("LEV", "Leverage ratio"),
            "LIQ_17": ("LCR", "Liquidity coverage ratio"), "PFT_21": ("ROE", "Return on equity"),
            "PFT_43": ("COR", "Cost of risk"), "AQT_41.2": ("COV", "NPL coverage ratio"),
            "FND_32": ("LTD", "Loans-to-deposits (HH + NFC)")}
SHORT = {"AQT_3.1": "NPE ratio", "AQT_3.2": "NPL ratio", "AQT_41.2": "NPL coverage ratio", "AQT_42.2": "Forbearance ratio",
         "FND_32": "Loans-to-deposits (HH+NFC)", "FND_33": "Asset encumbrance ratio", "LIQ_17": "Liquidity coverage ratio",
         "LIQ_20": "Net stable funding ratio", "PFT_21": "Return on equity", "PFT_23": "Cost-to-income ratio", "PFT_24": "Return on assets",
         "PFT_25": "NII / net operating income", "PFT_26": "Fee income / net operating income", "PFT_29": "Trading income / net operating income",
         "PFT_41": "Net interest margin", "PFT_43": "Cost of risk", "SVC_1": "Tier 1 capital ratio", "SVC_13": "Leverage ratio",
         "SVC_2": "Total capital ratio", "SVC_3": "CET1 capital ratio"}


def find_workbook():
    html = RS.http_text(PAGE, timeout=60, headers={"User-Agent": "Mozilla/5.0 (compatible; JustHodl research)"})
    links = re.findall(r'href="([^"]*Data%20Annex%20InteractiveRiskDashboard[^"]*\.xlsx)"', html)
    links += re.findall(r'href="([^"]*Data Annex InteractiveRiskDashboard[^"]*\.xlsx)"', html)
    if not links:
        raise RuntimeError("EBA page: no Data Annex link found")

    def key(u):
        m = re.search(r"Q(\d)%20(\d{4})|Q(\d) (\d{4})", urllib.parse.unquote(u).replace("%20", " "))
        return (int(m.group(2) or m.group(4)), int(m.group(1) or m.group(3))) if m else (0, 0)
    best = max(links, key=key)
    return urllib.parse.urljoin("https://www.eba.europa.eu/", best), key(best)


def pdate(p):
    s = str(int(float(p))) if isinstance(p, float) else str(p)
    return RS.period_to_date(s[:6])


def build(event):
    pk = RS.Packet(
        SLUG, "EBA Risk Dashboard", "EBA Risk Dashboard — EU banking key risk indicators and sovereign exposures",
        "The European Banking Authority's quarterly Risk Dashboard data: twenty key risk indicators (NPL ratio, "
        "CET1, leverage, liquidity coverage, NSFR, profitability, cost of risk) for the EU and each member state "
        "since 2014, and bank sovereign exposures by country — share held in the home sovereign, in other EU/EEA "
        "sovereigns, by accounting portfolio and maturity — since 2018. Read from the EBA's own Data Annex workbook.",
        {"provider": "EBA — European Banking Authority", "url": PAGE,
         "docs": "https://www.eba.europa.eu/risk-and-data-analysis/risk-analysis/risk-monitoring/risk-dashboard",
         "license": "EBA — free reuse with attribution", "cadence": "quarterly"},
        cadence="quarterly (checked daily)")
    url, (yy, qq) = find_workbook()
    blob = RS.http_get(url, timeout=120, headers={"User-Agent": "Mozilla/5.0 (compatible; JustHodl research)"})
    pk.raw(f"Data_Annex_InteractiveRiskDashboard_Q{qq}_{yy}.xlsx", blob,
           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", url=url)
    x = RS.Xlsx(blob)
    # ── KRIs
    K = x.rows("KRIs by country and EU")
    series = {}
    names = {}
    for r in K[1:]:
        if len(r) < 5 or r[0] is None or r[1] is None:
            continue
        d = pdate(r[0])
        cc, code, name, v = str(r[1]), str(r[2]), str(r[3]), RS.parse_num(r[4])
        if not d or v is None:
            continue
        names[code] = SHORT.get(code, re.sub(r"\s+", " ", name)[:60])
        series.setdefault((code, cc), []).append([d, round(100.0 * v, 3)])
    countries = sorted({cc for _, cc in series})
    codes = sorted({c for c, _ in series})
    for code in codes:
        if (code, "EU") in series:
            pk.add_series(f"KRI_{code.replace('.', '_')}_EU", f"EU — {names[code]}", series[(code, "EU")], unit="%", freq="quarterly", group="eu")
    for code, (short, label) in HEADLINE.items():
        for cc in countries:
            if cc != "EU" and (code, cc) in series:
                pk.add_series(f"KRI_{code.replace('.', '_')}_{cc}", f"{cc} — {label}", series[(code, cc)], unit="%", freq="quarterly", group="country", iso2=cc)
    latest_period = max(d for pts in series.values() for d, _ in pts)
    grid_cols = ["Country"] + [names[c] for c in HEADLINE] + ["NSFR", "Cost / income"]
    grid = []
    for cc in countries:
        def last(code):
            pts = series.get((code, cc))
            return pts[-1][1] if pts and pts[-1][0] == latest_period else (pts[-1][1] if pts else None)
        grid.append([cc] + [last(c) for c in HEADLINE] + [last("LIQ_20"), last("PFT_23")])
    pk.add_table("kri_grid", grid_cols, grid, title=f"Key risk indicators by country — {latest_period}",
                 note="Weighted averages of the EBA sample banks headquartered in each country, in percent. EU = whole sample.")
    # ── Sovereigns
    S = x.rows("Sovereigns")
    sov = {}
    gca = {}
    for r in S[1:]:
        if len(r) < 5 or r[0] is None:
            continue
        d = pdate(r[0])
        lbl, custom, cc, v = str(r[1]), str(r[2]), str(r[3]), RS.parse_num(r[4])
        if not d or v is None:
            continue
        sov.setdefault((lbl, cc), []).append([d, round(100.0 * v, 2)])
        if len(r) > 5 and RS.parse_num(r[5]) is not None:
            gca[(cc, d)] = RS.parse_num(r[5])
    sov_labels = {"T15_2": "sovereign exposure to home country (% of total)", "T15_3": "sovereign exposure to other EU/EEA (% of total)",
                  "T15_8": "sovereign exposure at amortised cost (% of total)", "T16_5": "sovereign exposure maturing 10Y+ (% of total)",
                  "T16_1": "sovereign exposure maturing 0–3M (% of total)"}
    for lbl, text in sov_labels.items():
        for cc in sorted({c for (l, c) in sov if l == lbl}):
            pk.add_series(f"SOV_{lbl}_{cc}", f"{cc} banks — {text}", sov[(lbl, cc)], unit="%", freq="quarterly", group="sovereign", iso2=cc)
    for cc in sorted({c for (c, _) in gca}):
        pts = sorted([[d, round(v / 1e9, 2)] for (c, d), v in gca.items() if c == cc])
        pk.add_series(f"SOV_GCA_{cc}", f"{cc} banks — total sovereign exposure, gross carrying amount", pts, unit="€bn", freq="quarterly", group="sovereign", iso2=cc)
    sov_latest = max(d for pts in sov.values() for d, _ in pts)
    rows = []
    for cc in sorted({c for (_, c) in sov}):
        def lastv(lbl):
            pts = sov.get((lbl, cc))
            return pts[-1][1] if pts else None
        rows.append([cc, lastv("T15_2"), lastv("T15_3"), lastv("T15_8"), lastv("T16_1"), lastv("T16_5"),
                     round(gca.get((cc, sov_latest), 0) / 1e9, 1) if gca.get((cc, sov_latest)) else None])
    pk.add_table("sovereign_grid", ["Banks of", "Home sovereign %", "Other EU/EEA %", "Amortised cost %", "0–3M %", "10Y+ %", "Total €bn"],
                 rows, title=f"Bank sovereign exposures — {sov_latest}",
                 note="Share of each banking system's sovereign portfolio held in its own government (home bias), in other EU/EEA sovereigns, held at amortised cost (not marked to market) and by residual maturity. The bank–sovereign nexus, from the banks' side.")
    eu_npl = pk.series.get("KRI_AQT_3_2_EU"); eu_cet1 = pk.series.get("KRI_SVC_3_EU"); eu_home = pk.series.get("SOV_T15_2_EU")
    if eu_npl:
        pk.kpi("EU NPL ratio", f"{eu_npl['latest']:.2f}%", f"{eu_npl['date']} · {RS.ordinal(eu_npl['pct_rank'])} pct since 2014", "info")
    if eu_cet1:
        pk.kpi("EU CET1 ratio", f"{eu_cet1['latest']:.1f}%", f"Δ {eu_cet1['chg']:+.2f}pp q/q" if eu_cet1["chg"] is not None else "", "pos")
    if eu_home:
        pk.kpi("Home-sovereign share", f"{eu_home['latest']:.1f}%", "EU banks' sovereign books held in own government", "warning" if eu_home["latest"] > 50 else "info")
    worst = max((r for r in grid if r[1] is not None and r[0] != "EU"), key=lambda r: r[1], default=None)
    if worst:
        pk.kpi("Highest NPL ratio", f"{worst[0]} {worst[1]:.1f}%", latest_period, "neg")
    pk.kpi("Workbook", f"Q{qq} {yy}", f"{len(countries)} countries · {len(codes)} KRIs", "mute")
    pk.note("The EBA sample covers ~160 of the largest EU/EEA banks (about 80% of EU banking assets). Country values are for banks headquartered there, "
            "on a consolidated basis, so cross-border groups appear under the parent's country.")
    pk.extra["latest_period"] = latest_period
    return pk


def lambda_handler(event, context=None):
    return RS.run(SLUG, build, event)
