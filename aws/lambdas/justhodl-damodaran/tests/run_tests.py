"""justhodl-damodaran offline tests: the readers on workbooks made here in his layouts (never his data), the plan of a
run, and whole runs against a made-up site and warehouse. No network, no AWS.

layouts.xls is a made-up workbook saved in the old Excel format, with his three layouts: an industry table under a
preamble, a table by year with a summary below it, and returns by year under grouped headings.

Run: python3 aws/lambdas/justhodl-damodaran/tests/run_tests.py
"""
from datetime import datetime, timezone
import io
import json
import os
import sys
import urllib.error
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
import dam_core as C            # noqa: E402
import lambda_function as L     # noqa: E402

XLS = open(os.path.join(HERE, "layouts.xls"), "rb").read()
NOW = datetime(2026, 11, 28, 23, 17, tzinfo=timezone.utc)


# ------------------------------------------------------------------------------------------ a workbook made by hand
def xlsx(sheets, date1904=False):
    """An .xlsx of {name: rows}; text goes to the shared strings as Excel writes it."""
    shared, order = {}, []

    def cell(col, r, v):
        ref = "%s%d" % ("".join(chr(65 + int(d)) for d in ([col] if col < 26 else [col // 26 - 1, col % 26])), r)
        if v is None:
            return ""
        if isinstance(v, bool):
            return '<c r="%s" t="b"><v>%d</v></c>' % (ref, 1 if v else 0)
        if isinstance(v, (int, float)):
            return '<c r="%s"><v>%r</v></c>' % (ref, v)
        if v not in shared:
            shared[v] = len(order)
            order.append(v)
        return '<c r="%s" t="s"><v>%d</v></c>' % (ref, shared[v])
    ns = 'xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
    parts, names = {}, list(sheets)
    for i, name in enumerate(names, 1):
        rows = "".join('<row r="%d">%s</row>' % (r, "".join(cell(c, r, v) for c, v in enumerate(row))) for r, row in enumerate(sheets[name], 1) if row)
        parts["xl/worksheets/sheet%d.xml" % i] = '<worksheet %s><sheetData>%s</sheetData></worksheet>' % (ns, rows)
    esc = lambda t: t.replace("&", "&amp;").replace("<", "&lt;")
    parts["xl/sharedStrings.xml"] = '<sst %s>%s</sst>' % (ns, "".join("<si><t>%s</t></si>" % esc(t) for t in order))
    parts["xl/workbook.xml"] = ('<workbook %s xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><workbookPr%s/><sheets>%s</sheets></workbook>' % (
        ns, ' date1904="1"' if date1904 else "", "".join('<sheet name="%s" sheetId="%d" r:id="rId%d"/>' % (esc(n), i, i) for i, n in enumerate(names, 1))))
    parts["xl/_rels/workbook.xml.rels"] = ('<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">%s</Relationships>' % "".join(
        '<Relationship Id="rId%d" Type="t" Target="worksheets/sheet%d.xml"/>' % (i, i) for i in range(1, len(names) + 1)))
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for k, v in parts.items():
            z.writestr(k, v)
    return out.getvalue()


MONTH_HEAD = ["Start of month", "S&P 500", "T.Bond Rate", "$ Riskfree Rate", "CF (Trailing 12 month)", "ERP (T12 m with sustainable payout)", "ERP (T12m)",
              "ERP (T12m) with adj riskfree rate", "ERP (Smoothed)", "ERP (Normalized)", "ERP (Net Cash Yield)", "Expected Return", "Notes"]


def month_file(last=(0.0441, 0.0379, 0.0401, 0.0581, 0.0339, 0.0343), extra=()):
    # days counted from 1904, as his file is: 38230 is 2008-09-01, 44500 is 2025-11-01, 44834 and 44865 are 2026-10-01 and 2026-11-01
    rows = [MONTH_HEAD, [38230, 1252, 0.0372, None, None, None, 0.0422], [38260, 1166, 0.0383, None, None, None, 0.0451],
            [44500, 6840.2, 0.0411, 0.0389, 190.0, 0.0373, 0.0395, 0.0417, 0.055, 0.03, 0.031, 0.0806],
            [44834, 7652, 0.0529, 0.0507, 198.17, 0.042, 0.037, 0.0392, 0.0567, 0.033, 0.034, 0.0899],
            [44865, 7580, 0.0494, 0.0472, 198.17] + list(last) + [0.0873]] + [list(x) for x in extra]
    return xlsx({"Historical ERP": rows, "Last 12 months data": [["Date", "S&P 500"], [44865, 7580]]}, date1904=True)


def country_file(update=44742, mature=0.042, us=0.0442, rows=None, n=60):
    # 44742 counted from 1904 is 2026-07-01 (44561 is 2026-01-01)
    head = [["Country and Equity Risk Premiums"], ["Date of update:", update, "(made up)"],
            ["Enter the current risk premium for a mature equity market", None, None, None, mature, "Implied ERP for S&P 500"],
            ["Enter the current risk premium for the US =", None, None, None, us, "US equity risk premium"],
            ["Do you want to adjust the country default spread?", None, None, None, "Yes"],
            ["If yes, enter the multiplier to use on the default spread", None, None, None, 1.5545, 'See "Relative Equity Volatility" worksheet'], [],
            ["Country", "Africa", "Moody's rating", "Rating-based Default Spread", "Total Equity Risk Premium", "Country Risk Premium",
             "Sovereign CDS, net of US", "Total Equity Risk Premium2", "Country Risk Premium3", "Rating", "Default spread in basis points"]]
    body = rows or ([["Germany", "Western Europe", "Aaa", 0.0, mature, 0.0, 0.0006, 0.0429, 0.0009, "A1", 59.9],
                     ["Turkey", "Western Europe", "Ba3", 0.0286, 0.0865, 0.0445, 0.027, 0.084, 0.042, "A2", 71.9],
                     ["Albania", "Eastern Europe & Russia", "Ba3", 0.0286, 0.0865, 0.0445, "NA", "NA", "NA", "A3", 101.9],
                     ["United States", "North America", "Aa1", 0.0022, us, 0.0022, 0.0043, 0.0487, 0.0067]] +
                    [["Country %02d" % i, "Asia", "Baa3", 0.0175, 0.0692, 0.0272, "NA", "NA", "NA"] for i in range(n)])
    tail = [["Frontier Markets (no sovereign ratings)"], ["Country", "PRS Score", None, "Country Risk Premium", "Total Equity Risk Premium"],
            ["Brunei", 86.5, None, 0.0048, 0.0468], ["Guyana", 70.0, None, 0.031, 0.073], [], [None, "Caa2", 765.1]]
    return xlsx({"Explanation and FAQ": [["Country Risk Premiums"]], "ERPs by country": head + body + tail}, date1904=True)


# ----------------------------------------------------------------------------------------------------- the readers
def test_an_xlsx_is_read_with_the_standard_library_alone():
    raw = xlsx({"First & second": [["a text", 1.5, None, True], [], [None, None, "far"], ["x" * 3, 2]], "Empty": []}, date1904=True)
    bk = C.book(raw)
    assert bk["date1904"] is True and list(bk["sheets"]) == ["First & second", "Empty"]
    assert bk["sheets"]["First & second"] == [["a text", 1.5, None, True], [], [None, None, "far"], ["xxx", 2.0]] and bk["sheets"]["Empty"] == []
    assert C.book(xlsx({"S": [[1]]}))["date1904"] is False


def test_a_day_count_is_read_in_the_workbooks_own_calendar():
    assert C.excel_date(38230, True) == "2008-09-01" and C.excel_date(44865, True) == "2026-11-01", "his monthly file counts from 1904"
    assert C.excel_date(46327, False) == "2026-11-01" and C.excel_date(39692, False) == "2008-09-01", "the same days counted from 1900"
    assert C.excel_date(38230, False) == "2004-08-31", "read in the wrong calendar the same number is four years and a day early: the workbook's flag decides"
    assert C.excel_date("2026-01-05") == "2026-01-05" and C.excel_date("09/30/2026") == "2026-09-30"
    for bad in (None, "", "Year", True, 0, -3, 1e9, "thru Nov 2025"):
        assert C.excel_date(bad) is None, bad


def test_numbers_and_what_is_not_a_number():
    assert C.num("NA") is None and C.num("") is None and C.num(None) is None and C.num(True) is None and C.num("#N/A") is None and C.num(float("nan")) is None
    assert C.num("4.41%") == 0.0441 and C.num("1,234.5") == 1234.5 and C.num(3) == 3.0 and C.num("text") is None
    assert C.tidy(0.04410000001) == 0.0441 and C.tidy(5994.0) == 5994 and C.tidy(None) is None and C.tidy(0.5) == 0.5
    assert C.unique_keys(["EV/EBITDA", "EV/EBIT", "EV/EBITDA", "S&P 500", "% of firms", ""]) == ["ev_ebitda", "ev_ebit", "ev_ebitda_2", "s_and_p_500", "pct_of_firms", "col"]


def test_the_monthly_premium_and_the_five_measures_his_page_states():
    m = C.erp_monthly(C.book(month_file()))
    assert (m["first"], m["last"], len(m["rows"])) == ("2008-09-01", "2026-11-01", 5)
    keys = [c["key"] for c in m["cols"]]
    assert keys[:4] == ["date", "s_and_p_500", "t_bond_rate", "usd_riskfree_rate"] and m["cols"][5]["label"] == "ERP (T12 m with sustainable payout)"
    got = {k: keys[j] for k, j in m["measures"].items()}
    assert got == {"sustainable_payout": "erp_t12_m_with_sustainable_payout", "cash_yield": "erp_t12m", "average_cf": "erp_smoothed",
                   "net_cash_yield": "erp_net_cash_yield", "normalized": "erp_normalized"}, got
    last = m["rows"][-1]
    assert [last[m["measures"][k]] for k, _l, _p in C.MEASURES] == [0.0441, 0.0379, 0.0581, 0.0343, 0.0339], "in the order his page gives them"
    assert m["rows"][0][keys.index("erp_t12m")] == 0.0422 and m["rows"][0][keys.index("erp_t12_m_with_sustainable_payout")] is None, "the early months carry one measure"
    assert "erp_t12m_with_adj_riskfree_rate" in keys and keys.index("erp_t12m_with_adj_riskfree_rate") not in m["measures"].values(), "a sixth premium column is kept and is none of the five"
    assert m["gaps"][0] == "2008-11" and "2026-10" not in m["gaps"] and len(m["gaps"]) == 214, "the months the file skips are named"
    unsorted = C.erp_monthly(C.book(month_file(extra=[[38291, 969, 0.0395, None, None, None, 0.059], ["a note", None], [None, 5]])))
    assert [r[0] for r in unsorted["rows"]][:3] == ["2008-09-01", "2008-10-01", "2008-11-01"] and len(unsorted["rows"]) == 6, "rows are put in order; a row with no date is not a month"
    try:
        C.erp_monthly(C.book(xlsx({"Other": [["Date", "ERP"], [1, 2]]})))
        raise AssertionError("a workbook without the monthly sheet must not be read as one")
    except ValueError:
        pass


def test_a_table_by_year_from_the_old_excel_format():
    bk = C.book(XLS)
    a = C.erp_annual(bk)
    assert a["updated"] == "2026-01-08" and [r[0] for r in a["rows"]] == [1960, 1961, 1962, 2024, 2025], "the summary rows under the table are not years"
    assert [c["key"] for c in a["cols"]] == ["year", "earnings_yield", "dividend_yield", "s_and_p_500", "t_bond_rate", "implied_erp_fcfe", "implied_premium_fcfe_with_sustainable_payout"]
    assert a["rows"][0][5] is None and a["rows"][-1][5:] == [0.0433, 0.0418]
    r = C.returns_by_year(bk)
    assert r["updated"] == "2026-01-01" and len(r["rows"]) == 22 and r["rows"][0][:5] == [1928, 0.2, 0.03, 0.05, 0.01]
    labels = [c["label"] for c in r["cols"]]
    assert labels[1] == "Annual Returns on Investments in: S&P 500 (includes dividends)" and labels[5].startswith("Value of $100 invested at start of 1928 in: S&P 500")
    assert labels[7] == "Annual Risk Premium: Stocks - Bills" and len(set(c["key"] for c in r["cols"])) == len(r["cols"]), "a heading used twice gets its group's name and its own key"


def test_premiums_are_worked_from_the_annual_returns():
    p = C.premiums_from_returns(C.returns_by_year(C.book(XLS)))
    whole, last50, last10 = p
    assert (whole["from"], whole["to"], whole["years"]) == (1928, 1949, 22) and last50["years"] == 22 and (last10["from"], last10["years"]) == (1940, 10)
    assert whole["stocks"] == {"arithmetic": 0.05, "compounded": round((1.2 * 0.9) ** 0.5 - 1, 4)}, "+20% and -10% in turn: 5% on average, 3.92% compounded"
    assert whole["stocks_minus_bills"] == {"arithmetic": 0.02, "compounded": round((1.2 * 0.9) ** 0.5 - 1.03, 4)} and whole["stocks_minus_bonds"]["arithmetic"] == 0.0
    assert C.premiums_from_returns({"cols": [{"label": "Year"}, {"label": "Gold"}], "rows": [[1, 2]]}) is None, "no stocks, bills and bonds: nothing is worked"


def test_country_premiums_stop_where_the_rated_countries_end():
    t = C.country_premiums(C.book(country_file()))
    assert t["updated"] == "2026-07-01" and t["mature_market_premium"] == 0.042 and t["us_premium"] == 0.0442 and t["equity_to_bond_volatility"] == 1.5545
    assert len(t["rows"]) == 64 and [c["key"] for c in t["cols"]][:6] == ["country", "region", "moodys_rating", "default_spread", "equity_risk_premium", "country_risk_premium"]
    by = {r[0]: r for r in t["rows"]}
    assert by["Albania"] == ["Albania", "Eastern Europe & Russia", "Ba3", 0.0286, 0.0865, 0.0445, None, None, None], "NA is not a number"
    assert by["United States"][4] == 0.0442 and by["Germany"][1] == "Western Europe", "the second column is the region whatever its heading says"
    assert "Brunei" not in by and "Frontier Markets (no sovereign ratings)" not in by and "Country" not in by, "the frontier markets are not rated countries"
    f = t["frontier"]
    assert f["title"].startswith("Frontier Markets") and [c["label"] for c in f["cols"]] == ["Country", "PRS Score", "Country Risk Premium", "Total Equity Risk Premium"]
    assert f["rows"] == [["Brunei", 86.5, 0.0048, 0.0468], ["Guyana", 70, 0.031, 0.073]]
    assert all(len(r) == 9 for r in t["rows"]), "the rating-to-spread table beside the countries is not read into them"


def test_an_industry_table_is_found_under_its_preamble():
    t = C.industry_table(C.book(XLS))
    assert t["sheet"] == "Industry Averages" and t["title"] == "Cost of equity and capital (made up)" and t["updated"] == "2026-01-05" and t["header_row"] == 8
    assert [c["label"] for c in t["cols"]] == ["Industry Name", "Number of Firms", "Beta", "Cost of Equity", "E/(D+E)", "Cost of Debt", "Tax Rate", "Cost of Capital"], "a heading over an empty column is dropped"
    assert len(t["rows"]) == 7 and t["rows"][0] == ["Advertising", 52, 1.21, 0.0935, 0.71, 0.0529, 0.05, 0.0781] and t["rows"][2][-1] is None and t["rows"][-1][0] == "Total Market"
    assert C.industry_table(C.book(month_file())) is None and C.industry_table(C.book(country_file())) is None, "a workbook with no such table is not forced into one"


def test_a_file_name_says_its_topic_and_region():
    assert C.region_of("wacc") == ("wacc", "US", "United States") and C.region_of("waccGlobal") == ("wacc", "Global", "Global")
    assert C.region_of("betaemerg")[1:] == ("Emerging", "Emerging markets") and C.region_of("R&DRest")[1:] == ("Rest", "Australia, NZ and Canada")
    assert C.region_of("India") == ("India", "US", "United States"), "a name that is only a region's word is not cut to nothing"
    pairs = {"pedata": "pe", "peGlobal": "pe", "pbvdata": "pbv", "pbvJapan": "pbv", "psdata": "ps", "betas": "beta", "betaChina": "beta", "totalbeta": "totalbeta",
             "vebitda": "vebitda", "vebitdaEurope": "vebitda", "R&D": "rd", "EVAGlobal": "eva", "wacc": "wacc", "margin": "margin"}
    assert {k: C.topic_of(k) for k in pairs} == pairs


def test_benchmarks_are_joined_by_his_own_headings():
    ind = ["Advertising", "Banks", "Total Market"]
    def tb(labels, vals):
        return {"cols": [{"key": "industry", "label": "Industry Name"}] + [{"key": C.slug(l), "label": l} for l in labels], "rows": [[n] + list(v) for n, v in zip(ind, vals)]}
    tables = {"wacc": tb(["Number of Firms", "Beta", "Cost of Equity", "E/(D+E)", "Std Dev in Stock", "Cost of Debt", "Tax Rate", "After-tax Cost of Debt", "D/(D+E)", "Cost of Capital", "Cost of Capital (Local Currency)"],
                         [[52, 1.2, 0.09, 0.7, 0.4, 0.05, 0.05, 0.04, 0.3, 0.078, 0.08], [15, 0.9, 0.08, 0.3, 0.2, 0.05, 0.2, 0.04, 0.7, 0.05, 0.051], [5994, 0.91, 0.08, 0.74, 0.3, 0.053, 0.083, 0.04, 0.26, 0.0696, 0.07]]),
              "margin": tb(["Number of firms", "Gross Margin", "Net Margin", "Pre-tax, Pre-stock compensation Operating Margin", "Pre-tax Unadjusted Operating Margin"], [[52, 0.36, -0.003, 0.11, 0.1], [15, None, 0.3, 0.0, 0.0], [5994, 0.38, 0.097, 0.14, 0.128]]),
              "pe": tb(["Number of firms", "Current PE", "Trailing PE", "Forward PE"], [[52, 40, 44.4, 52.9], [15, 12, 13, 11], [5994, 50, 57.9, 27.7]]),
              "vebitda": tb(["Number of firms", "EV/EBITDAR&D", "EV/EBITDA", "EV/EBIT", "EV/EBIT (1-t)", "EV/EBITDAR&D", "EV/EBITDA", "EV/EBIT"], [[52, 10, 12.0, 20.9, 25, 9, 11, 19], [15, None, None, None, None, None, None, None], [5994, 17, 19.7, 29.1, 35, 16, 18, 27]])}
    j = C.join_industries(tables)
    keys = [c["key"] for c in j["cols"]]
    assert keys == ["industry", "firms", "beta", "cost_of_equity", "equity_weight", "cost_of_debt", "tax_rate", "cost_of_capital", "gross_margin", "operating_margin", "net_margin",
                    "pe_trailing", "pe_forward", "ev_ebitda", "ev_ebit"], keys
    assert j["rows"][0] == ["Advertising", 52, 1.2, 0.09, 0.7, 0.05, 0.05, 0.078, 0.36, 0.1, -0.003, 44.4, 52.9, 12.0, 20.9], "the cost of capital in dollars, the unadjusted margin, the first EV/EBITDA"
    assert j["rows"][1][-2:] == [None, None] and [r[0] for r in j["rows"]] == ind
    assert sorted(j["missing"]) == sorted(["roe: Return on equity", "eva: Return on capital", "pbv: Price to book", "ps: Price to sales", "ps: EV to sales"]), "what could not be joined is said"


# ------------------------------------------------------------------------------------------------- pages and plan
HOME_HTML = """<html><body><a href="http://www.stern.nyu.edu/~adamodar/pc/implprem/ERPbymonth.xlsx">monthly</a>
<A HREF='../pc/datasets/histimpl.xls'>annual</A> <a href=../pc/datasets/histretSP.xls>returns</a>
<p>Implied ERP on October 1, 2026= 4.20% (Trailing 12 month, with adjusted payout); 3.70% (Trailing 12 month cash yield)</p>
<p>Implied ERP on November 1, 2026 = 4.41 % (Trailing 12 month, with adjusted payout); 3.79% (Trailing 12 month cash yield); 5.81% (Average CF yield last 10 years);
3.43% (Net cash yield); 3.39% (Normalized Earnings &amp; Payout) (with the US treasury rate of 4.94% used as the riskfree rate in US dollars; increase each by the default spread (0.22%))</p>
<p>Data: The latest overall data update was on January 9, 2026; my next one update will be on January 2027.</p></body></html>"""
CURRENT_HTML = """<a href="https://pages.stern.nyu.edu/~adamodar/pc/datasets/ctryprem.xlsx">Jan</a> <a href="https://pages.stern.nyu.edu/~adamodar/pc/datasets/ctrypremJuly26.xlsx">Jul</a>
<a href="https://pages.stern.nyu.edu/~adamodar/pc/datasets/wacc.xls">US</a> <a href="https://pages.stern.nyu.edu/~adamodar/pc/datasets/waccGlobal.xls">Global</a>
<a href="https://pages.stern.nyu.edu/~adamodar/pc/datasets/R&D.xls">R and D</a> <a href="https://pages.stern.nyu.edu/~adamodar/pc/datasets/S&P 500 Value.xlsx">a name with spaces</a>
<a href="https://pages.stern.nyu.edu/~adamodar/pc/ratings.xls">ratings</a> <a href="https://example.com/elsewhere.xls">not his</a> <a href="notes.pdf">a pdf</a>"""


def test_the_pages_are_read_for_his_files_and_his_own_words():
    got = L.links(HOME_HTML, L.HOME)
    assert got == {"ERPbymonth.xlsx": "https://pages.stern.nyu.edu/~adamodar/pc/implprem/ERPbymonth.xlsx", "histimpl.xls": "https://pages.stern.nyu.edu/~adamodar/pc/datasets/histimpl.xls",
                   "histretSP.xls": "https://pages.stern.nyu.edu/~adamodar/pc/datasets/histretSP.xls"}, got
    cur = L.links(CURRENT_HTML, L.CURRENT)
    assert sorted(cur) == ["R&D.xls", "S&P 500 Value.xlsx", "ctryprem.xlsx", "ctrypremJuly26.xlsx", "ratings.xls", "wacc.xls", "waccGlobal.xls"], sorted(cur)
    assert cur["S&P 500 Value.xlsx"].endswith("/S&P%20500%20Value.xlsx") and cur["R&D.xls"].endswith("/R&D.xls"), "a space is escaped, an ampersand is a letter of the name"
    h = L.home_facts(HOME_HTML)
    assert h["full_update"] == "January 9, 2026" and h["premium"]["date"] == "2026-11-01" and h["premium"]["tbond_pct"] == 4.94, "the newest month on the page"
    assert [v["pct"] for v in h["premium"]["values"]] == [4.41, 3.79, 5.81, 3.43, 3.39] and h["premium"]["values"][4]["label"] == "Normalized Earnings & Payout"
    assert L.home_facts("<p>nothing here</p>") == {"premium": None, "full_update": None}


def test_what_a_run_asks_about():
    files = {n: "u" for n in ("ERPbymonth.xlsx", "ctrypremJuly26.xlsx", "histimpl.xls", "histretSP.xls", "ratings.xls", "wacc.xls", "waccGlobal.xls", "betas.xls", "ERPNov26.xlsx")}
    ask, why = L.due(files, {}, NOW, "January 9, 2026")
    assert why == "the bank has never been swept" and len(ask) == 9 and ask[0] == "ctrypremJuly26.xlsx" and set(ask[:5]) == {"ERPbymonth.xlsx", "ctrypremJuly26.xlsx", "histimpl.xls", "histretSP.xls", "ratings.xls"}, "the files that move come first"
    banked = {"files": {n: {} for n in files if n != "ERPNov26.xlsx"}, "sweep": {"at": "2026-11-27T23:17:00+00:00", "full_update": "January 9, 2026"}}
    ask, why = L.due(files, banked, NOW, "January 9, 2026")
    assert why is None and ask == ["ctrypremJuly26.xlsx", "ERPbymonth.xlsx", "histimpl.xls", "histretSP.xls", "ratings.xls", "ERPNov26.xlsx"], "a normal day: the five that move, and the one file not yet banked"
    ask, why = L.due(files, dict(banked, sweep={"at": "2026-11-21T23:17:00+00:00", "full_update": "January 9, 2026"}), NOW, "January 9, 2026")
    assert why == "7 days since the last sweep" and len(ask) == 9
    ask, why = L.due(files, banked, NOW, "January 8, 2027")
    assert why == "his page announces a full update (January 8, 2027)" and len(ask) == 9
    ask, why = L.due(files, banked, NOW, None)
    assert why is None, "a page that could not be read for its date does not start a sweep"


# ------------------------------------------------------------------------------------------------------ whole runs
class Bucket:
    def __init__(self):
        self.objects, self.puts, self.copies = {}, [], []

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.objects[Key] = Body
        self.puts.append(Key)

    def copy_object(self, Bucket, Key, CopySource):
        self.objects[Key] = self.objects[CopySource["Key"]]
        self.copies.append((CopySource["Key"], Key))


class Site:
    """His site, made up: pages, files with an ETag each, and what was asked of it."""
    def __init__(self):
        self.files = {"ERPbymonth.xlsx": month_file(), "histimpl.xls": XLS, "histretSP.xls": XLS, "ctryprem.xlsx": country_file(update=44561, mature=0.0423, us=0.0446),
                      "ctrypremJuly26.xlsx": country_file(), "wacc.xls": XLS, "waccGlobal.xls": XLS, "R&D.xls": XLS, "ratings.xls": XLS, "S&P 500 Value.xlsx": month_file()}
        self.tags = {n: '"v1-%s"' % n for n in self.files}
        self.asked, self.broken, self.home = [], set(), HOME_HTML

    def get(self, url, etag=None, modified=None, timeout=45, tries=3):
        import urllib.parse
        name = urllib.parse.unquote(url.rsplit("/", 1)[-1])
        self.asked.append((name, etag))
        if url == L.HOME:
            return 200, {}, self.home.encode()
        if url == L.CURRENT:
            return 200, {}, CURRENT_HTML.encode()
        if name in self.broken:
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
        if etag and etag == self.tags[name]:
            return 304, {"ETag": etag}, b""
        return 200, {"ETag": self.tags[name], "Last-Modified": "Mon, 02 Nov 2026 16:19:09 GMT"}, self.files[name]


def go(site, s3, **kw):
    return L.run(s3, "b", get=site.get, now=kw.pop("now", NOW), sleep=lambda s: None, log=lambda t: None, **kw)


def test_a_first_run_banks_reads_and_publishes():
    site, s3 = Site(), Bucket()
    summary, docs = go(site, s3)
    assert summary["ok"] and summary["listed"] == 10 and summary["asked"] == 10 and len(summary["changed"]) == 10 and summary["swept"] and not summary["trouble"], summary
    assert summary["groups"] == {"premium": "derived", "country": "derived", "industry": "derived"} and summary["premium"] == {"as_of": "2026-11-01", "pct": 4.41}
    raw = sorted(k for k in s3.objects if k.startswith("damodaran/raw/"))
    assert len(raw) == 10 and "damodaran/raw/R&D.xls" in raw and s3.objects["damodaran/raw/ERPbymonth.xlsx"] == site.files["ERPbymonth.xlsx"], "every file is kept as it came"
    assert not [k for k in s3.objects if k.startswith("data/") and k.endswith((".xls", ".xlsx"))], "no file of his is put where the site serves it"
    p = json.loads(s3.objects["data/equity-risk-premium.json"])
    assert p["us"]["premium_pct"] == 4.41 and p["us"]["premium"] == 0.0441 and p["us"]["as_of"] == "2026-11-01" and p["us"]["measure"] == "sustainable_payout" and p["us"]["tbond_pct"] == 4.94
    assert p["us"]["expected_return_pct"] == 9.35 and p["us"]["a_month_before"] == {"date": "2026-10-01", "value": 0.042, "pct": 4.2} and p["us"]["a_year_before"]["pct"] == 3.73
    assert [m["pct"] for m in p["us"]["measures"]] == [4.41, 3.79, 5.81, 3.43, 3.39] and p["home_page"]["agree"] is True and p["home_page"]["page_values_pct"] == [4.41, 3.79, 5.81, 3.43, 3.39]
    assert p["attribution"].startswith("Data: Aswath Damodaran") and p["monthly"]["n"] == 5 and p["annual"]["from"] == 1960 and p["returns"]["from"] == 1928 and len(p["returns"]["premiums_worked_here"]) == 3
    assert [f["file"] for f in p["files"]] == ["ERPbymonth.xlsx", "histimpl.xls", "histretSP.xls"] and p["files"][0]["last_modified"].startswith("Mon, 02 Nov 2026")
    k = json.loads(s3.objects["data/country-risk-premiums.json"])
    assert k["file"] == "ctrypremJuly26.xlsx" and k["as_of"] == "2026-07-01" and k["other_files"] == ["ctryprem.xlsx"] and k["n"] == 64 and k["frontier"]["rows"][0][0] == "Brunei", "the newest of his dated files"
    i = json.loads(s3.objects["data/industry-benchmarks.json"])
    assert [g["code"] for g in i["regions"]] == ["US", "Global"] and i["n_tables"] == 3 and sorted(t["id"] for t in i["tables"]) == ["rd-us", "wacc-global", "wacc-us"]
    assert i["tables"][0]["key"].startswith("data/warm/damodaran/tables/") and "data/warm/damodaran/tables/wacc-us.json" in s3.objects
    assert i["not_tables"] == [{"file": "S&P 500 Value.xlsx", "why": "no table opening with 'Industry Name'"}], "a file that holds no industry table is named, with why"
    t = json.loads(s3.objects["data/warm/damodaran/tables/wacc-us.json"])
    assert t["region"] == "US" and t["n"] == 7 and t["file"]["file"] == "wacc.xls" and t["attribution"] and t["caution"].startswith("Industry averages are computed once a year")
    man = json.loads(s3.objects["data/warm/damodaran/manifest.json"])
    assert len(man["files"]) == 10 and man["sweep"] == {"at": NOW.isoformat(timespec="seconds"), "full_update": "January 9, 2026"} and len(man["runs"]) == 1 and man["files"]["wacc.xls"]["sha256"]


def test_a_day_with_nothing_new_writes_only_the_run_log():
    site, s3 = Site(), Bucket()
    go(site, s3)
    s3.puts.clear()
    site.asked.clear()
    later = datetime(2026, 11, 29, 23, 17, tzinfo=timezone.utc)
    summary, docs = go(site, s3, now=later)
    asked = [a for a in site.asked if a[0] not in ("home.htm", "datacurrent.html")]
    assert sorted(a[0] for a in asked) == ["ERPbymonth.xlsx", "ctryprem.xlsx", "ctrypremJuly26.xlsx", "histimpl.xls", "histretSP.xls", "ratings.xls"] and all(a[1] for a in asked), "only the files that move, each with its ETag"
    assert summary["unchanged"] == 6 and summary["changed"] == [] and summary["sweep"] is None and summary["groups"] == {"premium": "unchanged", "country": "unchanged", "industry": "unchanged"}
    assert s3.puts == ["data/warm/damodaran/manifest.json"] and docs == {}, "no document is rewritten for nothing"
    assert len(json.loads(s3.objects["data/warm/damodaran/manifest.json"])["runs"]) == 2


def test_a_new_month_keeps_the_file_it_replaces():
    site, s3 = Site(), Bucket()
    go(site, s3)
    old = site.files["ERPbymonth.xlsx"]
    site.files["ERPbymonth.xlsx"] = month_file(extra=[[44895, 7700, 0.05, 0.048, 199.0, 0.045, 0.039, 0.041, 0.059, 0.035, 0.036, 0.09]])       # 2026-12-01
    site.tags["ERPbymonth.xlsx"] = '"v2"'
    site.home = HOME_HTML.replace("November 1, 2026 = 4.41 %", "December 1, 2026 = 4.50 %")
    s3.puts.clear()
    summary, docs = go(site, s3, now=datetime(2026, 12, 2, 23, 17, tzinfo=timezone.utc))
    assert summary["changed"] == ["ERPbymonth.xlsx"] and summary["groups"] == {"premium": "derived", "country": "unchanged", "industry": "unchanged"} and summary["premium"] == {"as_of": "2026-12-01", "pct": 4.5}
    assert s3.copies == [("damodaran/raw/ERPbymonth.xlsx", "damodaran/raw/_superseded/ERPbymonth.xlsx.20261102")] and s3.objects["damodaran/raw/_superseded/ERPbymonth.xlsx.20261102"] == old
    assert sorted(s3.puts) == ["damodaran/raw/ERPbymonth.xlsx", "data/equity-risk-premium.json", "data/warm/damodaran/manifest.json"], s3.puts
    man = json.loads(s3.objects["data/warm/damodaran/manifest.json"])
    assert man["files"]["ERPbymonth.xlsx"]["superseded"][0]["key"].endswith(".20261102") and man["files"]["ERPbymonth.xlsx"]["etag"] == '"v2"'
    p = json.loads(s3.objects["data/equity-risk-premium.json"])
    assert p["us"]["a_month_before"]["pct"] == 4.41 and p["home_page"]["agree"] is True and p["monthly"]["n"] == 6


def test_a_file_resaved_with_the_same_content_changes_nothing():
    site, s3 = Site(), Bucket()
    go(site, s3)
    site.tags["histimpl.xls"] = '"touched"'
    s3.puts.clear()
    summary, docs = go(site, s3, now=datetime(2026, 11, 29, 23, 17, tzinfo=timezone.utc))
    assert summary["changed"] == [] and summary["unchanged"] == 6 and s3.puts == ["data/warm/damodaran/manifest.json"] and not s3.copies
    assert json.loads(s3.objects["data/warm/damodaran/manifest.json"])["files"]["histimpl.xls"]["etag"] == '"touched"', "the new tag is remembered, so tomorrow asks with it"


def test_a_dry_run_writes_nothing_and_still_derives():
    site, s3 = Site(), Bucket()
    summary, docs = go(site, s3, dry_run=True)
    assert s3.objects == {} and s3.puts == [] and summary["dry_run"] and summary["ok"] and len(summary["changed"]) == 10
    assert docs["data/equity-risk-premium.json"]["us"]["premium_pct"] == 4.41 and len([k for k in docs if "/tables/" in k]) == 3


def test_trouble_is_named_and_does_not_stop_the_rest():
    site, s3 = Site(), Bucket()
    site.broken = {"waccGlobal.xls"}
    site.files["ratings.xls"] = b"<html>moved</html>"
    summary, docs = go(site, s3)
    assert summary["ok"] and sorted(summary["trouble"]) == ["ratings.xls: not a spreadsheet (18 bytes)", "waccGlobal.xls: HTTP 404"], summary["trouble"]
    assert len(summary["changed"]) == 8 and "damodaran/raw/ratings.xls" not in s3.objects and "data/equity-risk-premium.json" in s3.objects
    site2, s32 = Site(), Bucket()
    site2.files["ERPbymonth.xlsx"] = country_file()                     # the monthly file is not what it should be
    summary2, _ = go(site2, s32)
    assert summary2["ok"] is False and summary2["groups"]["premium"] == "failed" and summary2["groups"]["country"] == "derived" and "data/equity-risk-premium.json" not in s32.objects
    assert any(t.startswith("premium documents: ValueError") for t in summary2["trouble"]), "a premium is never published from a file that does not read as his"

    class Down(Site):
        def get(self, url, *a, **k):
            raise OSError("no route")
    summary3, _ = go(Down(), Bucket())
    assert summary3["ok"] is False and summary3["error"] == "neither page listed a file"


def test_a_run_can_be_narrowed_and_stops_when_its_time_is_up():
    site, s3 = Site(), Bucket()
    summary, _ = go(site, s3, only=[r"^ERPbymonth", r"^hist"])
    assert sorted(summary["changed"]) == ["ERPbymonth.xlsx", "histimpl.xls", "histretSP.xls"] and summary["swept"] is False and summary["groups"]["premium"] == "derived"
    assert "sweep" not in json.loads(s3.objects["data/warm/damodaran/manifest.json"]) or not json.loads(s3.objects["data/warm/damodaran/manifest.json"])["sweep"], "a narrowed run is not a sweep: the next run still sweeps"
    ticks = iter(range(0, 100000, 100))
    site2, s32 = Site(), Bucket()
    summary2, _ = go(site2, s32, clock=lambda: next(ticks), budget=450)
    assert summary2["stopped"] and summary2["swept"] is False and 0 < len(summary2["changed"]) < 10, summary2
    assert not json.loads(s32.objects["data/warm/damodaran/manifest.json"])["sweep"], "an unfinished sweep is finished by the next run"


def test_the_handler_takes_only_plain_requests():
    import types
    seen = {}
    real = L.run
    L.run = lambda s3, bucket, **kw: (seen.update(kw) or {"ok": True}, {})
    had = sys.modules.get("boto3")
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *a, **k: None)          # no client is made, whatever is installed
    try:
        assert L.lambda_handler({"dry_run": 1, "only": ["^wacc"], "force_derive": 0}, None) == {"ok": True} and seen == {"dry_run": True, "force": False, "only": ["^wacc"]}
        L.lambda_handler({"only": "wacc"}, None)
        assert seen["only"] is None
        L.lambda_handler(None, None)
        assert seen == {"dry_run": False, "force": False, "only": None}
    finally:
        L.run = real
        if had is None:
            sys.modules.pop("boto3", None)
        else:
            sys.modules["boto3"] = had


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print("ok %s" % t.__name__)
    print("%d tests passed" % len(tests))
