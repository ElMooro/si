"""Dependency-free checks for the corp bonds measurements."""
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "source"))
import corp_bonds_core as core

SAMPLE = """NCB,ISIN,ISSUER,MATURITY DATE,COUPON RATE
FR,FR0013426368,AIR LIQUIDE FINANCE,2030-06-20,0.625
FR,FR001400XYZ1,Air Liquide Finance,2034-06-19,1.125
DE,DE000A289RN6,VOLKSWAGEN INTERNATIONAL FINANCE,2028-03-16,1.875
IT,XS2751666426,ENEL S.p.A.,2028-07-24,3.375
"""

CDS = {"groups": {"global_corp": {"rows": [
    {"name": "Air Liquide Finance", "spread_bp": 42.5, "sector": "Chemicals"},
    {"name": "Enel SpA", "spread_bp": 78.0, "sector": "Utilities"},
    {"name": "Porsche Automobil Holding", "spread_bp": 90.0, "sector": "Autos"},
]}}}


def check(cond, msg):
    if not cond:
        raise SystemExit("FAIL " + msg)


def test_fred_and_sleeve():
    text = "DATE,BAMLC0A0CM\n2024-10-15,1.10\n2026-10-13,0.80\n2026-10-14,0.84\n"
    points = core.parse_fred_csv(text + "junk\n2026-10-15,.\n")
    check(points[-1] == ("2026-10-14", 0.84), "last print")
    spec = {"id": "us_ig", "label": "US IG", "region": "United States", "market": "corporate", "kind": "oas", "fred": "BAMLC0A0CM"}
    # pad so 1-session lag exists
    padded = [("2026-10-13", 0.80), ("2026-10-14", 0.84)]
    row = core.sleeve_stats(spec, padded, "2026-10-15")
    check(row["level"] == 84.0, "oas percent to bp")
    check(row["unit"] == "bp", "unit")
    check(row["chg_1d"] == 4.0, "1d change in bp")
    check(core.parse_fred_csv("<html>nope</html>") == [], "html is not a series")


def test_euro_book_and_cds():
    bonds = core.parse_ecb_holdings(SAMPLE + "FR,FR0013426368,DUP,2030-06-20,1\n")
    check(len(bonds) == 4, "duplicate isin dropped")
    book = core.euro_book(bonds, "2026-10-15")
    check(book["n_bonds"] == 4 and book["n_issuers"] == 4, "counts")
    check(book["by_country"][0]["country"] == "FR", "country roll")
    check(any(row["year"] == "2028" and row["n"] == 2 for row in book["maturity_wall"]), "wall")
    matched = core.attach_cds(book["issuers"], CDS)
    check(matched == 3, "both air liquide spellings and enel; porsche is not a guess")
    names = {row["issuer"]: row for row in book["issuers"]}
    check(names["AIR LIQUIDE FINANCE"]["cds_bp"] == 42.5, "cds bp")
    check("cds_bp" not in names["VOLKSWAGEN INTERNATIONAL FINANCE"], "no weak match")


def test_trace_and_packet():
    rows = [
        {"tradeReportDate": "2026-10-13", "productCategory": "all securities", "advances": 1, "declines": 2, "totalVolume": 10},
        {"tradeReportDate": "2026-10-14", "productCategory": "all securities", "advances": 1200, "declines": 800, "unchanged": 40, "totalTrades": 50000, "totalVolume": 18000.5, "fiftyTwoWeekHigh": 30, "fiftyTwoWeekLow": 12},
        {"tradeReportDate": "2026-10-14", "productCategory": "high yield", "advances": 100, "declines": 200, "totalTrades": 9000, "totalVolume": 4000},
    ]
    compact = core.compact_breadth(core.latest_slice(rows))
    check(compact["as_of"] == "2026-10-14" and len(compact["rows"]) == 2, "latest breadth day")
    sent = core.compact_sentiment(core.latest_slice([
        {"tradeReportDate": "2026-10-14", "productCategory": "all securities", "tradeType": "customer buy", "totalVolume": 60, "totalTrades": 10},
        {"tradeReportDate": "2026-10-14", "productCategory": "all securities", "tradeType": "customer sell", "totalVolume": 40, "totalTrades": 8},
    ]))
    check(sent["rows"][0]["side"] == "customer buy", "sentiment side")
    check(core.latest_slice(None) is None and core.latest_slice([]) is None, "empty feed stays empty")
    packet = core.build_packet("2026-10-15T00:00:00Z", "2026-10-15", [], {"corporate": {"breadth": compact}}, core.euro_book([], "2026-10-15"), 0, {})
    check(packet["engine"] == "justhodl-corp-bonds" and packet["decision"]["call"] is None, "no call")
    check("TRACE corporate issues 1200 up" in packet["read"], "read uses the tape")
    check(core.coverage_block()["not_issue_level"], "coverage is explicit")


if __name__ == "__main__":
    test_fred_and_sleeve()
    test_euro_book_and_cds()
    test_trace_and_packet()
    print("corp bonds tests ok", date.today().isoformat())
