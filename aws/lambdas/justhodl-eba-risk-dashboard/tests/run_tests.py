"""Cloud-free regressions for justhodl-eba-risk-dashboard: period parsing, KRI names, workbook-link selection, doctrine."""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "shared"))
sys.path.insert(0, str(HERE.parent / "source"))
import risk_sources as RS  # noqa: E402
import lambda_function as L  # noqa: E402


def test_period_parsing():
    assert L.pdate(201412.0) == "2014-12-31" and L.pdate("202606") == "2026-06-30" and L.pdate(202503) == "2025-03-31"
    assert RS.period_to_date("2026-Q2") == "2026-06-30" and RS.period_to_date("2026-07") == "2026-07-31"


def test_kri_catalogue():
    assert L.SHORT["AQT_3.2"] == "NPL ratio" and L.SHORT["SVC_3"] == "CET1 capital ratio" and len(L.HEADLINE) == 8
    assert all(code in L.SHORT for code in L.HEADLINE)


def test_latest_workbook_is_picked(monkeypatch=None):
    html = ('<a href="/sites/default/files/2025-12/x/Data%20Annex%20InteractiveRiskDashboard%20Q3%202025.xlsx">a</a>'
            '<a href="/sites/default/files/2026-03/y/Data%20Annex%20InteractiveRiskDashboard%20Q4%202025.xlsx">b</a>'
            '<a href="/sites/default/files/2026-06/z/Data%20Annex%20InteractiveRiskDashboard%20Q1%202026.xlsx">c</a>')
    L.RS.http_text = lambda *a, **k: html
    url, (y, q) = L.find_workbook()
    assert (y, q) == (2026, 1) and url.startswith("https://www.eba.europa.eu/sites/default/files/2026-06/")


def test_doctrine():
    pk = RS.Packet(L.SLUG, "EBA", "t", "d", {"provider": "EBA"})
    pk.add_series("KRI_AQT_3_2_EU", "EU NPL", [["2026-03-31", 1.82], ["2026-06-30", 1.83]], unit="%")
    p = pk.build()
    assert RS.check_doctrine(p) == [] and p["decision"]["call"] is None


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for n, f in tests:
        try:
            f()
            print("PASS", n)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print("FAIL", n, repr(exc))
    print("%d/%d passed" % (len(tests) - failed, len(tests)))
    sys.exit(1 if failed else 0)
