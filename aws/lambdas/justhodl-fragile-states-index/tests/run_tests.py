"""Cloud-free regressions for justhodl-fragile-states-index: workbook harvest, Excel-serial years, doctrine."""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "shared"))
sys.path.insert(0, str(HERE.parent / "source"))
import risk_sources as RS  # noqa: E402
import lambda_function as L  # noqa: E402


def test_find_workbooks():
    html = ("<a href=\"https://fragilestatesindex.org/wp-content/uploads/data/fsi-2006.xlsx\">a</a>"
            "<a href=\" https://fragilestatesindex.org/wp-content/uploads/2023/06/FSI-2023-DOWNLOAD.xlsx \">b</a>")
    b = L.find_workbooks(html)
    assert list(b) == [2006, 2023] and b[2023].endswith("FSI-2023-DOWNLOAD.xlsx")


def test_parse_serial_year():
    rows = [["Country", "Year", "Rank", "Total", "C1: Security Apparatus", "X1: External Intervention"],
            ["Somalia", 44562.0, "1st", 110.5, 9.8, 9.0], ["Yemen", 2022.0, "2nd", 108.1, 9.5, 9.6], [None]]
    inds, out = L.parse(rows)
    assert inds == ["C1: Security Apparatus", "X1: External Intervention"]
    assert [(r["country"], r["year"], r["total"]) for r in out] == [("Somalia", 2022, 110.5), ("Yemen", 2022, 108.1)]


def test_packet_doctrine():
    pk = RS.Packet(L.SLUG, "FSI", "t", "d", {"provider": "FFP"})
    pk.add_series("TOTAL_SOMALIA", "s", [["2022-12-31", 110.5], ["2023-12-31", 111.9]], unit="score")
    p = pk.build()
    assert RS.check_doctrine(p) == [] and p["decision"]["call"] is None and p["warm_prefix"] == "data/warm/fragile-states-index/"


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
