"""Cloud-free regressions for justhodl-world-uncertainty-index: link harvest, quarterly table parser, doctrine."""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "shared"))
sys.path.insert(0, str(HERE.parent / "source"))
import risk_sources as RS  # noqa: E402
import lambda_function as L  # noqa: E402


def test_find_workbook():
    html = "<a href=\"https://worlduncertaintyindex.com/wp-content/uploads/2026/07/WUI_Data.xlsx\">x</a>"
    assert L.find_workbook(html).endswith("/2026/07/WUI_Data.xlsx") and L.find_workbook("") is None


def test_parse_table():
    rows = [["year", "Global (simple average)", "AFG"], ["1990q1", 9411.2, None], ["1990q2", 9267.3, 0.5], ["note", None, None]]
    c = L.parse_table(rows)
    assert c["Global (simple average)"] == [["1990-03-31", 9411.2], ["1990-06-30", 9267.3]] and c["AFG"] == [["1990-06-30", 0.5]]


def test_packet_doctrine():
    pk = RS.Packet(L.SLUG, "WUI", "t", "d", {"provider": "ABF"})
    pk.add_series("GLOBAL_X", "g", [["2026-03-31", 70000.0], ["2026-06-30", 77923.0]], unit="index")
    p = pk.build()
    assert RS.check_doctrine(p) == [] and p["decision"]["call"] is None and p["warm_prefix"] == "data/warm/world-uncertainty-index/"


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
