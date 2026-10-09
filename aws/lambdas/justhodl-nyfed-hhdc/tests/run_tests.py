"""Cloud-free regressions for justhodl-nyfed-hhdc: quarter labels, both sheet layouts, quarter walk-back, doctrine."""
import datetime as dt
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "shared"))
sys.path.insert(0, str(HERE.parent / "source"))
import risk_sources as RS  # noqa: E402
import lambda_function as L  # noqa: E402


def test_years_and_quarters():
    assert L._year("99") == 1999 and L._year("03") == 2003 and L._year("26") == 2026
    assert L.recent_quarters(dt.date(2026, 10, 9), 3) == [(2026, 4), (2026, 3), (2026, 2)]


def test_parse_normal_sheet():
    rows = [["Total Debt Balance and Its Composition"], ["Trillions of $"], ["Return to Table of Contents"],
            [None, "Mortgage", "Total"]] + [[f"{y:02d}:Q{q}", 5.0 + y, 7.0 + y] for y in range(3, 8) for q in (1, 2)]
    title, unit, cols = L.parse_sheet(rows)
    assert title.startswith("Total Debt") and unit == "Trillions of $" and cols["Total"][0] == ["2003-03-31", 10.0] and len(cols["Mortgage"]) == 10


def test_parse_transposed_sheet():
    header = [None] + [f"{y:02d}:Q{q}" for y in range(3, 8) for q in (1, 2)]
    rows = [["Total Debt Balance per Capita* by State"], ["Thousands of $"], header, ["CA"] + list(range(10)), ["AZ"] + list(range(10, 20))]
    title, unit, cols = L.parse_sheet(rows)
    assert set(cols) == {"CA", "AZ"} and cols["AZ"][-1] == ["2007-06-30", 19.0] and cols["CA"][0] == ["2003-03-31", 0.0]


def test_packet_doctrine():
    pk = RS.Packet(L.SLUG, "HHDC", "t", "d", {"provider": "NY Fed"})
    pk.add_series("P3_TOTAL", "t", [["2026-03-31", 18.78], ["2026-06-30", 18.77]], unit="tn")
    p = pk.build()
    assert RS.check_doctrine(p) == [] and p["decision"]["call"] is None and p["warm_prefix"] == "data/warm/nyfed-hhdc/"


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
