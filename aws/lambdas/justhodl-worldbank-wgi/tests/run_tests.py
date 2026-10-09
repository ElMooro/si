"""Cloud-free regressions for justhodl-worldbank-wgi: long-format parser, hot-tail override, doctrine."""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "shared"))
sys.path.insert(0, str(HERE.parent / "source"))
import risk_sources as RS  # noqa: E402
import lambda_function as L  # noqa: E402


def test_parse_long():
    rows = [["codeindyr", "code", "countryname", "year", "indicator", "estimate", "stddev", "nsource", "pctrank", "pctranklower", "pctrankupper"],
            ["USAcc2022", "USA", "United States", 2022.0, "cc", 1.1, 0.1, 10, 85.0, 80, 90],
            ["USAcc2023", "USA", "United States", 2023.0, "cc", 1.0, 0.1, 10, 83.0, 78, 88],
            ["USAzz2023", "USA", "United States", 2023.0, "zz", 1.0, 0.1, 10, 83.0, 78, 88]]
    d = L.parse(rows)
    assert list(d) == [("USA", "cc")] and d[("USA", "cc")]["est"] == [["2022-12-31", 1.1], ["2023-12-31", 1.0]] and d[("USA", "cc")]["rank"][-1] == ["2023-12-31", 83.0]


def test_hot_tail_override():
    pk = RS.Packet(L.SLUG, "WGI", "t", "d", {"provider": "WB"})
    pk.hot_tail = 2
    pk.add_series("CC_USA", "c", [[f"{y}-12-31", 1.0] for y in range(2000, 2024)], unit="est")
    p = pk.build()
    assert len(p["series"]["CC_USA"]["tail"]) == 2 and p["series"]["CC_USA"]["n"] == 24 and not p["series"]["CC_USA"]["tail_is_full"]
    assert RS.check_doctrine(p) == [] and p["decision"]["call"] is None and p["warm_prefix"] == "data/warm/worldbank-wgi/"


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
