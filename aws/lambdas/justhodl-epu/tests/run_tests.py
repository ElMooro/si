"""Cloud-free regressions for justhodl-epu: workbook/CSV parsers, rolling mean, doctrine."""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "shared"))
sys.path.insert(0, str(HERE.parent / "source"))
import risk_sources as RS  # noqa: E402
import lambda_function as L  # noqa: E402


def test_parse_global():
    rows = [["Year", "Month", "GEPU_current", "GEPU_ppp", "Australia"], [2026.0, 6.0, 240.1, 231.0, 150.0], [2026.0, 7.0, 242.0, None, 160.0], ["note", None]]
    c = L.parse_global(rows)
    assert c["GEPU_current"] == [["2026-06-30", 240.1], ["2026-07-31", 242.0]] and c["GEPU_ppp"] == [["2026-06-30", 231.0]] and len(c["Australia"]) == 2


def test_parse_daily_and_rolling():
    pts = L.parse_daily("day,month,year,daily_policy_index\n1,1,1985,103.83\n2,1,1985,296.43\n3,1,1985,56.06\n")
    assert pts[0] == ["1985-01-01", 103.83] and len(pts) == 3
    assert L.rolling_mean(pts, 2) == [["1985-01-02", 200.13], ["1985-01-03", 176.25]]


def test_packet_doctrine():
    pk = RS.Packet(L.SLUG, "EPU", "t", "d", {"provider": "BBD"})
    pk.add_series("GEPU_CURRENT", "g", [["2026-06-30", 240.0], ["2026-07-31", 242.0]], unit="index")
    p = pk.build()
    assert RS.check_doctrine(p) == [] and p["decision"]["call"] is None and p["warm_prefix"] == "data/warm/epu/"
    assert RS.ordinal(87.4) == "87th" and RS.ordinal(2) == "2nd" and RS.ordinal(11.6) == "12th"


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
