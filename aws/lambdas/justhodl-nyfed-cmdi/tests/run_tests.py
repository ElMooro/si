"""Cloud-free regressions for justhodl-nyfed-cmdi: Excel-date parsing, series stats, doctrine."""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "shared"))
sys.path.insert(0, str(HERE.parent / "source"))
import risk_sources as RS  # noqa: E402
import lambda_function as L  # noqa: E402


def test_excel_date_and_labels():
    assert RS.excel_date(38359) == "2005-01-07"      # first CMDI week
    assert RS.excel_date(46290) == "2026-09-25"
    assert set(L.LABELS) == {"Market CMDI", "IG CMDI", "HY CMDI"}


def test_series_stats_percentile():
    st, pts = RS.series_stats([["2005-01-07", 0.1], ["2008-12-19", 0.81], ["2026-09-25", 0.2]])
    assert st["latest"] == 0.2 and st["max"] == 0.81 and st["n"] == 3 and st["first"] == "2005-01-07"
    assert 0 < st["pct_rank"] < 100


def test_packet_doctrine():
    pk = RS.Packet(L.SLUG, "NY Fed CMDI", "t", "d", {"provider": "NY Fed"})
    pk.add_series("MARKET", "m", [["2026-09-18", 0.19], ["2026-09-25", 0.2]], unit="index 0–1")
    p = pk.build()
    assert RS.check_doctrine(p) == [] and p["decision"]["call"] is None and p["warm_prefix"] == "data/warm/nyfed-cmdi/"


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
