"""Cloud-free regressions for justhodl-esma-ratings: rating-scale mapping, agency detection, ISO codes, doctrine."""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "shared"))
sys.path.insert(0, str(HERE.parent / "source"))
import risk_sources as RS  # noqa: E402
import lambda_function as L  # noqa: E402


def test_notch_scale_across_agencies():
    assert L.notch("AAA") == 21 and L.notch("AA+") == 20 and L.notch("BBB-") == 12 and L.notch("D") == 1
    assert L.notch("Aa1") == 20 and L.notch("Baa2") == 13 and L.notch("Caa1") == 5
    assert L.notch("AA (high)") == 20 and L.notch("A (low)") == 15 and L.notch("BBB(H)") == 14
    assert L.notch("SD") == 1 and L.notch("WD") is None and L.notch("") is None and L.notch("AAu") == 19


def test_agency_and_iso():
    assert L.agency({"respCraLeiName": "Moody's Investors Service Ltd"}) == ("Moody's", True)
    assert L.agency({"craName": "Fitch Ratings Ireland Limited"}) == ("Fitch", True)
    assert L.agency({"craName": "Capital Intelligence Ratings Ltd"})[1] is False
    assert L.iso3("IT") == "ITA" and L.iso3("US") == "USA" and L.iso3("XK") == "XKX"


def test_short_outlook_and_doctrine():
    assert L._short_outlook("Placed under negative watch") == "neg watch" and L._short_outlook("Placed under stable outlook") == "stable"
    pk = RS.Packet(L.SLUG, "ESMA", "t", "d", {"provider": "ESMA"})
    pk.add_series("SCORE_ITA", "Italy", [["2015-07-01", 13.0], ["2026-04-17", 14.0]], unit="notch")
    pk.add_table("grid", ["ISO3"], [["ITA"]])
    p = pk.build()
    assert RS.check_doctrine(p) == [] and p["decision"]["sizing_eligible"] is False and p["series"]["SCORE_ITA"]["chg"] == 1.0


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
