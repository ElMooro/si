"""Cloud-free regressions for justhodl-fao-food-price-index: CSV parser, link harvest, doctrine."""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "shared"))
sys.path.insert(0, str(HERE.parent / "source"))
import risk_sources as RS  # noqa: E402
import lambda_function as L  # noqa: E402


def test_parse_csv():
    txt = "FAO Food Price Index,,,\n2014-2016=100,,,\nDate,Food Price Index,Meat,Dairy,Cereals,Oils,Sugar\n,,,\n1990-01,64.4,74.3,53.5,64.1,44.59,87.9\n2026-09,136.0,120,130,110,170,95\n"
    c = L.parse(txt)
    assert c["Food Price Index"] == [["1990-01-31", 64.4], ["2026-09-30", 136.0]] and c["Sugar"][-1] == ["2026-09-30", 95.0]


def test_find_csv():
    html = "<a href=\"https://www.fao.org/media/docs/x/food_price_indices_data.csv?sfvrsn=1&amp;download=true\">csv</a>"
    assert L.find_csv(html) == "https://www.fao.org/media/docs/x/food_price_indices_data.csv?sfvrsn=1&download=true"
    assert L.find_csv("<p>nothing</p>") == L.FALLBACK


def test_packet_doctrine():
    pk = RS.Packet(L.SLUG, "FFPI", "t", "d", {"provider": "FAO"})
    pk.add_series("FFPI", "f", [["2026-08-31", 134.0], ["2026-09-30", 136.0]], unit="index")
    p = pk.build()
    assert RS.check_doctrine(p) == [] and p["decision"]["call"] is None and p["warm_prefix"] == "data/warm/fao-food-price-index/"


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
