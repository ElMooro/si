"""Cloud-free regressions for justhodl-fhfa-hpi: master-CSV slicing, y/y lookup, doctrine."""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "shared"))
sys.path.insert(0, str(HERE.parent / "source"))
import risk_sources as RS  # noqa: E402
import lambda_function as L  # noqa: E402


def test_parse_slices():
    txt = ("hpi_type,hpi_flavor,frequency,level,place_name,place_id,yr,period,index_nsa,index_sa,rstderr,note\n"
           "traditional,purchase-only,monthly,USA or Census Division,United States,USA,2026,7,440.0,443.5,,\n"
           "traditional,purchase-only,quarterly,State,Alabama,AL,2026,2,300.0,301.0,,\n"
           "traditional,all-transactions,quarterly,MSA,Oakland,36084,2026,2,456.0,,,\n"
           "developmental,all-transactions,quarterly,Puerto Rico,PR,PR,2026,2,1,1,,\n"
           "traditional,expanded-data,quarterly,State,Alabama,AL,2026,2,1,1,,\n")
    k = L.parse(txt)
    assert len(k) == 3
    assert k[("purchase-only", "monthly", "USA or Census Division", "United States", "USA")]["sa"] == [["2026-07-31", 443.5]]
    assert k[("all-transactions", "quarterly", "MSA", "Oakland", "36084")]["nsa"] == [["2026-06-30", 456.0]]


def test_yoy_lookup():
    pts = [["2025-07-31", 430.0], ["2026-06-30", 442.0], ["2026-07-31", 443.5]]
    assert L._yoy(pts, "2026-07-31", 12) == 430.0 and L._yoy(pts, "2026-06-30", 12) is None


def test_packet_doctrine():
    pk = RS.Packet(L.SLUG, "FHFA", "t", "d", {"provider": "FHFA"})
    pk.add_series("US_SA", "u", [["2026-06-30", 442.0], ["2026-07-31", 443.5]], unit="index")
    p = pk.build()
    assert RS.check_doctrine(p) == [] and p["decision"]["call"] is None and p["warm_prefix"] == "data/warm/fhfa-hpi/"


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
