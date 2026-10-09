"""Cloud-free regressions for justhodl-fdic-bankfind: metric arithmetic (SVB 2022Q4), aggregates, packet doctrine."""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "shared"))
sys.path.insert(0, str(HERE.parent / "source"))
import risk_sources as RS  # noqa: E402
import lambda_function as L  # noqa: E402

SVB = {"CERT": 24735, "NAME": "SILICON VALLEY BANK", "STALP": "CA", "ASSET": 209026000, "DEP": 175378000, "DEPUNA": 151592000,
       "EQ": 15456000, "SCHA": 91327000, "SCHF": 76168000, "RBCT1J": 16995000, "LNRECONS": 424000, "LNREMULT": 629000,
       "LNRENRES": 1747000, "BRO": 0, "OTHBFHLB": 15000000, "OTHBOR": 15040000, "NCLNLSR": 0.1872, "IDT1CER": 15.26, "LNLSNET": 73613000, "NCLNLS": 139000}
SMALL = {"CERT": 1, "NAME": "TINY", "STALP": "AL", "ASSET": 500000, "DEP": 400000, "DEPUNA": 0, "EQ": 50000, "SCHA": 10000, "SCHF": 9000,
         "RBCT1J": 50000, "LNRECONS": 50000, "LNREMULT": 50000, "LNRENRES": 100000, "BRO": 20000, "OTHBFHLB": 0, "OTHBOR": 0, "LNLSNET": 300000, "NCLNLS": 3000}


def test_svb_metrics():
    m = L.bank_metrics(SVB)
    assert m["uninsured_share"] == 86.4 and m["htm_loss_to_equity"] == 98.1 and m["htm_loss_musd"] == 15159.0
    assert m["cre_to_tier1"] == 16.0 and m["wholesale_to_assets"] == 14.4 and m["noncurrent_ratio"] == 0.19


def test_aggregate_uses_reporting_base_for_uninsured():
    a = L.aggregate([SVB, SMALL])
    assert a["n_banks"] == 2 and a["uninsured_share"] == 86.44  # SMALL (< $1bn) does not report DEPUNA
    assert a["n_htm_loss_gt50_equity"] == 1 and a["n_cre_gt300_tier1"] == 1 and a["n_uninsured_gt50_1bn"] == 1
    assert a["htm_loss_bn"] == 15.2


def test_quarter_ends_walk_back():
    assert L.quarter_ends(3, "20260630") == ["20260630", "20260331", "20251231"]
    assert L.quarter_ends(2, "20250331") == ["20250331", "20241231"]


def test_packet_doctrine_and_shape():
    pk = RS.Packet("fdic-bankfind", "FDIC BankFind", "t", "d", {"provider": "FDIC"})
    pk.add_series("SYS_X", "x", [["2026-03-31", 1.0], ["2026-06-30", 2.0]], unit="%")
    pk.kpi("k", "v")
    p = pk.build()
    assert p["decision"] == {"call": None, "sizing_eligible": False, "note": p["decision"]["note"]}
    assert RS.check_doctrine(p) == [] and p["series"]["SYS_X"]["latest"] == 2.0 and p["page"] == "/fdic-bankfind.html"
    assert L.lambda_handler.__module__ == "lambda_function"


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
