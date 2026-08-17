"""LOCAL PUSH-GATE HARNESS -- stock-buying odds join (ops 4819).

Imports the real stock-buying lambda module with boto3 stubbed and
proves attach_base_rates() on the `symbol` key: field-matched quintile
cells, dd-band join, NON-GATING deep identity of every pre-existing
row field, honest None on absent/INSUFFICIENT spine, and unknown
symbols skipped.  The invest side of ops 4819 is covered by its own
pytest suite (tests/test_base_rates_join.py, 35/35).
Exit 1 on any failure: mandatory gate before push.
"""
import copy
import json
import sys
import types
from pathlib import Path

FAILS = []


def chk(name, cond, detail=""):
    print("  [%s] %s %s" % ("PASS" if cond else "FAIL", name, detail))
    if not cond:
        FAILS.append(name)


class _S3:
    def get_object(self, **kw):
        raise KeyError(kw.get("Key"))

    def put_object(self, **kw):
        pass


_stub = types.ModuleType("boto3")
_stub.client = lambda *a, **k: _S3()
sys.modules["boto3"] = _stub

SRC = (Path(__file__).resolve().parents[2] / "lambdas"
       / "justhodl-stock-buying" / "source")
sys.path.insert(0, str(SRC))
import lambda_function as sb  # noqa: E402

SPINE = {
    "status": "LIVE", "as_of": "2026-08-17",
    "diag": {"feeds": {"ledger_weeks": 53}},
    "current_assignments": {
        "NVDA": {"b": "large", "q": 5, "dd": "0_to_-10", "m6": 30.1},
        "PTC": {"b": "mid", "q": 1, "dd": "below_-35", "m6": -40.2},
    },
    "cohorts": {"26w": {
        "quintiles": [                      # out of order on purpose
            {"q": 1, "beat_pct": 32.7, "wilson_lb95_pct": 29.7,
             "median_excess_pp": -23.5, "n": 900},
            {"q": 5, "beat_pct": 41.6, "wilson_lb95_pct": 38.4,
             "median_excess_pp": -7.2, "n": 901}],
        "dd_bands": {"below_-35": {"beat_pct": 33.8}},
    }},
}


def rows():
    return [
        {"symbol": "NVDA", "score": 88.2, "tier": "EXPLOSIVE-SETUP",
         "gates": {"a": True}, "pillars": {"p": 1}},
        {"symbol": "PTC", "score": 61.0, "tier": "WATCH",
         "gates": {"a": False}, "pillars": {}},
        {"symbol": "ZZZZ", "score": 50.0, "tier": "SCREENED",
         "gates": {}, "pillars": {}},
    ]


def main():
    print("== stock-buying attach_base_rates ==")
    r = rows()
    before = copy.deepcopy(r)
    meta = sb.attach_base_rates(r, SPINE)
    o = r[0]["base_rate_odds"]
    chk("field-matched q=5 cell",
        o["q"] == 5 and o["beat_spx_pct"] == 41.6
        and o["lb95_pct"] == 38.4 and o["cohort_n"] == 901,
        json.dumps(o))
    chk("dd-band join on PTC",
        r[1]["base_rate_odds"]["dd_beat_spx_pct"] == 33.8
        and "dd_beat_spx_pct" not in o)
    chk("unknown symbol skipped", "base_rate_odds" not in r[2])
    ident = all(post[k] == v for pre, post in zip(before, r)
                for k, v in pre.items())
    chk("NON-GATING: every pre-existing field identical", ident)
    chk("meta", meta == {"as_of": "2026-08-17", "ledger_weeks": 53,
                         "rows_with_odds": 2})
    r2 = rows()
    b2 = copy.deepcopy(r2)
    chk("absent spine -> None + untouched",
        sb.attach_base_rates(r2, None) is None and r2 == b2)
    chk("INSUFFICIENT spine -> None",
        sb.attach_base_rates(r2, {"status": "INSUFFICIENT_DATA"})
        is None and r2 == b2)
    print()
    if FAILS:
        print("HARNESS FAILED: %s" % FAILS)
        sys.exit(1)
    print("HARNESS GREEN -- stock-buying join gate open (ops 4819)")


if __name__ == "__main__":
    main()
