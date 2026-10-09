"""Cloud-free regressions for the CDS desk: pricer round-trips, print semantics, sign resolution, packet doctrine."""
import json
import math
import sys
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
sys.path.insert(0, str(ROOT / "aws/shared"))
sys.path.insert(0, str(HERE.parent / "source"))

import cds_desk as C  # noqa: E402

# boto3 is not needed for these tests; stub it so the handler module imports without AWS.
if "boto3" not in sys.modules:
    try:
        import boto3  # noqa: F401
    except ImportError:
        stub = types.ModuleType("boto3")
        stub.client = lambda *a, **k: None
        sys.modules["boto3"] = stub
import lambda_function as L  # noqa: E402

RATE = 4.24

BASE_ROW = {
    "Dissemination Identifier": "1", "Action type": "NEWT", "Event type": "TRAD", "Cleared": "I",
    "Execution Timestamp": "2026-10-07T13:16:33Z", "Expiration Date": "2031-12-20", "Platform identifier": "GSBS",
    "Notional amount-Leg 1": "5,000,000", "Notional currency-Leg 1": "USD", "Fixed rate-Leg 1": "0.01",
    "Spread-Leg 1": "", "Spread notation-Leg 1": "", "Other payment amount": "", "Other payment type": "",
    "Underlying Asset Name": "Federative Republic of Brazil", "Package indicator": "FALSE",
    "Unique Product Identifier": "QZRHM0LDMDZH", "UPI FISN": "NA/CDS Sov SN Sr", "UPI Underlier Name": "GLOBAL BD",
}


def row(**over):
    r = dict(BASE_ROW)
    r.update(over)
    return r


def derived_row(spread_bp, coupon_bp=100.0, notional=5_000_000, expiry="2031-12-20", day="2026-10-07", **over):
    """A print whose only price information is the (unsigned) upfront implied by spread_bp."""
    tenor = (C.date.fromisoformat(expiry) - C.date.fromisoformat(day)).days / 365.25
    clean = C.clean_upfront_pct(spread_bp, coupon_bp, tenor, RATE)
    cash = abs(clean - C.accrued_pct(coupon_bp, C.date.fromisoformat(day)))
    return row(**{"Fixed rate-Leg 1": str(coupon_bp / 1e4), "Other payment amount": "%.2f" % (cash / 100.0 * notional),
                  "Other payment type": "UFRO", "Notional amount-Leg 1": "{:,}".format(notional),
                  "Execution Timestamp": day + "T13:00:00Z", "Expiration Date": expiry, **over})


def quoted_row(spread_bp, **over):
    return row(**{"Spread-Leg 1": str(spread_bp / 1e4), "Spread notation-Leg 1": "3", **over})


def test_pricer_round_trip_and_calibrated_sign():
    for s in (20, 60, 100, 140, 400, 1500):
        up = C.clean_upfront_pct(s, 100, 5.2, RATE)
        assert abs(C.solve_spread_bp(up, 100, 5.2, RATE) - s) < 0.05, s
    assert C.clean_upfront_pct(60, 100, 5.0, RATE) < 0 < C.clean_upfront_pct(140, 100, 5.0, RATE)
    # Brazil print from 2026-10-07: 112bp quoted, coupon 100, $24,582 upfront on $5M -> model within 0.1% of notional
    t = C.normalize_trade(row(**{"Spread-Leg 1": "0.0112", "Spread notation-Leg 1": "3", "Other payment amount": "24582.42",
                                 "Other payment type": "UFRO"}), "sec")
    model = C.clean_upfront_pct(112, 100, t["tenor"], RATE) - C.accrued_pct(100, C.date(2026, 10, 7))
    assert abs(abs(model) - 24582.42 / 5e6 * 100) < 0.1


def test_filters_keep_senior_single_names_and_indices_only():
    assert C.normalize_trade(row(**{"UPI FISN": "NA/CDS Corp SN Sub"}), "sec") is None
    assert C.normalize_trade(row(**{"UPI FISN": "NA/CDS Corp SN Mz"}), "sec") is None
    assert C.normalize_trade(row(**{"UPI FISN": "NA/CDS Corp Idx Tranche"}), "cftc") is None
    assert C.normalize_trade(row(**{"Event type": "NOVA"}), "sec") is None
    assert C.normalize_trade(row(**{"UPI FISN": "NA/CDS Corp SN Sr"}), "sec")["kind"] == "corp"
    assert C.normalize_trade(row(), "sec")["kind"] == "sov"


def test_quoted_print_is_authoritative_and_capped_flag_is_kept():
    t = C.normalize_trade(quoted_row(112, **{"Notional amount-Leg 1": "5,000,000+"}), "sec")
    assert t["quoted_bp"] == 112 and t["capped"] is True
    assert C.resolve_spread(t, RATE) == (112.0, "quoted")


def test_package_prints_are_counted_but_never_priced():
    t = C.normalize_trade(derived_row(142, **{"Package indicator": "TRUE"}), "sec")
    assert t["package"] is True and C.candidate_spreads(t, RATE) == []
    m = C._measure([t], RATE, None)
    assert m["n"] == 1 and m["spread_5y"] is None and m["basis"]["package"] == 1


def test_sign_resolution_orders_evidence():
    amb = C.normalize_trade(derived_row(140), "sec")
    cands = C.candidate_spreads(amb, RATE)
    assert len(cands) == 2 and abs(cands[0][0] - 140) < 0.5 and 40 < cands[1][0] < 75
    # no evidence -> honestly ambiguous
    assert C.resolve_spread(amb, RATE, None) == (None, "ambiguous")
    assert C.day_anchor([amb], RATE, None) == (None, None, None)
    # same-day quoted print anchors the derived print
    q = C.normalize_trade(quoted_row(138), "sec")
    a, src, quality = C.day_anchor([amb, q], RATE, None)
    assert src == "quoted" and quality == "firm" and a == 138
    s, basis = C.resolve_spread(amb, RATE, a)
    assert basis == "derived" and abs(s - 140) < 0.5
    # a far-away anchor must not force a branch
    assert C.resolve_spread(amb, RATE, 400.0) == (None, "ambiguous")
    # single feasible branch: wide name at coupon 100 whose cash exceeds the seller-pays ceiling
    wide = C.normalize_trade(derived_row(900), "sec")
    assert len(C.candidate_spreads(wide, RATE)) == 1
    assert C.day_anchor([wide], RATE, None)[1] == "single_branch"
    # two coupons on the same entity agree on exactly one branch
    c100 = C.normalize_trade(derived_row(300, coupon_bp=100), "sec")
    c500 = C.normalize_trade(derived_row(300, coupon_bp=500), "sec")
    a, src, quality = C.day_anchor([c100, c500], RATE, None)
    assert src in ("multi_coupon", "single_branch") and abs(a - 300) < 6 and quality == "firm"
    # trailing anchor carries its quality forward
    a, src, quality = C.day_anchor([amb], RATE, (139.0, "inferred"))
    assert src == "trailing" and quality == "inferred"


def test_curve_shape_is_last_resort_and_refuses_coupon_crossing():
    # Italy-like: 1.8Y and 9.8Y prints, true curve 17 -> 65 at coupon 100 (the crossing alternative would need 17 -> 144)
    short = C.normalize_trade(derived_row(17, expiry="2028-06-20"), "sec")
    long_ = C.normalize_trade(derived_row(65, expiry="2036-06-20"), "sec")
    five = C.normalize_trade(derived_row(30), "sec")
    a, src, quality = C.day_anchor([short, long_, five], RATE, None)
    assert src == "curve_shape" and quality == "inferred" and abs(a - 30) < 1.0
    # Brazil-like: 1Y 53 and 5Y 130 at coupon 100 -> the crossing combination is plausible, so no inference
    b_short = C.normalize_trade(derived_row(53, expiry="2027-12-20"), "sec")
    b_long = C.normalize_trade(derived_row(130, expiry="2031-12-20"), "sec")
    assert C.day_anchor([b_short, b_long], RATE, None)[1] is None
    # a trailing anchor outranks the curve inference
    assert C.day_anchor([short, long_, five], RATE, (29.0, "firm"))[1] == "trailing"


def test_tiny_cash_is_not_a_market_upfront():
    t = C.normalize_trade(row(**{"Other payment amount": "11.91", "Other payment type": "UFRO"}), "sec")
    assert C.candidate_spreads(t, RATE) == []


def test_index_quote_disambiguation():
    hy = C.normalize_trade(row(**{"UPI FISN": "NA/CDS Corp Idx", "UPI Underlier Name": "CDX.NA.HY", "Fixed rate-Leg 1": "0.05",
                                  "Spread-Leg 1": "0.010666", "Spread notation-Leg 1": "3", "Notional amount-Leg 1": "10,000,000",
                                  "Other payment amount": "689611.11", "Other payment type": "UFRO", "Package indicator": "FALSE"}), "cftc")
    s, basis = C.resolve_spread(hy, RATE)
    assert hy["price"] == 106.66 and hy["quoted_bp"] is None and basis == "price" and 250 < s < 420
    ig = C.normalize_trade(row(**{"UPI FISN": "NA/CDS Corp Idx", "UPI Underlier Name": "CDX.NA.IG", "Fixed rate-Leg 1": "0.01",
                                  "Spread-Leg 1": "0.005872", "Spread notation-Leg 1": "3", "Notional amount-Leg 1": "25,000,000"}), "cftc")
    assert C.resolve_spread(ig, RATE) == (58.72, "quoted")
    # a price-looking value in a spread family with a 500 coupon is read as a price (iTraxx Xover prints at ~98)
    xo = C.normalize_trade(row(**{"UPI FISN": "NA/CDS Corp Idx", "UPI Underlier Name": "ITRAXX EUROPE CROSSOVER", "Fixed rate-Leg 1": "0.05",
                                  "Spread-Leg 1": "0.0098", "Spread notation-Leg 1": "3", "Notional amount-Leg 1": "5,000,000+",
                                  "Notional currency-Leg 1": "EUR"}), "cftc")
    C.index_quote(xo, RATE)
    assert xo["price"] == 98.0


def test_entity_normalisation_merges_aliases():
    assert C.normalize_name("Lincoln Natl Corp") == C.normalize_name("Lincoln National Corporation")
    assert C.canonical_norm(C.normalize_name("ROINDONES")) == C.canonical_norm(C.normalize_name("Republic of Indonesia"))
    assert C.canonical_norm("CHINE") == C.canonical_norm("PEOPLE S REPUBLIC OF CHINA")
    assert C.display_name(C.normalize_name("Federative Republic of Brazil"), None) == "Brazil"


def test_bank_update_is_idempotent_and_packet_keeps_doctrine():
    bank = {"version": C.VERSION, "series": {}, "meta": {}, "entity_map": {}, "days": []}
    emap = {}
    days = ["2026-09-%02d" % d for d in range(1, 29) if C.date(2026, 9, d).weekday() < 5]
    for i, day in enumerate(days):
        ts = [C.normalize_trade(quoted_row(110 + i, **{"Execution Timestamp": day + "T13:00:00Z"}), "sec") for _ in range(3)]
        ts += [C.normalize_trade(derived_row(112 + i, day=day), "sec")]
        emap = C.entity_map(ts, emap)
        agg = C.aggregate_day(ts, emap, RATE, C.anchors_from_bank(bank, day))
        C.update_bank(bank, day, agg, emap)
        C.update_bank(bank, day, agg, emap)  # re-running a day replaces, never duplicates
    assert bank["days"] == days
    key = C.normalize_name("Federative Republic of Brazil")
    last = bank["series"][key][days[-1]]
    assert last["n"] == 4 and last["np"] == 4 and last["sb"] == "quoted" and last["q"] == 3
    packet = C.build_packet(bank, days[-1], "2026-09-29T00:00:00Z")
    assert packet["decision"] == {"call": None, "sizing_eligible": False, "basis": "descriptive measurements of public prints only"}
    sov = packet["groups"]["sovereign"]
    assert sov["n_liquid"] == 1 and sov["rows"][0]["name"] == "Brazil" and sov["rows"][0]["spread_basis"] == "quoted"
    assert sov["rows"][0]["chg_1d_bp"] == 1.0 and sov["rows"][0]["sign_evidence"] == "firm"
    text = json.dumps(packet)
    for banned in ("buy", "sell", "target", "forecast", "predict"):
        assert banned not in text.lower().replace("subject", ""), banned
    assert "2008" in " ".join(packet["method"]["limitations"])


def test_unpriced_but_active_names_are_listed_not_hidden():
    bank = {"version": C.VERSION, "series": {}, "meta": {}, "entity_map": {}, "days": []}
    emap = {}
    days = ["2026-09-%02d" % d for d in range(1, 29) if C.date(2026, 9, d).weekday() < 5]
    for day in days:
        ts = [C.normalize_trade(derived_row(140, day=day, **{"Underlying Asset Name": "Republic of Italy", "Unique Product Identifier": "QZITALY00001",
                                                              "Notional amount-Leg 1": "5,000,000+"}), "sec") for _ in range(2)]
        emap = C.entity_map(ts, emap)
        C.update_bank(bank, day, C.aggregate_day(ts, emap, RATE, C.anchors_from_bank(bank, day)), emap)
    packet = C.build_packet(bank, days[-1], "now")
    sov = packet["groups"]["sovereign"]
    assert sov["n_liquid"] == 0 and sov["unpriced"] and sov["unpriced"][0]["name"] == "Italy"
    assert sov["unpriced"][0]["trades_30d"] >= C.LIQUID_MIN_TRADES_30D


def test_lambda_glue_rate_and_file_date_logic(monkeypatch=None):
    rows = {"2026-10-06": {"5Y": 4.99}, "2026-10-01": {"5Y": 4.80}}
    assert L.rate_for("2026-10-07", rows) == 4.99 - C.SWAP_PROXY_OFFSET_PCT
    assert L.rate_for("2026-10-03", rows) == 4.80 - C.SWAP_PROXY_OFFSET_PCT
    assert L.rate_for("2024-01-01", {}) == L.FALLBACK_5Y_PAR - C.SWAP_PROXY_OFFSET_PCT
    assert L.file_url("sec", "2026-10-07").endswith("/sec/eod/SEC_CUMULATIVE_CREDITS_2026_10_07.zip")
    assert L.file_url("cftc", "2026-10-07").endswith("/cftc/eod/CFTC_CUMULATIVE_CREDITS_2026_10_07.zip")
    # finalisation: a late print for D-1 found in file D lands on D-1, and D is written from file D only
    bank = {"version": C.VERSION, "series": {}, "meta": {}, "entity_map": {}, "days": []}
    prev = [C.normalize_trade(quoted_row(110, **{"Execution Timestamp": "2026-10-06T13:00:00Z"}), "sec")]
    cur = [C.normalize_trade(quoted_row(111, **{"Execution Timestamp": "2026-10-07T13:00:00Z"}), "sec"),
           C.normalize_trade(quoted_row(130, **{"Execution Timestamp": "2026-10-06T22:00:00Z"}), "sec")]
    done = L.process_file_date(bank, "2026-10-07", cur, "2026-10-06", prev, {})
    assert [(d, n, k) for d, n, k in done] == [("2026-10-06", 2, "final"), ("2026-10-07", 1, "preliminary")]
    key = C.normalize_name("Federative Republic of Brazil")
    assert bank["series"][key]["2026-10-06"]["n"] == 2 and bank["series"][key]["2026-10-07"]["s"] == 111.0


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print("PASS", name)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            import traceback
            traceback.print_exc()
            print("FAIL", name, repr(exc))
    print("%d/%d passed" % (len(tests) - failed, len(tests)))
    sys.exit(1 if failed else 0)
