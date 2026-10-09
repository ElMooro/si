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
import cds_long_context as LC  # noqa: E402

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
    assert src == "quoted" and quality == "inferred" and a == 138      # unconfirmed quote, no history to check it against
    a, src, quality = C.day_anchor([amb, q], RATE, (135.0, "firm"))
    assert src == "quoted" and quality == "firm" and a == 138          # consistent with the name's own trailing anchor
    # a quote that agrees with the print's own upfront is confirmed on its own
    qc = C.normalize_trade(quoted_row(138, **{"Other payment amount": "%.2f" % (abs(C.clean_upfront_pct(138, 100, q["tenor"], RATE) - C.accrued_pct(100, C.date(2026, 10, 7))) / 100 * 5e6), "Other payment type": "UFRO"}), "sec")
    assert C.quote_status(qc, RATE) == "confirmed" and C.day_anchor([qc], RATE, None)[2] == "firm"
    # mis-filed spread columns are not quotes: 1.25bp on a 500-coupon name, or a bare coupon far from the name's history
    bad = C.normalize_trade(quoted_row(1.25, **{"Fixed rate-Leg 1": "0.05"}), "sec")
    assert C.quote_status(bad, RATE) == "reject" and C.candidate_spreads(bad, RATE) == []
    far = C.normalize_trade(quoted_row(100), "sec")
    assert C.candidate_spreads(far, RATE, 1000.0) == [] and C.resolve_spread(far, RATE, 1000.0) == (None, "none")
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


def test_stale_levels_are_listed_not_shown_as_current():
    bank = {"version": C.VERSION, "series": {}, "meta": {}, "entity_map": {}, "days": []}
    emap = {}
    # priced in early September, then three weeks of prints whose upfront sign cannot be resolved
    days = ["2026-09-%02d" % d for d in range(1, 29) if C.date(2026, 9, d).weekday() < 5]
    for i, day in enumerate(days):
        if i < 3:
            ts = [C.normalize_trade(quoted_row(400, **{"Execution Timestamp": day + "T13:00:00Z"}), "sec") for _ in range(3)]
        else:
            ts = [C.normalize_trade(derived_row(900, coupon_bp=500.0, day=day), "sec") for _ in range(3)]
        emap = C.entity_map(ts, emap)
        agg = C.aggregate_day(ts, emap, RATE, {})   # no trailing anchors: the sign stays unresolved
        C.update_bank(bank, day, agg, emap)
    packet = C.build_packet(bank, days[-1], "2026-09-29T00:00:00Z")
    sov = packet["groups"]["sovereign"]
    assert not sov["rows"], "a 3-week-old level must not be shown as current"
    assert sov["unpriced"] and sov["unpriced"][0]["last_priced_date"] == days[2] and sov["unpriced"][0]["last_spread_bp"] == 400 and sov["unpriced"][0]["last_date"] == days[-1]
    # dated changes: a 1-day change is never computed across a gap
    key = C.normalize_name("Federative Republic of Brazil")
    m = C.measure_entity(key, bank["series"][key], days[-1])
    assert m["stale_days"] > C.STALE_MAX_DAYS and m["chg_1d_bp"] is not None  # consecutive priced days 09-02/09-03 are 1 day apart
    series = dict(bank["series"][key]); series.pop(days[1])
    m2 = C.measure_entity(key, series, days[-1])
    assert m2["chg_1d_bp"] is None or (C.date.fromisoformat(days[2]) - C.date.fromisoformat(days[0])).days <= 5


def test_unpriced_records_carry_both_branches_and_packet_has_expansion_blocks():
    bank = {"version": C.VERSION, "series": {}, "meta": {}, "entity_map": {}, "days": []}
    emap = {}
    days = ["2026-09-%02d" % d for d in range(1, 29) if C.date(2026, 9, d).weekday() < 5]
    for day in days:
        ts = [C.normalize_trade(derived_row(900, coupon_bp=500.0, day=day), "sec") for _ in range(3)]
        emap = C.entity_map(ts, emap)
        C.update_bank(bank, day, C.aggregate_day(ts, emap, RATE, {}), emap)
    packet = C.build_packet(bank, days[-1], "2026-09-29T00:00:00Z")
    u = packet["groups"]["sovereign"]["unpriced"][0]
    assert u["candidates"] and u["candidates"][0]["coupon_bp"] == 500
    assert u["candidates"][0]["above_coupon_bp"] > 500 > u["candidates"][0]["below_coupon_bp"]
    assert u["days_active_30d"] >= 15 and u["last_date"] == days[-1]
    for k in ("term", "wides_1y", "tights_1y", "activity"):
        assert k in packet, k
    assert packet["activity"]["columns"][0] == "date" and len(packet["activity"]["rows"]) == len(days)
    assert packet["decision"]["call"] is None


def test_long_context_parsers_weekly_and_mapping():
    fred_txt = "observation_date,BAA10Y\n" + "\n".join("%s,%s" % (d, v) for d, v in [("2008-12-01", "6.0"), ("2008-12-02", "6.1"), ("2026-09-01", "1.5"), ("2026-09-02", "."), ("2026-09-03", "1.6")])
    baa = LC.parse_fred_csv(fred_txt)
    assert baa == {"2008-12-01": 6.0, "2008-12-02": 6.1, "2026-09-01": 1.5, "2026-09-03": 1.6}
    assert LC.weekly(baa, scale=100, nd=0) == [["2008-12-02", 610.0], ["2026-09-03", 160.0]]
    ofr = LC.parse_ofr_csv("Date,OFR FSI,Credit,Equity valuation,Safe assets,Funding,Volatility,United States,Other advanced economies,Emerging markets\n2020-03-16,10.1,2.5,1,1,1,3,7,2,1.2\n")
    assert ofr["ofr_credit"] == {"2020-03-16": 2.5} and ofr["ofr_em"] == {"2020-03-16": 1.2}
    ebp = LC.parse_ebp_csv("date,gz_spread,ebp,est_prob\n2008-12-01,7.9,3.4,0.9\n")
    assert ebp["gz_spread"]["2008-12-01"] == 7.9
    # mapping: an exact line is recovered, and a thin overlap is refused
    fit = LC.fit_map([float(i) for i in range(40)], [10 + 2.0 * i for i in range(40)])
    assert fit["a"] == 10 and fit["b"] == 2 and fit["r2"] == 1 and fit["n"] == 40
    assert LC.fit_map([1.0, 2.0], [1.0, 2.0]) is None
    assert LC.pct_rank([1, 2, 3, 4], 3) == 75.0
    # full block with a synthetic bank overlap
    series = {}
    for i, v in enumerate(range(50, 90)):
        d = (C.date(2026, 8, 1) + C.timedelta(days=i)).isoformat()
        series[d] = {"s": float(v), "n": 3}
        baa[d] = 1.0 + 0.01 * v
    bank = {"series": {"IDX:CDX.NA.IG": series}, "meta": {}}
    block = LC.build_long_context({"BAA10Y": baa}, ofr, ebp, bank, "2026-09-09")
    m = block["series"]["baa10y"]["cdx_ig_map"]
    assert m["fit"]["n"] == 40 and m["fit"]["r2"] > 0.99 and m["mapped_baa10y_bp"] == 189
    assert block["series"]["baa10y"]["peaks"][0]["episode"].startswith("GFC") and block["series"]["baa10y"]["peaks"][0]["value"] == 610
    assert block["series"]["gz_spread"]["last"]["value"] == 790 and "ofr_em" in block["series"]
    hist = LC.build_history(bank, "2026-09-09", block, ["IDX:CDX.NA.IG"])
    assert len(hist["names"]["IDX:CDX.NA.IG"]["points"]) == 40 and hist["long"] is block


def test_v130_universe_aliases_and_long_anchor():
    # (a) sovereign taxonomy carries region / tier / iso3 and the AI / software sectors exist
    cls = C.classify_entity("sov", "FEDERAL REPUBLIC OF GERMANY", {"USD": 3})
    assert cls == {"group": "sovereign", "region": "DM Europe", "tier": "DM", "iso3": "DEU", "ccy": "USD"}
    assert C.classify_entity(None, "NVIDIA", {"USD": 1})["sector"] == "AI & semis"
    assert C.classify_entity(None, "ORACLE", {"USD": 1})["sector"] == "Software & internet"
    assert C.classify_entity(None, C.normalize_name("Community Health Systems, Inc."), {"USD": 1})["sector"] == "Healthcare"   # keyword fallback (whole word)
    assert C.classify_entity(None, C.normalize_name("Bayerische Motoren Werke Aktiengesellschaft"), {"EUR": 1})["sector"] == "Autos & transport"
    assert C.classify_entity(None, C.normalize_name("Petroleo Brasileiro S.A. - Petrobras"), {"USD": 1})["group"] == "global_corp"
    # (b) reporter short codes and '&' spellings fold into one series
    assert C.canonical_norm("ORACLECORP") == "ORACLE" and C.normalize_name("Wells Fargo&Company") == C.normalize_name("WELLS FARGO & CO")
    bank = {"version": C.VERSION, "series": {"ORACLE": {"2026-09-01": {"n": 2, "n5": 2, "np": 0, "s": None, "q": 0, "amb": 2, "nm": 1.0, "cd": {"100": [150.0, 60.0]}}},
                                                "ORACLECORP": {"2026-09-01": {"n": 1, "n5": 1, "np": 1, "s": 62.0, "sb": "quoted", "q": 1, "amb": 0, "nm": 5.0, "cd": None, "aq": "firm", "as": "quoted"}}},
            "meta": {"ORACLE": {"kind": "corp", "raw": "Oracle Corp", "ccy": {"USD": 2}}, "ORACLECORP": {"kind": "corp", "raw": "ORACLECORP", "ccy": {"USD": 1}}},
            "entity_map": {"u1": {"norm": "ORACLECORP", "raw": "ORACLECORP", "votes": 1}}, "days": ["2026-09-01"]}
    merged = C.merge_aliases(bank)
    assert merged == [("ORACLECORP", "ORACLE")] and "ORACLECORP" not in bank["series"]
    row = bank["series"]["ORACLE"]["2026-09-01"]
    assert row["n"] == 3 and row["np"] == 1 and row["s"] == 62.0 and row["cd"] == {"100": [150.0, 60.0]} and bank["meta"]["ORACLE"]["ccy"] == {"USD": 3}
    assert bank["entity_map"]["u1"]["norm"] == "ORACLE" and C.merge_aliases(bank) == []   # idempotent
    # (c) an old firm level only picks between FAR-apart branches (South Korea: 22 vs 190 on a 100bp coupon), never near ones
    t = C.normalize_trade(derived_row(170, coupon_bp=100.0, day="2026-10-01"), "sec")
    cands = C.candidate_spreads(t, RATE)
    assert len(cands) == 2, cands
    hi, lo = max(c[0] for c in cands), min(c[0] for c in cands)
    assert C.resolve_spread(t, RATE, lo * 1.3, far_only=True)[0] is not None            # branches far apart, anchor near the low one
    near = C.normalize_trade(derived_row(600, coupon_bp=500.0, day="2026-10-01"), "sec")
    nc = C.candidate_spreads(near, RATE)
    if len(nc) == 2 and abs(C.math.log(nc[0][0] / nc[1][0])) < C.BRANCH_FAR_LOG:
        assert C.resolve_spread(near, RATE, min(nc)[0] * 1.3, far_only=True)[0] is None  # near branches: an old anchor is not enough
    # (d) anchors_from_bank: the name's own firm level 100 days back is offered as 'trailing_long'
    bank2 = {"version": C.VERSION, "series": {"REPUBLIC OF KOREA": {"2026-06-25": {"n": 3, "n5": 3, "np": 3, "s": 20.0, "sb": "quoted", "q": 3, "amb": 0, "nm": 10.0, "aq": "firm", "as": "quoted", "cd": None},
                                                                      "2026-10-01": {"n": 3, "n5": 3, "np": 0, "s": None, "q": 0, "amb": 3, "nm": 10.0, "cd": {"100": [190.0, 22.0]}}}},
             "meta": {"REPUBLIC OF KOREA": {"kind": "sov", "raw": "Republic of Korea", "ccy": {"USD": 6}}}, "entity_map": {}, "days": ["2026-06-25", "2026-10-01"]}
    anc = C.anchors_from_bank(bank2, "2026-10-02")
    assert anc["REPUBLIC OF KOREA"][2] == "trailing_long" and anc["REPUBLIC OF KOREA"][0] == 20.0
    # (e) the packet lists the whole universe: universe + coverage for sovereigns, dormant block, status on rows
    packet = C.build_packet(bank2, "2026-10-02", "2026-10-02T00:00:00Z")
    sov = packet["groups"]["sovereign"]
    assert sov["coverage"]["known"] >= 100 and any(e["name"] == "South Korea" and e["iso3"] == "KOR" and e["status"] in ("unpriced", "dormant") for e in sov["universe"])
    assert all("status" in e for e in sov["universe"]) and "dormant" in sov and "tiers" in sov and "universe_rule" in packet["method"]


def test_v130_comove_sign_test_picks_the_branch_that_moves_with_its_index():
    import random
    random.seed(7)
    days = [(C.date(2026, 4, 1) + C.timedelta(days=i)).isoformat() for i in range(120) if (C.date(2026, 4, 1) + C.timedelta(days=i)).weekday() < 5]
    idx, name = {}, {}
    level, true = 60.0, 40.0
    for d in days:
        shock = random.gauss(0, 0.02)
        level *= C.math.exp(shock)
        true *= C.math.exp(shock * 0.8 + random.gauss(0, 0.005))
        mirror = 2 * 100.0 - true * 1.1                      # the other branch mirrors through the coupon: it moves against the index
        idx[d] = {"n": 50, "n5": 50, "np": 50, "s": round(level, 2), "q": 50, "amb": 0, "nm": 500.0}
        name[d] = {"n": 2, "n5": 2, "np": 0, "s": None, "q": 0, "amb": 2, "nm": 10.0, "cd": {"100": [round(mirror, 1), round(true, 1)]}}
    bank = {"version": C.VERSION, "series": {"IDX:CDX.EM": idx, "STATE OF QATAR": name}, "meta": {"STATE OF QATAR": {"kind": "sov", "raw": "State of Qatar", "ccy": {"USD": 9}}}, "entity_map": {}, "days": days}
    anc = C.anchors_from_bank(bank, (C.date.fromisoformat(days[-1]) + C.timedelta(days=1)).isoformat())
    a = anc.get("STATE OF QATAR")
    assert a and a[2] == "comove" and a[1] == "inferred" and abs(a[0] - true) / true < 0.15, a


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
