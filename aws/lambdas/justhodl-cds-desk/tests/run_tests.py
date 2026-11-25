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
    # 2.0: subordinated and mezzanine names are read, as their own kind at 20% recovery; tranches are still never priced,
    # whichever way the file spells them ("Idx Tra" is how DTCC writes it - 1.6 looked for "Tranche" and read them as index prints)
    assert C.normalize_trade(row(**{"UPI FISN": "NA/CDS Corp SN Sub"}), "sec")["kind"] == "sub"
    assert C.normalize_trade(row(**{"UPI FISN": "NA/CDS Corp SN Mz"}), "sec")["rec"] == C.RECOVERY_SUB
    assert C.normalize_trade(row(**{"UPI FISN": "NA/CDS Corp Idx Tranche"}), "cftc") is None
    assert C.normalize_trade(row(**{"UPI FISN": "NA/CDS Corp Idx Tra", "UPI Underlier Name": "CDX.NA.IG"}), "cftc") is None
    assert C.normalize_trade(row(**{"UPI FISN": "NA/CDS Idx Swt", "UPI Underlier Name": "NA/CDS Corp Idx"}), "cftc") is None
    assert C.normalize_trade(row(**{"UPI FISN": "NA/CDS Sov SN Jr"}), "sec") is None
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


def test_v140_ecb_sovciss_and_imf_fundamentals():
    from datetime import date, timedelta
    cds_long_context = LC
    """ECB csvdata rows become per-country long series keyed by ISO3; IMF DataMapper JSON becomes per-sovereign fundamentals
    attached to universe entries and rows; both survive a missing source (empty dicts)."""
    hdr = "KEY,FREQ,REF_AREA,CURRENCY,PROVIDER_FM,INSTRUMENT_FM,PROVIDER_FM_ID,DATA_TYPE_FM,TIME_PERIOD,OBS_VALUE,OBS_STATUS\n"
    rows = []
    for i in range(0, 900, 3):
        d = (date(2006, 1, 2) + timedelta(days=i * 8)).isoformat()
        rows.append("CISS.D.IT.Z0Z.4F.EC.SOV_CIN.IDX,D,IT,Z0Z,4F,EC,SOV_CIN,IDX,%s,%.4f,A" % (d, 0.1 + (0.6 if d.startswith("2011") else 0.0)))
        rows.append("CISS.D.U2.Z0Z.4F.EC.SOV_GDPWN.IDX,D,U2,Z0Z,4F,EC,SOV_GDPWN,IDX,%s,%.4f,A" % (d, 0.2))
    ecb = cds_long_context.parse_ecb_csv(hdr + "\n".join(rows) + "\n")
    assert set(ecb) == {"ITA", "EA"}, ecb.keys()
    as_of = max(ecb["ITA"])
    block = cds_long_context.build_long_context({}, {}, {}, {"series": {}}, as_of, ecb=ecb)
    assert "sovciss_ITA" in block["series"] and "sovciss_EA" in block["series"]
    it = block["series"]["sovciss_ITA"]
    assert it["iso3"] == "ITA" and it["points"][0][0] >= "2006-01-01" and it["last"]["date"] == as_of
    euro = next(p for p in it["peaks"] if p["episode"].startswith("Euro"))
    assert euro["value"] > 0.6, euro
    imf_txt = json.dumps({"values": {"GGXWDG_NGDP": {"ITA": {"2024": 135.3, "2025": 136.9, "2026": 138.0}, "JPN": {"2025": 235.0}}}})
    imf = {"GGXWDG_NGDP": cds_long_context.parse_imf_json(imf_txt, "GGXWDG_NGDP")}
    f = cds_long_context.fundamentals_for(imf, "ITA", "2026-10-08")
    assert f["debt_gdp"] == 136.9 and f["debt_gdp_weo"] == 138.0 and f["year"] == "2025", f
    f2 = cds_long_context.fundamentals_for(imf, "JPN", "2026-10-08")
    assert f2["debt_gdp"] == 235.0 and f2["debt_gdp_weo"] is None
    assert cds_long_context.fundamentals_for(imf, "XXX", "2026-10-08") is None
    packet = {"groups": {"sovereign": {"universe": [{"name": "Italy", "iso3": "ITA", "key": "ITALY"}, {"name": "Chile", "iso3": "CHL", "key": None}],
                                       "rows": [{"key": "ITALY"}], "unpriced": [], "dormant": []}}}
    n = cds_long_context.attach_fundamentals(packet, imf, "2026-10-08")
    assert n == 1 and packet["groups"]["sovereign"]["rows"][0]["fund"]["debt_gdp"] == 136.9
    assert packet["fundamentals"]["n_sovereigns"] == 1 and "IMF" in packet["fundamentals"]["source"]
    assert cds_long_context.parse_ecb_csv("") == {} and cds_long_context.parse_imf_json("<html>", "X") == {}


def test_v150_ecb_ciss_clifs_families_attach_and_reconcile():
    """v1.5.0: CISS (daily, incl. US / UK / China) and CLIFS (monthly, rest of the EU) join SovCISS as ECB stress
    families; every sovereign gets its most specific record on the packet; a discontinued CLIFS country is skipped; the
    same-day cross-check against the sibling systemic-stress engine flags only real mismatches."""
    from datetime import date, timedelta
    hdr = "KEY,FREQ,REF_AREA,CURRENCY,PROVIDER_FM,INSTRUMENT_FM,PROVIDER_FM_ID,DATA_TYPE_FM,TIME_PERIOD,OBS_VALUE\n"
    sov, cis, cli = [], [], []
    for i in range(0, 900, 3):
        d = (date(2006, 1, 2) + timedelta(days=i * 8)).isoformat()
        sov.append("CISS.D.IT.Z0Z.4F.EC.SOV_CIN.IDX,D,IT,Z0Z,4F,EC,SOV_CIN,IDX,%s,%.4f" % (d, 0.1 + (0.6 if d.startswith("2011") else 0.0)))
        cis.append("CISS.D.IT.Z0Z.4F.EC.SS_CIN.IDX,D,IT,Z0Z,4F,EC,SS_CIN,IDX,%s,%.4f" % (d, 0.05))
        cis.append("CISS.D.US.Z0Z.4F.EC.SS_CIN.IDX,D,US,Z0Z,4F,EC,SS_CIN,IDX,%s,%.4f" % (d, 0.02 + (0.7 if d.startswith("2008-1") else 0.0)))
        cis.append("CISS.D.CN.Z0Z.4F.EC.SS_CIN.IDX,D,CN,Z0Z,4F,EC,SS_CIN,IDX,%s,%.4f" % (d, 0.01))
    for y in range(2006, 2027):
        for m in range(1, 13):
            per = "%04d-%02d" % (y, m)
            if per > "2026-08":
                break
            cli.append("CLIFS.M.SE._Z.4F.EC.CLIFS_CI.IDX,M,SE,_Z,4F,EC,CLIFS_CI,IDX,%s,%.4f" % (per, 0.09))
            cli.append("CLIFS.M.IT._Z.4F.EC.CLIFS_CI.IDX,M,IT,_Z,4F,EC,CLIFS_CI,IDX,%s,%.4f" % (per, 0.04))
            if y <= 2010:
                cli.append("CLIFS.M.EE._Z.4F.EC.CLIFS_CI.IDX,M,EE,_Z,4F,EC,CLIFS_CI,IDX,%s,%.4f" % (per, 0.05))
    ecb = LC.parse_ecb_csv(hdr + "\n".join(sov) + "\n")
    ciss = LC.parse_ecb_csv(hdr + "\n".join(cis) + "\n")
    clifs = LC.parse_ecb_csv(hdr + "\n".join(cli) + "\n")
    assert set(ciss) == {"ITA", "USA", "CHN"} and set(clifs) == {"SWE", "ITA", "EST"}
    assert "2026-08-31" in clifs["SWE"], "monthly periods become month-end dates"
    as_of = "2026-10-08"
    block = LC.build_long_context({}, {}, {}, {"series": {}}, as_of, ecb=ecb, ciss=ciss, clifs=clifs)
    ser = block["series"]
    assert {"sovciss_ITA", "ciss_ITA", "ciss_USA", "ciss_CHN", "clifs_SWE", "clifs_ITA"} <= set(ser), sorted(ser)
    assert "clifs_EST" not in ser and any("discontinued" in n for n in block.get("notes", [])), "Estonia's CLIFS stops in 2010"
    assert ser["ciss_USA"]["family"] == "ciss" and ser["ciss_USA"]["frequency"] == "daily" and "United States" in ser["ciss_USA"]["name"]
    assert ser["clifs_SWE"]["frequency"] == "monthly" and ser["clifs_SWE"]["points"][0][0] >= "2006-01-01" and len(ser["clifs_SWE"]["points"]) >= 240
    gfc = next(p for p in ser["ciss_USA"]["peaks"] if p["episode"].startswith("GFC"))
    assert gfc["value"] > 0.7
    packet = {"groups": {"sovereign": {"universe": [{"name": "Italy", "iso3": "ITA", "key": "ITALY"}, {"name": "United States", "iso3": "USA", "key": "USA"},
                                                    {"name": "Sweden", "iso3": "SWE", "key": None}, {"name": "China", "iso3": "CHN", "key": "CHINA"},
                                                    {"name": "Japan", "iso3": "JPN", "key": "JAPAN"}],
                                       "rows": [{"key": "ITALY", "iso3": "ITA"}, {"key": "JAPAN", "iso3": "JPN"}], "unpriced": [{"key": "CHINA", "iso3": "CHN"}], "dormant": []}}}
    n = LC.attach_ecb_stress(packet, ser)
    assert n == 4
    uni = {e["iso3"]: e.get("stress") for e in packet["groups"]["sovereign"]["universe"]}
    assert uni["ITA"]["family"] == "sovciss", "SovCISS wins over CISS and CLIFS for a euro state"
    assert uni["USA"]["family"] == "ciss" and uni["SWE"]["family"] == "clifs" and uni["CHN"]["family"] == "ciss" and uni["JPN"] is None
    assert packet["groups"]["sovereign"]["unpriced"][0]["stress"]["family"] == "ciss"
    assert packet["groups"]["sovereign"]["rows"][0]["stress"]["value"] == ser["sovciss_ITA"]["last"]["value"]
    assert "stress" not in packet["groups"]["sovereign"]["rows"][1]
    assert packet["ecb_stress"]["series_by_family"] == {"sovciss": 1, "ciss": 3, "clifs": 2}
    systemic = {"generated_at": "x", "sovereign_stress": {"countries": {"IT": {"value": ser["sovciss_ITA"]["last"]["value"] + 0.001, "as_of": ser["sovciss_ITA"]["last"]["date"]}}},
                "systemic_stress": {"countries": {"US": {"value": 0.5, "as_of": ser["ciss_USA"]["last"]["date"]}, "CN": {"value": 0.01, "as_of": "2000-01-01"}}}}
    xr = LC.reconcile_with_systemic_stress(packet, systemic)
    assert xr["n_checked"] == 3 and xr["n_same_day"] == 2 and xr["n_match"] == 1, xr
    assert LC.reconcile_with_systemic_stress(packet, None) is None
    for banned in ("buy", "sell", "target", "forecast", "predict"):
        assert banned not in json.dumps(packet["ecb_stress"]).lower()


def test_v160_risk_layers_attach_and_stay_descriptive():
    """v1.6.0: IMF ARA/GDD, ECB SUP bank→sovereign, EBA home bias, ESMA ratings, OFR Form PF and the U.S. credit block
    attach to sovereign universe entries + rows without touching the doctrine fields."""
    import cds_risk_layers as RL
    assert RL.VERSION == "1.6.0" and C.VERSION == "2.0.0"   # the layers are unchanged by 2.0
    pk = {"groups": {"sovereign": {"universe": [{"key": "ITA", "iso3": "ITA"}, {"key": "TUR", "iso3": "TUR"}, {"key": "USA", "iso3": "USA"}],
                                   "rows": [{"key": "ITA"}], "unpriced": [{"key": "TUR"}], "dormant": []}},
          "decision": {"call": None, "sizing_eligible": False}}
    imf = {"Reserves_ARA": {"TUR": {"2024": 0.73, "2025": 0.787}}, "Privatedebt_all": {"ITA": {"2023": 151.7, "2024": 147.0}, "USA": {"2024": 216.6}},
           "HH_ALL": {"ITA": {"2024": 47.5}}, "NFC_ALL": {"ITA": {"2024": 99.5}}}
    n_ara, n_debt = RL.attach_imf_layers(pk, imf, "2026-10-09")
    assert (n_ara, n_debt) == (1, 2)
    assert pk["groups"]["sovereign"]["unpriced"][0]["ara"] == {"ara": 0.79, "year": "2025"}
    assert pk["groups"]["sovereign"]["rows"][0]["pdebt"]["private_debt_gdp"] == 147.0
    sup_csv = ("KEY,FREQ,REF_AREA,COUNT_AREA,COUNTERPART_SECTOR,CB_ITEM,SBS_BREAKDOWN,SBS_DI_1,SBS_DI_2,CB_EXP_TYPE,DATA_TYPE,BS_SUFFIX,SBS_SAMPLE_TYPE,TIME_PERIOD,OBS_VALUE\n"
               "k,H,B01,IT,S13,E0010,_T,ALL,_Z,ALL,LE,E,C,2024-S2,503166.2\nk,H,B01,IT,S13,E0010,_T,ALL,_Z,ALL,LE,E,C,2025-S1,516247.5\n"
               "k,H,B01,IT,S13,E0010,_T,ALL,_Z,ALL,LE,E,C,2025-S2,540789.4\nk,H,IT,IT,S13,E0010,_T,ALL,_Z,ALL,LE,E,C,2025-S2,373521.9\n"
               "k,H,B01,W0,S13,E0010,_T,ALL,_Z,ALL,LE,E,C,2025-S2,3902804.8\n")
    sup = RL.parse_ecb_sup(sup_csv)
    assert RL.attach_ecb_sup(pk, sup) == 1
    b = pk["groups"]["sovereign"]["rows"][0]["banks"]
    assert b["ea_banks_eur_bn"] == 540.8 and b["home_banks_share_pct"] == 69.1 and b["chg_yoy_pct"] == 7.5 and b["period_end"] == "2025-12-31"
    assert pk["bank_sovereign"]["rows"][0][0] == "ITA" and pk["bank_sovereign"]["by_iso3"]["ITA"]["history"][-1] == ["2025-12-31", 540.8]
    eba = {"latest_period": "2026-06-30", "tables": {"sovereign_grid": {"columns": ["Banks of", "Home sovereign %", "Other EU/EEA %", "Amortised cost %", "0–3M %", "10Y+ %", "Total €bn"],
                                                                        "rows": [["IT", 49.57, 36.03, 65.98, 4.0, 19.7, 574.8], ["EU", 42.8, 30.0, 60.0, 5.0, 20.0, 3000.0]]}}}
    assert RL.attach_eba(pk, eba) == 1 and pk["groups"]["sovereign"]["rows"][0]["eba"]["home_bias_pct"] == 49.57
    assert pk["layers"]["eba"]["eu_home_bias_pct"] == 42.8
    esma = {"as_of": "2026-10-08", "consensus": {"ITA": {"consensus_label": "BBB+", "consensus_notch": 14.0, "n_agencies": 4, "negative_outlooks": 0, "positive_outlooks": 2,
                                                         "last_action_date": "2026-04-17", "ratings": {"S&P": {"rating": "BBB+", "outlook": "Placed under positive outlook", "date": "2026-01-30", "last_action": "x"}}}},
            "tables": {"actions": {"rows": [["2026-10-08", "MDV", "MALDIVES", "Moody's", "Upgrade", "Caa1", "FC"]]}}}
    assert RL.attach_esma(pk, esma) == 1 and pk["groups"]["sovereign"]["rows"][0]["rating"]["consensus"] == "BBB+"
    assert pk["layers"]["esma"]["recent_actions"][0][4] == "Upgrade"
    pts = [["2026-03-31", 2579e9], ["2026-06-30", 2774e9]]
    blob = json.dumps({"FPF-ASSETCLASS_SOVEREIGN_GNE_SUM": {"timeseries": {"aggregation": pts}}}).encode()
    n = RL.attach_ofr_form_pf(pk, lambda mn: blob if mn == "FPF-ASSETCLASS_SOVEREIGN_GNE_SUM" else None)
    assert n == 1 and pk["hedge_funds"]["series"]["sov_gne"]["last"] == {"date": "2026-06-30", "value": 2774.0}
    assert pk["hedge_funds"]["status"]["FPF-BORROW_REPO_SUM"] == "not in warm store"
    cmdi = {"as_of": "2026-09-25", "series": {"MARKET": {"latest": 0.2, "date": "2026-09-25", "pct_rank": 48.8, "chg": 0.01, "max": 0.81, "tail": [["2026-09-25", 0.2]]}}}
    fdic = {"latest_quarter": "2026-06-30", "aggregates": {"2026-06-30": {"htm_loss_to_equity": 8.22}}, "series": {}, "tables": {"fragility_screen": {"columns": ["Bank"], "rows": [["X"]]}}}
    assert RL.attach_us_credit(pk, cmdi, fdic) == 2
    assert pk["us_credit"]["cmdi"]["market"]["value"] == 0.2 and pk["us_credit"]["fdic"]["aggregates"]["htm_loss_to_equity"] == 8.22
    assert pk["decision"] == {"call": None, "sizing_eligible": False}
    blob = json.dumps(pk).lower()
    for w in ("buy", "sell", "target", "forecast", "predict"):
        assert (" %s " % w) not in blob, w
    # lambda glue: constants point at the sibling engines and the warm stores
    assert L.SIBLING_PACKETS["esma"] == "data/esma-ratings.json" and L.ECB_SUP_WARM == "data/warm/ecb-sup/"
    assert set(L.IMF_LAYER_SOURCES) == set(RL.IMF_ARA) | set(RL.IMF_GDD)


def test_capped_block_is_measured_at_the_cap_and_never_outvotes_a_quote():
    """A block notional is the disclosed cap, not a reason to drop the print.  It still cannot set the anchor when a quote exists."""
    wide = C.normalize_trade(derived_row(900, **{"Notional amount-Leg 1": "5,000,000+"}), "sec")
    assert wide["capped"] is True and len(C.candidate_spreads(wide, RATE)) == 1
    anchor, src, quality = C.day_anchor([wide], RATE, None)
    assert src == "single_branch" and quality == "firm" and anchor is not None
    measured = C._measure([wide], RATE, None)
    assert measured["spread_5y"] is not None and measured["anchor_source"] == "single_branch"
    # two feasible branches on a capped block stay unsigned — the cap is not a side
    amb = C.normalize_trade(derived_row(140, **{"Notional amount-Leg 1": "5,000,000+"}), "sec")
    assert C.day_anchor([amb], RATE, None) == (None, None, None)
    cand = C._measure([amb], RATE, None)["cand"]
    assert cand and cand["100"][0] > 100 > cand["100"][1]
    quote = C.normalize_trade(quoted_row(112), "sec")
    anchor, src, quality = C.day_anchor([wide, quote], RATE, None)
    assert src == "quoted" and anchor == 112


def test_contradicted_soft_quote_falls_through_to_the_upfront():
    """A Spread-Leg 1 value far from the name's anchor is not a quote, but the print's own upfront still has two branches."""
    trade = C.normalize_trade(derived_row(400, coupon_bp=100.0, **{"Spread-Leg 1": "0.01", "Spread notation-Leg 1": "3"}), "sec")
    assert C.quote_status(trade, RATE) == "soft"
    cands = C.candidate_spreads(trade, RATE, 1000.0)
    assert cands and cands[0][1] != "quoted"
    assert any(abs(v - 400) < 8 for v, _b in cands), cands
    # a bare coupon with no cash, far from the anchor, is still unresolved
    bare = C.normalize_trade(quoted_row(100), "sec")
    assert C.candidate_spreads(bare, RATE, 1000.0) == []


def test_own_firm_level_anchors_for_a_year_when_branches_are_far():
    """PG&E-shaped: last firm print ~8 months ago, today's two branches far apart.  180 days dropped it; 365 days keeps it."""
    assert C.LONG_ANCHOR_DAYS == 365
    series = {
        "2025-10-15": {"n": 3, "n5": 3, "np": 3, "s": 109.0, "sb": "quoted", "q": 2, "amb": 0, "nm": 10.0, "aq": "firm", "as": "quoted", "cd": None},
        "2026-10-01": {"n": 4, "n5": 4, "np": 0, "s": None, "q": 0, "amb": 4, "nm": 20.0, "cd": {"500": [917.0, 190.0]}},
    }
    bank = {"version": C.VERSION, "series": {"PGE": series}, "meta": {"PGE": {"kind": "corp", "raw": "PG&E", "ccy": {"USD": 4}}}, "entity_map": {}, "days": ["2025-10-15", "2026-10-01"]}
    anc = C.anchors_from_bank(bank, "2026-10-02")
    assert anc["PGE"][2] == "trailing_long" and anc["PGE"][0] == 109.0
    # resolved with that anchor: 190 is near 109, 917 is not, and the branches are far enough for an old anchor
    trade = C.normalize_trade(derived_row(190, coupon_bp=500.0, day="2026-10-01"), "sec")
    spread, basis = C.resolve_spread(trade, RATE, 109.0, far_only=True)
    assert basis == "derived" and spread is not None and abs(spread - 190) < 15
    stale = {"version": C.VERSION, "series": {"PGE": {"2025-08-01": series["2025-10-15"], "2026-10-01": series["2026-10-01"]}},
             "meta": bank["meta"], "entity_map": {}, "days": ["2025-08-01", "2026-10-01"]}
    assert "PGE" not in C.anchors_from_bank(stale, "2026-10-02")


def test_inferred_own_level_signs_a_later_far_branch():
    """Belgium-shaped. The only resolved level is inferred, three weeks old, and the next prints are capped
    blocks the old writer stored with no side. The near branch is kept; a close pair is not."""
    series = {
        "2026-09-16": {"n": 6, "n5": 6, "np": 6, "s": 17.59, "sb": "derived", "q": 0, "amb": 3, "nm": 8.0,
                       "aq": "inferred", "as": "curve_shape", "cd": {"100": [197.8, 19.7]}},
        "2026-10-08": {"n": 1, "n5": 1, "np": 0, "s": None, "q": 0, "amb": 1, "nm": 5.0, "cd": {"100": [173.9, 32.5]}},
    }
    meta = {"KINGDOM OF BELGIUM": {"kind": "sov", "raw": "Kingdom of Belgium", "ccy": {"EUR": 4}}}
    bank = {"version": C.VERSION, "series": {"KINGDOM OF BELGIUM": series}, "meta": meta, "entity_map": {}, "days": ["2026-09-16", "2026-10-08"]}
    anc = C.anchors_from_bank(bank, "2026-10-09")
    assert anc["KINGDOM OF BELGIUM"][2] == "trailing_long" and anc["KINGDOM OF BELGIUM"][1] == "inferred"
    assert abs(anc["KINGDOM OF BELGIUM"][0] - 17.59) < 0.01
    trade = C.normalize_trade(derived_row(32.5, coupon_bp=100.0, day="2026-10-08"), "sec")
    spread, basis = C.resolve_spread(trade, RATE, anc["KINGDOM OF BELGIUM"][0], far_only=True)
    assert basis == "derived" and spread is not None and abs(spread - 32.5) < 8, (spread, basis)
    near = {"2026-09-16": series["2026-09-16"], "2026-10-08": {"n": 1, "n5": 1, "np": 0, "s": None, "q": 0, "amb": 1, "cd": {"100": [140.0, 70.0]}}}
    bank_near = {"version": C.VERSION, "series": {"AVIS": near}, "meta": {"AVIS": {"kind": "corp", "raw": "Avis", "ccy": {"USD": 2}}}, "entity_map": {}, "days": ["2026-09-16", "2026-10-08"]}
    assert C._median_branch_picks(17.59, near["2026-10-08"]["cd"]) is False
    offered = C.anchors_from_bank(bank_near, "2026-10-09")["AVIS"]
    assert offered[2] == "trailing_long" and offered[1] == "inferred"
    close = C.normalize_trade(derived_row(80, coupon_bp=100.0, day="2026-10-08"), "sec")
    cands = C.candidate_spreads(close, RATE)
    if len(cands) == 2 and abs(C.math.log(cands[0][0] / cands[1][0])) < C.BRANCH_FAR_LOG:
        assert C.resolve_spread(close, RATE, 17.59, far_only=True)[0] is None


# ----------------------------------------------------------------------------------------------------- 2.0
def _bank():
    return {"version": C.VERSION, "series": {}, "meta": {}, "entity_map": {}, "days": []}


def _run(bank, day, rows_, anchors=None, external=None):
    ts = [t for t in (C.normalize_trade(r, "sec") for r in rows_) if t]
    emap = C.entity_map(ts, bank.get("entity_map") or {})
    agg = C.aggregate_day(ts, emap, RATE, anchors if anchors is not None else C.anchors_from_bank(bank, day, external=external))
    C.update_bank(bank, day, agg, emap)
    return agg


CORP = {"UPI FISN": "NA/CDS Corp SN Sr", "Underlying Asset Name": "AT&T Inc.", "Unique Product Identifier": "QZATT0000001"}
WEEKDAYS = ["2026-10-%02d" % d for d in range(1, 31) if C.date(2026, 10, d).weekday() < 5]


def test_v200_a_coupon_written_in_percent_is_one_hundred_basis_points():
    """The audit's root cause: one platform writes a 1% coupon as "1".  1.6 read 1 bp, found one feasible branch and
    published AT&T at 7 bp.  The reading that lands on a coupon the market trades with wins."""
    assert C.coupon_bp_of("0.01") == (100.0, "fraction") and C.coupon_bp_of("1") == (100.0, "percent") and C.coupon_bp_of("5") == (500.0, "percent")
    assert C.coupon_bp_of("0.05") == (500.0, "fraction") and C.coupon_bp_of("100") == (100.0, "bp") and C.coupon_bp_of("0.0025") == (25.0, "fraction")
    assert C.coupon_bp_of("0.04485") == (448.5, "bespoke") and C.coupon_bp_of("7") == (None, "unreadable") and C.coupon_bp_of("") == (None, None)
    # the real print: T CDS USD SR 5.25Y D14, 2026-11-19, $13,833.75 on $5M, coupon "1"
    t = C.normalize_trade(row(**dict(CORP, **{"Underlying Asset Name": "T CDS USD SR 5.25Y D14", "Fixed rate-Leg 1": "1", "Other payment type": "UFRO",
                                              "Other payment amount": "13833.75480", "Notional amount-Leg 1": "5,000,000+", "Execution Timestamp": "2026-11-19T13:27:00Z"})), "sec")
    assert t["coupon_bp"] == 100.0 and t["coupon_how"] == "percent"
    cands = C.candidate_spreads(t, RATE)
    assert len(cands) == 2 and all(85 < v < 115 for v, b in cands), cands      # 1.6: one branch, 6.9 bp
    assert C.resolve_spread(t, RATE)[1] == "bracketed" and 95 < C.resolve_spread(t, RATE)[0] < 105


def test_v200_one_entity_keeps_one_key_whatever_an_older_map_held():
    """The audit: AT&T was on the desk twice.  The map kept "AT&T" from an older spelling rule beside today's "AT & T"."""
    bank = _bank()
    bank["entity_map"] = {"QZATT0000001": {"norm": "AT&T", "raw": "AT&T Inc.", "votes": 800}}
    bank["series"] = {"AT&T": {"2026-10-01": {"n": 2, "n5": 2, "np": 2, "s": 99.0, "sb": "quoted", "q": 2, "amb": 0, "nm": 10.0}},
                      "AT & T": {"2026-09-30": {"n": 2, "n5": 2, "np": 2, "s": 98.0, "sb": "quoted", "q": 2, "amb": 0, "nm": 10.0}}}
    bank["meta"] = {"AT&T": {"kind": "corp", "raw": "AT&T Inc.", "ccy": {"USD": 2}}, "AT & T": {"kind": "corp", "raw": "AT&T Inc.", "ccy": {"USD": 2}}}
    assert C.merge_aliases(bank) == [("AT&T", "AT & T")]
    assert bank["entity_map"]["QZATT0000001"]["norm"] == "AT & T" and sorted(bank["series"]) == ["AT & T"]
    _run(bank, "2026-10-02", [quoted_row(100, **CORP, **{"Execution Timestamp": "2026-10-02T13:00:00Z"}) for _ in range(2)])
    assert sorted(bank["series"]) == ["AT & T"], "the day's prints must land on the one key"
    packet = C.build_packet(bank, "2026-10-02", "2026-10-03T00:00:00Z")
    assert [r["name"] for r in packet["groups"]["us_corp"]["rows"]] == ["AT&T"]


def test_v200_an_unrepeated_jump_is_neither_the_level_nor_an_anchor():
    bank = _bank()
    for day in WEEKDAYS[:12]:
        _run(bank, day, [quoted_row(95, **CORP, **{"Execution Timestamp": day + "T13:00:00Z"}) for _ in range(2)])
    key = "AT & T"
    bank["series"][key][WEEKDAYS[12]] = {"n": 1, "n5": 1, "np": 1, "s": 7.08, "sb": "derived", "q": 0, "amb": 0, "nm": 5.0, "as": "single_branch", "aq": "firm"}
    bank["days"].append(WEEKDAYS[12])
    assert C.unconfirmed_days(sorted(bank["series"][key].items())) == {WEEKDAYS[12]}
    m = C.measure_entity(key, bank["series"][key], WEEKDAYS[12])
    assert m["spread_bp"] == 95.0 and m["last_date"] == WEEKDAYS[11] and m["unconfirmed"]["spread_bp"] == 7.08 and m["n_unconfirmed_1y"] == 1
    assert C.anchors_from_bank(bank, WEEKDAYS[13])[key][0] == 95.0, "the anchor must not follow one unrepeated print"
    # a second day near it makes it real: a name that has truly repriced is not held back
    bank["series"][key][WEEKDAYS[13]] = {"n": 1, "n5": 1, "np": 1, "s": 7.5, "sb": "derived", "q": 0, "amb": 0, "nm": 5.0}
    assert C.unconfirmed_days(sorted(bank["series"][key].items())) == set()
    assert C.measure_entity(key, bank["series"][key], WEEKDAYS[13])["spread_bp"] == 7.5
    # and a day with three priced prints stands by itself
    bank["series"][key][WEEKDAYS[14]] = {"n": 3, "n5": 3, "np": 3, "s": 400.0, "sb": "derived", "q": 0, "amb": 0, "nm": 15.0}
    assert WEEKDAYS[14] not in C.unconfirmed_days(sorted(bank["series"][key].items()))


def test_v200_a_name_trading_at_its_coupon_gets_the_middle_of_its_two_branches():
    t = C.normalize_trade(derived_row(104, coupon_bp=100.0, **CORP), "sec")
    cands = sorted(v for v, b in C.candidate_spreads(t, RATE))
    assert len(cands) == 2 and C.math.log(cands[1] / cands[0]) < C.NEAR_COUPON_LOG
    s, basis = C.resolve_spread(t, RATE)
    assert basis == "bracketed" and cands[0] < s < cands[1]
    far = C.normalize_trade(derived_row(160, coupon_bp=100.0, **CORP), "sec")
    assert C.resolve_spread(far, RATE) == (None, "ambiguous"), "far-apart branches stay either / or"
    bank = _bank()
    agg = _run(bank, "2026-10-07", [derived_row(104, coupon_bp=100.0, **CORP) for _ in range(2)], anchors={})
    m = agg["entities"]["AT & T"]
    assert m["spread_basis"] == "bracketed" and m["anchor_source"] == "near_coupon" and m["anchor_quality"] == "bracketed"
    row_ = C.build_packet(bank, "2026-10-07", "2026-10-08T00:00:00Z")["groups"]["us_corp"]["rows"][0]
    assert row_["status"] == "bracketed" and row_["bracket_bp"][0] < row_["spread_bp"] < row_["bracket_bp"][1] and row_["sign_evidence"] == "bracketed"


def test_v200_a_companys_own_bonds_may_pick_between_far_branches_only():
    bank = _bank()
    day = "2026-10-07"
    hy = dict(CORP, **{"Underlying Asset Name": "MGM Resorts International", "Unique Product Identifier": "QZMGM0000001"})
    agg = _run(bank, day, [derived_row(250, coupon_bp=500.0, day=day, **hy) for _ in range(2)], external={"MGM RESORTS INTERNATIONAL": (162.3, "bonds")})
    m = agg["entities"]["MGM RESORTS INTERNATIONAL"]
    assert abs(m["spread_5y"] - 250) < 2 and m["anchor_source"] == "bonds" and m["anchor_quality"] == "inferred", m
    # branches that are not far apart: the bonds do not choose (150 against 60 around a 100 coupon, bonds at 95)
    ig = dict(CORP, **{"Underlying Asset Name": "HSBC Holdings plc", "Unique Product Identifier": "QZHSBC000001"})
    bank2 = _bank()
    agg2 = _run(bank2, day, [derived_row(150, coupon_bp=100.0, day=day, **ig) for _ in range(2)], external={"HSBC": (95.0, "bonds")})
    assert agg2["entities"]["HSBC"]["spread_5y"] is None and agg2["entities"]["HSBC"]["basis"]["ambiguous"] == 2
    # the name's own recent level always outranks them
    bank3 = _bank()
    _run(bank3, "2026-10-06", [quoted_row(700, **hy, **{"Execution Timestamp": "2026-10-06T13:00:00Z"}) for _ in range(2)])
    assert C.anchors_from_bank(bank3, day, external={"MGM RESORTS INTERNATIONAL": (162.3, "bonds")})["MGM RESORTS INTERNATIONAL"][2] == "trailing"


def test_v200_subordinated_names_are_their_own_series_at_twenty_percent_recovery():
    bank = _bank()
    sub = {"UPI FISN": "NA/CDS Corp SN Sub", "Underlying Asset Name": "BNP PARIBAS", "Unique Product Identifier": "QZBNPSUB0001", "Notional currency-Leg 1": "EUR"}
    sen = {"UPI FISN": "NA/CDS Corp SN Sr", "Underlying Asset Name": "BNP PARIBAS", "Unique Product Identifier": "QZBNPSEN0001", "Notional currency-Leg 1": "EUR"}
    day = "2026-10-07"
    _run(bank, day, [quoted_row(120, **sub) for _ in range(2)] + [quoted_row(60, **sen) for _ in range(2)])
    assert set(bank["series"]) == {"BNP PARIBAS", "SUB:BNP PARIBAS"} and bank["meta"]["SUB:BNP PARIBAS"]["kind"] == "sub"
    packet = C.build_packet(bank, day, "2026-10-08T00:00:00Z")
    assert packet["groups"]["sub"]["rows"][0]["spread_bp"] == 120.0 and packet["groups"]["sub"]["rows"][0]["name"] == "BNP Paribas"
    assert packet["groups"]["global_corp"]["rows"][0]["spread_bp"] == 60.0
    t = C.normalize_trade(derived_row(300, coupon_bp=100.0, **sub), "sec")
    senior = C.normalize_trade(derived_row(300, coupon_bp=100.0, **sen), "sec")
    above = lambda tr: [v for v, b in C.candidate_spreads(tr, RATE) if b == "above_coupon"][0]
    assert t["rec"] == C.RECOVERY_SUB and above(t) != above(senior), "the same upfront is a different spread at 20% recovery"
    assert C.merge_aliases(bank) == [] and "SUB:BNP PARIBAS" in bank["series"]


def test_v200_an_aged_level_is_shown_with_its_date_unless_later_prints_are_unresolved():
    bank = _bank()
    for day in WEEKDAYS[:3]:
        _run(bank, day, [quoted_row(28, **{"Execution Timestamp": day + "T13:00:00Z"}) for _ in range(2)])
    as_of = WEEKDAYS[14]      # about three weeks later, nothing printed since
    bank["days"].append(as_of)
    sov = C.build_packet(bank, as_of, "2026-10-22T00:00:00Z")["groups"]["sovereign"]
    assert sov["rows"] and sov["rows"][0]["status"] == "aged" and sov["rows"][0]["liquid"] is False and sov["rows"][0]["spread_bp"] == 28.0
    assert sov["rows"][0]["stale_days"] > C.STALE_MAX_DAYS and sov["n_aged"] == 1 and sov["n_liquid"] == 0
    too_old = C.build_packet(bank, "2026-12-15", "2026-12-16T00:00:00Z")["groups"]["sovereign"]
    assert not too_old["rows"], "past %d days it is no longer a level" % C.AGED_MAX_DAYS


def test_v200_quiet_names_are_listed_not_dropped():
    bank = _bank()
    day = "2026-10-07"
    quiet = dict(CORP, **{"Underlying Asset Name": "Schneider Electric SE", "Unique Product Identifier": "QZSCHN000001", "Notional currency-Leg 1": "EUR"})
    _run(bank, day, [derived_row(160, coupon_bp=100.0, day=day, **quiet)], anchors={})
    g = C.build_packet(bank, "2026-11-20", "2026-11-21T00:00:00Z")["groups"]["global_corp"]
    assert not g["rows"] and not g["unpriced"] and [q["name"] for q in g["quiet"]] == ["Schneider Electric SE"] and g["quiet"][0]["prints_1y"] == 1
    assert g["n_quiet"] == 1 and g["quiet"][0]["last_date"] == day


def test_v200_every_new_trade_is_counted_by_product():
    base = {"Execution Timestamp": "2026-11-19T15:00:00Z"}
    rows_ = [row(**base, **{"UPI FISN": "NA/CDS Corp Idx", "UPI Underlier Name": "CDX.NA.IG", "Notional amount-Leg 1": "100,000,000"}),
             row(**base, **{"UPI FISN": "NA/CDS Corp Idx Tra", "UPI Underlier Name": "CDX.NA.IG", "Notional amount-Leg 1": "25,000,000"}),
             row(**base, **{"UPI FISN": "NA/CDS Idx Swt", "UPI Underlier Name": "NA/CDS Corp Idx", "Fixed rate-Leg 1": "0.01", "Strike Price": "70", "Expiration Date": "2027-02-17",
                            "Notional amount-Leg 1": "400,000,000+"}),
             row(**base, **{"UPI FISN": "NA/CDS Idx Swt", "UPI Underlier Name": "NA/CDS Corp Idx", "Fixed rate-Leg 1": "0.05", "Strike Price": "0.01045", "Expiration Date": "2027-01-20",
                            "Notional amount-Leg 1": "110,000,000"}),
             row(**base, **{"UPI FISN": "NA/CDS Corp Idx", "UPI Underlier Name": "CMBX.NA.BBB-", "Fixed rate-Leg 1": "0.03", "Expiration Date": "2056-12-15",
                            "Other payment type": "UFRO", "Other payment amount": "909791.67", "Notional amount-Leg 1": "5,000,000"}),
             row(**base, **{"UPI FISN": "NA/CDS Corp SN Sub", "Notional currency-Leg 1": "EUR"}),
             row(**base, **{"UPI FISN": "NA/Cr Sw SN TRtn Corp Cs", "Notional currency-Leg 1": "KZT"}),
             row(**base, **CORP, **{"Fixed rate-Leg 1": "1"}), row(**base), row(**base, **{"Event type": "NOVA"})]
    recs = [r for r in (C.print_record(x) for x in rows_) if r]
    assert len(recs) == 9, "a novation is not a new trade"
    c = C.census_day(recs)
    assert {k: v[0] for k, v in c["cls"].items()} == {"index": 2, "tranche": 1, "option": 2, "sub": 1, "trs": 1, "corp": 1, "sov": 1}
    assert c["cls"]["trs"][3] == 1 and c["cls"]["option"][4] == 1, "another currency is counted as a print; a capped block is counted as capped"
    assert c["fam"]["option|CDX.NA.IG"][0] == 1 and c["fam"]["option|CDX.NA.HY"][0] == 1 and c["fam"]["tranche|CDX.NA.IG"][0] == 1
    assert c["fam"]["trs|Cr Sw SN TRtn Corp Cs"][0] == 1, "a product with no index behind it is counted under the name the file gives it"
    assert c["opt"] == [["CDX.NA.IG", "2027-02-17", 70.0, "spread", 400.0, 1, "USD"], ["CDX.NA.HY", "2027-01-20", 104.5, "price", 110.0, 0, "USD"]]
    assert c["str"]["CMBX.NA.BBB-|2056-12-15"][:2] == [1, round(909791.67 / 5e6 * 100, 2)] and c["how"] == {"percent": 1, "fraction": 2}
    share_of_par = C.print_record(row(**base, **{"UPI FISN": "NA/CDS Idx Swt", "UPI Underlier Name": "NA/CDS Corp Idx", "Fixed rate-Leg 1": "0.05", "Strike Price": "0.94", "Expiration Date": "2027-01-20"}))
    assert (share_of_par["strike"], share_of_par["strike_kind"]) == (94.0, "price"), "0.94 of par is a price strike of 94, not a 9,400 bp spread"
    wild = C.census_day([dict(recs[0], notional=1e14)])
    assert wild["cls"]["index"][:2] == [1, 0.0], "a notional no trade can have is counted as a print, not as a hundred trillion"
    assert C.normalize_trade(rows_[4], "cftc") is None, "a commercial-mortgage index has no 5-year spread: it is counted, never priced as one"
    bank = _bank()
    bank["census"] = {"2026-11-19": dict(c, cls=dict(c["cls"], index=[2000, 5e4, 0, 0, 0]))}
    bank["days"] = ["2026-11-19"]
    packet = C.build_packet(bank, "2026-11-19", "2026-11-20T00:00:00Z")
    classes = {x["cls"]: x for x in packet["products"]["windows"]["1d"]["classes"]}
    assert classes["tranche"]["measured"].startswith("activity") and classes["option"]["n"] == 2 and packet["products"]["daily"]["rows"][0][0] == "2026-11-19"
    assert [f["family"] for f in packet["options"]["families"]] == ["CDX.NA.IG", "CDX.NA.HY"] and packet["options"]["families"][0]["ladder"][0]["strike"] == 70.0
    assert packet["structured"]["rows"][0]["label"] == "CMBX BBB-" and packet["structured"]["rows"][0]["price_if_paid_for_protection"] == round(100 - 18.2, 2)
    assert packet["quality"]["coupon_written_as"] == {"percent": 1, "fraction": 2}


def test_v200_listings_reference_and_ties_are_read_from_the_warehouses_own_documents():
    assert C.listing_key("HSBC Holdings plc") == C.listing_key("HSBC HOLDINGS") == "HSBC" and C.listing_key("AT&T Inc.") == "AT AND T"
    assert C.listing_key("Schneider Electric SE") == C.listing_key("SCHNEIDER ELECTRIC S E".replace("S E", "SE")) == "SCHNEIDER ELECTRIC"
    # legal words go only where a name ends: Public Power Corporation is not "Power" (the audit matched it to PWR Holdings in Sydney)
    assert C.listing_key("Public Power Corporation S.A.") == "PUBLIC POWER" and C.listing_key("PWR Holdings Limited") == "POWER"
    assert C.listing_key("Johnson Controls International Public Limited Company") == C.listing_key("Johnson Controls International plc")
    assert C.listing_key("Vornado Realty L.P.") == C.listing_key("Vornado Realty Trust") == "VORNADO REALTY"
    # a swap in euros is on a European company: EDP Corporation of Tokyo is not EDP, S.A.
    assert not C.listing_allowed({"country": "JP"}, "EUR") and C.listing_allowed({"country": "PT"}, "EUR") and C.listing_allowed({"country": "JP"}, "USD")
    assert C.listing_allowed({"country": "JP"}, "JPY") and not C.listing_allowed({"country": "US"}, "JPY") and "ROGERS" in C.LISTING_DENY
    home = {"exchange": "LSE", "country": "GB", "adr": False, "volume": 1.0}
    adr = {"exchange": "NYSE", "country": "GB", "adr": True, "volume": 9e9}
    otc = {"exchange": "OTC", "country": "GB", "adr": False, "volume": 9e9}
    assert C.listing_rank(home) > C.listing_rank(adr) > C.listing_rank(otc)
    bank = _bank()
    bank["meta"] = {"AT & T": {"kind": "corp", "raw": "AT&T Inc.", "ccy": {"USD": 9}}, "SUB:BNP PARIBAS": {"kind": "sub", "raw": "BNP PARIBAS", "ccy": {"EUR": 2}},
                    "PARAMOUNT SKYDANCE": {"kind": "corp", "raw": "PARAMOUNT SKYDANCE CORP", "ccy": {"USD": 3}}, "IDX:CDX.NA.IG": {"kind": "index"}}
    board = {"as_of": "2026-11-23", "cds": {"priced": [{"name": "AT&T", "ticker": "T", "company": "AT&T Inc."}], "no_level": []},
             "issuers": {"cols": ["id", "ticker", "name", "book", "spread_bp", "px_m1"],
                         "rows": [["T:T", "T", "AT&T Inc.", "ig", 145.3, 6.87], ["I:PARAMOUNT SKYDANCE", None, "PARAMOUNT SKYDANCE", "hy", 310.0, None], ["I:BNP PARIBAS", None, "BNP PARIBAS", "xig", 88.0, None]]}}
    ties, status = L.load_ties(bank, board)
    assert ties["AT & T"] == {"board_id": "T:T", "ticker": "T", "company": "AT&T Inc.", "bond_bp": 145.3, "bond_book": "ig", "bond_as_of": "2026-11-23", "stock_1m_pct": 6.87}
    assert ties["PARAMOUNT SKYDANCE"]["bond_bp"] == 310.0 and "ticker" not in ties["PARAMOUNT SKYDANCE"] and status.startswith("ok: 3 names")
    ref, _ = L.load_reference({"generated_at": "2026-11-24T18:16:36", "countries": [{"country": "Poland", "cds_bp": 151.0, "as_of": "24 November 2026"},
                                                                                    {"country": "Russia", "cds_bp": None}, {"country": "Atlantis", "cds_bp": 5.0}]})
    assert list(ref) == ["POL"] and ref["POL"]["cds_bp"] == 151.0
    ext = L.external_anchors(bank, ties, ref)
    assert ext["AT & T"] == (145.3, "bonds") and "SUB:BNP PARIBAS" not in ext, "senior bonds are no evidence for a subordinated swap"
    assert [v for k, v in ext.items() if v[1] == "reference"] == [(151.0, "reference")]
    packet = C.build_packet({**bank, "series": {}}, "2026-11-24", "2026-11-25T00:00:00Z", extras={"ties": ties, "reference": ref})
    poland = [u for u in packet["groups"]["sovereign"]["universe"] if u["iso3"] == "POL"][0]
    assert poland["status"] == "not_in_tape" and poland["ref_bp"] == 151.0 and packet["groups"]["sovereign"]["coverage"]["reference_only"] == 1


def test_v200_a_distressed_print_is_carried_at_the_ceiling_and_read_in_points():
    day = "2026-11-24"
    acc = C.accrued_pct(500.0, C.date.fromisoformat(day))

    def pts(points):                      # the file gives the cash: the quoted points less the accrued coupon
        return C.normalize_trade(row(**{"Fixed rate-Leg 1": "0.05", "Other payment amount": "%.2f" % ((points - acc) / 100.0 * 2_000_000), "Other payment type": "UFRO",
                                        "Notional amount-Leg 1": "2,000,000", "Execution Timestamp": day + "T13:00:00Z", "Expiration Date": "2031-12-20",
                                        "UPI FISN": "NA/CDS Corp SN Sr", "Underlying Asset Name": "Virgin Media Finance PLC"}), "sec")
    beyond, near, usual, small = pts(63.0), pts(58.8), pts(30.0), pts(2.0 + acc)
    assert C.candidate_spreads(beyond, RATE, 3000.0) == [(C.SPREAD_CEILING_BP, "above_coupon")], "63 points is more than a 40% recovery can ever pay: for a name already wide, still a level"
    assert C.candidate_spreads(near, RATE, 3000.0) == [(C.SPREAD_CEILING_BP, "above_coupon")], "the print the audit read as 36,103 bp"
    # one such print on a healthy name is a mis-filed amount, not a default: Toyota was carried at the ceiling for a day in a dry run
    assert C.candidate_spreads(beyond, RATE, 50.0) == [] and C.candidate_spreads(beyond, RATE) == [] and C.candidate_spreads(near, RATE, 50.0) == []
    assert C._measure([dict(beyond)], RATE, 50.0)["spread_5y"] is None
    two = C.mark_day([dict(beyond), dict(beyond)])                   # two prints of the day beyond the model's reach are a distressed name
    assert two[0]["day_distress"] and C.candidate_spreads(two[0], RATE) == [(C.SPREAD_CEILING_BP, "above_coupon")]
    got = C.candidate_spreads(usual, RATE)
    assert len(got) == 1 and 1500 < got[0][0] < 2200, got                       # an ordinary distressed print is left alone
    assert len(C.candidate_spreads(small, RATE)) == 2                            # two points: both sides of the coupon stay feasible
    m = C._measure([beyond, dict(beyond), near], RATE, None)
    assert m["spread_5y"] == C.SPREAD_CEILING_BP and 58 < m["points_5y"] <= 63.01, m


def test_v200_a_quote_that_is_its_own_upfront_in_points_is_points_not_a_spread():
    """The audit: Hertz at 49 bp for a day.  A dealer platform filed 49 points in the spread field; the quote less the cash
    the print disseminates is exactly the accrued coupon, which also says which coupon it is."""
    day = "2026-10-27"                                   # 38 days into the quarter: a 5% coupon has accrued 0.5278 points
    acc5 = C.accrued_pct(500.0, C.date.fromisoformat(day))

    def gsbs(quote, cash_pts, **over):                   # that platform gives no coupon
        return C.normalize_trade(row(**{"Fixed rate-Leg 1": "", "Spread-Leg 1": str(quote), "Spread notation-Leg 1": "4", "Other payment amount": "%.2f" % (cash_pts / 100.0 * 3_000_000),
                                        "Other payment type": "UFRO", "Notional amount-Leg 1": "3,000,000", "Execution Timestamp": day + "T14:00:00Z", "Expiration Date": "2031-12-20",
                                        "UPI FISN": "NA/CDS Corp SN Sr", "Underlying Asset Name": "The Hertz Corporation", **over}), "sec")
    hertz = gsbs(53.5, 53.5 - acc5)
    assert C.points_quote(hertz) == (53.5, 500.0), "the gap is the accrued coupon of a 5% contract"
    (hs, hb), = C.candidate_spreads(hertz, RATE)
    assert C.quote_status(hertz, RATE) == "points" and hb == "above_coupon" and 5000 < hs <= C.SPREAD_CEILING_BP, hs      # not 53.5 bp
    assert C.candidate_spreads(gsbs(81.0, 81.0 - acc5), RATE, 4000.0) == [(C.SPREAD_CEILING_BP, "above_coupon")], "81 points is past what a 40% recovery pays"
    whirl = gsbs(12.5, 12.5 - C.accrued_pct(100.0, C.date.fromisoformat(day)))
    assert C.points_quote(whirl) == (12.5, 100.0)
    (s, branch), = C.candidate_spreads(whirl, RATE)
    assert branch == "above_coupon" and 330 < s < 460, s                    # 12.5 points on a 1% coupon over five years, not 12.5 bp
    m = C._measure([hertz, dict(hertz)], RATE, None)
    assert m["spread_5y"] == round(hs, 2) and m["points_5y"] == 53.5 and m["basis"]["quoted"] == 0, m
    # a real quoted spread is left alone: 112 bp with the upfront a 112 bp spread gives is still a quote
    real = C.normalize_trade(quoted_row(112.0, **{"Other payment amount": "%.2f" % (abs(C.clean_upfront_pct(112.0, 100.0, 5.2, RATE) - C.accrued_pct(100.0, C.date(2026, 10, 7))) / 100.0 * 5_000_000),
                                                 "Other payment type": "UFRO"}), "sec")
    assert C.points_quote(real) is None and C.resolve_spread(real, RATE)[1] == "quoted"
    # the day itself (2026-04-20): six prints quoted "49" on a 5% coupon whose cash was 48.87 - the quote is rounded, so the gap is
    # not the accrual to the cent, and read as a spread the 1.6 engine found it "confirmed" by a 10% coupon nobody quoted
    apr = C.normalize_trade(row(**{"Fixed rate-Leg 1": "0.05", "Spread-Leg 1": "49", "Spread notation-Leg 1": "4", "Other payment amount": "%.2f" % (48.87 / 100.0 * 3_000_000), "Other payment type": "UFRO",
                                   "Notional amount-Leg 1": "3,000,000", "Execution Timestamp": "2026-04-20T14:00:00Z", "Expiration Date": "2031-06-20", "UPI FISN": "NA/CDS Corp SN Sr",
                                   "Underlying Asset Name": "The Hertz Corporation"}), "sec")
    assert C.points_quote(apr) == (49.0, 500.0) and C.quote_status(apr, RATE) == "points"
    (s49, b49), = C.candidate_spreads(apr, RATE)
    assert b49 == "above_coupon" and s49 > 3000, "49 points, not 49 bp: %s" % s49
    nocpn = C.normalize_trade(row(**{"Fixed rate-Leg 1": "", "Spread-Leg 1": "30", "Spread notation-Leg 1": "4", "Other payment amount": "%.2f" % (29.75 / 100.0 * 3_000_000), "Other payment type": "UFRO",
                                     "Notional amount-Leg 1": "3,000,000", "Execution Timestamp": "2026-04-20T14:00:00Z", "Expiration Date": "2031-06-20", "UPI FISN": "NA/CDS Corp SN Sr",
                                     "Underlying Asset Name": "Some Name Inc."}), "sec")
    assert C.points_quote(nocpn) == (30.0, None) and C.candidate_spreads(nocpn, RATE) == [], "points on a coupon the print does not say and the accrual does not name: no level, and never 30 bp"
    bare = C.normalize_trade(quoted_row(49.0), "sec")                       # a quote with no cash beside it cannot be told apart: it stays a soft quote
    assert C.points_quote(bare) is None


def test_v200_a_leg_quoted_like_the_days_points_quotes_is_points_too():
    """The rows themselves (ops 6958, 2026-04-20): seven Hertz prints at "49".  Five disseminate 48.556% - the quote less the
    accrued coupon.  Two disseminate 21.53% - a leg whose cash is netted against another - and a 49 bp spread on a 5% coupon
    happens to cost about that, so 1.6 called them confirmed quotes and published Hertz at 49 bp."""
    def gsbs(quote, cash_pct, expiry="2031-06-20", name="The Hertz Corporation", day="2026-04-20", notional=3_000_000):
        return C.normalize_trade(row(**{"Fixed rate-Leg 1": "", "Spread-Leg 1": str(quote / 1e4), "Spread notation-Leg 1": "3", "Other payment amount": "%.2f" % (cash_pct / 100.0 * notional),
                                        "Other payment type": "UFRO", "Notional amount-Leg 1": "{:,}".format(notional), "Execution Timestamp": day + "T19:20:00Z", "Expiration Date": expiry,
                                        "UPI FISN": "NA/CDS Corp SN Sr", "Underlying Asset Name": name}), "sec")
    own, netted = gsbs(49.0, 48.556), gsbs(49.0, 21.53)
    assert C.points_quote(own) == (49.0, 500.0) and C.points_quote(netted) is None
    assert C.quote_status(netted, RATE) == "confirmed", "read alone, the netted leg looks like a 49 bp quote"
    day = C.mark_day([own, dict(own), netted, dict(netted)])
    assert day[2]["points_as"] == (49.0, 500.0) and "points_as" not in day[0]
    (s, branch), = C.candidate_spreads(day[2], RATE)
    assert branch == "above_coupon" and s > 3000, "49 points like its siblings, not 49 bp: %s" % s
    m = C._measure([own, dict(own), netted, dict(netted)], RATE, None)
    assert m["spread_5y"] > 3000 and m["basis"]["quoted"] == 0 and m["points_5y"] == 49.0, m
    # even with no sibling that day, a quote "confirmed" only under a coupon the print does not state is not believed far from the name's own level
    assert C.candidate_spreads(gsbs(49.0, 21.53), RATE, 4000.0) == [] and C.candidate_spreads(gsbs(49.0, 21.53), RATE) == [(49.0, "quoted")]
    # DISH DBS, 2026-03-20: "10" with 9.986% is ten points; the leg with 23.614% is the same quote
    dish = C.mark_day([gsbs(10.0, 9.986, name="DISH DBS Corp", day="2026-03-20"), gsbs(10.0, 23.614, name="DISH DBS Corp", day="2026-03-20")])
    assert dish[1]["points_as"] == (10.0, 500.0) and 700 < C.candidate_spreads(dish[1], RATE)[0][0] < 900


def test_v200_a_spike_that_came_back_was_never_the_level():
    """South Africa at 5,224 bp for two days between 121 and 121; Heidelberg Materials at 1,250 for three days; the United
    States at 5,656 for one.  Each was a strong or repeated day, so the unrepeated-jump rule let it stand."""
    def rows(levels, strong=True):
        return [("2026-08-%02d" % (i + 1), {"s": v, "n": 3, "n5": 3, "np": 3 if strong else 1, "q": 0}) for i, v in enumerate(levels)]
    za = rows([118, 121, 120, 123, 121, 119, 5223.7, 4410.6, 121, 124, 120])
    assert C.unconfirmed_days(za) == {"2026-08-07", "2026-08-08"}
    hm = rows([56, 57, 55, 59, 56, 58, 1282, 1224, 1233, 52, 54, 52])
    assert C.unconfirmed_days(hm) == {"2026-08-07", "2026-08-08", "2026-08-09"}
    hertz = rows([4100, 3900, 4300, 4000, 4200, 49.0, 3792, 4813, 5308])
    assert C.unconfirmed_days(hertz) == {"2026-08-06"}
    # a name that leaves and stays is a move, not a spike - and the newest days are left alone until later days can say
    moved = rows([120, 121, 119, 122, 120, 121, 1500, 1620, 1580, 1700, 1650])
    assert C.unconfirmed_days(moved) == set()
    fresh = rows([120, 121, 119, 122, 120, 121, 5200])
    assert C.unconfirmed_days(fresh) == set(), "a strong newest day stands until the tape says otherwise"
    long_stay = rows([120, 121, 119, 122, 120, 121, 5200, 5100, 5300, 5250, 120, 121])
    assert C.unconfirmed_days(long_stay) == set(), "four days is not a spike"
    usa = [("2025-%02d-10" % m, {"s": v, "n": 1, "n5": 1, "np": 1, "q": 0}) for m, v in ((4, 60.0), (5, 60.0), (9, 43.1), (10, 42.1), (11, 38.2))] + \
          [("2026-%02d-10" % m, {"s": v, "n": 1, "n5": 1, "np": 1, "q": 0}) for m, v in ((1, 31.7), (4, 36.8), (8, 38.4), (9, 5656.25), (10, 43.75), (11, 48.24))]
    assert "2026-09-10" in C.unconfirmed_days(usa), "a sovereign that prints a dozen times a year: the day at 5,656 bp between 38 and 44"
    m = C.measure_entity("X", dict(za), "2026-08-11")
    assert m["hi_1y_bp"] < 130 and m["spread_bp"] == 120, "the range of its year no longer runs to 5,224"


def test_v200_a_portfolio_booked_at_one_flat_price_is_nobodys_level():
    """2026-09-21..29: CSX, Northrop Grumman, Allstate and Quest each carried the same sixteen prints - one upfront, the
    upfront of a 52 bp name - while CSX traded at 15.  1.6 published Quest at 52 to 56 bp for a week; a dry run of 2.0
    froze CSX there."""
    names = ["CSX Corp", "Northrop Grumman Corporation", "The Allstate Corporation", "Quest Diagnostics Incorporated"]
    flat = [derived_row(52.0, day="2026-09-21", **{"Underlying Asset Name": n, "UPI FISN": "NA/CDS Corp SN Sr", "Unique Product Identifier": "QZ%08d" % i}) for i, n in enumerate(names) for _ in range(3)]
    own = derived_row(15.0, day="2026-09-21", **{"Underlying Asset Name": "CSX Corp", "UPI FISN": "NA/CDS Corp SN Sr", "Unique Product Identifier": "QZ%08d" % 0})
    ts = [x for x in (C.normalize_trade(r, "sec") for r in flat + [own, dict(own)]) if x]
    emap = C.entity_map(ts, {})
    agg = C.aggregate_day(ts, emap, RATE, {"CSX": (15.5, "firm", "trailing")})
    csx = agg["entities"]["CSX"]
    assert csx["basis"]["package"] == 3 and abs(csx["spread_5y"] - 15.0) < 0.6, csx                   # its own two prints are its level
    assert all(agg["entities"][k]["spread_5y"] is None and agg["entities"][k]["cand"] is None for k in agg["entities"] if k != "CSX"), "a flat price is nobody's level, and no branch to anchor on later"
    three = [x for x in (C.normalize_trade(r, "sec") for r in flat[:9]) if x]                         # three names only: could be a coincidence, left alone
    agg3 = C.aggregate_day(three, C.entity_map(three, {}), RATE, {})
    assert all(m["basis"]["package"] == 0 for m in agg3["entities"].values())
    # a confirmed quote stands against a wrong anchor when the cash fits it to the cent (CSX's own quotes were being thrown out)
    q15 = C.normalize_trade(row(**{"Fixed rate-Leg 1": "", "Spread-Leg 1": "0.0015", "Spread notation-Leg 1": "3", "Underlying Asset Name": "CSX Corp", "UPI FISN": "NA/CDS Corp SN Sr",
                                   "Other payment amount": "%.2f" % (abs(C.clean_upfront_pct(15.0, 100.0, 5.2, RATE) - C.accrued_pct(100.0, C.date(2026, 10, 7))) / 100.0 * 5_000_000), "Other payment type": "UFRO"}), "sec")
    assert C.quote_status(q15, RATE) == "confirmed" and C._quote_fit(q15, RATE) < 0.05
    assert C.candidate_spreads(q15, RATE, 56.0) == [(15.0, "quoted")], "far from a wrong anchor, a quote its own cash confirms exactly is still the quote"


def test_v200_a_quote_in_the_wrong_unit_is_repaired_and_never_believed_alone():
    """The audit: history points at 1.8 million bp (Telecom Italia) and 70 million (Hertz).  One platform writes 57 under the
    decimal notation and 0.49 under basis points."""
    assert C.quoted_bp_of({"Spread-Leg 1": "57", "Spread notation-Leg 1": "3"}) == (57.0, "bp_under_decimal")
    assert C.quoted_bp_of({"Spread-Leg 1": "0.0057", "Spread notation-Leg 1": "3"}) == (57.0, None)
    assert C.quoted_bp_of({"Spread-Leg 1": "0.49", "Spread notation-Leg 1": "4"}) == (49.0, "percent_under_bp")
    assert C.quoted_bp_of({"Spread-Leg 1": "49", "Spread notation-Leg 1": "4"}) == (49.0, None)
    assert C.quoted_bp_of({"Spread-Leg 1": "70498.4", "Spread notation-Leg 1": "3"}) == (None, None), "nothing a credit trades at in either unit"
    t = C.normalize_trade(row(**{"Spread-Leg 1": "57", "Spread notation-Leg 1": "3", "Underlying Asset Name": "NEXT plc", "UPI FISN": "NA/CDS Corp SN Sr"}), "sec")
    assert t["quoted_bp"] == 57.0 and t["quote_how"] == "bp_under_decimal"
    assert C.candidate_spreads(t, RATE) == [], "with nothing beside it a repaired quote is not a level"
    assert C.candidate_spreads(t, RATE, 48.0) == [(57.0, "quoted")] and C.candidate_spreads(t, RATE, 400.0) == []
    wild = C.normalize_trade(quoted_row(20000.0), "sec")
    assert C.quote_status(wild, RATE) == "reject", "a quote past the ceiling is not a level"
    assert "quote_how" in L.TRADE_COLS, "the repair is kept with the print, so a day measured again from the warehouse reads it the same way"


def test_v200_an_old_firm_level_that_cannot_pick_a_branch_does_not_end_the_search():
    """The third dry run lost Grifols and NEXT: each had a quoted day months old, the name had moved since, and the old
    level sat between its two branches - while its own newer levels, which could tell them apart, were never asked.
    HSBC was lost the same way to its bonds (105 bp, between 48 and 155)."""
    def bank_with(rows):
        return {"series": {"GRIFOLS": rows}, "meta": {"GRIFOLS": {"kind": "corp", "ccy": {"EUR": 9}}}, "days": sorted(rows)}
    firm = {"n": 2, "n5": 2, "np": 2, "s": 400.0, "sb": "quoted", "q": 2, "aq": "firm", "as": "quoted"}
    carried = {"n": 1, "n5": 1, "np": 1, "s": 255.0, "sb": "derived", "q": 0, "aq": "inferred", "as": "trailing"}
    unresolved = {"n": 1, "n5": 1, "np": 0, "s": None, "q": 0, "amb": 1, "cd": {"500": [725.8, 255.0]}}
    rows = {"2026-05-0%d" % i: dict(firm) for i in range(4, 9)}
    rows.update({"2026-09-%02d" % i: dict(carried, s=250.0 + i) for i in range(1, 9)})
    rows["2026-10-06"] = dict(unresolved)
    got = C.anchors_from_bank(bank_with(rows), "2026-10-20")["GRIFOLS"]
    assert got[2] == "trailing_long" and got[1] == "inferred" and 250 < got[0] < 262, "its own newer levels pick the branch the 400 could not: %s" % (got,)
    only_firm = {d: r for d, r in rows.items() if d < "2026-09"}
    only_firm["2026-10-06"] = dict(unresolved)
    assert C.anchors_from_bank(bank_with(only_firm), "2026-10-20")["GRIFOLS"] == (400.0, "firm", "trailing_long"), "with nothing better the old firm level is still what the day gets"
    far = dict(rows)
    far["2026-10-06"] = dict(unresolved, cd={"500": [1200.0, 395.0]})
    assert C.anchors_from_bank(bank_with(far), "2026-10-20")["GRIFOLS"] == (400.0, "firm", "trailing_long"), "a firm level that does tell the branches apart still comes first"
    # bonds between the branches yield to the name's own carried level instead of freezing it
    hsbc = {"2026-09-%02d" % i: dict(carried, s=47.0 + i * 0.1) for i in range(1, 6)}
    hsbc["2026-10-06"] = {"n": 1, "n5": 1, "np": 0, "s": None, "q": 0, "amb": 1, "cd": {"100": [155.0, 48.0]}}
    b2 = {"series": {"HSBC": hsbc}, "meta": {"HSBC": {"kind": "corp", "ccy": {"EUR": 9}}}, "days": sorted(hsbc)}
    got = C.anchors_from_bank(b2, "2026-10-20", external={"HSBC": (105.3, "bonds")})["HSBC"]
    assert got[2] == "trailing_long" and 47 < got[0] < 48, got


def test_v200_option_expiries_and_strikes_off_the_ladder_are_set_aside_not_drawn():
    bank = _bank()
    bank["census"] = {"2026-11-20": {"cls": {"option": [9, 0, 0], "index": [60, 0, 0]}, "fam": {}, "str": {}, "how": {}, "opt": [
        ["ITRAXX EUROPE", "2026-12-16", 65.0, "spread", 300.0, 0, "EUR"], ["ITRAXX EUROPE", "2026-12-16", 80.0, "spread", 100.0, 0, "EUR"],
        ["ITRAXX EUROPE", "2026-11-18", 60.0, "spread", 50.0, 0, "EUR"],            # an expiry already past on the as-of day
        ["ITRAXX EUROPE", "2031-12-20", 70.0, "spread", 200.0, 0, "EUR"],           # the index's maturity, not an option expiry
        ["ITRAXX EUROPE", "2026-12-16", 287.5, "spread", 50.0, 0, "EUR"],           # a Crossover strike filed under the 100 bp coupon
        ["CDX.NA.HY", "2026-12-16", 104.0, "price", 100.0, 0, "USD"], ["CDX.NA.HY", "2026-12-16", 60.0, "price", 10.0, 0, "USD"]]}}
    o = C.options_block(bank, "2026-11-24", {"ITRAXX EUROPE": {"spread_bp": 64.6}, "CDX.NA.HY": {"spread_bp": 379.0, "price": 104.8}})
    eu = [f for f in o["families"] if f["family"] == "ITRAXX EUROPE"][0]
    assert [r["strike"] for r in eu["ladder"]] == [60.0, 65.0, 70.0, 80.0] and [r["strike"] for r in eu["off_ladder"]] == [287.5]
    assert [(e["expiry"], e["expired"]) for e in eu["expiries"]] == [("2026-11-18", True), ("2026-12-16", False)]
    assert eu["expiry_not_given"] == {"n": 1, "notional_mm": 200.0} and eu["share_struck_wider_pct"] == round(100.0 * 600.0 / 650.0, 1)
    hy = [f for f in o["families"] if f["family"] == "CDX.NA.HY"][0]
    assert [r["strike"] for r in hy["ladder"]] == [104.0] and [r["strike"] for r in hy["off_ladder"]] == [60.0], hy
    assert o["n_all"] == 9 and o["n_other_underlier"] == 2


def test_v200_a_home_listing_is_matched_by_name_in_the_right_part_of_the_world():
    import gzip as _gz
    import io as _io
    head = "symbol,companyName,exchange,country,currency,isEtf,isFund,isActivelyTrading,isAdr,averageVolume,price,sector\n"
    part0 = head + "\n".join([
        "7794.T,EDP Corporation,JPX,JP,JPY,false,false,true,false,90000,900,Technology",          # the same name in Tokyo: another company
        "EDP.LS,\"EDP, S.A.\",LIS,PT,EUR,false,false,true,false,5000000,4,Utilities",
        "SIEMENS.NS,Siemens Limited,NSE,IN,INR,false,false,true,false,900000,3000,Industrials",
        "SIE.DE,Siemens AG,XETRA,DE,EUR,false,false,true,false,1000000,180,Industrials",
        "ROG,Rogers Corporation,NYSE,US,USD,false,false,true,false,100000,100,Technology",
        "HSBA.L,HSBC Holdings plc,LSE,GB,GBp,false,false,true,false,20000000,800,Financial Services",
        "HSBC,HSBC Holdings plc,NYSE,GB,USD,false,false,true,true,2000000,45,Financial Services",
        "0235.HK,CSC Holdings Limited,HKSE,HK,HKD,false,false,true,false,100000,1,Financial Services",
        "SPY,SPDR S&P 500 ETF Trust,AMEX,US,USD,true,false,true,false,90000000,600,",
    ]) + "\n"
    part1 = head + "T,AT&T Inc.,NYSE,US,USD,false,false,true,false,30000000,25,Communication Services\n"

    class FakeS3:
        def get_paginator(self, _):
            class P:
                def paginate(self, **kw):
                    return [{"Contents": [{"Key": L.PROFILE_PREFIX + "part-0/daily/2026-11-24.csv.gz"}, {"Key": L.PROFILE_PREFIX + "part-1/daily/2026-11-24.csv.gz"},
                                          {"Key": L.PROFILE_PREFIX + "part-0/daily/2026-11-25.csv.gz"}]}]      # the 25th has one part of two: not complete
            return P()

        def get_object(self, Bucket, Key):
            assert "2026-11-24" in Key, "the newest day with every part is read, not a half-posted one"
            return {"Body": _io.BytesIO(_gz.compress((part0 if "part-0" in Key else part1).encode()))}

    bank = _bank()
    bank["meta"] = {"EDP": {"kind": "corp", "raw": "Edp,S.A.", "ccy": {"EUR": 9}}, "SIEMENS AKTIENGESELLSCHAFT": {"kind": "corp", "raw": "Siemens Aktiengesellschaft", "ccy": {"EUR": 5}},
                    "ROGERS": {"kind": "corp", "raw": "Rogers Group,Inc.", "ccy": {"USD": 2}}, "HSBC": {"kind": "corp", "raw": "HSBC HOLDINGS plc", "ccy": {"EUR": 9, "USD": 1}},
                    "SUB:HSBC": {"kind": "sub", "raw": "HSBC HOLDINGS plc", "ccy": {"EUR": 3}}, "CSC": {"kind": "corp", "raw": "CSC HOLDINGS LLC", "ccy": {"USD": 7}},
                    "AT & T": {"kind": "corp", "raw": "AT&T Inc.", "ccy": {"USD": 9}}, "FEDERATIVE REPUBLIC OF BRAZIL": {"kind": "sov", "raw": "Brazil", "ccy": {"USD": 9}}}
    old = L.s3
    L.s3 = FakeS3()
    try:
        names, status = L.load_listings(bank, "2026-11-25")
    finally:
        L.s3 = old
    assert names["EDP"]["symbol"] == "EDP.LS" and names["SIEMENS AKTIENGESELLSCHAFT"]["symbol"] == "SIE.DE", names
    assert names["HSBC"]["symbol"] == names["SUB:HSBC"]["symbol"] == "HSBA.L", "the home exchange outranks the depositary receipt"
    assert "ROGERS" not in names and "FEDERATIVE REPUBLIC OF BRAZIL" not in names and names["AT & T"]["symbol"] == "T"
    assert names["CSC"]["symbol"] == "0235.HK", "a dollar name may be listed anywhere - which is why a reviewed tie outranks it (below)"
    assert bank["listed"]["profiles_of"] == "2026-11-24" and "of 6 corporate names" in status
    again, st2 = L.load_listings(bank, "2026-11-27")
    assert again is bank["listed"]["names"] and st2.startswith("kept"), "matched once a week, not every run"


def test_v200_a_dry_run_writes_nothing_and_the_days_prints_round_trip():
    puts = []

    class FakeS3:
        def put_object(self, **kw):
            puts.append(kw["Key"])

    old_s3, old_dry = L.s3, L.DRY
    L.s3 = FakeS3()
    try:
        L.DRY = True
        assert L._put_json("data/x.json", {"a": 1}) > 0 and puts == []
        L.DRY = False
        L._put_json("data/x.json", {"a": 1})
        assert puts == ["data/x.json"]
        trades = [C.normalize_trade(quoted_row(112, **CORP), "sec"), C.normalize_trade(derived_row(150, **CORP, **{"Fixed rate-Leg 1": "1"}), "sec")]
        L.DAY_RECORDS["2026-10-07"] = [C.print_record(quoted_row(112, **CORP))]
        doc = L.prints_doc("2026-10-07", trades, L.DAY_RECORDS["2026-10-07"])
        back = L.trades_of(json.loads(json.dumps(doc)))
        assert [C.candidate_spreads(dict(t), RATE) for t in back] == [C.candidate_spreads(dict(t), RATE) for t in trades]
        assert "Underlier" not in json.dumps(doc) and doc["records"][0][doc["record_cols"].index("cls")] == "corp"
    finally:
        L.s3, L.DRY = old_s3, old_dry
        L.DAY_RECORDS.clear()


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
