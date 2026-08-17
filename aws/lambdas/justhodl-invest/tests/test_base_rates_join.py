"""ops 4819 -- attach_base_rates() unit tests (Fusion 1 consumer).

Proves the join is additive and NON-GATING: pre-existing pick fields
are byte-identical with or without the spine doc, quintile cells are
matched by their `q` field (never list position -- boom_score
doctrine), and absence of the spine yields honest None with rows
untouched (no invented odds).
"""
import copy

import lambda_function as lf


def _spine():
    # quintiles deliberately OUT OF ORDER to prove field-match
    return {
        "status": "LIVE", "as_of": "2026-08-17",
        "diag": {"feeds": {"ledger_weeks": 53}},
        "current_assignments": {
            "AAPL": {"b": "large", "q": 5, "dd": "0_to_-10",
                     "m6": 12.3},
            "PTC": {"b": "mid", "q": 2, "dd": "-20_to_-35",
                    "m6": -8.0},
        },
        "cohorts": {"26w": {
            "quintiles": [
                {"q": 5, "beat_pct": 41.6, "wilson_lb95_pct": 38.4,
                 "median_excess_pp": -7.2, "n": 901},
                {"q": 2, "beat_pct": 36.4, "wilson_lb95_pct": 33.3,
                 "median_excess_pp": -10.9, "n": 901},
                {"q": 1, "beat_pct": 32.7, "wilson_lb95_pct": 29.7,
                 "median_excess_pp": -23.5, "n": 900},
            ],
            "dd_bands": {"-20_to_-35": {"beat_pct": 42.3}},
        }},
    }


def _picks():
    return [
        {"ticker": "AAPL", "composite_score": 71.0,
         "vs_industry_etf": "PICK", "thesis": "t1"},
        {"ticker": "PTC", "composite_score": 55.0,
         "vs_industry_etf": "BUY_THE_ETF", "thesis": "t2"},
        {"ticker": "ZZZZ", "composite_score": 40.0,
         "vs_industry_etf": "BUY_THE_ETF", "thesis": "t3"},
        {"industry": "X", "status": "INSUFFICIENT_DATA"},
    ]


def test_field_matched_attach():
    picks = _picks()
    meta = lf.attach_base_rates(picks, _spine())
    o = picks[0]["base_rate_odds"]
    assert o["q"] == 5 and o["beat_spx_pct"] == 41.6
    assert o["lb95_pct"] == 38.4 and o["cohort_n"] == 901
    assert o["bucket"] == "large" and o["horizon"] == "26w"
    assert meta == {"as_of": "2026-08-17", "ledger_weeks": 53,
                    "picks_with_odds": 2}


def test_dd_band_cell_joined_when_present():
    picks = _picks()
    lf.attach_base_rates(picks, _spine())
    assert picks[1]["base_rate_odds"]["dd_beat_spx_pct"] == 42.3
    assert "dd_beat_spx_pct" not in picks[0]["base_rate_odds"]


def test_non_gating_identity():
    picks = _picks()
    before = copy.deepcopy(picks)
    lf.attach_base_rates(picks, _spine())
    for pre, post in zip(before, picks):
        for k, v in pre.items():
            assert post[k] == v  # every pre-existing field untouched


def test_unknown_ticker_and_rowless_dict_skipped():
    picks = _picks()
    lf.attach_base_rates(picks, _spine())
    assert "base_rate_odds" not in picks[2]
    assert "base_rate_odds" not in picks[3]


def test_absent_or_insufficient_spine_is_honest():
    picks = _picks()
    before = copy.deepcopy(picks)
    assert lf.attach_base_rates(picks, None) is None
    assert lf.attach_base_rates(
        picks, {"status": "INSUFFICIENT_DATA"}) is None
    assert picks == before


def test_missing_quintile_cell_still_attaches_assignment():
    sp = _spine()
    sp["cohorts"]["26w"]["quintiles"] = [
        c for c in sp["cohorts"]["26w"]["quintiles"] if c["q"] != 5]
    picks = _picks()
    lf.attach_base_rates(picks, sp)
    o = picks[0]["base_rate_odds"]
    assert o["q"] == 5 and "beat_spx_pct" not in o
