"""Unit tests for the Wyckoff campaign annotator against live-shaped rows."""
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "aws", "shared"))
from wyckoff_campaign import annotate, campaign_of, project_board


def test_campaign_map():
    assert campaign_of("CLIMAX") == "BOTTOM"
    assert campaign_of("TESTING") == "BOTTOM"
    assert campaign_of("NO_RALLY") == "BOTTOM"
    assert campaign_of("NO_TEST_BREAKOUT") == "BOTTOM"
    assert campaign_of("ST_CONFIRMED") == "ACCUM"
    assert campaign_of("TRIGGERED") == "ACCUM"
    assert campaign_of("MARKUP") == "PUMP"
    assert campaign_of("COMPLETED") == "PUMP"
    assert campaign_of("FAILED") == "ABORT"
    assert campaign_of("STOPPED") == "ABORT"
    assert campaign_of("mystery") == "WATCH"


def test_sar_lps_ready():
    # Live 2026-09-24 TRIGGERED spring with quiet test — LPS entry.
    row = {
        "ticker": "SAR", "state": "TRIGGERED", "grade": "A", "score": 89.0,
        "sc_date": "2026-07-08", "sc_vol_x": 10.35, "sc_range_x": 3.09, "sc_decline_pct": -25.3,
        "ar_high": 20.05, "ar_rally_pct": 15.6, "st_date": "2026-09-14",
        "st_vol_ratio_sc": 0.12, "st_depth_class": "SPRING", "n_tests": 1,
        "approach_vol_slope": -0.005, "approach_range_x": 0.53,
        "trigger_date": "2026-09-16", "last": 17.33, "bars_in_state": 6,
    }
    out = annotate(row)
    assert out["campaign"] == "ACCUM"
    assert out["hinge"]["hinge"] is True
    assert out["hinge"]["springboard"] is True
    assert out["pump_start"]["ready"] is True
    assert out["pump_start"]["flag"] is False
    assert out["proofs_passed"] == 4  # P5 false: last still below AR high
    assert out["proofs"][3]["passed"] is True  # spring


def test_cdzi_accum_not_hinge():
    row = {
        "ticker": "CDZI", "state": "ST_CONFIRMED",
        "sc_date": "2026-07-17", "ar_high": 4.26, "st_date": "2026-09-18",
        "st_vol_ratio_sc": 0.49, "st_depth_class": "HIGHER_LOW",
        "approach_vol_slope": -0.237, "approach_range_x": 0.85, "last": 3.71,
    }
    out = annotate(row)
    assert out["campaign"] == "ACCUM"
    assert out["hinge"]["hinge"] is False  # 0.49 > 0.4
    assert out["pump_start"]["ready"] is False


def test_tds_pump_chase():
    row = {
        "ticker": "TDS", "state": "MARKUP", "st_vol_ratio_sc": 0.30,
        "st_depth_class": "HIGHER_LOW", "approach_vol_slope": -0.1,
        "approach_range_x": 0.6, "sc_date": "2026-01-01", "ar_high": 40,
        "st_date": "2026-02-01", "bars_in_state": 12, "last": 50,
        "event": {"trigger": {"sos_date": "2026-03-01"}},
    }
    out = annotate(row)
    assert out["campaign"] == "PUMP"
    assert out["pump_start"]["flag"] is True
    assert out["pump_start"]["chase"] is True
    assert out["proofs"][4]["passed"] is True


def test_project_board_counts():
    doc = {"engine": "justhodl-bottom", "version": "1.2.0", "board": [
        {"ticker": "A", "state": "CLIMAX", "sc_date": "2026-01-01"},
        {"ticker": "B", "state": "TRIGGERED", "sc_date": "2026-01-01", "ar_high": 2,
         "st_date": "2026-02-01", "st_vol_ratio_sc": 0.2, "st_depth_class": "SPRING",
         "approach_vol_slope": -0.1, "approach_range_x": 0.5},
        {"ticker": "C", "state": "MARKUP", "sc_date": "2026-01-01", "ar_high": 2,
         "bars_in_state": 8},
    ]}
    out = project_board(doc)
    assert out["counts"]["BOTTOM"] == 1
    assert out["counts"]["ACCUM"] == 1
    assert out["counts"]["PUMP"] == 1
    assert out["hinge_n"] == 1
    assert out["pump_ready_n"] == 1


if __name__ == "__main__":
    test_campaign_map()
    test_sar_lps_ready()
    test_cdzi_accum_not_hinge()
    test_tds_pump_chase()
    test_project_board_counts()
    print("ok")
