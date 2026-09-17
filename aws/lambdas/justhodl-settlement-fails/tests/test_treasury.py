import math
from datetime import datetime, timezone
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).parents[1]/"source"))
from treasury import (annotate_treasury, normalized_treasury, scope_quality,
                      series_stats, strict_json_dumps, sum_on_common_dates, weekly_quality)


def _now(value):
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def _treasury_classes():
    return [
        {"key": "ust_ex_tips", "ftd": [["2026-09-02", 86.03]], "ftr": [["2026-09-02", 87.26]]},
        {"key": "tips", "ftd": [["2026-09-02", 6.58]], "ftr": [["2026-09-02", 10.37]]},
    ]


def test_weekly_observation_is_not_stale_at_361_hours():
    q = weekly_quality("2026-09-02", "2026-09-16T21:30:00+00:00", now=_now("2026-09-17T01:00:00"))
    assert q["status"] == "fresh"
    assert q["freshness_basis"] == "weekly_observation" and q["frequency"] == "weekly"
    assert q["next_expected_publication_date"] == "2026-09-17"
    assert q["max_age_hours"] == 336


def test_weekly_observation_expires_after_next_thursday_plus_one_week():
    for moment, expected in [("2026-09-24T23:59:59", "fresh"), ("2026-09-25T00:00:00", "stale")]:
        q = weekly_quality("2026-09-02", "2026-09-24T21:30:00+00:00", now=_now(moment))
        assert q["status"] == expected


def test_old_publication_expires_even_when_observation_is_within_weekly_grace():
    q = weekly_quality("2026-09-02", "2026-09-03T00:00:00+00:00", now=_now("2026-09-17T01:00:00"))
    assert q["status"] == "stale"


def test_invalid_and_future_dates_do_not_get_fresh_status():
    for observed, published in [("bad", "2026-09-16T21:30:00Z"), ("2026-09-23", "2026-09-16T21:30:00Z"),
                                ("2026-09-02", "2026-09-18T21:30:00Z")]:
        assert weekly_quality(observed, published, now=_now("2026-09-17T01:00:00"))["status"] == "invalid"


def test_including_tips_narrative_score_and_percentile_share_scope():
    rows = _treasury_classes()
    t = annotate_treasury(normalized_treasury(rows), rows, "2026-09-16T21:30:00Z", now=_now("2026-09-17T01:00:00"))
    assert t["scope"] == "US_TREASURY_INCLUDING_TIPS"  # existing consumer contract
    assert t["scope_id"] == "treasury_incl_tips"
    assert t["gross_bn"] == 190.24
    assert t["stats"]["gross"]["latest"] == t["gross_bn"]
    assert "including TIPS" in t["narrative"] and "$190.2bn" in t["narrative"]
    assert "%.1f%%ile" % t["stats"]["gross"]["pctile"] in t["narrative"]
    assert t["score_0_100"] == t["score"]
    assert t["field_units"]["gross_bn"] == "usd_bn"
    assert "not unique par" in t["measurement_note"]
    assert t["quality"]["status"] == "fresh" and t["quality"]["missing"] == []


def test_missing_treasury_side_expires_classification_without_zero_fill():
    rows = _treasury_classes()
    rows[1]["ftr"] = []
    t = annotate_treasury(normalized_treasury(rows), rows, "2026-09-16T21:30:00Z", now=_now("2026-09-17T01:00:00"))
    assert t["quality"]["status"] == "incomplete"
    assert t["quality"]["missing"] == ["classes.tips.ftr"]
    assert t["gross_bn"] is None and t["regime"] == "UNKNOWN" and t["score"] is None


def test_failed_required_endpoint_is_unavailable_even_if_other_sides_exist():
    rows = _treasury_classes()
    rows[1]["ftr"] = []
    t = annotate_treasury(normalized_treasury(rows), rows, "2026-09-16T21:30:00Z", now=_now("2026-09-17T01:00:00"),
                         feed_status={"classes.tips.ftr": {"status": "unavailable"}})
    assert t["quality"]["status"] == "unavailable"
    assert t["regime"] == "UNKNOWN" and t["score_0_100"] is None


def test_masked_new_print_does_not_relabel_old_common_history_as_fresh():
    rows = _treasury_classes()
    q = scope_quality(rows, ("ust_ex_tips", "tips"), "2026-09-16T21:30:00Z", now=_now("2026-09-17T01:00:00"),
                      feed_status={"classes.tips.ftr": {"status": "incomplete", "observation_date": "2026-09-09"}})
    assert q["observation_date"] == "2026-09-02"
    assert q["status"] == "incomplete" and len(q["missing"]) == 4


def test_stale_treasury_preserves_dated_values_but_expires_the_call():
    rows = _treasury_classes()
    t = annotate_treasury(normalized_treasury(rows), rows, "2026-09-25T01:00:00Z", now=_now("2026-09-25T01:00:00"))
    assert t["gross_bn"] == 190.24 and t["as_of"] == "2026-09-02"
    assert t["quality"]["status"] == "stale"
    assert t["regime"] == "UNKNOWN" and t["score"] is None
    assert "stale" in t["narrative"] and "calm" not in t["narrative"].lower()


def test_observed_zero_is_preserved_and_never_treated_as_missing():
    rows = _treasury_classes()
    for row in rows:
        row["ftd"][0][1] = row["ftr"][0][1] = 0
    t = annotate_treasury(normalized_treasury(rows), rows, "2026-09-16T21:30:00Z", now=_now("2026-09-17T01:00:00"))
    assert t["gross_bn"] == 0 and t["quality"]["missing"] == []
    assert t["quality"]["status"] == "fresh"


def test_no_treasury_data_is_unavailable_not_calm():
    t = annotate_treasury(normalized_treasury([]), [], "2026-09-16T21:30:00Z", now=_now("2026-09-17T01:00:00"))
    assert t["quality"]["status"] == "unavailable" and len(t["quality"]["missing"]) == 4
    assert t["gross_bn"] is None and t["score"] is None and t["regime"] == "UNKNOWN"

def test_overflowed_normalized_treasury_is_invalid_and_does_not_crash_narrative():
    rows = _treasury_classes()
    for row in rows:
        row["ftd"][0][1] = row["ftr"][0][1] = 1e308
    t = annotate_treasury(normalized_treasury(rows), rows, "2026-09-16T21:30:00Z", now=_now("2026-09-17T01:00:00"))
    assert t["quality"]["status"] == "invalid"
    assert t["gross_bn"] is None and t["score"] is None and t["regime"] == "UNKNOWN"
    strict_json_dumps(t)


def test_treasury_scope_uses_ex_tips_plus_tips_common_dates_only():
    classes=[
      {"key":"ust_ex_tips","label":"ex","ftd":[["2026-08-20",10],["2026-08-27",20]],"ftr":[["2026-08-20",30],["2026-08-27",40]]},
      {"key":"tips","label":"tips","ftd":[["2026-08-27",2],["2026-09-03",999]],"ftr":[["2026-08-27",4],["2026-09-03",999]]},
      {"key":"corporate","label":"corp","ftd":[["2026-08-27",9000]],"ftr":[["2026-08-27",9000]]},
    ]
    out=normalized_treasury(classes)
    assert out["scope"]=="US_TREASURY_INCLUDING_TIPS"
    assert out["as_of"]=="2026-08-27"
    assert out["ftd_bn"]==22 and out["ftr_bn"]==44 and out["gross_bn"]==66
    assert out["ftd"]==[["2026-08-27",22.0]] and out["ftr"]==[["2026-08-27",44.0]]
    assert out["complete"] is True
    assert {c["key"] for c in out["components"]}=={"ust_ex_tips","tips"}

def test_missing_tips_side_is_incomplete_not_fabricated():
    out=normalized_treasury([{"key":"ust_ex_tips","ftd":[["2026-08-27",20]],"ftr":[["2026-08-27",40]]},{"key":"tips","ftd":[],"ftr":[]}])
    assert out["complete"] is False
    assert out["as_of"] is None and out["gross_bn"] is None
    assert out["regime"]=="UNKNOWN" and out["score"] is None

def test_overflowing_treasury_sum_is_dropped_instead_of_emitting_infinity():
    out=sum_on_common_dates(
        [["2026-08-27",1e308],["2026-09-03",10]],
        [["2026-08-27",1e308],["2026-09-03",20]],
    )
    assert out==[["2026-09-03",30.0]]
    assert all(math.isfinite(point[1]) for point in out)

def test_large_finite_treasury_stats_stay_finite():
    stats=series_stats([["2026-08-27",1e308],["2026-09-03",1e308]])
    for key in ("latest","mean","max","min","z","pctile","avg_52w"):
        assert math.isfinite(stats[key])

def test_normalized_treasury_overflow_fails_closed_and_remains_serializable():
    classes=[
      {"key":"ust_ex_tips","ftd":[["2026-09-03",1e308]],"ftr":[["2026-09-03",1e308]]},
      {"key":"tips","ftd":[["2026-09-03",1e308]],"ftr":[["2026-09-03",1e308]]},
    ]
    out=normalized_treasury(classes)
    assert out["complete"] is False
    assert out["as_of"] is None
    assert out["ftd"]==[] and out["ftr"]==[] and out["gross"]==[]
    strict_json_dumps(out)

def test_settlement_json_serialization_rejects_non_finite_numbers():
    for value in (float("nan"),float("inf"),float("-inf")):
        try:
            strict_json_dumps({"value":value})
        except ValueError:
            pass
        else:
            raise AssertionError("non-finite JSON value was serialized")
