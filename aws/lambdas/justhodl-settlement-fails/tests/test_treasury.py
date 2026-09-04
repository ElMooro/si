import math
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).parents[1]/"source"))
from treasury import normalized_treasury, series_stats, strict_json_dumps, sum_on_common_dates

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
