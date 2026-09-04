from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys

SOURCE=Path(__file__).parents[1]/"source"
sys.path.insert(0,str(SOURCE))
from risk_engine import build_output, validate_output  # noqa:E402

NOW=datetime(2026,9,4,20,0,tzinfo=timezone.utc)
REGISTRY=json.loads((SOURCE/"input_registry.json").read_text())

def base_inputs():
    ts=NOW.isoformat()
    feeds={
      "risk_gate":{"generated_at":ts,"posture":"RISK_ON","composite":1,"sizing_multiplier":0.8},
      "crisis":{"generated_at":ts,"defcon_level":4,"master_crisis_score":20,"components_available":4,"components":[{"available":True,"age_hours":1} for _ in range(4)]},
      "bond_warroom":{"generated_at":ts,"heartbeat":{"score":20,"regime":"CALM"},"equity_risk":{"score":20,"state":"CALM"},"eurodollar_shortage":{"score":20,"state":"CALM"},"panels":{"rates":[{"key":"UST10","last":4.0}]}},
      "eurodollar_stress":{"generated_at":ts,"composite_score":20,"severity":"CALM"},
      "credit_composite":{"generated_at":ts,"composite":20},
    }
    metas={s["id"]:{"last_modified":ts,"error":None} for s in REGISTRY["inputs"]}
    for spec in REGISTRY["inputs"]:
        if spec["id"] not in feeds: metas[spec["id"]]["error"]="missing optional fixture"
    return feeds,metas

def settlement(as_of="2026-09-03",regime="CALM",score=20,ftd=10,ftr=20):
    return {"generated_at":NOW.isoformat(),"treasury":{"scope":"US_TREASURY_INCLUDING_TIPS","as_of":as_of,"unit":"USD_bn_par","ftd_bn":ftd,"ftr_bn":ftr,"gross_bn":ftd+ftr,"ftd":[[as_of,ftd]],"ftr":[[as_of,ftr]],"gross":[[as_of,ftd+ftr]],"stats":{"ftd":{"latest":ftd,"z":0,"pctile":20},"ftr":{"latest":ftr,"z":0,"pctile":30},"gross":{"latest":ftd+ftr,"z":0,"pctile":25}},"regime":regime,"score":score,"components":[{"key":"ust_ex_tips"},{"key":"tips"}],"complete":True}}

def test_separate_ftd_ftr_combined_stats_regime_and_as_of_are_visible():
    feeds,metas=base_inputs(); feeds["settlement_fails"]=settlement(ftd=11,ftr=23); metas["settlement_fails"]["error"]=None
    out=build_output(REGISTRY,feeds,metas,NOW)
    card=next(c for c in out["domains"] if c["id"]=="settlement_fails")
    metrics={m["label"]:m["value"] for m in card["metrics"]}
    assert metrics["Fails to deliver"]==11
    assert metrics["Fails to receive"]==23
    assert metrics["Gross fails"]==34
    assert metrics["Gross z-score"]==0
    assert metrics["Gross percentile"]==25
    assert metrics["Observation as-of"]=="2026-09-03"
    assert card["state"]=="CALM"
    assert out["treasury_fails"]["scope"]=="US_TREASURY_INCLUDING_TIPS"
    assert out["treasury_fails"]["unit"]=="USD_bn_par"
    assert out["treasury_fails"]["ftd_bn"]==11
    assert out["treasury_fails"]["ftr_bn"]==23
    assert out["treasury_fails"]["gross_bn"]==34
    assert out["treasury_fails"]["stats"]["gross"]["pctile"]==25
    assert out["treasury_fails"]["status"]=="FRESH"
    assert 0 <= out["coverage"]["ratio"] <= 1
    assert out["coverage"]["fresh"] >= 6
    assert out["freshness"]["status"]=="DEGRADED"
    assert "Missing or stale critical evidence never counts as an all-clear." in out["plain_english"]

def test_weekly_settlement_freshness_uses_observation_not_daily_generation():
    feeds,metas=base_inputs(); stale=(NOW-timedelta(hours=241)).date().isoformat(); feeds["settlement_fails"]=settlement(as_of=stale,regime="CRISIS",score=99); metas["settlement_fails"]["error"]=None
    out=build_output(REGISTRY,feeds,metas,NOW)
    health=next(h for h in out["source_health"] if h["name"]=="settlement_fails")
    assert health["status"]=="STALE"
    assert health["freshness_basis"]=="weekly_observation"
    assert out["treasury_fails"]["status"]=="STALE"
    assert out["treasury_fails"]["ftd_bn"] is None
    assert out["policy"]["mode"]=="SELECTIVE_RISK_ON"
    assert not any("settlement fails" in r.lower() for r in out["hard_vetoes"])

def test_severe_fails_veto_and_tighten_capital():
    feeds,metas=base_inputs(); feeds["settlement_fails"]=settlement(regime="CRISIS",score=90); metas["settlement_fails"]["error"]=None
    out=build_output(REGISTRY,feeds,metas,NOW)
    assert out["policy"]["mode"]=="DEFENSIVE"
    assert out["policy"]["allows_new_entries"] is False
    assert out["exposure_cap_pct"]<=10
    assert out["capital_decision"]=="STAY IN CASH / SHORT-TERM TREASURIES"
    assert any("settlement fails" in r.lower() for r in out["hard_vetoes"])

def test_malformed_and_nonfinite_critical_data_fail_closed():
    feeds,metas=base_inputs(); feeds["credit_composite"]["composite"]=float("nan")
    out=build_output(REGISTRY,feeds,metas,NOW)
    health=next(h for h in out["source_health"] if h["name"]=="credit_composite")
    assert health["status"]=="INVALID"
    assert out["policy"]["mode"]=="DATA_HOLD"
    assert out["exposure_cap_pct"]==0
    validate_output(out)

def test_optional_malformed_data_does_not_fabricate_neutral_score():
    feeds,metas=base_inputs(); feeds["dollar_radar"]={"generated_at":NOW.isoformat(),"dollar_pressure":"NaN"}; metas["dollar_radar"]["error"]=None
    out=build_output(REGISTRY,feeds,metas,NOW)
    card=next(c for c in out["domains"] if c["id"]=="dollar_radar")
    assert card["status"]=="INVALID" and card["score"] is None

def test_independent_lenses_never_loosen_master_veto():
    feeds,metas=base_inputs(); feeds["risk_gate"].update(posture="RISK_OFF",composite=-1,sizing_multiplier=0.2); feeds["settlement_fails"]=settlement(); metas["settlement_fails"]["error"]=None
    out=build_output(REGISTRY,feeds,metas,NOW)
    assert out["policy"]["mode"]=="DEFENSIVE"
    assert out["exposure_cap_pct"]<=10
    assert out["capital_decision"]=="STAY IN CASH / SHORT-TERM TREASURIES"

def test_missing_critical_feed_fails_closed_only_by_documented_criticality():
    feeds,metas=base_inputs(); del feeds["risk_gate"]; metas["risk_gate"]["error"]="NoSuchKey"
    out=build_output(REGISTRY,feeds,metas,NOW)
    assert out["status"]=="DATA_HOLD" and out["policy"]["mode"]=="DATA_HOLD"

def test_materially_future_critical_timestamp_is_invalid_and_fails_closed():
    feeds,metas=base_inputs()
    feeds["risk_gate"]["generated_at"]=(NOW+timedelta(hours=1)).isoformat()
    out=build_output(REGISTRY,feeds,metas,NOW)
    health=next(h for h in out["source_health"] if h["name"]=="risk_gate")
    assert health["status"]=="INVALID"
    assert "future" in health["error"]
    assert health["age_h"] < 0
    assert out["status"]=="DATA_HOLD"
    assert out["policy"]["allows_new_entries"] is False

def test_small_clock_skew_does_not_invalidate_risk_source():
    feeds,metas=base_inputs()
    feeds["risk_gate"]["generated_at"]=(NOW+timedelta(minutes=4)).isoformat()
    out=build_output(REGISTRY,feeds,metas,NOW)
    health=next(h for h in out["source_health"] if h["name"]=="risk_gate")
    assert health["status"]=="FRESH"
    assert health["age_h"] < 0
