from copy import deepcopy
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import sys
SOURCE=Path(__file__).parents[1]/"source"
sys.path.insert(0,str(SOURCE))
from fusion_engine import build_output,dedupe_packets,graph_cycles  # noqa:E402
NOW=datetime(2026,9,4,20,0,tzinfo=timezone.utc)
POLICY=json.loads((SOURCE/"fusion-policy.v1.json").read_text())
SCHEMA=json.loads((SOURCE/"fusion-schema.v1.json").read_text())

def spec(sid,family,domain="risk",max_age=24):
    return {"id":sid,"artifact":f"data/{sid}.json","producer":f"justhodl-{sid}","domain":domain,"source_family":family,"evidence_level":"L1","role":"root_evidence","independence_eligible":True,"max_age_hours":max_age,"timestamp_paths":["generated_at"],"adapter":"credit","confidence":0.8,"depends_on":[],"contract":{"required":["composite"],"types":{"composite":"number"},"ranges":{"composite":[0,100]}}}

def registry(*sources):
    return {"schema_version":"1.0.0","output_id":"engine_fusion","dynamic_manifest_ingestion":False,"sources":list(sources),"known_graph_nodes":{}}

def build(reg,feeds,metas=None,subs=None):
    metas=metas or {k:{"error":None,"last_modified":NOW.isoformat()} for k in feeds}
    return build_output(reg,subs or {"subscriptions":{}},POLICY,SCHEMA,feeds,metas,NOW)

def test_cycle_and_output_self_dependency_are_rejected():
    reg=registry(spec("a","fa"),spec("b","fb")); reg["sources"][0]["depends_on"]=["b"]; reg["sources"][1]["depends_on"]=["a"]
    assert graph_cycles(reg)
    try: build(reg,{})
    except ValueError as exc: assert "cycle" in str(exc)
    else: raise AssertionError("cycle was accepted")

def test_stale_evidence_is_visible_but_no_op():
    reg=registry(spec("old","family",max_age=24)); ts=(NOW-timedelta(hours=25)).isoformat(); out=build(reg,{"old":{"generated_at":ts,"composite":90}})
    assert out["packets"]==[]
    assert out["inactive_packets"][0]["freshness"]=="STALE"
    assert out["inactive_packets"][0]["active"] is False
    assert out["vetoes"]==[]

def test_source_family_dedupe_keeps_one_root_vote():
    packets=[{"evidence_id":"a","source_id":"a","subject":"X","domain":"risk","source_family":"same","role":"root_evidence","confidence":0.7,"age_h":1},{"evidence_id":"b","source_id":"b","subject":"X","domain":"risk","source_family":"same","role":"synthesized_view","confidence":0.99,"age_h":1}]
    kept,dropped=dedupe_packets(packets)
    assert [p["evidence_id"] for p in kept]==["a"]
    assert dropped[0]["evidence_id"]=="b" and dropped[0]["kept_evidence_id"]=="a"

def test_disagreement_is_exposed_without_aggregate_verdict():
    a,b=spec("risk_on","family_a"),spec("risk_off","family_b")
    feeds={"risk_on":{"generated_at":NOW.isoformat(),"composite":20},"risk_off":{"generated_at":NOW.isoformat(),"composite":90}}
    out=build(registry(a,b),feeds)
    assert out["authoritative_verdict"] is None
    assert len(out["disagreements"])==1
    assert out["disagreements"][0]["resolution"]=="UNRESOLVED_VISIBLE_CONFLICT"

def test_malformed_nonfinite_packet_is_invalid_not_neutral():
    out=build(registry(spec("bad","family")),{"bad":{"generated_at":NOW.isoformat(),"composite":float("nan")}})
    assert out["packets"]==[]
    assert out["inactive_packets"][0]["freshness"]=="INVALID"
    assert out["inactive_packets"][0]["score"] is None

def test_veto_packet_can_never_loosen_risk():
    out=build(registry(spec("severe","family")),{"severe":{"generated_at":NOW.isoformat(),"composite":90}})
    assert len(out["vetoes"])==1
    assert out["vetoes"][0]["effect"]=="TIGHTEN_OR_VETO_ONLY"
    assert out["vetoes"][0]["may_loosen_risk"] is False

def test_l2_packet_is_not_counted_as_independent_root_evidence():
    source=spec("synth","family"); source.update(evidence_level="L2",role="synthesized_view",independence_eligible=False)
    out=build(registry(source),{"synth":{"generated_at":NOW.isoformat(),"composite":50}})
    assert out["coverage"]["independent_root_evidence"]==0
    assert out["coverage"]["synthesized_views"]==1

def test_declared_consumer_view_is_excluded_from_active_packets():
    source=spec("view","family"); source.update(evidence_level="L3",role="consumer_view",independence_eligible=False,exclude_from_scoring=True,adapter="excluded_view",contract={})
    out=build(registry(source),{"view":{"generated_at":NOW.isoformat(),"composite":90}})
    assert out["packets"]==[] and out["inactive_packets"][0]["detail"]["excluded"] is True

def test_source_that_depends_on_fusion_output_creates_a_cycle():
    source=spec("feedback","family")
    source["depends_on"]=["engine_fusion"]
    reg=registry(source)
    cycles=graph_cycles(reg)
    assert cycles
    assert any("engine_fusion" in cycle and "feedback" in cycle for cycle in cycles)
    try: build(reg,{"feedback":{"generated_at":NOW.isoformat(),"composite":50}})
    except ValueError as exc: assert "cycle" in str(exc)
    else: raise AssertionError("fusion feedback cycle was accepted")

def test_materially_future_evidence_is_invalid_and_inactive():
    future=(NOW+timedelta(hours=1)).isoformat()
    out=build(registry(spec("future","family")),{"future":{"generated_at":future,"composite":90}})
    assert out["packets"]==[]
    packet=out["inactive_packets"][0]
    assert packet["freshness"]=="INVALID"
    assert packet["age_h"] < 0
    assert "future" in packet["error"]
    assert out["vetoes"]==[]

def test_small_clock_skew_remains_fresh():
    future=(NOW+timedelta(minutes=4)).isoformat()
    out=build(registry(spec("skew","family")),{"skew":{"generated_at":future,"composite":20}})
    assert out["packets"][0]["freshness"]=="FRESH"
    assert out["packets"][0]["age_h"] < 0
