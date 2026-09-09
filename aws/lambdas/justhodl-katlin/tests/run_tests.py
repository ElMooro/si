"""justhodl-katlin -- war-room capital-authority tests (audit 2026-09-08 FR-01/FR-02).

Dependency-free: boto3 is stubbed with a fail-on-any-call client so no AWS or
network is touched. Runs in the deploy-lambdas preflight for this engine.
"""
from __future__ import annotations

import importlib.util
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE.parent / "source" / "lambda_function.py"
# shared modules (managed_secret etc.) are bundled into the Lambda zip; make them importable here too
sys.path.insert(0, str(HERE.parents[2] / "shared"))


class _NoAWS:
    def __getattr__(self, name):
        def _boom(*a, **k):
            raise AssertionError("AWS call attempted in unit test: %s" % name)
        return _boom


def _load():
    fake = types.ModuleType("boto3")
    fake.client = lambda *a, **k: _NoAWS()
    fake.resource = lambda *a, **k: _NoAWS()
    sys.modules["boto3"] = fake
    for name in ("botocore", "botocore.exceptions", "botocore.config"):
        m = types.ModuleType(name); sys.modules.setdefault(name, m)
    sys.modules["botocore.exceptions"].ClientError = Exception
    sys.modules["botocore.config"].Config = lambda *a, **k: None
    spec = importlib.util.spec_from_file_location("katlin_under_test", SRC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _iso(hours_ago):
    return (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat()


def _gate(posture="RISK_ON", sizing=1.0, hours_ago=2):
    return {"engine":"justhodl-risk-gate", "posture": posture, "sizing_multiplier": sizing, "generated_at": _iso(hours_ago)}


def _auth(cap=50, allows=True, mode="SELECTIVE", hours_ago=1, vetoes=None):
    ts = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return {"engine":"justhodl-khalid-risk", "schema_version":"1.0.0", "generated_at":ts.isoformat(), "expires_at":(ts+timedelta(hours=8)).isoformat(), "status":"OK",
            "capital_decision":"INVEST SELECTIVELY", "exposure_cap_pct":cap, "policy":{"mode":mode,"allows_new_entries":allows,"exposure_cap_pct":cap,"reasons":["test"]},
            "hard_vetoes":vetoes or [], "critical_failures":[], "source_health":[{"name":name,"critical":True,"status":"FRESH","as_of":ts.isoformat(),"max_age_h":sla} for name,sla in {"risk_gate":30,"crisis":8,"bond_warroom":84,"eurodollar_stress":30,"credit_composite":30}.items()]}



def test_authority_cap_binds_and_desk_opinion_is_kept_separately(mod):
    wr = mod.war_room({"risk_gate": _gate(), "khalid_risk": _auth(cap=50)})
    assert wr["local"]["posture"] == "FULL_RISK" and wr["local"]["exposure_cap_pct"] == 100, wr["local"]
    assert wr["exposure_cap_pct"] == 50, wr["exposure_cap_pct"]
    assert wr["posture"] == "SELECTIVE", wr["posture"]
    assert wr["entries_allowed"] is True
    assert wr["authority"]["status"] == "FRESH" and wr["authority"]["source"] == "justhodl-khalid-risk"


def test_authority_blocks_new_entries_regardless_of_desk_rank(mod):
    wr = mod.war_room({"risk_gate": _gate(), "khalid_risk": _auth(cap=50, allows=False, mode="SELECTIVE", vetoes=["credit composite STRESS"])})
    assert wr["entries_allowed"] is False
    assert wr["posture"] == "CASH_OR_TBILLS", wr["posture"]
    assert any(v.startswith("authority:") for v in wr["vetoes"]), wr["vetoes"]
    rows = [{"tier": "KATLIN_PRIME"}, {"tier": "READY"}, {"tier": "BASING"}]
    basket = mod.build_basket([dict(r, learned_excess_126s_pct=9, composite=80) for r in rows], wr)
    assert basket["core"] == [] and basket["cash_pct"] == 100.0


def test_ancient_gate_with_no_authority_is_a_data_hold_not_full_risk(mod):
    # the audit's reproduction: one RISK_ON gate dated 2000-01-01, everything else missing -> was FULL_RISK 100%
    wr = mod.war_room({"risk_gate": {"posture": "RISK_ON", "sizing_multiplier": 1.0, "generated_at": "2000-01-01T00:00:00+00:00"}})
    assert wr["posture"] == "DATA_HOLD", wr["posture"]
    assert wr["exposure_cap_pct"] == 0
    assert wr["entries_allowed"] is False
    assert wr["authority"]["status"] == "MISSING"
    assert wr["hold_reasons"], "hold reason must be explicit"


def test_stale_authority_is_a_data_hold_even_with_a_fresh_gate(mod):
    wr = mod.war_room({"risk_gate": _gate(sizing=1.0), "khalid_risk": _auth(cap=100, hours_ago=30)})
    assert wr["authority"]["status"] == "STALE"
    assert wr["posture"] == "DATA_HOLD" and wr["exposure_cap_pct"] == 0 and wr["entries_allowed"] is False
    assert any("STALE" in reason for reason in wr["hold_reasons"])


def test_zero_sizing_multiplier_is_a_zero_cap(mod):
    wr = mod.war_room({"risk_gate": _gate(posture="SEVERE", sizing=0.0), "khalid_risk": _auth(cap=50)})
    assert wr["raw_gate"]["cap_pct"] == 0
    assert wr["exposure_cap_pct"] == 0, wr["exposure_cap_pct"]
    assert wr["entries_allowed"] is False


def test_no_legs_is_a_data_hold_not_25_percent(mod):
    wr = mod.war_room({"khalid_risk": _auth(cap=50)})
    assert wr["local"]["posture"] == "UNKNOWN" and wr["local"]["exposure_cap_pct"] == 0
    assert wr["posture"] == "DATA_HOLD" and wr["exposure_cap_pct"] == 0


def test_missing_authority_never_increases_the_cap(mod):
    fresh = mod.war_room({"risk_gate": _gate(sizing=0.75), "khalid_risk": _auth(cap=100)})
    absent = mod.war_room({"risk_gate": _gate(sizing=0.75)})
    assert absent["exposure_cap_pct"] <= fresh["exposure_cap_pct"]


def test_future_invalid_and_stale_local_evidence_cannot_permit_capital(mod):
    bad_authorities=[_auth(hours_ago=-10000),dict(_auth(),schema_version="wrong"),dict(_auth(),status="INVALID")]
    for authority in bad_authorities:
        out=mod.war_room({"risk_gate":_gate(),"khalid_risk":authority})
        assert out["entries_allowed"] is False and out["exposure_cap_pct"]==0, out
    out=mod.war_room({"risk_gate":_gate(hours_ago=10000),"khalid_risk":_auth(cap=100)})
    assert out["posture"]=="DATA_HOLD" and out["exposure_cap_pct"]==0
    assert out["local"]["posture"]=="UNKNOWN", "stale raw gate cannot remain the sole research leg"


def test_permission_expiry_never_outlives_authority(mod):
    authority=_auth()
    out=mod.war_room({"risk_gate":_gate(),"khalid_risk":authority})
    assert datetime.fromisoformat(out["expires_at"]) <= datetime.fromisoformat(authority["expires_at"])
    assert out["authority"]["schema_version"]=="1.0.0"


def test_basket_redistribution_never_exceeds_name_or_total_cap(mod):
    rows=[{"ticker":"S%d"%i,"tier":"READY","asset_class":"stock","learned_excess_126s_pct":100 if i==0 else 2,"vol_ann_pct":25,"composite":80} for i in range(3)]
    out=mod.build_basket(rows,{"exposure_cap_pct":100,"entries_allowed":True,"posture":"FULL_RISK"})
    assert all(r["weight_pct"]<=10 for r in out["core"]), out
    assert sum(r["weight_pct"] for r in out["core"])==30 and out["cash_pct"]==70
    assert out["constraints_valid"]


def test_permission_refresh_preserves_research_age_and_uses_conditional_write(mod):
    import io,json
    class FakeS3:
        def __init__(self,research,conflict=False):
            self.research,self.conflict,self.writes=research,conflict,[]
        def get_object(self,**kw):
            data=self.research if kw["Key"]==mod.OUT_KEY else _gate() if kw["Key"]=="data/risk-gate.json" else _auth() if kw["Key"]=="data/khalid-risk.json" else {}
            return {"Body":io.BytesIO(json.dumps(data).encode()),"ETag":"version-one"}
        def put_object(self,**kw):
            assert kw.get("IfMatch")=="version-one", "refresh must not overwrite a concurrently rebuilt ranking"
            if self.conflict:
                exc=RuntimeError("concurrent research");exc.response={"Error":{"Code":"PreconditionFailed"}};raise exc
            self.writes.append(json.loads(kw["Body"]))
    research_at=_iso(1)
    research={"engine":mod.ENGINE,"generated_at":research_at,"session":datetime.now(timezone.utc).date().isoformat(),"picks":[]}
    old=mod.s3
    try:
        fake=FakeS3(research);mod.s3=fake
        result=mod.lambda_handler({"mode":"permission_refresh"})
        assert result["ok"] and result["research_generated_at"]==research_at
        assert fake.writes[0]["research_generated_at"]==research_at and fake.writes[0]["war_room"]["entries_allowed"]
        published = fake.writes[0]
        dry=FakeS3(research);mod.s3=dry
        validation=mod.lambda_handler({"mode":"permission_refresh","validate_only":True})
        assert validation["ok"] and validation["validation_only"] and validation["schema_version"]=="1.1" and validation["artifact_size_bytes"]>0
        assert dry.writes==[] and mod.VALIDATION_ONLY is False
        stale=dict(published,research_generated_at="2000-01-01T00:00:00Z")
        fake=FakeS3(stale);mod.s3=fake
        result=mod.lambda_handler({"mode":"permission_refresh"})
        assert result["research_status"]=="STALE" and fake.writes[0]["war_room"]["exposure_cap_pct"]==0
        assert fake.writes[0]["research_generated_at"]=="2000-01-01T00:00:00Z"
        fake=FakeS3(research,conflict=True);mod.s3=fake
        result=mod.lambda_handler({"mode":"permission_refresh"})
        assert result["status"]=="RESEARCH_CHANGED" and fake.writes==[]
    finally:
        mod.s3=old


def test_validate_only_suppresses_shared_cache_writer_and_restores_state(mod):
    original=mod._run_handler
    try:
        def compute(event,context):
            assert mod.VALIDATION_ONLY is True
            size=mod.s3_put_json("data/test-cache-never-written.json",{"a":1})
            assert size>0
            return {"ok":True}
        mod._run_handler=compute
        assert mod.lambda_handler({"mode":"validate_only"})["ok"]
        assert mod.VALIDATION_ONLY is False
        def fail(event,context):
            raise ValueError("validation failure")
        mod._run_handler=fail
        try:mod.lambda_handler({"mode":"validate_only"})
        except ValueError:pass
        else:raise AssertionError("failure must propagate")
        assert mod.VALIDATION_ONLY is False
    finally:
        mod._run_handler=original


if __name__ == "__main__":
    mod = _load()
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for name, fn in tests:
        fn(mod)
        print("ok", name)
    print("katlin war-room tests passed: %d" % len(tests))
