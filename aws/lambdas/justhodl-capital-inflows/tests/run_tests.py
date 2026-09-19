"""Aligned monthly transaction and missing-input regression tests."""
import importlib.util
import json
import sys
import types
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch
SOURCE=Path(__file__).resolve().parents[1]/"source"
sys.path.insert(0,str(SOURCE))
from tic_contract import roll, align, monthly_quality
with patch.dict(sys.modules,{"boto3":types.SimpleNamespace(client=lambda *a,**k:None),
                            "managed_secret":types.SimpleNamespace(managed_secret=lambda *a,**k:"TEST_ONLY")}):
    spec=importlib.util.spec_from_file_location("tic_engine_test",SOURCE/"lambda_function.py")
    engine=importlib.util.module_from_spec(spec);spec.loader.exec_module(engine)
NOW=datetime.now(timezone.utc)
MONTH=NOW.year*12+NOW.month-1-2
DATES=[f"{(MONTH-i)//12:04d}-{(MONTH-i)%12+1:02d}-01" for i in range(30)]
OBS=[(d,1000) for d in DATES]


def test_rolling_windows_require_consecutive_months_and_keep_zero():
    assert roll(OBS,12)==12
    assert roll(OBS[:4]+OBS[5:],12) is None
    assert roll(OBS[:1]*12,12) is None
    assert roll([(d,0) for d in DATES],12)==0
    assert roll([(DATES[0],float("nan"))],1) is None


def test_alignment_never_uses_another_month_as_the_current_leg():
    assert align(OBS[1:],DATES[0])==[]
    assert align(OBS,DATES[1])[0][0]==DATES[1]
    q=monthly_quality({"total":OBS,"abroad":OBS[1:]},DATES[0],NOW.isoformat(),NOW)
    assert q["status"]=="incomplete" and q["missing"]==["abroad"]


def run(missing=None):
    writes=[]
    def fred(sid,**kw):
        assert kw.get("vintage")==NOW.date().isoformat()
        if sid==missing:return []
        value=10000 if sid==engine.TOTAL_INTO_US else 4000 if sid.endswith("99990") else 6000 if sid.endswith("99991") else 2500
        return [(d,value) for d in DATES]
    with patch.object(engine,"fred",side_effect=fred),patch.object(engine,"S3",types.SimpleNamespace(put_object=lambda **kw:writes.append(kw))):
        assert engine._legacy_unvalidated_handler({},None)["statusCode"]==200
    return json.loads(writes[0]["Body"])


def test_handler_preserves_schema_and_aligns_holder_split():
    out=run()
    assert out["quality"]["status"]=="fresh" and out["holder_splits"]["lt_total"]["recon_gap_bn"]==0
    assert out["headline"]["net_cross_border_lt_12mo_b"]==90
    assert all(v["data_asof"]==out["data_asof"] for v in out["by_asset_class"].values())


def test_missing_outflow_is_null_and_expired_not_subtracted_as_zero():
    out=run(engine.US_ABROAD)
    assert out["headline"]["net_cross_border_lt_12mo_b"] is None
    assert out["regime"]=="UNAVAILABLE" and out["quality"]["status"]=="incomplete"
    out=run(engine.TOTAL_INTO_US)
    assert "headline" in out and "by_asset_class" in out and "history_12mo_rolling_b" in out
    assert out["headline"]["latest_month_b"] is None and out["quality"]["status"]=="unavailable"


if __name__=="__main__":
    tests=[f for n,f in list(globals().items()) if n.startswith("test_")]
    for f in tests:f()
    print(f"TIC integrity tests passed: {len(tests)}")
    import subprocess
    subprocess.run([sys.executable,str(Path(__file__).with_name('test_originals.py'))],check=True)
