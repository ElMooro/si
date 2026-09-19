"""Exercise actual Compass publication, quality coverage and alert suppression."""
import importlib.util
import io
import json
import sys
import types
from pathlib import Path
from unittest.mock import patch
root = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(root/'shared'), str(root/'shared/tests')]
from pd_fails_context_tests import run
run()
with patch.dict(sys.modules, {
    'boto3': types.SimpleNamespace(client=lambda *a, **k: None),
    '_sentry_lite': types.SimpleNamespace(track_errors=lambda f: f),
    'wl_fusion': types.SimpleNamespace(load=lambda: {}, context=lambda d: None, divergences=lambda d: []),
}):
    spec = importlib.util.spec_from_file_location('compass_test', root/'lambdas/justhodl-alpha-compass/source/lambda_function.py')
    engine = importlib.util.module_from_spec(spec); spec.loader.exec_module(engine)
    writes = {}
    fake_s3 = types.SimpleNamespace(
        get_object=lambda **kw: {'Body': io.BytesIO(b'{}')},
        put_object=lambda **kw: writes.update({kw['Key']:json.loads(kw['Body'])}))
    def feed(key):
        if key == engine.OUT_KEY: return {'generated_at':'2026-09-16T00:00:00Z','top_calls':[{'subject':'old'}]}
        return {}
    with patch.object(engine,'s3',fake_s3), patch.object(engine,'safe_load',side_effect=feed), \
         patch.object(engine,'fuse_regime',return_value={'risk_multiplier':1,'label':'NEUTRAL','sources':[]}), \
         patch.object(engine,'backfill_entries',return_value=0), \
         patch.object(engine,'update_track_record',return_value=({'entries':[]},{'graded_this_run':0,'quotes_available':0})), \
         patch.object(engine,'run_deltas',return_value=({'entered':['test'],'dropped':[],'moves':[]},{})), \
         patch.object(engine,'send_telegram') as telegram:
        result = engine._legacy_unvalidated_handler({'suppress_alerts':True},None)
    assert result['statusCode']==200
    telegram.assert_not_called()
    doc = writes[engine.OUT_KEY]
    assert doc['schema_version']=='2.0' and doc['top_calls']==[]
    assert doc['quality']['status']=='partial' and len(doc['quality']['missing'])==12
    assert doc['pd_settlement_fails']['scope_id']=='treasury_incl_tips'
    assert doc['pd_settlement_fails']['combined_bn'] is None
    assert doc['pd_settlement_fails']['calls_eligible'] is False
    assert engine.HIST_KEY in writes
print('Alpha Compass publication and suppression checks passed')
from alpha_research_tests import run as run_research
run_research()
