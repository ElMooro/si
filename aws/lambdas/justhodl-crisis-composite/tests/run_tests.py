"""Verify canonical/alias parity and quiet maintenance invocations."""
import importlib.util
import json
import sys
import types
from pathlib import Path
from unittest.mock import patch
root=Path(__file__).resolve().parents[3]
with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:None),
    'wl_series':types.SimpleNamespace(block=lambda *a: {}),
    'wl_fusion':types.SimpleNamespace(block=lambda *a: {})}):
    spec=importlib.util.spec_from_file_location('crisis_test',root/'lambdas/justhodl-crisis-composite/source/lambda_function.py')
    e=importlib.util.module_from_spec(spec);spec.loader.exec_module(e)
    writes={}
    fake=types.SimpleNamespace(put_object=lambda **kw:writes.update({kw['Key']:kw['Body']}))
    def get(key):
        return {'snapshots':[{'defcon':1,'score':90}]} if key==e.S3_HISTORY_KEY else {}
    with patch.object(e,'s3',fake),patch.object(e,'get_s3_json',side_effect=get), \
         patch.object(e,'COMPONENTS',[('data/fixture.json',1,lambda _:20,'Fixture')]), \
         patch.object(e,'_massive_cross_asset',return_value={}),patch.object(e,'maybe_telegram') as telegram:
        result=e._legacy_unqualified_handler({'suppress_alerts':True},None)
    assert result['statusCode']==200
    assert writes['data/defcon.json']==writes[e.S3_KEY]
    assert json.loads(writes[e.S3_KEY])['master_crisis_score']==20
    assert len(json.loads(writes[e.S3_HISTORY_KEY])['snapshots'])==2
    telegram.assert_not_called()
print('Retained legacy DEFCON alias parity, score/history preservation and alert suppression passed')
import subprocess
subprocess.run([sys.executable,str(Path(__file__).with_name('test_native_research.py'))],check=True)
subprocess.run([sys.executable,str(Path(__file__).with_name('test_consumer_boundaries.py'))],check=True)
