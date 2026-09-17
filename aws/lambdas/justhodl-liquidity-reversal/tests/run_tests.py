"""Exercise real publication without network or a fabricated liquidity dial."""
import importlib.util
import json
import sys
import types
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
root=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(root/'shared'),str(root/'shared/tests')]
from pd_fails_context_tests import run
run()
with patch.dict(sys.modules, {'boto3':types.SimpleNamespace(client=lambda *a,**k:None),
    'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:'')}):
    spec=importlib.util.spec_from_file_location('reversal_test',root/'lambdas/justhodl-liquidity-reversal/source/lambda_function.py')
    e=importlib.util.module_from_spec(spec);spec.loader.exec_module(e)
writes={}
fake=types.SimpleNamespace(put_object=lambda **kw:writes.update({kw['Key']:json.loads(kw['Body'])}))
with patch.object(e,'s3',fake),patch.object(e,'s3_json',return_value={}):
    result=e.lambda_handler({},None)
doc=writes[e.OUT_KEY]
assert doc['generated_at']==doc['as_of'] and datetime.fromisoformat(doc['generated_at']).tzinfo
assert doc['rows']==[] and doc['liquidity']['trend_score'] is None
assert doc['liquidity']['reversal_score'] is None
assert doc['pd_settlement_fails']['scope_id']=='treasury_incl_tips'
assert doc['pd_settlement_fails']['quality']['status']=='unavailable'
assert doc['pd_settlement_fails']['combined_bn'] is None
assert not json.loads(result['body'])['ok']
print('Liquidity reversal publication, dates and missing-source checks passed')
