"""Run the canonical CISS producer/consumer contracts."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[3]/"shared/tests"))
from ciss_vintage_test_support import run
if __name__=="__main__":
    run()
    from pd_fails_context_tests import run as pd_checks
    pd_checks()
    import io, json, types
    from unittest.mock import patch
    from ciss_vintage_test_support import load, warehouse, TODAY
    e=load('justhodl-sovereign-stress');writes=[]
    source={'treasury':{'as_of':TODAY,'ftd_bn':0,'ftr_bn':2,'gross_bn':2,'quality':{'status':'fresh'}},
            'headline':{'as_of':TODAY,'ftd_bn':0,'ftr_bn':1,'combined_bn':1,'quality':{'status':'fresh'}}}
    client=types.SimpleNamespace(get_object=lambda **kw:{'Body':io.BytesIO(json.dumps(source).encode())},
        put_object=lambda **kw:writes.append(kw))
    with patch.object(e,'s3',client),patch.object(e,'read_existing',side_effect=lambda k:warehouse() if k=='data/ciss-stress.json' else {}), \
         patch.object(e,'wgb_country',return_value=None),patch.object(e,'fred',return_value=[]), \
         patch.object(e,'eurostat',return_value=[]),patch.object(e,'build_gssi',return_value=None):
        e.lambda_handler({'suppress_alerts':True},None)
    doc=json.loads(next(w['Body'] for w in writes if w['Key']==e.OUT_KEY))
    pd=doc['pd_settlement_fails']
    assert pd['combined_bn']==2 and pd['ftd_bn']==0 and pd['ust_ex_tips']['combined_bn']==1
    assert pd['calls_eligible'] is False and doc['most_stressed_sovereign']=='france'
    print('Sovereign settlement publication preserves separate scopes and canonical ranking')
