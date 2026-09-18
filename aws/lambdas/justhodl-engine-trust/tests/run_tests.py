"""Trust permissions must survive stale and unverified upstream artifacts."""
import ast
import json
import sys
import time
import types
from datetime import datetime,timezone,timedelta
from pathlib import Path
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parents[2]/'shared'))
from outcome_integrity import finite,stamp


def run(scorecard,conditional=None):
    tree=ast.parse((HERE.parent/'source/lambda_function.py').read_text(encoding='utf-8'))
    output={}
    env=dict(json=json,time=time,datetime=datetime,timezone=timezone,finite=finite,stamp=stamp,
             VERSION='1.1.0',S3_BUCKET='fixture',SCORECARD_KEY='scorecard',OUT_KEY='trust',MIN_REGIME_N=5,WARMING_N=12,
             s3=types.SimpleNamespace(put_object=lambda **kw:output.update(json.loads(kw['Body']))))
    exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef)],type_ignores=[]),'trust','exec'),env)
    env['_read']=lambda key:scorecard if key=='scorecard' else conditional if key=='data/regime-conditional-trust.json' else {}
    env['current_regime']=lambda:'BULL'
    env['lambda_handler']({},None)
    return output


def row():
    return {'signal_type':'eng:fixture','n_scored':100,'status':'PROMOTED','performance_multiplier':1.25,
            'wilson_lb':.7,'alpha_status':'ALPHA_PROVEN','alpha_n':100,'by_regime':{'BULL':{'n':100,'wilson_lb':.8}}}


def test_legacy_boost_and_regime_overlay_cannot_create_authority():
    sc={'generated_at':datetime.now(timezone.utc).isoformat(),'scorecard':[row()]}
    doc=run(sc,{'engines':{'eng:fixture':{'current_regime_status':'PROVEN','current_regime_factor':9}}})
    e=doc['engines'][0]
    assert e['effective_trust']==1 and e['status']=='WARMING' and not e['promotion_eligible']
    assert not e['sizing_eligible'] and not doc['alpha_gate']['proven_boosted']


def test_stale_scorecard_cannot_grant_permissions_even_with_claimed_validation():
    r=row();r.update(promotion_eligible=True,alpha_validation_scope='OUT_OF_SAMPLE',validation_manifest_sha256='a'*64)
    sc={'generated_at':(datetime.now(timezone.utc)-timedelta(days=2)).isoformat(),'scorecard':[r],
        'integrity':{'contract':'outcome-lineage.v1','scan_complete':True}}
    doc=run(sc)
    assert not doc['integrity']['source_fresh'] and doc['engines'][0]['effective_trust']==1


def test_current_descriptive_scorecard_is_visible_without_sizing_authority():
    r=row();r.update(status='ACTIVE',promotion_eligible=False,n_quarantined=50,alpha_validation_scope='NOT_OUT_OF_SAMPLE')
    sc={'generated_at':datetime.now(timezone.utc).isoformat(),'scorecard':[r],
        'integrity':{'contract':'outcome-lineage.v1','scan_complete':True,
                     'price_evidence_contract':'provider-price-replay.v1','price_archive_verification_required':True}}
    doc=run(sc);assert doc['integrity']['scorecard_contract_verified']
    assert doc['engines'][0]['n_quarantined']==50 and doc['engines'][0]['effective_trust']==1


def test_structural_lineage_alone_cannot_grant_trust():
    r=row();r.update(promotion_eligible=True,alpha_validation_scope='OUT_OF_SAMPLE',validation_manifest_sha256='a'*64)
    sc={'generated_at':datetime.now(timezone.utc).isoformat(),'scorecard':[r],
        'integrity':{'contract':'outcome-lineage.v1','scan_complete':True}}
    doc=run(sc)
    assert not doc['integrity']['scorecard_contract_verified']
    assert not doc['engines'][0]['promotion_eligible'] and doc['engines'][0]['effective_trust']==1


if __name__=='__main__':
    tests=[(n,f) for n,f in sorted(globals().items()) if n.startswith('test_') and callable(f)]
    for n,f in tests:f();print('ok',n)
    print('Trust permission tests passed:',len(tests))
