"""Free producer: evidence, privacy, no providers, explicit WAIT on failures."""
import ast
import copy
import json
import sys
from pathlib import Path
root=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(root/'shared'))
from calls_free_brief import build
from calls_contract import make_snapshot, candidate_verb
at='2026-09-17T23:00:00+00:00'
q={'status':'fresh','observation_date':'2026-09-09'}
scope={'as_of':'2026-09-09','ftd_bn':0,'ftr_bn':2,'quality':q}
fixtures={
 'data/settlement-fails.json':{'treasury':{**scope,'gross_bn':2},'headline':{**scope,'ftd_bn':1,'combined_bn':3}},
 'data/ciss-stress.json':{'ea_composite':0,'ea_composite_date':'2026-09-16','quality':q},
 'data/risk-regime.json':{'risk_regime_score':5,'quality':{'status':'fresh'}},
 'data/market-extremes.json':{'scores':{'top_risk':70},'quality':{'status':'fresh'}},
}
reads=[]
def load(key):
    reads.append(key)
    d=copy.deepcopy(fixtures.get(key,{}))
    d.update(snapshot={'positions':['PRIVATE-CANARY']},narrative='PRIVATE-CANARY',notes='PRIVATE-CANARY')
    return d
out=build(load,at)
assert out['paid_api_calls']==0 and out['model'] is None and out['sizing_eligible'] is False
assert len(out['brief_md'])>120 and candidate_verb(out['brief_md'])=='WAIT'
assert 'PRIVATE-CANARY' not in json.dumps(out)
assert all(k.startswith('data/') and k not in ('data/ai-brief.json','data/brain.json') for k in reads)
rows={r['series_id']:r for r in out['evidence']}
assert rows['data/ciss-stress.json#ea_composite']['value']==0
assert rows['data/settlement-fails.json#treasury.ftd_bn']['value']==0
assert rows['data/settlement-fails.json#headline.ftd_bn']['value']==1
assert rows['data/risk-regime.json#risk_regime_score']['quality_status']=='unavailable'
assert all(r['calls_eligible'] is False for r in out['evidence'])
assert out['coverage']['eligible_votes']==0
empty=build(lambda _: {},at)
for output in [empty,{**out,'brief_md':'x'*54}]:
    row=make_snapshot({'as_of':at},output)
    assert row['call_verb']=='WAIT' and row['decision_status']=='ERROR' and not row['sizing_eligible']
assert make_snapshot({'as_of':at},out)['decision_status']=='ABSTAIN'
source=(root/'lambdas/justhodl-ai-brief/source/lambda_function.py').read_text(encoding='utf-8')
tree=ast.parse(source)
handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
names={n.id for n in ast.walk(handler) if isinstance(n,ast.Name)}
assert not names & {'get_anthropic_key','call_anthropic','anthropic_shim','llm_router'}
assert 'import anthropic_shim' not in source
print('Free brief checks passed: real evidence, zero/null, scope isolation, privacy whitelist, WAIT, no paid provider path')
