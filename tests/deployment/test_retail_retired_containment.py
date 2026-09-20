"""Retired account-aware outputs stay preserved but outside public inspection."""
from pathlib import Path
import copy,runpy,sys
ROOT=Path(__file__).resolve().parents[2]
SCRIPT=next((ROOT/'aws/ops').glob('*/ops_5943_retail_retired_output_containment.py'))
ns=runpy.run_path(str(SCRIPT))
def test_retail_deny_preserves_existing_statements_and_same_account_iam():
    old={'Version':'2012-10-17','Statement':[{'Sid':'ExistingProtection','Effect':'Deny','Resource':['example']}]};saved=copy.deepcopy(old)
    result=ns['merged'](old);assert old==saved and result['Statement'][0]==old['Statement'][0]
    assert ns['merged'](result)==result
    deny=result['Statement'][-1];assert deny['Effect']=='Deny' and deny['Principal']=='*'
    assert set(deny['Action'])=={'s3:GetObject','s3:GetObjectVersion'}
    assert deny['Condition']=={'StringNotEquals':{'aws:PrincipalAccount':'857687956942'}}
    for key in ns['KEYS']:
        assert 'arn:aws:s3:::justhodl-dashboard-live/'+key in deny['Resource']
        assert 'arn:aws:s3:::justhodl-dashboard-live/history/archive/feed/'+key+'/*' in deny['Resource']
    assert not any('retail-sentiment.json' in key for key in deny['Resource'])
def test_existing_conflicting_policy_or_oversized_merge_is_refused():
    for old in ({'Version':'2012-10-17','Statement':[{'Sid':ns['SID'],'Effect':'Allow'}]},
                {'Version':'2012-10-17','Statement':[{'Sid':'x'*21000}]},{}):
        try:ns['merged'](old)
        except ValueError:continue
        raise AssertionError('Unreviewed policy should not be written')

def test_aws_policy_normalization_preserves_exact_security_semantics():
    original={'Version':'2012-10-17','Statement':[ns['statement']()]}
    normalized=copy.deepcopy(original);row=normalized['Statement'][0]
    row['Principal']={'AWS':'*'};row['Action'].reverse();row['Resource'].reverse()
    row['Condition']['StringNotEquals']['aws:PrincipalAccount']=['857687956942']
    assert ns['policies_equal'](original,normalized)
    assert ns['merged'](normalized)==normalized
    row['Condition']['StringNotEquals']['aws:PrincipalAccount'].append('another-account')
    assert not ns['policies_equal'](original,normalized)
    try:ns['merged'](normalized)
    except ValueError:pass
    else:raise AssertionError('Additional account must not bypass protection')
def test_inspector_excludes_retired_state_and_uses_native_constant_scope():
    sys.path.insert(0,str(ROOT/'scripts'));import build_page_data_contracts as builder
    for key in ns['KEYS']:assert not builder.public_key(key)
    assert builder.public_key('data/retail-sentiment.json')
    import json
    role=json.loads((ROOT/'config/page-role-overrides.json').read_text(encoding='utf-8'))['pages']['retail/index.html']
    engines={'justhodl-retail-sentiment':{'keys':['data/retail-sentiment.json',*ns['KEYS']]}}
    assert builder.validated_primary_scopes(role,engines,ROOT,'retail/index.html')=={'justhodl-retail-sentiment':['data/retail-sentiment.json']}
