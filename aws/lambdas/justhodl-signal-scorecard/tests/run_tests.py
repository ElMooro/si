"""Price identity, lineage and statistical permission regressions; no network/AWS."""
import copy
import importlib.util
import io
import json
import math
import sys
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / 'shared'))
from instrument_identity import resolve_instrument
from outcome_integrity import assess_outcome, finite, pair_return


def mark(price, day, identity='equity:US:AAA'):
    return {'price': price, 'observed_at': f'2026-09-{day:02d}T20:00:00+00:00', 'provider': 'fixture',
            'instrument_id': identity, 'currency': 'USD', 'evidence_sha256': 'a'*64,
            'adjustment_basis': 'split_adjusted_price', 'adjustment_vintage': 'same-fixture-snapshot'}


def outcome():
    return {'signal_type': 'test-engine', 'predicted_dir': 'UP', 'prediction_origin': 'explicit_direction',
            'logged_at': '2026-09-01T12:00:00+00:00', 'window_key': 'day_1',
            'outcome': {'lineage_contract': 'outcome-lineage.v1', 'return_pct': 20,
                        'entry_marks': {'asset': mark(100,2)}, 'marks': {'asset': mark(120,3)}}}


def load(rows=None, allow_fixture_evidence=False):
    class S3:
        def __init__(self): self.objects = {}
        def get_object(self, Bucket, Key): return {'Body': io.BytesIO(self.objects[Key])}
        def put_object(self, Bucket, Key, Body, **kwargs): self.objects[Key] = Body; return {}
    s3 = S3()
    boto = types.ModuleType('boto3')
    boto.client = lambda name, **kw: s3 if name=='s3' else types.SimpleNamespace(put_parameter=lambda **kw:{'Version':1})
    boto.resource = lambda *a,**kw: types.SimpleNamespace(Table=lambda name:types.SimpleNamespace(scan=lambda **kw:{'Items':rows or []}))
    sys.modules['boto3'] = boto
    sys.modules['boto3.dynamodb'] = types.ModuleType('boto3.dynamodb')
    conditions = types.ModuleType('boto3.dynamodb.conditions'); conditions.Attr = object
    sys.modules['boto3.dynamodb.conditions'] = conditions
    secret = types.ModuleType('managed_secret'); secret.managed_secret = lambda *a,**kw:''
    sys.modules['managed_secret'] = secret
    spec = importlib.util.spec_from_file_location('scorecard_test', HERE.parent/'source/lambda_function.py')
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    module.maybe_telegram = lambda *_: (_ for _ in ()).throw(AssertionError('unexpected message'))
    if allow_fixture_evidence:
        # Statistical/lineage unit fixtures isolate the independent byte parser.
        # The real parser is tested separately against retained original bytes.
        class FixtureVerifier:
            def __init__(self,*a): self.stats={}
            def __call__(self,mark): return [] if mark.get('provider')=='fixture' else ['not_fixture']
        module.PriceEvidenceVerifier=FixtureVerifier
    return module, s3


def test_bitcoin_and_etf_aliases_never_share_identity_or_pricing_route():
    assert resolve_instrument('BTC') is None and resolve_instrument('ETH') is None
    token = resolve_instrument('BTC', 'crypto'); fund = resolve_instrument('BTC', 'etf')
    assert token['instrument_id'] != fund['instrument_id']
    assert token['provider_symbols']['fmp']=='BTCUSD' and fund['provider_symbols']['fmp']=='BTC'
    row=outcome();row['outcome']['entry_marks']['asset']=mark(28.47,2,'equity:US:BTC')
    row['outcome']['marks']['asset']=mark(79899,3,'crypto:BTC/USD')
    check=assess_outcome(row)
    assert not check['verified'] and 'instrument_or_currency_mismatch' in check['reasons']


def test_audited_280543_percent_legacy_gain_is_quarantined_without_mutation():
    row={'signal_type':'eng:crypto-emergence','predicted_dir':'UP','outcome':{'price_at_signal':28.47,'price_at_check':79899,'return_pct':280542.781876,'actual_direction':'UP'}}
    before=copy.deepcopy(row);mod,_=load()
    assert mod.classify(row,'UP')==('quarantined',None) and row==before


def test_known_marks_recompute_direction_and_reject_a_conflicting_return():
    mod,_=load();row=outcome();row['outcome']['actual_direction']='DOWN'
    assert mod.classify(row,'UP',assess_outcome(row,lambda _:[]))==('scored',True)
    row['outcome']['return_pct']=-20
    assert mod.classify(row,'UP')==('quarantined',None)


def test_missing_evidence_vintage_currency_or_forward_entry_is_not_scored():
    for field,value in [('evidence_sha256',None),('adjustment_vintage','different'),('currency','EUR'),('observed_at','2026-08-31T20:00:00Z')]:
        row=outcome();row['outcome']['entry_marks']['asset'][field]=value
        assert not assess_outcome(row)['verified'],field
    row=outcome();row['prediction_origin']='rank_observation'
    assert 'direction_not_explicitly_recorded' in assess_outcome(row)['reasons']


def test_benchmark_requires_the_same_window_and_never_processing_date():
    row=outcome();row['predicted_dir']='OUTPERFORM';oc=row['outcome'];oc['excess_return']=10
    oc['entry_marks']['benchmark']=mark(100,2,'equity:US:SPY');oc['marks']['benchmark']=mark(110,3,'equity:US:SPY')
    oc['checked_at']='2030-01-01T00:00:00Z'
    assert assess_outcome(row,lambda _:[])['verified']
    oc['marks']['benchmark']['observed_at']='2026-09-04T20:00:00Z'
    assert 'asset_benchmark_window_mismatch' in assess_outcome(row)['reasons']


def test_real_large_return_is_not_capped_and_nonfinite_prices_are_invalid():
    a,b=mark(1,2),mark(1000,3)
    value,errors=pair_return(a,b)
    assert not errors and value==99900
    for bad in [float('nan'),float('inf'),True,-1,0]:
        b['price']=bad
        assert pair_return(a,b)[0] is None
    assert finite('Infinity') is None
    assert not assess_outcome({'outcome':{'marks':['bad']}})['verified']


def test_lower_bound_below_45_is_not_evidence_of_underperformance():
    mod,_=load();n=25;hits=15
    lower=mod.wilson_lower(hits,n);upper=1-mod.wilson_lower(n-hits,n)
    assert lower < .45 and mod.status_for(lower,n,upper)=='ACTIVE'
    assert mod.status_for(0,100,1-mod.wilson_lower(100,100))=='DEPRECATED'
    assert mod.status_for(0,100)=='ACTIVE'


def test_full_scan_does_not_stop_at_100000_rows():
    mod,_=load();calls=[]
    def scan(**kw):
        calls.append(kw)
        return {'Items':[None]*100001,'LastEvaluatedKey':{'id':'next'}} if len(calls)==1 else {'Items':[None]}
    mod.ddb=types.SimpleNamespace(Table=lambda _:types.SimpleNamespace(scan=scan))
    assert len(list(mod.scan_outcomes()))==100002 and len(calls)==2


def test_handler_preserves_quarantine_counts_and_cannot_promote_overlapping_samples():
    rows=[outcome() for _ in range(30)]
    rows += [{'signal_type':'bad','predicted_dir':'UP','outcome':{'return_pct':280542.78}}]
    mod,s3=load(rows,allow_fixture_evidence=True);result=mod.lambda_handler({'suppress_alerts':True},None)
    assert result['statusCode']==200
    doc=json.loads(s3.objects['data/signal-scorecard.json'])
    assert doc['n_outcomes_scanned']==31 and doc['n_outcomes_quarantined']==1 and doc['n_outcomes_scored']==30
    assert doc['n_promoted']==doc['alpha']['n_alpha_proven']==0
    assert all(r['performance_multiplier']==1 and not r['sizing_eligible'] for r in doc['scorecard'])
    assert next(r for r in doc['scorecard'] if r['signal_type']=='bad')['hit_rate'] is None
    assert doc['integrity']['scan_complete'] and doc['ssm_ok']


def test_handler_rejects_hash_only_marks_without_a_real_archive():
    mod,s3=load([outcome()]);result=mod.lambda_handler({'suppress_alerts':True},None)
    assert result['statusCode']==200
    doc=json.loads(s3.objects['data/signal-scorecard.json'])
    assert doc['n_outcomes_scored']==0 and doc['n_outcomes_quarantined']==1
    assert doc['integrity']['price_archive_verification_required'] is True
    assert doc['integrity']['price_evidence_checks']['marks_verified']==0


if __name__=='__main__':
    tests=[(n,f) for n,f in sorted(globals().items()) if n.startswith('test_') and callable(f)]
    for name,fn in tests: fn();print('ok',name)
    print('Scorecard integrity tests passed:',len(tests))
    import subprocess
    subprocess.run([sys.executable,str(HERE.parents[2]/'shared/tests/test_outcome_price_evidence.py')],check=True)
