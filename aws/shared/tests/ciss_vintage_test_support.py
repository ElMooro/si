"""Exercise the canonical selector and actual ECB producer/consumer handlers offline."""
import importlib.util
import json
import sys
import types
from datetime import datetime,timedelta,timezone
from pathlib import Path
from unittest.mock import patch, Mock
SHARED=Path(__file__).resolve().parents[1]
ROOT=SHARED.parents[1]
sys.path.insert(0,str(SHARED))
from ciss_vintage import select_series,observation_quality
NOW=datetime.now(timezone.utc);TODAY=NOW.date().isoformat()


def load(name):
    with patch.dict(sys.modules,{"boto3":types.SimpleNamespace(client=lambda *a,**k:None),
        "managed_secret":types.SimpleNamespace(managed_secret=lambda *a,**k:"TEST_ONLY"),"_fred_shim":types.ModuleType("_fred_shim")}):
        spec=importlib.util.spec_from_file_location(name.replace('-','_'),ROOT/"aws/lambdas"/name/"source/lambda_function.py")
        m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def seal(doc):
    from ciss_source_model import digest
    doc['replay']={'manifest_key':'data/ciss-research/runs/'+('a'*64)+'.json',
                   'output_sha256':digest({k:v for k,v in doc.items() if k!='replay'}),'fixture':'SYNTHETIC'}
    return doc


def warehouse():
    from ciss_source_model import summarize,csv_series,CONTRACT
    from test_ciss_source_model import csv_bytes
    rows=[]
    for area in ('U2','US','CN','GB','DE','FR','IT','ES','PT','GR','FI','NL','BE','AT','IE'):
        for indicator in ('SS_CIN','SOV_GDPWN' if area=='U2' else 'SOV_CIN'):
            value=.2 if area=='FR' else .05
            key=f'CISS.D.{area}.Z0Z.4F.EC.{indicator}.IDX'
            points=[((NOW.date()-timedelta(days=i*7)).isoformat(),str(value-i*.0001),'A') for i in reversed(range(60))]
            raw=csv_bytes(key,points)
            row=summarize(key,csv_series(raw,key)[key],{'first_received_at':NOW.isoformat(),'fixture':'SYNTHETIC'},NOW.isoformat(),NOW.isoformat())
            row['percentile_3y']=75;row['percentile_5y']=99 if area=='FR' else 25
            rows.append(row)
    return seal({'contract':CONTRACT,'generated_at':NOW.isoformat(),'series':rows,'provenance':'ECB fixture',
                 'ea_composite':.05,'ea_composite_date':TODAY})


def test_selection_uses_maintained_key_and_exact_latest_zero():
    doc=warehouse()
    row=next(r for r in doc['series'] if r['area']=='DE' and 'SOV_' in r['key']);row['latest']=0
    doc['series'].append({**row,'key':'CISS.M.DE.Z0Z.4F.EC.SOV_CI.IDX','latest_date':'2025-04','freq':'M','latest':.9})
    pts,q,row=select_series(seal(doc),'DE',True,NOW)
    assert q['status']=='fresh' and pts[0]==(TODAY,0) and '.D.' in row['key']


def test_stale_future_and_missing_publication_cannot_supply_ranks():
    for published in ('2000-01-01T00:00:00Z',(NOW+timedelta(days=1)).isoformat(),None):
        doc=warehouse();doc['generated_at']=published
        assert not select_series(doc,'DE',True,NOW)[0]
    assert observation_quality('2025-04','M',NOW.isoformat(),NOW)['status']=='stale'
    assert observation_quality((NOW.date()+timedelta(days=1)).isoformat(),'D',NOW.isoformat(),NOW)['status']=='invalid'


def test_sovereign_actual_handler_uses_one_warehouse_and_one_worst_country():
    m=load('justhodl-sovereign-stress');writes=[];doc=warehouse()
    client=types.SimpleNamespace(put_object=lambda **kw:writes.append(kw))
    with patch.object(m,'s3',client),patch.object(m,'read_existing',side_effect=lambda k:doc if k=='data/ciss-stress.json' else {}), \
         patch.object(m,'ecb_ciss',side_effect=AssertionError('No independent CISS fetch')), \
         patch.object(m,'wgb_country',return_value=None),patch.object(m,'fred',return_value=[]), \
         patch.object(m,'eurostat',return_value=[]),patch.object(m,'build_gssi',return_value=None):
        m.lambda_handler({'suppress_alerts':True},None)
        out=json.loads(next(w['Body'] for w in writes if w['Key']==m.OUT_KEY))
        assert out['most_stressed_sovereign']==out['europe_stress']['worst_country']=='france'
        assert out['sovereign_stress_sovciss']['france']['level']==.2
        assert out['systemic_stress_ciss']['euro_area']['yoy_pct'] is None
        doc['generated_at']='2000-01-01T00:00:00Z';writes.clear()
        m.lambda_handler({},None)
        out=json.loads(next(w['Body'] for w in writes if w['Key']==m.OUT_KEY))
        assert out['most_stressed_sovereign'] is None and out['europe_stress']['worst_country'] is None
        assert out['europe_stress']['score_0_100'] is None and out['quality']['status']=='unavailable'


def test_fragmentation_actual_handler_rejects_missing_warehouse():
    m=load('justhodl-euro-fragmentation');writes=[]
    with patch.object(m,'s3',types.SimpleNamespace(put_object=lambda **kw:writes.append(kw))), \
         patch.object(m,'read_existing',return_value={}),patch.object(m,'fred',return_value=[]), \
         patch.object(m,'ciss_series',side_effect=AssertionError('No independent CISS fetch')):
        m.lambda_handler({},None)
    out=json.loads(writes[-1]['Body'])
    assert out['fragmentation']['score_0_100'] is None
    assert out['quality']['missing'] and all(r['sovciss'] is None for r in out['countries'].values())


def test_fragmentation_reads_exact_same_current_country_values():
    m=load('justhodl-euro-fragmentation');doc=warehouse();writes=[]
    completed=NOW.date().replace(day=1)-timedelta(days=1)
    yields=[((completed-timedelta(days=i*31)).isoformat(),3+i*.01) for i in range(15)]
    with patch.object(m,'s3',types.SimpleNamespace(put_object=lambda **kw:writes.append(kw))), \
         patch.object(m,'read_existing',side_effect=lambda k:doc if k=='data/ciss-stress.json' else {}), \
         patch.object(m,'fred',return_value=yields),patch.object(m,'ciss_series',side_effect=AssertionError('No direct CISS calls')):
        m.lambda_handler({},None)
    out=json.loads(writes[-1]['Body'])
    assert out['countries']['FR']['sovciss']==.2 and out['quality']['status']=='fresh'
    assert out['countries']['DE']['spread_vs_bund_bp']==0


def test_actual_warehouse_publisher_uses_verified_source_store():
    m=load('justhodl-ciss-stress')
    with patch.object(m,'run',return_value={'published':True}) as run:
        response=m.lambda_handler({},types.SimpleNamespace(get_remaining_time_in_millis=lambda:600000))
    assert response['statusCode']==200
    run.assert_called_once_with(m.S3,m.BUCKET,budget_seconds=480)


def test_actual_commentary_publisher_uses_deterministic_source_store():
    m=load('justhodl-ciss-ai')
    with patch.object(m,'run_commentary',return_value={'published':True}) as run:
        response=m.lambda_handler({},None)
    assert response['statusCode']==200
    run.assert_called_once_with(m.S3,m.BUCKET)


def test_selector_rechecks_acquisition_and_preserves_missing_current_methodology():
    doc=warehouse();row=next(r for r in doc['series'] if r['area']=='DE' and 'SOV_' in r['key'])
    row['acquired_at']=(NOW-timedelta(days=4)).isoformat()
    points,q,_=select_series(seal(doc),'DE',True,NOW)
    assert not points and q['status']=='stale' and q['publication_date'] is None
    doc=warehouse();row=next(r for r in doc['series'] if r['area']=='DE' and 'SOV_' in r['key'])
    doc['series'].append({**row,'key':row['key'].replace('SOV_CIN','SOV_CI')})
    row['latest']=None;row['quality']={**row['quality'],'status':'missing','missing':['current_observation']}
    points,q,chosen=select_series(seal(doc),'DE',True,NOW)
    assert not points and q['status']=='missing' and chosen['key'].endswith('SOV_CIN.IDX')
    assert q['source_replay']==doc['replay'] and q['calls_eligible'] is False
    monthly=observation_quality('2026-07','M','2026-09-19T00:00:00Z',datetime(2026,9,19,tzinfo=timezone.utc))
    assert monthly['period_end']=='2026-07-31' and monthly['publication_date'] is None


def test_sovereign_spike_cannot_emit_unvalidated_down_forecast():
    m=load('justhodl-sovereign-stress');doc=warehouse();writes=[]
    ledger={'rows':[{'date':(NOW.date()-timedelta(days=i)).isoformat(),'countries':{name:{'s':10} for name in m.COUNTRY_ETF}} for i in reversed(range(1,8))]}
    signal=Mock(return_value=True);price=Mock(return_value=100)
    donor={'bond10y_pct':6,'cds_bp':200,'spread_vs_bund_bp':150,'as_of':TODAY}
    with patch.dict(sys.modules,{'signals_emit':types.SimpleNamespace(log_signal=signal,yprice=price)}), \
         patch.object(m,'s3',types.SimpleNamespace(put_object=lambda **kw:writes.append(kw))), \
         patch.object(m,'read_existing',side_effect=lambda key:doc if key=='data/ciss-stress.json' else ledger if key==m.HIST_KEY else {}), \
         patch.object(m,'wgb_country',return_value=donor),patch.object(m,'fred',return_value=[]), \
         patch.object(m,'eurostat',return_value=[]),patch.object(m,'_get',return_value=b'{}'),patch.object(m,'build_gssi',return_value=None), \
         patch.object(m.boto3,'resource',return_value=types.SimpleNamespace(Table=lambda _:object()),create=True):
        m.lambda_handler({},None)
    out=json.loads(next(w['Body'] for w in writes if w['Key']==m.OUT_KEY))
    # Some descriptive countries have no ledger baseline. Test the named
    # qualifying country, independently of Python's set/hash iteration order.
    assert out['deltas']['france']['d5']>10
    assert any(d['d5'] is None for d in out['deltas'].values())
    signal.assert_not_called();price.assert_not_called()
    assert out['signals_fired']==[] and out['signal_emission']['enabled'] is False
    assert out['call'] is None and out['decision']['meaning']=='abstain'
    assert out['ciss_warehouse']['replay']==doc['replay']


def run():
    tests=[f for n,f in list(globals().items()) if n.startswith('test_')]
    for test in tests:test()
    print(f'CISS warehouse contracts passed: {len(tests)}')


if __name__=='__main__':run()
