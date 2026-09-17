"""Exercise the canonical selector and actual ECB producer/consumer handlers offline."""
import importlib.util
import json
import sys
import types
from datetime import datetime,timedelta,timezone
from pathlib import Path
from unittest.mock import patch
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


def warehouse():
    rows=[]
    for area in ('U2','US','CN','GB','DE','FR','IT','ES','PT','GR','FI','NL','BE','AT','IE'):
        for indicator in ('SS_CIN','SOV_GDPWN' if area=='U2' else 'SOV_CIN'):
            value=.2 if area=='FR' else .05
            rows.append({"key":f"CISS.D.{area}.Z0Z.4F.EC.{indicator}.IDX","area":area,"freq":"D","latest_date":TODAY,
                "latest":value,"percentile_3y":75,"percentile_5y":99 if area=='FR' else 25,
                "points":[[(NOW.date()-timedelta(days=i*7)).isoformat(),value-i*.0001] for i in reversed(range(60))]})
    return {"generated_at":NOW.isoformat(),"series":rows,"provenance":"ECB","ea_composite":.05,"ea_composite_date":TODAY}


def test_selection_uses_maintained_key_and_exact_latest_zero():
    doc=warehouse()
    row=next(r for r in doc['series'] if r['area']=='DE' and 'SOV_' in r['key']);row['latest']=0
    doc['series'].append({**row,'key':'CISS.M.DE.Z0Z.4F.EC.SOV_CI.IDX','latest_date':'2025-04','freq':'M','latest':.9})
    pts,q,row=select_series(doc,'DE',True,NOW)
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


def test_producer_calendar_change_is_not_a_twelve_observation_proxy():
    m=load('justhodl-ciss-stress')
    pts=[['2025-09-17',.01],['2026-09-15',.02],['2026-09-17',.03]]
    result=m.stats(pts)
    assert result['chg_1y']==.02 and result['yoy_pct'] is None
    assert m.stats([['2026-09-17',0]])['chg_1y'] is None
    rows=m._csv_rows('KEY,TIME_PERIOD,OBS_VALUE,TITLE\nCISS.D.FR,2026-09-17,0.2,"with, comma"\nCISS.D.DE,2026-09-17,NaN,missing\n')
    assert len(rows)==1


def test_fragmentation_reads_exact_same_current_country_values():
    m=load('justhodl-euro-fragmentation');doc=warehouse();writes=[]
    yields=[((NOW.date()-timedelta(days=i*31)).isoformat(),3+i*.01) for i in range(15)]
    with patch.object(m,'s3',types.SimpleNamespace(put_object=lambda **kw:writes.append(kw))), \
         patch.object(m,'read_existing',side_effect=lambda k:doc if k=='data/ciss-stress.json' else {}), \
         patch.object(m,'fred',return_value=yields),patch.object(m,'ciss_series',side_effect=AssertionError('No direct CISS calls')):
        m.lambda_handler({},None)
    out=json.loads(writes[-1]['Body'])
    assert out['countries']['FR']['sovciss']==.2 and out['quality']['status']=='fresh'
    assert out['countries']['DE']['spread_vs_bund_bp']==0


def test_actual_warehouse_publisher_flags_legacy_and_preserves_exact_head():
    m=load('justhodl-ciss-stress');writes=[]
    key='CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX'
    old='CISS.M.GB.Z0Z.4F.EC.SOV_CI.IDX'
    with patch.object(m,'S3',types.SimpleNamespace(put_object=lambda **kw:writes.append(kw))), \
         patch.object(m,'discover',side_effect=lambda flow:{key:TODAY,old:'2025-04'} if flow=='CISS' else {}), \
         patch.object(m,'history',side_effect=lambda k:[[TODAY,0]] if k==key else [['2025-04',.4]]), \
         patch.object(m.time,'sleep'):
        m.lambda_handler({},None)
    out=json.loads(writes[0]['Body'])
    assert out['ea_composite']==0 and out['quality']['status']=='fresh'
    assert next(r for r in out['series'] if r['key']==old)['ranking_eligible'] is False


def run():
    tests=[f for n,f in list(globals().items()) if n.startswith('test_')]
    for test in tests:test()
    print(f'CISS warehouse contracts passed: {len(tests)}')


if __name__=='__main__':run()
