"""Invented inputs run through actual risk producer, Khalid projection and page fixtures."""
import ast,hashlib,json,runpy,sys
from copy import deepcopy
from pathlib import Path
import lambda_function as handler
from risk_diagnostics import project
ROOT=Path(__file__).resolve().parents[4]
RISK=ROOT/'aws/lambdas/justhodl-khalid-risk'
sys.path.insert(0,str(RISK/'source'))
import risk_engine
risk_tests=runpy.run_path(str(RISK/'tests/test_risk_engine.py'))
NOW=risk_tests['NOW']

def producer():
    feeds,meta=risk_tests['withheld_inputs']()
    return risk_engine.build_output(risk_tests['REGISTRY'],feeds,meta,NOW)

def packet():
    risk=producer()
    return handler.build_output({'khalid_risk':risk},{'khalid_risk':{'last_modified':risk['generated_at']}},NOW,[])

def test_risk_diagnostics_full_producer_packet_path():
    risk=producer();p=packet();out=p['risk_authority_diagnostics']
    assert len(out['rows'])==4
    assert out['generated_at']==risk['generated_at']==p['risk_artifact']['generated_at']
    assert out['expires_at']==risk['expires_at']
    for row in out['rows']:
        h=next(h for h in risk['source_health'] if h['name']==row['source_id'])
        assert row['authority_diagnostic']==h['authority_diagnostic']
        assert row['source_as_of']==h['as_of'] and row['age_h']==h['age_h']
    assert p['risk_control']['mode']=='DATA_HOLD'
    assert p['risk_control']['allows_new_entries'] is False
    assert p['risk_control']['sizing_multiplier']==p['risk_control']['exposure_cap_pct']==0

def test_risk_diagnostics_strict_versions_sources_types_and_bounds():
    original=producer()
    for value in (None,False,0,{},[],{'schema_version':'future'}):
        assert all(r['status']=='UNAVAILABLE' for r in project(value)['rows'])
    for field,value in [('schema_version','future'),('engine','other'),('generated_at',False),('expires_at','garbage'),('source_health',[{}]*65)]:
        p=deepcopy(original);p[field]=value
        assert all(r['status']=='UNAVAILABLE' for r in project(p)['rows'])
    for field,value in [('name','other'),('key','data/other.json'),('producer','other'),('critical',1),('status','FRESH'),('age_h',False),('age_h',-1),('max_age_h',0),('as_of','<img src=x onerror=alert(1)>')]:
        p=deepcopy(original);p['source_health'][0][field]=value
        assert project(p)['rows'][0]['status']=='UNAVAILABLE'
    for field,value in [('schema_version','future'),('source_id','bond_warroom'),('explanation','x'*10000),('explanation','<script>alert(1)</script>'),('effect',False),('code',0)]:
        p=deepcopy(original);p['source_health'][0]['authority_diagnostic'][field]=value
        assert project(p)['rows'][0]['status']=='UNAVAILABLE'
    p=deepcopy(original);p['source_health'].append(deepcopy(p['source_health'][0]))
    assert project(p)['rows'][0]['status']=='UNAVAILABLE'
    p=deepcopy(original);p['source_health'][0]['age_h']=0
    assert project(p)['rows'][0]['age_h']==0
    original_copy=deepcopy(original);project(original);assert original==original_copy

def test_risk_diagnostics_policy_and_entire_packet_parity():
    path=ROOT/'aws/lambdas/justhodl-khalid/source/lambda_function.py'
    current=path.read_text();line='        "risk_authority_diagnostics": __import__("risk_diagnostics").project(risk_artifact),\n'
    assert current.count(line)==1
    before=current.replace(line,'')
    assert hashlib.sha256(before.encode()).hexdigest()=='08ded7620ccfa5842113a57b481a515c021b47e4d50e00ee6bca7c2745f8b798'
    tree=ast.parse(before);fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='build_output')
    scope=dict(handler.__dict__);exec(compile(ast.Module(body=[fn],type_ignores=[]),str(path),'exec'),scope)
    from test_scoring import valid_khalid_risk_artifact
    cases=[producer(),{},None,valid_khalid_risk_artifact(NOW.isoformat())]
    for mode in ("DATA_HOLD","DEFENSIVE","SELECTIVE","SELECTIVE_RISK_ON"):
        risk=valid_khalid_risk_artifact(NOW.isoformat());risk["policy"]["mode"]=mode;cases.append(risk)
    for risk in cases:
        feeds={'khalid_risk':risk};meta={};snapshot=deepcopy(feeds)
        old=scope['build_output'](feeds,meta,NOW,[]);new=handler.build_output(feeds,meta,NOW,[])
        new.pop('risk_authority_diagnostics');assert new==old;assert feeds==snapshot


def test_numeric_diagnostics_never_suppress_base_publication():
    path=ROOT/'aws/lambdas/justhodl-khalid/source/lambda_function.py'
    source=path.read_text().replace('        "risk_authority_diagnostics": __import__("risk_diagnostics").project(risk_artifact),\n','')
    assert hashlib.sha256(source.encode()).hexdigest()=='08ded7620ccfa5842113a57b481a515c021b47e4d50e00ee6bca7c2745f8b798'
    fn=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef) and n.name=='build_output')
    scope=dict(handler.__dict__);exec(compile(ast.Module(body=[fn],type_ignores=[]),str(path),'exec'),scope)
    values=[10**400,-10**400,2**53,2**53-1,float(2**53),float(2**53-1),
            float('inf'),float('-inf'),float('nan'),None,False,True,'0',{},[], -1,0,0.0,-0.0,0.5,1]
    count=0
    for name in ('risk_gate','bond_warroom','eurodollar_stress','credit_composite'):
        for field in ('age_h','max_age_h'):
            for value in values:
                risk=producer();row=next(h for h in risk['source_health'] if h['name']==name);row[field]=value
                if type(value) is int:risk=json.loads(json.dumps(risk,allow_nan=False))
                feeds={'khalid_risk':risk};old=scope['build_output'](feeds,{},NOW,[])
                new=handler.build_output(feeds,{},NOW,[])
                projection=new.pop('risk_authority_diagnostics');assert new==old
                assert new['risk_control']['mode']=='DATA_HOLD'
                assert new['risk_control']['allows_new_entries'] is False
                assert new['risk_control']['exposure_cap_pct']==new['risk_control']['sizing_multiplier']==0
                projected=next(r for r in projection['rows'] if r['source_id']==name)
                valid=type(value) in (int,float) and 0<=value<=2**53-1 and (field=='age_h' or value>0)
                assert projected['status']==('INVALID' if valid else 'UNAVAILABLE')
                if not valid:assert projected['authority_diagnostic'] is None
                json.dumps(projection,allow_nan=False)
                count+=1
    assert count==168
