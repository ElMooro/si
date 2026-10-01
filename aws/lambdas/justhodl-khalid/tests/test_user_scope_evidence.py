"""Offline source-snapshot scope contract and whole-output parity."""
import copy
import hashlib
import json
from pathlib import Path
import types
from user_scope_evidence import project, numeric, SUPPORTED_INDUSTRIES
from test_scoring import base_row, valid_khalid_risk_artifact
from test_qualification import NOW

SOURCE = Path(__file__).resolve().parents[1] / "source"
LINE = '    output["user_scope_evidence"] = __import__("user_scope_evidence").project(output, active_feeds)\n'
AT = NOW.isoformat()


def inputs(ac="STOCK", cap=3e9, ind="Medical Devices"):
    raw = {"ticker": "TEST", "industry": ind, "market_cap": cap}
    container = "etfs" if ac == "ETF" else "board"
    feeds = {"fortress": {"engine": "justhodl-fortress", "as_of": AT, "board": [], "etfs": [], "ledger": [],
                          "inputs": {"finviz_universe": AT, "fundamental_census_matrix": AT}}}
    feeds['fortress'][container] = [raw]
    output = {"generated_at": AT, "qualification_evidence": {"revision": "a" * 64},
              "source_health": [{"name": "fortress", "producer": "justhodl-fortress", "key": "data/fortress.json", "as_of": AT, "status": "FRESH", "max_age_h": 84}],
              "opportunity_radar": [{"ticker": "TEST", "asset_class": ac, "sources": ["fortress-execution"]}]}
    return output, feeds, raw


def test_scope_source_identity_clocks_and_exact_industry():
    o, f, r = inputs(ind="Biotechnology")
    before = copy.deepcopy((o, f)); p = project(o, f)
    assert p['rows'][0]['checks'] == [['PASS','STOCK','observed'], ['FAIL','Biotechnology','observed'], ['PASS',3e9,'observed']]
    assert (o, f) == before and p['effective_at'] is p['available_at'] is None
    for value in ('Medical Devices', 'Medical Instruments & Supplies'):
        r['industry'] = value; assert project(o,f)['rows'][0]['checks'][1][0] == 'PASS'
    for value in (None, True, 1, {}, [], '', 'unknown', 'N/A', ' Biotechnology', 'Exchange Traded Fund', 'Shell Companies', 'bad\nlabel', 'biotechnology', 'BIOTECHNOLOGY', 'Unclassified', 'garbage', 'Healthcare', 'Diagnostics & Research'):
        r['industry'] = value; assert project(o,f)['rows'][0]['checks'][1] == ['UNAVAILABLE',None,'industry']
    for field, value in [('engine','wrong'),('as_of',None),('as_of','2026-10-02T00:00:00Z'),('as_of','2026-10-01T04:00:00')]:
        a,b,_ = inputs(); b['fortress'][field]=value
        assert all(c[0]=='UNAVAILABLE' for c in project(a,b)['rows'][0]['checks'])
    for field, value in [('status','STALE'),('key','data/katlin.json'),('producer','other'),('max_age_h',True),('max_age_h',10**400),('as_of','2026-09-30T04:00:00Z')]:
        a,b,_=inputs();a['source_health'][0][field]=value
        assert project(a,b)['rows'][0]['checks'][0][0]=='UNAVAILABLE'
    for value in [None,'invalid','2026-10-02T04:00:00Z','2026-10-01T04:00:00']:
        a,b,_=inputs();b['fortress']['inputs']['finviz_universe']=value
        assert project(a,b)['rows'][0]['checks'][1][2]=='clock'
    # A source timestamp is an explicitly dated snapshot, not a newly invented freshness SLA.
    f['fortress']['inputs']['finviz_universe']='2025-01-01T00:00:00Z'
    assert 'not independently freshness-qualified' in project(o,f)['clock_policy']


def test_scope_cap_boundaries_units_buckets_hugeints_and_applicability():
    for value,status in [(1,'UNRESOLVED'),(49e6,'UNRESOLVED'),(50e6,'UNRESOLVED'),(299999999,'UNRESOLVED'),(300e6,'FAIL'),(1999999999,'FAIL'),(2e9,'PASS'),(10e9,'PASS'),(200e9,'PASS')]:
        o,f,r=inputs(cap=value);assert project(o,f)['rows'][0]['checks'][2][0]==status
    for value in [10**400,-10**400,2**53,float('inf'),float('-inf'),float('nan'),None,False,True,'300000000',{},[],0,-1]:
        o,f,r=inputs(cap=value);p=project(o,f)
        assert p['rows'][0]['checks'][2]==['UNAVAILABLE',None,'cap'];json.dumps(p,allow_nan=False)
    for supplied in ['small','SMALL',False,[], 'MID ', 'whatever']:
        o,f,r=inputs(cap=3e9);r['cap_bucket']=supplied
        assert project(o,f)['rows'][0]['checks'][2][2]=='bucket'
    o,f,r=inputs(cap=3e9);r['cap_bucket']='mid';assert project(o,f)['rows'][0]['checks'][2][0]=='PASS'
    o,f,r=inputs(cap=49e6);r['cap_bucket']='nano';assert project(o,f)['rows'][0]['checks'][2][0]=='UNRESOLVED'
    o,f,r=inputs(ac='ETF');assert [c[0] for c in project(o,f)['rows'][0]['checks']]==['PASS','UNRESOLVED','UNRESOLVED']
    for ac in ['BOND','COMMODITY','UNKNOWN','stock',None,{},[]]:
        o,f,r=inputs();o['opportunity_radar'][0]['asset_class']=ac
        assert project(o,f)['rows'][0]['checks'][0][0]=='UNAVAILABLE'


def test_scope_no_ticker_cross_asset_join_duplicates_or_source_substitution():
    o,f,r=inputs();f['fortress']['etfs']=[dict(r)]
    assert project(o,f)['rows'][0]['checks'][0][2]=='identity'
    o,f,r=inputs();f['fortress']['board'].append(dict(r))
    assert project(o,f)['rows'][0]['checks'][0][2]=='identity'
    o,f,r=inputs();o['opportunity_radar'][0]['sources']=['katlin']
    assert project(o,f)['rows'][0]['checks'][0][2]=='missing'
    o,f,r=inputs();r['asset_class']='crypto'
    assert project(o,f)['rows'][0]['checks'][0][2]=='identity'
    o,f,r=inputs();r['ticker']='test'
    assert project(o,f)['rows'][0]['checks'][0][2]=='missing'
    o,f,r=inputs();o['source_health'].append(dict(o['source_health'][0]))
    assert project(o,f)['rows'][0]['checks'][0][2]=='source'


def katlin_inputs(ac='stock'):
    o,f,r=inputs();f['katlin']={'engine':'justhodl-katlin','generated_at':AT,'research_status':'FRESH','research_max_age_h':36,'picks':[{'ticker':'TEST','asset_class':ac,'industry':'Medical Devices','mcap':3e9}],'watch':[], 'feeds_asof':{'finviz':AT,'census':AT}}
    o['source_health'].append({'name':'katlin','key':'data/katlin.json','producer':'justhodl-katlin','status':'FRESH','max_age_h':36,'as_of':AT})
    o['opportunity_radar'][0]['sources']=['katlin'];f['fortress']['board']=[]
    o['opportunity_radar'][0]['asset_class']=ac.upper()
    return o,f


def test_scope_katlin_native_mcap_research_clock_and_conflicts():
    o,f=katlin_inputs();assert project(o,f)['rows'][0]['checks'][2][1]==3e9
    assert 'market_cap' not in f['katlin']['picks'][0]
    for ac in ['etf','crypto']:
        o,f=katlin_inputs(ac);assert [c[0] for c in project(o,f)['rows'][0]['checks']]==['PASS','UNRESOLVED','UNRESOLVED']
    o,f=katlin_inputs();f['katlin']['permission_refreshed_at']=AT
    assert project(o,f)['rows'][0]['checks'][0][2]=='source'
    f['katlin']['research_generated_at']='2026-09-28T00:00:00Z'
    assert project(o,f)['rows'][0]['checks'][0][2]=='source'
    f['katlin']['research_generated_at']=AT
    assert project(o,f)['rows'][0]['checks'][0][0]=='PASS'
    o,f=katlin_inputs();o['opportunity_radar'][0]['sources'].append('fortress-execution')
    f['fortress']['board']=[{'ticker':'TEST','industry':'Biotechnology','market_cap':4e9}]
    assert [c[2] for c in project(o,f)['rows'][0]['checks']]==['observed','conflict','conflict']
    f['katlin']['picks'][0]['asset_class']='crypto'
    assert project(o,f)['rows'][0]['checks'][0][2]=='identity'


def test_scope_whole_output_and_legacy_qualification_byte_parity():
    import lambda_function as candidate
    text=(SOURCE/'lambda_function.py').read_text();assert text.count(LINE)==1
    original=text.replace(LINE,'')
    assert hashlib.sha256(original.encode()).hexdigest()=='9abcfd7c7afeebc26c3e7e0f3850933fd2a7ccca3a83ceadd7662d88a223507a'
    module=types.ModuleType('pre_scope');module.__file__=str(SOURCE/'lambda_function.py');exec(compile(original,module.__file__,'exec'),module.__dict__)
    for risk in [{},valid_khalid_risk_artifact(AT)]:
        for trigger in [False,True]:
            o,feeds,_=inputs();raw=base_row();raw['entry_triggered']=trigger;feeds['fortress']['board']=[raw];feeds['khalid_risk']=risk
            before=copy.deepcopy(feeds);metas={k:{'last_modified':AT,'error':None} for k in feeds}
            old=module.build_output(feeds,metas,NOW,[]);new=candidate.build_output(feeds,metas,NOW,[])
            new.pop('user_scope_evidence');assert new==old and feeds==before
            assert json.dumps(new['qualification_evidence'],separators=(',',':'))==json.dumps(old['qualification_evidence'],separators=(',',':'))
            assert json.dumps(new,separators=(',',':'))==json.dumps(old,separators=(',',':'))


def test_scope_bounded_vocabulary_is_existing_product_labels_only():
    import ast
    path=SOURCE.parents[1]/"justhodl-fortress/source/lambda_function.py"
    node=next(n for n in ast.parse(path.read_text()).body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=="IND_ETF" for t in n.targets))
    assert SUPPORTED_INDUSTRIES==set(ast.literal_eval(node.value))


def test_scope_native_katlin_research_health_is_not_renewed_by_permission():
    for key,values in [("research_status",[None,"STALE","UNKNOWN",False]),("research_max_age_h",[None,True,10**400,35,84,"36"])]:
        for value in values:
            o,f=katlin_inputs();f["katlin"][key]=value
            assert all(c[0]=="UNAVAILABLE" for c in project(o,f)["rows"][0]["checks"])
