"""Exercise actual downstream boundaries without invoking any live consumer."""
from pathlib import Path
from types import SimpleNamespace
import ast,json,sys,textwrap,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import extremes_research as adapter
from extremes_native_test_support import packet,AT
BAD={'signal':'GENERATIONAL_BUY','capitulation_score':99,'posture':'CAPITULATION','cycle_posture':'CAPITULATION','cycle_position':0,'score':99,
    'scores':{'top_risk':99,'capitulation':99},'candidates':[{'ticker':'TEST','score':99}],'calls_eligible':True,'sizing_eligible':True}
FUNCTIONS=('accumulation-radar','ai-chat','allocator','cro-escalation','forced-selling-bounce','master-ranker','pm-decision','prepump-alerts-router','vol-radar','morning-intelligence','calibration-fleet')
def source(fn):return (ROOT/'aws/lambdas'/('justhodl-'+fn)/'source/lambda_function.py').read_text(encoding='utf-8')
def function(fn,name,ns):
    node=next(n for n in ast.parse(source(fn)).body if isinstance(n,ast.FunctionDef) and n.name==name)
    exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-'+fn,'exec'),ns);return ns[name]

class Boundaries(unittest.TestCase):
    def test_all_actual_read_boundaries_refuse_legacy_and_self_qualified_scores(self):
        mapping={'accumulation-radar':('_read','_cap'),'ai-chat':('get_s3','cap'),'allocator':('fs3','d'),
            'cro-escalation':('read_json','mx'),'forced-selling-bounce':('fetch_s3_json','me'),'master-ranker':('fetch_json','capit'),
            'pm-decision':('get_s3','capit'),'prepump-alerts-router':('_read_json','doc'),'vol-radar':('read_json','capit')}
        for fn,(reader,var) in mapping.items():
            lines=[l for l in source(fn).splitlines() if 'extremes_research' in l and '.decision_view(' in l]
            self.assertTrue(lines,fn)
            for line in lines:
                ns={reader:lambda *args:BAD};exec(textwrap.dedent(line),ns);out=ns[var]
                self.assertIsNone(out['capitulation_score']);self.assertIsNone(out['signal']);self.assertIsNone(out['posture']);self.assertFalse(out['sizing_eligible'])
    def test_allocator_actual_rule_cannot_tilt_for_legacy_buy(self):
        def forbidden(*args,**kw):raise AssertionError('Unqualified tilt')
        ns={'fs3':lambda key:BAD,'tilt':forbidden,'add':forbidden};fn=function('allocator','rule_capitulation',ns)
        scores={'unchanged':5};evidence=[];fn(scores,evidence);self.assertEqual(scores,{'unchanged':5});self.assertEqual(evidence,[])
    def test_actual_alert_router_returns_no_candidate_alert(self):
        ns={'_read_json':lambda key:BAD,'List':list};fn=function('prepump-alerts-router','check_capitulation',ns)
        state={};self.assertEqual(fn(state),[]);self.assertEqual(state,{})
    def test_forced_selling_cannot_get_market_extremes_confirmation(self):
        ns={'fetch_s3_json':lambda key:BAD if key=='data/market-extremes.json' else {},'safe_get':lambda p,k:p.get(k)}
        fired,detail=function('forced-selling-bounce','evaluate_c5_capitulation_posture',ns)()
        self.assertFalse(fired);self.assertIsNone(detail['cycle_posture'])
    def test_morning_loader_retains_inputs_and_removes_unqualified_decision_fields(self):
        line=next(l for l in source('morning-intelligence').splitlines() if 'extremes_research' in l)
        line=line.strip().removeprefix('return ');keys={'capitulation':'data/capitulation.json','market_extremes':'data/market-extremes.json','other':'data/other.json'}
        out=eval(line,{'keys':keys,'fs3':lambda key:BAD})
        self.assertEqual(set(out),set(keys));self.assertEqual(out['other'],BAD)
        for key in ('capitulation','market_extremes'):self.assertIsNone(out[key]['signal']);self.assertIsNone(out[key]['posture'])
        self.assertIn('No validated top/bottom forecast',source('morning-intelligence'))
    def test_calibration_does_not_promote_old_fleet_or_DDB_history(self):
        s=source('calibration-fleet');a=s.index('    # ---- 5. per-engine');b=s.index('    # normalise weight',a)
        def forbidden(*args,**kw):raise AssertionError('Unqualified history used')
        ns={'REGISTRY':[{'name':'market_extremes','direction':'stress','label':'Extremes','source_key':'data/market-extremes.json','score_path':['score']}],
            'snaps':[{'date':'2026-09-01','scores':{'market_extremes':99}}],'forward_dd':forbidden,'ddb_history_snapshots':forbidden}
        exec(textwrap.dedent(s[a:b]),ns);out=ns['engines_out'][0]
        self.assertEqual(out['quality_rating'],'UNQUALIFIED');self.assertIsNone(out['ic_spearman']);self.assertEqual(ns['weight_props'],{})
    def test_current_native_research_also_has_zero_decision_authority(self):
        s,i,p=packet();self.assertTrue(adapter.context(p,AT)['available']);out=adapter.decision_view(p)
        self.assertIsNone(out['signal']);self.assertIsNone(out['posture']);self.assertEqual(out['candidates'],[])
    def test_public_HTTP_is_read_only_and_validation_creates_no_AWS_client(self):
        s,i,p=packet()
        def forbidden(*args,**kw):raise AssertionError('Unexpected acquisition or publication')
        for engine in ('capitulation','market-extremes'):
            ns={'ENGINE':engine,'json':json,'CONTRACT':adapter.CONTRACT,'Config':lambda **kw:None,'boto3':SimpleNamespace(client=lambda *a,**kw:None),
                'reader':lambda *a:lambda key:json.dumps({**p,'engine':engine}).encode(),'current':lambda e:'data/'+e+'.json','run':forbidden}
            h=function(engine,'lambda_handler',ns);self.assertEqual(h({'httpMethod':'GET'})['statusCode'],200)
            ns['boto3']=SimpleNamespace(client=forbidden);self.assertEqual(h({'validate_only':True})['statusCode'],200)
    def test_native_helpers_are_packaged_through_transitive_imports(self):
        sys.path.insert(0,str(ROOT/'aws/ops/checks'));from release_package_evidence import shared_imports
        for fn in FUNCTIONS:
            paths=list((ROOT/'aws/lambdas'/('justhodl-'+fn)/'source').glob('*.py'))
            self.assertIn('extremes_research.py',[p.name for p in shared_imports(ROOT,paths)],fn)
        for fn in ('capitulation','market-extremes'):
            paths=list((ROOT/'aws/lambdas'/('justhodl-'+fn)/'source').glob('*.py'))
            names={p.name for p in shared_imports(ROOT,paths)}
            self.assertTrue({'extremes_native_model.py','extremes_native_store.py','insider_research.py','eurodollar_research.py','aaii_research.py','breadth_series.py'}<=names)

def run():
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Boundaries))
    if not result.wasSuccessful():raise SystemExit(1)
if __name__=='__main__':run()
