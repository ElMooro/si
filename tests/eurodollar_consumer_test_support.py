"""Exercise deployed consumer code with synthetic legacy and native USD packets."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime,timezone
from types import SimpleNamespace
from unittest.mock import patch
import ast,json,sys,textwrap,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-eurodollar-stress/tests')]
from native_eurodollar_tests import model,store,store_fixture,Storage,STAMP
import eurodollar_research as adapter
FUNCTIONS=('justhodl-ai-brief', 'justhodl-alert-router', 'justhodl-allocator', 'justhodl-auction-interpreter', 'justhodl-boj-detail', 'justhodl-calibration-fleet', 'justhodl-capitulation', 'justhodl-chart-data', 'justhodl-dollar-radar', 'justhodl-ecb-detail', 'justhodl-kb-matcher', 'justhodl-market-interpreter', 'justhodl-master-allocator', 'justhodl-page-ai-commentary', 'justhodl-regime-conditional-router', 'justhodl-repo-lending', 'justhodl-reversal-radar', 'justhodl-snb-detail', 'justhodl-stress-scenarios', 'justhodl-wave-signal-logger', 'openbb-websocket-broadcast')
def source(fn):return (ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_text(encoding='utf-8')
def functions(fn,names,ns):
    nodes=[n for n in ast.parse(source(fn)).body if isinstance(n,ast.FunctionDef) and n.name in names]
    assert len(nodes)==len(names)
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual-'+fn,'exec'),ns)
    return ns
class ConsumerCases(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        objects,i=store_fixture();client=Storage(objects)
        client.objects[store.SOURCES[0]]=model.encoded(i['macro'])
        inputs={'contract':'eurodollar-native-inputs.v1','generated_at':STAMP,'macro':store.snapshot(client,'b',store.SOURCES[0]),'fx':None}
        out=store.compile_output(inputs,store.reader(client,'b'))
        cls.packet={**out,'replay':store.retain(client,'b',inputs,out)}
        cls.at=datetime.fromisoformat(STAMP)
    def test_typed_values_retain_dates_units_and_no_vote(self):
        c=adapter.context(self.packet,self.at);self.assertEqual(len(c['measurements']),9)
        self.assertEqual(c['measurements']['BAMLH0A0HYM2']['value_bps'],270)
        self.assertEqual(c['measurements']['DTWEXBGS']['observation_date'],'2026-09-11')
        self.assertIsNone(adapter.qualified_score(self.packet))
    def test_legacy_self_qualified_tampered_future_stale_cannot_enter(self):
        for p in ({'composite_score':99,'regime':'PANIC'},None,[],{}):self.assertFalse(adapter.context(p,self.at)['available'])
        for stamp in ('2026-09-19T09:30:00Z','2026-09-22T09:30:00Z'):
            self.assertFalse(adapter.context(self.packet,adapter.clock(stamp))['available'])
        p=deepcopy(self.packet);p['measurements']['SOFR']['value']=99
        self.assertFalse(adapter.context(p,self.at)['available'])
        p=deepcopy(self.packet);p['calls_eligible']=True
        p['replay']['output_sha256']=model.sha(model.encoded({k:v for k,v in p.items() if k!='replay'}))
        self.assertFalse(adapter.context(p,self.at)['available'])
    def test_brief_preserves_research_without_legacy_score(self):
        ns=functions('justhodl-ai-brief',['compress_ed_stress'],{})
        out=ns['compress_ed_stress']({'composite_score':99,'severity':'CRITICAL'})
        self.assertIsNone(out['score']);self.assertIsNone(out['severity']);self.assertFalse(out['calls_eligible'])
    def test_allocator_alert_and_logger_ignore_legacy_high_and_low_scores(self):
        def forbidden(*a,**kw):raise AssertionError('Unqualified score reached an action')
        for value in (0,20,70,99):
            p={'composite_score':value,'composite_stress_score':value,'calls_eligible':True}
            ns=functions('justhodl-allocator',['rule_eurodollar_stress'],{'fs3':lambda key:p,'add':forbidden})
            self.assertIsNone(ns['rule_eurodollar_stress']({},{}))
            ns=functions('justhodl-alert-router',['check_eurodollar_stress'],{'load_json':lambda key:p})
            alerts=[];ns['check_eurodollar_stress'](alerts);self.assertEqual(alerts,[])
            ns=functions('justhodl-wave-signal-logger',['log_eurodollar_stress'],{'fs3':lambda key:p,'log_sig':forbidden})
            self.assertEqual(ns['log_eurodollar_stress'](),[])
    def test_master_allocation_tilts_cannot_come_from_legacy_usd_score(self):
        ns=functions('justhodl-master-allocator',['gather_signals','clamp'],{'read_json':lambda key:{'composite_score':99} if key=='data/eurodollar-stress.json' else {}})
        self.assertNotIn('eurodollar_stress',ns['gather_signals']())
    def test_router_abstains_instead_of_zero_funding_or_shortage_score(self):
        names=['detect_eurodollar_stress','detect_dollar_shortage','safe_get']
        ns=functions('justhodl-regime-conditional-router',names,{})
        for name in names[:2]:
            score,evidence=ns[name]({'score':99,'composite_score':99},{'score':99},{'score':99})
            self.assertIsNone(score);self.assertEqual(evidence['status'],'ABSTAIN')
        scores={'EURODOLLAR_STRESS':None,'DOLLAR_SHORTAGE_COLLATERAL':None,'OTHER':60,'PERMANENT_PORTFOLIO':40}
        line=next(l for l in source('justhodl-regime-conditional-router').splitlines() if 'sorted_fwks = ' in l)
        ns={'scores':scores};exec(textwrap.dedent(line),ns);self.assertEqual(ns['sorted_fwks'][0],('OTHER',60))
    def test_old_fleet_or_ddb_history_cannot_requalify_eurodollar(self):
        s=source('justhodl-calibration-fleet');start=s.index('    # ---- 5. per-engine');end=s.index('    # normalise weight',start)
        def forbidden(*a,**kw):raise AssertionError('Unqualified history used')
        ns={'REGISTRY':[{'name':'eurodollar_stress','direction':'stress','label':'USD','source_key':'data/eurodollar-stress.json','score_path':['composite_score']}],
            'snaps':[{'date':'2026-09-01','scores':{'eurodollar_stress':99}}],'forward_dd':forbidden,'ddb_history_snapshots':forbidden}
        exec(textwrap.dedent(s[start:end]),ns)
        out=ns['engines_out'][0];self.assertEqual(out['quality_rating'],'UNQUALIFIED');self.assertIsNone(out['ic_spearman']);self.assertEqual(ns['weight_props'],{})
    def test_kb_state_has_context_and_no_stress_rule_value(self):
        ns=functions('justhodl-kb-matcher',['build_today_state'],{'s3j':lambda key:{'composite_score':99} if key=='data/eurodollar-stress.json' else {},'fred_last':lambda *a:[]})
        out=ns['build_today_state']();self.assertNotIn('eurodollar_stress',out);self.assertIn('eurodollar_research',out)
    def test_detail_and_capitulation_actual_read_boundaries(self):
        from extremes_native_test_support import synthesis_with
        out=synthesis_with('funding',{'score':99,'composite_score':99},datetime.fromisoformat(STAMP))
        self.assertEqual(out['measurements'],[]);self.assertIsNone(out['capitulation_score'])
        for fn,var,reader in [('justhodl-boj-detail','eds','read_existing'),('justhodl-ecb-detail','eds','read_existing'),('justhodl-snb-detail','eds','read_existing')]:
            line=next(l for l in source(fn).splitlines() if 'eurodollar_research' in l and 'decision_view' in l)
            ns={reader:lambda key:{'score':99,'composite_score':99}};exec(textwrap.dedent(line),ns)
            self.assertIsNone(ns[var]['score']);self.assertIsNone(ns[var]['composite_score'])
    def test_legacy_chart_does_not_draw_a_fabricated_score(self):
        ns=functions('justhodl-chart-data',['fetch_internal'],{'INTERNAL_SERIES_MAP':{'eurodollar_stress':('unused',None,'unused')}})
        self.assertIsNone(ns['fetch_internal']('eurodollar_stress'))
    def test_every_consumer_packages_native_boundary(self):
        sys.path.insert(0,str(ROOT/'aws/ops/checks'));from release_package_evidence import shared_imports
        for fn in FUNCTIONS:
            if fn=='justhodl-chart-data':continue
            paths=list((ROOT/'aws/lambdas'/fn/'source').glob('*.py'))
            self.assertIn('eurodollar_research.py',[p.name for p in shared_imports(ROOT,paths)],fn)
    def test_http_and_validation_do_not_publish_or_read_credentials(self):
        def forbidden(*a,**kw):raise AssertionError('Unexpected mutation')
        ns={'json':json,'CONTRACT':model.CONTRACT,'CURRENT':store.CURRENT,'Config':lambda **kw:None,
            'boto3':SimpleNamespace(client=lambda *a,**kw:None),'reader':lambda *a:lambda key:model.encoded(self.packet),'run':forbidden}
        functions('justhodl-eurodollar-stress',['lambda_handler'],ns)
        self.assertEqual(ns['lambda_handler']({'httpMethod':'GET'})['statusCode'],200)
        ns['boto3']=SimpleNamespace(client=forbidden)
        self.assertEqual(ns['lambda_handler']({'validate_only':True})['statusCode'],200)

def run():
    if not unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(ConsumerCases)).wasSuccessful():raise SystemExit(1)
if __name__=='__main__':run()
