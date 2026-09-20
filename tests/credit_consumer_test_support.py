"""Execute the native credit boundaries against real consumer code blocks."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
from types import SimpleNamespace
from unittest.mock import patch
import ast,copy,json,sys,textwrap,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-credit-stress/tests')]
from credit_fixtures import fixture,model,STAMP
from native_credit_tests import Memory
import credit_research_store as store
import credit_research as adapter

FUNCTIONS=('ai-chat','bottleneck-boom','capitulation','credit-composite','cycle-clock','market-extremes','morning-intelligence','vol-radar')
def source(fn):return (ROOT/'aws/lambdas'/('justhodl-'+fn)/'source/lambda_function.py').read_text(encoding='utf-8')


class ConsumerCases(unittest.TestCase):
    def setUp(self):
        inputs,bodies=fixture();s=Memory();s.objects.update(bodies)
        out=store.compile_output(inputs,store.reader(s,'test'))
        self.packet={**out,'replay':store.retain(s,'test',inputs,out)}
        self.at=datetime.fromisoformat(STAMP)

    def test_typed_measurements_and_source_dates(self):
        c=adapter.context(self.packet,self.at);self.assertTrue(c['available']);self.assertEqual(len(c['measurements']),28)
        hy=c['measurements']['BAMLH0A0HYM2'];self.assertEqual(hy['value_bps'],hy['value_pct']*100)
        text=adapter.describe(c);self.assertIn('observed 2026-09-17',text);self.assertIn('https://fred.stlouisfed.org/series/BAMLH0A0HYM2',text)
        self.assertIn('mixed ratings',text);self.assertIn('no qualified return forecast',text)

    def test_stale_future_untyped_and_tampered_context_withheld(self):
        for at in ('2026-09-22T03:00:00+00:00','2026-09-20T13:00:00+00:00'):
            self.assertFalse(adapter.context(self.packet,datetime.fromisoformat(at))['available'])
        self.assertFalse(adapter.context({'current_bps':{'BAMLH0A0HYM2':2.7}},self.at)['available'])
        for key,value in (('calls_eligible',True),('generated_at','2026-09-20T13:59:00+00:00')):
            p=copy.deepcopy(self.packet);p[key]=value
            self.assertFalse(adapter.context(p,self.at)['available'])
            self.assertIsNone(adapter.qualified_signal(p))
        p=copy.deepcopy(self.packet);p['calls_eligible']=True;p['replay']['output_sha256']=model.sha(model.encoded({k:v for k,v in p.items() if k!='replay'}))
        self.assertFalse(adapter.context(p,self.at)['available'])

    def test_extremes_and_vol_radar_do_not_relabel_another_series_hy(self):
        for fn,start,end in (('market-extremes','    from credit_research import context','    if cred_z is not None:'),
                             ('vol-radar','    from credit_research import context','    euro_score =')):
            text=source(fn);a=text.index(start);b=text.index(end,a)
            ns={'credit':{'metrics':{'OTHER':{'z_score_60d':-99}},'calls_eligible':True}}
            exec(textwrap.dedent(text[a:b]),ns)
            self.assertIsNone(ns['cred_z']);self.assertFalse(ns['credit_research_context']['available'])

    def test_cycle_clock_unqualified_credit_adds_no_neutral_weight(self):
        text=source('cycle-clock');a=text.index('    from credit_research import qualified_signal');b=text.index('    if nowcast_regime',a)
        ns={'credit':self.packet,'rp_max':0,'rp_votes':0}
        exec(textwrap.dedent(text[a:b]),ns)
        self.assertEqual((ns['rp_max'],ns['rp_votes']),(0,0));self.assertIsNone(ns['cr_regime'])

    def test_capitulation_does_not_promote_legacy_crisis_label(self):
        text=source('capitulation');a=text.index('    from credit_research import decision_view');b=text.index('\n',text.index('    credit =',a))+1
        ns={'get_s3_json':lambda key:{'composite_regime':'CRISIS','composite_score':99}}
        exec(textwrap.dedent(text[a:b]),ns)
        self.assertIsNone(ns['credit']['composite_regime']);self.assertNotIn('composite_score',ns['credit'])

    def test_bottleneck_uses_percent_not_corrected_bps_and_no_funding_cost_label(self):
        tree=ast.parse(source('bottleneck-boom'));node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='capital_availability')
        s=Memory();s.objects[store.CURRENT]=model.encoded(self.packet)
        ns={'S3':s,'BUCKET':'test','json':json,'datetime':datetime,'timezone':timezone,'timedelta':timedelta,'_edgar_fts_sic':lambda *args,**kw:(None,[])}
        original=adapter.context(self.packet,self.at)
        exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-capital-availability','exec'),ns)
        with patch.object(adapter,'context',return_value=original),patch('time.sleep'):out=ns['capital_availability']()
        self.assertEqual(out['hy_oas_pct'],self.packet['current_pct']['BAMLH0A0HYM2'])
        self.assertIsNone(out['capital_cost']);self.assertIsNone(out['credit_regime']);self.assertIsNone(out['supply_response_funded'])
        self.assertIn('unclassified',out['read'])

    def test_morning_retains_source_inventory_and_corrects_credit_units(self):
        tree=ast.parse(source('morning-intelligence'));names={n.name for n in tree.body if isinstance(n,ast.FunctionDef)}
        self.assertIn('load_all',names);self.assertIn('extract_metrics',names)
        c=adapter.context(self.packet,self.at)
        with patch.object(adapter,'context',return_value=c):out=adapter.morning_fields(self.packet)
        self.assertEqual(out['credit_hy_minus_ig_unit'],'percentage_points');self.assertEqual(out['credit_hy_observation_date'],'2026-09-17')
        self.assertIsNone(out['credit_composite_regime']);self.assertIsNone(out['credit_hy_z_60d'])
        self.assertIn('"credit_stress":"data/credit-stress.json"',source('morning-intelligence'))

    def test_public_http_and_validation_cannot_resolve_credentials_or_publish(self):
        text=source('credit-stress');node=next(n for n in ast.parse(text).body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        def forbidden(*args,**kwargs):raise AssertionError('Must not publish')
        ns={'json':json,'CONTRACT':model.CONTRACT,'CURRENT':store.CURRENT,'Config':lambda **kwargs:None,
            'boto3':SimpleNamespace(client=lambda *a,**kw:None),'reader':lambda *a:lambda key:model.encoded(self.packet),'run':forbidden}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-credit-handler','exec'),ns)
        h=ns['lambda_handler'];self.assertEqual(h({'httpMethod':'GET'})['statusCode'],200)
        ns['boto3']=SimpleNamespace(client=forbidden)
        self.assertEqual(h({'validate_only':True})['statusCode'],200)

    def test_every_consumer_packages_the_typed_boundary(self):
        sys.path.insert(0,str(ROOT/'aws/ops/checks'));from release_package_evidence import shared_imports
        for fn in FUNCTIONS:
            paths=list((ROOT/'aws/lambdas'/('justhodl-'+fn)/'source').glob('*.py'))
            self.assertIn('credit_research.py',[p.name for p in shared_imports(ROOT,paths)],fn)


def run():
    if not unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(ConsumerCases)).wasSuccessful():raise SystemExit(1)
if __name__=='__main__':run()
