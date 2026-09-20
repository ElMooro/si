"""Execute dealer authority boundaries with concrete original-research fixtures."""
import ast,copy,importlib.util,json,sys,unittest
from datetime import datetime,timezone
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-nyfed-pd/source')]
from dealer_research_context import project
spec=importlib.util.spec_from_file_location('dealer_fixtures',ROOT/'aws/lambdas/justhodl-nyfed-pd/tests/test_research.py')
fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
AT=datetime.fromisoformat(fixture.AT)


def packet():
    p=fixture.original_result();p['replay']={'manifest_key':'data/dealer-research/runs/'+'a'*64+'.json','output_sha256':'b'*64};return p


def functions(name,wanted,scope):
    text=(ROOT/'aws/lambdas'/name/'source/lambda_function.py').read_text(encoding='utf-8');tree=ast.parse(text)
    nodes=[x for x in tree.body if isinstance(x,ast.FunctionDef) and x.name in wanted]
    assert len(nodes)==len(wanted)
    exec(compile(ast.Module(body=nodes,type_ignores=[]),name,'exec'),scope)
    return text


class DealerConsumers(unittest.TestCase):
    def test_typed_projection_preserves_units_dated_sums_and_no_authority(self):
        p=packet();result=project(p,AT)
        self.assertEqual(result['corporate']['net_bonds_b'],p['corporate']['net_bonds_b'])
        self.assertEqual(result['treasury_financing']['gross_two_sided_b'],p['financing']['treasury']['gross_two_sided_b'])
        self.assertFalse(result['calls_eligible']);self.assertFalse(result['sizing_eligible']);self.assertIsNone(result['corporate']['regime'])
        self.assertEqual(result['source_replay'],p['replay'])

    def test_stale_future_bad_scope_and_corrupted_components_withheld(self):
        p=packet()
        for change in (lambda x:x.update(generated_at='2020-01-01T00:00:00Z'),lambda x:x.update(contract='old'),
                       lambda x:x.update(call='LONG'),lambda x:x.update(calls_eligible=True),lambda x:x['replay'].update(manifest_key='data/private.json')):
            q=copy.deepcopy(p);change(q);self.assertEqual(project(q,AT)['status'],'unavailable')
        for change in (lambda g:g.update(usd_mn=0),lambda g:g.update(unit='USD_par'),
                       lambda g:g['components']['PDPOSCSBND-L13'].update(usd_mn=False),
                       lambda g:g['components']['PDPOSCSBND-L13'].update(date='2020-01-01'),
                       lambda g:g['quality'].update(acquired_at='2020-01-01T00:00:00Z')):
            q=copy.deepcopy(p);change(q['groups']['corp_bonds']);self.assertIsNone(project(q,AT)['corporate'])

    def test_credit_stress_actual_dealer_reader_uses_context_and_not_squeeze(self):
        sys.path.insert(0,str(ROOT/'aws/lambdas/justhodl-credit-stress/tests'))
        from credit_fixtures import fixture as credit_fixture
        from native_credit_tests import Memory
        import credit_research_store as credit_store
        inputs,bodies=credit_fixture();c=Memory();c.objects.update(bodies)
        c.objects['data/nyfed-primary-dealer.json']=json.dumps(packet()).encode()
        inputs['dealer']=credit_store.collect_dealer(c,'fixture')
        result=credit_store.compile_output(inputs,credit_store.reader(c,'fixture'))['dealer_positioning']
        self.assertIsNotNone(result);self.assertIsNone(result['squeeze_setup']);self.assertFalse(result['calls_eligible'])
        c.objects['data/nyfed-primary-dealer.json']=b'{"corporate":{"net_bonds_b":-100,"squeeze_setup":true}}'
        inputs['dealer']=credit_store.collect_dealer(c,'fixture')
        self.assertIsNone(credit_store.compile_output(inputs,credit_store.reader(c,'fixture'))['dealer_positioning'])

    def test_credit_composite_actual_handler_never_emits_or_authorizes(self):
        c=fixture.Storage();legacy=b'{"generated_at":"2026-01-01T00:00:00Z","composite":99,"plans":[{"etf":"HYG"}]}'
        c.objects['data/credit-composite.json']=legacy;c.objects['data/nyfed-primary-dealer.json']=json.dumps(packet()).encode()
        scope={'s3':c,'S3_BUCKET':'fixture','OUT_KEY':'data/credit-composite.json','json':json,'datetime':datetime,'timezone':timezone}
        functions('justhodl-credit-composite',{'rj','score_lenses','lambda_handler'},scope)
        result=scope['lambda_handler']({'_probe':{'nyfed':{'corporate':{'squeeze_setup':True}}}})
        self.assertEqual(result['statusCode'],200);out=json.loads(c.objects['data/credit-composite.json'])
        self.assertEqual(out['plans'],[]);self.assertEqual(out['logged'],0);self.assertIsNone(out['composite'])
        self.assertTrue(all(v['pts'] is None for v in out['lenses'].values()))
        self.assertIn(legacy,c.objects.values());self.assertTrue(out['legacy_retention']['protected_backup'])
        before=copy.deepcopy(c.objects);response=scope['lambda_handler']({'requestContext':{'http':{'method':'GET'}}})
        self.assertEqual(response['statusCode'],200);self.assertEqual(before,c.objects)
        self.assertIsNone(scope['score_lenses']({'nyfed':[],'stfm':[1]})[1])

    def test_footprint_assignment_cannot_fuzzy_substitute_specific_issues(self):
        source=(ROOT/'aws/lambdas/justhodl-institutional-footprint/source/lambda_function.py').read_text(encoding='utf-8')
        block=source[source.index('    # Exact typed dealer'):source.index('    # ── RISK-NOW composite')]
        scope={'F':{'data/nyfed-primary-dealer.json':packet()}}
        import textwrap
        exec(compile(textwrap.dedent(block),'actual-footprint-dealer-block','exec'),scope)
        self.assertIsNone(scope['pd_pos']);self.assertIsNone(scope['pd_block']['net_treasury_b'])
        self.assertIsNotNone(scope['pd_block']['coupons_by_tenor_b']);self.assertFalse(scope['pd_block']['sizing_eligible'])

    def test_sentinel_assignment_preserves_no_directional_alert(self):
        source=(ROOT/'aws/lambdas/justhodl-alert-sentinel/source/lambda_function.py').read_text(encoding='utf-8')
        block=source[source.index('    pd_ = gj("data/nyfed-primary-dealer.json")'):source.index('    sf_ = gj("data/settlement-fails.json")')]
        scope={'s':{},'gj':lambda _:packet()};import textwrap
        exec(compile(textwrap.dedent(block),'actual-sentinel-dealer-block','exec'),scope)
        self.assertIsNone(scope['s']['dealer_regime']);self.assertIsNone(scope['s']['dealer_squeeze']);self.assertIsNotNone(scope['s']['dealer_source_replay'])

    def test_collateral_assignment_keeps_original_financing_context_without_score(self):
        source=(ROOT/'aws/lambdas/justhodl-treasury-rehypo/source/lambda_function.py').read_text(encoding='utf-8')
        block=source[source.index("    if dealer.get('contract')=='dealer-original-research.v1':"):source.index('    # OFR uses different clearing')]
        scope={'dealer':packet(),'dc':{'fixture':True}};import textwrap
        exec(compile(textwrap.dedent(block),'actual-collateral-dealer-block','exec'),scope)
        result=scope['financing_context'];self.assertIsNotNone(result['data']);self.assertEqual(result['score_contribution'],0);self.assertFalse(result['sizing_eligible'])

    def test_morning_assignment_uses_dated_measurements_without_squeeze(self):
        source=(ROOT/'aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py').read_text(encoding='utf-8')
        block=source[source.index('    from dealer_research_context import project as dealer_context'):source.index('    _of=data.get("ofr_stfm")')]
        scope={'data':{'primary_dealers':packet()}};import textwrap
        exec(compile(textwrap.dedent(block),'actual-morning-dealer-block','exec'),scope)
        self.assertIsNotNone(scope['_pc']['net_bonds_b']);self.assertIsNone(scope['_pc']['squeeze_setup']);self.assertFalse(scope['_pd_context']['sizing_eligible'])


def run(name=None):
    mapping={'nyfed-pd':['test_typed_projection_preserves_units_dated_sums_and_no_authority','test_stale_future_bad_scope_and_corrupted_components_withheld'],
             'credit-stress':['test_credit_stress_actual_dealer_reader_uses_context_and_not_squeeze'],
             'credit-composite':['test_credit_composite_actual_handler_never_emits_or_authorizes'],
             'institutional-footprint':['test_footprint_assignment_cannot_fuzzy_substitute_specific_issues'],
             'alert-sentinel':['test_sentinel_assignment_preserves_no_directional_alert'],
             'treasury-rehypo':['test_collateral_assignment_keeps_original_financing_context_without_score'],
             'morning-intelligence':['test_morning_assignment_uses_dated_measurements_without_squeeze']}
    suite=unittest.TestSuite(DealerConsumers(method) for method in mapping[name]) if name else unittest.defaultTestLoader.loadTestsFromTestCase(DealerConsumers)
    return unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful()


if __name__=='__main__':sys.exit(0 if run(sys.argv[1] if len(sys.argv)>1 else None) else 1)
