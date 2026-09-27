"""Whole business-cycle publication, authority and recovery boundaries."""
from pathlib import Path
from datetime import datetime, timezone, timedelta
from io import BytesIO
from copy import deepcopy
from unittest.mock import patch
import importlib.util
import ast
import json
import sys
import types
import unittest
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
sys.path.insert(0, str(HERE.parent/'source'))
import business_cycle_store as store
NOW = datetime(2026, 9, 27, 12, tzinfo=timezone.utc)


class Error(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


class Memory:
    def __init__(self):
        self.rows, self.versions, self.writes = {}, {}, []
        self.fail = self.after = None
    def seed(self, key, raw):
        self.rows[key] = raw;self.versions[key] = self.versions.get(key, 0)+1
    def get_object(self, Bucket, Key):
        if self.fail == Key:raise Error('AccessDenied')
        if Key not in self.rows:raise Error('NoSuchKey')
        raw=self.rows[Key]
        return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':str(self.versions[Key])}
    def put_object(self, Bucket, Key, Body, **kw):
        if self.fail == 'retention' and Key.startswith(store.PRIVATE):raise Error('AccessDenied')
        if kw.get('IfNoneMatch') == '*' and Key in self.rows:raise Error('PreconditionFailed')
        if kw.get('IfMatch') is not None and kw['IfMatch'] != str(self.versions.get(Key)):raise Error('PreconditionFailed')
        self.seed(Key,Body);self.writes.append(Key)
        if self.after:self.after(Key)


def fixture():
    memory=Memory()
    for key in store.KEYS:
        memory.seed(key,store.encode({'generated_at':(NOW-timedelta(days=1)).isoformat(),
                    'all_history':list(range(2500)),'legacy_field':{'zero':0,'missing':None}}))
    return memory


def packet():
    return {'generated_at':NOW.isoformat(),'by_country':{'USA':{'phase':'EXPANSION','cli_level':102,'confidence':0.7}},
            'interpretation':{'decisive_call':'Buy aggressively', 'cross_asset':{'gold':{'signal':2,'rationale':'Strategic 5%'}},
                              'country_tilts':{'USA':{'phase':'EXPANSION','tilt':2}}},
            'downturn_probability_6m':{'ok':True,'probability_now':0.73,'coefficients':{'intercept':0.1}},
            'complete_history':list(range(1800))}


class Frozen(datetime):
    @classmethod
    def now(cls,tz=None):return NOW


def module(memory):
    spec=importlib.util.spec_from_file_location('business_cycle_under_test',HERE.parent/'source/lambda_function.py')
    loaded=importlib.util.module_from_spec(spec)
    secret=types.SimpleNamespace(managed_secret=lambda *a,**k:'fixture-only',__file__=str(ROOT/'aws/shared/managed_secret.py'))
    with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**kw:memory),
                                'managed_secret':secret,'_fred_shim':types.SimpleNamespace()}):
        spec.loader.exec_module(loaded)
    loaded.datetime=Frozen
    return loaded


class Tests(unittest.TestCase):
    def test_complete_predecessor_calculations_and_inputs_are_unchanged(self):
        previous=ast.parse((ROOT/'tests/fixtures/pre-research-global-business-cycle.py.txt').read_text(encoding='utf-8'))
        current=ast.parse((HERE.parent/'source/lambda_function.py').read_text(encoding='utf-8'))
        current.body=[n for n in current.body if not (isinstance(n,ast.Import) and any(a.name=='business_cycle_store' for a in n.names))
                      and not (isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')]
        for tree in (previous,current):
            for node in tree.body:
                if isinstance(node,ast.FunctionDef) and node.name in ('lambda_handler','_produce'):node.name='_produce'
                if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='ENGINE_VERSION' for t in node.targets):node.value=ast.Constant('reviewed-version-change')
            # The module documentation's version changes, not its calculations.
            tree.body=tree.body[1:]
        self.assertEqual(ast.dump(current),ast.dump(previous))

    def test_compiler_closure_cannot_be_partial_extra_or_nonhex(self):
        for compilers in ({'lambda_function.py':'a'*64},{**{n:'a'*64 for n in store.COMPILERS},'extra.py':'b'*64},
                          {n:'x'*64 for n in store.COMPILERS}):
            memory=fixture();pub=store.PublicationClient(memory,'b',NOW.isoformat())
            for key in store.KEYS:pub.put_object(Bucket='b',Key=key,Body=store.encode(packet()))
            with self.assertRaises(store.PublicationError):pub.finish(compilers)
            self.assertFalse(any(k in store.KEYS for k in memory.writes))

    def test_every_original_field_and_history_survives_before_advice_is_withheld(self):
        memory=fixture();old=deepcopy(memory.rows);pub=store.PublicationClient(memory,'b',NOW.isoformat());value=packet()
        for key in store.KEYS:pub.put_object(Bucket='b',Key=key,Body=store.encode(value))
        self.assertFalse(any(key in store.KEYS for key in memory.writes))
        proof=pub.finish({name:'a'*64 for name in store.COMPILERS})
        for key in store.KEYS:
            result=store.strict(memory.rows[key]);context=result['publication_context']
            self.assertEqual(memory.rows[context['predecessors'][key]['key']],old[key])
            self.assertEqual(store.strict(memory.rows[context['complete_unmodified_calculations'][key]['key']]),value)
            self.assertEqual(result['complete_history'],value['complete_history'])
            self.assertEqual(result['unqualified_legacy_interpretation'],value['interpretation'])
            self.assertEqual(result['unqualified_in_sample_fit'],value['downturn_probability_6m'])
            self.assertIsNone(result['interpretation']['cross_asset']['gold']['signal'])
            self.assertIsNone(result['interpretation']['country_tilts']['USA']['tilt'])
            self.assertIsNone(result['downturn_probability_6m']['probability_now'])
            self.assertTrue(all(result[k] is False for k in store.PERMISSIONS))
            self.assertEqual(result['decision']['verb'],'WAIT')
        self.assertEqual(store.strict(memory.rows[proof['attempt']['key']])['status'],'planned_bytes_only')
        self.assertEqual([k for k in memory.writes if k in store.KEYS],[store.HISTORY,store.COMPOSITE,store.HEAD])

    def test_actual_entrypoint_stages_every_output_and_restores_client(self):
        memory=fixture();engine=module(memory);native_client=engine.S3
        def produce(*args):
            for key in store.KEYS:engine.S3.put_object(Bucket=engine.BUCKET,Key=key,Body=store.encode(packet()))
            self.assertFalse(any(key in store.KEYS for key in memory.writes))
            return {'statusCode':200,'body':'legacy result'}
        engine._produce=produce
        with patch.object(store,'compiler_hashes',return_value={name:'a'*64 for name in store.COMPILERS}):result=engine.lambda_handler()
        self.assertIs(engine.S3,native_client)
        self.assertEqual(json.loads(result['body'])['portfolio_action'],'WAIT')
        self.assertNotIn('global_phase',json.loads(result['body']))

    def test_producer_failure_never_publishes_partial_calculations(self):
        memory=fixture();engine=module(memory);before=deepcopy(memory.rows)
        def produce(*args):
            engine.S3.put_object(Bucket=engine.BUCKET,Key=store.HEAD,Body=store.encode(packet()))
            raise ValueError('history failed')
        engine._produce=produce
        with self.assertRaises(ValueError):engine.lambda_handler()
        self.assertIs(engine.S3,memory)
        self.assertTrue(all(memory.rows[k]==before[k] for k in store.KEYS))

    def test_missing_composite_is_explicit_and_never_erases_its_previous_history(self):
        memory=fixture();old=memory.rows[store.COMPOSITE];pub=store.PublicationClient(memory,'b',NOW.isoformat())
        for key in (store.HEAD,store.HISTORY):pub.put_object(Bucket='b',Key=key,Body=store.encode(packet()))
        pub.finish({name:'a'*64 for name in store.COMPILERS})
        self.assertEqual(memory.rows[store.COMPOSITE],old)
        self.assertEqual(store.strict(memory.rows[store.HEAD])['publication_context']['unchanged_output_keys'],[store.COMPOSITE])

    def test_denied_corrupt_future_or_unretained_predecessors_abort_before_acquisition(self):
        for mode in ('denied','corrupt','duplicate','future','retention','length'):
            memory=fixture();engine=module(memory);calls=[];engine._produce=lambda *a:calls.append('provider')
            if mode=='denied':memory.fail=store.HISTORY
            if mode=='corrupt':memory.seed(store.HEAD,b'broken')
            if mode=='duplicate':memory.seed(store.HEAD,b'{"generated_at":"x","generated_at":"y"}')
            if mode=='future':memory.seed(store.HEAD,store.encode({'generated_at':(NOW+timedelta(seconds=1)).isoformat()}))
            if mode=='retention':memory.fail='retention'
            if mode=='length':
                get=memory.get_object;memory.get_object=lambda **kw:{**get(**kw),'ContentLength':0}
            with self.subTest(mode=mode),self.assertRaises(Exception):engine.lambda_handler()
            self.assertEqual(calls,[])
            self.assertFalse(any(k in store.KEYS for k in memory.writes))

    def test_concurrent_writer_is_never_overwritten_or_rolled_back(self):
        for during in (False,True):
            memory=fixture();pub=store.PublicationClient(memory,'b',NOW.isoformat())
            for key in store.KEYS:pub.put_object(Bucket='b',Key=key,Body=store.encode(packet()))
            newer=store.encode({'generated_at':(NOW+timedelta(seconds=1)).isoformat(),'other_writer':True})
            if during:memory.after=lambda key:memory.seed(store.HEAD,newer) if key==store.HISTORY else None
            else:memory.seed(store.HEAD,newer)
            with self.assertRaises((store.PublicationError,Error)):pub.finish({name:'a'*64 for name in store.COMPILERS})
            self.assertEqual(memory.rows[store.HEAD],newer)
            self.assertFalse(store.HEAD in memory.writes)

    def test_unknown_private_writes_duplicate_outputs_nonfinite_and_missing_history_fail(self):
        memory=fixture();pub=store.PublicationClient(memory,'b',NOW.isoformat())
        for key in ('data/portfolio.json',store.PRIVATE+'x.bin'):
            with self.assertRaises(store.PublicationError):pub.put_object(Bucket='b',Key=key,Body=b'{}')
        with self.assertRaises(store.PublicationError):pub.put_object(Bucket='b',Key=store.HEAD,Body=b'{"generated_at":"2026-09-27T12:00:00Z","x":NaN}')
        pub.put_object(Bucket='b',Key=store.HEAD,Body=store.encode(packet()))
        with self.assertRaises(store.PublicationError):pub.put_object(Bucket='b',Key=store.HEAD,Body=store.encode(packet()))
        with self.assertRaises(store.PublicationError):pub.finish({name:'a'*64 for name in store.COMPILERS})
        self.assertFalse(any(k in store.KEYS for k in memory.writes))

    def test_every_legacy_phase_loses_all_tilts_without_destroying_the_calculation(self):
        engine=module(Memory())
        for phase in ('GLOBAL_EXPANSION','GLOBAL_PEAKING','GLOBAL_CONTRACTION','GLOBAL_RECOVERY','MIXED'):
            agg={'global_phase':phase,'global_avg_cli':100,'expansion_breadth_pct':60,'contraction_breadth_pct':40,'global_phase_mix_pct':{'RECOVERY':30}}
            original=engine.interpret_global_cycle(agg,{'USA':{'phase':'EXPANSION'}})
            result=store.research_projection({'generated_at':NOW.isoformat(),'interpretation':original})
            self.assertEqual(result['unqualified_legacy_interpretation'],original)
            self.assertTrue(all(v['signal'] is None for v in result['interpretation']['cross_asset'].values()))
            self.assertTrue(all(v['tilt'] is None for v in result['interpretation']['country_tilts'].values()))


if __name__=='__main__':unittest.main(verbosity=2)
