"""Complete invented publication frames, never account reads or archived execution."""
from pathlib import Path
from decimal import Decimal
from datetime import datetime
import ast,copy,gzip,hashlib,json,math,sys,unittest
import test_research_enrichment as research_support
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-portfolio-risk/source')]
from portfolio_risk_model import snapshot_value_identity

def restore(value):
    if isinstance(value,dict):
        if set(value)=={'invented_input_type','exact_literal'} and value['invented_input_type']=='Decimal':return Decimal(value['exact_literal'])
        return {key:restore(item) for key,item in value.items()}
    if isinstance(value,list):return [restore(item) for item in value]
    return value

class PublicationCompatibility(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture=json.loads(gzip.decompress((ROOT/'tests/fixtures/pre-snapshot-publication-compatibility/complete-synthetic.json.gz').read_bytes()))
    def setUp(self):self.support=research_support.Research();self.support.setUp();self.mod=self.support.mod
    def test_complete_inert_predecessor_and_all_failure_frames_are_hash_bound(self):
        audit=json.loads((ROOT/'docs/audit/2026-09-30/portfolio-publication-compatibility.json').read_bytes())
        for row in audit['fixtures'].values():
            raw=(ROOT/row['path']).read_bytes();self.assertEqual(len(raw),row['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),row['sha256'])
        self.assertEqual(len(self.fixture['cases']),3)
        for row in self.fixture['cases']:
            self.assertEqual(row['write_count'],2)
            with self.assertRaises(ValueError):snapshot_value_identity(row['output'])
        self.assertEqual(len(self.fixture['cases'][-1]['complete_invented_inputs']['documents']['screener/alpha-score.json']['stocks'][0]['risk_flags']),1600000)
    def test_only_new_guard_and_handler_change_from_exact_predecessor(self):
        def functions(path):return {n.name:ast.dump(n,include_attributes=False) for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef)}
        before=functions(ROOT/'tests/fixtures/pre-snapshot-publication-compatibility/lambda_function.py.txt');after=functions(ROOT/'aws/lambdas/justhodl-portfolio-snapshot/source/lambda_function.py')
        self.assertEqual(set(after)-set(before),{'validate_snapshot_publication'})
        self.assertEqual({name for name in before if before[name]!=after[name]},{'lambda_handler'})
    def test_cross_runtime_vectors_match_existing_identity_byte_count(self):
        for row in json.loads((ROOT/'tests/fixtures/portfolio-value-identity-vectors.json').read_bytes()):
            self.assertEqual(self.mod.validate_snapshot_publication(row['value']),row['encoded_bytes'])
    def test_scalars_and_unicode_counts_match_real_risk_consumer(self):
        for value in [None,True,False,0,-0.0,1e-300,2**53-1,-(2**53-1),'','π 😀',[],{},[False,0,None],{'😀':[], 'a':{'x':1}},[{'a':'x'*n,'b':[0,False,None]} for n in range(32)]]:
            self.assertEqual(self.mod.validate_snapshot_publication(value),snapshot_value_identity(value)['encoded_bytes'])
    def test_object_order_does_not_change_size_and_every_array_occurrence_counts(self):
        v=self.mod.validate_snapshot_publication
        self.assertEqual(v({'a':0,'b':None}),v({'b':None,'a':0}))
        self.assertEqual(v([0]*10)-v([0]*9),10)  # 9 numeric bytes plus one extra count digit.
        self.assertNotEqual(v({'a':0}),v({'a':0,'unused':None}))
    def test_exact_byte_boundary_is_accepted_and_one_byte_over_is_refused(self):
        value={'x':'π'};size=snapshot_value_identity(value)['encoded_bytes']
        self.mod.SNAPSHOT_IDENTITY_MAX_BYTES=size;self.assertEqual(self.mod.validate_snapshot_publication(value),size)
        self.mod.SNAPSHOT_IDENTITY_MAX_BYTES=size-1
        with self.assertRaisesRegex(ValueError,'identity byte bound'):self.mod.validate_snapshot_publication(value)
    def test_unsafe_and_nonfinite_numbers_are_never_rounded_to_fit(self):
        for value in (2**53,-2**53,10**400,float(2**53),float('nan'),float('inf'),-float('inf')):
            with self.subTest(value=type(value).__name__),self.assertRaisesRegex(ValueError,'consumer-unsafe number'):self.mod.validate_snapshot_publication({'value':value})
    def test_unsupported_types_and_non_string_keys_are_refused(self):
        for value in (Decimal('1'),datetime(2026,1,1),(1,),{1:'a'},set(),b'{}'):
            with self.assertRaisesRegex(ValueError,'unsupported'):self.mod.validate_snapshot_publication(value)
    def test_invalid_unicode_is_redacted_without_copying_the_value(self):
        for value in ({'note':'INVENTED_SECRET\ud800'},{'INVENTED_SECRET\udfff':0}):
            with self.assertRaisesRegex(ValueError,'unsupported Unicode') as error:self.mod.validate_snapshot_publication(value)
            self.assertNotIn('INVENTED_SECRET',str(error.exception))
    def test_exact_nesting_boundary_matches_consumer(self):
        value=None
        for _ in range(128):value=[value]
        self.assertEqual(self.mod.validate_snapshot_publication(value),snapshot_value_identity(value)['encoded_bytes'])
        with self.assertRaisesRegex(ValueError,'nesting bound'):self.mod.validate_snapshot_publication([value])
    def test_cycle_refuses_boundedly_without_mutating_input(self):
        value=[];value.append(value)
        with self.assertRaisesRegex(ValueError,'nesting bound'):self.mod.validate_snapshot_publication(value)
        self.assertIs(value[0],value)
    def test_validated_values_are_not_changed(self):
        value={'n':-0.0,'mixed':[0,False,None,'π'], 'unknown':{'preserve':'all'}};before=copy.deepcopy(value)
        self.mod.validate_snapshot_publication(value);self.assertEqual(value,before);self.assertEqual(math.copysign(1,value['n']),-1)
    def test_unsafe_quantity_and_deep_note_prevent_both_publishers(self):
        for row in self.fixture['cases'][:2]:
            support=research_support.Research();support.setUp()
            with self.assertRaisesRegex(ValueError,'consumer-unsafe number|nesting bound'):support.handler(**restore(row['complete_invented_inputs']))
            self.assertEqual(support.writes,[])
    def test_actual_numeric_expansion_frame_prevents_both_publishers(self):
        row=self.fixture['cases'][-1]
        self.assertLess(len(json.dumps(row['output']).encode('utf-8')),self.mod.SNAPSHOT_MIRROR_MAX_BYTES)
        with self.assertRaisesRegex(ValueError,'identity byte bound'):self.support.handler(**row['complete_invented_inputs'])
        self.assertEqual(self.support.writes,[])
    def test_valid_complete_handler_frame_is_publishable_and_preserves_inputs(self):
        docs=self.support.documents([{'symbol':'AAA','alpha_score':0,'risk_flags':['a','b','c','d']}]);before=copy.deepcopy(docs)
        result,payload=self.support.handler(documents=docs,positions=[{'symbol':'AAA','qty':Decimal('2'),'cost_basis_per_share':Decimal('1.25')}])
        self.assertEqual(result['statusCode'],200);self.assertEqual(len(self.support.writes),2);self.assertEqual(docs,before)
        self.assertEqual(self.mod.validate_snapshot_publication(payload),snapshot_value_identity(payload)['encoded_bytes'])
        self.assertFalse(payload['capital_book']['allows_new_entries']);self.assertEqual(payload['positions'][0]['risk_flags'],['a','b','c','d'])

if __name__=='__main__':unittest.main()
