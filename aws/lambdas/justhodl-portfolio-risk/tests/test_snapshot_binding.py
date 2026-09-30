"""Complete synthetic snapshot identity and acquisition; no account or network."""
from copy import deepcopy
import io,json,math
from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared'),str(Path(__file__).resolve().parents[1]/'source')]
import portfolio_risk_model as model
from private_portfolio_test_support import Store,load


class Chunks(io.BytesIO):
    def read(self,n=-1):return super().read(min(n,3))


class SnapshotBinding(unittest.TestCase):
    def setUp(self):
        self.predecessor=json.loads((ROOT/'tests/fixtures/pre-portfolio-coherence-native.json').read_bytes())
        self.store=Store();_,_,self.env=load('portfolio-risk',self.store)
        self.read=self.env['snapshot_document']

    def test_complete_predecessor_math_is_unchanged(self):
        before=deepcopy(self.predecessor['bundle']['inputs'])
        output=model.build(**before);binding=output.pop('snapshot_binding')
        self.assertEqual(output,self.predecessor['output'])
        self.assertEqual(binding,{'contract':'portfolio-snapshot-value.v1','key':'portfolio/snapshot.json',
            'generated_at':before['snapshot']['generated_at'],**model.snapshot_value_identity(before['snapshot'])})
        self.assertEqual(before,self.predecessor['bundle']['inputs'])

    def test_current_complete_bundle_replays_with_binding(self):
        retained=json.loads((ROOT/'tests/fixtures/portfolio-coherence-bound-synthetic.json').read_bytes())
        # Retain the whole old compiler-bound bundle; execute only current code.
        with self.assertRaisesRegex(ValueError,'code/schema mismatch'):model.replay(retained['bundle'])
        current,output=model.freeze(**retained['bundle']['inputs'])
        self.assertEqual(model.replay(current),output);self.assertEqual(output,retained['output'])
        bundle,output=model.freeze(**self.predecessor['bundle']['inputs'])
        self.assertEqual(model.replay(bundle),output)
        altered=deepcopy(bundle);altered['inputs']['snapshot']['positions'][0]['qty']+=1
        with self.assertRaises(ValueError):model.replay(altered)

    def test_cross_runtime_vectors(self):
        for row in json.loads((ROOT/'tests/fixtures/portfolio-value-identity-vectors.json').read_bytes()):
            self.assertEqual(model.snapshot_value_identity(row['value']),{k:v for k,v in row.items() if k!='value'})

    def test_all_values_and_array_order_are_bound_but_object_order_is_not(self):
        identity=model.snapshot_value_identity
        self.assertEqual(identity({'a':1,'b':[False,None,-0.0]}),identity({'b':[False,None,0],'a':1.0}))
        for a,b in [([1,2],[2,1]),(False,0),(None,0),('1',1),({'x':1},{'x':1,'unused':None})]:self.assertNotEqual(identity(a),identity(b))
        original=self.predecessor['bundle']['inputs']['snapshot']
        for key,value in [('qty',11),('market_value',999),('sector','Other'),('price_asof_unix_ms',0)]:
            changed=deepcopy(original);changed['positions'][0][key]=value
            self.assertNotEqual(identity(changed),identity(original))

    def test_unrepresentable_and_non_json_values_rejected(self):
        for value in [math.nan,math.inf,9007199254740992,10**400,'\ud800',{1:'invalid'},(1,2)]:
            with self.assertRaises((ValueError,UnicodeError)):model.snapshot_value_identity(value)
        deep=None
        for _ in range(130):deep=[deep]
        with self.assertRaises(ValueError):model.snapshot_value_identity(deep)

    def test_short_chunks_read_through_eof_and_close(self):
        document=self.predecessor['bundle']['inputs']['snapshot'];raw=json.dumps(document).encode()
        stream=Chunks(raw);self.assertEqual(self.read({'Body':stream,'ContentLength':len(raw)}),document)
        self.assertTrue(stream.closed)

    def test_declared_length_must_be_typed_and_complete(self):
        for declared in [True,'2',2.0,-1,4*1024*1024+1,3]:
            stream=Chunks(b'{}')
            with self.assertRaises(ValueError):self.read({'Body':stream,'ContentLength':declared})
            self.assertTrue(stream.closed)

    def test_unambiguous_complete_unicode_json_required(self):
        for raw in [b'{"nav":null,"nav":2000}',b'{"a":1,"\\u0061":2}',b'{"x":NaN}',b'{"x":1e999}',b'{"x":"\\ud800"}',b'{"x":"\xff"}',b'{}{}',b'[]']:
            stream=Chunks(raw)
            with self.assertRaises((ValueError,UnicodeError)):self.read({'Body':stream,'ContentLength':len(raw)})
            self.assertTrue(stream.closed)

    def test_oversize_and_nonbyte_bodies_close_without_prefix_success(self):
        stream=io.BytesIO(b' '*(4*1024*1024)+b'{}')
        with self.assertRaises(ValueError):self.read({'Body':stream})
        self.assertTrue(stream.closed)
        stream=io.StringIO('{}')
        with self.assertRaises(ValueError):self.read({'Body':stream})
        self.assertTrue(stream.closed)

    def test_bad_snapshot_cannot_reach_provider_or_publication(self):
        class Broken(Store):
            def get_object(self,**request):
                self.reads.append(request['Key']);return {'Body':io.BytesIO(b'{"positions":[],"positions":[]}')}
        store=Broken();_,mirrors,env=load('portfolio-risk',store)
        env['batch_fetch_bars']=lambda *a: self.fail('Provider work after invalid snapshot')
        with self.assertRaises(ValueError):env['_run_private']({},None)
        self.assertEqual(store.reads,['portfolio/snapshot.json']);self.assertEqual(store.writes,[]);self.assertEqual(mirrors,[])


if __name__=='__main__':unittest.main()
