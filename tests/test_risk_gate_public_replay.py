"""Public replay must bind the actual head, typed values and complete code closure."""
from pathlib import Path
from copy import deepcopy
from unittest.mock import patch
import gzip,io,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-risk-gate/tests'),str(ROOT/'scripts')]
import test_risk_gate_research_store as fixture
import replay_risk_gate_research as cli


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client,_,_=fixture.prepared()
        with patch.object(fixture.store,'datetime',fixture.Frozen):fixture.store.run(cls.client,'fixture')
        cls.packet=json.loads(cls.client.objects[fixture.store.CURRENT][0])

    def read(self,key):
        raw=self.client.objects[key][0]
        return gzip.decompress(raw) if key.endswith('.gz') else raw

    def test_complete_native_packet_reproduces_with_typed_authority(self):
        out=cli.verify(self.packet,self.read)
        self.assertTrue(out['replayed']);self.assertIs(out['calls_eligible'],False);self.assertIs(out['sizing_eligible'],False)
        self.assertEqual(out['output_sha256'],self.packet['replay']['output_sha256'])

    def test_permission_type_and_value_changes_cannot_reuse_reference(self):
        for change in (lambda p:p.update(calls_eligible=0),lambda p:p.update(sizing_eligible=0.0),
                       lambda p:p['series']['RRPONTSYD'].update(latest_value=999)):
            bad=deepcopy(self.packet);change(bad)
            with self.assertRaisesRegex(ValueError,'Current packet'):cli.verify(bad,self.read)

    def test_replay_digest_and_compiler_reference_substitution_fail(self):
        for change in (lambda r:r.update(output_sha256='0'*64),lambda r:r.update(compilers={}),lambda r:r.update(extra=True)):
            bad=deepcopy(self.packet);change(bad['replay'])
            with self.assertRaisesRegex(ValueError,'public replay reference'):cli.verify(bad,self.read)
        for key in ('audit-private/private.json','data/risk-gate-research/runs/../current.json','https://example.test/a'):
            bad=deepcopy(self.packet);bad['replay']['manifest_key']=key
            with self.assertRaisesRegex(ValueError,'manifest path'):cli.verify(bad,lambda key:self.fail('Read before path validation'))

    def test_duplicate_nonfinite_or_reencoded_manifest_bytes_are_not_verified(self):
        for raw in (b'{"v":false,"v":0}',b'{"x":NaN}',b'{"x":Infinity}',b'{"x":1e999}'):
            with self.assertRaises(ValueError):cli.strict(raw)
        key=self.packet['replay']['manifest_key'];raw=self.read(key)
        for altered in (raw+b' ',b'{"contract":"risk-gate-replay.v2",'+raw[1:]):
            with self.assertRaises(ValueError):cli.verify(self.packet,lambda path:altered if path==key else self.read(path))

    def test_extra_compilers_cannot_be_silently_ignored(self):
        manifest=cli.strict(self.read(self.packet['replay']['manifest_key']))
        manifest['compilers']['unknown']={'key':'not-read','sha256':'0'*64}
        with self.assertRaisesRegex(ValueError,'compiler closure'):cli.replay(manifest,lambda key:self.fail('Unreviewed closure read'))

    def test_public_reads_are_exact_and_cannot_trigger_acquisition(self):
        for key,body in (('data/risk-gate.json',b'{}'),('data/evidence/fred/test.bin.gz',gzip.compress(b'{}'))):
            with patch.object(cli.urllib.request,'urlopen',return_value=io.BytesIO(body)) as opened:
                self.assertEqual(cli.read_public(key),b'{}')
                self.assertEqual(opened.call_args.args[0].full_url,'https://justhodl.ai/'+key+'?exact=1&nogen=1')
        with patch.object(cli.urllib.request,'urlopen',side_effect=AssertionError('Unexpected network read')):
            for key in ('https://example.test/a','data/../private.json','audit-private/a.json',None):
                with self.assertRaises(ValueError):cli.read_public(key)


if __name__=='__main__':unittest.main(verbosity=2)
