from pathlib import Path
from unittest.mock import Mock,patch
import ast,hashlib,json,sys,unittest,urllib.error
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/checks','aws/ops/staged','aws/ops')]
import share_structure_batch_runner as batches
import ops_6099_share_structure_batch3_recovery as recovery
import ops_6098_share_structure_batch3_limit_check as diagnostic


class Tests(unittest.TestCase):
    def test_failed_parent_is_accepted_only_by_the_reviewed_recovery_link(self):
        record=json.loads((ROOT/'tests/fixtures/share-structure-batch3-recovery.json').read_bytes())
        raw=(ROOT/record['predecessor']).read_bytes();self.assertEqual((len(raw),hashlib.sha256(raw).hexdigest()),(record['bytes'],record['sha256']))
        self.assertEqual(batches.accepted_batch_report(3,{}),'ops_6085_share_structure_sources_part_3.md')
        state={'accepted_by_ops':'ops_6099_share_structure_batch3_recovery','recovered_from':diagnostic.FAILED}
        self.assertEqual(batches.accepted_batch_report(3,state),'ops_6099_share_structure_batch3_recovery.md')
        with self.assertRaises(AssertionError):batches.accepted_batch_report(3,{**state,'recovered_from':{}})

    def test_transport_paces_every_new_start_without_retrying_429(self):
        calls=[];rate=Mock();rate.acquire.side_effect=lambda:calls.append('pace');opener=Mock()
        opener.open.side_effect=lambda *a,**k:calls.append('request') or 'ok'
        with patch.object(recovery.source,'Rate',return_value=rate) as factory,patch.object(recovery.urllib.request,'build_opener',return_value=opener):
            transport=recovery.limited_transport();factory.assert_called_once_with(interval=2.0)
            self.assertEqual(transport('request',25),'ok');self.assertEqual(calls,['pace','request'])
            opener.open.side_effect=urllib.error.HTTPError('https://financialmodelingprep.com/stable/quote',429,'limit',{},None)
            with self.assertRaises(urllib.error.HTTPError):transport('request',25)
            self.assertEqual(opener.open.call_count,2)

    def test_retained_success_and_canary_adoption_avoid_provider_calls_and_keep_original_origins(self):
        original={'spec':{'url':'success'},'original':{'key':'body'}}
        canary={'spec':{'url':'canary'},'original':{'key':'canary-body'}}
        def read(client,ref):return json.dumps(original).encode() if ref['key']=='capture' else b'whole-original'
        with patch.object(recovery.source,'read',side_effect=read):
            adopt=recovery.adopter(None,{'captures':{'success':{'key':'capture'}}},canary,{'key':'canary-origin'},{'captures':{}},{'captures':{}})
            self.assertEqual(adopt({'url':'success'}),(original,b'whole-original',diagnostic.FAILED))
            self.assertEqual(adopt({'url':'canary'}),(canary,b'whole-original',{'key':'canary-origin'}))
            self.assertIsNone(adopt({'url':'not_requested'}))

    def test_no_frozen_plan_module_was_changed(self):
        # Existing full-population plan fixture pins every collector dependency.
        path=ROOT/'tests/fixtures/share-structure-batch3-recovery.json'
        record=json.loads(path.read_bytes())
        for name,digest in record['frozen_collectors'].items():
            self.assertEqual(hashlib.sha256((ROOT/('aws/ops/checks/'+name+'.py')).read_bytes()).hexdigest(),digest)
        tree=ast.parse((ROOT/'aws/ops/staged/ops_6098_share_structure_batch3_limit_check.py').read_text(encoding='utf-8'))
        assertions=[ast.unparse(n.test) for n in ast.walk(tree) if isinstance(n,ast.Assert)]
        self.assertIn('elapsed >= 1800',assertions)


if __name__=='__main__':unittest.main(verbosity=2)
