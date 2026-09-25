from pathlib import Path
from unittest.mock import Mock,patch
import ast,hashlib,json,sys,unittest,urllib.error
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/ops/checks')]
import share_structure_batch_runner as runner


class Tests(unittest.TestCase):
    def test_previous_runner_and_failed_part_cannot_be_silently_reclassified(self):
        record=json.loads((ROOT/'tests/fixtures/share-structure-batch-recovery.json').read_bytes())
        raw=(ROOT/record['predecessor']).read_bytes()
        self.assertEqual((len(raw),hashlib.sha256(raw).hexdigest()),(record['bytes'],record['sha256']))
        for previous in record['related_predecessors']:
            body=(ROOT/previous['predecessor']).read_bytes()
            self.assertEqual((len(body),hashlib.sha256(body).hexdigest()),(previous['bytes'],previous['sha256']))
        self.assertEqual(runner.accepted_batch_report(2,{}),'ops_6084_share_structure_sources_part_2.md')
        linked={'accepted_by_ops':'ops_6093_share_structure_sources_recovery','recovered_from':runner.FAILED_BATCH}
        self.assertEqual(runner.accepted_batch_report(2,linked),'ops_6093_share_structure_sources_recovery.md')
        self.assertEqual(runner.accepted_batch_report(3,linked),'ops_6085_share_structure_sources_part_3.md')
        with self.assertRaises(AssertionError):runner.accepted_batch_report(2,{**linked,'recovered_from':{}})

    def test_every_transport_start_is_paced_without_retrying_an_http_error(self):
        calls=[];rate=Mock();rate.acquire.side_effect=lambda:calls.append('pace')
        opener=Mock();opener.open.side_effect=lambda *a,**kw:calls.append('request') or 'response'
        with patch.object(runner.source,'Rate',return_value=rate) as factory,patch.object(runner.urllib.request,'build_opener',return_value=opener):
            transport=runner.limited_transport();factory.assert_called_once_with(interval=1.0)
            self.assertEqual(transport('one',25),'response');self.assertEqual(transport('two',25),'response')
            self.assertEqual(calls,['pace','request','pace','request'])
            opener.open.side_effect=urllib.error.HTTPError('https://financialmodelingprep.com/stable/quote',429,'limit',{},None)
            with self.assertRaises(urllib.error.HTTPError):transport('three',25)
            self.assertEqual(opener.open.call_count,3)

    def test_successful_canary_adoption_does_not_overwrite_the_original_probe_manifest(self):
        tree=ast.parse((ROOT/'aws/ops/checks/share_structure_batch_runner.py').read_text(encoding='utf-8'))
        adoption=next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name=='adoption')
        request={'url':'canary'};canary={'spec':request,'original':{'key':'canary-body'}}
        source=Mock();source.read.side_effect=lambda s3,ref: b'original' if ref['key']=='canary-body' else json.dumps({'original':{'key':'probe-body'}}).encode() if ref['key']=='probe-cap' else b'probe-original'
        scope={'canary':canary,'canary_origin':{'key':'reviewed-canary'},'diagnostic':{'captures':{'probe':{'key':'probe-cap'}}},
            'previous':{'captures':{}},'source':source,'s3':None,'json':json,'PROBE':runner.PROBE}
        exec(compile(ast.Module(body=[adoption],type_ignores=[]),'adoption','exec'),scope)
        self.assertEqual(scope['adoption'](request),(canary,b'original',{'key':'reviewed-canary'}))
        self.assertEqual(scope['adoption']({'url':'probe'})[1],b'probe-original')
        self.assertIsNone(scope['adoption']({'url':'not-previously-requested'}))


if __name__=='__main__':unittest.main(verbosity=2)
