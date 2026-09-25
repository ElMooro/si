from pathlib import Path
from io import BytesIO
from unittest.mock import Mock, patch
from datetime import datetime, timezone
import ast, hashlib, importlib.util, json, sys, unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import short_interest_context as gate
import short_interest_research_model as model
from test_short_interest_research import fixture
LEGACY={'by_ticker':{'ABC':{'short_interest':100,'short_float_pct':80,'days_to_cover':999.99,'signal':'SQUEEZE_RISK','score':99}},'items':[{'utilization':99}],'rows':[{'ticker':'ABC','score':99}],'tracker':[{'ticker':'ABC','signal':'SQUEEZE_RISK'}]}
MIGRATION=json.loads((ROOT/'tests/fixtures/short-interest-consumer-migration.json').read_bytes())


def source(name):
    path=MIGRATION['consumers'][name].get('path',f'aws/lambdas/justhodl-{name}/source/lambda_function.py')
    return (ROOT/path).read_text(encoding='utf-8')


def function(name, fn, scope=None):
    tree=ast.parse(source(name));node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==fn)
    scope=dict(scope or {});exec(compile(ast.Module(body=[node],type_ignores=[]),name,'exec'),scope);return scope[fn]


class Tests(unittest.TestCase):
    def test_whole_predecessors_remain_exact(self):
        for entry in MIGRATION['archives'].values():
            body=(ROOT/entry['file']).read_bytes()
            self.assertEqual(len(body),entry['bytes'])
            self.assertEqual(hashlib.sha256(body).hexdigest(),entry['sha256'])

    def test_actual_decision_readers_remove_legacy_scores_and_keep_research_context(self):
        for name,entry in MIGRATION['consumers'].items():
            if entry['method'] not in ('wrapped_read','wrapped_decode'):continue
            count=0
            for node in ast.walk(ast.parse(source(name))):
                if not (isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute) and node.func.attr=='decision_view' and ast.unparse(node.func.value)=="__import__('short_interest_context')"):continue
                client=Mock();client.get_object.return_value={'Body':BytesIO(json.dumps(LEGACY).encode())}
                scope={'s3':client,'S3':client,'BUCKET':'b','json':json,'donor_docs':{gate.CURRENT:LEGACY},'FEED_SHORT_INTEREST':gate.CURRENT}
                reader=entry['reader']
                if '.' not in reader:scope[reader]=lambda *a,**k:LEGACY
                value=eval(compile(ast.Expression(body=node),name,'eval'),scope)
                self.assertEqual(value['by_ticker'],{},name)
                self.assertEqual(value['items'],[],name)
                self.assertEqual(value['rows'],[],name)
                self.assertIsNone(value['score'],name)
                self.assertEqual(value['independent_investment_votes'],0,name)
                count+=1
            self.assertEqual(count,entry['count'],name)

    def test_footprint_key_guard_preserves_other_source_contracts(self):
        client=Mock();client.get_object.side_effect=lambda **kw:{'Body':BytesIO(json.dumps(LEGACY).encode())}
        read=function('institutional-footprint','_j',{'json':json,'s3':client,'BUCKET':'b'})
        self.assertEqual(read(gate.CURRENT)['by_ticker'],{})
        self.assertEqual(read('data/unrelated.json'),LEGACY)

    def test_snapshot_differences_cannot_reanimate_old_squeeze_labels(self):
        diff=function('whats-changed','diff_short_interest')
        self.assertEqual(diff(LEGACY,{'tracker':[]}),[])
        self.assertEqual(diff({},LEGACY),[])

    def test_new_native_reference_requires_its_output_digest_and_no_forecast_flags(self):
        blobs,inputs=fixture();packet=model.compile_output(inputs,blobs.__getitem__)['packet']
        packet['generated_at']='2026-09-24T06:00:00+00:00'
        packet['replay']={'manifest_key':model.PREFIX+'runs/'+'a'*64+'.json','output_sha256':model.digest(packet)}
        self.assertTrue(gate.context(packet)['native_reference_available'])
        packet['calls_eligible']=True
        self.assertFalse(gate.context(packet)['native_reference_available'])
        packet['calls_eligible']=False;packet['settlement_date']='2026-08-31'
        self.assertFalse(gate.context(packet)['native_reference_available'])

    def test_actual_native_validation_and_storage_boundary_have_no_account_or_notification_path(self):
        path=ROOT/'aws/lambdas/justhodl-short-interest/source/lambda_function.py'
        spec=importlib.util.spec_from_file_location('short_interest_native_test',path);native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
        with patch.object(native.boto3,'client',side_effect=AssertionError('No AWS expected')):
            out=native.lambda_handler({'validate_only':True},None)
        self.assertEqual(out['statusCode'],200)
        self.assertFalse(json.loads(out['body'])['published'])
        client=Mock();storage=native.EvidenceStorage(client,native.publish_current)
        for key in ('portfolio/account.json','data/trade-tickets.json','data/other.json'):
            with self.assertRaises(ValueError):storage.get_object(Bucket=native.BUCKET,Key=key)
            with self.assertRaises(ValueError):storage.put_object(Bucket=native.BUCKET,Key=key,Body=b'{}')
        client.get_object.assert_not_called();client.put_object.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
