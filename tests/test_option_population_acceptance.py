from pathlib import Path
from unittest.mock import Mock,patch
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared'),str(ROOT/'aws/ops/staged')]
import ops_6000_population_native_acceptance as op
from test_option_flow_store import S3


class Tests(unittest.TestCase):
    def test_existing_native_publication_is_adopted_without_invocation(self):
        packet={'contract':op.desk.CONTRACT,'replay':op.RECOVERY};s3=S3({op.desk.CURRENT:json.dumps(packet).encode()})
        with patch.object(op,'completed_request',return_value={'invoke_sent':False}) as complete,patch.object(op,'invoke_when_available') as invoke:
            self.assertFalse(op.invoke(Mock(),s3,'a'*40)['invoke_sent']);complete.assert_called_once_with(s3,packet);invoke.assert_not_called()
    def test_only_one_explicit_recovery_is_accepted_and_durably_recorded(self):
        s3=S3();commit='a'*40
        def accept(lam,args):
            self.assertEqual(args['FunctionName'],'justhodl-dealer-gex');self.assertEqual(args['InvocationType'],'Event')
            event=json.loads(args['Payload']);self.assertEqual(event['recover_run'],op.RECOVERY)
            status={'status':'complete','provider_requests':0,'recovered_from':op.RECOVERY,'replay':op.RECOVERY,
                'published':True,'compatibility_published':True}
            s3.data[op.source_store.request_key('population:'+event['request_id'])]=json.dumps(status).encode()
            s3.data[op.desk.CURRENT]=json.dumps({'contract':op.desk.CONTRACT,'replay':op.RECOVERY}).encode()
            return {'StatusCode':202},0
        with patch.object(op,'invoke_when_available',side_effect=accept) as invoke:
            result=op.invoke(Mock(),s3,commit);self.assertTrue(result['invoke_sent']);invoke.assert_called_once()
        claim=json.loads(s3.data[result['dispatch_key']]);self.assertEqual(claim['status'],'accepted_async')
        self.assertEqual(claim['recovery'],op.RECOVERY)
    def test_existing_uncertain_claim_is_not_reinvoked_when_publication_is_missing(self):
        commit='a'*40;request='chatgpt-justhodl-dealer-gex-'+commit[:12]+'-1';prefix='population:'+request
        claim={'contract':'option-population-native-dispatch.v1','request_id':request,'recovery':op.RECOVERY}
        status={'status':'complete','provider_requests':0,'recovered_from':op.RECOVERY,'replay':op.RECOVERY}
        s3=S3({op.source_store.request_key(prefix+'-dispatch'):json.dumps(claim).encode(),op.source_store.request_key(prefix):json.dumps(status).encode()})
        with patch.object(op,'invoke_when_available') as invoke:
            with self.assertRaises(AssertionError):op.invoke(Mock(),s3,commit)
            invoke.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
