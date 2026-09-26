from pathlib import Path
import contextlib,json,sys,unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/ops/staged')]
import ops_6173_capital_refresh_failure as check


class Tests(unittest.TestCase):
    def fixture(self):
        request=check.refresh.request_id(check.RUN);spec=check.capture.spec('ABC','quote');url=spec['url']
        state={'request_id':request,'status':'failed','active_phase':'part-1','completed_phases':['plan'],'plan':{'fixture':'plan'}}
        batch={'request_id':request,'status':'failed','plan':state['plan'],'source_errors':{url:'RuntimeError'},'captures':{}}
        item={'request_id':request,'status':'failed','spec':spec,'http_status':429,'error_type':'ValueError','original':{'fixture':'body'}}
        stack=contextlib.ExitStack();self.addCleanup(stack.close)
        control=stack.enter_context(patch.object(check.refresh,'load_control',return_value=(state,'etag',b'control')))
        stack.enter_context(patch.object(check.campaign,'specifications',return_value=[spec]))
        journals=stack.enter_context(patch.object(check.campaign,'read_journal',side_effect=[batch,item]))
        stack.enter_context(patch.object(check.capture,'read',side_effect=[b'{}',b'Rate limit exceeded. Echoed credential: DO-NOT-LOG']))
        return state,batch,item,control,journals

    def test_read_only_diagnostic_classifies_but_never_prints_provider_body(self):
        self.fixture();result=check.diagnostic(object())
        self.assertEqual(result['failed_requests'][0]['classified_body_symptoms'],['rate_limit'])
        self.assertNotIn('DO-NOT-LOG',json.dumps(result));self.assertEqual(result['provider_requests'],0)
        self.assertEqual(result['control_writes'],0);self.assertEqual(result['unattempted_batch_sources'],0)

    def test_changed_control_and_unreviewed_request_are_rejected(self):
        state,batch,item,control,journals=self.fixture()
        control.side_effect=[(state,'etag',b'control'),(state,'changed',b'new')]
        with self.assertRaisesRegex(ValueError,'Control changed'):check.diagnostic(object())
        control.side_effect=None;item['spec']={'url':'https://unreviewed.invalid/?apikey=DO-NOT-READ'}
        journals.side_effect=[batch,item]
        with patch.object(check.capture,'read',return_value=b'{}') as read:
            with self.assertRaisesRegex(ValueError,'identity differs'):check.diagnostic(object())
            self.assertEqual(read.call_count,1)


if __name__=='__main__':unittest.main()
