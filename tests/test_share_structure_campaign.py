from pathlib import Path
from unittest.mock import Mock,patch
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/ops/checks'),str(ROOT/'tests')]
import share_structure_campaign as campaign
import share_structure_sources as source
from test_share_structure_sources import S3 as Parent, response


class Missing(Exception):response={'Error':{'Code':'NoSuchKey'}}
class S3(Parent):
    def get_object(self,**args):
        if args['Key'] not in self.files:raise Missing()
        return super().get_object(**args)


def now():return '2026-09-25T12:00:00Z'
def plan(client):
    value=campaign.plan(['AAPL'],{'key':'baseline'},{'key':'probe'},{'key':'accounting'},now)
    return value,source.retain(client,source.encode(value))


class Tests(unittest.TestCase):
    def test_all_1615_names_are_partitioned_without_overlap_or_lost_sources(self):
        doc=campaign.plan([f'A{n}' for n in range(1615)],{}, {}, {},now)
        parts=[campaign.specifications(doc,n) for n in range(1,doc['batches']+1)]
        urls=[s['url'] for part in parts for s in part]
        self.assertEqual(len(urls),11305);self.assertEqual(len(set(urls)),11305)
        self.assertEqual([len(part) for part in parts],[2000,2000,2000,2000,2000,1305])
        doc['source_module_sha256']='0'*64
        with self.assertRaises(ValueError):campaign.specifications(doc,1)

    @patch.object(campaign,'CHUNK',4)
    @patch.object(source.Rate,'acquire',lambda self:None)
    def test_completion_requires_every_whole_batch_and_completed_batches_do_not_refetch(self):
        client=S3();doc,ref=plan(client);calls=Mock(side_effect=lambda *a,**kw:response(b'[]'))
        one,_=campaign.run_batch(client,'full',ref,1,'test-key',now,lambda spec:None,transport=calls)
        self.assertEqual(calls.call_count,4)
        repeated,sent=campaign.run_batch(client,'full',ref,1,'test-key',now,lambda spec:None,transport=calls)
        self.assertEqual(repeated,one);self.assertFalse(sent);self.assertEqual(calls.call_count,4)
        with self.assertRaises(ValueError):campaign.complete_population(client,ref,[one])
        two,_=campaign.run_batch(client,'full',ref,2,'test-key',now,lambda spec:None,transport=calls)
        complete=campaign.complete_population(client,ref,[two,one])
        self.assertEqual(complete['counts']['provider_requests'],7)
        self.assertEqual(complete['counts']['empty_arrays'],7)
        self.assertEqual(len(complete['captures']),7);self.assertFalse(complete['sizing_qualified'])
        with self.assertRaises(ValueError):campaign.complete_population(client,ref,[one,one])
        broken=json.loads(source.read(client,one));broken['counts']['empty_arrays']=0
        with self.assertRaises(ValueError):campaign.verify_batch(client,doc,broken)
        key=next(iter(broken['captures']));del broken['captures'][key]
        with self.assertRaises(ValueError):campaign.verify_batch(client,doc,broken)

    @patch.object(campaign,'CHUNK',4)
    @patch.object(source.Rate,'acquire',lambda self:None)
    def test_failed_batch_retains_originals_and_forbids_blind_retry(self):
        client=S3();doc,ref=plan(client);calls=Mock(side_effect=lambda *a,**kw:response(b'{"error":"unavailable"}',403))
        with self.assertRaises(ValueError):campaign.run_batch(client,'failed',ref,1,'test-key',now,lambda spec:None,transport=calls)
        self.assertLessEqual(calls.call_count,3);count=calls.call_count
        state=campaign.read_journal(client,source.request_key('failed','batch:1'))
        self.assertEqual(state['status'],'failed')
        self.assertTrue(any(body==b'{"error":"unavailable"}' for body in client.files.values()))
        with self.assertRaisesRegex(ValueError,'already attempted'):
            campaign.run_batch(client,'failed',ref,1,'test-key',now,lambda spec:None,transport=calls)
        self.assertEqual(calls.call_count,count)


if __name__=='__main__':unittest.main(verbosity=2)
