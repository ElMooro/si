from pathlib import Path
from io import BytesIO
from unittest.mock import patch
import json, sys, unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/ops/checks'),str(ROOT/'tests')]
import financial_statement_campaign as campaign
from test_financial_statement_source import row
import financial_statement_source as source


class Conflict(Exception):
    response={'Error':{'Code':'412'}}


class Storage:
    def __init__(self):self.data={};self.puts=[]
    def put_object(self,**kw):
        assert kw['Key'].startswith(campaign.PRIVATE)
        if kw.get('IfNoneMatch')=='*' and kw['Key'] in self.data:raise Conflict()
        self.data[kw['Key']]=kw['Body'];self.puts.append(kw)
    def get_object(self,**kw):return {'Body':BytesIO(self.data[kw['Key']])}


class Tests(unittest.TestCase):
    def test_full_declared_universe_does_not_expand_request_destinations(self):
        symbols=frozenset(('AAPL','JPM','BRK.B'))
        spec=source.request_spec('BRK.B','income-statement','annual',symbols)
        self.assertEqual(spec['url'],'https://financialmodelingprep.com/stable/income-statement?symbol=BRK.B&period=annual&limit=5')
        for allowed in (('AAPL&apikey=bad',),(),frozenset('X'+str(i) for i in range(601)), 'AAPL'):
            with self.assertRaises(ValueError):source.request_spec('AAPL','income-statement','annual',allowed)
        with self.assertRaises(ValueError):source.request_spec('UNLISTED','income-statement','annual',symbols)
        value=campaign.inventory(b'[]',spec,symbols)
        self.assertEqual(value['rows'],0);self.assertEqual(value['status'],'provider_returned_no_statements')
        self.assertFalse(value['accounting_calculations_qualified'])

    def test_complete_capture_preserves_originals_error_bodies_and_one_attempt(self):
        spec=source.request_spec('AAPL','income-statement','annual');symbols=frozenset(('AAPL','JPM'))
        for code,body in ((200,json.dumps([row()]).encode()),(200,b'[]'),(429,b''),(503,b'provider failure')):
            client=Storage();calls=[]
            def transport(request,**kw):
                calls.append(request.full_url);response=BytesIO(body);response.status=code;response.headers={'Content-Type':'application/json'};return response
            class Rate:
                def acquire(self):pass
            args=(client,'test-request',spec,symbols,'example-test-value',Rate(),lambda:'2026-09-25T00:00:00Z',transport)
            if code==200:
                value=campaign.capture(*args)
                capsule=json.loads(campaign.read(client,value['retained_capture']))
                self.assertEqual(campaign.read(client,capsule['original']),body)
            else:
                with self.assertRaises(RuntimeError):campaign.capture(*args)
            self.assertIn(body,client.data.values())
            with self.assertRaises(Conflict):campaign.capture(*args)
            self.assertEqual(len(calls),1)
            self.assertTrue(calls[0].endswith('&apikey=example-test-value'))
            for key,data in client.data.items():
                if key.endswith('.json'):self.assertNotIn(b'example-test-value',data)

    def test_adopted_sources_are_not_requested_and_every_remaining_source_is_retained(self):
        specs=[{'url':str(i)} for i in range(80)];seen=[];snapshots=[]
        def fetch(spec):seen.append(spec['url']);return {'raw':spec['url']}
        out=campaign.collect(specs,{'0':{'raw':'original'}},fetch,
            lambda completed,errors:snapshots.append((dict(completed),dict(errors))))
        self.assertEqual(len(seen),79);self.assertNotIn('0',seen);self.assertEqual(len(out),80)
        self.assertEqual(out['0']['raw'],'original');self.assertEqual(len(snapshots[-1][0]),80)
        self.assertEqual(snapshots[-1][1],{})
        with self.assertRaises(ValueError):campaign.collect([{'url':'x'},{'url':'x'}],{},fetch,lambda *a:None)

    def test_failure_stops_new_work_but_keeps_already_started_successes(self):
        seen=[];snapshots=[]
        def fetch(spec):
            seen.append(spec['url'])
            if spec['url']=='0':raise RuntimeError('retained response failed')
            return {'retained':True}
        with self.assertRaises(ValueError):campaign.collect([{'url':str(i)} for i in range(100)],{},fetch,
            lambda completed,errors:snapshots.append((dict(completed),dict(errors))))
        self.assertEqual(set(seen),{'0','1','2'})
        self.assertEqual(set(snapshots[-1][0]),{'1','2'})
        self.assertEqual(snapshots[-1][1],{'0':'RuntimeError'})

    def test_rate_limit_and_original_read_boundary(self):
        rate=campaign.Rate()
        with patch.object(campaign.time,'monotonic',side_effect=[1,1,1.1,1.4,1.6,1.8]),patch.object(campaign.time,'sleep') as sleep:
            rate.acquire();rate.acquire();rate.acquire()
        values=[c.args[0] for c in sleep.call_args_list]
        self.assertAlmostEqual(values[0],0);self.assertAlmostEqual(values[1],.3);self.assertAlmostEqual(values[2],.2)
        for key in ('data/trade-tickets.json','accounts/current.json'):
            with self.assertRaises(ValueError):campaign.read(Storage(),{'key':key,'sha256':'a'*64,'bytes':1})
        client=Storage();ref=campaign.retain(client,b'whole original');client.data[ref['key']]=b'changed'
        with self.assertRaises(ValueError):campaign.read(client,ref)


if __name__=='__main__':unittest.main(verbosity=2)
