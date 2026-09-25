from pathlib import Path
from io import BytesIO
from unittest.mock import Mock
import json,sys,unittest,urllib.error
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/ops/checks'))
import share_structure_sources as source


class Conflict(Exception):response={'Error':{'Code':'PreconditionFailed'}}


class S3:
    def __init__(self):self.files={};self.writes=[]
    def get_object(self,Bucket,Key):
        assert Bucket==source.BUCKET
        return {'Body':BytesIO(self.files[Key])}
    def put_object(self,**args):
        assert args['Bucket']==source.BUCKET and args['Key'].startswith(source.PRIVATE)
        if args.get('IfNoneMatch')=='*' and args['Key'] in self.files:raise Conflict()
        self.files[args['Key']]=args['Body'];self.writes.append(args)


def response(body,status=200):
    value=BytesIO(body);value.status=status;value.headers={'content-type':'application/json'};return value


def capture(client,body,request=None,status=200):
    request=request or source.spec('AAPL','cash-flow-statement','quarter');transport=Mock(return_value=response(body,status))
    result=source.capture(client,'unique',request,{'AAPL'},'test-credential-never-published',Mock(),lambda:'2026-09-25T12:00:00Z',transport)
    return result,transport


class Tests(unittest.TestCase):
    def test_even_valid_json_cannot_hide_transport_length_or_encoding_mismatch(self):
        for headers in ({'content-length':'100'},{'content-encoding':'gzip'}):
            client=S3();req=source.spec('AAPL','quote');reply=response(b'[]');reply.headers.update(headers)
            transport=Mock(return_value=reply)
            with self.assertRaises(RuntimeError):
                source.capture(client,'length-test',req,{'AAPL'},'test-secret',Mock(),lambda:'2026-09-25T12:00:00Z',transport)
            state=json.loads(client.files[source.request_key('length-test',req['url'])])
            self.assertEqual(state['status'],'failed');self.assertEqual(source.read(client,state['original']),b'[]')
            self.assertEqual(transport.call_count,1)

    def test_adoption_preserves_original_acquisition_clock_and_has_no_transport(self):
        client=S3();request=source.spec('AAPL','income-statement','annual');body=b'[]'
        prior={'spec':request,'http_status':200,'request_id':'old','headers':{},
            'requested_at':'2026-09-24T12:00:00Z','received_at':'2026-09-24T12:00:01Z',
            'original':{'sha256':source.sha(body),'bytes':len(body)}}
        result=source.adopt(client,'new',request,{'AAPL'},prior,body,{'key':'retained-manifest'},lambda:'2026-09-25T12:00:00Z')
        self.assertEqual(result['received_at'],prior['received_at']);self.assertFalse(result['transport_attempted'])
        self.assertEqual(source.read(client,result['original']),body)
        with self.assertRaises(ValueError):source.adopt(client,'bad',request,{'AAPL'},prior,body+b' ',{},lambda:'now')

    def test_full_population_and_distinct_source_specifications(self):
        values=source.specifications([f'A{n}' for n in range(1166)])
        self.assertEqual(len(values),1166*7);self.assertEqual(len({v['url'] for v in values}),len(values))
        self.assertEqual(source.spec('BRK-B','income-statement','quarter')['limit'],13)
        for label in ('AAPL&apikey=bad','../AAPL','brk.b'):
            with self.assertRaises(ValueError):source.spec(label,'quote')
        with self.assertRaises(ValueError):source.population(['AAPL','AAPL'])
        with self.assertRaises(ValueError):source.spec('AAPL','quote','annual')

    def test_zero_missing_negative_and_malformed_identity_survive_without_calculations(self):
        body=b'[{"symbol":"AAPL","date":"2026-06-30","period":"Q3","fiscalYear":2026,"reportedCurrency":"USD","commonStockRepurchased":0,"commonStockIssued":null,"stockBasedCompensation":-123.50,"cik":{},"filingDate":null}]'
        client=S3();result,_=capture(client,body)
        self.assertEqual(source.read(client,result['original']),body)
        info=result['inventory'];self.assertEqual(info['zero_field_counts'],{'commonStockRepurchased':1})
        self.assertEqual(info['null_field_counts'],{'commonStockIssued':1,'filingDate':1})
        self.assertEqual(info['identity_metadata'][0]['metadata']['cik'],{'type':'object','value':{}})
        self.assertFalse(info['free_float_change_qualified']);self.assertFalse(info['share_split_comparability_verified'])
        self.assertNotIn('test-credential-never-published',source.encode(result).decode())

    def test_empty_arrays_are_explicit_missing_populations_not_zeros(self):
        result,_=capture(S3(),b'[]',source.spec('AAPL','shares-float'))
        self.assertEqual(result['inventory']['rows'],0)
        self.assertEqual(result['inventory']['status'],'provider_returned_empty_array')
        self.assertEqual(result['inventory']['zero_field_counts'],{})

    def test_malformed_and_error_originals_are_retained_before_failure_no_blind_retry(self):
        for body,status in ((b'{"error":"entitlement unavailable"}',403),(b'[',200),(b'[{"n":NaN}]',200)):
            client=S3();request=source.spec('AAPL','quote');calls=Mock(return_value=response(body,status))
            args=(client,'failed',request,{'AAPL'},'private-test-secret',Mock(),lambda:'2026-09-25T12:00:00Z',calls)
            with self.assertRaisesRegex(RuntimeError,'inspect retained'):source.capture(*args)
            journal=json.loads(client.files[source.request_key('failed',request['url'])])
            self.assertEqual(journal['status'],'failed');self.assertEqual(source.read(client,journal['original']),body)
            self.assertNotIn('private-test-secret',source.encode(journal).decode())
            with self.assertRaises(Conflict):source.capture(*args)
            self.assertEqual(calls.call_count,1)

    def test_changed_request_outside_population_or_corrupt_original_is_rejected(self):
        client=S3();request=source.spec('AAPL','quote');request['url']+='&limit=5'
        with self.assertRaises(ValueError):capture(client,b'[]',request)
        self.assertEqual(client.writes,[])
        with self.assertRaises(ValueError):capture(client,b'[]',source.spec('MSFT','quote'))
        ref=source.retain(client,b'[]');client.files[ref['key']]+=b' '
        with self.assertRaises(ValueError):source.read(client,ref)


if __name__=='__main__':unittest.main(verbosity=2)
