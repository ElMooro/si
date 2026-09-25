from pathlib import Path
from io import BytesIO
from unittest.mock import Mock
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/ops/checks'))
import buyback_filing_sources as source

ACCESSION='0001193125-26-000123'  # filing agent differs from issuer
ISSUER='0000000123'


def row(symbol='ABC',accession=ACCESSION):
    return {'ticker':symbol,'filing_adsh':accession,'announcement_date':'2026-09-20',
        'filing_url':'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK='+ISSUER+'&type=8-K&dateb=&owner=include&count=40'}


def filing(exhibit=b'Authorization: repurchase; a buyback reference is not proof.'):
    return (b'<SEC-DOCUMENT>0001193125-26-000123.txt : 20260920\n<SEC-HEADER>example.hdr.sgml : 20260920\n'
        b'ACCESSION NUMBER:\t0001193125-26-000123\nCONFORMED SUBMISSION TYPE:\t8-K\n'
        b'PUBLIC DOCUMENT COUNT:\t2\nFILED AS OF DATE:\t20260920\n'
        b'FILER:\n COMPANY DATA:\n  CENTRAL INDEX KEY:\t0000000123\n</SEC-HEADER>\n'
        b'<DOCUMENT>\n<TYPE>8-K\n<SEQUENCE>1\n<FILENAME>main.htm\n<TEXT>\nPrimary refers to Exhibit 99.1.\n</TEXT>\n</DOCUMENT>\n'
        b'<DOCUMENT>\n<TYPE>EX-99.1\n<SEQUENCE>2\n<FILENAME>ex991.htm\n<TEXT>\n'+exhibit+b'\n</TEXT>\n</DOCUMENT>\n</SEC-DOCUMENT>\n')


class Conflict(Exception):response={'Error':{'Code':'PreconditionFailed'}}


class S3:
    def __init__(self):self.files={};self.writes=[]
    def get_object(self,Bucket,Key):
        assert Bucket==source.BUCKET and Key.startswith(source.PRIVATE)
        return {'Body':BytesIO(self.files[Key])}
    def put_object(self,**args):
        assert args['Bucket']==source.BUCKET and args['Key'].startswith(source.PRIVATE)
        if args.get('IfNoneMatch')=='*' and args['Key'] in self.files:raise Conflict()
        self.files[args['Key']]=args['Body'];self.writes.append(args)


def response(body,status=200,headers=None):
    value=BytesIO(body);value.status=status;value.headers=headers or {};return value


class Tests(unittest.TestCase):
    def test_plan_retains_duplicate_tickers_accessions_and_every_source_row(self):
        rows=[row(),row(),row('ABC','0001193125-26-000124'),row('DEF')]
        rows.append({**row(),'filing_adsh':'bad'})
        result=source.plan({'as_of':'2026-09-21','top_opportunities':rows})
        self.assertEqual(result['reported_rows'],5);self.assertEqual(len(result['requests']),2)
        self.assertEqual(result['requests'][0]['source_rows'],[0,1,3]);self.assertEqual(result['unresolved_rows'],[4])
        self.assertEqual(result['rows'][4]['reported_accession'],'bad')
        self.assertFalse(result['filing_population_complete']);self.assertFalse(result['reported_announcement_date_is_verified_event_date'])
        self.assertEqual(source.plan({'top_opportunities':[]})['requests'],[])

    def test_issuer_is_not_inferred_from_filing_agent_accession(self):
        request=source.row_spec(row());self.assertEqual(request['issuer_cik'],ISSUER)
        result=source.inspect(filing(),request);self.assertEqual(result['header_issuer_ciks'],[ISSUER])
        self.assertEqual(result['filing_date'],'2026-09-20')
        self.assertFalse(result['ticker_identity_verified']);self.assertFalse(result['authorization_amount_qualified'])

    def test_hostile_ambiguous_or_conflicting_links_cannot_select_network_targets(self):
        for url in ('http://www.sec.gov/cgi-bin/browse-edgar?CIK=123',
                row()['filing_url']+'&CIK=456',row()['filing_url']+'&apikey=secret',
                row()['filing_url'].replace('www.sec.gov','www.sec.gov.evil.test'),
                row()['filing_url'].replace('www.sec.gov','me@www.sec.gov'),
                row()['filing_url'].replace('www.sec.gov','www.sec.gov:443'),
                row()['filing_url']+'#fragment'):
            with self.assertRaises(ValueError):source.row_spec({**row(),'filing_url':url})
        with self.assertRaises(ValueError):source.row_spec({**row(),'cik':'456'})
        for value in (True,0,'123/../456','00000000123',None):
            with self.assertRaises(ValueError):source.spec(value,ACCESSION)

    def test_complete_exhibit_over_legacy_400kb_limit_and_byte_coordinates(self):
        body=filing('préface '.encode()+b'x'*450000+b' repurchase $50 million; BUYBACK')
        result=source.inspect(body,source.row_spec(row()));self.assertEqual(len(result['documents']),2)
        self.assertGreater(result['original_bytes'],450000);self.assertEqual(result['literal_keyword_occurrences'],2)
        exhibit=result['documents'][1];self.assertEqual(exhibit['type'],'EX-99.1')
        self.assertEqual(source.sha(body[exhibit['byte_start']:exhibit['byte_end']]),exhibit['sha256'])
        self.assertEqual(source.sha(body[exhibit['text_byte_start']:exhibit['text_byte_end']]),exhibit['text_sha256'])
        for point in exhibit['literal_keyword_occurrences']:
            self.assertEqual(body[point['byte_start']:point['byte_end']].decode(),point['reported_text'])
        self.assertTrue(result['all_declared_documents_retained']);self.assertFalse(result['buyback_execution_qualified'])
        self.assertEqual(source.inspect(body.replace(b'\n',b'\r\n'),source.row_spec(row()))['reported_document_count'],2)

    def test_truncation_wrong_identity_count_malformed_document_and_html_error_are_rejected(self):
        body=filing()
        failures=[body[:-20],body.replace(b'COUNT:\t2',b'COUNT:\t3'),
            body.replace(b'KEY:\t0000000123',b'KEY:\t0000000456'),
            body.replace(b'NUMBER:\t0001193125-26-000123',b'NUMBER:\t0001193125-26-000124'),
            body.replace(b'<SEQUENCE>2',b'<SEQUENCE>1'),body.replace(b'ex991.htm',b'../ex991.htm'),
            body.replace(b'20260920\nFILER',b'20260230\nFILER'),body.replace(b'<TYPE>8-K',b'<TYPE>EX-99.2'),
            body.replace(b'</TEXT>',b'<BROKEN>',1),body+b'unparsed tail',b'<html>Request Rate Threshold Exceeded</html>']
        for value in failures:
            with self.assertRaises(ValueError):source.inspect(value,source.row_spec(row()))

    def test_success_retains_whole_original_and_capsule_without_public_writes(self):
        client=S3();body=filing();transport=Mock(return_value=response(body,headers={'Content-Length':str(len(body))}))
        result=source.capture(client,'one-request',source.row_spec(row()),Mock(),lambda:'2026-09-25T17:00:00Z',transport)
        self.assertEqual(source.read(client,result['original']),body);self.assertEqual(transport.call_count,1)
        request=transport.call_args.args[0];self.assertEqual(request.full_url,source.row_spec(row())['url'])
        self.assertFalse(request.has_header('Authorization'));self.assertNotIn('apikey',request.full_url)
        state=json.loads(client.files[result['request_status_key']]);self.assertEqual(state['status'],'complete')
        self.assertEqual(json.loads(source.read(client,state['capture']))['inventory'],result['inventory'])
        client.files[result['original']['key']]+=b' '
        with self.assertRaises(ValueError):source.read(client,result['original'])

    def test_errors_lengths_encoding_and_truncation_retained_no_automatic_retry(self):
        for body,status,headers in ((b'Blocked',403,{}),(b'Rate limit',429,{'Retry-After':'600'}),
                (filing()[:-40],200,{}),(filing(),200,{'Content-Length':'1'}),(filing(),200,{'Content-Encoding':'gzip'})):
            client=S3();transport=Mock(return_value=response(body,status,headers));request=source.row_spec(row())
            args=(client,'failure',request,Mock(),lambda:'2026-09-25T17:00:00Z',transport)
            with self.assertRaises(RuntimeError):source.capture(*args)
            state=json.loads(client.files[source.request_key('failure',request['url'])]);self.assertEqual(state['status'],'failed')
            self.assertEqual(source.read(client,state['original']),body)
            with self.assertRaises(Conflict):source.capture(*args)
            self.assertEqual(transport.call_count,1)


if __name__=='__main__':unittest.main(verbosity=2)
