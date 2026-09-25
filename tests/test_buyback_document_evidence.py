from pathlib import Path
from copy import deepcopy
from unittest.mock import Mock
import json, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/ops/checks'), str(ROOT/'tests')]
import buyback_document_evidence as model
import buyback_filing_sources as source
from test_buyback_filing_sources import S3, row, filing, response


def fixture(duplicate=False):
    client = S3()
    packet = {'as_of': '2026-09-21T12:00:00Z', 'top_opportunities': [dict(row(), company='Fixture issuer',
        authorization_usd=1000000, ignored_private_field='NOT_FOR_PUBLIC')], 'ignored_private_field':'NOT_FOR_PUBLIC'}
    if duplicate:packet['top_opportunities'].append(deepcopy(packet['top_opportunities'][0]))
    plan = source.plan(packet);request = plan['requests'][0]['spec']
    body = filing('préface <p>repurchase $50 million</p> '.encode()+b'x'*450000+b' BUYBACK')
    captured = source.capture(client, 'fixture', request, Mock(), lambda:'2026-09-25T17:00:01Z',
        Mock(return_value=response(body)))
    def keep(value):return source.retain(client, source.encoded(value))
    manifest = {'status':'captured', 'started_at':'2026-09-25T17:00:00Z', 'completed_at':'2026-09-25T17:00:02Z',
        'all_reported_rows_conserved':True, 'all_declared_documents_retained':True,
        'inspector_source':source.retain(client, Path(source.__file__).read_bytes()),
        'whole_scanner':keep(packet), 'plan':keep(plan), 'captures':{request['url']:captured['retained_capture']},
        'records':[{'request':request, 'source_rows':plan['requests'][0]['source_rows'],
            'capture':captured['retained_capture'], 'original':captured['original'],
            'received_at':captured['received_at'], 'filing_date':captured['inventory']['filing_date'],
            'documents':2, 'literal_keyword_occurrences':2, **source.FLAGS}], **source.FLAGS}
    return client, manifest, keep


class Tests(unittest.TestCase):
    def test_complete_document_coordinates_replay_whole_utf8_and_large_exhibits(self):
        client, manifest, keep = fixture();ref=keep(manifest);before=len(client.writes)
        read=lambda ref:source.read(client,ref)
        out=model.compile_output(ref,read)
        self.assertEqual(out, model.compile_output(ref,read));self.assertEqual(len(client.writes),before)
        self.assertEqual((out['reported_rows'],out['distinct_filings'],out['documents']), (1,1,2))
        self.assertEqual(out['literal_keyword_occurrences'],2)
        body=source.read(client,manifest['records'][0]['original'])
        for doc in out['filings'][0]['documents']:
            self.assertEqual(source.sha(body[doc['byte_start']:doc['byte_end']]),doc['sha256'])
            self.assertEqual(source.sha(body[doc['text_byte_start']:doc['text_byte_end']]),doc['text_sha256'])
            self.assertFalse(doc['document_url_current_bytes_verified'])
            for word in doc['literal_keyword_occurrences']:
                self.assertEqual(body[word['byte_start']:word['byte_end']].decode(),word['reported_text'])
        self.assertGreater(out['filings'][0]['documents'][1]['text_bytes'],450000)
        self.assertEqual(out['filings'][0]['documents'][1]['document_url'],
            source.row_spec(row())['url'].rsplit('/',1)[0]+'/ex991.htm')

    def test_duplicate_reported_rows_share_one_filing_without_double_counting_documents(self):
        client, manifest, keep=fixture(True);out=model.compile_output(keep(manifest),lambda ref:source.read(client,ref))
        self.assertEqual((out['reported_rows'],out['distinct_filings'],out['documents']), (2,1,2))
        self.assertEqual(out['filings'][0]['source_rows'],[0,1])
        self.assertEqual(out['rows'][0]['filing_id'],out['rows'][1]['filing_id'])

    def test_projection_excludes_private_journals_and_never_promotes_keyword_or_scanner_claim(self):
        client, manifest, keep=fixture();out=model.compile_output(keep(manifest),lambda ref:source.read(client,ref))
        raw=source.encoded(out).decode()
        self.assertNotIn('audit-private/',raw);self.assertNotIn('NOT_FOR_PUBLIC',raw)
        for item in (out,out['filings'][0]):
            self.assertTrue(all(item[key] is False for key in model.FLAGS))
        self.assertEqual(out['rows'][0]['reported_authorization_usd'],1000000)
        self.assertFalse(out['rows'][0]['reported_authorization_amount_verified'])
        self.assertFalse(out['rows'][0]['reported_announcement_date_is_verified_event_date'])
        self.assertEqual(out['filings'][0]['issuer_cik'],'0000000123')
        self.assertTrue(out['filings'][0]['accession'].startswith('0001193125'))

    def test_truncated_original_missing_document_or_changed_inventory_is_rejected(self):
        for failure in ('truncated','missing','inventory','coordinates','clock','inspector'):
            client,manifest,keep=fixture()
            record=manifest['records'][0];request=record['request'];cap=json.loads(source.read(client,record['capture']))
            if failure=='truncated':client.files[record['original']['key']]=b'truncated'
            elif failure=='missing':cap['inventory']['documents'].pop()
            elif failure=='inventory':cap['inventory']['documents'][1]['filename']='wrong.htm'
            elif failure=='coordinates':cap['inventory']['documents'][1]['text_byte_start']+=1
            elif failure=='clock':cap['received_at']='2026-09-26T17:00:00Z'
            else:manifest['inspector_source']=source.retain(client,b'not the accepted parser')
            if failure in ('missing','inventory','coordinates','clock'):
                ref=keep(cap);record['capture']=ref;manifest['captures'][request['url']]=ref
            with self.assertRaises(ValueError):model.compile_output(keep(manifest),lambda ref:source.read(client,ref))

    def test_incomplete_duplicated_or_promoted_capture_population_is_rejected(self):
        for failure in ('missing','duplicate','row','authority','summary'):
            client,manifest,keep=fixture(True)
            if failure=='missing':manifest['records']=[]
            elif failure=='duplicate':manifest['records'].append(deepcopy(manifest['records'][0]))
            elif failure=='row':manifest['records'][0]['source_rows']=[0]
            elif failure=='authority':manifest['authorization_amount_qualified']=True
            else:manifest['records'][0]['documents']=1
            with self.assertRaises(ValueError):model.compile_output(keep(manifest),lambda ref:source.read(client,ref))

    def test_reference_validation_precedes_storage_read(self):
        read=Mock()
        for ref in ({'key':'private/accounts.json','sha256':'a'*64,'bytes':1},
                {'key':source.PRIVATE+'a'*64+'.bin','sha256':'a'*64,'bytes':True},
                {'key':source.PRIVATE+'a'*64+'.bin','sha256':'a'*64,'bytes':source.MAX+1}):
            with self.assertRaises(ValueError):model.original(ref,read)
        read.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
