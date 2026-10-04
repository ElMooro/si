"""Invented CFTC observations only; no network or application state."""
import base64
import hashlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import urllib.error
from urllib.parse import parse_qs,urlsplit
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'source'))
import cftc_series as c

ALIAS='COT3:132741_FO_TAM_S'
SID=c.definition(ALIAS)['id']
FIELD=c.definition(ALIAS)['field']
def row(value='9',day='2026-01-06T00:00:00.000',code='132741',field=FIELD):
    return {'id':'invented-'+day,'report_date_as_yyyy_mm_dd':day,'cftc_contract_market_code':code,field:value,'market_and_exchange_names':'Invented fixture'}
class Reader:
    def __init__(self,pages):self.pages=list(pages);self.calls=[]
    def __call__(self,url,timeout,cap):
        self.calls.append((url,timeout,cap));page=self.pages.pop(0)
        if isinstance(page,Exception):raise page
        return (page if isinstance(page,bytes) else json.dumps(page).encode()),200,{'Content-Type':'application/json'}

class ExactCFTCTests(unittest.TestCase):
    def test_all_reviewed_watchlist_definitions_are_distinct_and_case_preserving(self):
        self.assertEqual(len(c.CATALOG['aliases']),346)
        for alias,r in c.CATALOG['aliases'].items():
            d=c.definition(alias);self.assertEqual(d['id'],r['canonical']);self.assertEqual(d['requested'],alias)
            self.assertEqual(c.definition(d['id'])['field'],r['field'])
            self.assertEqual(d['unit'],r['unit']);self.assertEqual(d['scope'],r['scope'])
        self.assertEqual(c.definition(ALIAS)['dataset'],'yw9f-hn96')
        self.assertEqual(c.definition(ALIAS)['unit'],'traders')
        self.assertNotEqual(c.definition('COT:132741_FO_NCP_L')['dataset'],c.definition(ALIAS)['dataset'])

    def test_invalid_or_ambiguous_ids_never_make_http_requests(self):
        reader=Reader([])
        for sid in [None,True,'COT3:unknown','cftc:'+SID.split(':',1)[1].upper(),SID+'|extra',SID.replace('132741',"123' OR true"),SID.replace(FIELD,'noncomm_short'),'cftc:badx-badx|132741|'+FIELD]:
            with self.subTest(sid=sid),self.assertRaises(ValueError):c.fetch(sid,reader)
        self.assertEqual(reader.calls,[])

    def test_code_preserves_leading_zero_plus_and_no_padding(self):
        for code in ['020601','12460+','11700']:
            d=c.definition(SID.replace('132741',code));q=parse_qs(urlsplit(c.query_url(d,1000)).query)
            self.assertEqual(q['$where'],["cftc_contract_market_code='"+code+"'"])
            self.assertEqual(q['$offset'],['1000']);self.assertEqual(q['$order'],['report_date_as_yyyy_mm_dd ASC,id ASC'])

    def test_original_bytes_and_observation_clock_are_retained(self):
        raw=(' [ '+json.dumps(row())+' ]\n').encode();out=c.fetch(SID,Reader([raw]));receipt=out['source_receipts'][0]
        self.assertEqual(base64.b64decode(receipt['body_base64']),raw)
        self.assertEqual(receipt['sha256'],hashlib.sha256(raw).hexdigest())
        self.assertEqual(out['obs'],[['2026-01-06',9]])
        self.assertIsNone(out['source_published_at']);self.assertIsNone(out['measurement_evidence'][0]['source_available_at'])
        self.assertFalse(out['history']['point_in_time_vintages_verified']);self.assertFalse(out['calls_eligible'])

    def test_units_missing_suppression_and_unsafe_numbers_never_become_zero(self):
        for value in [None,True,False,'',' ','.','·','NaN','1e-400','9007199254740992','1.2','-1']:
            with self.subTest(value=value):
                out=c.fetch(SID,Reader([[row(value)]]));self.assertEqual(out['obs'],[['2026-01-06',None]]);self.assertEqual(out['n'],0)
        self.assertEqual(c.fetch(SID,Reader([[row('0')]]))['obs'],[['2026-01-06',0]])
        self.assertEqual(c.number('12.5','percent_of_open_interest'),(12.5,None))
        self.assertEqual(c.number('0.5','contracts'),(0.5,None))
        self.assertEqual(c.number('100.01','percent_of_open_interest')[1],'invalid_percentage')
        self.assertEqual(c.number('0.1234567890123456789','percent_of_open_interest')[1],'lossy_numeric_projection')

    def test_duplicate_date_and_wrong_contract_do_not_win(self):
        for rows in [[row('9'),row('10')],[row('9'),row('9')],[row(code='0132741')],[row(day='2026-02-30')]]:
            out=c.fetch(SID,Reader([rows]));self.assertEqual(out['n'],0)
            self.assertEqual(len(out['measurement_evidence']),len(rows))
        out=c.fetch(SID,Reader([[row('9'),row('10',day='2026-01-13')]]));self.assertEqual(out['n'],2)

    def test_missing_middle_period_is_not_filled(self):
        out=c.fetch(SID,Reader([[row('1','2026-01-06'),row('3','2026-01-20')]]))
        self.assertEqual(out['obs'],[['2026-01-06',1],['2026-01-20',3]])
        self.assertFalse(out['history']['missing_periods_filled'])

    def test_pagination_requires_final_short_page_and_preserves_all_rows(self):
        reader=Reader([[row('1','2026-01-06'),row('2','2026-01-13')],[row('3','2026-01-20')]])
        with patch.object(c,'PAGE_SIZE',2):out=c.fetch(SID,reader)
        self.assertEqual(out['n'],3);self.assertTrue(out['history']['pagination_complete'])
        self.assertEqual(parse_qs(urlsplit(reader.calls[1][0]).query)['$offset'],['2'])
        with patch.object(c,'PAGE_SIZE',2),patch.object(c,'MAX_PAGES',1):out=c.fetch(SID,Reader([[row(),row('2','2026-01-13')]]))
        self.assertEqual(out['obs'],[]);self.assertEqual(len(out['measurement_evidence']),2)
        self.assertFalse(out['history']['pagination_complete'])

    def test_denial_stops_without_alternative_or_partial_history(self):
        reader=Reader([[row()],urllib.error.HTTPError('fixture',403,'denied',{},None)])
        with patch.object(c,'PAGE_SIZE',1):out=c.fetch(SID,reader)
        self.assertEqual(len(reader.calls),2);self.assertEqual(out['obs'],[])
        self.assertEqual(out['source_receipts'][1]['http_status'],403)
        self.assertEqual(len(out['measurement_evidence']),1)

    def test_malformed_duplicate_keys_and_overflow_remain_inspectable_but_unplotted(self):
        for raw in [b'[] []',b'[{"id":"a","id":"b"}]',b'{"error":"bad query"}',b'[NaN]',b'\xff']:
            with self.subTest(raw=raw):
                out=c.fetch(SID,Reader([raw]));self.assertEqual(out['n'],0);self.assertEqual(base64.b64decode(out['source_receipts'][0]['body_base64']),raw)
        with patch.object(c,'MAX_BYTES',1):out=c.fetch(SID,Reader([b'[]']))
        self.assertEqual(out['obs'],[]);self.assertFalse(out['history']['pagination_complete'])

    def test_deadline_and_timeout_fail_closed(self):
        ticks=iter([0,30]);reader=Reader([]);out=c.fetch(SID,reader,monotonic=lambda:next(ticks))
        self.assertEqual(reader.calls,[]);self.assertEqual(out['n'],0)
        reader=Reader([TimeoutError('invented')]);out=c.fetch(SID,reader)
        self.assertEqual(len(reader.calls),1);self.assertEqual(out['n'],0)

    def test_cache_is_bound_to_id_definition_version_and_complete_pagination(self):
        out=c.fetch(SID,Reader([[row()]]));self.assertTrue(c.cache_valid(out,SID))
        for patching in [{'contract':'old'},{'id':SID.replace('132741','133741')},{'definition_sha256':'bad'},{'history':None},{'history':{'pagination_complete':'true'}}]:
            self.assertFalse(c.cache_valid(dict(out,**patching),SID))

    def test_metadata_directory_pages_and_reports_scope(self):
        first=c.directory('132741',3,0);second=c.directory('132741',3,3)
        self.assertEqual(len(first['rows']),3);self.assertEqual(first['total'],second['total'])
        self.assertTrue(set(r['id'] for r in first['rows']).isdisjoint(r['id'] for r in second['rows']))
        self.assertTrue(all(r['contract_existence_verified'] is False for r in first['rows']))
        with self.assertRaises(ValueError):c.directory('',3,-1)

if __name__=='__main__':unittest.main()
