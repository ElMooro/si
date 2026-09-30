from pathlib import Path
from copy import deepcopy
import csv,hashlib,io,json,sys,unittest
import run_tests as native
import fifx_candidate as candidate
import fifx_catalog as catalog
import fifx_originals as originals
import fifx_fred as fred
import verify_fifx_arithmetic as independent
import test_fifx_candidate as fixture

def api_case(sid,n=550,missing=False):
    raw=fixture.csv(sid,n=n);table=list(csv.reader(io.StringIO(raw.decode())));as_of=fixture.NOW[:10]
    if missing:table[-1][1]='.';raw=('\n'.join(','.join(r) for r in table)+'\n').encode()
    doc={'realtime_start':as_of,'realtime_end':as_of,'observation_start':'1988-01-01','observation_end':as_of,
         'units':'lin','output_type':1,'file_type':'json','order_by':'observation_date','sort_order':'asc','count':n,'offset':0,'limit':50000,
         'observations':[{'realtime_start':as_of,'realtime_end':as_of,'date':d,'value':v} for d,v in table[1:]]}
    body=json.dumps(doc,separators=(',',':')).encode();receipt=fixture.receipt(sid,body);receipt['source_url']=fred.source_url(sid,as_of)
    return raw,body,receipt,doc

class Tests(unittest.TestCase):
    def test_all_six_full_json_histories_reproduce_csv_mathematics_and_every_independent_scalar(self):
        for sid in catalog.FRED:
            raw,body,receipt,_=api_case(sid);definition=fixture.definition(sid)
            prior=candidate.build_source(sid,raw,fixture.receipt(sid,raw),fixture.NOW,definition)
            out=candidate.build_source(sid,body,receipt,fixture.NOW,definition);proof=independent.verify(out,body,receipt,definition)
            self.assertEqual(out['quality']['status'],'within_age_ceiling');self.assertEqual(out['specification']['provider'],'fred_api')
            for key in ('original_rows','history','current','last_calculated','quality','methodology','session_evidence'):self.assertEqual(out[key],prior[key],(sid,key))
            self.assertEqual(proof['original_rows'],550)
            self.assertEqual(proof,independent.verify(prior,raw,fixture.receipt(sid,raw),definition))
            self.assertTrue(out['source_identity']['population']['complete_requested_window']);self.assertFalse(out['source_identity']['population']['full_series_history'])
    def test_missing_latest_is_preserved_and_not_backfilled(self):
        _,body,receipt,_=api_case('DGS10',missing=True);out=candidate.build_source('DGS10',body,receipt,fixture.NOW,fixture.definition('DGS10'))
        self.assertEqual(out['quality']['status'],'missing_latest_value');self.assertIsNone(out['current']);self.assertEqual(out['original_rows'][-1]['value'],'.')
        independent.verify(out,body,receipt,fixture.definition('DGS10'))
    def test_partial_count_is_retained_failure_with_no_calculation(self):
        _,_,receipt,doc=api_case('DGS10');doc['count']+=1;body=json.dumps(doc).encode();receipt.update(bytes=len(body),sha256=hashlib.sha256(body).hexdigest())
        out=candidate.build_source('DGS10',body,receipt,fixture.NOW,fixture.definition('DGS10'))
        self.assertEqual(out['quality']['status'],'invalid_original_schema');self.assertEqual(out['history'],[]);self.assertIsNone(out['current'])
        independent.verify(out,body,receipt,fixture.definition('DGS10'))
    def test_whole_http_error_does_not_become_an_observation(self):
        _,body,receipt,_=api_case('DGS10');receipt['http_status']=429
        out=candidate.build_source('DGS10',body,receipt,fixture.NOW,fixture.definition('DGS10'))
        self.assertEqual(out['quality']['status'],'http_error');self.assertEqual(out['original_bytes'],len(body));self.assertEqual(out['original_rows'],[])
        independent.verify(out,body,receipt,fixture.definition('DGS10'))
    def test_receipt_byte_type_must_be_exact_for_csv_and_api(self):
        raw,body,receipt,_=api_case('DGS10')
        for content,rec in [(body,receipt),(raw,fixture.receipt('DGS10',raw))]:
            rec['bytes']=float(rec['bytes'])
            with self.assertRaises(ValueError):candidate.build_source('DGS10',content,rec,fixture.NOW,fixture.definition('DGS10'))
    def test_request_day_can_cross_midnight_without_faking_a_new_vintage(self):
        _,body,receipt,_=api_case('DGS10');receipt['acquired_at']='2026-09-27T00:00:10Z';stamp='2026-09-27T00:00:20Z'
        out=candidate.build_source('DGS10',body,receipt,stamp,fixture.definition('DGS10'),requested_url=receipt['source_url'])
        self.assertEqual(out['quality']['status'],'within_age_ceiling');self.assertEqual(out['source_identity']['population']['realtime_end'],'2026-09-26')
        independent.verify(out,body,receipt,fixture.definition('DGS10'),requested_url=receipt['source_url'])
        for changed in ('2026-09-25T23:59:00Z','2026-09-29T00:00:00Z'):
            receipt['acquired_at']=changed
            with self.assertRaises(ValueError):candidate.build_source('DGS10',body,receipt,changed,fixture.definition('DGS10'))
    def test_declared_failed_request_retains_api_identity_without_inventing_a_receipt(self):
        url=fred.source_url('DGS10',fixture.NOW[:10])
        out=candidate.build_source('DGS10',None,None,fixture.NOW,fixture.definition('DGS10'),requested_url=url)
        self.assertEqual(out['specification']['provider'],'fred_api');self.assertEqual(out['requested_url'],url);self.assertIsNone(out['receipt'])
        self.assertEqual(out['quality']['status'],'unavailable');independent.verify(out,None,None,fixture.definition('DGS10'),requested_url=url)

if __name__=='__main__':unittest.main(verbosity=2)
