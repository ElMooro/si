import base64,csv,gzip,hashlib,io,json,unittest
from urllib.error import HTTPError
import bis_fx_series as m

HEADER=['FREQ','REF_AREA','CURRENCY','COLLECTION','UNIT_MULT','TIME_PERIOD','OBS_VALUE','OBS_STATUS','OBS_CONF','OBS_PRE_BREAK']
def fixture(rows=None):
    buffer=io.StringIO(newline='');writer=csv.writer(buffer);writer.writerow(HEADER)
    writer.writerows(rows or [['D','JP','JPY','A','0','2026-01-02','150.25','A','F','']]);return buffer.getvalue().encode()
class Tests(unittest.TestCase):
    def test_every_catalog_identity_is_exact_and_new_objects_do_not_mutate_it(self):
        self.assertEqual(len(m.CATALOG['series']),1234)
        for key,row in m.CATALOG['series'].items():
            d=m.definition('bis:WS_XRU:'+key);self.assertEqual(d['unit'],row['currency']+' per USD')
            d['currency']='BAD';self.assertEqual(m.definition('bis:WS_XRU:'+key)['currency'],row['currency'])
        for identifier in ['bis:WS_XRU:D.JP.USD.A','bis:WS_XRU:D.XX.USD.A','bis:WS_XRU:D.JP.JPY.E','bis:WS_CBPOL:D.JP']:
            with self.assertRaises(ValueError):m.definition(identifier)
    def test_zero_negative_nan_missing_status_and_private_are_not_rates(self):
        rows=[['D','JP','JPY','A','0','2026-01-0'+str(i+1),value,status,conf,''] for i,(value,status,conf) in enumerate([('0','A','F'),('-1','A','F'),('NaN','M','F'),('10','M','F'),('150','A','C'),('150.25','A','F')])]
        doc=m.fetch('bis:WS_XRU:D.JP.JPY.A',lambda _:(fixture(rows),{}))
        self.assertEqual(doc['n'],1);self.assertEqual(doc['quality']['rejected_rows'],5)
        self.assertEqual([v for _,v in doc['obs']],[None]*5+[150.25])
    def test_wrong_country_currency_collection_and_scale_reject_packet(self):
        for pos,value in [(0,'M'),(1,'US'),(2,'USD'),(3,'E'),(4,'3')]:
            row=['D','JP','JPY','A','0','2026-01-02','150','A','F',''];row[pos]=value
            doc=m.fetch('bis:WS_XRU:D.JP.JPY.A',lambda _:(fixture([row]),{}));self.assertEqual(doc['n'],0);self.assertFalse(doc['history']['response_complete'])
    def test_duplicates_are_withheld_not_deduplicated(self):
        row=['D','JP','JPY','A','0','2026-01-02','150','A','F',''];doc=m.fetch('bis:WS_XRU:D.JP.JPY.A',lambda _:(fixture([row,row]),{}))
        self.assertEqual(doc['obs'],[['2026-01-02',None]]);self.assertEqual(doc['quality']['rejected_rows'],2)
    def test_original_response_is_replayable_and_not_rewritten(self):
        raw=fixture();doc=m.fetch('bis:WS_XRU:D.JP.JPY.A',lambda _:(gzip.compress(raw),{'Set-Cookie':'DO-NOT-RETAIN','Content-Type':'text/csv'}));receipt=doc['source_receipts'][0]
        self.assertEqual(gzip.decompress(base64.b64decode(receipt['body_base64'])),raw)
        self.assertEqual(receipt['sha256'],hashlib.sha256(raw).hexdigest());self.assertNotIn('Set-Cookie',receipt['headers'])
        self.assertFalse(doc['equivalence_to_watchlist_provider_verified']);self.assertFalse(doc['calls_eligible']);self.assertFalse(doc['sizing_eligible'])
        for key in ['full_upstream_history_verified','point_in_time_vintages_verified','same_time_fix_verified','redenomination_continuity_verified','release_clock_verified']:
            self.assertFalse(doc['history'][key])
    def test_period_anchors_preserve_frequency(self):
        for original,freq,anchor in [('2026','A','2026-01-01'),('2026-Q4','Q','2026-10-01'),('2026-02','M','2026-02-01'),('2026-02-28','D','2026-02-28')]:self.assertEqual(m.period(original,freq),anchor)
        for original,freq in [('2026-Q5','Q'),('2026-02-30','D'),('2026','D'),('2026-13','M')]:self.assertIsNone(m.period(original,freq))
    def test_denial_is_one_attempt_without_fallback(self):
        for status in [401,403,429]:
            called=[]
            def denied(url):called.append(url);raise HTTPError(url,status,'denied',{},None)
            doc=m.fetch('bis:WS_XRU:D.JP.JPY.A',denied);self.assertEqual(len(called),1);self.assertEqual(doc['source_receipts'][0]['http_status'],status);self.assertEqual(doc['n'],0)
    def test_cache_binds_exact_catalog_definition_and_complete_response(self):
        doc=m.fetch('bis:WS_XRU:D.JP.JPY.A',lambda _:(fixture(),{}));self.assertTrue(m.cache_valid(doc,doc['id']))
        for key,value in [('definition_sha256','bad'),('contract','bad'),('id','bis:WS_XRU:D.XW.XDR.A')]:
            wrong={**doc,key:value};self.assertFalse(m.cache_valid(wrong,doc['id']))
    def test_directory_pagination_has_no_duplicates_or_omissions(self):
        ids=[]
        for offset in range(0,1234,200):ids.extend(r['id'] for r in m.directory('',200,offset)['rows'])
        self.assertEqual(len(ids),1234);self.assertEqual(len(set(ids)),1234)
if __name__=='__main__':unittest.main()
