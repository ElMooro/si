from copy import deepcopy
from datetime import date,timedelta
from pathlib import Path
import json,unittest
import run_tests as native
import fifx_fred as protocol

def whole():
    return {'realtime_start':'2026-09-30','realtime_end':'2026-09-30','observation_start':'1988-01-01','observation_end':'2026-09-30',
      'units':'lin','output_type':1,'file_type':'json','order_by':'observation_date','sort_order':'asc','offset':0,'limit':50000,'count':40,
      'observations':[{'realtime_start':'2026-09-30','realtime_end':'2026-09-30','date':str(date(2026,8,22)+timedelta(days=i)),
                       'value':['0','-1.50','.','0.0000','1.25e-3'][i%5]} for i in range(40)]}

def parse(doc,sid='DGS10'):
    return protocol.original(json.dumps(doc).encode(),sid,protocol.source_url(sid,'2026-09-30'),'2026-09-30')

class Tests(unittest.TestCase):
    def test_full_requests_keep_every_numeric_lexeme_and_explicit_range(self):
        for sid in protocol.SERIES:
            doc=whole();rows,pop=parse(doc,sid)
            self.assertEqual([r['value'] for r in rows],[r['value'] for r in doc['observations']])
            self.assertEqual(len(rows),40);self.assertTrue(pop['complete_requested_window']);self.assertFalse(pop['full_series_history'])
    def test_no_short_page_count_aggregation_realtime_or_type_substitution(self):
        changes=[('count',39),('count',41),('count',40.0),('count',True),('count',50001),('offset',1),('offset',False),('limit',4000),
                 ('units','pch'),('output_type',4),('sort_order','desc'),('realtime_end','2026-09-29'),('observation_start','2000-01-01')]
        for key,value in changes:
            doc=whole();doc[key]=value
            with self.subTest(key=key,value=value),self.assertRaises(ValueError):parse(doc)
    def test_no_auth_parameter_duplicate_query_or_wrong_identity_can_be_public(self):
        url=protocol.source_url('DGS10','2026-09-30')
        for changed in [url+'&api_key=INVENTED',url+'&units=lin',url.replace('DGS10','VIXCLS'),url.replace('https:','http:'),url+'#fragment']:
            with self.assertRaises(ValueError):protocol.original(json.dumps(whole()).encode(),'DGS10',changed,'2026-09-30')
    def test_duplicate_members_and_invalid_source_tokens_fail(self):
        for value in [0,False,None,'','NaN','Infinity',' 1.0','1_000','1e1000','9'*41]:
            doc=whole();doc['observations'][0]['value']=value
            with self.subTest(value=value),self.assertRaises(ValueError):parse(doc)
        raw=b'{"count":40,'+json.dumps(whole()).encode()[1:]
        with self.assertRaises(ValueError):protocol.original(raw,'DGS10',protocol.source_url('DGS10','2026-09-30'),'2026-09-30')
    def test_duplicates_future_rows_and_row_vintage_fail(self):
        for change in [lambda d:d['observations'][1].update(date=d['observations'][0]['date']),lambda d:d['observations'][-1].update(date='2026-10-01'),
                       lambda d:d['observations'][1].update(realtime_start='2026-09-29')]:
            doc=whole();change(doc)
            with self.assertRaises(ValueError):parse(doc)

if __name__=='__main__':unittest.main(verbosity=2)
