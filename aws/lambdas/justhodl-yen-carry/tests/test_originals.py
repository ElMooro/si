import copy,gzip,hashlib,io,json,sys,unittest
from datetime import datetime,timezone
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(Path(__file__).resolve().parents[1]/'source')]
import yen_original as n
import yen_research as m
import yen_store as s
import evidence_store
AT='2026-09-19T13:00:00+00:00'

class StorageError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}
class Storage:
    def __init__(self):self.objects={};self.meta={}
    def get_object(self,**kw):
        key=kw['Key']
        if key not in self.objects:raise StorageError('NoSuchKey')
        raw=self.objects[key]
        return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest(),'Metadata':self.meta.get(key,{})}
    def put_object(self,**kw):
        key=kw['Key'];old=self.objects.get(key)
        if kw.get('IfNoneMatch')=='*' and old is not None:raise StorageError('412')
        if 'IfMatch' in kw and (old is None or hashlib.sha256(old).hexdigest()!=kw['IfMatch']):raise StorageError('412')
        self.objects[key]=kw['Body'];self.meta[key]=kw.get('Metadata',{})

def retain(client,url,document):
    receipt=evidence_store.capture(client,'b','yen',url,m.encoded(document),n.clock(AT))
    return {'url':url,'acquired_at':AT,'evidence':receipt}

def fred(client,sid='DEXJPUS',last=None):
    definition,urls=n.fred_urls(sid,AT);unit,freq=n.SERIES[sid][1:3]
    out={'definition':retain(client,definition,{'seriess':[{'id':sid,'title':sid,'units':unit,'frequency_short':freq,'seasonal_adjustment':'Not Seasonally Adjusted'}]}),'observations':[]}
    for i,(start,end) in enumerate(n.segments(sid,AT)):
        day=last or ('2026-09-11' if sid=='DEXJPUS' else '2026-08-01') if i==len(urls)-1 else end
        doc={'units':'lin','count':1,'limit':4000,'offset':0,'observations':[{'date':day,'value':'153.71'}]}
        out['observations'].append(retain(client,urls[i],doc))
    return out

def cot_record(day='2026-09-15'):
    row={'cftc_contract_market_code':'097741','futonly_or_combined':'FutOnly','market_and_exchange_names':'JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE',
       'contract_units':'(CONTRACTS OF JPY 12,500,000)','report_date_as_yyyy_mm_dd':day+'T00:00:00.000','open_interest_all':'1000',
       'tot_rept_positions_long_all':'800','tot_rept_positions_short':'800'}
    for name,(a,b,c) in n.CLASSES.items():
        row[a]=row[b]='200' if c is None else '190'
        if c:row[c]='10'
    return row

def cot(client,records=None):
    records=records or [cot_record()];keys={v for t in n.CLASSES.values() for v in t if v}|{'open_interest_all','tot_rept_positions_long_all','tot_rept_positions_short'}
    meta={'id':'gpe5-46if','name':'TFF - Futures Only','columns':[{'fieldName':k,'dataTypeName':'number'} for k in keys]}
    return {k:retain(client,n.CFTC_URLS[k],v) for k,v in {'metadata':meta,'count':[{'count':str(len(records))}],'rows':records}.items()}

class Originals(unittest.TestCase):
    def test_complete_segment_history_and_missing_latest(self):
        c=Storage();refs=fred(c);item=n.fred('DEXJPUS',refs,s.raw_reader(c,'b'),AT)
        self.assertEqual(len(item['rows']),3);self.assertEqual(item['rows'][0]['date'],'2009-12-31')
        self.assertEqual(item['quality']['status'],'fresh');self.assertEqual(item['quality']['observation_age_days'],8)
        doc={'units':'lin','count':1,'limit':4000,'offset':0,'observations':[{'date':'2026-09-11','value':'.'}]}
        refs['observations'][-1]=retain(c,n.fred_urls('DEXJPUS',AT)[1][-1],doc)
        self.assertIsNone(n.fred('DEXJPUS',refs,s.raw_reader(c,'b'),AT)['value_decimal'])

    def test_truncated_segment_wrong_units_or_identity_rejected(self):
        for bad in ('count','unit','url'):
            c=Storage();refs=fred(c)
            if bad=='count':
                doc={'units':'lin','count':2,'limit':4000,'offset':0,'observations':[{'date':'2026-09-11','value':'1'}]}
                refs['observations'][-1]=retain(c,n.fred_urls('DEXJPUS',AT)[1][-1],doc)
            elif bad=='unit':
                refs['definition']=retain(c,n.fred_urls('DEXJPUS',AT)[0],{'seriess':[{'id':'DEXJPUS','title':'x','units':'USD per JPY','frequency_short':'D','seasonal_adjustment':'NSA'}]})
            else:refs['observations'][-1]['url']='https://wrong.example/'
            with self.assertRaises(ValueError):n.fred('DEXJPUS',refs,s.raw_reader(c,'b'),AT)

    def test_h10_weekly_cutoff_and_observation_clock(self):
        before=n.quality('DEXJPUS','2026-09-11',AT,'2026-09-21T20:14:00Z')
        after=n.quality('DEXJPUS','2026-09-11','2026-09-21T20:15:00Z','2026-09-21T20:15:00Z')
        self.assertEqual(before['nominal_expected_observation_date'],'2026-09-11')
        self.assertEqual(after['status'],'release_due_unverified')
        self.assertEqual(after['nominal_expected_observation_date'],'2026-09-18')
        self.assertFalse(after['holiday_calendar_verified'])

    def test_all_cftc_categories_reconcile_without_double_counting(self):
        c=Storage();v=n.cftc(cot(c),s.raw_reader(c,'b'),AT)
        self.assertEqual(len(v['current']['categories']),5);self.assertEqual(v['current']['open_interest'],1000)
        self.assertIsNone(v['current']['categories']['nonreportable']['spreading'])
        self.assertIsNone(v['whole_carry_trade_size']);self.assertIsNone(v['statistics']['dealer']['z'])

    def test_cftc_invalid_population_counts_and_duplicate_dates_fail_whole_query(self):
        for field,value in [('open_interest_all','1001'),('lev_money_positions_long','190.5'),('futonly_or_combined','Combined'),('contract_units','USD 100000')]:
            c=Storage();row=cot_record();row[field]=value
            with self.assertRaises(ValueError):n.cftc(cot(c,[row]),s.raw_reader(c,'b'),AT)
        c=Storage()
        with self.assertRaises(ValueError):n.cftc(cot(c,[cot_record(),cot_record()]),s.raw_reader(c,'b'),AT)

    def test_cftc_monday_holiday_report_is_retained(self):
        c=Storage();v=n.cftc(cot(c,[cot_record('2026-09-14')]),s.raw_reader(c,'b'),AT)
        self.assertEqual(v['as_of'],'2026-09-14')

    def test_original_byte_corruption_cannot_replay(self):
        c=Storage();refs=cot(c);key=refs['rows']['evidence']['key'];c.objects[key]=gzip.compress(b'[]')
        with self.assertRaises(ValueError):n.cftc(refs,s.raw_reader(c,'b'),AT)

    def test_latest_month_not_replaced_with_old_common_month(self):
        item=lambda rows:{'rows':rows,'quality':{'status':'fresh'},'as_of':rows[-1]['date']}
        row=lambda day,value:{'date':day,'value_decimal':value,'segment':0,'row_index':0}
        us=item([row(f'2026-07-{i:02}','4') for i in range(1,32)])
        jp=item([row('2026-07-01','1'),row('2026-08-01','1')])
        r=m.monthly_pair({'DFF':us,'IR3TIB01JPM156N':jp},'x','DFF','IR3TIB01JPM156N')
        self.assertIsNone(r['value']);self.assertEqual(r['status'],'latest_month_incomplete');self.assertEqual(r['rows'][0]['value_decimal'],'3')

    def test_store_replays_history_retains_legacy_and_rejects_rewind(self):
        c=Storage();legacy=m.encoded({'generated_at':AT,'old_inputs':['keep all']});c.objects[m.CURRENT]=legacy
        sources={sid:fred(c,sid) for sid in n.SERIES};positioning=cot(c)
        with patch.object(s,'collect',return_value=(sources,positioning,{})),patch.object(s,'now',return_value=AT):
            result=s.run(c,'b','configured-test-key')
        self.assertTrue(result['published']);p=json.loads(c.objects[m.CURRENT]);manifest=json.loads(c.objects[p['replay']['manifest_key']])
        self.assertEqual(s.replay(manifest,s.raw_reader(c,'b')), {k:v for k,v in p.items() if k!='replay'})
        self.assertEqual(c.objects[s.PRIVATE+hashlib.sha256(legacy).hexdigest()+'.bin'],legacy)
        old=copy.deepcopy(p);old['generated_at']='2026-09-18T13:00:00Z';self.assertFalse(s.publish(c,'b',old))
        h=next(iter(p['measurements'].values()))['history']['key'];c.objects[h]=b'{}'
        with self.assertRaises(ValueError):s.replay(manifest,s.raw_reader(c,'b'))

    def test_empty_or_invalid_sources_have_no_vote(self):
        p,_=m.build({'contract':'yen-original-inputs.v1','legacy':{},'fred':{},'cftc':None},lambda k:b'',AT)
        self.assertEqual(p['quality']['status'],'unavailable');self.assertFalse(p['calls_eligible']);self.assertIsNone(p['unwind_risk_score'])

    def test_current_incomplete_month_is_not_a_completed_month_average(self):
        row=lambda day:{'date':day,'value_decimal':'3','segment':0,'row_index':0}
        us={'rows':[row(f'2026-09-{i:02}') for i in range(1,19)],'as_of':'2026-09-18','quality':{'status':'fresh'}}
        jp={'rows':[row('2026-09-01')],'as_of':'2026-09-01','quality':{'status':'fresh'}}
        v=m.monthly_pair({'DGS10':us,'IRLTLT01JPM156N':jp},'x','DGS10','IRLTLT01JPM156N',AT)
        self.assertIsNone(v['value']);self.assertEqual(v['status'],'latest_month_incomplete')

    def test_latest_missing_fixing_cannot_be_backfilled_for_volatility(self):
        from datetime import timedelta
        rows=[{'date':(datetime(2026,8,1)+timedelta(days=i)).date().isoformat(),'value_decimal':str(140+i/10),'segment':0,'row_index':i} for i in range(40)]
        self.assertIsNotNone(m.fx_measurement({'rows':rows})['volatility']['20']['annualized_pct'])
        rows[-1]['value_decimal']=None
        self.assertIsNone(m.fx_measurement({'rows':rows})['volatility']['20']['annualized_pct'])

    def test_http_reader_does_not_collect_or_mutate(self):
        import ast
        tree=ast.parse((Path(__file__).resolve().parents[1]/'source/lambda_function.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        c=Storage();body=m.encoded({'contract':m.CONTRACT});c.objects[m.CURRENT]=body
        scope={'s3':c,'S3_BUCKET':'b','FRED_KEY':'no-collection','json':json}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-yen-handler','exec'),scope)
        with patch.object(s,'run',side_effect=AssertionError('collection forbidden')):
            out=scope['lambda_handler']({'requestContext':{'http':{'method':'GET'}}})
        self.assertEqual(out['statusCode'],200);self.assertEqual(out['body'],body.decode());self.assertEqual(len(c.objects),1)

if __name__=='__main__':unittest.main()
