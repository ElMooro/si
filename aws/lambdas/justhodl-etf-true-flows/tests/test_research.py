from datetime import datetime,timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
import csv,gzip,hashlib,io,json,unittest
import etf_native as n,etf_research as m,etf_store as s

AT='2026-09-19T12:00:00+00:00'
DATES=['2026-09-14','2026-09-15','2026-09-16','2026-09-17','2026-09-18']

def row(day,nav='100',shares='100',**kw):
    return {'date':day,'nav_decimal':nav,'shares_decimal':shares,'share_display_quantum_decimal':'1','nav_display_quantum_decimal':'0.01',**kw}

def xml_history(rows,extra=False):
    ns=n.NS['s'];header=n.ISHARES_HEADER+(['Non-FV NAV'] if extra else [])
    def table(values):return '<ss:Table>'+''.join('<ss:Row>'+''.join('<ss:Cell><ss:Data>'+v+'</ss:Data></ss:Cell>' for v in r)+'</ss:Row>' for r in values)+'</ss:Table>'
    histories=[[datetime.strptime(v['date'],'%Y-%m-%d').strftime('%b %d, %Y'),v.get('nav_decimal','--'),v.get('distribution_decimal','--'),v.get('shares_decimal','--')]+(['80'] if extra else []) for v in reversed(rows)]
    return ('<ss:Workbook xmlns:ss="'+ns+'"><ss:Worksheet ss:Name="Performance">'+table([['Test Fund']])+'</ss:Worksheet><ss:Worksheet ss:Name="Historical">'+table([header]+histories)+'</ss:Worksheet></ss:Workbook>').encode()

def fixture():
    raw={};refs={}
    def add(label,url,body):
        if not isinstance(body,bytes):body=m.encoded(body)
        source=n.public_source_url(url);sha=hashlib.sha256(body).hexdigest();key='data/evidence/etf_original/'+hashlib.sha256(source.encode()).hexdigest()+'/'+sha+'.bin.gz'
        raw[key]=body;refs[label]={'url':url,'acquired_at':AT,'evidence':{'key':key,'sha256':sha,'bytes':len(body),'provider':'etf_original','source_url':source,'contract':'source-evidence.v1','captured':True,'first_received_at':AT}}
    days=[];day=datetime(2026,8,1)
    while day.strftime('%Y-%m-%d')<='2026-09-18':
        if day.weekday()<5:days.append(day.strftime('%Y-%m-%d'))
        day+=timedelta(days=1)
    rows=[row(day,nav=str(100+i),shares=str(1000+i)) for i,day in enumerate(days)]
    catalog={'239726':{'localExchangeTicker':'IVV','portfolioId':239726,'productPageUrl':'/us/products/239726/test-fund','isin':'US4642872000','cusip':'464287200','fundName':'Test Fund','navAmount':{'r':int(rows[-1]['nav_decimal'])},'navAmountAsOf':{'r':20260918},'totalNetAssets':{'r':1},'totalNetAssetsFundAsOf':{'r':20260918}}}
    add('ishares_catalog',n.ISHARES_URL,catalog)
    add('ssga_catalog',n.SSGA_URL,{'data':{'funds':{'etfs':{'datas':[{'fundTicker':'UNUSED'}]}}}})
    out=io.StringIO();csv.writer(out).writerow(n.SPLIT_HEADER);add('proshares_splits',n.SPLIT_URL,out.getvalue().encode())
    add('ishares_IVV',n.DOWNLOAD.format(pid=239726),xml_history(rows))
    return raw,{'contract':'etf-original-inputs.v1','originals':refs,'acquisition_errors':{},'legacy':{'fixture':True}},rows

class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}

class Storage:
    def __init__(self):self.data={};self.calls=[]
    def get_object(self,**args):
        key=args['Key']
        if key not in self.data:raise Error('NoSuchKey')
        raw=self.data[key];return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**args):
        key=args['Key'];old=self.data.get(key)
        if args.get('IfNoneMatch')=='*' and old is not None:raise Error('PreconditionFailed')
        if args.get('IfMatch') and (old is None or hashlib.sha256(old).hexdigest()!=args['IfMatch']):raise Error('PreconditionFailed')
        raw=args['Body'];self.data[key]=raw if isinstance(raw,bytes) else raw.encode();self.calls.append(key)

class Tests(unittest.TestCase):
    def test_active_http_handler_reads_without_collecting_or_promoting_legacy(self):
        import ast
        source=Path(s.__file__).with_name('lambda_function.py').read_text()
        tree=ast.parse(source);node=next(v for v in tree.body if isinstance(v,ast.FunctionDef) and v.name=='lambda_handler')
        client=Storage();scope={'json':json,'s3':client,'BUCKET':'bucket','FMP_KEY':'fixture'}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'<actual HTTP handler>','exec'),scope)
        client.data[m.CURRENT]=m.encoded({'contract':m.CONTRACT,'call':None})
        with patch.object(s,'run',side_effect=AssertionError('HTTP must not collect')):
            result=scope['lambda_handler']({'requestContext':{'http':{'method':'GET'}}})
            self.assertEqual(result['statusCode'],200);self.assertEqual(result['headers']['Cache-Control'],'no-store')
            self.assertEqual(json.loads(result['body']),json.loads(client.data[m.CURRENT]))
            client.data[m.CURRENT]=m.encoded({'legacy':True})
            self.assertEqual(scope['lambda_handler']({'httpMethod':'GET'})['statusCode'],503)

    def test_flow_sums_each_nav_not_endpoint_times_latest_nav(self):
        days=DATES[:3];rows=[row(days[0]),row(days[1],shares='110'),row(days[2],nav='200',shares='120')]
        result=n.window(n.flow_history(rows,days,[]),days,2)
        self.assertEqual(n.dec(result['value_decimal']),3000);self.assertNotEqual(n.dec(result['value_decimal']),4000)

    def test_dividend_never_fabricates_external_capital(self):
        rows=[row(d,distribution_decimal='5') for d in DATES]
        result=n.flow_history(rows,DATES,[])
        self.assertEqual(n.dec(result[-1]['nav_valued_share_change_decimal']),0)

    def test_missing_source_date_does_not_compress_window(self):
        rows=[row(d) for d in DATES if d!=DATES[2]];result=n.window(n.flow_history(rows,DATES,[]),DATES,3)
        self.assertIsNone(result['value_decimal']);self.assertEqual(result['status'],'incomplete_or_unreviewed')

    def test_action_transition_not_counted_as_issuance(self):
        rows=[row(d,shares='200' if i>1 else '100',nav='50' if i>1 else '100') for i,d in enumerate(DATES)]
        result=n.flow_history(rows,DATES,[{'date':DATES[2]}])
        self.assertEqual(result[2]['flow_status'],'corporate_action_transition_requires_review');self.assertIsNone(result[2]['nav_valued_share_change_decimal'])
        self.assertEqual(result[3]['nav_valued_share_change_decimal'],'0')

    def test_unlisted_inverse_discontinuity_requires_review(self):
        result=n.flow_history([row(DATES[0]),row(DATES[1],nav='50',shares='200')],DATES[:2],[])
        self.assertEqual(result[-1]['flow_status'],'possible_split_or_unreviewed_action')

    def test_zero_nav_and_zero_rounded_shares_are_not_flow_inputs(self):
        rows=[row(DATES[0],nav='0',shares='0'),row(DATES[1]),row(DATES[2])]
        result=n.flow_history(rows,DATES[:3],[])
        self.assertIsNone(result[1]['nav_valued_share_change_decimal']);self.assertEqual(result[2]['flow_status'],'descriptive_estimate')

    def test_proshares_thousand_unit_and_exact_decimal(self):
        out=io.StringIO();writer=csv.writer(out);writer.writerow(n.PRO_HEADER)
        writer.writerow(['09/18/2026','Test','TQQQ','10.25','10','2.5','.25','100.25','1027562.5'])
        result=n.proshares_history(out.getvalue().encode(),'TQQQ',AT)['rows'][0]
        self.assertEqual(n.dec(result['shares_decimal']),100250);self.assertEqual(n.dec(result['aum_residual_decimal']),0)

    def test_future_or_duplicate_native_dates_fail(self):
        for rows in ([row('2026-09-20')],[row(DATES[0]),row(DATES[0])]):
            with self.assertRaises(ValueError):n.validate_dates(rows,AT)

    def test_raw_non_fair_value_nav_never_substitutes(self):
        result=n.ishares_history(xml_history([row(DATES[-1])],extra=True),{'fund_name':'Test Fund'},AT)['rows'][0]
        self.assertEqual(result['nav_decimal'],'100');self.assertEqual(result['non_fair_value_nav_decimal'],'80')

    def test_unknown_columns_and_external_entities_fail(self):
        raw=xml_history([row(DATES[-1])])
        for changed in (raw.replace(b'NAV per Share',b'Market price'),b'<!DOCTYPE x [<!ENTITY test SYSTEM "file:///tmp/test">]>'+raw):
            with self.assertRaises(ValueError):n.ishares_history(changed,{'fund_name':'Test Fund'},AT)

    def test_original_hash_identity_and_clock_fail_closed(self):
        raw,inputs,_=fixture();ref=inputs['originals']['ishares_IVV'];url=n.DOWNLOAD.format(pid=239726)
        self.assertEqual(n.original(ref,raw.__getitem__,AT,url),raw[ref['evidence']['key']])
        with self.assertRaises(ValueError):n.original(ref,lambda k:b'changed',AT,url)
        with self.assertRaises(ValueError):n.original(ref,raw.__getitem__,'2026-09-18T12:00:00+00:00',url)

    def test_unknown_vendor_shares_do_not_restore_missing_flow(self):
        raw,inputs,_=fixture();output,histories=m.build(inputs,raw.__getitem__,AT)
        self.assertEqual(output['n_etfs'],128);self.assertIsNone(output['by_etf']['QQQ']['net_flow_5d_usd']);self.assertFalse(output['calls_eligible']);self.assertFalse(output['sizing_eligible']);self.assertIsNone(output['portfolio_impact'])
        self.assertEqual(output['quality']['native_histories'],1);self.assertEqual(output['quality']['status'],'partial');self.assertEqual(len(histories),2)

    def test_same_period_subtotals_keep_zero_distinct_from_missing(self):
        window={'value_decimal':'0','start_date':'2026-09-10','end_date':'2026-09-17'}
        rows={'X':{'ticker':'X','net_flow_5d_usd':0,'flow_windows':{'5d':window}}}
        self.assertEqual(m.aggregate(['X','Y'],rows)['net_flow_5d_usd'],0)
        self.assertIsNone(m.aggregate(['Y'],rows)['net_flow_5d_usd'])
        rows['Y']={'ticker':'Y','net_flow_5d_usd':0,'flow_windows':{'5d':{**window,'end_date':'2026-09-18'}}}
        with self.assertRaises(ValueError):m.aggregate(['X','Y'],rows)

    def test_history_download_contains_every_source_observation(self):
        raw,inputs,rows=fixture();output,histories=m.build(inputs,raw.__getitem__,AT);ref=output['by_etf']['IVV']['history'];history=json.loads(histories[ref['key']])
        self.assertEqual(len(history['rows']),len(rows));self.assertEqual(history['rows'][0]['date'],rows[0]['date']);self.assertEqual(hashlib.sha256(histories[ref['key']]).hexdigest(),ref['sha256'])

    def test_whole_legacy_snapshots_preserved_before_publication(self):
        client=Storage();keys=(m.CURRENT,'data/etf-shares-history.json','data/etf-shares-snapshots/latest.json')
        for k in keys:client.data[k]=m.encoded({'legacy':k})
        marker=s.preserve(client,'bucket',s.raw_reader(client,'bucket'))
        self.assertEqual(len(marker['objects']),3)
        for value in marker['objects']:self.assertEqual(client.data[s.PRIVATE+value['sha256']+'.bin'],client.data[value['source']])
        self.assertEqual(s.preserve(client,'bucket',s.raw_reader(client,'bucket')),marker)

    def test_publication_cannot_regress_generation_or_reference_date(self):
        client=Storage();packet={'contract':m.CONTRACT,'generated_at':AT,'reference_calendar':{'latest_date':'2026-09-18'},'source_clocks':{'native':AT}}
        self.assertTrue(s.publish(client,'bucket',packet));before=client.data[m.CURRENT]
        self.assertFalse(s.publish(client,'bucket',{**packet,'generated_at':'2026-09-18T12:00:00+00:00'}))
        self.assertFalse(s.publish(client,'bucket',{**packet,'reference_calendar':{'latest_date':'2026-09-17'}}));self.assertEqual(client.data[m.CURRENT],before)

    def test_real_store_retains_replay_and_rejects_tampering(self):
        raw,inputs,_=fixture();client=Storage();client.data[m.CURRENT]=m.encoded({'legacy':True})
        for k,v in raw.items():client.data[k]=gzip.compress(v,mtime=0)
        with patch.object(s,'collect',return_value=(inputs['originals'],{})),patch.object(s,'now',return_value=AT):result=s.run(client,'bucket','fixture')
        self.assertTrue(result['published']);read=s.raw_reader(client,'bucket');manifest=json.loads(read(result['replay']['manifest_key']))
        self.assertEqual(s.replay(manifest,read)['contract'],m.CONTRACT)
        history=json.loads(read(m.CURRENT))['by_etf']['IVV']['history']['key'];client.data[history]=b'{}'
        with self.assertRaises(ValueError):s.replay(manifest,read)

    def test_failed_source_candidate_retains_previous_complete_publication(self):
        client=Storage();before=m.encoded({'legacy':True});client.data[m.CURRENT]=before
        with patch.object(s,'collect',return_value=({}, {'ishares_catalog':'HTTP_503'})),patch.object(s,'now',return_value=AT):
            with self.assertRaises(KeyError):s.run(client,'bucket',None)
        self.assertEqual(client.data[m.CURRENT],before);self.assertTrue(any('/attempts/' in k for k in client.data))

    def test_ssga_catalog_million_unit_is_explicit(self):
        v={'fundTicker':'SPY','domicile':'US','fundUri':'/us/en/intermediary/etfs/test-spy','fundName':'Test',
           'documentPdf':[{'docType':'Navhist','docs':[{'path':'/library-content/products/fund-data/etfs/us/navhist-us-en-spy.xlsx'}]}],
           'nav':['$100.00',100],'aum':['$1,000.00 M',1000],'asOfDate':['Sep 18 2026','2026-09-18']}
        raw=m.encoded({'data':{'funds':{'etfs':{'datas':[v]}}}});parsed=n.ssga_catalog(raw,{'SPY'})
        self.assertEqual(n.dec(parsed['SPY']['net_assets_decimal']),1000000000)
