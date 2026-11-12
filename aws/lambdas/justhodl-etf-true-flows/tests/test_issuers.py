"""Issuer registry: parsers against the issuers' own originals (ops 6799/6800, 2026-11-12), ledger accrual, the build
and the store's pointer discipline."""
from datetime import datetime,timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
import hashlib,io,json,re,time,unittest,urllib.error
import etf_issuers as iss,etf_native as n,etf_research as m,etf_store as s
from test_research import AT,Storage,fixture

FIX=Path(__file__).resolve().parent/'fixtures'/'issuers'
NOW='2026-11-12T17:30:00+00:00'

def ref(url,at=NOW):return {'url':url,'acquired_at':at,'evidence':{'key':'data/evidence/etf_original/x/'+'0'*64+'.bin.gz','sha256':'0'*64}}
def fx(name):return (FIX/name).read_bytes()

class Parsers(unittest.TestCase):
    def test_globalx_dated_cards(self):
        row=iss.globalx_snapshot(fx('globalx_botz_cards.html'),'BOTZ',ref(iss.GLOBALX_URL.format(slug='botz')))
        self.assertEqual((row['date'],row['nav_decimal'],row['shares_decimal'],row['net_assets_decimal'],row['net_assets_display']),('2026-11-11','32.20','92970000',None,'$2.99 billion'))
        self.assertEqual((row['share_display_quantum_decimal'],row['nav_display_quantum_decimal'],row['issuer_isin']),('1','0.01','US37954Y7159'))
        with self.assertRaises(ValueError):iss.globalx_snapshot(fx('globalx_botz_cards.html'),'LIT',ref('u'))
        shifted=fx('globalx_botz_cards.html').replace(b'ETF Prices</h3><p class="text-xs text-muted-foreground">As of Nov 11, 2026',b'ETF Prices</h3><p class="text-xs text-muted-foreground">As of Nov 10, 2026')
        with self.assertRaises(ValueError):iss.globalx_snapshot(shifted,'BOTZ',ref('u'))   # NAV and fund-details as-of dates must agree

    def test_vaneck_page_date_binds_the_undated_block(self):
        page=fx('vaneck_gdx_overview_items.html');block=fx('vaneck_gdx_block_reduced.json')
        row=iss.vaneck_snapshot(page,block,'GDX',ref('p'),ref('b'))
        self.assertEqual((row['date'],row['nav_decimal'],row['shares_decimal'],row['net_assets_decimal'],row['issuer_cusip']),('2026-11-11','85.47','309552500',None,'92189F106'))
        self.assertEqual(len(row['source']),2)
        moved=block.replace(b'"AsOfDate": "2026-11-11T00:00:00"',b'"AsOfDate": "2026-11-10T00:00:00"',1)
        with self.assertRaises(ValueError):iss.vaneck_snapshot(page,moved,'GDX',ref('p'),ref('b'))
        with self.assertRaises(ValueError):iss.vaneck_snapshot(page,block.replace(b'"$85.47"',b'"$85.48"'),'GDX',ref('p'),ref('b'))

    def test_first_trust_current_fund_data(self):
        row=iss.firsttrust_snapshot(fx('firsttrust_cibr_summary.html'),'CIBR',ref('f'))
        self.assertEqual((row['date'],row['nav_decimal'],row['shares_decimal'],row['net_assets_decimal']),('2026-11-11','96.20','173600002','16700141737'))
        self.assertTrue(row['aum_within_display_precision'])
        with self.assertRaises(ValueError):iss.firsttrust_snapshot(fx('firsttrust_cibr_summary.html'),'HACK',ref('f'))

    def test_robo_nuxt_payload(self):
        row=iss.robo_snapshot(fx('robo_nuxt_data.html'),'ROBO',ref('r'))
        self.assertEqual((row['date'],row['nav_decimal'],row['shares_decimal'],row['net_assets_decimal'],row['issuer_cusip']),('2026-11-11','73.87','25125000','1855897406','301505707'))
        self.assertTrue(row['aum_within_display_precision'])

    def test_invesco_and_uscf_listings(self):
        row=iss.invesco_snapshot(fx('invesco_qqq_prices.json'),'QQQ',ref('i'))
        self.assertEqual((row['date'],row['nav_decimal'],row['shares_decimal'],row['net_assets_decimal']),('2026-11-11','690.234086','686450000','473811188526.53'))
        row=iss.invesco_snapshot(fx('invesco_dbc_prices.json'),'DBC',ref('i'))
        self.assertEqual((row['shares_decimal'],row['net_assets_decimal'],row['aum_within_display_precision']),('56000040','1729363870.54',True))
        with self.assertRaises(ValueError):iss.invesco_snapshot(fx('invesco_dbc_prices.json'),'DBA',ref('i'))
        row=iss.uscf_snapshot(fx('uscf_uso_dailyprice.json'),'USO',ref('u'))
        self.assertEqual((row['date'],row['nav_decimal'],row['shares_decimal'],row['net_assets_decimal'],row['displayed_nav_decimal']),('2026-11-11','141.92213','14423603','2047028408.51','141.92'))
        self.assertTrue(row['aum_within_display_precision'])

    def test_amplify_daily_documents(self):
        listing=json.loads(fx('amplify_hack_daily_list.json'));docs=[m.encoded(d) for d in listing['documents']]
        urls=[iss.AMPLIFY_DOC.format(project='amplify-etfs-data-feed',ticker='HACK',day=d) for d in ('2026-11-10','2026-11-11')]
        doc=iss.amplify_history(docs,'HACK',[ref(u) for u in urls],NOW)
        rows=doc['rows'];self.assertEqual([v['date'] for v in rows],['2026-11-10','2026-11-11'])
        self.assertEqual(iss.amplify_history([fx('amplify_hack_daily_list.json')],'HACK',[ref('l')],NOW)['rows'],[dict(v,source=[{**v['source'][0],'url':'l'}]) for v in rows])
        self.assertEqual((rows[-1]['nav_decimal'],rows[-1]['shares_decimal'],rows[-1]['net_assets_decimal']),('117.4096','28750000','3375526798.46'))
        flows=n.flow_history(rows,['2026-11-10','2026-11-11'],[])
        self.assertEqual(flows[-1]['net_share_change_decimal'],'100000');self.assertEqual(n.dec(flows[-1]['nav_valued_share_change_decimal']),Decimal('11740960.0000'))
        with self.assertRaises(ValueError):iss.amplify_history([docs[0],docs[0]],'HACK',[ref('a'),ref('a')],NOW)
        days=iss.weekdays_back(datetime(2026,11,12).date(),iss.AMPLIFY_DAYS);self.assertEqual((len(days),days[-1],days[0]),(40,'2026-11-12','2026-09-18'))
        self.assertEqual(len(iss.requests_for('HACK',datetime(2026,11,12).date())),40)
        with self.assertRaises(ValueError):iss.requests_for('HACK')

    def test_dws_nav_history_workbook(self):
        doc=iss.dws_history(fx('dws_ashr_nav_history.xlsx'),'ASHR',ref('d'),NOW)
        rows=doc['rows'];self.assertEqual((len(rows),rows[0]['date'],rows[-1]['date'],doc['as_of']),(3273,'2013-11-05','2026-11-11','2026-11-11'))
        self.assertEqual((rows[-1]['nav_decimal'],rows[-1]['shares_decimal'],rows[-1]['net_assets_decimal']),('30.7','42350001','1300074443.19'))
        self.assertEqual((rows[0]['nav_decimal'],rows[0]['shares_decimal'],rows[0]['net_assets_decimal']),('25','4350001','108750025'))
        with self.assertRaises(ValueError):iss.dws_history(fx('dws_ashr_nav_history.xlsx'),'ASHX',ref('d'),NOW)

    def test_spdr_gold_archive_shares_land_on_baskets(self):
        doc=iss.gld_history(fx('spdrgold_gld_archive.xlsx'),'GLD',ref('g'),NOW)
        rows=doc['rows'];last=rows[-1]
        self.assertEqual((len(rows),rows[0]['date'],last['date']),(5529,'2004-11-18',last['date']))
        self.assertEqual((last['date'],last['nav_decimal'],last['shares_decimal'],last['net_assets_decimal']),('2026-11-11','367.823974','365100000','134292532991.65'))
        grid=[abs(n.dec(v['shares_decimal'])-(n.dec(v['shares_decimal'])/100000).quantize(Decimal(1))*100000) for v in rows[-250:]]
        self.assertLessEqual(max(grid),4);self.assertEqual((last['share_display_quantum_decimal'],last['aum_within_display_precision']),('10',True))   # 100,000-share baskets within the published precision
        flows=n.flow_history(rows,[v['date'] for v in rows],[])
        self.assertEqual((flows[-1]['net_share_change_decimal'],flows[-1]['flow_status']),('-400000','descriptive_estimate'))
        self.assertEqual(n.dec(flows[-1]['nav_valued_share_change_decimal']),Decimal('-147129589.6'))

class Ledger(unittest.TestCase):
    def snapshot(self,day,nav='32.20',shares='92970000'):
        return iss._row(day,n.dec(nav),n.dec(shares),None,nav,shares,'$2.99 billion',[{'url':'u','acquired_at':NOW,'key':'k','sha256':'0'*64}])

    def test_accrue_appends_and_never_overwrites(self):
        first=iss.accrue(None,self.snapshot('2026-11-10'),'BOTZ','Global X',NOW)
        self.assertEqual((first['contract'],[v['date'] for v in first['rows']],first['restatements']),(iss.LEDGER_CONTRACT,['2026-11-10'],[]))
        second=iss.accrue(first,self.snapshot('2026-11-11',shares='93070000'),'BOTZ','Global X',NOW)
        self.assertEqual([v['date'] for v in second['rows']],['2026-11-10','2026-11-11']);self.assertEqual([v['source_row'] for v in second['rows']],[1,2])
        same=iss.accrue(second,self.snapshot('2026-11-11',shares='93070000'),'BOTZ','Global X',NOW)
        self.assertEqual(m.encoded(same),m.encoded(second))
        restated=iss.accrue(second,self.snapshot('2026-11-11',shares='93170000'),'BOTZ','Global X','2026-11-13T15:45:00+00:00')
        self.assertEqual(restated['rows'][-1]['shares_decimal'],'93070000');self.assertEqual(restated['restatements'][0]['observed']['shares_decimal'],'93170000')
        unchanged=iss.accrue(second,None,'BOTZ','Global X',NOW);self.assertEqual(len(unchanged['rows']),2)
        with self.assertRaises(ValueError):iss.accrue({**second,'ticker':'LIT'},None,'BOTZ','Global X',NOW)
        with self.assertRaises(ValueError):iss.accrue({**second,'rows':list(reversed(second['rows']))},None,'BOTZ','Global X',NOW)

def registry_fixture():
    """The research fixture plus Global X BOTZ: five retained ledger days and today's page dated like IVV's latest."""
    raw,inputs,rows=fixture();dates=[v['date'] for v in rows]
    page=fx('globalx_botz_cards.html').replace(b'As of Nov 11, 2026',b'As of Sep 18, 2026')
    url=iss.GLOBALX_URL.format(slug='botz');source=n.public_source_url(url);sha=hashlib.sha256(page).hexdigest()
    key='data/evidence/etf_original/'+hashlib.sha256(source.encode()).hexdigest()+'/'+sha+'.bin.gz';raw[key]=page
    inputs['originals']['globalx_BOTZ']={'url':url,'acquired_at':AT,'evidence':{'key':key,'sha256':sha,'bytes':len(page),'provider':'etf_original','source_url':source,'contract':'source-evidence.v1','captured':True,'first_received_at':AT}}
    ledger=None
    for i,day in enumerate(dates[-6:-1]):
        snap=iss._row(day,n.dec('32.00'),n.dec(str(92000000+i*100000)),None,'32.00',str(92000000+i*100000),'$2.9 billion',[{'url':url,'acquired_at':day+'T15:46:00+00:00','key':'k','sha256':'0'*64}])
        ledger=iss.accrue(ledger,snap,'BOTZ','Global X',day+'T15:46:00+00:00')
    lref,lraw=m.ref_doc(ledger,'ledgers');raw[lref['key']]=lraw;inputs['ledgers']={'BOTZ':{k:lref[k] for k in ('key','sha256','bytes','rows')}}
    return raw,inputs,rows,lref

class Build(unittest.TestCase):
    def test_ledger_accrues_todays_snapshot_and_yields_the_five_day_estimate(self):
        raw,inputs,rows,previous=registry_fixture();output,histories=m.build(inputs,raw.__getitem__,AT)
        entry=output['by_etf']['BOTZ'];ledger=output['snapshot_ledgers']['BOTZ']
        self.assertEqual((ledger['rows'],ledger['latest_date'],ledger['observed_today'],ledger['status'],ledger['previous']['sha256']),(6,'2026-09-18','2026-09-18','accruing',previous['sha256']))
        self.assertIn(ledger['key'],histories);self.assertEqual(json.loads(histories[ledger['key']])['contract'],iss.LEDGER_CONTRACT)
        self.assertEqual((entry['source_status'],entry['nav_source'],entry['identity']['issuer']),('retained_native_snapshot_ledger','Global X_NATIVE_SNAPSHOT_LEDGER','Global X'))
        # five daily changes: +100000 x 32.00 on four days, then (92970000-92400000) x 32.20 today
        expected=Decimal('100000')*Decimal('32.00')*4+Decimal('570000')*Decimal('32.20')
        self.assertEqual(n.dec(entry['flow_windows']['5d']['value_decimal']),expected);self.assertEqual(entry['net_flow_5d_usd'],float(expected))
        self.assertEqual(entry['flow_windows']['5d']['end_date'],output['aggregation_period']['end_date'])
        self.assertEqual(output['coverage']['snapshot_ledgers'],1);self.assertEqual(output['version'],'3.1.0')
        self.assertEqual(output['by_etf']['VOO']['reviewed_limit'],'issuer_publishes_month_end_shares_only');self.assertEqual(output['reviewed_limits']['ARKK'],'issuer_site_bot_challenge')
        self.assertEqual(output['snapshot_ledgers']['LIT']['status'],'awaiting_first_reviewed_snapshot');self.assertNotIn('key',output['snapshot_ledgers']['LIT'])
        self.assertEqual(output['by_etf']['LIT']['source_error']['status'],'snapshot_unavailable_or_unreviewed')

    def test_ledger_fund_never_moves_the_common_alignment_date(self):
        raw,inputs,rows,_=registry_fixture();stale=fx('globalx_botz_cards.html').replace(b'As of Nov 11, 2026',b'As of Sep 17, 2026')
        ref=inputs['originals']['globalx_BOTZ'];sha=hashlib.sha256(stale).hexdigest();key=ref['evidence']['key'].rsplit('/',1)[0]+'/'+sha+'.bin.gz';raw[key]=stale
        ref['evidence'].update(key=key,sha256=sha,bytes=len(stale))
        output,_=m.build(inputs,raw.__getitem__,AT)
        self.assertEqual(output['aggregation_period']['end_date'],rows[-1]['date']);self.assertIsNone(output['by_etf']['BOTZ']['net_flow_5d_usd'])
        self.assertEqual(output['snapshot_ledgers']['BOTZ']['restatements'],1)   # the held Sep 17 row keeps its first-observed values

    def test_tampered_ledger_reference_fails_closed(self):
        raw,inputs,_,previous=registry_fixture();raw[previous['key']]=b'{"contract":"etf-snapshot-ledger.v1","ticker":"BOTZ","rows":[]}'
        with self.assertRaises(ValueError):m.build(inputs,raw.__getitem__,AT)

class Store(unittest.TestCase):
    def test_run_retains_ledger_and_advances_pointer_without_regressing(self):
        raw,inputs,rows,previous=registry_fixture();client=Storage();client.data[m.CURRENT]=m.encoded({'legacy':True})
        for k,v in raw.items():client.data[k]=v if k.endswith('.json') else __import__('gzip').compress(v,mtime=0)
        pointer=s.pointer_key('BOTZ');client.data[pointer]=m.encoded({'contract':s.POINTER_CONTRACT,'ticker':'BOTZ',**inputs['ledgers']['BOTZ']})
        with patch.object(s,'collect',return_value=(inputs['originals'],{})),patch.object(s,'now',return_value=AT):result=s.run(client,'bucket',None)
        self.assertTrue(result['published']);self.assertEqual(result['ledgers_moved'],1)
        moved=json.loads(client.data[pointer]);self.assertEqual((moved['rows'],moved['latest_date']),(6,'2026-09-18'))
        self.assertEqual(client.data[moved['key']][:1],b'{');self.assertEqual(hashlib.sha256(client.data[moved['key']]).hexdigest(),moved['sha256'])
        self.assertEqual(s.read_pointers(s.raw_reader(client,'bucket'))['BOTZ']['sha256'],moved['sha256'])
        self.assertEqual(s.advance_pointers(client,'bucket',{'BOTZ':{**previous,'latest_date':'2026-09-17'}},AT),0)   # a shorter ledger never replaces a longer one
        self.assertEqual(json.loads(client.data[pointer])['sha256'],moved['sha256'])
        manifest=json.loads(s.raw_reader(client,'bucket')(result['replay']['manifest_key']));self.assertEqual(s.replay(manifest,s.raw_reader(client,'bucket'))['version'],'3.1.0')

    def test_collect_requests_amplify_days_with_a_transient_key_kept_out_of_references(self):
        raw,inputs,_=fixture();seen=[];holiday=iss.weekdays_back(datetime.now().date(),iss.AMPLIFY_DAYS)[0]
        def acquire(client,bucket,url,key=None,deadline=None,key_param='apikey',headers=None,key_header=None,key_prefix=''):
            seen.append((url,key,key_param,key_header,(headers or {}).get('Authorization')))
            if 'firestore' in url:
                if url.endswith(holiday):raise urllib.error.HTTPError(url,404,'Not Found',{},None)   # an exchange holiday has no document
                body=m.encoded({'name':'x','fields':{}});sha=hashlib.sha256(body).hexdigest();key_=('data/evidence/etf_original/x/'+sha+'.bin.gz');raw[key_]=body
                return {'url':url,'acquired_at':AT,'evidence':{'key':key_,'sha256':sha}}
            if url in {v['url'] for v in inputs['originals'].values()}:return next(v for v in inputs['originals'].values() if v['url']==url)
            raise urllib.error.HTTPError(url,404,'Not Found',{},None)
        with patch.object(s,'acquire',side_effect=acquire),patch.object(s,'transient_web_key',side_effect=lambda url,pattern,deadline=None:'K'*40 if 'amplify' in url else 'T'*64):
            originals,errors=s.collect(Storage(),'bucket',None,raw.__getitem__,time.monotonic()+60)
        days=[k for k in originals if k.startswith('amplify_HACK_')];self.assertEqual(len(days),39);self.assertTrue(all(re.fullmatch(r'amplify_HACK_\d{4}-\d{2}-\d{2}',k) for k in days))
        self.assertFalse(any('K'*40 in v['url'] for v in originals.values()));self.assertEqual(errors['amplify_HACK_'+holiday],'HTTP_404')
        firestore=[v for v in seen if 'firestore' in v[0]];self.assertEqual(len(firestore),40);self.assertEqual({(v[1],v[2],v[3]) for v in firestore},{('K'*40,'key',None)})
        uscf=[v for v in seen if 'alpsinc' in v[0]];self.assertEqual({(v[1],v[3]) for v in uscf},{('T'*64,'Authorization')})
        invesco=[v for v in seen if 'dng-api' in v[0]];self.assertEqual(len(invesco),6);self.assertEqual(errors['vendor_context'],'configured_vendor_credential_unavailable')

    def test_web_key_echoed_in_a_response_is_never_retained(self):
        class Opener:
            def __init__(self,body):self.body=body
            def open(self,request,timeout=30):
                self.request=request;return self
            def __enter__(self):return self
            def __exit__(self,*a):return False
            def read(self,limit):return self.body
        key='A'*40;opener=Opener(b'{"echo":"'+key.encode()+b'"}')
        with patch.object(s.urllib.request,'build_opener',return_value=opener):
            with self.assertRaises(ValueError):s.acquire(Storage(),'bucket','https://example.test/x',key,None,'key')
            self.assertEqual(opener.request.full_url,'https://example.test/x?key='+key)
            opener.body=b'{"ok":true}';opener.request=None
            with patch.object(s.evidence_store,'capture',return_value={'contract':'source-evidence.v1'}):
                ref=s.acquire(Storage(),'bucket','https://example.test/y',key,None,'key',None,'Authorization','Bearer ')
            self.assertEqual(ref['url'],'https://example.test/y');self.assertEqual(opener.request.full_url,'https://example.test/y')
            self.assertEqual(opener.request.get_header('Authorization'),'Bearer '+key)

if __name__=='__main__':unittest.main()
