from copy import deepcopy
from datetime import date,datetime,timedelta
import hashlib,importlib.util,io,json,sys,types,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests'),str(ROOT/'scripts'),str(Path(__file__).resolve().parents[1]/'source')]
import reversal_research as m
import reversal_store as s
import report_observations as native
import fr2004_research_context as fails
from reversal_consumer_context import context,native_yield
from reversal_research_catalog import SERIES,extend_catalog
from evidence_store import capture
from test_report_observations import inputs,NOW


class StorageError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Storage:
    def __init__(self):self.objects={};self.reads=[]
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key not in self.objects:raise StorageError('NoSuchKey')
        raw=self.objects[key]
        return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        key=kw['Key']
        if kw.get('IfNoneMatch')=='*' and key in self.objects:raise StorageError('PreconditionFailed')
        if kw.get('IfMatch') and (key not in self.objects or kw['IfMatch']!=hashlib.sha256(self.objects[key]).hexdigest()):raise StorageError('PreconditionFailed')
        self.objects[key]=kw['Body']


def weekly(n=100):
    return [((date(2026,9,16)-timedelta(days=7*i)).isoformat(),str((i%3)+i*2)) for i in range(n)]


def prepared(sid='DGS10',frequency='D',rows=None,unit='Percent'):
    client=Storage();item=inputs(sid,frequency,rows or [('2026-09-17','0'),('2026-09-16','0.1')],unit)
    for part in ('definition','observations'):
        item['evidence'][part]=capture(client,'fixture','fred',item['evidence'][part]['source_url'],m.encoded(item[part]),received_at=datetime.fromisoformat(NOW))
    source=native.build({sid:{}},{sid:item},NOW)
    body=Path(native.__file__).read_bytes();sha=hashlib.sha256(body).hexdigest();key='data/report-research/compilers/'+sha+'.py';client.objects[key]=body
    manifest={'contract':'report-research-replay.v1','generated_at':NOW,'compiler':{'key':key,'sha256':sha},
        'inputs':{sid:{'evidence':item['evidence'],'acquired_at':item['acquired_at']}},'output_sha256':m.digest(source)}
    key='data/report-research/runs/'+m.digest(manifest)+'.json';client.objects[key]=m.encoded(manifest)
    source['replay']={'manifest_key':key,'compiler_sha256':sha,'output_sha256':manifest['output_sha256']}
    client.objects['data/report-measurements.json']=m.encoded(source)
    client.objects[m.CURRENT]=m.encoded({'generated_at':NOW,'list_name':'PRIVATE OWNER LIST','rows':[
        {'symbol':'FRED:'+sid,'last':999,'source_lists':['PRIVATE OWNER LIST'],'name':'Owner-specific private label'},
        {'symbol':'TVC:USOIL','last':0,'move_z':3,'trend_state':'UP'}]})
    return client,source,item


def rows(pairs):return [{'date':d,'value':v,'row_index':i} for i,(d,v) in enumerate(pairs)]


class Research(unittest.TestCase):
    def test_inventory_keeps_every_zero_without_owner_prose(self):
        c,_,_=prepared();original=c.objects[m.CURRENT];p=m.project_inventory(json.loads(original));m.validate_inventory(p)
        self.assertEqual(len(p['rows']),2);self.assertEqual(p['rows'][1]['legacy']['last'],0)
        self.assertNotIn('PRIVATE',m.encoded(p).decode());self.assertNotIn('Owner-specific',m.encoded(p).decode())
        p['rows'][0]['private']='extra'
        with self.assertRaises(ValueError):m.validate_inventory(p)

    def test_duplicate_inventory_is_rejected_not_silently_lost(self):
        with self.assertRaises(ValueError):m.project_inventory({'rows':[{'symbol':'X'},{'symbol':'X'}]})

    def test_native_zero_and_unit_dates_are_retained(self):
        c,source,item=prepared();native_row=m.compile_native('DGS10',item,source,NOW)
        self.assertEqual(native_row['measurement']['current'],0);self.assertTrue(native_row['measurement_eligible'])
        self.assertEqual(native_row['technical']['change_direction'],'DOWN')
        self.assertEqual(native_row['technical']['latest_change']['value_decimal'],'-0.1')
        self.assertEqual(native_row['technical']['change_unit'],'percentage_points')
        self.assertFalse(native_row['calls_eligible'])

    def test_current_move_excluded_from_prior_z_reference(self):
        values=weekly(100);base=m.technical(rows(values),'W','Index')
        values[0]=(values[0][0],'10000');shock=m.technical(rows(values),'W','Index')
        self.assertEqual(base['z_n_prior'],90);self.assertEqual(base['z_mean_decimal'],shock['z_mean_decimal'])
        self.assertEqual(base['z_sd_decimal'],shock['z_sd_decimal']);self.assertGreater(shock['z_absolute'],100)
        self.assertEqual(shock['latest_change']['baseline_date'],values[1][0])

    def test_signed_shock_is_not_absolute_move_direction(self):
        pairs=weekly(40);pairs=[(d,str(200+i*2+(i%2))) for i,(d,_) in enumerate(pairs)]
        pairs[0]=(pairs[0][0],str(float(pairs[1][1])-0.1))
        result=m.technical(rows(pairs),'W','Index')
        self.assertEqual(result['change_direction'],'DOWN');self.assertGreater(result['z_signed'],0)

    def test_missing_latest_and_weekly_gap_do_not_become_current_changes(self):
        pair=weekly(100);pair[0]=(pair[0][0],None)
        result=m.technical(rows(pair),'W','Index');self.assertIsNone(result['latest_change']);self.assertIsNone(result['trend'])
        result=m.technical(rows([weekly(100)[0]]+weekly(100)[2:]),'W','Index')
        self.assertIsNone(result['latest_change']);self.assertIsNone(result['trend'])

    def test_range_is_calendar_year_and_constant_range_is_not_fifty(self):
        pair=weekly(260);result=m.technical(rows(pair),'W','Index')
        self.assertLess(result['range_1y']['numeric_rows'],55)
        self.assertEqual(result['range_1y']['start_cutoff'],'2025-09-16')
        result=m.technical(rows([(d,'0') for d,_ in pair]),'W','Index')
        self.assertIsNone(result['range_1y']['position_pct']);self.assertEqual(result['range_reason'],'constant_range')
        self.assertIsNone(result['z_signed']);self.assertEqual(result['z_reason'],'constant_prior_changes')

    def test_elapsed_day_slopes_work_at_zero_mean_and_quarterly(self):
        r=rows([('2026-01-01','-1'),('2026-01-02','0'),('2026-01-03','1')])
        self.assertEqual(m.slope(r),1)
        pairs=[(native.months_before(date(2026,4,1),3*i).isoformat(),str(i*i-8*i)) for i in range(20)]
        result=m.technical(rows(pairs),'Q','Index')
        self.assertIsNotNone(result['trend']);self.assertEqual(result['trend']['short_observations'],3)
        self.assertEqual(result['trend']['slope_unit'],'Index per calendar day')

    def test_opposite_cross_cannot_confirm_slope_change(self):
        import random
        rng=random.Random(9);found=False
        for _ in range(3000):
            values=[rng.randrange(-20,21) for _ in range(20)]
            pairs=[(native.months_before(date(2026,8,1),i).isoformat(),str(v)) for i,v in enumerate(values)]
            trend=m.technical(rows(pairs),'M','Index')['trend']
            if trend and trend['slope_direction_change'] and trend['most_recent_ma_cross'] and trend['slope_direction_change']!=trend['most_recent_ma_cross']['direction']:
                self.assertFalse(trend['matching_ma_cross']);found=True;break
        self.assertTrue(found)

    def test_old_acquisition_and_discontinued_identity_cannot_vote(self):
        _,source,item=prepared();out=m.compile_native('DGS10',item,source,'2026-09-20T20:00:00+00:00')
        self.assertFalse(out['measurement_eligible']);self.assertEqual(out['measurement']['quality']['status'],'stale_source')
        _,source,item=prepared('TREASURY','M',[('2025-10-01','50')],'Billions of U.S. Dollars')
        out=m.compile_native('TREASURY',item,source,NOW)
        self.assertFalse(out['measurement_eligible']);self.assertEqual(out['measurement']['quality']['status'],'historical_discontinued')

    def test_unfinished_quarter_stays_unqualified(self):
        _,source,item=prepared('MMMFFAQ027S','Q',[('2026-07-01','1')],'Millions of U.S. Dollars')
        out=m.compile_native('MMMFFAQ027S',item,source,NOW)
        self.assertFalse(out['measurement_eligible']);self.assertEqual(out['measurement']['period_end'],'2026-09-30')

    def test_original_replay_and_protected_complete_legacy_preservation(self):
        c,source,item=prepared();legacy=c.objects[m.CURRENT]
        with patch.object(s,'now',return_value=NOW):result=s.run(c,'fixture')
        read=s.raw_reader(c,'fixture');packet=json.loads(read(m.CURRENT));manifest=json.loads(read(result['replay']['manifest_key']))
        self.assertEqual(s.replay(manifest,read),{k:v for k,v in packet.items() if k!='replay'})
        self.assertEqual(packet['inventory']['entries'],2);self.assertEqual(packet['quality']['original_verified_series'],1)
        self.assertEqual(c.objects[s.PRIVATE+hashlib.sha256(legacy).hexdigest()+'.bin'],legacy)
        self.assertFalse(any('PRIVATE OWNER' in raw.decode(errors='ignore') for key,raw in c.objects.items() if key.startswith(m.PREFIX)))
        self.assertTrue(all(not r['calls_eligible'] for r in packet['rows']))
        self.assertIsNone(packet['liquidity']['reversal_score']);self.assertIsNone(packet['portfolio_consequences']['allocation'])
        self.assertFalse(any('tv-watchlists' in key or 'tradingview.json' in key for key in c.reads))

    def test_corrupted_original_prevents_publication_and_replay(self):
        c,source,item=prepared();old=c.objects[m.CURRENT]
        c.objects[item['evidence']['observations']['key']]=b'corrupted'
        with patch.object(s,'now',return_value=NOW),self.assertRaises(Exception):s.run(c,'fixture')
        self.assertEqual(c.objects[m.CURRENT],old)

    def test_output_compiler_and_pointer_tampering_are_rejected(self):
        from replay_reversal_research import verify_current
        c,_,_=prepared()
        with patch.object(s,'now',return_value=NOW):result=s.run(c,'fixture')
        read=s.raw_reader(c,'fixture');manifest=json.loads(read(result['replay']['manifest_key']))
        for key in (manifest['output']['key'],manifest['input']['key'],manifest['compilers']['reversal_research']['key']):
            old=c.objects[key];c.objects[key]=b'{}'
            with self.assertRaises(ValueError):s.replay(manifest,read)
            c.objects[key]=old
        p=json.loads(read(m.CURRENT));p['rows'][0]['last']=55;c.objects[m.CURRENT]=m.encoded(p)
        with self.assertRaises(ValueError):verify_current(read)

    def test_same_clock_conflicts_and_older_runs_do_not_replace_current(self):
        c,_,_=prepared()
        with patch.object(s,'now',return_value=NOW):s.run(c,'fixture')
        packet=json.loads(c.objects[m.CURRENT]);changed=deepcopy(packet);changed['version']='different'
        with self.assertRaises(ValueError):s.publish(c,'fixture',changed)
        changed['generated_at']='2026-09-18T19:00:00+00:00';self.assertFalse(s.publish(c,'fixture',changed))
        self.assertEqual(json.loads(c.objects[m.CURRENT]),packet)

    def test_context_never_promotes_unqualified_labels_or_wrapper_freshness(self):
        for p in ({'liquidity':{'reversal_label':'CONFIRMED TURN TO TIGHTEN'}},
                  {'contract':m.CONTRACT,'generated_at':NOW,'calls_eligible':True}):
            result=context(p,datetime.fromisoformat(NOW));self.assertEqual(result['state'],'ABSTAIN');self.assertFalse(result['calls_eligible'])

    def test_independent_native_yield_keeps_zero_and_rejects_corruption(self):
        c,source,item=prepared();result=native_yield(c,'fixture',datetime.fromisoformat(NOW))
        self.assertEqual(result['value'],0);self.assertEqual(result['unit'],'Percent')
        self.assertEqual(result['date'],'2026-09-17');self.assertIn('evidence',result)
        c.objects[item['evidence']['observations']['key']]=b'bad'
        self.assertIsNone(native_yield(c,'fixture',datetime.fromisoformat(NOW))['value'])
        self.assertFalse(any('blackswan-watch' in key or key==m.CURRENT for key in c.reads))

    def test_pd_fails_units_and_distinct_gross_reconciliation(self):
        d={'treasury':{'scope_id':'treasury_incl_tips','unit':'USD_bn_par','field_units':{k:'usd_bn' for k in ('ftd_bn','ftr_bn','gross_bn')},
            'as_of':'2026-09-09','ftd_bn':128.4,'ftr_bn':134.17,'gross_bn':262.57,'quality':{'status':'fresh'}},
            'headline':{'scope':'ust_ex_tips','unit':'usd_bn','as_of':'2026-09-09','ftd_bn':113.83,'ftr_bn':119.22,'combined_bn':233.1,'quality':{'status':'fresh'}}}
        p=fails.project(d,datetime.fromisoformat(NOW));self.assertEqual(p['display_values'],[128.4,134.17,262.57])
        self.assertIsNone(p['ust_ex_tips']['display_values']);self.assertIn('gross_reconciliation_failed',p['ust_ex_tips']['reasons'])
        d['treasury']['field_units']={};self.assertIsNone(fails.project(d,datetime.fromisoformat(NOW))['display_values'])

    def test_active_handler_cannot_select_legacy_private_routes(self):
        fake=types.SimpleNamespace(client=lambda *a,**kw:None)
        with patch.dict(sys.modules,{'boto3':fake,'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**kw:(_ for _ in ()).throw(AssertionError('secret access')))}):
            spec=importlib.util.spec_from_file_location('active_reversal',Path(__file__).resolve().parents[1]/'source/lambda_function.py')
            engine=importlib.util.module_from_spec(spec);spec.loader.exec_module(engine)
        with patch.object(engine,'_legacy_unvalidated_handler',side_effect=AssertionError('private legacy')),patch.object(s,'run',return_value={'ok':True}) as run:
            self.assertEqual(engine.lambda_handler({'legacy':True,'force':True},None)['statusCode'],200);run.assert_called_once()
        with patch.object(s,'run',side_effect=ValueError('private detail')):
            result=engine.lambda_handler({},None);self.assertEqual(result['statusCode'],503);self.assertNotIn('private detail',result['body'])

    def test_catalog_retains_exact_identities_without_alias_substitution(self):
        self.assertEqual(len(SERIES),len(set(SERIES)))
        for sid in ('TREASURY','WTREGEN','FEDFUNDS','DFF','SOFR','MMMFTAQ027S','MMMFFAQ027S'):self.assertIn(sid,SERIES)
        self.assertEqual(extend_catalog({'DFF':{'existing':True}})['DFF'],{'existing':True})


if __name__=='__main__':unittest.main()
