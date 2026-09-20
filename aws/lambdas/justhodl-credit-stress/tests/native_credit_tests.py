from pathlib import Path
from datetime import date,timedelta
from decimal import Decimal
from unittest.mock import patch
import copy,io,json,unittest
from credit_fixtures import model,fixture,originals,STAMP,EVALUATION
import credit_research_store as store


class ModelCases(unittest.TestCase):
    def parsed(self,sid='BAMLH0A0HYM2',values=None):
        src=originals(sid,values)
        return model.parse(sid,src['definition'],src['observations'],EVALUATION)

    def test_typed_basis_points_and_percentage_points(self):
        d,rows=self.parsed(values=[('2026-09-16','2.68'),('2026-09-17','2.70')])
        m=model.metric('BAMLH0A0HYM2',d,rows,STAMP)
        self.assertEqual((m['value_pct'],m['value_bps'],m['change_pp'],m['change_bps']),(2.7,270.0,.02,2.0))
        self.assertEqual(m['exact']['change_pp'],'0.02')

    def test_missing_previous_is_not_shifted_to_earlier_number(self):
        d,rows=self.parsed(values=[('2026-09-15','2.0'),('2026-09-16','.'),('2026-09-17','2.7')])
        m=model.metric('BAMLH0A0HYM2',d,rows,STAMP)
        self.assertIsNone(m['change_bps']);self.assertEqual(m['previous_observation_date'],'2026-09-16')

    def test_missing_current_retains_dated_context_not_live_value(self):
        d,rows=self.parsed(values=[('2026-09-16','2.0'),('2026-09-17','.')])
        m=model.metric('BAMLH0A0HYM2',d,rows,STAMP)
        self.assertIsNone(m['value_pct']);self.assertEqual(m['quality']['status'],'unavailable')
        self.assertEqual(m['latest_numeric_context'],{'value_pct':2.0,'observation_date':'2026-09-16'})

    def test_zero_and_negative_measurements_not_missing(self):
        d,rows=self.parsed('T10Y2Y',[('2026-09-16','-0.25'),('2026-09-17','0.00')])
        m=model.metric('T10Y2Y',d,rows,STAMP)
        self.assertEqual((m['value_bps'],m['change_bps'],m['unit']),(0.0,25.0,'percentage_points'))

    def test_weekend_accrual_and_real_gap_are_preserved(self):
        d,rows=self.parsed(values=[('2026-08-28','2.0'),('2026-08-30','2.1'),('2026-09-17','2.2')])
        m=model.metric('BAMLH0A0HYM2',d,rows,STAMP)
        self.assertEqual(m['history_coverage']['weekend_numeric_rows'],1)
        self.assertEqual(m['comparison_gap_days'],18)

    def test_constant_reference_window_has_no_z_score(self):
        values=[(str(date(2026,7,1)+timedelta(days=i)),'2.0') for i in range(60)]
        _,rows=self.parsed(values=values)
        s=model.statistics(rows,Decimal('2'),60)
        self.assertEqual(s['mean_pct'],2.0);self.assertEqual(s['sample_stddev_pp'],0.0)
        self.assertIsNone(s['z_score']);self.assertEqual(s['status'],'constant_window')

    def test_statistics_count_missing_in_actual_span(self):
        values=[(str(date(2026,7,1)+timedelta(days=i)),'.' if i==30 else str(i)) for i in range(61)]
        _,rows=self.parsed(values=values)
        s=model.statistics(rows,rows[-1]['value'],60)
        self.assertEqual(s['numeric_observations'],60);self.assertEqual(s['missing_rows_inside_span'],1)
        self.assertEqual(s['first_date'],'2026-07-01');self.assertEqual(s['last_date'],'2026-08-30')

    def test_midrank_percentile_and_insufficient_span(self):
        rows=[{'date':str(date(2025,9,1)+timedelta(days=i)),'value':Decimal('2')} for i in range(370)]
        self.assertEqual(model.percentile(rows,Decimal('2'),'2025-09-01')['percentile_pct'],50.0)
        self.assertIsNone(model.percentile(rows,Decimal('2'),'2020-01-01')['percentile_pct'])

    def test_latest_date_mismatch_is_lagged_not_current_spread(self):
        a,ar=self.parsed('BAMLH0A0HYM2',[('2026-09-16','2.7'),('2026-09-17','2.8')])
        b,br=self.parsed('BAMLC0A0CM',[('2026-09-16','0.8')])
        ms={'BAMLH0A0HYM2':model.metric('BAMLH0A0HYM2',a,ar,STAMP),'BAMLC0A0CM':model.metric('BAMLC0A0CM',b,br,STAMP)}
        result=model.difference('hy_minus_ig',ms,{'BAMLH0A0HYM2':ar,'BAMLC0A0CM':br})
        self.assertEqual(result['value_bps'],190);self.assertFalse(result['current_comparison_available'])
        self.assertEqual(result['observation_date'],'2026-09-16');self.assertEqual(result['status'],'lagged_context')

    def test_pair_missing_latest_common_row_not_skipped(self):
        _,ar=self.parsed('BAMLH0A0HYM2',[('2026-09-16','2.7'),('2026-09-17','.')])
        _,br=self.parsed('BAMLC0A0CM',[('2026-09-16','0.8'),('2026-09-17','0.9')])
        ms={sid:{'observation_date':'2026-09-17','quality':{'status':'within_age_ceiling'}} for sid in ('BAMLH0A0HYM2','BAMLC0A0CM')}
        result=model.difference('hy_minus_ig',ms,{'BAMLH0A0HYM2':ar,'BAMLC0A0CM':br})
        self.assertIsNone(result['value_bps']);self.assertFalse(result['current_comparison_available'])

    def test_fresh_collection_does_not_refresh_stale_observation(self):
        d,rows=self.parsed(values=[('2026-09-01','2.7')]);m=model.metric('BAMLH0A0HYM2',d,rows,STAMP)
        self.assertEqual(m['quality']['status'],'stale');self.assertEqual(m['source_valid_until'],'2026-09-07T00:00:00+00:00')

    def test_definition_unit_identity_and_vintage_fail_closed(self):
        sid='BAMLH0A0HYM2';raw=originals(sid)
        for field,bad in [('id','OTHER'),('units','Basis Points'),('frequency_short','M'),('realtime_end','2026-09-19'),('title','Something unrelated')]:
            with self.subTest(field=field):
                d=json.loads(raw['definition']);d['seriess'][0][field]=bad
                with self.assertRaises(ValueError):model.parse(sid,model.encoded(d),raw['observations'],EVALUATION)

    def test_observation_incomplete_duplicate_future_and_mixed_vintage(self):
        sid='BAMLH0A0HYM2';raw=originals(sid,[('2026-09-16','2'),('2026-09-17','3')])
        mutations=[lambda o:o.update(count=3),lambda o:o['observations'][1].update(date='2026-09-16'),
            lambda o:o['observations'][1].update(date='2026-09-21'),lambda o:o['observations'][1].update(realtime_start='2026-09-19'),
            lambda o:o.update(units='pch'),lambda o:o.update(offset=True),lambda o:o.update(limit=1)]
        for mutate in mutations:
            obs=json.loads(raw['observations']);mutate(obs)
            with self.assertRaises(ValueError):model.parse(sid,raw['definition'],model.encoded(obs),EVALUATION)

    def test_invalid_numbers_and_duplicate_json_rejected(self):
        for value in (None,False,2,2.0,'NaN','Infinity','1e3','1,234',''):
            with self.subTest(value=value),self.assertRaises(ValueError):model.number(value)
        with self.assertRaises(ValueError):model.json_object(b'{"x":1,"x":2}')
        with self.assertRaises(ValueError):model.json_object(b'{"x":NaN}')

    def test_complete_fixture_has_no_forecast_or_legacy_false_statistic(self):
        inputs,bodies=fixture();out=store.compile_output(inputs,bodies.__getitem__)
        self.assertEqual(out['quality']['within_age_ceiling'],28);self.assertEqual(len(out['source_evidence']),56)
        self.assertEqual(out['current_bps']['BAMLH0A0HYM2'],out['current_pct']['BAMLH0A0HYM2']*100)
        self.assertEqual(out['metrics']['BAMLEMCBPIOAS']['meta']['rating'],'IG and below IG')
        self.assertIsNone(out['metrics']['BAMLH0A0HYM2']['z_score_60d']);self.assertIsNone(out['metrics']['BAMLH0A0HYM2']['pct_all_time'])
        self.assertIsNone(out['call']);self.assertEqual(out['portfolio_action'],'WAIT')
        for key in model.PERMISSIONS:self.assertIs(out[key],False)
        self.assertFalse(out['history_reference']['used_in_native_calculations'])
        self.assertEqual(model.encoded(out),model.encoded(store.compile_output(copy.deepcopy(inputs),bodies.__getitem__)))

    def test_failed_source_does_not_remove_catalog_or_become_zero(self):
        inputs,bodies=fixture();inputs['sources']['BAMLH0A0HYM2']['observations']={'error':'provider_http_503'}
        out=store.compile_output(inputs,bodies.__getitem__)
        self.assertEqual(len(out['measurements']),28);self.assertEqual(out['quality']['status'],'partial')
        self.assertIsNone(out['current_bps']['BAMLH0A0HYM2']);self.assertIsNone(out['derived_spreads']['hy_minus_ig'])

    def test_original_hash_request_and_clock_tampering_rejected(self):
        inputs,bodies=fixture();sid=model.SERIES[0]
        for change in ('hash','request','clock'):
            x=copy.deepcopy(inputs);item=x['sources'][sid]['observations']
            if change=='hash':item['evidence']['sha256']='0'*64
            elif change=='request':item['evidence']['request_url']+='&api_key=should-not-appear'
            else:item['acquired_at']='2026-09-21T00:00:00Z'
            with self.assertRaises(ValueError):store.compile_output(x,bodies.__getitem__)


class Conflict(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):self.objects={};self.reads=[];self.writes=[]
    def get_object(self,Bucket,Key):
        self.reads.append(Key)
        if Key not in self.objects:raise Conflict('NoSuchKey')
        raw=self.objects[Key];return {'Body':io.BytesIO(raw),'ETag':model.sha(raw)}
    def put_object(self,Bucket,Key,Body,**kw):
        current=self.objects.get(Key)
        if kw.get('IfNoneMatch')=='*' and current is not None:raise Conflict('PreconditionFailed')
        if kw.get('IfMatch') is not None and (current is None or model.sha(current)!=kw['IfMatch']):raise Conflict('PreconditionFailed')
        self.objects[Key]=Body;self.writes.append((Key,kw));return {}


class StorageCases(unittest.TestCase):
    def test_original_replay_and_output_integrity(self):
        inputs,bodies=fixture();s=Memory();s.objects.update(bodies)
        out=store.compile_output(inputs,store.reader(s,'test'));ref=store.retain(s,'test',inputs,out)
        self.assertEqual(store.replay(ref,store.reader(s,'test')),out)
        key=next(iter(bodies));s.objects[key]=b'{}'
        with self.assertRaises(ValueError):store.replay(ref,store.reader(s,'test'))

    def test_mutated_compiler_cannot_replay(self):
        inputs,bodies=fixture();s=Memory();s.objects.update(bodies)
        out=store.compile_output(inputs,store.reader(s,'test'));ref=store.retain(s,'test',inputs,out)
        manifest=json.loads(s.objects[ref['manifest_key']]);key=next(iter(manifest['compilers'].values()))['key'];s.objects[key]=b'changed'
        with self.assertRaises(ValueError):store.replay(ref,store.reader(s,'test'))

    def test_whole_predecessor_is_preserved_before_cas(self):
        s=Memory();old=model.encoded({'generated_at':'2026-09-18T00:00:00Z','data_date':'2026-09-17','unrelated':{'preserve':'whole'}})
        s.objects[store.CURRENT]=old
        packet={'generated_at':STAMP,'as_of':'2026-09-17','sample':None}
        self.assertTrue(store.publish(s,'test',packet));self.assertEqual(s.objects[store.PRIVATE+model.sha(old)+'.bin'],old)
        self.assertEqual(json.loads(s.objects[store.CURRENT]),packet)

    def test_newer_or_later_observation_cannot_be_replaced(self):
        for old in ({'generated_at':'2026-09-21T00:00:00Z','as_of':'2026-09-17'},
                    {'generated_at':'2026-09-19T00:00:00Z','as_of':'2026-09-18'}):
            s=Memory();s.objects[store.CURRENT]=model.encoded(old)
            self.assertFalse(store.publish(s,'test',{'generated_at':STAMP,'as_of':'2026-09-17'}));self.assertEqual(s.writes,[])

    def test_same_clock_conflict_is_rejected(self):
        s=Memory();s.objects[store.CURRENT]=model.encoded({'generated_at':STAMP,'as_of':'2026-09-17','value':1})
        with self.assertRaises(ValueError):store.publish(s,'test',{'generated_at':STAMP,'as_of':'2026-09-17','value':2})

    def test_idempotent_retry_never_reacquires(self):
        s=Memory();key=store.request_key('test-request');prior={'status':'running'};s.objects[key]=model.encoded(prior)
        with patch.object(store,'collect',side_effect=AssertionError('must not reacquire')):
            self.assertEqual(store.run(s,'test','test-request','execution','test-key'),prior)

    def test_private_paths_and_content_addresses_are_enforced(self):
        s=Memory()
        for key in ('brain.json','data/accounts/x','../secret',store.PRIVATE+'x.bin'):
            with self.assertRaises(ValueError):store.reader(s,'test')(key)
        with self.assertRaises(ValueError):store.immutable(s,'test',store.PRIVATE+'0'*64+'.bin',b'{}')

    def test_source_credential_never_retained_or_redirected(self):
        class Response(io.BytesIO):status=200
        class Open:
            def open(self,request,timeout):return Response(b'{"reflected":"test-credential"}')
        s=Memory();c=store.Collector(s,'test','test-credential',EVALUATION,store.time.monotonic()+15,Open())
        self.assertEqual(c.acquire('BAMLH0A0HYM2','observations'),{'error':'provider_body_rejected'});self.assertEqual(s.writes,[])
        with self.assertRaises(ValueError):store.NoRedirect().redirect_request(None,None,302,'',{},'https://other.invalid')

    def test_live_run_uses_only_source_and_known_dealer_paths(self):
        inputs,bodies=fixture();s=Memory();s.objects.update(bodies)
        clocks=iter([inputs['started_at'],inputs['generated_at'],inputs['generated_at']])
        with patch.object(store,'collect',return_value=inputs['sources']),patch.object(store,'now',side_effect=lambda:next(clocks)):
            result=store.run(s,'test','fresh-request','execution','synthetic-credential')
        self.assertEqual(result['status'],'complete');self.assertTrue(result['published'])
        self.assertTrue(all(k.startswith((store.PREFIX,store.PRIVATE)) or k in (store.CURRENT,'data/nyfed-primary-dealer.json') for k in s.reads))
        self.assertEqual(result['notifications_sent'],0);self.assertEqual(result['private_account_reads'],0)


if __name__=='__main__':unittest.main()
