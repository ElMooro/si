import copy
from datetime import datetime, timezone, timedelta
import hashlib
import io
import json
from pathlib import Path
import sys
import unittest
from urllib.parse import urlencode
ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT/'aws/lambdas/justhodl-risk-regime/source'), str(ROOT/'aws/shared')]
import regime_model as m
import regime_store as s

FIXTURES = Path(__file__).parent/'fixtures'


def fixture():
    inputs = {'contract': 'risk-regime-inputs.v1', 'evaluation_date': '2026-09-20', 'generated_at': '2026-09-20T07:00:00+00:00',
        'sources': {}, 'option_pages': {}, 'context': {}, 'settlement': None}
    bodies = {}
    def add(name, filename, url):
        raw = (FIXTURES/(filename+'.json')).read_bytes(); sha = hashlib.sha256(raw).hexdigest(); request_sha = hashlib.sha256(url.encode()).hexdigest()
        inputs['sources'][name] = {'status': 'captured', 'provider': 'fred' if 'fred-' in filename else 'massive',
            'request_url': url, 'request_sha256': request_sha, 'key': m.PREFIX+'sources/'+request_sha+'/'+sha+'.json',
            'sha256': sha, 'bytes': len(raw), 'acquired_at': '2026-09-20T06:44:28+00:00'}
        bodies[name] = raw
    for sid in m.SERIES:
        q = dict(series_id=sid, file_type='json', realtime_start='2026-09-20', realtime_end='2026-09-20')
        add('definition:'+sid, 'fred-definition-'+sid, 'https://api.stlouisfed.org/fred/series?'+urlencode(q))
        q.update(observation_start='2024-07-12', observation_end='2026-09-20', units='lin', sort_order='asc', limit=10000, offset=0, output_type=1)
        add('observations:'+sid, 'fred-observations-'+sid, 'https://api.stlouisfed.org/fred/series/observations?'+urlencode(q))
    for symbol, lo, hi in [('SPY','670.29','853.09'), ('HYG','69.11','87.95')]:
        add('previous:'+symbol, 'previous-'+symbol, 'https://api.massive.com/v2/aggs/ticker/'+symbol+'/prev?adjusted=true')
        q = {'strike_price.gte':lo, 'strike_price.lte':hi, 'expiration_date.gte':'2026-10-11', 'expiration_date.lte':'2026-11-04', 'sort':'ticker','order':'asc','limit':250}
        name='options:'+symbol+':1';add(name,'options-'+symbol,'https://api.massive.com/v3/snapshot/options/'+symbol+'?'+urlencode(q))
        inputs['option_pages'][symbol] = [name]
    add('fx:AUDJPY','fx-AUDJPY','https://api.massive.com/v2/aggs/ticker/C:AUDJPY/range/1/day/2026-07-17/2026-09-19?adjusted=true&sort=asc&limit=50000')
    return inputs, bodies


def mutate(inputs, bodies, name, edit):
    doc = json.loads(bodies[name]);edit(doc);raw = m.encoded(doc);bodies[name] = raw;ref=inputs['sources'][name]
    ref['sha256']=hashlib.sha256(raw).hexdigest();ref['bytes']=len(raw)
    ref['key']=m.PREFIX+'sources/'+ref['request_sha256']+'/'+ref['sha256']+'.json'


class FakeError(Exception):
    def __init__(self, code):self.response={'Error':{'Code':code}}


class Storage:
    def __init__(self):self.objects={};self.writes=[]
    def put_object(self,**kw):
        key=kw['Key'];old=self.objects.get(key)
        if kw.get('IfNoneMatch')=='*' and old is not None:raise FakeError('PreconditionFailed')
        if kw.get('IfMatch') and (old is None or hashlib.sha256(old).hexdigest()!=kw['IfMatch']):raise FakeError('PreconditionFailed')
        self.objects[key]=kw['Body'];self.writes.append(kw)
    def get_object(self,**kw):
        if kw['Key'] not in self.objects:raise FakeError('NoSuchKey')
        raw=self.objects[kw['Key']];return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest()}


class Native(unittest.TestCase):
    def setUp(self):self.inputs,self.bodies=fixture()
    def build(self):return m.build(self.inputs,self.bodies)
    def test_provider_fixtures_and_true_percentile(self):
        out=self.build();self.assertEqual(out['source_failures'],{})
        hy=out['measurements']['BAMLH0A0HYM2'];self.assertEqual(hy['value'],2.7)
        p=hy['percentile_2y'];self.assertGreater(p['n_finite'],400)
        values=[r['value'] for r in hy['history'] if r['date']>=p['window_start'] and r['value'] is not None]
        expected=100*(sum(v<2.7 for v in values)+.5*sum(v==2.7 for v in values))/len(values)
        self.assertAlmostEqual(p['value'],expected)
        self.assertNotAlmostEqual(p['value'],100*(2.7-min(values))/(max(values)-min(values)))
        self.assertEqual(out['risk_regime_score'],None);self.assertEqual(out['decision']['verb'],'WAIT')
        self.assertFalse(out['sizing_eligible']);self.assertIsNone(out['posture']['size_mult'])
    def test_source_tamper_rejected(self):
        self.bodies['observations:VIXCLS']+=b' '
        out=self.build();self.assertIn('VIXCLS',out['source_failures']);self.assertIsNone(out['term_structure']['value'])
    def test_missing_latest_is_not_replaced_by_last_good(self):
        mutate(self.inputs,self.bodies,'observations:VIXCLS',lambda d:d['observations'][-1].update(value='.'))
        row=self.build()['measurements']['VIXCLS'];self.assertIsNone(row['value']);self.assertEqual(row['quality']['status'],'unavailable')
        self.assertIsNone(row['percentile_2y']['value'])
    def test_missing_baseline_keeps_change_null(self):
        mutate(self.inputs,self.bodies,'observations:BAMLH0A0HYM2',lambda d:d['observations'][-6].update(value='.'))
        self.assertIsNone(self.build()['measurements']['BAMLH0A0HYM2']['change_5_provider_rows']['value'])
    def test_actual_dated_difference(self):
        change=self.build()['measurements']['BAMLH0A0HYM2']['change_5_provider_rows']
        self.assertEqual(change['baseline_date'],'2026-09-10');self.assertEqual(change['current_date'],'2026-09-17')
        self.assertEqual(change['decimal'],'0.0')
    def test_vix_mismatched_dates_withheld(self):
        def edit(d):d['observations'].pop();d['count']-=1
        mutate(self.inputs,self.bodies,'observations:VXVCLS',edit)
        self.assertIsNone(self.build()['term_structure']['value'])
    def test_wrong_units_rejected(self):
        mutate(self.inputs,self.bodies,'definition:VIXCLS',lambda d:d['seriess'][0].update(units='Percent'))
        self.assertIn('VIXCLS',self.build()['source_failures'])
    def test_fred_request_binding_and_pagination(self):
        self.inputs['sources']['observations:VIXCLS']['request_url']+='&units=pc1'
        self.assertIn('VIXCLS',self.build()['source_failures'])
        self.inputs,self.bodies=fixture()
        mutate(self.inputs,self.bodies,'observations:VIXCLS',lambda d:d.update(count=d['count']+1))
        self.assertIn('VIXCLS',self.build()['source_failures'])
    def test_duplicate_fred_rows_rejected(self):
        mutate(self.inputs,self.bodies,'observations:VIXCLS',lambda d:d['observations'][-1].update(date=d['observations'][-2]['date']))
        self.assertIn('VIXCLS',self.build()['source_failures'])
    def test_incomplete_spy_chain_cannot_compute_skew_or_ratio(self):
        row=self.build()['option_cohorts']['SPY'];self.assertFalse(row['pagination_complete'])
        self.assertTrue(all(x['put_call_volume_ratio'] is None and x['skew_25delta_vol_points'] is None for x in row['expiries']))
    def test_hyg_complete_chain_keeps_missing_volume(self):
        row=self.build()['option_cohorts']['HYG'];self.assertTrue(row['pagination_complete']);self.assertEqual(row['contracts'],158)
        self.assertTrue(any(x['volume_coverage']<1 for x in row['expiries']))
        for x in row['expiries']:
            if x['volume_coverage']<1:self.assertIsNone(x['put_call_volume_ratio'])
            self.assertIsNone(x['iv_observed_at']);self.assertFalse(x['iv_timing_verified'])
    def test_oi_never_changes_volume_ratio(self):
        before=self.build()['option_cohorts']['HYG']['expiries']
        mutate(self.inputs,self.bodies,'options:HYG:1',lambda d:[r.update(open_interest=99999) for r in d['results']])
        after=self.build()['option_cohorts']['HYG']['expiries']
        self.assertEqual([r['put_call_volume_ratio'] for r in before],[r['put_call_volume_ratio'] for r in after])
    def test_option_duplicate_or_out_of_universe_rejected(self):
        mutate(self.inputs,self.bodies,'options:HYG:1',lambda d:d['results'].append(copy.deepcopy(d['results'][0])))
        self.assertIn('options:HYG',self.build()['source_failures'])
        self.inputs,self.bodies=fixture()
        mutate(self.inputs,self.bodies,'options:HYG:1',lambda d:d['results'][0]['details'].update(expiration_date='2027-01-01'))
        self.assertIn('options:HYG',self.build()['source_failures'])
    def test_cursor_chain_requires_exact_link(self):
        name='options:SPY:2';self.inputs['option_pages']['SPY'].append(name)
        self.inputs['sources'][name]=copy.deepcopy(self.inputs['sources']['options:SPY:1']);self.bodies[name]=self.bodies['options:SPY:1']
        self.assertIn('options:SPY',self.build()['source_failures'])
    def test_untrusted_next_url_rejected(self):
        mutate(self.inputs,self.bodies,'options:SPY:1',lambda d:d.update(next_url='https://attacker.example/collect?cursor=x'))
        self.assertIn('options:SPY',self.build()['source_failures'])
    def test_interpolation_bracket_and_evidence(self):
        candidates=[{'ticker':'A','iv':.3,'abs_delta':.2},{'ticker':'B','iv':.2,'abs_delta':.3}]
        r=m.interpolate(candidates);self.assertAlmostEqual(r['iv'],.25);self.assertEqual(len(r['contracts']),2)
        self.assertIsNone(m.interpolate(candidates[:1]));self.assertIsNone(m.interpolate([dict(candidates[0],abs_delta=.1),candidates[1]]))
    def test_fx_exact_week_baseline(self):
        row=self.build()['fx_measurement'];self.assertEqual(row['unit'],'JPY_per_AUD');self.assertEqual(row['baseline_date'],row['target_date'])
        def edit(d):
            target=row['baseline_date'];d['results']=[r for r in d['results'] if datetime.fromtimestamp(r['t']/1000,timezone.utc).date().isoformat()!=target];d['resultsCount']=len(d['results'])
        mutate(self.inputs,self.bodies,'fx:AUDJPY',edit)
        self.assertIsNone(self.build()['fx_measurement']['change_7_calendar_days_pct'])
    def test_no_source_means_abstention(self):
        self.inputs['sources']={};self.inputs['option_pages']={};out=self.build()
        self.assertEqual(out['quality']['fresh_native_series'],0);self.assertIsNone(out['risk_regime_score'])
        self.assertEqual(out['independent_vote_count'],0)


class Store(unittest.TestCase):
    def setUp(self):
        self.inputs,self.bodies=fixture();self.client=Storage();self.read=s.reader(self.client,'fixture')
        for name,raw in self.bodies.items():self.client.objects[self.inputs['sources'][name]['key']]=raw
    def test_original_source_replay_and_tamper(self):
        output=s.compile_output(self.inputs,self.bodies,self.read);ref=s.retain(self.client,'fixture',self.inputs,output)
        self.assertEqual(s.replay(ref,self.read),output)
        key=self.inputs['sources']['observations:VIXCLS']['key'];self.client.objects[key]+=b' '
        with self.assertRaisesRegex(ValueError,'original response'):s.replay(ref,self.read)
    def test_reviewed_compiler_not_downloaded_execution(self):
        output=s.compile_output(self.inputs,self.bodies,self.read);ref=s.retain(self.client,'fixture',self.inputs,output)
        manifest=json.loads(self.read(ref['manifest_key']));self.client.objects[manifest['compilers']['regime_model']['key']]=b'print("untrusted")'
        with self.assertRaisesRegex(ValueError,'compiler differs'):s.replay(ref,self.read)
    def test_publication_preserves_whole_previous_in_private_prefix(self):
        old=b'{"generated_at":"2026-09-19T20:00:00Z","legacy_opaque":"fixture"}'
        self.client.objects[m.CURRENT]=old
        packet={'generated_at':'2026-09-20T07:00:00Z','call':None}
        self.assertTrue(s.publish(self.client,'fixture',packet))
        key=s.PRIVATE+hashlib.sha256(old).hexdigest()+'.bin';self.assertEqual(self.client.objects[key],old)
        self.assertTrue(any(w['Key']==key and w['CacheControl']=='no-store' for w in self.client.writes))
    def test_older_publication_cannot_overwrite(self):
        self.client.objects[m.CURRENT]=b'{"generated_at":"2026-09-20T09:00:00Z"}'
        self.assertFalse(s.publish(self.client,'fixture',{'generated_at':'2026-09-20T07:00:00Z'}))
    def test_cas_publication_race(self):
        old=self.client.put_object;attempts=[]
        def put(**kw):
            if kw['Key']==m.CURRENT and not attempts:
                attempts.append(True);self.client.objects[m.CURRENT]=b'{"generated_at":"2026-09-20T09:00:00Z"}';raise FakeError('PreconditionFailed')
            return old(**kw)
        self.client.put_object=put
        self.assertFalse(s.publish(self.client,'fixture',{'generated_at':'2026-09-20T07:00:00Z'}))
    def test_capture_rejects_reflected_credential(self):
        c=s.Collector(self.client,'fixture','secret_fred','secret_massive',transport=lambda *a,**k:io.BytesIO(b'{"next_url":"secret_massive"}'))
        self.assertIsNone(c.fetch('x','https://api.massive.com/v3/snapshot/options/SPY?limit=250','massive'))
        self.assertFalse(self.client.writes);self.assertEqual(c.sources['x']['reason'],'ValueError')
    def test_capture_auth_not_retained(self):
        requests=[]
        def transport(req,**kw):requests.append(req);return io.BytesIO(b'{"results":[]}')
        c=s.Collector(self.client,'fixture','secret_fred','secret_massive',transport=transport)
        url='https://api.massive.com/v3/snapshot/options/SPY?limit=250';c.fetch('x',url,'massive')
        self.assertEqual(requests[0].get_header('Authorization'),'Bearer secret_massive')
        self.assertNotIn('secret_massive',json.dumps(c.sources));self.assertEqual(c.sources['x']['request_url'],url)
    def test_source_size_budget_and_deadline(self):
        c=s.Collector(self.client,'fixture','f','m',deadline=1)
        self.assertIsNone(c.fetch('x','https://api.massive.com/a','massive'))
        self.assertEqual(c.sources['x']['reason'],'TimeoutError')
        with self.assertRaisesRegex(ValueError,'byte bound'):s.bounded(io.BytesIO(b'x'*(s.MAX_BYTES+1)))
    def test_settlement_scopes_keep_zero_and_separation(self):
        d={'treasury':{'ftd_bn':0,'ftr_bn':97,'gross_bn':97,'as_of':'2026-09-09','quality':{'status':'fresh'}},
           'headline':{'ftd_bn':86,'ftr_bn':87,'combined_bn':173,'as_of':'2026-09-09','quality':{'status':'fresh'}}}
        raw=m.encoded(d);sha=hashlib.sha256(raw).hexdigest();key=m.PREFIX+'settlement-inputs/'+sha+'.json'
        self.client.objects[key]=raw;self.inputs['settlement']={'key':key,'sha256':sha,'bytes':len(raw)}
        row=s.compile_output(self.inputs,self.bodies,self.read)['pd_settlement_fails']
        self.assertEqual(row['ftd_bn'],0);self.assertEqual(row['combined_bn'],97);self.assertEqual(row['ust_ex_tips']['combined_bn'],173)


if __name__=='__main__':unittest.main()
