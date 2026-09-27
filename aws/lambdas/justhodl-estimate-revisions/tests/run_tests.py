"""Native observation/archive regressions; synthetic public provider bytes, no network/AWS."""
from pathlib import Path
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch
import ast,base64,hashlib,json,sys,time,unittest,urllib.request,urllib.error,urllib.parse
ROOT=Path(__file__).resolve().parents[4];SRC=ROOT/'aws/lambdas/justhodl-estimate-revisions/source';sys.path.insert(0,str(SRC))
from estimate_observations import CONTRACT,number,strict,day,clock,envelope,original,projection,compare,dossier
NOW=datetime.now(timezone.utc).date().isoformat()


def rows(eps=2,target='2030-12-31',currency='USD'):
    return [{'symbol':'TEST','date':target,'cik':'0001','reportedCurrency':currency,'epsBasis':'reported_adjusted',
             'epsAvg':eps,'epsLow':0,'epsHigh':20,'numAnalystsEps':10}]


def acquisition(values=None,stamp=None):
    return envelope(json.dumps(rows() if values is None else values).encode(),stamp or NOW+'T01:00:00Z')


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self,previous=b'{"version":"3.1.0","top_picks":[]}'):
        self.data={} if previous is None else {'data/estimate-revisions.json':previous};self.reads=[];self.writes=[];self.denied=False;self.race=False;self.corrupt=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=self.data[key]
        if self.corrupt and '/history/' in key:raw=b'wrong'
        return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(key)
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if key=='data/estimate-revisions.json' and self.race:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=hashlib.sha256(self.data.get(key,b'')).hexdigest():raise Error('PreconditionFailed')
        self.data[key]=kw['Body']


def native(memory=None,**extra):
    scope={'S3':memory or Memory(),'BUCKET':'fixture','OUT_KEY':'data/estimate-revisions.json','FMP_KEY':'fixture-secret-only',
           'HORIZON_DAYS':75,'MIN_IMPORTANCE':2,'FMP_SEED_CAP':280,'CONTRACT':CONTRACT,'strict':strict,'number':number,'day':day,'clock':clock,'envelope':envelope,'dossier':dossier,
           'datetime':datetime,'timezone':timezone,'ThreadPoolExecutor':ThreadPoolExecutor,'hashlib':hashlib,'json':json,'time':time,'urllib':urllib,
           'fetch_calendar':lambda **kw:[{'ticker':'TEST','date':'2030-01-01','importance':2,'fiscal_period':'Q1'}],**extra}
    tree=ast.Module(body=[n for n in ast.parse((SRC/'lambda_function.py').read_bytes()).body if isinstance(n,ast.FunctionDef) and (n.name.startswith('_estimate_') or n.name=='lambda_handler')],type_ignores=[])
    exec(compile(tree,'<actual native observation functions>','exec'),scope)
    return scope


class Tests(unittest.TestCase):
    def test_all_bytes_unknown_fields_and_every_occurrence_are_retained(self):
        a=acquisition(rows()+[None,{'unknown':'whole'}]+rows());p=projection('TEST',a,NOW)
        self.assertEqual(len(p),4);self.assertEqual([r['raw'] for r in p],original(a));self.assertEqual(p[1]['measurement_status'],'unqualified_record')
    def test_zero_missing_boolean_nonfinite_and_huge_numbers_remain_distinct(self):
        self.assertEqual(number(0),0)
        for v in [True,False,'0','',float('inf'),10**1000,None]:self.assertIsNone(number(v))
    def test_past_targets_stay_past_and_calendar_period_is_not_inferred(self):
        p=projection('TEST',acquisition(rows(target='2020-12-31')),NOW)[0]
        self.assertEqual(p['target_status'],'past_target');self.assertEqual(p['target_period_end'],'2020-12-31');self.assertEqual(p['requested_period'],'annual');self.assertIsNone(p['estimate_strength'])
    def test_only_same_target_issuer_unit_basis_ordered_snapshot_can_compare(self):
        old=projection('TEST',acquisition(rows(2),NOW+'T00:00:00Z'),NOW);current=projection('TEST',acquisition(rows(3)),NOW)
        result=compare(current,old)[0];self.assertEqual(result['eps_change'],1);self.assertEqual(result['eps_change_pct_positive_base'],50)
        for field,value in [('target_period_end','2031-12-31'),('reported_currency','JPY'),('eps_basis','GAAP'),('reported_cik','2'),('reported_currency',None),('received_at',NOW+'T00:00:00Z')]:
            changed=deepcopy(current);changed[0][field]=value;self.assertIsNone(compare(changed,old)[0]['eps_change'],field)
    def test_zero_and_negative_baselines_have_absolute_change_not_infinite_percent(self):
        for value in [0,-2]:
            a=rows(value);a[0]['epsLow']=-5
            old=projection('TEST',acquisition(a,NOW+'T00:00:00Z'),NOW);new=projection('TEST',acquisition(rows(2)),NOW)
            result=compare(new,old)[0];self.assertEqual(result['eps_change'],2-value);self.assertIsNone(result['eps_change_pct_positive_base'])
    def test_duplicate_targets_do_not_select_one_convenient_baseline(self):
        old=projection('TEST',acquisition(rows(2)*2,NOW+'T00:00:00Z'),NOW);new=projection('TEST',acquisition(rows(3)),NOW)
        self.assertIsNone(compare(new,old)[0]['eps_change']);self.assertIsNone(compare(new*2,old[:1])[0]['eps_change'])
    def test_wrong_symbols_invalid_dates_counts_ranges_and_clocks_do_not_qualify(self):
        for field,value in [('symbol','OTHER'),('date','2030-02-30'),('numAnalystsEps',-1),('numAnalystsEps',1.5),('epsLow',3),('epsHigh',1)]:
            a=rows();a[0][field]=value;p=projection('TEST',acquisition(a),NOW)[0];self.assertEqual(p['measurement_status'],'unqualified_record',field)
        with self.assertRaises(ValueError):envelope(b'[]','2026-01-01')
    def test_whole_original_tampering_and_unsafe_json_fail(self):
        a=acquisition();a['original_bytes']+=1
        with self.assertRaises(ValueError):original(a)
        for raw in [b'{"x":1,"x":2}',b'{"x":NaN}',b'[1e999]',b'[1e-999]',b'\xff']:
            with self.assertRaises((ValueError,UnicodeError)):strict(raw)
        with self.assertRaises(ValueError):acquisition(rows()*7)
    def test_original_byte_budget_declares_unattempted_requests_without_truncating_responses(self):
        calendar=[{'ticker':'TEST','date':'2030-01-01'} for _ in range(30)]
        values=rows();values[0]['complete_extra_field']='x'*500000
        a=acquisition(values);n=native(fetch_calendar=lambda **kw:calendar);calls=[]
        n['_estimate_fetch']=lambda symbol:(calls.append(symbol) or a)
        n['lambda_handler']();p=strict(n['S3'].data['data/estimate-revisions.json'])
        self.assertEqual(len(calls),10);self.assertEqual(len(p['request_records']),30)
        self.assertEqual(len(p['request_records'][0]['observations'][0]['raw']['complete_extra_field']),500000)
    def test_native_keeps_calendar_and_duplicate_requests_without_scores_private_state_or_notifications(self):
        memory=Memory();calendar=[{'ticker':'TEST','date':'2030-01-01','fiscal_period':'Q1'},{'ticker':'TEST','date':'2030-04-01','fiscal_period':'Q2'},None]
        n=native(memory,fetch_calendar=lambda **kw:calendar);n['_estimate_fetch']=lambda symbol:acquisition() if symbol else {'status':'invalid_symbol_not_requested'}
        n['lambda_handler']();p=strict(memory.data['data/estimate-revisions.json'])
        self.assertEqual(p['calendar_rows'],calendar);self.assertEqual(len(p['request_records']),3);self.assertEqual(p['n_estimate_observations'],2)
        self.assertEqual(p['direction_map'],{});self.assertEqual(p['top_picks'],[]);self.assertIsNone(p['call']);self.assertFalse(p['private_state_read_or_written'])
        self.assertTrue(all(k=='data/estimate-revisions.json' or k.startswith('data/estimate-revisions/history/') for k in memory.reads+memory.writes))
        self.assertEqual(sum('/history/' in k for k in memory.data),2)
    def test_cap_and_budget_retain_every_unselected_and_unattempted_occurrence(self):
        calendar=[{'ticker':'TEST','date':'2030-01-01'} for _ in range(300)];n=native(fetch_calendar=lambda **kw:calendar);calls=[]
        n['_estimate_fetch']=lambda symbol:(calls.append(symbol) or acquisition())
        times=iter([30000,10000]);n['lambda_handler'](context=SimpleNamespace(get_remaining_time_in_millis=lambda:next(times,10000)))
        p=strict(n['S3'].data['data/estimate-revisions.json']);self.assertEqual(len(calls),10);self.assertEqual(len(p['request_records']),280);self.assertEqual(len(p['not_selected_calendar_indices']),20)
        self.assertEqual(sum(r['acquisition']['status'].startswith('not_attempted') for r in p['request_records']),270)
    def test_rate_limit_stops_remaining_windows_and_never_retries(self):
        calendar=[{'ticker':'TEST','date':'2030-01-01'} for _ in range(30)];n=native(fetch_calendar=lambda **kw:calendar);calls=[]
        def acquire(symbol):
            calls.append(symbol);return {'status':'rate_limited'} if len(calls)==1 else acquisition()
        n['_estimate_fetch']=acquire;n['lambda_handler']();self.assertEqual(len(calls),10)
    def test_failed_calendar_or_all_failed_acquisitions_preserve_original(self):
        for calendar,status in [([],None),([{'ticker':'TEST'}],'unavailable')]:
            memory=Memory();before=dict(memory.data);n=native(memory,fetch_calendar=lambda **kw:calendar);n['_estimate_fetch']=lambda s:{'status':status}
            with self.assertRaises(ValueError):n['lambda_handler']()
            self.assertEqual(memory.data,before);self.assertEqual(memory.writes,[])
    def test_archive_corruption_or_concurrent_publish_preserves_current(self):
        for field in ['corrupt','race']:
            memory=Memory();setattr(memory,field,True);before=memory.data['data/estimate-revisions.json'];n=native(memory);n['_estimate_fetch']=lambda s:acquisition()
            with self.assertRaises((ValueError,Error)):n['lambda_handler']()
            self.assertEqual(memory.data['data/estimate-revisions.json'],before)
    def test_missing_prior_allowed_but_access_denied_and_malformed_prior_fail(self):
        n=native(Memory(None));self.assertEqual(n['_estimate_previous'](),(None,None,None))
        memory=Memory();memory.denied=True
        with self.assertRaises(Error):native(memory)['_estimate_previous']()
        with self.assertRaises(ValueError):native(Memory(b'{bad'))['_estimate_previous']()
    def test_native_endpoint_scope_zero_originals_and_error_redaction(self):
        n=native();raw=json.dumps(rows(0)).encode()
        with patch.object(urllib.request,'urlopen',return_value=BytesIO(raw)) as mock:
            result=n['_estimate_fetch']('TEST')
        self.assertEqual(original(result)[0]['epsAvg'],0);self.assertEqual(mock.call_count,1);self.assertIn('period=annual&limit=6',mock.call_args.args[0].full_url)
        error=urllib.error.HTTPError('https://invalid/?secret=DO_NOT_LEAK',429,'secret text',{},None)
        with patch.object(urllib.request,'urlopen',side_effect=error) as mock:result=n['_estimate_fetch']('TEST')
        self.assertEqual(mock.call_count,1);self.assertEqual(result['status'],'rate_limited');self.assertNotIn('secret',json.dumps(result))
        with patch.object(urllib.request,'urlopen',return_value=BytesIO(b'[{"echo":"fixture-secret-only"}]')):result=n['_estimate_fetch']('TEST')
        self.assertEqual(result['status'],'unavailable');self.assertNotIn('original_base64',result)


if __name__=='__main__':unittest.main(verbosity=2)
