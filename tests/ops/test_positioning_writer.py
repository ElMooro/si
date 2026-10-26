"""Isolated actual writer and transport nodes; no live source or native I/O."""
from pathlib import Path
from datetime import datetime,timezone
from io import BytesIO
from types import SimpleNamespace
from threading import Event
from concurrent.futures import ThreadPoolExecutor,as_completed
from unittest.mock import patch
from copy import deepcopy
import ast,gzip,json,sys,time,unittest,urllib.request,urllib.error
ROOT=Path(__file__).resolve().parents[2];SRC=ROOT/'aws/lambdas/justhodl-pump-positioning/source'
sys.path[:0]=[str(ROOT/'aws/shared'),str(SRC)]
import context_evidence_store as store
import managed_secret
import positioning_observations as m
import bank_originals as real_bank_originals
from test_positioning_observations import captures,prices,AT,ASOF
from datetime import timedelta

PULLED='2026-09-27T23:27:00+00:00'


class Bank:
    """Stand-in for aws/shared/bank_originals: answers the URLs in `answers` ({url: raw}); records every ask."""
    TRANSPORT=real_bank_originals.TRANSPORT
    newer_than=staticmethod(real_bank_originals.newer_than)
    vintage=staticmethod(real_bank_originals.vintage)
    def __init__(self,answers=None,pulled=PULLED):self.answers=dict(answers or {});self.asked=[];self.pulled=pulled
    def bank_original(self,url,now=None):
        self.asked.append(url);raw=self.answers.get(url)
        if raw is None:return None
        return {'raw':raw,'transport':self.TRANSPORT,'received_at':AT,'bank':{'object':'data/warm/fmp/synthetic/'+url.rsplit('/',1)[1][:40],'pulled_at':self.pulled,'as_of':None}}


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        inputs,self.attempts,self.sources=captures()
        self.data={a['source_key']:self.sources[a['original_ref']['key']] for a in inputs.values()}
        self.previous=b'{"synthetic_previous":"whole original"}';self.data[m.HEAD]=self.previous
        self.reads=[];self.writes=[];self.corrupt=False;self.fail_retention=False;self.lost_ack=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key not in self.data:raise Error('NoSuchKey')
        raw=self.data[key]
        if self.corrupt and key.startswith(m.PRIVATE):raw+=b'bad'
        return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':store.sha(raw)}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(kw)
        if self.fail_retention and key.startswith(m.PRIVATE):raise Error('AccessDenied')
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=store.sha(self.data.get(key,b'')):raise Error('PreconditionFailed')
        self.data[key]=kw['Body']
        if self.lost_ack and key==m.HEAD:raise Error('RequestTimeout')


class FixedDateTime(datetime):
    @classmethod
    def now(cls,tz=None):return datetime.fromisoformat(AT.replace('Z','+00:00'))


def native(memory=None,bank=None):
    mem=memory or Memory();requests=[];bank=bank or Bank()
    ns={'Path':Path,'__file__':str(SRC/'lambda_function.py'),'Event':Event,'time':time,
        'datetime':FixedDateTime,'timezone':timezone,'ThreadPoolExecutor':ThreadPoolExecutor,'as_completed':as_completed,
        'boto3':SimpleNamespace(client=lambda *a,**k:mem),'Config':lambda **kw:kw,'S3_BUCKET':'synthetic',
        'observations':m,'context_evidence_store':store,'managed_secret_module':managed_secret,'bank_originals':bank,
        'ContextStore':store.ContextStore,'encode':store.encode,'code':store.code,'now':lambda:AT,
        'json':json,'urllib':urllib,'FMP_KEY':'synthetic-only'}
    tree=ast.parse((SRC/'lambda_function.py').read_bytes())
    nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and n.name in
           ('lambda_handler','_positioning_request','_PositioningNoRedirect','_positioning_previous_vintages')]
    assert len(nodes)==4
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated positioning writer>','exec'),ns)
    real_request=ns['_positioning_request']
    def synthetic_request(ticker,kind,as_of,started,stop,previous_vintage=None):
        requests.append((ticker,kind,as_of));a=deepcopy(next(a for a in mem.attempts if a['ticker']==ticker and a['kind']==kind))
        return a,mem.sources[a.pop('original_ref')['key']]
    ns['_positioning_request']=synthetic_request
    return ns,mem,requests,real_request


def bank_attempts(mem):
    """The memory's synthetic attempts with history and profile received from the warehouse bank."""
    for a in mem.attempts:
        if a['kind'] in m.BANK_KINDS:
            a.update(http_status=None,network_attempted=False,transport='warehouse_bank',bank={'object':'data/warm/fmp/synthetic/'+a['kind'],'pulled_at':PULLED,'as_of':None})
        else:a.update(transport='provider',bank_attempt=None)
    return mem


class Response:
    def __init__(self,raw=b'[]',status=200,length=None):
        self.stream=BytesIO(raw);self.status=status;self.headers={'Content-Length':str(len(raw) if length is None else length)};self.closed=False
    def read1(self,n):return self.stream.read(n)
    def close(self):self.closed=True


class Tests(unittest.TestCase):
    def test_complete_writer_replay_original_retention_and_fixed_scope(self):
        ns,mem,requests,_=native();r=ns['lambda_handler']({'head':'private/accounts.json','tickers':['OTHER']},None)
        self.assertEqual(r['statusCode'],200);p=json.loads(mem.data[m.HEAD]);manifest=json.loads(mem.data[p['replay']['input_ref']['key']])
        replay=m.build(manifest['input_attempts'],manifest['provider_attempts'],mem.data,manifest['generated_at'])
        for key,value in replay.items():self.assertEqual(p[key],value)
        self.assertEqual(len(manifest['source_files']),4);self.assertEqual(len(manifest['input_attempts']),6)
        self.assertEqual(sorted(requests),sorted(('SYNA',kind,ASOF) for kind in ('history','quote','profile')))
        self.assertEqual(mem.data[p['replay']['previous_publication']['key']],mem.previous)
        for name,ref in manifest['source_files'].items():
            path=SRC/name if name in ('lambda_function.py','positioning_observations.py') else ROOT/'aws/shared'/name
            self.assertEqual(mem.data[ref['key']],path.read_bytes())
        self.assertEqual(mem.data[store.identity(mem.data[m.HEAD],m.PRIVATE,'outputs')['key']],mem.data[m.HEAD])
        self.assertTrue(all(k in (*m.INPUTS.values(),m.HEAD) or k.startswith(m.PRIVATE) for k in mem.reads))
        self.assertEqual(p['call'],'WAIT');self.assertEqual(p['coverage']['eligible_votes'],0)
    def test_prepublication_retention_failures_never_replace_previous(self):
        for setting in ('corrupt','fail_retention'):
            ns,mem,_,_=native();setattr(mem,setting,True);r=ns['lambda_handler']({},None)
            self.assertEqual(r['statusCode'],503);self.assertEqual(mem.data[m.HEAD],mem.previous)
            self.assertIs(json.loads(r['body'])['previous_publication_preserved'],True)
            self.assertFalse(any(w['Key']==m.HEAD for w in mem.writes))
    def test_commit_then_timeout_never_claims_preservation_or_retries(self):
        ns,mem,_,_=native();mem.lost_ack=True;r=ns['lambda_handler']({},None)
        self.assertEqual(r['statusCode'],503);self.assertNotEqual(mem.data[m.HEAD],mem.previous)
        self.assertIsNone(json.loads(r['body'])['previous_publication_preserved'])
        self.assertEqual(json.loads(r['body'])['publication_status'],'acknowledgement_unknown')
        self.assertEqual(sum(w['Key']==m.HEAD for w in mem.writes),1)
    def test_missing_primary_and_invalid_population_never_fabricate_empty_selection(self):
        for value in (None,{}, {'pump_candidates':[{'ticker':'../BAD'}]}):
            ns,mem,requests,_=native()
            if value is None:mem.data.pop(m.INPUTS['radar'])
            else:mem.data[m.INPUTS['radar']]=store.encode(value)
            self.assertEqual(ns['lambda_handler']({},None)['statusCode'],503)
            self.assertEqual(mem.data[m.HEAD],mem.previous);self.assertEqual(requests,[])
    def test_explicit_empty_selection_is_research_only_without_provider_requests(self):
        ns,mem,requests,_=native();mem.data[m.INPUTS['radar']]=b'{"pump_candidates":[]}'
        self.assertEqual(ns['lambda_handler']({},None)['statusCode'],200);p=json.loads(mem.data[m.HEAD])
        self.assertEqual(p['selection']['status'],'reported_empty_selection');self.assertEqual(requests,[])
        self.assertIsNone(p['aggressive_basket']['total_exposure'])
    def test_transport_retains_complete_body_and_key_stays_out_of_identity(self):
        _,_,_,request=native();response=Response(b'{"complete":true}');seen=[]
        def open_request(req,timeout):seen.append(req);self.assertEqual(timeout,12);return response
        with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=open_request)):
            a,raw=request('SYNA','history',ASOF,time.monotonic(),Event())
        self.assertEqual(raw,b'{"complete":true}');self.assertEqual(a['status'],'received');self.assertTrue(response.closed)
        self.assertNotIn('synthetic-only',a['endpoint']);self.assertNotIn('apikey',seen[0].full_url)
        self.assertEqual(seen[0].get_header('Apikey'),'synthetic-only')
    def test_declared_length_truncation_oversize_and_elapsed_transfer_are_unavailable(self):
        for response in (Response(b'[]',length=20),Response(b'[]',length=m.MAX_SOURCE_BYTES+1)):
            _,_,_,request=native()
            with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=lambda *a,**k:response)):
                a,raw=request('SYNA','history',ASOF,time.monotonic(),Event())
            self.assertIsNone(raw);self.assertNotEqual(a['status'],'received');self.assertTrue(response.closed)
        _,_,_,request=native();response=Response()
        with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=lambda *a,**k:response)),patch.object(time,'monotonic',side_effect=[0,0,21]):
            a,raw=request('SYNA','history',ASOF,0,Event())
        self.assertEqual(a['status'],'transport_unavailable');self.assertIsNone(raw)
    def test_rate_limit_stops_new_requests_without_retry_and_retains_error_body(self):
        _,_,_,request=native();response=Response(b'{"error":"synthetic quota"}',429);stop=Event()
        with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=lambda *a,**k:response)) as opener:
            a,raw=request('SYNA','history',ASOF,time.monotonic(),stop)
            b,empty=request('SYNA','quote',ASOF,time.monotonic(),stop)
        self.assertTrue(stop.is_set());self.assertEqual(a['status'],'http_error');self.assertEqual(raw,b'{"error":"synthetic quota"}')
        self.assertEqual(b['status'],'not_attempted_stop');self.assertFalse(b['network_attempted']);self.assertIsNone(empty);self.assertEqual(opener.call_count,1)
    def test_redirects_refused_before_credentials_can_follow(self):
        ns,_,_,_=native();handler=ns['_PositioningNoRedirect']();req=urllib.request.Request(m.endpoint('SYNA','quote',ASOF))
        with self.assertRaises(urllib.error.HTTPError):handler.redirect_request(req,BytesIO(),302,'redirect',{},'https://example.com/steal')
    def test_original_runtime_and_schedule_are_preserved(self):
        cfg=json.loads((SRC.parent/'config.json').read_bytes());original=json.loads((ROOT/'tests/fixtures/pre-positioning-observations-config.json.txt').read_bytes())
        for key in ('runtime','memory','timeout','handler','role_arn','environment','eventbridge_scheduler'):self.assertEqual(cfg[key],original[key])

    # ---- bank rung (2026-10-26): history / profile originals from the warehouse bank, quotes live, provenance recorded ----
    def test_bank_rung_answers_history_and_profile_with_provenance_and_never_asks_for_quotes(self):
        history=m.endpoint('SYNA','history',ASOF);profile=m.endpoint('SYNA','profile',ASOF);quote=m.endpoint('SYNA','quote',ASOF)
        bank=Bank({history:b'[{"date":"2026-09-26","close":1}]',profile:b'[{"symbol":"SYNA","currency":"USD"}]'})
        _,_,_,request=native(bank=bank);stop=Event()
        with patch.object(urllib.request,'build_opener') as opener:
            a,raw=request('SYNA','history',ASOF,time.monotonic(),stop)
            b,praw=request('SYNA','profile',ASOF,time.monotonic(),stop)
        self.assertEqual(opener.call_count,0,'a bank answer opens no connection')
        self.assertEqual((a['status'],a['transport'],a['network_attempted'],a['http_status'],a['content_encoding']),('received','warehouse_bank',False,None,''))
        self.assertEqual(raw,b'[{"date":"2026-09-26","close":1}]');self.assertEqual(praw,b'[{"symbol":"SYNA","currency":"USD"}]')
        self.assertEqual(set(a['bank']),set(m.BANK_PROVENANCE));self.assertEqual(a['bank']['pulled_at'],PULLED);self.assertNotIn('bank_attempt',a)
        self.assertEqual(b['status'],'received');self.assertEqual(b['transport'],'warehouse_bank')
        self.assertEqual(bank.asked,[history,profile])
        # the quote is never asked of the bank and goes live exactly as before
        response=Response(b'[{"symbol":"SYNA","price":101}]')
        with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=lambda *x,**k:response)):
            q,qraw=request('SYNA','quote',ASOF,time.monotonic(),stop)
        self.assertEqual((q['status'],q['transport'],q['network_attempted'],q['http_status']),('received','provider',True,200))
        self.assertNotIn('bank_attempt',q);self.assertEqual(bank.asked,[history,profile])
        self.assertFalse(stop.is_set())
        # a provider stop does not apply to the bank: history still comes from the bank, the quote is not attempted
        stop.set()
        with patch.object(urllib.request,'build_opener') as opener:
            a2,raw2=request('SYNA','history',ASOF,time.monotonic(),stop);q2,none=request('SYNA','quote',ASOF,time.monotonic(),stop)
        self.assertEqual(a2['status'],'received');self.assertEqual(raw2,raw);self.assertEqual(q2['status'],'not_attempted_stop');self.assertIsNone(none)
        self.assertEqual(opener.call_count,0)
    def test_bank_vintage_not_newer_or_no_answer_falls_to_the_provider_and_records_why(self):
        history=m.endpoint('SYNA','history',ASOF);bank=Bank({history:b'[]'})
        _,_,_,request=native(bank=bank);response=Response(b'[{"date":"2026-09-26","close":1}]');seen=[]
        def open_request(req,timeout):seen.append(req);return response
        with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=open_request)):
            a,raw=request('SYNA','history',ASOF,time.monotonic(),Event(),previous_vintage='2026-09-27T23:30:00+00:00')   # live receipt after the bank's pull
        self.assertEqual((a['status'],a['transport'],a['network_attempted'],a['http_status']),('received','provider',True,200))
        self.assertEqual(a['bank_attempt'],{'status':'bank_vintage_not_newer','bank':{'object':'data/warm/fmp/synthetic/'+history.rsplit('/',1)[1][:40],'pulled_at':PULLED,'as_of':None}})
        self.assertNotIn('bank',a);self.assertEqual(raw,b'[{"date":"2026-09-26","close":1}]');self.assertEqual(len(seen),1)
        with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=open_request)):
            b,_=request('SYNA','history',ASOF,time.monotonic(),Event(),previous_vintage='2026-09-27T20:00:00+00:00')   # bank pulled after the previous receipt
        self.assertEqual(b['transport'],'warehouse_bank');self.assertEqual(len(seen),1)
        with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=open_request)):
            c,_=request('SYNA','profile',ASOF,time.monotonic(),Event())                                              # nothing banked for this URL
        self.assertEqual(c['bank_attempt'],{'status':'no_bank_answer'});self.assertEqual(c['transport'],'provider');self.assertEqual(len(seen),2)
        # the live rung's 429 still stops the collection; a bank answer never did
        response429=Response(b'{"error":"quota"}',429);stop=Event()
        with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=lambda *x,**k:response429)):
            d,_=request('SYNA','quote',ASOF,time.monotonic(),stop)
        self.assertTrue(stop.is_set());self.assertEqual(d['status'],'http_error')
    def test_complete_writer_publishes_bank_originals_counts_and_replays(self):
        mem=bank_attempts(Memory());ns,mem,requests,_=native(mem);r=ns['lambda_handler']({},None)
        self.assertEqual(r['statusCode'],200);p=json.loads(mem.data[m.HEAD]);manifest=json.loads(mem.data[p['replay']['input_ref']['key']])
        self.assertEqual(p['bank_rung'],{'module':'bank_originals','kinds':['history','profile'],
            'rule':'warehouse original admitted only when its pulled_at is later than the previous retained original for the same request; quotes never',
            'n_bank_originals':2,'n_live_originals':1,'n_vintage_refused':0})
        self.assertEqual(manifest['controls']['bank_rung'],p['bank_rung']);self.assertEqual(manifest['controls']['provider_retries'],0)
        replay=m.build(manifest['input_attempts'],manifest['provider_attempts'],mem.data,manifest['generated_at'])
        for key,value in replay.items():self.assertEqual(p[key],value)
        by={a['kind']:a for a in p['acquisitions']}
        self.assertEqual(by['history']['transport'],'warehouse_bank');self.assertEqual(by['history']['bank']['pulled_at'],PULLED);self.assertIsNone(by['history']['http_status'])
        self.assertEqual(by['quote']['transport'],'provider');self.assertIsNone(by['quote']['bank'])
        self.assertEqual(p['price_observations'][0]['reported_quote']['price'],101);self.assertEqual(p['coverage']['measured_descriptive_fields'],7)
        self.assertTrue(any('warehouse-bank original' in s for s in p['limitations']))
        # the next run compares the bank's vintage against what was retained: bank pull time for warehouse originals, receipt for live
        vintages=ns['_positioning_previous_vintages'](mem.data[m.HEAD])
        self.assertEqual(vintages,{('SYNA','history'):PULLED,('SYNA','profile'):PULLED,('SYNA','quote'):AT})
        self.assertEqual(ns['_positioning_previous_vintages'](mem.previous),{});self.assertEqual(ns['_positioning_previous_vintages'](None),{})
        self.assertEqual(ns['_positioning_previous_vintages'](b'not json'),{})
    def test_real_bank_rung_answers_the_engines_exact_urls_from_a_fake_warehouse(self):
        import fmp_bank,fmp_bankfirst
        class S3:
            def __init__(self):self.store={};self.meta={};self.gets=[]
            def get_object(self,Bucket,Key):
                self.gets.append(Key)
                if Key not in self.store:raise KeyError(Key)
                return {'Body':BytesIO(self.store[Key]),'Metadata':self.meta.get(Key,{})}
        class Fixed(datetime):
            @classmethod
            def now(cls,tz=None):
                fixed=datetime(2026,9,29,1,0,tzinfo=timezone.utc)
                return fixed if tz else fixed.replace(tzinfo=None)
        s3=S3();fmp_bank._S3=s3;fmp_bank._cache.clear();fmp_bank._TABLES.clear();fmp_bank._MISSING.clear();fmp_bank._EVICTED.clear()
        fmp_bankfirst._reset_ctl(time.time());saved=fmp_bankfirst.datetime;fmp_bankfirst.datetime=Fixed
        try:
            W=fmp_bank.WALKER_ROOT;R=fmp_bank.ROOT
            rows=prices()+[dict(prices()[-1],date=ASOF.isoformat()),dict(prices()[-1],date='2026-06-01')]   # 65 complete bars, the as-of day, one outside the window
            s3.store[W+'historical-price-eod-full/SYNA.json.gz']=gzip.compress(json.dumps(rows).encode());s3.meta[W+'historical-price-eod-full/SYNA.json.gz']={'pulled_at':'2026-09-28'}
            s3.store[R+'profile-bulk/latest.csv.gz']=gzip.compress(b'symbol,companyName,currency,marketCap\nOTHER,Other,EUR,1\nSYNA,Synthetic,USD,5000000\n');s3.meta[R+'profile-bulk/latest.csv.gz']={'pulled_at':'2026-09-28T05:00:00Z'}
            s3.store[R+'manifest.json']=json.dumps({'updated_at':'2026-09-28T05:00:00Z','endpoints_served':['profile']}).encode()
            h=real_bank_originals.bank_original(m.endpoint('SYNA','history',ASOF))
            self.assertIsNotNone(h);dates=[r['date'] for r in json.loads(h['raw'])]
            self.assertEqual(len(dates),66);self.assertEqual((min(dates),max(dates)),((ASOF-timedelta(days=65)).isoformat(),ASOF.isoformat()),'window from/to applied, FMP shape, 2026-06-01 outside')
            self.assertEqual(h['bank']['object'],W+'historical-price-eod-full/SYNA.json.gz');self.assertEqual(h['bank']['pulled_at'],'2026-09-28T00:00:00+00:00')
            p=real_bank_originals.bank_original(m.endpoint('SYNA','profile',ASOF))
            self.assertIsNotNone(p);self.assertEqual(json.loads(p['raw']),[{'symbol':'SYNA','companyName':'Synthetic','currency':'USD','marketCap':5000000}])
            # a bulk-served head reports the bulk object itself (manifest reads are provenance-neutral in fmp_bank)
            self.assertEqual(p['bank']['object'],R+'profile-bulk/latest.csv.gz');self.assertEqual(p['bank']['pulled_at'],'2026-09-28T05:00:00+00:00')
            self.assertIsNone(real_bank_originals.bank_original(m.endpoint('SYNA','quote',ASOF)),'quotes are never banked')
            self.assertIsNone(real_bank_originals.bank_original(m.endpoint('NOPE','history',ASOF)))
            # the real build accepts what the real rung returns
            a,b,s=captures()
            for att in b:
                if att['kind'] in m.BANK_KINDS:
                    out=h if att['kind']=='history' else p
                    ref=store.identity(out['raw'],m.PRIVATE,'sources');s[ref['key']]=out['raw']
                    att.update(original_ref=ref,http_status=None,network_attempted=False,transport=out['transport'],bank=out['bank'])
            packet=m.build(a,b,s,AT);self.assertEqual(packet['bank_rung']['n_bank_originals'],2);self.assertEqual(packet['bank_rung']['n_live_originals'],1)
            self.assertEqual(packet['price_observations'][0]['reported_profile_currency']['value'],'USD')
        finally:
            fmp_bankfirst.datetime=saved;fmp_bank._cache.clear();fmp_bank._TABLES.clear();fmp_bank._MISSING.clear()


if __name__=='__main__':unittest.main(verbosity=2)
