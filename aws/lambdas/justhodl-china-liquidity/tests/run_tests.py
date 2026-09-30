"""Isolated native entry, full retained originals, definitions and calendar replay."""
from pathlib import Path
from copy import deepcopy
from datetime import date, timedelta
from io import BytesIO
from types import ModuleType, SimpleNamespace
from unittest.mock import patch
import ast, importlib.util, json, sys, unittest, urllib.parse

ROOT=Path(__file__).resolve().parents[4];SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path.insert(0,str(SOURCE));import china_store as store;import china_measurements as model
AT='2026-09-28T14:30:00+00:00'


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        history=[{'ts':(date(2025,1,1)+timedelta(days=i)).isoformat()+'T14:30:00Z',
                  'regime':'PRIOR_UNQUALIFIED','m2_yoy':i,'old_extra':[0,False,None]} for i in range(300)]
        self.data={store.HEAD:store.encode({'generated_at':'2026-09-25T14:31:28Z','unknown':[0,False,None],
                                          'money':{'old_nested':42},'tsf':{'old_method':'kept'}}),
                   store.KEYS[1]:store.encode({'snapshots':history,'old_history_field':42}),
                   store.KEYS[2]:store.encode({'reports':{str(i):{'old':i} for i in range(50)},'unknown':False})}
        self.reads=[];self.writes=[];self.denied=set();self.truncated=set();self.race=None;self.corrupt=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key in self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=self.data[key]
        if self.corrupt and key.startswith(store.PRIVATE):raw+=b'!'
        return {'Body':BytesIO(raw),'ContentLength':len(raw)+(key in self.truncated),'ETag':store.sha(raw)}
    def put_object(self,**kw):
        key=kw['Key'];raw=kw['Body']
        if self.race:self.race(key)
        if key in self.denied:raise Error('AccessDenied')
        old=self.data.get(key)
        if kw.get('IfNoneMatch')=='*' and old is not None:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and (old is None or store.sha(old)!=kw['IfMatch']):raise Error('PreconditionFailed')
        self.data[key]=raw;self.writes.append(key)


class Response(BytesIO):
    def __init__(self,raw,url,status=200):
        super().__init__(raw);self.url=url;self.status=status;self.headers={'Content-Length':str(len(raw))}
    def getcode(self):return self.status
    def geturl(self):return self.url


def packets(sid):
    concept,unit,freq,adjustment,reviewed=model.PROFILES[sid]
    meta={'seriess':[{'id':sid,'units':unit,'frequency_short':freq,'seasonal_adjustment_short':adjustment}]}
    if freq=='D':labels=[(date(2026,9,25)-timedelta(days=i)).isoformat() for i in range(1000)][::-1]
    else:
        latest='2018-10-01' if freq=='Q' else '2019-08-01' if concept=='M2' else '2018-12-01' if concept in ('M1','M3') else '2026-08-01'
        labels=[model.shift(latest,-i*(3 if freq=='Q' else 1)) for i in range(160 if freq=='Q' else 500)][::-1]
    rows=[{'date':label,'value':str(10000+i),'realtime_start':AT[:10],'realtime_end':AT[:10]} for i,label in enumerate(labels)]
    return meta,{'count':len(rows),'offset':0,'units':'lin','output_type':1,'observations':rows}


class Tests(unittest.TestCase):
    def test_failure_stages_bind_retained_attempts_and_never_emit_exception_payloads(self):
        for name,error,stage in [('compiler_hashes',store.EvidenceError('Whole source acquisition or retention refused'),'identify_compilers'),
                                 ('publications',RuntimeError('fixture-only-secret source-body-do-not-emit'),'project_outputs')]:
            with patch.object(store,name,side_effect=error):
                with self.assertRaises(type(error)) as caught:self.execute()
            diagnostic=caught.exception.research_failure
            self.assertEqual(diagnostic['stage'],stage)
            self.assertGreater(diagnostic['retained_attempts'],0)
            self.assertEqual(set(diagnostic['staged_outputs']),set(store.KEYS))
            self.assertRegex(diagnostic['last_attempt_sha256'],r'^[a-f0-9]{64}$')
            self.assertNotIn('_session',diagnostic)
            self.assertNotIn('fixture-only-secret',str(diagnostic));self.assertNotIn('source-body-do-not-emit',str(diagnostic))
            if name=='publications':self.assertEqual(diagnostic['reason'],'Unclassified; raw exception withheld')

    def test_original_store_measurements_and_history_are_unchanged_by_failure_observation(self):
        raw=(ROOT/'tests/fixtures/pre-china-failure-stages-store.py.txt').read_bytes()
        self.assertEqual(store.sha(raw),'5921cab4edc391fb0bf0a5145fd418f8cd5f5a58ae51f68b231a11e4e46c7ef2')
        self.assertEqual(store.sha((ROOT/'tests/fixtures/pre-china-failure-stages-handler.py.txt').read_bytes()),'0c7bf8ec3dddb9feb443d3afaa281a6dfbb4865797e7db1c7ba4165bdc86dc03')
        old=ModuleType('original_china_store');old.__file__=str(SOURCE/'china_store.py');exec(compile(raw,old.__file__,'exec'),old.__dict__)
        memory=Memory();self.calls=[];self.module.s3=memory;self.module.FRED_KEY='fixture-only-secret'
        with patch.object(self.module,'os',SimpleNamespace(environ={})):
            old.run(self.module,at=AT,opener=self.source)
        prior=store.strict(memory.data[store.HEAD]);history=memory.data[store.KEYS[1]]
        current,packet,_=self.execute()
        prior.pop('publication_context');packet.pop('publication_context')
        self.assertEqual(packet,prior)
        self.assertEqual(current.data[store.KEYS[1]],history)

    @classmethod
    def setUpClass(cls):
        fake=ModuleType('boto3');fake.client=lambda *a,**kw:None
        spec=importlib.util.spec_from_file_location('china_native_test',SOURCE/'lambda_function.py')
        cls.module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=cls.module
        fake_os=ModuleType('os');fake_os.environ={}
        with patch.dict(sys.modules,{'boto3':fake,'os':fake_os}):spec.loader.exec_module(cls.module)
    def source(self,req,timeout=None):
        url=req if isinstance(req,str) else req.full_url;self.calls.append(url)
        p=urllib.parse.urlsplit(url);q=dict(urllib.parse.parse_qsl(p.query))
        if p.netloc=='api.stlouisfed.org':
            meta,obs=packets(q['series_id']);raw=store.encode(meta if p.path.endswith('/series') else obs)
        elif p.netloc=='api.db.nomics.world':
            raw=store.encode({'series':{'docs':[{'series_code':'annual','series_name':'unverified annual composition','period':['2024','2025'],'value':[0,100]}]}})
        else:
            target=urllib.parse.urlsplit(q['u']) if 'u' in q else p
            self.assertEqual(target.scheme,'https');self.assertIn(target.netloc,('www.pbc.gov.cn','pbc.gov.cn'))
            if target.path.endswith('/index.html') and '/en/' in target.path:
                raw=b'<a href="/en/report.html">2026 Aggregate financing (flow)</a>'
            elif target.path=='/en/report.html':
                raw=b'<table><tr><td>Item</td><td>Jan</td><td>Feb</td><td>Mar</td></tr><tr><td>Aggregate financing</td><td>0</td><td>120</td><td>130</td></tr><tr><td>Other</td><td>1</td><td>2</td><td>3</td></tr></table>'
            elif 'index' in target.path:
                raw='<a href="/cn/report.html">2026年8月社会融资规模增量统计数据报告</a>'.encode()+b' '*501000
            else:raw='2026年8月社会融资规模增量为1.2万亿元，比上年同期少0.1万亿元'.encode()+b' '*401
        return Response(raw,url)
    def execute(self,m=None,opener=None,at=AT,monthly=None):
        m=m or Memory();self.calls=[];self.module.s3=m;self.module.FRED_KEY='fixture-only-secret'
        with patch.object(self.module,'os',SimpleNamespace(environ={'NBS_TSF_MONTHLY':monthly} if monthly else {})):
            result=store.run(self.module,at=at,opener=opener or self.source)
        return m,store.strict(m.data[store.HEAD]),result
    def test_original_functions_preserved_except_scoped_transport(self):
        raw=(ROOT/'tests/fixtures/pre-china-liquidity-research.py.txt').read_bytes()
        self.assertEqual(store.sha(raw),'40f1718635438159971eb1303ab9753bc179210ce3a0d57eb2e5a61f967919a7')
        before={n.name:n for n in ast.parse(raw).body if isinstance(n,ast.FunctionDef)}
        after={n.name:n for n in ast.parse((SOURCE/'lambda_function.py').read_bytes()).body if isinstance(n,ast.FunctionDef)}
        for name,node in before.items():
            if name in ('_edge','_html'):continue
            other=deepcopy(after['_legacy_lambda_handler' if name=='lambda_handler' else name]);other.name=name
            self.assertEqual(ast.dump(node),ast.dump(other),name)
        with patch.object(store,'run',return_value={'entry':True}) as run:
            self.assertEqual(self.module.lambda_handler({},None),{'entry':True});self.assertIs(run.call_args.args[0],self.module)
        source=(SOURCE/'lambda_function.py').read_text(encoding='utf-8')
        self.assertNotIn('import _fred_shim',source)
        self.assertNotIn('os.environ.get("TELEGRAM',source)
    def test_full_native_entry_all_sources_history_caches_and_offline_replay(self):
        original=Memory();old=deepcopy(original.data);m,p,result=self.execute(original)
        self.assertTrue(result['published']);self.assertEqual(p['regime'],'WAIT');self.assertFalse(p['calls_eligible'])
        self.assertEqual(p['unknown'],[0,False,None]);self.assertEqual(p['money']['old_nested'],42);self.assertEqual(p['tsf']['old_method'],'kept')
        history=store.strict(m.data[store.KEYS[1]]);self.assertEqual(len(history['snapshots']),301)
        self.assertEqual(history['snapshots'][:-1],store.strict(old[store.KEYS[1]])['snapshots']);self.assertEqual(history['old_history_field'],42)
        self.assertEqual(len(store.strict(m.data[store.KEYS[2]])['reports']),51)
        self.assertFalse(store.strict(m.data[store.KEYS[2]])['unknown'])
        self.assertIsNone(p['money']['m1_yoy_pct']);self.assertIsNone(p['money']['m2_yoy_pct']);self.assertIsNone(p['credit_impulse']['value_pp'])
        self.assertIsNone(p['dr_copper']['copper_gold_ratio']);self.assertEqual(len(p['measurement_review']['series']),11)
        plan=store.strict(store.retained(m,self.module.S3_BUCKET,p['publication_context']['manifest']))
        self.assertEqual(len(plan['native_outputs']),3);self.assertEqual(plan['credential_presence'],[True]);self.assertEqual(plan['notifications_suppressed'],1)
        self.assertTrue(any(r.get('original',{}).get('bytes',0)>500000 for r in plan['http_attempts']))
        for row in plan['http_attempts']:self.assertNotIn('fixture-',str(row))
        before=len(self.calls);r=store.replay(self.module,m,self.module.S3_BUCKET,p)
        self.assertEqual(len(self.calls),before);self.assertEqual(r['provider_requests'],0);self.assertEqual(r['china_observations'],5320)
        self.assertEqual(m.writes[-1],store.HEAD);self.assertFalse(p['publication_context']['multiple_head_atomic'])
    def test_monthly_and_quarterly_calendars_and_concepts_remain_distinct(self):
        for sid in ('MANMM101CNM189S','MANMM101CNQ189S','MYAGM2CNM189N','MABMM301CNM189S','MABMM301CNQ189S'):
            meta,obs=packets(sid);r=model.fred(sid,meta,obs,AT)
            self.assertEqual(r['status'],'measured');self.assertEqual(r['freshness'],'stale');self.assertFalse(r['current_measurement_eligible'])
            distance=4 if model.PROFILES[sid][2]=='Q' else 12
            self.assertAlmostEqual(r['yoy']['value'],(float(obs['observations'][-1]['value'])/float(obs['observations'][-1-distance]['value'])-1)*100)
            self.assertFalse(r['money_growth_acceleration']['is_credit_impulse'])
        self.assertEqual(model.fred('MABMM301CNM189S',*packets('MABMM301CNM189S'),AT)['concept'],'M3')
        self.assertEqual(model.fred('IQ12260',*packets('IQ12260'),AT)['concept'],'gold_export_price_index')
    def test_exact_period_holes_missing_zero_and_boolean_are_not_filled(self):
        sid='PCOPPUSDM';meta,obs=packets(sid)
        for case in ('hole','null','zero','latest','boolean'):
            copy=deepcopy(obs)
            if case=='hole':del copy['observations'][-13];copy['count']-=1
            elif case=='null':copy['observations'][-13]['value']='.'
            elif case=='zero':copy['observations'][-13]['value']='0'
            elif case=='latest':copy['observations'][-1]['value']='.'
            else:copy['observations'][-13]['value']=True
            out=model.fred(sid,meta,copy,AT)
            self.assertIsNone(out.get('yoy',{}).get('value'))
            self.assertEqual(len(out['observations']),copy['count'])
        for value in ('NaN','1_000','1e999','-1','1e-9999',True):
            with self.assertRaises(model.MeasurementError):model.number(value)
    def test_definition_changes_counts_future_duplicates_and_incomplete_periods(self):
        sid='PCOPPUSDM';meta,obs=packets(sid)
        for case in ('unit','frequency','adjustment','count','offset','transform','duplicate','future','incomplete'):
            mm=deepcopy(meta);oo=deepcopy(obs)
            if case=='unit':mm['seriess'][0]['units']='Index'
            elif case=='frequency':mm['seriess'][0]['frequency_short']='Q'
            elif case=='adjustment':mm['seriess'][0]['seasonal_adjustment_short']='SA'
            elif case in ('count','offset'):oo[case]+=1
            elif case=='transform':oo['units']='pch'
            elif case=='duplicate':oo['observations'][-1]=oo['observations'][-2]
            else:oo['observations'][-1]['date']='2099-01-01' if case=='future' else '2026-09-01'
            r=model.fred(sid,mm,oo,AT);self.assertFalse(r['current_measurement_eligible']);self.assertNotEqual(r['status'],'measured')
            self.assertEqual(r['observation_response'],oo)
    def test_daily_fx_anchor_and_missing_value_do_not_become_capital_flows(self):
        sid='DEXCHUS';meta,obs=packets(sid);r=model.fred(sid,meta,obs,AT)
        self.assertEqual(r['three_month']['comparison_date'],'2026-06-25')
        obs['observations']=[o for o in obs['observations'] if o['date']!='2026-06-25'];obs['count']-=1
        r=model.fred(sid,meta,obs,AT);self.assertEqual(r['three_month']['comparison_date'],'2026-06-24')
        next(o for o in obs['observations'] if o['date']=='2026-06-24')['value']='.'
        self.assertIsNone(model.fred(sid,meta,obs,AT)['three_month']['value'])
        self.assertEqual(model.shift('2024-02-29',-12),'2023-02-28')
    def test_denied_truncated_corrupt_or_newer_predecessor_has_no_provider_or_public_writes(self):
        for case in ('denied','truncated','corrupt','newer','malformed','missing_history'):
            m=Memory()
            if case=='denied':m.denied.add(store.HEAD)
            elif case=='truncated':m.truncated.add(store.KEYS[1])
            elif case=='corrupt':m.corrupt=True
            elif case=='newer':m.data[store.HEAD]=store.encode({'generated_at':'2099-01-01T00:00:00Z'})
            elif case=='malformed':m.data[store.KEYS[2]]=b'{malformed'
            else:del m.data[store.KEYS[1]]
            before=deepcopy(m.data)
            with self.assertRaises(Exception):self.execute(m)
            self.assertEqual(self.calls,[]);self.assertEqual({k:m.data[k] for k in before},before)
    def test_partial_response_redirect_or_failed_retention_keeps_all_original_heads(self):
        for case in ('length','redirect','retention'):
            m=Memory();before=deepcopy(m.data)
            def broken(req,timeout=None):
                response=self.source(req,timeout)
                if case=='length':response.headers['Content-Length']=str(int(response.headers['Content-Length'])+1)
                elif case=='redirect':response.url='https://foreign.invalid'
                else:m.corrupt=True
                return response
            with self.assertRaises(store.EvidenceError):self.execute(m,broken)
            self.assertEqual({k:m.data[k] for k in before},before)
    def test_rate_limit_is_not_retried_direct_or_through_proxy(self):
        attempts=[]
        def limited(req,timeout=None):
            url=req if isinstance(req,str) else req.full_url
            if 'pbc.gov.cn' in urllib.parse.unquote(url):attempts.append(url);return Response(b'limited',url,429)
            return self.source(req,timeout)
        m,p,_=self.execute(opener=limited);self.assertEqual(len(attempts),1)
        self.assertEqual(store.replay(self.module,m,self.module.S3_BUCKET,p)['provider_requests'],0)
    def test_provider_timeout_stays_unavailable_and_replays_without_retry(self):
        def failed(req,timeout=None):
            url=req if isinstance(req,str) else req.full_url
            if 'api.stlouisfed.org' in url:raise TimeoutError('fixture')
            return self.source(req,timeout)
        m,p,_=self.execute(opener=failed)
        self.assertEqual(p['quality']['current_series'],0);self.assertIsNone(p['currency']['usd_cny'])
        self.assertEqual(store.replay(self.module,m,self.module.S3_BUCKET,p)['provider_requests'],0)
    def test_undeclared_sources_private_reads_and_monthly_dataset_are_rejected(self):
        for url in ('http://private.invalid/a','https://169.254.169.254/latest','https://www.pbc.gov.cn.evil.invalid/a',
                    'https://api.db.nomics.world/v22/series/NBS/OTHER?limit=40&observations=1',
                    'https://justhodl-data-proxy.raafouis.workers.dev/gov?u=https%3A%2F%2Fprivate.invalid'):
            with self.assertRaises(store.EvidenceError):store.identity(url)
        session=store.Session(Memory(),'bucket',AT,{})
        with self.assertRaises(store.EvidenceError):session.get_object(Bucket='bucket',Key='private/account')
        with self.assertRaises(store.EvidenceError):self.execute(monthly='../secret')
    def test_tampered_public_packet_and_retained_body_cannot_replay(self):
        m,p,_=self.execute();bad=deepcopy(p);bad['currency']['usd_cny']=99
        with self.assertRaises(store.EvidenceError):store.replay(self.module,m,self.module.S3_BUCKET,bad)
        m.corrupt=True
        with self.assertRaises(store.EvidenceError):store.replay(self.module,m,self.module.S3_BUCKET,p)
    def test_foreign_writer_not_rolled_back_and_repeated_runs_preserve_all_rows(self):
        m,p,_=self.execute();first=m.data[store.HEAD]
        m,q,_=self.execute(m,at='2026-09-29T14:30:00+00:00')
        self.assertEqual(len(store.strict(m.data[store.KEYS[1]])['snapshots']),302)
        plan=store.strict(store.retained(m,self.module.S3_BUCKET,q['publication_context']['manifest']))
        self.assertEqual(store.retained(m,self.module.S3_BUCKET,plan['predecessors'][store.HEAD]['original']),first)
        self.assertEqual(store.replay(self.module,m,self.module.S3_BUCKET,q)['provider_requests'],0)
        foreign=b'{"generated_at":"2026-09-30T14:31:00Z","foreign":true}'
        def race(key):
            if key==store.HEAD:m.data[key]=foreign
        m.race=race
        with self.assertRaises(Error):self.execute(m,at='2026-09-30T14:30:00+00:00')
        self.assertEqual(m.data[store.HEAD],foreign)
    def test_http_entry_and_low_runtime_have_no_effect(self):
        m=Memory();self.module.s3=m
        self.assertEqual(self.module.lambda_handler({'httpMethod':'GET'},None)['statusCode'],409)
        self.assertEqual(m.reads,[]);self.assertEqual(m.writes,[])
        self.calls=[]
        with self.assertRaises(store.EvidenceError):store.run(self.module,context=SimpleNamespace(get_remaining_time_in_millis=lambda:10000),at=AT,opener=self.source)
        self.assertEqual(self.calls,[]);self.assertEqual(m.writes,[])
        session=store.Session(m,self.module.S3_BUCKET,AT,{},self.source);session.started-=81
        with self.assertRaises(store.EvidenceError):session.urlopen('https://www.pbc.gov.cn/en/index.html',timeout=10)
        self.assertEqual(self.calls,[]);self.assertEqual(session.http[0]['status'],'budget_not_attempted')
    def test_missing_afre_cache_and_optional_public_dataset_preserve_source_population(self):
        m=Memory();del m.data[store.KEYS[2]];m,p,_=self.execute(m,monthly='MONTHLY_TEST')
        self.assertEqual(len(store.strict(m.data[store.KEYS[2]])['reports']),1)
        self.assertEqual(store.replay(self.module,m,self.module.S3_BUCKET,p)['provider_requests'],0)

    def test_echoed_provider_credential_cannot_enter_public_outputs(self):
        m=Memory();before=deepcopy(m.data)
        def echoed(req,timeout=None):
            response=self.source(req,timeout)
            if '/fred/series?' in req.full_url:
                value=store.strict(response.getvalue());value['echo']='fixture-only-secret'
                return Response(store.encode(value),req.full_url)
            return response
        with self.assertRaises(store.EvidenceError):self.execute(m,echoed)
        self.assertEqual({key:m.data[key] for key in before},before)


    def outcome(self,printed):
        rows=[call for call in printed.call_args_list if call.args and call.args[0]==store.PUBLICATION_OUTCOME_PREFIX]
        self.assertEqual(len(rows),1);return store.strict(rows[0].args[1].encode('utf-8'))

    def test_publication_witness_binds_every_verified_output_without_changing_return(self):
        with patch.object(store,'print',create=True) as printed:m,p,result=self.execute()
        witness=self.outcome(printed)
        self.assertEqual(result,{'published':True,'contract':store.CONTRACT,'provider_attempts':len(self.calls),'portfolio_action':'WAIT','multiple_head_atomic':False})
        self.assertEqual(witness['contract'],'china-publication-outcome.v1');self.assertEqual(witness['status'],'producer_readbacks_verified')
        self.assertEqual(witness['calculation_at'],AT);self.assertEqual(witness['compiler_sha256'],store.compiler_hashes())
        self.assertEqual(witness['outputs'],[{'key':key,'bytes':len(m.data[key]),'sha256':store.sha(m.data[key])} for key in sorted(store.KEYS)])
        for key in ('multiple_head_atomic','independent_source_replay_verified','current_pointer_independently_verified','schedule_causation_verified','investment_authority'):self.assertIs(witness[key],False)
        self.assertNotIn('fixture-only-secret',str(witness));self.assertNotIn(store.PRIVATE,str(witness))
        self.assertIsNone(witness['request_id']);self.assertIsNone(witness['function_version'])

    def test_failed_or_partial_publication_never_emits_success_witness(self):
        for target in (store.KEYS[1],store.HEAD):
            m=Memory();before=deepcopy(m.data)
            def deny(key):
                if key==target:m.denied.add(key)
            m.race=deny
            with patch.object(store,'print',create=True) as printed:
                with self.assertRaises(Error):self.execute(m)
            self.assertFalse(printed.called);self.assertEqual(m.data[store.HEAD],before[store.HEAD])
            if target==store.HEAD:self.assertNotEqual(m.data[store.KEYS[1]],before[store.KEYS[1]])

    def test_log_sink_failure_does_not_change_verified_publication(self):
        with patch.object(store,'print',side_effect=OSError('invented log sink failure'),create=True):m,p,result=self.execute()
        self.assertTrue(result['published']);self.assertEqual(p['contract'],store.CONTRACT)
        self.assertEqual(len(store.strict(m.data[store.KEYS[1]])['snapshots']),301)

    def test_context_identity_is_bounded_and_never_leaks_unknown_fields(self):
        outputs={store.HEAD:b'{}',store.KEYS[1]:b'{}'};compilers=store.compiler_hashes()
        valid=SimpleNamespace(aws_request_id='12345678-1234-1234-1234-123456789abc',function_version='$LATEST',private='fixture-only-secret')
        with patch.object(store,'print',create=True) as printed:self.assertTrue(store._emit_publication_outcome(outputs,compilers,AT,1,valid))
        witness=self.outcome(printed);self.assertEqual(witness['request_id'],valid.aws_request_id);self.assertEqual(witness['function_version'],'$LATEST')
        bad=SimpleNamespace(aws_request_id='fixture-only-secret',function_version='fixture-only-secret')
        with patch.object(store,'print',create=True) as printed:self.assertTrue(store._emit_publication_outcome(outputs,compilers,AT,1,bad))
        witness=self.outcome(printed);self.assertIsNone(witness['request_id']);self.assertIsNone(witness['function_version']);self.assertNotIn('fixture-only-secret',str(witness))

    def test_malformed_witness_metadata_refuses_without_affecting_writer(self):
        outputs={store.HEAD:b'{}',store.KEYS[1]:b'{}'};compilers=store.compiler_hashes()
        cases=[({**outputs,'portfolio/snapshot.json':b'invented'},compilers,1),({store.HEAD:b'{}'},compilers,1),
          ({**outputs,store.HEAD:b''},compilers,1),(outputs,{**compilers,'unknown.py':'a'*64},1),(outputs,compilers,True),(outputs,compilers,65)]
        for raw,hashes,count in cases:
            with patch.object(store,'print',create=True) as printed:self.assertFalse(store._emit_publication_outcome(raw,hashes,AT,count,None))
            self.assertFalse(printed.called)

    def test_whole_predecessor_measurements_history_and_return_are_identical(self):
        raw=(ROOT/'tests/fixtures/pre-china-publication-outcome-store.py.txt').read_bytes()
        self.assertEqual(store.sha(raw),'e7ba58170bb1316057486783087d0f6999791fb3f88a43f0a64299af24fde98c')
        old=ModuleType('pre_outcome_store');old.__file__=str(SOURCE/'china_store.py');exec(compile(raw,old.__file__,'exec'),old.__dict__)
        first=Memory();self.calls=[];self.module.s3=first;self.module.FRED_KEY='fixture-only-secret'
        # Compare complete outputs at the same original acquisition clock.
        actual_datetime=store.datetime
        class Fixed(actual_datetime):
            @classmethod
            def now(cls,tz=None):
                at=actual_datetime.fromisoformat(AT)
                return at.astimezone(tz) if tz else at.replace(tzinfo=None)
        with patch.object(old,'datetime',Fixed),patch.object(store,'datetime',Fixed):
            with patch.object(self.module,'os',SimpleNamespace(environ={})):before=old.run(self.module,at=AT,opener=self.source)
            with patch.object(store,'print',create=True):after,packet,result=self.execute()
        self.assertEqual(before,result)
        for key in store.KEYS:self.assertEqual(first.data[key],after.data[key],key)


if __name__=='__main__':unittest.main()
