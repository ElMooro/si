"""Offline complete workbook, native entry, preservation and replay tests."""
from pathlib import Path
from io import BytesIO
from copy import deepcopy
from types import ModuleType,SimpleNamespace
from unittest.mock import patch
from xml.sax.saxutils import escape
import ast,importlib.util,json,sys,unittest,zipfile,urllib.error
ROOT=Path(__file__).resolve().parents[4];SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path.insert(0,str(SOURCE))
import air_store as store
import air_measurements as m
AT='2026-09-27T10:40:00+00:00'


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        self.data={store.HEAD:store.encode({'generated_at':'2026-09-26T10:40:00Z','version':'2.0.0','month':'2026-05','unknown':{'zero':0,'null':None,'false':False}}),store.LEVELS:store.encode({'levels':{'1997-12':0,'1998-01':1.2,'1998-02':None},'unknown_metadata':['preserve',None,0]})}
        self.reads=[];self.writes=[];self.denied=set();self.truncated=set();self.race=None
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key in self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=self.data[key];return {'Body':BytesIO(raw),'ContentLength':len(raw)+(key in self.truncated),'ETag':store.sha(raw)}
    def put_object(self,**kw):
        key=kw['Key'];raw=kw['Body']
        if self.race:self.race(key)
        if key in self.denied:raise Error('AccessDenied')
        old=self.data.get(key)
        if kw.get('IfNoneMatch')=='*' and old is not None:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and (old is None or kw['IfMatch']!=store.sha(old)):raise Error('PreconditionFailed')
        self.data[key]=raw;self.writes.append(key)


class Response(BytesIO):
    def __init__(self,raw,url,length=None):super().__init__(raw);self.url=url;self.headers={'Content-Length':str(len(raw) if length is None else length),'Content-Type':'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}
    def getcode(self):return 200
    def geturl(self):return self.url


def workbook(change=None):
    # All 344 real calendar positions, entirely synthetic freight values.
    rows={}
    def cell(address,value,kind='inlineStr',formula=None):
        col=''.join(c for c in address if c.isalpha());r=int(address[len(col):]);rows.setdefault(r,{})[col]={'value':str(value),'kind':kind,'formula':formula}
    for addr,text in m.EXPECTED.items():cell(addr,text)
    cell('A9','2026','n');cell('B9','#¶');cell('L9',100000,'n');cell('M9',200000,'n');cell('N9',300000,'n')
    for i in range(344):
        year=1998+i//12;month=i%12+1;r=20+i
        if month==1:cell('A'+str(r),year,'n')
        cell('B'+str(r),m.MONTHS[month-1][:3].title());cell('C'+str(r),'#' if year==2026 else '')
        cell('L'+str(r),100000+i,'n');cell('M'+str(r),200000+i,'n');cell('N'+str(r),300000+2*i,'n');cell('O'+str(r),'1.3','n')
    for i,note in enumerate(m.NOTES):cell('A'+str(400+i),note)
    if change:change(rows)
    text=[]
    for r,cols in sorted(rows.items()):
        vals=[]
        for col,v in sorted(cols.items()):
            a=f'{col}{r}';f='<f>'+escape(v['formula'])+'</f>' if v['formula'] else ''
            content='<is><t>'+escape(v['value'])+'</t></is>' if v['kind']=='inlineStr' else f+'<v>'+escape(v['value'])+'</v>'
            vals.append(f'<c r="{a}" t="{v["kind"]}">{content}</c>')
        text.append(f'<row r="{r}">'+''.join(vals)+'</row>')
    sheet=f'<worksheet xmlns="{m.NS}"><sheetData>'+''.join(text)+'</sheetData></worksheet>'
    output=BytesIO()
    with zipfile.ZipFile(output,'w',zipfile.ZIP_DEFLATED) as z:
        z.writestr('xl/workbook.xml',f'<workbook xmlns="{m.NS}" xmlns:r="{m.RNS}"><sheets><sheet name="Eng" sheetId="1" r:id="rId1"/></sheets></workbook>')
        z.writestr('xl/_rels/workbook.xml.rels',f'<Relationships xmlns="{m.PNS}"><Relationship Id="rId1" Target="worksheets/sheet1.xml" Type="{m.RNS}/worksheet"/></Relationships>')
        z.writestr('xl/sharedStrings.xml',f'<sst xmlns="{m.NS}" uniqueCount="0"></sst>')
        z.writestr('xl/worksheets/sheet1.xml',sheet)
    return output.getvalue()


class Tests(unittest.TestCase):
    def execute(self,mem,raw=None,opener=None):
        raw=raw or workbook();calls=[]
        def default(req,timeout):calls.append(req.full_url);self.assertEqual(timeout,25);return Response(raw,req.full_url)
        result=store.run(mem,'b',at=AT,opener=opener or default)
        return result,calls
    def test_full_calendar_abbreviations_values_and_native_replay(self):
        mem=Memory();before=deepcopy(mem.data);raw=workbook();result,calls=self.execute(mem,raw)
        self.assertEqual(len(calls),1);self.assertEqual(result['month'],'2026-08');self.assertEqual(result['monthly_observations'],344)
        packet=store.strict(mem.data[store.HEAD]);review=packet['measurement_review'];self.assertEqual(review['annual_count'],1)
        self.assertEqual(review['monthly_observations'][-1]['row'],363);self.assertTrue(review['monthly_observations'][-1]['provisional'])
        self.assertEqual(packet['unknown'],{'zero':0,'null':None,'false':False});self.assertFalse(packet['sizing_eligible']);self.assertEqual(packet['portfolio_action'],'WAIT')
        self.assertEqual(review['monthly_observations'][18]['comparison_status'],'airport_transition_comparability_unverified')
        levels=store.strict(mem.data[store.LEVELS]);self.assertEqual(len(levels['levels']),345);self.assertEqual(levels['levels']['1997-12'],0);self.assertEqual(levels['unknown_metadata'],['preserve',None,0])
        for original in before.values():self.assertIn(original,mem.data.values())
        self.assertIn(raw,mem.data.values());self.assertEqual(store.replay(mem,'b',packet)['monthly_observations'],344)
    def test_new_native_handler_routes_to_reviewed_store_without_legacy_fetch(self):
        mem=Memory();boto=ModuleType('boto3');boto.client=lambda *a,**kw:mem
        spec=importlib.util.spec_from_file_location('air_native',SOURCE/'lambda_function.py');native=importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules,{'boto3':boto}):spec.loader.exec_module(native)
        with patch.object(store,'run',return_value={'reviewed':True}) as call:
            self.assertEqual(native.lambda_handler({},None),{'reviewed':True});self.assertIs(call.call_args.args[0],mem)
    def test_exact_missing_year_month_zero_and_published_yoy_are_not_conflated(self):
        def mutate(rows):
            rows[363]['N']['value']='0';rows[363]['O']['value']='-1.3';del rows[351]
        review=m.measure(workbook(mutate),AT);latest=review['monthly_observations'][-1]
        self.assertEqual(latest['total'],0);self.assertEqual(latest['reported_yoy_pct'],-1.3);self.assertIsNone(latest['yoy_from_published_levels_pct']);self.assertEqual(latest['comparison_status'],'prior_month_missing')
        self.assertEqual(review['monthly_count'],343)
        def absent(rows):del rows[363]['N']
        latest=m.measure(workbook(absent),AT)['monthly_observations'][-1]
        self.assertEqual(latest['month'],'2026-08');self.assertIsNone(latest['total']);self.assertEqual(latest['comparison_status'],'latest_missing')
    def test_year_only_from_year_column_not_flight_or_passenger_number(self):
        def irrelevant(rows):rows[363]['D']={'value':'2001','kind':'n','formula':None}
        self.assertEqual(m.measure(workbook(irrelevant),AT)['latest_month'],'2026-08')
        def bad(rows):del rows[20]['A']
        with self.assertRaises(m.WorkbookError):m.measure(workbook(bad),AT)
    def test_schema_scope_formula_duplicate_and_unknown_month_refuse_publication(self):
        mutations=[lambda rows:rows[7]['L'].update(value='Freight (dollars)'),lambda rows:rows[1]['A'].update(value='Another airport'),lambda rows:rows[363]['B'].update(value='Sept'),lambda rows:rows[363]['N'].update(formula='L363+M363'),lambda rows:rows[363]['B'].update(value='Jul'),lambda rows:rows[363]['N'].update(value='NaN'),lambda rows:rows[363]['N'].update(value='1e-999'),lambda rows:rows[363]['C'].update(value='?'),lambda rows:rows[400]['A'].update(value='mail included')]
        for change in mutations:
            with self.subTest(change=change):
                mem=Memory();before=deepcopy(mem.data)
                with self.assertRaises((ValueError,KeyError)):self.execute(mem,workbook(change))
                for key in before:self.assertEqual(mem.data[key],before[key])
    def test_rounding_gap_and_independent_yoy_remain_separate(self):
        def change(rows):rows[363]['N']['value']='300687';rows[351]['N']['value']='300000'
        latest=m.measure(workbook(change),AT)['monthly_observations'][-1]
        self.assertEqual(latest['loaded_plus_unloaded_minus_total_tonnes'],-1)
        self.assertAlmostEqual(latest['yoy_from_published_levels_pct'],.229);self.assertEqual(latest['reported_yoy_pct'],1.3)
    def test_whole_http_truncation_invalid_zip_and_redirect_never_write(self):
        for kind in ('truncated','redirect','not_zip'):
            mem=Memory();before=deepcopy(mem.data);raw=workbook()
            def opener(req,timeout):return Response(b'badzip' if kind=='not_zip' else raw,'https://other.invalid/' if kind=='redirect' else req.full_url,len(raw)+1 if kind=='truncated' else None)
            with self.assertRaises(ValueError):self.execute(mem,opener=opener)
            for key in before:self.assertEqual(mem.data[key],before[key])
    def test_transport_fallback_retains_attempts_but_429_does_not_retry(self):
        mem=Memory();calls=[];raw=workbook()
        def opener(req,timeout):
            calls.append(req.full_url)
            if len(calls)==1:raise TimeoutError('synthetic')
            return Response(raw,req.full_url)
        self.execute(mem,opener=opener);packet=store.strict(mem.data[store.HEAD]);self.assertEqual(packet['fetch_via'],'direct');self.assertEqual(store.replay(mem,'b',packet)['http_attempts'],2)
        mem=Memory();calls=[]
        def limited(req,timeout):calls.append(req.full_url);raise urllib.error.HTTPError(req.full_url,429,'limited',{},BytesIO(b'{"limited":true}'))
        with self.assertRaises(store.EvidenceError):self.execute(mem,opener=limited)
        self.assertEqual(len(calls),1);self.assertIn(b'{"limited":true}',mem.data.values())
    def test_predecessor_denial_missing_malformed_truncated_and_future_block_fetch(self):
        for mode in ('missing','denied','truncated','malformed','future'):
            mem=Memory()
            if mode=='missing':del mem.data[store.LEVELS]
            if mode=='denied':mem.denied.add(store.LEVELS)
            if mode=='truncated':mem.truncated.add(store.LEVELS)
            if mode=='malformed':mem.data[store.LEVELS]=b'{"levels":{},"levels":{}}'
            if mode=='future':mem.data[store.HEAD]=store.encode({'generated_at':'2030-01-01T00:00:00Z'})
            calls=[]
            with self.assertRaises(ValueError):self.execute(mem,opener=lambda *a,**kw:calls.append(1))
            self.assertEqual(calls,[])
    def test_retention_failure_and_concurrent_writes_preserve_both_heads(self):
        mem=Memory();raw=workbook();mem.denied.add(store.PRIVATE+store.sha(raw)+'.bin');before=deepcopy(mem.data)
        with self.assertRaises(store.EvidenceError):self.execute(mem,raw)
        for key in before:self.assertEqual(mem.data[key],before[key])
        for target in (store.LEVELS,store.HEAD):
            mem=Memory();before=deepcopy(mem.data);foreign=b'{"foreign":true}'
            def race(key):
                if key==target:mem.data[key]=foreign
            mem.race=race
            with self.assertRaises(Error):self.execute(mem)
            self.assertEqual(mem.data[target],foreign)
            if target==store.LEVELS:self.assertEqual(mem.data[store.HEAD],before[store.HEAD])
            else:self.assertGreater(len(store.strict(mem.data[store.LEVELS])['levels']),26)
    def test_replay_detects_output_permission_original_and_compiler_changes(self):
        mem=Memory();self.execute(mem);packet=store.strict(mem.data[store.HEAD])
        for key,value in [('tonnes',10),('sizing_eligible',True),('generated_at','2026-09-28T00:00:00Z')]:
            bad=deepcopy(packet);bad[key]=value
            with self.assertRaises(store.EvidenceError):store.replay(mem,'b',bad)
        plan=store.strict(store.retained(mem,'b',packet['publication_context']['manifest']));mem.data[plan['http_attempts'][-1]['original']['key']]=b'altered'
        with self.assertRaises(store.EvidenceError):store.replay(mem,'b',packet)
    def test_full_predecessor_functions_runtime_and_history_are_preserved(self):
        raw=(ROOT/'tests/fixtures/pre-freight-research-air-cargo.py.txt').read_bytes();self.assertEqual(store.sha(raw),'6fa370a2ae7f1082f110180abcf16aedfc71d1c04e76598d81405e78c613ea61')
        tree=ast.parse((SOURCE/'lambda_function.py').read_bytes());functions={n.name:n for n in tree.body if isinstance(n,ast.FunctionDef)}
        for n in ast.parse(raw).body:
            if isinstance(n,ast.FunctionDef):
                if n.name=='lambda_handler':n.name='_legacy_calculation'
                self.assertEqual(ast.dump(n),ast.dump(functions[n.name]))
        cfg=json.loads((SOURCE.parent/'config.json').read_bytes());self.assertEqual((cfg['memory'],cfg['timeout']),(1024,180));self.assertNotIn('schedule',cfg)


if __name__=='__main__':unittest.main(verbosity=2)
