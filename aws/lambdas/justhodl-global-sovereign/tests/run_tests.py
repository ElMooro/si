"""Complete history, correct calendar comparisons, and storage-failure boundaries."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
from io import BytesIO
from copy import deepcopy
import ast,json,math,re,sys,types,unittest,urllib.request
HERE=Path(__file__).resolve().parent;sys.path.insert(0,str(HERE.parent/'source'))
import sovereign_history as pub
NOW=datetime(2026,9,26,22,0,tzinfo=timezone.utc)

class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}

class Memory:
    def __init__(self):self.rows={};self.version={};self.writes=[];self.fail=None;self.after=None
    def seed(self,key,raw):self.rows[key]=raw;self.version[key]=self.version.get(key,0)+1
    def get_object(self,Bucket,Key):
        if Key==self.fail:raise Error('AccessDenied')
        if Key not in self.rows:raise Error('NoSuchKey')
        return {'Body':BytesIO(self.rows[Key]),'ETag':str(self.version[Key])}
    def put_object(self,Bucket,Key,Body,**kw):
        if kw.get('IfNoneMatch')=='*' and Key in self.rows:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=str(self.version.get(Key)):raise Error('PreconditionFailed')
        self.seed(Key,Body);self.writes.append(Key)
        if self.after:self.after(Key)

def fixture(n=3):
    m=Memory();history=[{'date':(NOW.date()-timedelta(days=n-i)).isoformat(),'stress':i%80,'retain':'unchanged'} for i in range(n)]
    m.seed(pub.HISTORY,pub.encode(history));m.seed(pub.HEAD,pub.encode({'generated_at':(NOW-timedelta(days=1)).isoformat(),'legacy':True}))
    return m,history

def payload(history):
    return {'generated_at':NOW.isoformat(),'eurodollar_hub_history':history,
            **{k:False for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')}}

class Frozen(datetime):
    @classmethod
    def now(cls,tz=None):return NOW

def handler(m):
    source=(HERE.parent/'source/lambda_function.py').read_text(encoding='utf-8');tree=ast.parse(source)
    env={'sovereign_history':pub,'s3':m,'json':json,'re':re,'math':math,'datetime':Frozen,'timezone':timezone,
         'timedelta':timedelta,'urllib':urllib,'time':types.SimpleNamespace(time=lambda:1,sleep=lambda n:None)}
    nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.Assign)) and not (isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='s3' for t in n.targets))]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual_sovereign_handler','exec'),env)
    return env

class Tests(unittest.TestCase):
    def test_actual_handler_preserves_all_rows_and_compares_exact_calendar_dates(self):
        m,history=fixture(1105);env=handler(m)
        env['wgb_country']=lambda slug:{'bond10y_pct':3,'cds_bp':40,'spread_vs_bund_bp':20,'as_of':'provider date unverified'}
        env['lambda_handler']();out=json.loads(m.rows[pub.HEAD]);saved=json.loads(m.rows[pub.HISTORY])
        self.assertEqual(saved[:-1],history);self.assertEqual(len(saved),1106);self.assertEqual(out['eurodollar_hub_history'],saved)
        old={r['date']:r for r in history}
        for days in (7,30):
            prior=old[(NOW.date()-timedelta(days=days)).isoformat()]['stress']
            self.assertEqual(out['eurodollar_hub_chg_'+str(days)+'d'],round(out['eurodollar_hub_stress_0_100']-prior,1))
        self.assertEqual(out['quality']['status'],'unverified');self.assertIs(out['calls_eligible'],False)
        record=json.loads(m.rows[out['publication_record']['manifest_key']])
        self.assertEqual(m.rows[record['predecessors'][pub.HISTORY]['key']],pub.encode(history))
        self.assertEqual(pub.sha(pub.encode({k:v for k,v in out.items() if k!='publication_record'})),out['publication_record']['output_sha256'])

    def test_missing_calendar_target_is_not_replaced_by_positional_history(self):
        m,history=fixture(100);history=[r for r in history if r['date']!=(NOW.date()-timedelta(days=7)).isoformat()]
        m.seed(pub.HISTORY,pub.encode(history));env=handler(m)
        env['wgb_country']=lambda slug:{'bond10y_pct':3,'cds_bp':40,'spread_vs_bund_bp':20}
        env['lambda_handler']();out=json.loads(m.rows[pub.HEAD])
        self.assertIsNone(out['eurodollar_hub_chg_7d']);self.assertIsNotNone(out['eurodollar_hub_chg_30d'])

    def test_denial_and_corrupt_history_fail_before_provider_work(self):
        for corrupt in (None,b'{}',b'[',b'[{"date":"2026-09-25","stress":NaN}]',b'[{"date":"2026-09-25","date":"2026-09-24"}]'):
            m,_=fixture();env=handler(m);calls=[];env['wgb_country']=lambda slug:calls.append(slug)
            if corrupt is None:m.fail=pub.HISTORY
            else:m.seed(pub.HISTORY,corrupt)
            with self.assertRaises(Exception):env['lambda_handler']()
            self.assertEqual(calls,[]);self.assertEqual(m.writes,[])

    def test_source_nonfinite_and_boolean_values_cannot_poison_json(self):
        m,_=fixture();env=handler(m);env['_http']=lambda *a:'var jsGlobalVars = {"country":1};'
        raw=json.dumps({'success':True,'bond10y':'NaN','lastCds':'0','lastCdsDefaultProb':True,'mainSpreadValue':'Infinity','cbRateNumber':0}).encode()
        old=urllib.request.urlopen;urllib.request.urlopen=lambda *a,**kw:BytesIO(raw)
        try:row=env['wgb_country']('fixture')
        finally:urllib.request.urlopen=old
        self.assertIsNone(row['bond10y_pct']);self.assertIsNone(row['cds_default_prob_pct']);self.assertIsNone(row['spread_vs_bund_bp'])
        self.assertEqual(row['cds_bp'],0);self.assertEqual(row['cb_rate_pct'],0)

    def test_future_duplicate_or_unversioned_predecessors_and_corrupt_retention_are_rejected(self):
        for mode in ('future_head','naive_head','future_history','duplicate_history','unversioned'):
            m,hist=fixture()
            if mode=='future_head':m.seed(pub.HEAD,pub.encode({'generated_at':(NOW+timedelta(seconds=1)).isoformat()}))
            if mode=='naive_head':m.seed(pub.HEAD,b'{"generated_at":"2026-09-25T12:00:00"}')
            if mode=='future_history':hist.append({'date':'2026-09-27'})
            if mode=='duplicate_history':hist.append(hist[0])
            m.seed(pub.HISTORY,pub.encode(hist))
            if mode=='unversioned':
                get=m.get_object
                m.get_object=lambda **kw:{k:v for k,v in get(**kw).items() if k!='ETag'}
            with self.assertRaises(ValueError):pub.begin(m,'b',NOW.isoformat())
            self.assertEqual(m.writes,[])
        m=Memory();raw=b'whole';m.seed(pub.PRIVATE+pub.sha(raw)+'.bin',b'corrupt')
        with self.assertRaises(ValueError):pub.retain(m,'b',raw)

    def test_dropped_changed_backfilled_or_forged_authority_cannot_publish(self):
        for mode in ('drop','change','backfill','authority','nonfinite','display'):
            m,old=fixture();state=pub.begin(m,'b',NOW.isoformat());new=deepcopy(old)+[{'date':NOW.date().isoformat(),'stress':0}];doc=payload(new)
            if mode=='drop':new.pop(0)
            if mode=='change':new[0]['retain']=False
            if mode=='backfill':new.append({'date':'2020-01-01','stress':0})
            if mode=='authority':doc['sizing_eligible']=True
            if mode=='nonfinite':doc['bad']=float('nan')
            if mode=='display':doc['eurodollar_hub_history']=[]
            with self.assertRaises(ValueError):pub.publish(m,'b',state,doc,new)
            self.assertEqual(m.writes,[])

    def test_concurrent_write_is_not_overwritten_and_partial_commit_is_not_rolled_back(self):
        for before in (True,False):
            m,old=fixture();state=pub.begin(m,'b',NOW.isoformat());new=old+[{'date':NOW.date().isoformat(),'stress':0}]
            concurrent=pub.encode({'generated_at':(NOW+timedelta(seconds=1)).isoformat(),'newer':True})
            if before:m.seed(pub.HEAD,concurrent)
            else:m.after=lambda key:m.seed(pub.HEAD,concurrent) if key==pub.HISTORY else None
            with self.assertRaises((ValueError,Error)):pub.publish(m,'b',state,payload(new),new)
            self.assertEqual(m.rows[pub.HEAD],concurrent)
            self.assertEqual(json.loads(m.rows[pub.HISTORY]),old if before else new)

    def test_empty_absent_history_and_same_day_replacement_retain_predecessors(self):
        for absent in (True,False):
            m=Memory()
            if not absent:m.seed(pub.HISTORY,b'[]')
            state=pub.begin(m,'b',NOW.isoformat());hist=[{'date':NOW.date().isoformat(),'stress':0}]
            ref=pub.publish(m,'b',state,payload(hist),hist);record=json.loads(m.rows[ref['key']])
            self.assertEqual(record['predecessors'][pub.HISTORY] is None,absent)
        m,hist=fixture();hist.append({'date':NOW.date().isoformat(),'stress':99});m.seed(pub.HISTORY,pub.encode(hist))
        state=pub.begin(m,'b',NOW.isoformat());new=hist[:-1]+[{'date':NOW.date().isoformat(),'stress':0}]
        ref=pub.publish(m,'b',state,payload(new),new);record=json.loads(m.rows[ref['key']])
        self.assertEqual(m.rows[record['predecessors'][pub.HISTORY]['key']],pub.encode(hist))

    def test_release_keeps_existing_rule_state_input_and_targets(self):
        sys.path.insert(0,str(HERE.parents[3]/'scripts'));from normalize_lambda_config import normalize_config
        conf=json.loads((HERE.parent/'config.json').read_bytes());normalized=normalize_config(conf)
        self.assertNotIn('schedule',normalized);self.assertEqual(normalized['release_schedule_note']['binding_action'],'PRESERVE_EXISTING')
        self.assertEqual(conf['schedule'],conf['preserved_schedule_reference']['cron'])

if __name__=='__main__':unittest.main()
