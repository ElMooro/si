from pathlib import Path
from io import BytesIO
from datetime import datetime,timezone,timedelta
from unittest.mock import patch
import ast,copy,hashlib,importlib.util,json,sys,unittest,urllib.request,statistics,time

ROOT=Path(__file__).resolve().parents[4];SRC=Path(__file__).resolve().parents[1]/'source'
sys.path[:0]=[str(SRC),str(ROOT/'tests')]
import bond_flow_store as store
import bond_flow as model
from test_bond_flow_candidate import packet

AT='2026-09-26T19:30:00Z'


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):self.data={};self.reads=[];self.writes=[]
    def get_object(self,Bucket,Key):
        self.reads.append(Key)
        if Key not in self.data:raise Error('NoSuchKey')
        return {'Body':BytesIO(self.data[Key])}
    def put_object(self,Bucket,Key,Body,**kw):
        self.writes.append(Key)
        if kw.get('IfNoneMatch')=='*' and Key in self.data:raise Error('PreconditionFailed')
        self.data[Key]=Body


def fixture():
    m=Memory();p=packet();p['by_etf']={};raw=store.encode(p);digest=store.sha(raw)
    output={'key':'data/etf-research/outputs/'+digest+'.json','sha256':digest,'bytes':len(raw)};m.data[output['key']]=raw
    compiler={}
    for name,h in store.UPSTREAM.items():
        path=ROOT/'aws/shared/evidence_store.py' if name=='evidence_store' else ROOT/'aws/lambdas/justhodl-etf-true-flows/source'/(name+'.py')
        key='data/etf-research/compilers/'+h+'.py';m.data[key]=path.read_bytes();compiler[name]={'key':key,'sha256':h}
    manifest={'contract':'etf-original-replay.v1','generated_at':p['generated_at'],'output':output,'output_sha256':digest,'compilers':compiler}
    raw=store.encode(manifest);key='data/etf-research/runs/'+store.sha(raw)+'.json';m.data[key]=raw
    p['replay']={'manifest_key':key,'output_sha256':digest};m.data[store.SOURCE]=store.encode(p)
    return m,p


class Tests(unittest.TestCase):
    def test_complete_native_seal_replay_compiler_identity_and_no_current_write(self):
        m,p=fixture();out=store.collect(m,'bucket',AT)
        self.assertEqual(store.replay(out,store.reader(m,'bucket')),{k:v for k,v in out.items() if k!='replay'})
        self.assertEqual(out['arithmetic_checks']['members_checked'],138)
        self.assertIs(out['upstream_binding']['issuer_originals_replayed_here'],False)
        self.assertTrue(all(key.startswith(store.PREFIX) for key in m.writes))
        self.assertEqual(out['decision']['verb'],'WAIT')
        for row in store.compatibility(out).values():
            self.assertIsNone(row['flow_5d_usd']);self.assertIsNone(row['flow_21d_usd']);self.assertEqual(row['n'],0)
        self.assertEqual((SRC/'bond_flow.py').read_bytes(),(ROOT/'aws/ops/checks/bond_flow_candidate.py').read_bytes())
        self.assertEqual((SRC/'verify_bond_flow.py').read_bytes(),(ROOT/'aws/ops/checks/verify_bond_flow.py').read_bytes())

    def test_corrupt_outputs_partial_input_wrong_code_and_permissions_fail_closed(self):
        for mutation in ('head','compiler','manifest','expired'):
            m,p=fixture()
            if mutation=='head':p['calls_eligible']=True;m.data[store.SOURCE]=store.encode(p)
            if mutation=='compiler':m.data[next(k for k in m.data if '/compilers/' in k)]=b'changed'
            if mutation=='manifest':m.data[p['replay']['manifest_key']]+=b' '
            with self.assertRaises(ValueError):store.collect(m,'bucket','2026-09-30T00:00:00Z' if mutation=='expired' else AT)
            self.assertEqual(m.writes,[])
        m,p=fixture();out=store.collect(m,'bucket',AT);tampered=copy.deepcopy(out);tampered['independent_votes']=False
        with self.assertRaises(ValueError):store.replay(tampered,store.reader(m,'bucket'))
        key=out['replay']['manifest_key'];m.data[key]=m.data[key][:-1]
        with self.assertRaises((ValueError,KeyError)):store.replay(out,store.reader(m,'bucket'))

    def test_retention_and_read_failures_are_not_missing_or_repaired_in_place(self):
        m,p=fixture();read=store.reader(m,'bucket')
        for key in ('data/portfolio.json','data/bond-desk-research/flows/inputs/../private.json'):
            with self.assertRaises(ValueError):read(key)
        self.assertEqual(m.reads,[])
        raw=b'whole';key=store.PREFIX+'inputs/'+store.sha(raw)+'.json';m.data[key]=b'partial'
        with self.assertRaises(ValueError):store.retain(m,'bucket',raw,'inputs')
        with patch.object(m,'get_object',side_effect=Error('AccessDenied')),self.assertRaises(Error):store.collect(m,'bucket',AT)
        with self.assertRaises(ValueError):store.strict(b'{"x":1,"x":2}')

    def test_actual_handler_with_missing_cohorts_cannot_create_zero_flows_or_call_ai(self):
        tree=ast.parse((SRC/'lambda_function.py').read_text(encoding='utf-8'));nodes=[]
        for node in tree.body:
            if isinstance(node,ast.FunctionDef):nodes.append(node)
            elif isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id in ('BUCKETS','WORLD_TILES','CRISES','HIST','ANALOG_KEY') for t in node.targets):nodes.append(node)
        m=Memory();scope={'s3':m,'BUCKET':'bucket','OUT':'data/bond-desk.json','json':json,'time':time,'urllib':urllib,'st':statistics,
            'datetime':datetime,'timezone':timezone,'timedelta':timedelta,'FRED_KEY':None,'FMP':None}
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual-bond-handler','exec'),scope)
        history={(datetime(2020,1,1)+timedelta(days=i)).date().isoformat():{'anxiety':i,'appetite':0,'eqbond':0} for i in range(600)}
        scope.update(_s3json=lambda key,d=None:copy.deepcopy(history) if key==scope['HIST'] else {},_fred=lambda *a:[],
            _fred_hist=lambda *a,**k:[],_crisis_analogs=lambda:{'crises':[]},_llm=lambda *a,**k:(_ for _ in ()).throw(AssertionError('AI must not be called')))
        with patch('urllib.request.urlopen',side_effect=RuntimeError('offline')):
            scope['lambda_handler']({},None)
        result=json.loads(m.data['data/bond-desk.json']);flows=result['regions']['us']['flows']
        self.assertEqual(result['decision']['verb'],'WAIT');self.assertIsNone(result['world_anxiety'])
        self.assertIsNone(result['regions']['em']['score']);self.assertIsNone(flows['duration_tilt']);self.assertIsNone(flows['equity_to_bond_5d_usd'])
        self.assertTrue(all(row['flow_5d_usd'] is None for row in flows['buckets'].values()))
        self.assertIsNone(result['ai_brief']);self.assertEqual(len(json.loads(m.data[scope['HIST']])),601)
        self.assertEqual(len(result['anxiety_history']),601)

if __name__=='__main__':unittest.main()
