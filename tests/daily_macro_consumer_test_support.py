"""Exercise the actual consumer code without notifications or private accounts."""
import ast
from datetime import datetime,timezone
import hashlib
import io
import json
from pathlib import Path
import sys
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
from daily_macro_model import CONTRACT,build,market_context
from research_brief_model import narrative_context
from test_daily_macro_model import auxiliary
from test_research_brief_model import source_packet,NOW
from tenor_research_model import public_summary


def functions(engine,names,scope):
    path=ROOT/'aws/lambdas'/('justhodl-'+engine)/'source/lambda_function.py'
    tree=ast.parse(path.read_text(encoding='utf-8'))
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
    assert len(nodes)==len(names)
    for n in nodes:n.decorator_list=[]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(path),'exec'),scope)
    return scope


class FixedDate(datetime):
    @classmethod
    def now(cls,tz=None):return datetime.fromisoformat(NOW)


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class MemoryS3:
    def __init__(self):self.objects={};self.puts=[]
    def get_object(self,Bucket,Key):
        if Key not in self.objects:raise Error('NoSuchKey')
        body=self.objects[Key]
        return {'Body':io.BytesIO(body),'ETag':hashlib.sha256(body).hexdigest()}
    def put_object(self,Bucket,Key,Body,**kwargs):
        old=self.objects.get(Key)
        if kwargs.get('IfNoneMatch') and old is not None:raise Error('PreconditionFailed')
        if kwargs.get('IfMatch') and (old is None or hashlib.sha256(old).hexdigest()!=kwargs['IfMatch']):raise Error('PreconditionFailed')
        self.objects[Key]=Body;self.puts.append(Key)


class Consumers(unittest.TestCase):
    def packet(self):return build(source_packet(),auxiliary(),NOW)

    def test_crypto_enricher_cannot_invent_risk_liquidity_or_sector(self):
        scope=functions('crypto-enricher',{'compute_market_intelligence'}, {'DAILY_CONTRACT':CONTRACT,'market_context':market_context})
        p=self.packet();p['khalid_index']['score']=100;p['risk_dashboard']['overall_risk']=0
        result=scope['compute_market_intelligence'](p)
        for key in ('ml_regime','risk_level','risk_score','liquidity','carry_risk','sector_regime'):
            self.assertIsNone(result[key],key)
        self.assertFalse(result['sizing_eligible'])

    def test_adaptive_preserves_history_and_never_notifies_or_blends_legacy_accuracy(self):
        s3=MemoryS3();s3.objects['data/report.json']=json.dumps(self.packet()).encode()
        old=b'{"generated_at":"2026-09-17T00:00:00Z","adaptive":{"score":90}}'
        s3.objects['data/khalid-adaptive.json']=old
        s3.objects['data/khalid-adaptive-history.json']=b'{"snapshots":[{"adaptive_score":90}]}'
        history=s3.objects['data/khalid-adaptive-history.json']
        scope=functions('khalid-adaptive',{'research_output','lambda_handler'},
                        {'s3':s3,'json':json,'hashlib':hashlib,'datetime':FixedDate,'timezone':timezone,
                         'S3_BUCKET':'test','S3_REPORT':'data/report.json','S3_KEY_OUT':'data/khalid-adaptive.json'})
        result=scope['lambda_handler']({'send_alerts':True},None)
        self.assertEqual(result['statusCode'],200)
        p=json.loads(s3.objects['data/khalid-adaptive.json'])
        self.assertIsNone(p['adaptive']['score']);self.assertIsNone(p['divergence']['score_delta'])
        self.assertEqual(s3.objects['data/khalid-adaptive-history.json'],history)
        self.assertEqual(s3.objects['data/khalid-adaptive/legacy-unvalidated/'+hashlib.sha256(old).hexdigest()+'.json'],old)
        self.assertEqual(len(s3.puts),2)

    def test_logger_warm_invocation_clears_old_regime_and_does_not_log_report(self):
        snapshot={'regime':'BULL','khalid_score':99}
        scope=functions('signal-logger',{'_capture_regime_snapshot'},
                        {'_REGIME_SNAPSHOT':snapshot,'fs3':lambda _:self.packet()})
        scope['_capture_regime_snapshot']();self.assertEqual(snapshot,{'regime':None,'khalid_score':None})
        scope['fs3']=lambda _:(_ for _ in ()).throw(ValueError('offline fixture'))
        snapshot.update(regime='BULL',khalid_score=99)
        scope['_capture_regime_snapshot']();self.assertEqual(snapshot,{'regime':None,'khalid_score':None})
        # Execute the actual report section, ending at the first unrelated feed.
        path=ROOT/'aws/lambdas/justhodl-signal-logger/source/lambda_function.py'
        node=next(n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        stop=next(i for i,n in enumerate(node.body) if isinstance(n,ast.Assign) and isinstance(n.value,ast.Call) and any(isinstance(a,ast.Constant) and a.value=='crypto-intel.json' for a in n.value.args))
        node.body=node.body[:stop]+[ast.Return(value=ast.Name(id='logged',ctx=ast.Load()))];node.decorator_list=[]
        module=ast.fix_missing_locations(ast.Module(body=[node],type_ignores=[]))
        scope.update(fs3=lambda _:self.packet(),log_sig=lambda *a,**k:self.fail('research report became a forecast'))
        exec(compile(module,str(path),'exec'),scope)
        self.assertEqual(scope['lambda_handler']({},None),[])

    def test_ai_chat_gets_dated_research_without_a_null_score_label(self):
        packet=self.packet()
        scope=functions('ai-chat',{'build_context'},{'datetime':FixedDate,'timezone':timezone,'json':json,
                       'research_brief_context':narrative_context,'tenor_research_summary':public_summary,
                       'detect_entities':lambda _:([],[]),'get_s3':lambda key:packet if key=='data/report.json' else {}})
        text=scope['build_context']('Show macro measurements')
        self.assertNotIn('[KHALID INDEX]',text)
        line=next(row for row in text.splitlines() if row.startswith('[DAILY REPORT MACRO'))
        result=json.loads(line.split('] ',1)[1]);self.assertFalse(result['sizing_eligible'])
        claims=next(r for r in result['observations'] if r['series_id']=='ICSA')
        self.assertEqual((claims['value'],claims['unit']),('196000','Number'))

    def test_crisis_matching_does_not_substitute_50_or_interpret_high_ki_as_growth_scare(self):
        s3=MemoryS3()
        scope=functions('crisis-knowledge-base',{'get_current_state'},
                        {'s3':s3,'json':json,'S3_BUCKET':'test','datetime':FixedDate,'timezone':timezone,
                         'fetch_fred':lambda *a,**k:[], 'CRISIS_PATTERNS':[{'id':'growth_scare','name':'Growth scare'}]})
        for score in (None,0,90):
            p=self.packet();p['khalid_index']['score']=score;s3.objects['data/report.json']=json.dumps(p).encode()
            result=scope['get_current_state']()
            self.assertEqual(result['active_patterns'],[])
            self.assertEqual(result['indicators']['khalid_index']['value'],score)
            self.assertFalse(result['indicators']['khalid_index']['calls_eligible'])


def run(engine):
    cases={'crypto-enricher':'test_crypto_enricher_cannot_invent_risk_liquidity_or_sector',
           'khalid-adaptive':'test_adaptive_preserves_history_and_never_notifies_or_blends_legacy_accuracy',
           'signal-logger':'test_logger_warm_invocation_clears_old_regime_and_does_not_log_report',
           'ai-chat':'test_ai_chat_gets_dated_research_without_a_null_score_label',
           'crisis-knowledge-base':'test_crisis_matching_does_not_substitute_50_or_interpret_high_ki_as_growth_scare'}
    suite=unittest.TestSuite([Consumers(cases[engine])])
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():raise SystemExit(1)


if __name__=='__main__':unittest.main()
