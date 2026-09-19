"""Real consumer functions: research must not dilute or acquire a trading vote."""
import ast
from datetime import datetime, timezone, timedelta
import io, json, time, unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]


def functions(engine,names,scope):
    path=ROOT/f'aws/lambdas/justhodl-{engine}/source/lambda_function.py'
    tree=ast.parse(path.read_text(encoding='utf-8'))
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
    if len(nodes)!=len(names):raise AssertionError('production function missing')
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(path),'exec'),scope)
    return scope


class Tests(unittest.TestCase):
    def test_signal_board_excludes_abstention_from_denominator(self):
        class Storage:
            objects={}
            def put_object(self,**kw):self.objects[kw['Key']]=json.loads(kw['Body'])
            def get_object(self,**kw):raise KeyError('No previous posture; no event emission')
        s3=Storage();stamp=datetime.now(timezone.utc)
        packet={'generated_at':stamp.isoformat(),'calls_eligible':False,'usd':{'impulse_z':10},'us_money':{'z':10}}
        scope=functions('signal-board',{'lambda_handler','n_liquidity_inflection','n_us_money','clamp'},
            {'time':time,'datetime':datetime,'timezone':timezone,'timedelta':timedelta,'json':json,
             's3':s3,'S3_BUCKET':'fixture','OUT_KEY':'data/signal-board.json','STALE_HOURS':48,
             'SIG_LABEL':{1:'POSITIVE'},'guard_output':None,'read_json':lambda key:(packet,stamp)})
        scope['FEEDS']=[('Liquidity','macro','data/liquidity-inflection.json',scope['n_liquidity_inflection']),
                        ('M2','macro','data/liquidity-inflection.json',scope['n_us_money']),
                        ('Qualified test vote','macro','fixture',lambda d:(1,'fixture'))]
        result=scope['lambda_handler']({},None);self.assertEqual(result['statusCode'],200)
        out=s3.objects['data/signal-board.json'];self.assertEqual(out['n_live'],1);self.assertEqual(out['composite_signal'],1)
        self.assertTrue(all(row['signal'] is None for row in out['engines'][:2]))

    def test_risk_and_ranker_drop_unqualified_legacy_score(self):
        packet={'composite':{'liquidity_score':100,'regime':'EXPANDING'},'trajectory':{'heading':'EASING AHEAD'}}
        scope=functions('risk-regime',{'liquidity_block'},{'_read':lambda key:packet})
        self.assertIsNone(scope['liquidity_block']()[0])
        scope=functions('master-ranker',{'get_regime_context'},{'fetch_json':lambda key,**kw:packet if 'liquidity-inflection' in key else {},'REGIME_FORWARDS':{}})
        self.assertIsNone(scope['get_regime_context']()['liquidity_score'])


if __name__=='__main__':unittest.main()
