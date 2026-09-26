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
        import sys
        sys.path.insert(0,str(ROOT/'tests'))
        from signal_board_native_test_support import assert_abstention
        assert_abstention('data/liquidity-inflection.json', (
            {'calls_eligible':False,'usd':{'impulse_z':10},'us_money':{'z':10}},
            {'calls_eligible':True,'usd':{'impulse_z':10},'us_money':{'z':10}}))

    def test_risk_and_ranker_drop_unqualified_legacy_score(self):
        packet={'composite':{'liquidity_score':100,'regime':'EXPANDING'},'trajectory':{'heading':'EASING AHEAD'}}
        scope=functions('risk-regime',{'liquidity_block'},{'_read':lambda key:packet})
        self.assertIsNone(scope['liquidity_block']()[0])
        scope=functions('master-ranker',{'get_regime_context'},{'fetch_json':lambda key,**kw:packet if 'liquidity-inflection' in key else {},'REGIME_FORWARDS':{}})
        self.assertIsNone(scope['get_regime_context']()['liquidity_score'])


if __name__=='__main__':unittest.main()
