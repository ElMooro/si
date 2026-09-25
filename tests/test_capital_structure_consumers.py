from pathlib import Path
from io import BytesIO
from unittest.mock import Mock,patch
from datetime import datetime,timezone
import ast,hashlib,json,math,sys,time,types,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import capital_structure_context as gate
from test_capital_structure_context import packet

LEGACY={'tickers':{'ABC':{'ticker':'ABC','market_cap':10e9,'sh_yoy_pct':500,
    'sh_1y_cagr_pct':500,'buyback_net_yield_pct':20,'read':'EXTREME_DILUTION',
    'flags':['BUYBACK_BLUFF','INSIDER_CONVICTION']}},'call':'LONG','score':100}


def tree(name):
    return ast.parse((ROOT/'aws/lambdas'/name/'source/lambda_function.py').read_text(encoding='utf-8'))


def functions(name,names,scope):
    nodes=[n for n in tree(name).body if isinstance(n,ast.FunctionDef) and n.name in names]
    assert len(nodes)==len(names)
    exec(compile(ast.Module(body=nodes,type_ignores=[]),name,'exec'),scope)
    return scope


class Tests(unittest.TestCase):
    def test_all_whole_predecessors_survive_byte_for_byte(self):
        migration=json.loads((ROOT/'tests/fixtures/capital-structure-consumer-migration.json').read_bytes())
        self.assertEqual(len(migration['files']),6)
        for record in migration['files']:
            raw=(ROOT/record['predecessor']).read_bytes()
            self.assertEqual((len(raw),hashlib.sha256(raw).hexdigest()),(record['bytes'],record['sha256']))

    def test_short_book_and_opportunity_read_boundaries_exclude_legacy_and_native_votes(self):
        for function,reader in (('justhodl-short-book','rj'),('justhodl-opportunity-engine','load')):
            nodes=[n for n in ast.walk(tree(function)) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)
                and n.func.attr=='decision_view' and ast.unparse(n.func.value)=="__import__('capital_structure_context')"]
            self.assertEqual(len(nodes),1)
            for document in (LEGACY,packet(),None):
                out=eval(compile(ast.Expression(nodes[0]),function,'eval'),{reader:lambda key:document})
                self.assertEqual(out['tickers'],{});self.assertFalse(out['calls_eligible'])
                self.assertEqual(out['research_context']['independent_investment_votes'],0)

    def test_cannibals_cannot_emit_or_price_buyback_plans_from_unqualified_fields(self):
        for document in (LEGACY,packet()):
            client=Mock();emit=Mock(side_effect=AssertionError('No signal permitted'))
            price=Mock(side_effect=AssertionError('No price acquisition permitted'))
            scope={'json':json,'time':time,'datetime':datetime,'timezone':timezone,'VERSION':'test',
                'S3_BUCKET':'test','SRC_KEY':'data/share-flows.json','OUT_KEY':'data/cannibals.json',
                'BAD':{'BUYBACK_BLUFF'},'ETF_OF':{},'s3':client,'ddb':Mock(),
                'rj':lambda key:document if key=='data/share-flows.json' else {'stocks':[]},
                'log_signal':emit,'yprice':price}
            functions('justhodl-cannibals',{'plan','lambda_handler'},scope)['lambda_handler']({},None)
            body=json.loads(client.put_object.call_args.kwargs['Body'])
            self.assertEqual(body['plans'],[]);self.assertEqual(body['logged'],0)
            self.assertFalse(body['input_eligibility']['buyback_signal'])
            self.assertFalse(body['capital_structure_context']['buyback_signal_qualified'])
            emit.assert_not_called();price.assert_not_called()

    def test_comeback_neither_refetches_bad_dilution_proxy_nor_rewards_missing_data(self):
        universe=[{'Ticker':'X'+str(i),'Price':0} for i in range(2999)]
        universe.append({'Ticker':'ABC','Price':1,'52-Week Low':100,'52-Week High':-60,
            'Average Volume':1000000,'Volume':1000000,'Performance (Quarter)':10,
            'Performance (Half Year)':30,'SMA200':5,'SMA50':10,'Market Cap':10000000})
        finviz=types.ModuleType('finviz');finviz.fetch_custom=lambda:universe
        client=Mock();client.get_object.return_value={'Body':BytesIO(json.dumps(LEGACY).encode())}
        fallback=Mock(side_effect=AssertionError('No provider fallback permitted'))
        scope={'json':json,'math':math,'time':time,'datetime':datetime,'timezone':timezone,
            'BUCKET':'test','OUT_KEY':'data/comeback-screener.json','SCHEMA':'test','S3':client,
            'census_idx':lambda *args:{},'fmp_sh_cagr':fallback}
        functions('justhodl-comeback-screener',{'g','num','lambda_handler'},scope)
        with patch.dict(sys.modules,{'finviz':finviz}):scope['lambda_handler']({},None)
        body=json.loads(client.put_object.call_args.kwargs['Body']);fallback.assert_not_called()
        self.assertEqual(body['fmp_dilution_pulls'],0)
        self.assertEqual(body['boards']['dilution_traps'],[])
        row=body['boards']['confirmed'][0]
        self.assertIsNone(row['sh_1y_cagr_pct']);self.assertIsNone(row['dilution_fueled'])
        self.assertEqual(row['comeback_score'],71.7)  # no +10 "low dilution" bonus
        self.assertFalse(body['input_eligibility']['ownership_dilution'])
        helper=functions('justhodl-comeback-screener',{'fmp_sh_cagr'},{})['fmp_sh_cagr']
        self.assertIsNone(helper('ABC'))


if __name__=='__main__':unittest.main(verbosity=2)
