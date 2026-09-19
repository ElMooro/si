"""Exercise the actual changed consumer statements without account reads or live handlers."""
import ast
from datetime import datetime,timezone
from pathlib import Path
import unittest
from unittest.mock import patch
import reversal_consumer_context as boundary

ROOT=Path(__file__).resolve().parents[3]


def source(name):
    return ast.parse((ROOT/'aws/lambdas'/('justhodl-'+name)/'source/lambda_function.py').read_text(encoding='utf-8'))


def run(name):
    class Consumer(unittest.TestCase):
        def test_unvalidated_words_do_not_create_risk_or_direction(self):
            packet={'generated_at':datetime.now(timezone.utc).isoformat(),'rows':[{'symbol':'TVC:USOIL','last':50,'dod_pct':-10,'move_z':5}],
                    'liquidity':{'reversal_label':'CONFIRMED TURN TO TIGHTEN','trend_label':'EASING'}}
            result=boundary.context(packet)
            self.assertEqual(result['state'],'ABSTAIN');self.assertIsNone(result['trend_score']);self.assertFalse(result['calls_eligible'])

        def test_current_wrapper_never_grants_portfolio_authority(self):
            result=boundary.context({'contract':boundary.CONTRACT,'generated_at':datetime.now(timezone.utc).isoformat(),'calls_eligible':True})
            self.assertFalse(result['calls_eligible']);self.assertFalse(result['sizing_eligible'])

        def test_actual_consumer_wiring(self):
            tree=source(name);handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
            if name=='stock-buying':
                node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='fetch_us10y')
                scope={};exec(compile(ast.Module(body=[node],type_ignores=[]),'<actual consumer>','exec'),scope)
                scope.update(s3=object(),BUCKET='fixture')
                evidence={'value':0,'date':'2026-09-17','evidence':{'retained':True}}
                with patch.object(boundary,'native_yield',return_value=evidence):self.assertEqual(scope['fetch_us10y'](),0)
                self.assertEqual(scope['_US10Y_EVIDENCE'],evidence)
                with patch.object(boundary,'native_yield',return_value={'value':None}):self.assertIsNone(scope['fetch_us10y']())
            else:
                text=ast.unparse(handler)
                self.assertNotIn('"CONFIRMED" in',text);self.assertNotIn("'CONFIRMED' in",text)
                self.assertNotIn("z > 0",text)
                relevant=[n for n in handler.body if isinstance(n,ast.ImportFrom) and n.module=='reversal_consumer_context']
                self.assertEqual(len(relevant),1)
                if name=='catalyst':
                    node=next(n for n in handler.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='macro' for t in n.targets))
                    relevant.append(node);scope={'s3_json':lambda key:{'rows':[{'move_z':5,'dod_pct':-9}]}}
                    exec(compile(ast.Module(body=relevant,type_ignores=[]),'<actual consumer>','exec'),scope)
                    self.assertEqual(scope['macro']['commodity_inflections'],[]);self.assertIsNone(scope['macro']['rate_cuts'])
                else:
                    node=next(n for n in handler.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Subscript) and isinstance(t.slice,ast.Constant) and t.slice.value=='liquidity_reversal' for t in n.targets))
                    relevant.append(node);scope={'s3_json':lambda key:{'liquidity':{'reversal_label':'CONFIRMED TURN'}},'now':datetime.now(timezone.utc),'canaries':{}}
                    exec(compile(ast.Module(body=relevant,type_ignores=[]),'<actual consumer>','exec'),scope)
                    self.assertEqual(scope['canaries']['liquidity_reversal']['state'],'ABSTAIN')

    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Consumer))
    if not result.wasSuccessful():raise SystemExit(1)
