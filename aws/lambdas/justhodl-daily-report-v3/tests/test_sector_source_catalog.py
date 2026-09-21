"""Canonical ETF inventory uses one actual source path for every declared wrapper."""
import ast,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[4]
class SectorSourceCatalog(unittest.TestCase):
    def test_actual_canonical_universe_contains_all_declared_sector_wrappers(self):
        import sector_research_catalog as catalog
        tree=ast.parse((Path(__file__).resolve().parents[1]/'source/lambda_function.py').read_text(encoding='utf-8'))
        values={}
        for node in tree.body:
            if isinstance(node,ast.Assign):
                for target in node.targets:
                    if isinstance(target,ast.Name) and target.id in ('STOCK_TICKERS','TICKER_NAMES'):values[target.id]=ast.literal_eval(node.value)
        self.assertEqual(len(values['STOCK_TICKERS']),len(set(values['STOCK_TICKERS'])))
        self.assertTrue(set(catalog.SYMBOLS)<=set(values['STOCK_TICKERS']))
        self.assertTrue(set(catalog.SYMBOLS)<=set(values['TICKER_NAMES']))
        calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='collect_market_sources']
        self.assertEqual(len(calls),1)
        self.assertEqual(calls[0].args[2].id,'STOCK_TICKERS')
        self.assertEqual(calls[0].args[3].id,'TICKER_NAMES')
