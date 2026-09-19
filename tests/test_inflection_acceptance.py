import ast
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[1]


class Tests(unittest.TestCase):
    def test_missing_receipt_waits_but_permission_failure_does_not_hide(self):
        path=ROOT/'aws/ops/pending/ops_5834_inflection_research_acceptance.py'
        if not path.exists():path=ROOT/'aws/ops/ran'/path.name
        node=next(n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name=='pending_receipt')
        scope={};exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope)
        class StorageError(Exception):
            def __init__(self,code):self.response={'Error':{'Code':code}}
        for code in ('NoSuchKey','404'):
            def missing(key):raise StorageError(code)
            self.assertIsNone(scope['pending_receipt'](missing,'fixture'))
        def denied(key):raise StorageError('AccessDenied')
        with self.assertRaises(StorageError):scope['pending_receipt'](denied,'fixture')
        self.assertEqual(scope['pending_receipt'](lambda key:{'commit':'x'},'fixture'),{'commit':'x'})


if __name__=='__main__':unittest.main()
