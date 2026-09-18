"""Missing first receipts are pending; permission/corruption errors must fail."""
import ast
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


class StorageError(Exception):
    def __init__(self, code): self.response = {'Error': {'Code': code}}


class ReceiptWaitTests(unittest.TestCase):
    def test_absence_waits_but_access_denied_and_corruption_do_not(self):
        candidates = list((ROOT/'aws/ops').glob('*/ops_5733_research_intelligence_reverify.py'))
        self.assertEqual(len(candidates), 1)
        node = next(n for n in ast.parse(candidates[0].read_text(encoding='utf-8')).body
                    if isinstance(n, ast.FunctionDef) and n.name == 'pending_receipt')
        env = {'ClientError': StorageError}
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(candidates[0]), 'exec'), env)
        read = env['pending_receipt']
        for code in ('NoSuchKey', '404'):
            self.assertIsNone(read(lambda _: (_ for _ in ()).throw(StorageError(code)), 'fn'))
        with self.assertRaises(StorageError):
            read(lambda _: (_ for _ in ()).throw(StorageError('AccessDenied')), 'fn')
        with self.assertRaises(ValueError):
            read(lambda _: (_ for _ in ()).throw(ValueError('invalid json')), 'fn')
        receipt = {'commit': 'a'*40}
        self.assertEqual(read(lambda _: receipt, 'fn'), receipt)


if __name__ == '__main__': unittest.main()
