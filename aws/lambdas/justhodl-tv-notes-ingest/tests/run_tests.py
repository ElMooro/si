"""Whole restored handler and note-limit regressions without AWS or Brain writes."""
from pathlib import Path
from unittest.mock import patch
import hashlib,importlib.util,json,sys,types,unittest
SOURCE=Path(__file__).resolve().parents[1]/'source/lambda_function.py'
with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**kw:None)}):
    spec=importlib.util.spec_from_file_location('tv_notes_restored',SOURCE)
    engine=importlib.util.module_from_spec(spec);spec.loader.exec_module(engine)


class Tests(unittest.TestCase):
    def test_complete_predecessor_restored_with_only_intended_note_limit(self):
        raw=SOURCE.read_bytes().replace(b'\r\n',b'\n')
        self.assertEqual(raw.count(b'MAX_TEXT = 32000'),1)
        original=raw.replace(b'MAX_TEXT = 32000',b'MAX_TEXT = 8000')
        self.assertEqual(len(original),22973)
        self.assertEqual(hashlib.sha256(original).hexdigest(),'27c008f20ab921355b5cf5a0622140575572af231399d4c97eca874e200f8e37')
    def test_note_limit_keeps_long_complete_notes_and_caps_bounded_text(self):
        for n in (9000,31990,31991,40000):
            note={'symbol':'SPY','text':'A'*n,'created':1720000000000}
            result=engine._norm(note)
            self.assertEqual(len(result['text']),min(n+9,32000))
            self.assertEqual(result['text'].endswith('…'),n+9>32000)
            self.assertEqual(result['id'],engine._norm(note)['id'])
    def test_unicode_limit_is_characters_and_rejection_remains(self):
        self.assertEqual(len(engine._norm({'symbol':'SPY','text':'界'*40000})['text']),32000)
        self.assertIsNone(engine._norm({'text':'x'}));self.assertIsNone(engine._norm(None))
    def test_actual_preflight_handler_needs_no_account_or_provider_access(self):
        with patch.object(engine,'_mirror_read',side_effect=AssertionError('account read')), \
             patch.object(engine,'_brain_put',side_effect=AssertionError('Brain write')):
            result=engine.lambda_handler({'requestContext':{'http':{'method':'OPTIONS'}}},None)
        self.assertEqual(result['statusCode'],204)


if __name__=='__main__':unittest.main(verbosity=2)
