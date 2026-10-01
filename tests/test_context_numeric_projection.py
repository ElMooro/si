from pathlib import Path
from decimal import localcontext
import gzip,hashlib,json,math,types,unittest
R=Path(__file__).resolve().parents[1];D=R/'tests/fixtures/context-numeric-projection'
def load(old=False):
 p=D/'predecessor.py.txt' if old else R/'aws/shared/context_evidence_store.py';m=types.ModuleType('context_under_test');exec(compile(p.read_bytes(),str(p),'exec'),m.__dict__);return m
class ContextNumbers(unittest.TestCase):
 def test_predecessor_underflow_and_precision_loss_are_reproduced(self):
  m=load(True);self.assertEqual(m.strict(b'{"v":1e-1000}')['v'],0);self.assertEqual(m.strict(b'{"v":0.100000000000000000001}')['v'],0.1)
 def test_lossy_numbers_fail_as_complete_packets(self):
  m=load()
  for token in ('1e-1000','-1e-1000','0.100000000000000000001','9007199254740993.0','1.234567890123456789'):
   raw=('{"valid":0,"lost":'+token+'}').encode()
   for body,encoding in ((raw,''),(gzip.compress(raw),'gzip')):
    with self.subTest(token=token,encoding=encoding),self.assertRaises(ValueError):m.strict(body,encoding)
 def test_genuine_zeros_and_roundtrip_numbers_remain_available(self):
  m=load()
  for token in ('0.0','-0.0','0e-1000','0.1','1.25','1e-300','1e300','5e-324','9007199254740992.0'):
   self.assertEqual(m.strict(('{"v":'+token+'}').encode())['v'],float(token))
  self.assertLess(math.copysign(1,m.strict(b'{"v":-0.0}')['v']),0)
 def test_integers_null_bool_strings_are_not_reinterpreted(self):
  m=load();value={'i':9007199254740993,'b':True,'null':None,'string':'1e-1000','list':[0,False]};self.assertEqual(m.strict(json.dumps(value).encode()),value)
 def test_numeric_allocation_and_nonfinite_tokens_fail_closed(self):
  m=load()
  for token in ('1e9999999999999999999999999999','1e309','NaN','Infinity','-Infinity','0.'+'1'*600):
   with self.subTest(token=token[:30]),self.assertRaises(ValueError):m.strict(('{"v":'+token+'}').encode())
 def test_ambient_decimal_settings_do_not_change_validation(self):
  m=load()
  with localcontext() as c:
   c.prec=2;c.Emax=9;c.Emin=-9
   self.assertEqual(m.strict(b'{"v":1.23456789}')['v'],1.23456789)
   with self.assertRaises(ValueError):m.strict(b'{"v":1.234567890123456789}')
 def test_existing_transport_duplicate_and_whole_member_guards_remain(self):
  m=load()
  for raw,encoding in ((b'{"a":1,"a":2}',''),(b'{broken',''),(gzip.compress(b'{}')[:-1],'gzip'),(gzip.compress(b'{}')+gzip.compress(b'{}'),'gzip'),(b'{}','gzip')):
   with self.subTest(encoding=encoding),self.assertRaises(ValueError):m.strict(raw,encoding)
 def test_whole_predecessor_is_preserved_with_only_reviewed_numeric_edits(self):
  p=json.loads((D/'edits.json').read_bytes());raw=(D/'predecessor.py.txt').read_bytes();self.assertEqual(hashlib.sha256(raw).hexdigest(),p['predecessor_sha256']);s=raw.decode()
  for a,b in p['edits']:self.assertEqual(s.count(a),1);s=s.replace(a,b)
  self.assertEqual(s,(R/'aws/shared/context_evidence_store.py').read_text(encoding='utf-8'));self.assertEqual(hashlib.sha256(s.encode()).hexdigest(),p['candidate_sha256'])
if __name__=='__main__':unittest.main(verbosity=2)
