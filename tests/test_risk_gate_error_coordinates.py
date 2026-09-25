from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6122_risk_gate_retained_error_coordinates as op

class Tests(unittest.TestCase):
 def test_only_error_class_and_code_coordinates_leave_private_log(self):
  text='[ERROR] ValueError: api_key=SECRET\nTraceback (most recent call last):\n  File "/var/task/risk_gate_research_store.py", line 123, in run\n    private_sensitive_statement()\n  File "/private/user/name.py", line 5, in other\n'
  out=op.coordinates([{'message':text},{'message':text}]);self.assertEqual(len(out),1)
  self.assertEqual(out[0]['error_class'],'ValueError');self.assertEqual(out[0]['count'],2)
  self.assertEqual(out[0]['frames'],[{'module':'risk_gate_research_store.py','line':123,'function':'run'}])
  for value in ('SECRET','api_key','private_sensitive_statement','/private/user'):self.assertNotIn(value,str(out))
 def test_unrecognized_lines_are_not_published_as_a_fallback(self):
  self.assertEqual(op.coordinates([{'message':'private arbitrary data'}]),[])
  self.assertEqual(op.coordinates([{'message':'[ERROR] secret payload'}])[0]['error_class'],'unclassified')

if __name__=='__main__':unittest.main(verbosity=2)
