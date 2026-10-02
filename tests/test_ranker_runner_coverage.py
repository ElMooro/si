from pathlib import Path
from unittest.mock import patch
import runpy,sys,types,unittest,subprocess,tempfile
R=Path(__file__).resolve().parents[1];D=R/'tests/fixtures/ranker-final-audit'
class RunnerCoverage(unittest.TestCase):
 def execute(self,source,fail=None):
  calls=[]
  class Suite:
   def addTests(self,items):pass
  class Loader:
   def discover(self,*args,**kw):return Suite()
   def loadTestsFromName(self,*args):return Suite()
   def loadTestsFromTestCase(self,*args):return 'sector'
  class Reporter:
   def __init__(self,**kw):pass
   def run(self,suite):
    name='sector' if suite=='sector' else 'initial';calls.append(name);return types.SimpleNamespace(wasSuccessful=lambda:fail!=name)
  def extremes():
   calls.append('extremes')
   if fail=='extremes':raise SystemExit(1)
  def command(*args,**kwargs):
   name='numeric' if str(args[0][-1]).endswith('test_ranker_numeric.py') else 'audit'
   calls.append(name)
   if fail==name:raise subprocess.CalledProcessError(1,args[0])
  modules={'extremes_consumer_test_support':types.SimpleNamespace(run=extremes),'sector_consumer_test_support':types.SimpleNamespace(SectorBoundaries=object)}
  with tempfile.TemporaryDirectory() as td:
   p=Path(td)/'aws/lambdas/invented/tests/run_tests.py';p.parent.mkdir(parents=True);p.write_bytes(source)
   with patch.dict(sys.modules,modules),patch.object(unittest,'TestLoader',Loader),patch.object(unittest,'defaultTestLoader',Loader()),patch.object(unittest,'TextTestRunner',Reporter),patch.object(subprocess,'run',command):
    try:runpy.run_path(str(p),run_name='__main__');code=0
    except SystemExit as exc:code=exc.code
    except subprocess.CalledProcessError as exc:code=exc.returncode
  return calls,code
 def test_predecessor_success_skips_existing_followup_suites(self):self.assertEqual(self.execute((D/'runner-before.py.txt').read_bytes()),(['initial'],0))
 def test_candidate_success_reaches_every_existing_suite_and_new_audit(self):self.assertEqual(self.execute((R/'aws/lambdas/justhodl-master-ranker/tests/run_tests.py').read_bytes()),(['initial','extremes','sector','audit','numeric'],0))
 def test_each_failure_makes_runner_nonzero(self):
  order=['initial','extremes','sector','audit','numeric']
  for name in order:
   with self.subTest(name=name):self.assertEqual(self.execute((R/'aws/lambdas/justhodl-master-ranker/tests/run_tests.py').read_bytes(),name),(order[:order.index(name)+1],1))
if __name__=='__main__':unittest.main(verbosity=2)
