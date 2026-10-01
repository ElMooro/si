"""Actual workflow copy loop over invented files; no repository fixture bodies."""
from pathlib import Path
import hashlib,json,os,re,shutil,subprocess,sys,tempfile,textwrap,unittest
R=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(R/'scripts'))
import check_public_site_artifact as gate
from page_sources import DENY

class PublicArtifact(unittest.TestCase):
 def test_complete_workflow_predecessor_and_exact_correction_remain(self):
  root=R/'tests/fixtures/public-artifact-boundary';spec=json.loads((root/'preservation.json').read_bytes());raw=(root/'pages-before.yml.txt').read_bytes()
  self.assertEqual(len(raw),spec['source_bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),spec['source_sha256'])
  text=raw.decode('utf-8')
  for a,b in spec['edits']:self.assertEqual(text.count(a),1);text=text.replace(a,b)
  self.assertEqual(self.workflow(),text);self.assertEqual(hashlib.sha256(text.encode()).hexdigest(),spec['candidate_sha256'])
 def workflow(self):return (R/'.github/workflows/pages.yml').read_text(encoding='utf-8')
 def loop(self,raw=None):
  raw=self.workflow() if raw is None else raw
  start=raw.index('          DENY="');end=raw.index('\n          done',start)+len('\n          done')
  return textwrap.dedent(raw[start:end])
 def tree(self,root):
  for name in sorted(DENY|{'crypto','intel','screener'}):
   if name=='_site':continue
   p=root/name/'nested';p.mkdir(parents=True);(p/'invented.html').write_text('<html>invented</html>',encoding='utf-8');(p/'invented.txt').write_text('invented non-page source',encoding='utf-8')
  (root/'_site').mkdir()
 def execute(self,root,script):
  bash=shutil.which('bash')
  if not bash:raise RuntimeError('Bash is required to test the actual deployment copy loop')
  result=subprocess.run([bash,'-c','set -euo pipefail\n'+script],cwd=root,capture_output=True,text=True,encoding='utf-8')
  self.assertEqual(result.returncode,0,result.stdout+result.stderr)
 def test_actual_workflow_deny_set_matches_source_route_boundary(self):
  deny=set(re.search(r'DENY="([^"]+)"',self.loop()).group(1).split())
  self.assertEqual(deny,DENY)
 def test_real_copy_loop_keeps_public_subdirectories_and_omits_source_trees(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);self.tree(root);self.execute(root,self.loop())
   for name in ('crypto','intel','screener'):
    for file in ('invented.html','invented.txt'):self.assertEqual((root/'_site'/name/'nested'/file).read_bytes(),(root/name/'nested'/file).read_bytes())
   for name in DENY:self.assertFalse((root/'_site'/name).exists(),name)
   self.assertEqual(gate.check(root/'_site')['content_bodies_read'],0)
 def test_predecessor_html_fixture_reproduces_unwanted_tree_copy_and_gate_rejects(self):
  loop=self.loop();old=re.sub(r'DENY="([^"]+)"',lambda m:'DENY=" '+' '.join(x for x in m.group(1).split() if x not in ('tests','vendor','.git'))+' "',loop)
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);self.tree(root);self.execute(root,old)
   self.assertTrue((root/'_site/tests/nested/invented.txt').is_file())
   with self.assertRaisesRegex(ValueError,'tests'):gate.check(root/'_site')
 def test_final_gate_is_fail_closed_and_allows_generated_public_config(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);(root/'config').mkdir();(root/'config/public.json').write_text('{"invented":true}',encoding='utf-8')
   self.assertEqual(gate.check(root)['status'],'public_artifact_boundary_checked')
   for name in sorted(gate.FORBIDDEN_ROOTS):
    target=root/name;target.mkdir()
    with self.subTest(name=name),self.assertRaises(ValueError):gate.check(root)
    target.rmdir()
   with self.assertRaises(ValueError):gate.check(root/'missing')
   (root/'tests').mkdir();result=subprocess.run([sys.executable,str(R/'scripts/check_public_site_artifact.py'),str(root)],capture_output=True,text=True)
   self.assertEqual(result.returncode,1);self.assertIn('tests',result.stderr)
 def test_workflow_checks_tests_before_assembly_and_artifact_before_manifest(self):
  text=self.workflow();test='python3 tests/test_public_artifact_boundary.py';guard='python3 scripts/check_public_site_artifact.py _site'
  self.assertEqual(text.count(test),1);self.assertEqual(text.count(guard),1)
  self.assertLess(text.index(test),text.index('- name: Assemble lean site artifact'))
  self.assertGreater(text.index(guard),text.index('- name: Final built page syntax gate'))
  self.assertLess(text.index(guard),text.index('- name: Stamp every page and asset in commit-bound build manifest'))
if __name__=='__main__':unittest.main(verbosity=2)
