"""Exercise the real lean-site config publisher, including missing dependency failure."""
from pathlib import Path
import importlib.util,json,tempfile,unittest,subprocess,sys
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('sections',ROOT/'scripts/bake_sections.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class Tests(unittest.TestCase):
    def test_lean_artifact_contains_exact_reviewed_universe_not_arbitrary_configs(self):
        old=(m.ROOT,m.SITE)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'repo';site=Path(tmp)/'site';(root/'config').mkdir(parents=True);site.mkdir()
            (site/'global-sovereign.html').write_text('<!DOCTYPE html><html><head></head><body><h1>Research</h1></body></html>',encoding='utf-8')
            raw=(ROOT/'config/global-sovereign-universe.json').read_bytes();(root/'config/global-sovereign-universe.json').write_bytes(raw)
            (root/'config/never-publish.json').write_text('{"private_fixture":true}',encoding='utf-8')
            try:
                m.ROOT=str(root);m.SITE=str(site);m.main()
                self.assertEqual((site/'config/global-sovereign-universe.json').read_bytes(),raw)
                self.assertFalse((site/'config/never-publish.json').exists())
            finally:m.ROOT,m.SITE=old
    def test_required_asset_missing_or_invalid_fails_instead_of_green_page(self):
        old=(m.ROOT,m.SITE)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'repo';site=Path(tmp)/'site';(root/'config').mkdir(parents=True);(site/'config').mkdir(parents=True)
            (site/'global-sovereign.html').write_text('<!DOCTYPE html>',encoding='utf-8')
            try:
                m.ROOT=str(root);m.SITE=str(site)
                with self.assertRaises(m.RequiredPageDependencyError):m.publish_sovereign_universe()
                (root/'config/global-sovereign-universe.json').write_text('{}',encoding='utf-8')
                with self.assertRaises(m.RequiredPageDependencyError):m.publish_sovereign_universe()
            finally:m.ROOT,m.SITE=old
    def test_cli_propagates_required_dependency_failure_to_pages(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'scripts').mkdir();site=root/'site';site.mkdir()
            (root/'scripts/bake_sections.py').write_bytes((ROOT/'scripts/bake_sections.py').read_bytes())
            (site/'global-sovereign.html').write_text('<!DOCTYPE html>',encoding='utf-8')
            run=subprocess.run([sys.executable,str(root/'scripts/bake_sections.py'),str(site)],capture_output=True,text=True)
            self.assertNotEqual(run.returncode,0)
            self.assertIn('required dependency failed',run.stdout)


if __name__=='__main__':unittest.main()
