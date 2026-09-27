"""Exercise the actual CLI against isolated source trees; no site code executes."""
import hashlib
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class PageScriptGate(unittest.TestCase):
    def run_gate(self, files):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, content in files.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding='utf-8')
            return subprocess.run([sys.executable, str(ROOT / 'scripts/check_page_scripts.py'), str(root)],
                                  capture_output=True, text=True)

    def test_inline_and_transitive_failures_are_fatal(self):
        for files in [
            {'index.html': '<script>const broken = ;</script>'},
            {'index.html': '<script src="/entry.js"></script>', 'entry.js': 'import "./child.js";', 'child.js': 'let broken = ;'},
        ]:
            result = self.run_gate(files)
            self.assertEqual(result.returncode, 1)
            self.assertIn('JavaScript parse failure', result.stderr)

    def test_json_scripts_are_data_and_untrusted_source_is_never_executed(self):
        result = self.run_gate({'index.html': '<script type="application/ld+json">{"@context":"example"}</script><script>throw Error("MUST NOT RUN")</script>'})
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_actual_broken_predecessors_cannot_ship(self):
        for name in ['8k-items.html', 'filing-redflags.html']:
            original = (ROOT / 'tests/fixtures' / ('pre-filing-desk-' + name + '.txt')).read_text(encoding='utf-8')
            result = self.run_gate({name: original})
            self.assertEqual(result.returncode, 1)
            self.assertIn('JavaScript parse failure', result.stderr)

    def test_empty_tree_cannot_pass(self):
        result = self.run_gate({})
        self.assertEqual(result.returncode, 1)
        self.assertIn('INCOMPLETE', result.stderr)

    def test_upload_markers_cannot_pass_as_valid_javascript(self):
        for marker in ('PLACEHOLDER', 'PLACEHOLDER_WILL_NOT_USE', 'SEE_FILE'):
            result = self.run_gate({'index.html': '<script src="/entry.js"></script>', 'entry.js': '// upload\n'+marker+';\n'})
            self.assertEqual(result.returncode, 1, marker)
            self.assertIn('JavaScript placeholder source', result.stderr)
        marker = (ROOT/'tests/fixtures/rejected-sidebar-placeholder.js.txt').read_text(encoding='utf-8')
        result = self.run_gate({'index.html': '<script src="/sidebar.js"></script>', 'sidebar.js': marker})
        self.assertEqual(result.returncode, 1)
        safe = self.run_gate({'index.html': '<script>/* PLACEHOLDER */ const text="SEE_FILE";</script>'})
        self.assertEqual(safe.returncode, 0, safe.stderr)

    def test_build_checks_sources_and_final_artifact_before_upload(self):
        workflow = (ROOT / '.github/workflows/pages.yml').read_text(encoding='utf-8')
        self.assertLess(workflow.index('python3 scripts/check_page_scripts.py\n'), workflow.index('Assemble lean site artifact'))
        self.assertLess(workflow.index('Bake SEO layer'), workflow.index('python3 scripts/check_page_scripts.py _site'))
        self.assertLess(workflow.index('python3 scripts/check_page_scripts.py _site'), workflow.index('actions/upload-pages-artifact'))


if __name__ == '__main__':
    unittest.main()
