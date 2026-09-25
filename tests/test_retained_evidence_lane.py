"""Execute actual workflow selection; reject arbitrary paths and shell input."""
from pathlib import Path
import ast,os,re,subprocess,tempfile,textwrap,unittest
ROOT=Path(__file__).resolve().parents[1]
WORKFLOW=ROOT/'.github/workflows/retained-evidence-audit.yml'


class Tests(unittest.TestCase):
    def select(self,choice):
        text=WORKFLOW.read_text(encoding='utf-8')
        selection=text.split('      - name: Select reviewed audit\n',1)[1].split('      - run:',1)[0]
        script=textwrap.dedent(selection.split('        run: |\n',1)[1])
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/'environment.txt';output.touch()
            env={**os.environ,'AUDIT_CHOICE':choice,'GITHUB_ENV':str(output)}
            result=subprocess.run(['bash','-c',script],env=env,text=True,capture_output=True)
            fields=dict(line.split('=',1) for line in output.read_text().splitlines())
        return result.returncode,fields

    def test_exact_reviewed_audits_select_only_their_own_script_and_report(self):
        for choice,name in (('buyback-document-catalog','ops_6112_buyback_document_evidence'),
                ('buyback-document-independent','ops_6113_buyback_document_independent_acceptance')):
            code,fields=self.select(choice)
            self.assertEqual(code,0)
            self.assertEqual(fields,{'AUDIT_SCRIPT':'staged/'+name+'.py','AUDIT_REPORT':'aws/ops/reports/latest/'+name+'.md'})
            self.assertTrue((ROOT/'aws/ops'/fields['AUDIT_SCRIPT']).is_file())

    def test_unreviewed_provider_jobs_arbitrary_paths_and_shell_strings_do_not_select_a_script(self):
        for choice in ('','staged/ops_6087_share_structure_sources_part_5.py','../other.py',
                'buyback-document-catalog; echo unexpected','$(echo unexpected)','buyback-document-catalog\nAUDIT_SCRIPT=other'):
            code,fields=self.select(choice)
            self.assertEqual(code,2);self.assertEqual(fields,{})

    def test_scripts_are_fixed_retained_source_checks_without_invokes_or_transport_calls(self):
        for name in ('ops_6112_buyback_document_evidence','ops_6113_buyback_document_independent_acceptance'):
            tree=ast.parse((ROOT/'aws/ops/staged'/str(name+'.py')).read_text())
            attributes={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
            self.assertTrue({'get_object','read','retain','journal'}<=attributes)
            self.assertFalse({'invoke','capture','urlopen','get_secret_value','send_message','publish','update_function_code'}&attributes)

    def test_manual_main_only_lane_is_separate_and_publishes_only_the_reviewed_report(self):
        text=WORKFLOW.read_text()
        self.assertIn("if: github.ref == 'refs/heads/main'",text)
        self.assertIn('group: retained-evidence-audit',text)
        self.assertIn('cancel-in-progress: false',text)
        self.assertNotIn('schedule:',text);self.assertNotIn('push:\n',text)
        self.assertIn('git add -- "$AUDIT_REPORT"',text)
        self.assertNotIn('reports/latest/*.md',text)
        self.assertIn("env.AUDIT_RC != '0'",text)
        self.assertNotIn('inputs.audit }}"',text)


if __name__=='__main__':unittest.main(verbosity=2)
