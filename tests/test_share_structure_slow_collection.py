"""Pacing and budget regression for the three unstarted complete source parts."""
from pathlib import Path
from unittest.mock import Mock,patch
import hashlib,json,re,sys,unittest,urllib.error
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/ops/checks'))
import share_structure_batch_runner as runner


class Tests(unittest.TestCase):
    def test_previous_workflow_and_runner_remain_whole(self):
        manifest=json.loads((ROOT/'tests/fixtures/share-structure-slow-collection.json').read_bytes())
        for row in manifest['files']:
            body=(ROOT/row['predecessor']).read_bytes()
            self.assertEqual((len(body),hashlib.sha256(body).hexdigest()),(row['bytes'],row['sha256']))
        frozen=json.loads((ROOT/'tests/fixtures/share-structure-batch3-recovery.json').read_bytes())['frozen_collectors']
        for name,digest in frozen.items():
            self.assertEqual(hashlib.sha256((ROOT/('aws/ops/checks/'+name+'.py')).read_bytes()).hexdigest(),digest)

    def test_budget_preserves_previous_parts_and_covers_worst_case_new_request_pacing(self):
        for part in (1,2,3):self.assertEqual(runner.collection_limits(part),(1.0,3300))
        for part,requests in ((4,2000),(5,2000),(6,1305)):
            interval,budget=runner.collection_limits(part)
            self.assertEqual(interval,2.0);self.assertEqual(budget,5100)
            self.assertGreater(budget-120,requests*interval+300)
            self.assertGreaterEqual(90*60-budget,300)
        for part in (0,7,True,'4'):
            with self.assertRaises(ValueError):runner.collection_limits(part)

    def test_only_exact_three_remaining_source_scripts_receive_ninety_minutes(self):
        text=(ROOT/'.github/workflows/run-ops-direct.yml').read_text(encoding='utf-8')
        expression=next(line.strip().split(': ',1)[1] for line in text.splitlines() if line.strip().startswith('timeout-minutes:'))
        match=re.fullmatch(r"\$\{\{ contains\(fromJSON\('(\[.*\])'\), inputs.script\) && 90 \|\| 60 \}\}",expression)
        self.assertIsNotNone(match)
        paths=json.loads(match[1]);self.assertEqual(len(paths),3)
        self.assertEqual(set(paths),{'staged/ops_'+str(6082+n)+'_share_structure_sources_part_'+str(n)+'.py' for n in (4,5,6)})
        for path in paths:self.assertTrue((ROOT/'aws/ops'/path).is_file())
        self.assertIn('group: run-ops-direct',text);self.assertIn('cancel-in-progress: false',text)

    def test_actual_slow_transport_paces_each_attempt_and_does_not_retry_provider_failure(self):
        order=[];rate=Mock();rate.acquire.side_effect=lambda:order.append('pace')
        opener=Mock();opener.open.side_effect=lambda *a,**k:order.append('request') or 'whole-response'
        with patch.object(runner.source,'Rate',return_value=rate) as factory,patch.object(runner.urllib.request,'build_opener',return_value=opener):
            transport=runner.limited_transport(2.0);factory.assert_called_once_with(interval=2.0)
            self.assertEqual(transport('request',25),'whole-response');self.assertEqual(order,['pace','request'])
            opener.open.side_effect=urllib.error.HTTPError('https://financialmodelingprep.com/stable/quote',429,'limit',{},None)
            with self.assertRaises(urllib.error.HTTPError):transport('request',25)
            self.assertEqual(opener.open.call_count,2)
        with self.assertRaises(ValueError):runner.limited_transport(0.1)


if __name__=='__main__':unittest.main(verbosity=2)
