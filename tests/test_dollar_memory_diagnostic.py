from pathlib import Path
from unittest.mock import patch
import sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared'),str(ROOT/'aws/ops/staged')]
from dollar_fixture import fixture,STAMP
from test_futures_research_store import Memory
import ops_6023_dollar_memory_diagnostic as diagnostic
store=diagnostic.store;model=store.model


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client=Memory();cls.client.data.update(fixture()[4])
        with patch.object(store,'now',return_value=STAMP):cls.status=store.run(cls.client,diagnostic.BUCKET,diagnostic.candidate.REQUEST,'test',publish_current=False)
        # Production run's candidate reference is retained even if a later audit fails.
        cls.client.data[diagnostic.candidate.STATUS]=model.encoded({**cls.status,'status':'claimed','candidate_replay':cls.status['replay']})
    def test_both_read_only_profiles_reuse_the_exact_claim(self):
        def forbidden(**kwargs):raise AssertionError('Diagnostic attempted a write')
        with patch.object(self.client,'put_object',side_effect=forbidden):
            for mode in ('production_replay','independent_audit'):
                out=diagnostic.profile(mode,self.client,lambda:123)
                self.assertEqual(out['candidate_replay'],self.status['replay']);self.assertEqual(out['s3_writes'],0)
                self.assertEqual(out['stages'][-1]['peak_rss_kib'],123)
                if mode=='independent_audit':self.assertEqual(out['counts']['original_series'],32)
    def test_unknown_profile_is_refused_before_storage(self):
        with self.assertRaises(AssertionError):diagnostic.profile('invoke',self.client,lambda:0)


if __name__=='__main__':unittest.main(verbosity=2)
