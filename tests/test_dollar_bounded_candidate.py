from pathlib import Path
from unittest.mock import patch
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared'),str(ROOT/'aws/ops/staged')]
from dollar_fixture import fixture,STAMP
from test_futures_research_store import Memory
import ops_6024_dollar_bounded_candidate as candidate
store=candidate.store;model=store.model


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client=Memory();cls.client.data.update(fixture()[4])
        with patch.object(store,'now',return_value=STAMP):cls.status=store.run(cls.client,'b','bounded','execution',publish_current=False)
        cls.sources={m.__name__:Path(m.__file__).read_bytes() for m in store.COMPILERS}
    def test_prior_record_verifies_whole_compilers_inputs_and_output_without_execution(self):
        with patch.object(candidate,'REJECTED',self.status['replay']):
            inputs,out=candidate.prior_record(store.reader(self.client,'b'),self.sources)
        self.assertEqual(inputs['generated_at'],STAMP);self.assertEqual(out['generated_at'],STAMP)
        self.assertEqual(model.sha(model.encoded(out)),self.status['replay']['output_sha256'])
    def test_changed_compiler_and_wrong_record_digest_fail(self):
        sources=dict(self.sources);sources['dollar_research_model']+=b'\n'
        with patch.object(candidate,'REJECTED',self.status['replay']):
            with self.assertRaises(AssertionError):candidate.prior_record(store.reader(self.client,'b'),sources)
        bad={**self.status['replay'],'output_sha256':'0'*64}
        with patch.object(candidate,'REJECTED',bad):
            with self.assertRaises(AssertionError):candidate.prior_record(store.reader(self.client,'b'),self.sources)


if __name__=='__main__':unittest.main(verbosity=2)
