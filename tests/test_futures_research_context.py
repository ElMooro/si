from pathlib import Path
import copy,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import futures_research_context as c
def packet():return {'contract':'futures-original-research.v1','generated_at':'2026-09-21T00:00:00Z',
    'source_capture_completed_at':'2026-09-20T23:59:00Z',
    'replay':{'manifest_key':'data/futures-research/runs/'+'a'*64+'.json','output_sha256':'b'*64},**dict.fromkeys(c.FLAGS,False)}
class Tests(unittest.TestCase):
    def test_native_reference_is_descriptive_without_independent_verification_claim(self):
        out=c.context(packet());self.assertTrue(out['native_reference_available'])
        self.assertFalse(out['reference_hashes_independently_checked_by_consumer']);self.assertFalse(out['freshness_independently_checked_by_consumer'])
        self.assertEqual(out['signals'],[]);self.assertFalse(out['identity_ok']);self.assertTrue(all(out[k] is False for k in c.FLAGS))
    def test_legacy_or_promoted_packet_never_recreates_signals(self):
        for value in ({'identity_ok':True,'signals':['OIL_BACKWARDATION'],'product_data':{'CL':[1]}},None,{**packet(),'calls_eligible':True}):
            out=c.context(value);self.assertFalse(out['native_reference_available']);self.assertEqual(out['signals'],[])
    def test_alias_requires_canonical_identity_empty_old_signals_and_no_authority(self):
        p=packet();p.update(contract='futures-original-compatibility.v1',canonical={'key':c.CURRENT,'replay':p.pop('replay')},signals=[],product_data={},identity_ok=False)
        self.assertTrue(c.context(p)['native_reference_available']);p['signals']=['unqualified']
        self.assertFalse(c.context(p)['native_reference_available'])
    def test_foreign_reference_or_inconsistent_clock_is_unavailable(self):
        for edit in (lambda p:p['replay'].update(manifest_key='data/trade-tickets.json'),lambda p:p.update(generated_at='2099-01-01T00:00:00Z'),
            lambda p:p.update(source_capture_completed_at='2026-09-22T00:00:00Z'),lambda p:p.update(generated_at='2026-09-21')):
            p=packet();edit(p);self.assertFalse(c.context(p)['native_reference_available'])
    def test_reference_is_copied_and_caller_is_unchanged(self):
        p=packet();original=copy.deepcopy(p);out=c.context(p);out['canonical']['replay']['output_sha256']='x'
        self.assertEqual(p,original)
if __name__=='__main__':unittest.main(verbosity=2)
