from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops/staged')]
import ops_6007_fx_native_candidate as audit


class Tests(unittest.TestCase):
    def test_source_mapping_keeps_exact_clocks_and_whole_predecessor(self):
        key=audit.model.LEGACY;original={'key':'exact','sha256':'original','bytes':500}
        source={'sources':{'EUR_USD':{'complete':False}},'provider_requests':19,'provider_response_bytes':163680,
            'predecessors':{key:{'source_key':key,'original':original,'acquired_at':'original-clock','etag':'not-replay-input'}},
            'runtime':{'unrelated':'preserved-in-source-audit'}}
        result=audit.candidate_inputs(source,{'generated_at':'calculation-clock'})
        self.assertEqual(result,{'contract':'fx-original-inputs.v1','generated_at':'calculation-clock','sources':source['sources'],
            'provider_requests':19,'source_bytes':163680,'predecessors':{
                key:{'source_key':key,'original':original,'acquired_at':'original-clock'},audit.model.CURRENT:None}})
        self.assertIn('etag',source['predecessors'][key])


if __name__=='__main__':unittest.main(verbosity=2)
