from pathlib import Path
from copy import deepcopy
import base64,importlib.util,json,unittest
R=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('api_delivery',R/'aws/ops/checks/fifx_api_delivery.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
fixture=json.loads((R/'tests/fixtures/fifx-api-browser-whole-inputs.json').read_bytes());p=fixture['packet']
key=p['replay']['manifest_key'];manifest=json.loads(base64.b64decode(fixture['objects'][key]))
hashes={name:ref['sha256'] for name,ref in manifest['compilers'].items()}
a={'replayed':True,'status':'complete_original_archive_replayed','generated_at':p['generated_at'],'selected_manifest':key,
   'manifests':[{'key':key,'manifest':manifest}],'complete_arithmetic_proofs':fixture['independent_arithmetic'],
   'source_recovery':{sid:{**row,'current_available_at_run':row['current'] is not None} for sid,row in p['series'].items()}}
CUTOFF='2026-09-26T00:00:00Z'
class Tests(unittest.TestCase):
    def test_whole_six_api_originals_require_current_compilers_and_independent_replay(self):
        result=m.qualify(a,hashes,CUTOFF);self.assertTrue(result['api_originals_verified']);self.assertEqual(len(result['fred_sources']),6)
        self.assertEqual(sum(r['returned_rows'] for r in result['fred_sources'].values()),3300)
        self.assertFalse(result['investment_authority']);self.assertFalse(result['current_pointer_delivery_verified']);self.assertFalse(result['schedule_causation_verified'])
    def test_partial_failed_and_untyped_source_populations_never_become_complete_api_delivery(self):
        changes=[lambda r:r['receipt'].update(http_status=429),lambda r:r['source_identity']['population'].update(returned_rows=True),
          lambda r:r['source_identity']['population'].update(reported_rows=551),lambda r:r.update(retained_original_rows=550.0),
          lambda r:r['source_identity'].update(identity_reviewed=False),lambda r:r['receipt'].update(source_url='https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10')]
        for change in changes:
            b=deepcopy(a);change(b['source_recovery']['DGS10']);result=m.qualify(b,hashes,CUTOFF)
            self.assertFalse(result['api_originals_verified']);self.assertFalse(result['fred_sources']['DGS10']['complete_api_original_verified'])
    def test_missing_replay_old_run_and_different_compilers_remain_pending(self):
        self.assertFalse(m.qualify({'replayed':False},hashes,CUTOFF)['api_originals_verified'])
        self.assertFalse(m.qualify(a,hashes,'2026-09-27T00:00:00Z')['api_originals_verified'])
        changed={**hashes,'fifx_acquire':'a'*64};result=m.qualify(a,changed,CUTOFF)
        self.assertEqual(result['status'],'selected_originals_not_from_exact_new_compilers')
    def test_duplicate_manifest_missing_compiler_and_conflicting_clocks_refuse(self):
        b=deepcopy(a);b['manifests']*=2
        with self.assertRaises(ValueError):m.qualify(b,hashes,CUTOFF)
        with self.assertRaises(ValueError):m.qualify(a,{k:v for k,v in hashes.items() if k!='fifx_fred'},CUTOFF)
        b=deepcopy(a);b['generated_at']='2026-09-27T00:00:00Z'
        with self.assertRaises(ValueError):m.qualify(b,hashes,CUTOFF)
    def test_request_must_bind_exact_series_controls_origin_and_no_duplicate_or_secret_query(self):
        mutations=[lambda u:u.replace('series_id=DGS10','series_id=VIXCLS'),lambda u:u+'&series_id=DGS10',
          lambda u:u+'&api_key=invented-sensitive',lambda u:u.replace('units=lin','units=pch'),lambda u:u.replace('offset=0','offset=1'),
          lambda u:u.replace('sort_order=asc','sort_order=desc'),lambda u:u.replace('api.stlouisfed.org','example.invalid'),lambda u:u+'#fragment']
        for mutate in mutations:
            b=deepcopy(a);r=b['source_recovery']['DGS10'];r['receipt']['source_url']=mutate(r['receipt']['source_url'])
            result=m.qualify(b,hashes,CUTOFF);self.assertFalse(result['api_originals_verified']);self.assertIsNone(result['fred_sources']['DGS10']['original_receipt'])
            self.assertNotIn('invented-sensitive',json.dumps(result))
    def test_request_window_and_receipt_acquisition_must_match_typed_original_population(self):
        for field,value in [('requested_start','2000-01-01'),('requested_end','2026-09-25'),('realtime_end','2026-09-25'),('offset',False),('limit',50000.0),('full_series_history',True)]:
            b=deepcopy(a);b['source_recovery']['DGS10']['source_identity']['population'][field]=value
            self.assertFalse(m.qualify(b,hashes,CUTOFF)['api_originals_verified'])
        for field,value in [('acquired_at','2026-09-25T07:00:00Z'),('acquired_at','2026-09-26T08:00:00Z'),('acquired_at','2026-09-26T07:00:00'),('bytes',True),('bytes',0),('sha256','invalid')]:
            b=deepcopy(a);b['source_recovery']['DGS10']['receipt'][field]=value
            self.assertFalse(m.qualify(b,hashes,CUTOFF)['api_originals_verified'])
    def test_independent_proof_identity_history_and_availability_must_agree(self):
        for change in ({'source_id':'VIXCLS'},{'history_rows':-1},{'history_rows':550},{'current_available':False},{'current_available':1}):
            b=deepcopy(a);b['complete_arithmetic_proofs']['DGS10'].update(change)
            self.assertFalse(m.qualify(b,hashes,CUTOFF)['api_originals_verified'])
    def test_exact_compiler_names_required_even_when_map_contains_fourteen_hashes(self):
        changed=dict(hashes);changed['wrong_module']=changed.pop('fifx_fred')
        with self.assertRaises(ValueError):m.qualify(a,changed,CUTOFF)


def test_whole_six_api_originals_require_current_compilers_and_independent_replay():
    Tests().test_whole_six_api_originals_require_current_compilers_and_independent_replay()

def test_partial_failed_and_untyped_source_populations_never_become_complete_api_delivery():
    Tests().test_partial_failed_and_untyped_source_populations_never_become_complete_api_delivery()

def test_missing_replay_old_run_and_different_compilers_remain_pending():
    Tests().test_missing_replay_old_run_and_different_compilers_remain_pending()

def test_duplicate_manifest_missing_compiler_and_conflicting_clocks_refuse():
    Tests().test_duplicate_manifest_missing_compiler_and_conflicting_clocks_refuse()

def test_request_must_bind_exact_series_controls_origin_and_no_duplicate_or_secret_query():
    Tests().test_request_must_bind_exact_series_controls_origin_and_no_duplicate_or_secret_query()

def test_request_window_and_receipt_acquisition_must_match_typed_original_population():
    Tests().test_request_window_and_receipt_acquisition_must_match_typed_original_population()

def test_independent_proof_identity_history_and_availability_must_agree():
    Tests().test_independent_proof_identity_history_and_availability_must_agree()

def test_exact_compiler_names_required_even_when_map_contains_fourteen_hashes():
    Tests().test_exact_compiler_names_required_even_when_map_contains_fourteen_hashes()
