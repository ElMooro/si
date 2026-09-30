"""Complete invented API packets and retained historical publications; no AWS."""
from pathlib import Path
from copy import deepcopy
from unittest.mock import patch
import base64,gzip,io,json,unittest
import run_tests as native
import test_fifx_api_arithmetic as api

store,model=native.store,native.model
ROOT=native.ROOT

def full_api():
    client=native.Store();value,defs=native.inputs(client)
    for sid in store.catalog.FRED:
        _,raw,receipt,_=api.api_case(sid)
        value['sources'][sid]={'original':store.retain_bytes(client,'bucket',raw,'originals','bin'),
            'receipt':store.retain_bytes(client,'bucket',model.encoded(receipt),'receipts'),
            'requested_url':receipt['source_url']}
    with patch.object(store,'definitions',return_value=defs):out=store.retain(client,'bucket',value)
    return client,value,defs,out

class Tests(unittest.TestCase):
    def test_all_eighteen_whole_sources_retain_and_replay_with_new_api_closure(self):
        client,value,defs,out=full_api();read=store.reader(client,'bucket');run=store.binding(out,read)
        self.assertEqual(len(run['compilers']),14);self.assertIn('fifx_fred',run['compilers'])
        with patch.object(store,'definitions',return_value=defs):proof=store.replay(out,read)
        self.assertEqual(sum(r['original_rows'] for r in proof.values()),6*550+12*42)
        for sid in store.catalog.FRED:
            source=store.checked(run['series'][sid],'series',read)
            self.assertEqual(source['requested_url'],value['sources'][sid]['requested_url'])
            self.assertEqual(source['source_identity']['population']['returned_rows'],550)
            self.assertEqual(source['source_identity']['population']['reported_rows'],550)
            self.assertTrue(source['source_identity']['population']['complete_requested_window'])
            original=store.checked(value['sources'][sid]['original'],'originals',read,'bin')
            self.assertEqual(original,api.api_case(sid)[1])
        self.assertEqual(out['decision'],{'verb':'WAIT','meaning':'abstain'})
        self.assertTrue(all(out[k] is False for k in store.catalog.AUTHORITY))
        self.assertIsNone(out['portfolio_consequences']['target_weights'])

    def test_new_api_run_cannot_omit_decoder_or_substitute_wrong_compiler_slot(self):
        client,_,defs,out=full_api();read=store.reader(client,'bucket');run=store.binding(out,read)
        for mode in ('missing','wrong_slot'):
            altered=deepcopy(run)
            if mode=='missing':del altered['compilers']['fifx_fred']
            else:altered['compilers']['fifx_fred']=altered['compilers']['fifx_catalog']
            ref=store.retain_bytes(client,'bucket',model.encoded(altered),'runs')
            packet=deepcopy(out);packet['replay']['manifest_key']=ref['key']
            with patch.object(store,'definitions',return_value=defs),self.assertRaises(ValueError):store.replay(packet,read)

    def test_whole_prior_csv_publications_replay_without_archived_code_execution(self):
        fixture=json.loads((ROOT/'tests/fixtures/fifx-browser-whole-inputs.json').read_bytes())
        objects={key:base64.b64decode(value,validate=True) for key,value in fixture['objects'].items()}
        def read(key):
            self.assertTrue(key.startswith(('data/fifx-vol-research/','data/report-research/','data/evidence/fred/')))
            raw=objects[key]
            return store.bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw
        for name in ('first','second'):
            out=fixture[name];proof=store.replay(out,read)
            self.assertEqual(len(proof),18);self.assertEqual(sum(p['original_rows'] for p in proof.values()),714)
            self.assertNotIn('fifx_fred',store.binding(out,read)['compilers'])

    def test_normal_loop_preserves_public_request_for_success_and_failure(self):
        client=native.Store();client.objects[store.SOURCE]=b'{}';client.objects[store.BOND]=b'{}'
        seen=[]
        def acquired(url,timeout):
            seen.append(url)
            if 'series_id=DGS10&' in url:
                _,raw,rec,_=api.api_case('DGS10');return raw,rec
            raise TimeoutError('whole invented unavailable request')
        predecessors={'packet':native.private(),'history':native.private(b'whole invented history')}
        with patch.object(store,'now',return_value=native.fixture.NOW),patch.object(store,'previous_state',return_value=(predecessors,model.empty_watermarks())),patch.object(store,'existing_move',side_effect=native.Missing()),patch.object(store.acquisition,'acquire',side_effect=acquired),patch.object(store,'retain',return_value={'generated_at':native.fixture.NOW,'quality':{},'replay':{}}) as retained,patch.object(store,'publish',return_value=True):
            store.run(client,'bucket','invented-api-run')
        value=retained.call_args.args[2];plan=store.acquisition.plan(native.fixture.NOW)
        self.assertEqual(len(seen),17)
        for sid,url in plan.items():
            entry=value['sources'][sid];self.assertEqual(entry['requested_url'],url)
            if sid=='DGS10':self.assertEqual(client.objects[entry['original']['key']],api.api_case(sid)[1])
            else:self.assertIsNone(entry['original']);self.assertIsNone(entry['receipt'])
        self.assertNotIn('requested_url',value['sources']['^MOVE'])

if __name__=='__main__':unittest.main(verbosity=2)
