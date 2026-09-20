"""Offline reuse of existing retained source fixtures; no new provider access."""
from pathlib import Path
from copy import deepcopy
import gzip,hashlib,json,sys,unittest
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-crisis-composite/tests')]
import canonical_fred_replay as replay
from test_native_research import store_fixture

SERIES=('STLFSI4','BAMLH0A0HYM2','BAMLC0A0CM','VIXCLS','DTWEXBGS','DTB3','DGS10','SOFR','DFF')
class ReplayCases(unittest.TestCase):
    def setUp(self):
        self.objects,inputs=store_fixture();self.packet=inputs['macro']
        self.reads=[]
    def read(self,key):
        self.reads.append(key);raw=self.objects[key]
        return gzip.decompress(raw) if key.endswith('.gz') else raw
    def rebind(self,packet,change_manifest=None):
        manifest=json.loads(self.objects[self.packet['replay']['manifest_key']])
        manifest['output_sha256']=replay.report_observations.digest({k:v for k,v in packet.items() if k!='replay'})
        if change_manifest:change_manifest(manifest)
        raw=replay.report_observations.encoded(manifest);key=replay.PREFIX+'runs/'+replay.sha(raw)+'.json'
        self.objects[key]=raw;packet['replay']={**packet['replay'],'manifest_key':key,'output_sha256':manifest['output_sha256']}
        return packet
    def test_nine_selected_series_reconstruct_without_other_series_originals(self):
        rows=replay.restore(self.packet,SERIES,self.read)
        self.assertEqual(set(rows),set(SERIES));self.assertTrue(all(rows.values()))
        self.assertEqual(len([k for k in self.reads if k.endswith('.gz')]),18)
        self.assertEqual(rows['BAMLH0A0HYM2']['definition']['seriess'][0]['units'],'Percent')
    def test_corrupt_original_bytes_rejected(self):
        ref=self.packet['measurements']['SOFR']['evidence']['observations']
        self.objects[ref['key']]=gzip.compress(b'{}')
        with self.assertRaisesRegex(ValueError,'original bytes differ'):replay.restore(self.packet,SERIES,self.read)
    def test_modified_measurement_fails_even_after_output_digest_rebound(self):
        p=deepcopy(self.packet);p['measurements']['SOFR']['current']=99
        self.rebind(p)
        with self.assertRaisesRegex(ValueError,'reconstruction differs'):replay.restore(p,SERIES,self.read)
    def test_missing_measurement_is_explicitly_absent(self):
        p=deepcopy(self.packet);del p['measurements']['SOFR'];self.rebind(p)
        out=replay.restore(p,SERIES,self.read);self.assertIsNone(out['SOFR']);self.assertEqual(len(out),9)
    def test_unapproved_source_url_and_future_receipt_rejected(self):
        for name,value in (('source_url','https://other.invalid/series'),('first_received_at','2099-01-01T00:00:00Z')):
            p=deepcopy(self.packet)
            def change(m):m['inputs']['SOFR']['evidence']['observations'][name]=value
            self.rebind(p,change)
            with self.assertRaises(ValueError):replay.restore(p,SERIES,self.read)
    def test_compiler_or_run_bytes_cannot_change_under_same_address(self):
        key=self.packet['replay']['manifest_key'];m=json.loads(self.objects[key]);self.objects[m['compiler']['key']]+=b'\n'
        with self.assertRaisesRegex(ValueError,'compiler'):replay.restore(self.packet,SERIES,self.read)
        self.objects[key]+=b'\n'
        with self.assertRaisesRegex(ValueError,'run bytes'):replay.restore(self.packet,SERIES,self.read)
    def test_invalid_or_duplicate_series_do_not_read_any_source(self):
        for ids in ((),('SOFR','SOFR'),('../brain',)):
            with self.assertRaises(ValueError):replay.restore(self.packet,ids,self.read)
        self.assertEqual(self.reads,[])
    def test_unbound_current_packet_rejected(self):
        p=deepcopy(self.packet);p['generated_at']='2099-01-01T00:00:00Z'
        with self.assertRaises(ValueError):replay.restore(p,SERIES,self.read)

if __name__=='__main__':unittest.main()
