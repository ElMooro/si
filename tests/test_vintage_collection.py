"""Pinned research replay must not silently move to a later current collection."""
from pathlib import Path
import hashlib,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
from replay_fred_vintage import load_collection

class CollectionTests(unittest.TestCase):
    def test_pinned_replay_reads_only_the_exact_immutable_collection(self):
        raw=b'{"contract":"fred-vintage-index.v1","collection_id":"reviewed"}'
        sha=hashlib.sha256(raw).hexdigest();seen=[]
        def read(key):seen.append(key);return raw
        self.assertEqual(load_collection(sha,read)['collection_id'],'reviewed')
        self.assertEqual(seen,['data/vintage-research/collections/'+sha+'.json'])

    def test_corruption_unsafe_identity_and_legacy_index_fail_closed(self):
        raw=b'{"contract":"fred-vintage-index.v1"}'
        with self.assertRaisesRegex(ValueError,'immutable collection differs'):load_collection('a'*64,lambda key:raw)
        with self.assertRaisesRegex(ValueError,'lowercase SHA'):load_collection('../../private',lambda key:self.fail('must not fetch'))
        with self.assertRaisesRegex(ValueError,'original archive index'):load_collection(None,lambda key:b'{"series":[]}')

if __name__=='__main__':unittest.main()
