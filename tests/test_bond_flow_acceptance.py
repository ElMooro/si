from pathlib import Path
from io import BytesIO
import gzip,hashlib,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops/checks','aws/ops','aws/shared','aws/lambdas/justhodl-etf-true-flows/source')]
import ops_6169_bond_flow_candidate as op


class Memory:
    def __init__(self,data):self.data=data;self.reads=[]
    def get_object(self,Bucket,Key):self.reads.append(Key);return {'Body':BytesIO(self.data[Key])}


class Tests(unittest.TestCase):
    def test_exact_namespaces_whole_decompression_and_mutation_detection(self):
        key='data/evidence/etf_original/'+'a'*64+'/'+'b'*64+'.bin.gz'
        raw=b'complete source\n'*300;m=Memory({key:gzip.compress(raw)});read,observed=op.reader(m)
        self.assertEqual(read(key),raw);self.assertEqual(observed[key],{'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)})
        for invalid in ('data/portfolio.json','data/etf-research/runs/../private.json','https://example.com/data.json'):
            with self.assertRaises(ValueError):read(invalid)
        self.assertEqual(m.reads,[key]);m.data[key]=gzip.compress(raw+b'changed')
        with self.assertRaises(ValueError):read(key)

    def test_retained_baseline_requires_exact_bytes_and_scope(self):
        raw=b'{"all":"bytes"}';sha=hashlib.sha256(raw).hexdigest();ref={'key':op.PRIVATE+sha+'.bin','sha256':sha,'bytes':len(raw)}
        m=Memory({ref['key']:raw});self.assertEqual(op.private_read(m,ref),raw)
        with self.assertRaises(ValueError):op.private_read(m,{**ref,'bytes':len(raw)-1})
        with self.assertRaises(ValueError):op.private_read(m,{**ref,'key':'data/bond-desk.json'})
        self.assertNotIn('data/bond-desk.json',m.reads)

if __name__=='__main__':unittest.main()
