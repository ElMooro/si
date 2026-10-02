"""Invented exact public release receipt and source-bound reader checks."""
from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import Mock,patch
import hashlib,io,json,runpy,sys,types,unittest
R=Path(__file__).resolve().parents[3];sys.path[:0]=[str(R/'aws/shared'),str(R/'aws/ops/checks')]
class Receipt(unittest.TestCase):
 def test_operation_pins_complete_committed_source_bytes(self):
  m=self.load()
  self.assertEqual(len(m['SOURCE_HASHES']),7)
  for path,digest in m['SOURCE_HASHES'].items():
   self.assertEqual(hashlib.sha256((R/'tests/fixtures/crypto-sentiment/native-6441'/(path+'.txt')).read_bytes()).hexdigest(),digest,path)
 def load(self):
  helper=types.ModuleType('crypto_stablecoin_archive_acceptance');helper.inspect=Mock()
  with patch.dict(sys.modules,{'crypto_stablecoin_archive_acceptance':helper}):m=runpy.run_path(str(R/'aws/ops/staged/ops_6441_crypto_stablecoin_original_replay.py'))
  m['receipt'].__globals__['EXPECTED_DEPLOY_COMMIT']='a'*40
  return m
 def test_exact_complete_public_receipt_is_required(self):
  m=self.load();raw=json.dumps({'commit':'a'*40,'deployed_at':'2020-01-01T00:00:00Z'}).encode();stream=io.BytesIO(raw);client=Mock();client.get_object.return_value={'Body':stream,'ContentLength':len(raw)}
  doc,digest=m['receipt'](client);self.assertEqual(doc['commit'],'a'*40);self.assertEqual(len(digest),64);self.assertTrue(stream.closed)
  client.get_object.assert_called_once_with(Bucket='justhodl-dashboard-live',Key='data/ops/releases/justhodl-crypto-intel.json')
 def test_wrong_commit_or_invalid_clock_cannot_choose_cutoff(self):
  for change in [{'commit':'b'*40},{'deployed_at':'2020-01-01'},{'deployed_at':None}]:
   m=self.load();raw=json.dumps({'commit':'a'*40,'deployed_at':'2020-01-01T00:00:00Z',**change}).encode();stream=io.BytesIO(raw);client=Mock();client.get_object.return_value={'Body':stream,'ContentLength':len(raw)}
   with self.assertRaises(ValueError):m['receipt'](client)
   self.assertTrue(stream.closed)
 def test_receipt_short_chunks_are_read_to_eof(self):
  class Stream(io.BytesIO):
   def read(self,n):return super().read(min(n,2))
  m=self.load();raw=b'{"commit":"'+b'a'*40+b'","deployed_at":"2020-01-01T00:00:00Z"}';client=Mock();client.get_object.return_value={'Body':Stream(raw),'ContentLength':len(raw)}
  self.assertEqual(m['receipt'](client)[0]['commit'],'a'*40)
 def test_incomplete_or_nonfinite_receipt_is_rejected(self):
  m=self.load()
  for raw,size in [(b'{}',3),(b'{}',True),(b'{"v":1e999}',11)]:
   client=Mock();stream=io.BytesIO(raw);client.get_object.return_value={'Body':stream,'ContentLength':size}
   with self.assertRaises(ValueError):m['receipt'](client)
   self.assertTrue(stream.closed)
 def test_complete_native_inventory_timestamps_survive_report_roundtrip(self):
  m=self.load();value={'pages':[{'LastModified':datetime(2020,1,1,tzinfo=timezone.utc),'IsTruncated':False,'KeyCount':0}]}
  out=m['jsonable'](value);self.assertEqual(json.loads(json.dumps(out)),out);self.assertEqual(out['pages'][0]['LastModified'],'2020-01-01T00:00:00+00:00')
  for invalid in [datetime(2020,1,1),float('nan'),float('inf'),b'unreviewed',{1:'key'}]:
   with self.assertRaises(ValueError):m['jsonable'](invalid)
if __name__=='__main__':unittest.main(verbosity=2)
