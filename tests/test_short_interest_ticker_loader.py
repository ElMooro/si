from io import BytesIO
from unittest.mock import Mock
import unittest
from test_short_interest_ticker_identity import t
class Stream(BytesIO):
 def read(self,size=-1):self.requested_size=size;return super().read(size)
class Loader(unittest.TestCase):
 def load(self,raw):
  stream=Stream(raw);client=Mock();client.get_object.return_value={'Body':stream}
  result=t.load_tickers(client,'invented');self.assertTrue(stream.closed)
  self.assertEqual(stream.requested_size,16*1024*1024+1);return result
 def test_whole_packet_and_real_zero_preserved(self):
  self.assertEqual(self.load(b'{"by_ticker":{"TEST":{"short_interest":0}},"value":0.5}'),{'by_ticker':{'TEST':{'short_interest':0}},'value':0.5})
 def test_invalid_and_non_object_packet_close_stream(self):
  for raw in [b'',b'{',b'null',b'[]',b'{"by_ticker":[]}',b'\xff']:
   with self.subTest(raw=raw):self.assertEqual(self.load(raw),{})
 def test_duplicate_fields_are_not_silently_overwritten(self):
  self.assertEqual(self.load(b'{"by_ticker":{"TEST":1,"TEST":2}}'),{})
 def test_nonfinite_and_numeric_precision_loss_are_unavailable(self):
  for raw in [b'NaN',b'Infinity',b'1e999',b'1e-999',b'0.1234567890123456789012345']:
   with self.subTest(raw=raw):self.assertEqual(self.load(b'{"by_ticker":{},"bad":'+raw+b'}'),{})
 def test_oversize_whole_document_is_rejected(self):
  self.assertEqual(self.load(b' '*(16*1024*1024)+b'{}'),{})
 def test_denied_storage_remains_unavailable(self):
  client=Mock();client.get_object.side_effect=PermissionError('invented')
  self.assertEqual(t.load_tickers(client,'invented'),{})
 def test_stream_failure_still_closes(self):
  stream=Mock();stream.read.side_effect=OSError('invented');client=Mock();client.get_object.return_value={'Body':stream}
  self.assertEqual(t.load_tickers(client,'invented'),{});stream.close.assert_called_once()
if __name__=='__main__':unittest.main(verbosity=2)
