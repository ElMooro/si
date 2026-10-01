from unittest.mock import patch
import copy,unittest
import signal_board_native_test_support as support
class BoardSupport(unittest.TestCase):
 def test_null_scalars_arrays_and_objects_retain_distinct_statuses_and_abstain(self):
  support.assert_abstention('data/short-pressure.json',(None,False,0,'invented',[],{}, {'calls_eligible':True}))
 def test_wrong_retained_status_is_still_rejected(self):
  original=support.candidate.build
  def corrupt(*args,**kwargs):
   out=original(*args,**kwargs);out['sources']['data/short-pressure.json']['status']='derived_packet_retained';return out
  with patch.object(support.candidate,'build',side_effect=corrupt),self.assertRaises(AssertionError):support.assert_abstention('data/short-pressure.json',(None,))
 def test_authority_or_body_identity_regression_is_still_rejected(self):
  original=support.candidate.build
  for field in ('calls_eligible','sha256'):
   def corrupt(*args,**kwargs):
    out=original(*args,**kwargs)
    if field=='sha256':out['sources']['data/short-pressure.json']['original']['sha256']='0'*64
    else:out[field]=True
    return out
   with patch.object(support.candidate,'build',side_effect=corrupt),self.assertRaises(AssertionError):support.assert_abstention('data/short-pressure.json',({},))
if __name__=='__main__':unittest.main()
