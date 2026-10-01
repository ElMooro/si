"""Complete invented identifier evidence; no acquisition or native invocation."""
from copy import deepcopy
from pathlib import Path
import json,sys,unittest
HERE=Path(__file__).resolve().parent
SOURCE=HERE if (HERE/'directory_identity.py').exists() else HERE.parent/'aws/lambdas/justhodl-symdir/source'
sys.path.insert(0,str(SOURCE))
import directory_identity as identity

DOC={'as_of':'2000-01-01T00:00:00Z','by_ticker':{'ZZTEST':{'ticker':'ZZTEST','cik':'0000000111','isin':'US0000000000','isin_match_qualified':False}}}
REF={'ticker':'ZZTEST','cik':'0000000111','name':'Invented issuer'}
def result(document=DOC,reference=REF,market='stocks'):
 return identity.evidence('ZZTEST',reference,document,market)

class Tests(unittest.TestCase):
 def test_same_issuer_is_only_a_candidate_not_security_authority(self):
  out=result();self.assertIsNone(out['isin']);e=out['identifier_evidence']
  self.assertEqual(e['status'],'candidate_only');self.assertTrue(e['issuer_ids_match']);self.assertEqual(e['candidate_isin'],'US0000000000')
  self.assertFalse(e['automatic_identifier_routing_eligible']);self.assertFalse(e['current_security_relationship_verified'])
 def test_conflicting_issuer_cannot_borrow_flat_identifier(self):
  out=result(reference={**REF,'cik':'222'});self.assertIsNone(out['isin'])
  self.assertEqual(out['identifier_evidence']['status'],'conflicting_issuer');self.assertEqual(out['identifier_evidence']['reported_isin'],'US0000000000')
 def test_unverified_issuer_never_becomes_matching_zero(self):
  for bad in (None,True,False,0,-1,1.0,'0','0000000000','1e2','12345678901',{},[]):
   e=result(reference={**REF,'cik':bad})['identifier_evidence'];self.assertIsNone(e['reference_cik']);self.assertIsNone(e['issuer_ids_match'])
   self.assertEqual(e['status'],'issuer_identity_unverified')
 def test_malformed_optional_source_cannot_raise(self):
  for bad in (None,[],True,{'by_ticker':[]},{'by_ticker':{'ZZTEST':5}}):
   out=result(document=bad);self.assertIsNone(out['isin']);self.assertIn(out['identifier_evidence']['status'],('source_unavailable_or_invalid','invalid_optional_row'))
 def test_missing_ticker_is_not_source_absence(self):
  self.assertEqual(result(document={'by_ticker':{}})['identifier_evidence']['status'],'ticker_not_in_received_population')
 def test_conflicting_spine_ticker_is_explicit(self):
  doc=deepcopy(DOC);doc['by_ticker']['ZZTEST']['ticker']='OTHER'
  self.assertEqual(result(document=doc)['identifier_evidence']['status'],'conflicting_row_ticker')
 def test_cik_match_does_not_join_different_market(self):
  self.assertEqual(result(market='fx')['identifier_evidence']['status'],'market_relationship_unqualified')
 def test_malformed_identifier_is_not_coerced_to_a_valid_candidate(self):
  for bad in (True,999,{},[],['US0000000000'],'bad','x'*129):
   doc=deepcopy(DOC);doc['by_ticker']['ZZTEST']['isin']=bad;e=result(document=doc)['identifier_evidence']
   self.assertIsNone(e['candidate_isin']);self.assertEqual(e['status'],'invalid_identifier_character_pattern')
 def test_reported_spelling_and_generation_are_not_silently_qualified(self):
  doc=deepcopy(DOC);doc['by_ticker']['ZZTEST']['isin']=' us0000000000 '
  e=result(document=doc)['identifier_evidence'];self.assertEqual(e['reported_isin'],' us0000000000 ');self.assertEqual(e['candidate_isin'],'US0000000000')
  self.assertEqual(e['reported_source_clocks'],{'as_of':'2000-01-01T00:00:00Z'});self.assertFalse(e['independent_source_replay_verified'])
 def test_upstream_boolean_claim_does_not_grant_routing_authority(self):
  doc=deepcopy(DOC);doc['by_ticker']['ZZTEST']['isin_match_qualified']=True;e=result(document=doc)['identifier_evidence']
  self.assertTrue(e['source_claims_isin_qualified']);self.assertFalse(e['automatic_identifier_routing_eligible'])
 def test_optional_projection_does_not_mutate_whole_inputs(self):
  a,b=deepcopy(DOC),deepcopy(REF);json.dumps(result(document=a,reference=b),allow_nan=False)
  self.assertEqual(a,DOC);self.assertEqual(b,REF)
 def test_nonfinite_or_unbounded_invalid_numeric_value_cannot_poison_publication(self):
  for value in (float('nan'),float('inf'),10**5000):
   doc=deepcopy(DOC);doc['by_ticker']['ZZTEST']['isin']=value
   out=result(document=doc);json.dumps(out,allow_nan=False)
   self.assertIsNone(out['identifier_evidence']['reported_isin'])
 def test_explicit_null_row_remains_invalid_not_missing(self):
  self.assertEqual(result(document={'by_ticker':{'ZZTEST':None}})['identifier_evidence']['status'],'invalid_optional_row')
 def test_different_reference_ticker_cannot_claim_issuer_binding(self):
  self.assertEqual(result(reference={**REF,'ticker':'OTHER'})['identifier_evidence']['status'],'conflicting_reference_ticker')

if __name__=='__main__':unittest.main()
