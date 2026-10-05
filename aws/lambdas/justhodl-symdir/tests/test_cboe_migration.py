from pathlib import Path
from datetime import datetime,timedelta,timezone
import copy,hashlib,importlib.util,json,runpy,sys,unittest
from importlib.machinery import SourceFileLoader
from test_cboe_index import Store
import cboe_index as m
ROOT=Path(__file__).resolve().parents[4];D=ROOT/'tests/fixtures/chart-cboe'
loader=SourceFileLoader('cboe_legacy_fixture',str(D/'legacy/cboe_index.py.txt'));spec=importlib.util.spec_from_loader(loader.name,loader);old=importlib.util.module_from_spec(spec);loader.exec_module(old)
NOW=datetime(2026,10,5,3,tzinfo=timezone.utc)
def raw(symbol):return (D/'data'/(symbol+'-chart.json')).read_bytes()
class Tests(unittest.TestCase):
 def test_adding_definitions_reuses_all_original_retained_sources(self):
  for symbol in old.CATALOGUE['series']:
   sid='cboeindex:'+symbol;store=Store();first=old.fetch(sid,store,'fixture',reader=lambda _:(raw(symbol),{}),now=NOW)
   self.assertGreater(first['n'],0);calls=[]
   def denied(url):calls.append(url);raise AssertionError('No new request for unchanged retained definition')
   second=m.fetch(sid,store,'fixture',reader=denied,now=NOW+timedelta(minutes=1))
   self.assertEqual(calls,[]);self.assertEqual(first['obs'],second['obs']);self.assertEqual(first['source_receipts'][0]['sha256'],second['source_receipts'][0]['sha256']);self.assertEqual(second['definition_sha256'],m.CATALOGUE_HASH);self.assertEqual(second['source_receipts'][0]['definition_sha256'],old.CATALOGUE_HASH)
 def test_changed_legacy_definition_cannot_relabel_original(self):
  sid='cboeindex:VIX6M';store=Store();p=old.fetch(sid,store,'fixture',reader=lambda _:(raw('VIX6M'),{}),now=NOW);receipt=p['source_receipts'][0]
  for field in ['unit','name','currency','source_url','measurement_kind']:
   d=m.definition(sid);d[field]='unreviewed'
   with self.assertRaises(ValueError):m.validate_receipt(receipt,d,raw('VIX6M'))
 def test_new_receipts_bind_complete_definition_independently_of_catalogue_membership(self):
  sid='cboeindex:BDES50N';store=Store();first=m.fetch(sid,store,'fixture',reader=lambda _:(raw('BDES50N'),{}),now=NOW);self.assertGreater(first['n'],0)
  r=first['source_receipts'][0];self.assertEqual(r['series_definition_sha256'],m.definition_hash(first['definition']))
  original=m.CATALOGUE_HASH
  try:
   m.CATALOGUE_HASH='a'*64;second=m.fetch(sid,store,'fixture',reader=lambda _:self.fail('Unchanged definition must not redownload'),now=NOW+timedelta(minutes=2));self.assertEqual(first['obs'],second['obs']);self.assertEqual(second['definition_sha256'],'a'*64)
  finally:m.CATALOGUE_HASH=original
 def test_unknown_legacy_generation_or_tampered_definition_digest_is_rejected(self):
  sid='cboeindex:VIX6M';store=Store();p=old.fetch(sid,store,'fixture',reader=lambda _:(raw('VIX6M'),{}),now=NOW)
  for edits in [{'definition_sha256':'a'*64},{'series_definition_sha256':'b'*64},{'definition_sha256':'invalid'}]:
   r=dict(p['source_receipts'][0],**edits)
   with self.assertRaises(ValueError):m.validate_receipt(r,m.definition(sid),raw('VIX6M'))
 def test_every_new_source_has_exact_close_and_preserves_return_variant(self):
  original=old.CATALOGUE['series'];review=json.loads((ROOT/'docs/audit/2026-10-04/chart-cboe-watchlist-review.json').read_bytes())
  self.assertEqual(len(review['alternatives']),77)
  for symbol,d in m.CATALOGUE['series'].items():
   if symbol in original:self.assertEqual(d,original[symbol]);continue
   p=m.fetch('cboeindex:'+symbol,Store(),'fixture',reader=lambda _:(raw(symbol),{}),now=NOW);source=json.loads(raw(symbol));self.assertIsNone(p['quality']['error']);self.assertGreater(p['n'],0)
   self.assertEqual(p['obs'],[[r['date'],float(r['close'])] for r in source['data']]);self.assertEqual(p['unit'],'Index points');self.assertFalse(p['history']['market_ohlc_qualified']);self.assertFalse(p['history']['traded_volume_qualified']);self.assertFalse(p['calls_eligible']);self.assertFalse(p['sizing_eligible'])
   if d['source_product_label'].endswith('- net'):self.assertTrue(p['name'].endswith('- net'))
if __name__=='__main__':unittest.main()
