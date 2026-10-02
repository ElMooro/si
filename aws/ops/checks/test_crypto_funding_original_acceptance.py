from pathlib import Path
from datetime import datetime,timezone,timedelta
from unittest.mock import Mock
import copy,io,json,runpy,sys,types,unittest
R=Path(__file__).resolve().parents[3];sys.path[:0]=[str(R/'aws/shared'),str(R/'aws/ops/checks')]
import crypto_funding_archive as A
F=runpy.run_path(str(R/'tests/test_crypto_funding_observations.py'))
H=runpy.run_path(str(R/'aws/ops/checks/crypto_funding_archive_acceptance.py'))
STAMP=datetime(2020,1,1,tzinfo=timezone.utc)

class Exists(Exception):
 def __init__(self):self.response={'Error':{'Code':'412'}}
class Store:
 def __init__(self):self.data={};self.calls=[];self.modified=STAMP+timedelta(seconds=1);self.pages_override=None;self.short=False
 def put_object(self,**kw):
  assert kw['IfNoneMatch']=='*'
  if kw['Key'] in self.data:raise Exists()
  self.data[kw['Key']]=kw['Body']
 def get_object(self,**kw):
  self.calls.append(('get',kw));raw=self.data[kw['Key']]
  class Short(io.BytesIO):
   def read(self,n):return super().read(min(n,3))
  return {'Body':Short(raw) if self.short else io.BytesIO(raw),'ContentLength':len(raw),'LastModified':self.modified}
 def get_paginator(self,method):
  assert method=='list_objects_v2';return types.SimpleNamespace(paginate=self.pages)
 def pages(self,**kw):
  assert kw=={'Bucket':'invented','Prefix':A.PREFIX+'runs/'};self.calls.append(('list',kw))
  rows=[{'Key':key,'Size':len(raw),'LastModified':self.modified} for key,raw in sorted(self.data.items()) if key.startswith(kw['Prefix'])]
  return self.pages_override or [{'Name':'invented','Prefix':kw['Prefix'],'IsTruncated':False,'KeyCount':len(rows),'Contents':rows}]

def retained(client=None,rate='0',stamp=STAMP):
 client=client or Store();p=A.model.collect_funding(F['transport_factory'](rate),now=lambda:stamp)
 ref=A.retain(client,'invented',p);client.calls=[];return client,ref,p
def inspect(client,cutoff=STAMP.isoformat(),checked=(STAMP+timedelta(days=1)).isoformat()):
 return H['inspect'](client,'invented',A,cutoff=cutoff,checked_at=checked)

class OriginalAcceptance(unittest.TestCase):
 def test_whole_original_and_compiler_replay_without_current_or_private_reads(self):
  client,ref,p=retained();out=inspect(client)
  self.assertEqual(out['status'],'complete_funding_original_replayed');self.assertEqual(out['selected_manifest'],ref['manifest']);self.assertEqual(out['retained_attempts'],10);self.assertEqual(out['complete_responses'],10);self.assertEqual(out['observed_instruments'],10)
  self.assertEqual(out['complete_replayed_packet_sha256'],A.sha(A.encode(p)));self.assertEqual(len(out['complete_artifacts_read']),4)
  self.assertTrue(all(kind in ('get','list') for kind,_ in client.calls));self.assertFalse(out['current_head_read']);self.assertFalse(out['investment_authority'])
 def test_empty_or_pre_cutoff_archive_is_explicitly_pending(self):
  self.assertFalse(inspect(Store())['replayed']);client,_,_=retained()
  out=inspect(client,cutoff=(STAMP+timedelta(hours=1)).isoformat());self.assertFalse(out['replayed']);self.assertNotIn('selected_manifest',out)
 def test_subsecond_acquisition_uses_explicit_storage_clock_interval(self):
  client=Store();client.modified=STAMP;client,_,_=retained(client,stamp=STAMP+timedelta(microseconds=250000));out=inspect(client)
  self.assertTrue(out['replayed']);self.assertEqual(out['manifests'][0]['storage_clock_resolution_seconds'],1)
 def test_short_reads_remain_whole_originals(self):
  client,_,_=retained();client.short=True;self.assertTrue(inspect(client)['replayed'])
 def test_subsecond_future_acquisition_is_not_hidden_by_storage_rounding(self):
  client=Store();client.modified=STAMP
  client,_,_=retained(client,stamp=STAMP+timedelta(microseconds=750000))
  with self.assertRaisesRegex(ValueError,'clocks'):
   inspect(client,checked=(STAMP+timedelta(microseconds=250000)).isoformat())
 def test_truncated_or_wrong_prefix_inventory_fails(self):
  for changes in [{'IsTruncated':True},{'Prefix':'private/'},{'KeyCount':3}]:
   client=Store();client.pages_override=[{'Name':'invented','Prefix':A.PREFIX+'runs/','IsTruncated':False,'KeyCount':0,'Contents':[],**changes}]
   with self.assertRaises(ValueError):inspect(client)
 def test_foreign_current_or_private_reference_rejected_before_get(self):
  client=Store();reader=H['PublicOriginals'](client,'invented',A)
  for key in ['crypto-intel.json','data/crypto-intel-history.json','private/account.json',A.PREFIX+'runs/../x.json']:
   with self.assertRaises(ValueError):reader.get_object(Bucket='invented',Key=key)
  self.assertEqual(client.calls,[])
 def test_corrupt_body_and_declared_size_fail_closed(self):
  client,ref,_=retained();key=ref['capture']['key'];client.data[key]+=b'changed'
  with self.assertRaises(ValueError):inspect(client)
 def test_original_from_other_reviewed_version_needs_matching_checkout(self):
  client,ref,_=retained();manifest=A.strict(client.data[ref['manifest']['key']]);old=ref['manifest']['key']
  manifest['compilers']['crypto_funding_archive.py']=A.reference('compilers',b'invented other version')
  client.data.pop(old);new=A.reference('runs',A.encode(manifest));client.data[new['key']]=A.encode(manifest)
  out=inspect(client);self.assertFalse(out['replayed']);self.assertFalse(out['manifests'][0]['matching_local_compilers'])
 def test_future_and_reversed_clocks_cannot_qualify(self):
  client,_,_=retained()
  with self.assertRaises(ValueError):inspect(client,checked=(STAMP-timedelta(seconds=1)).isoformat())
  client.modified=STAMP-timedelta(seconds=5)
  with self.assertRaises(ValueError):inspect(client)
 def test_two_distinct_manifests_with_same_interval_are_conflict(self):
  client,_,_=retained();retained(client,rate='0.001')
  with self.assertRaises(ValueError):inspect(client)
 def test_permissions_cannot_change_even_with_rehashed_manifest(self):
  client,ref,_=retained();manifest=A.strict(client.data.pop(ref['manifest']['key']));manifest['calls_eligible']=True;raw=A.encode(manifest);client.data[A.reference('runs',raw)['key']]=raw
  with self.assertRaises(ValueError):inspect(client)
 def test_all_unavailable_still_replays_honestly_without_claiming_recovery(self):
  client,_,_=retained(rate='NaN');out=inspect(client)
  self.assertTrue(out['replayed']);self.assertEqual(out['retained_attempts'],20);self.assertEqual(out['observed_instruments'],0);self.assertEqual(out['unavailable_instruments'],10)
 def test_inventory_change_between_complete_censuses_refuses_acceptance(self):
  client,_,_=retained();original=client.pages;calls=[]
  def pages(**kw):
   calls.append(1)
   return original(**kw) if len(calls)==1 else [{'Name':'invented','Prefix':A.PREFIX+'runs/','IsTruncated':False,'KeyCount':0,'Contents':[]}]
  client.pages=pages
  with self.assertRaisesRegex(ValueError,'inventory changed'):inspect(client)
 def test_future_capture_storage_cannot_hide_behind_valid_manifest(self):
  client,ref,_=retained();original=client.get_object
  def get(**kw):
   out=original(**kw)
   if kw['Key']==ref['capture']['key']:out['LastModified']=STAMP+timedelta(days=2)
   return out
  client.get_object=get
  with self.assertRaisesRegex(ValueError,'storage clock'):inspect(client)
 def test_invalid_size_or_nonbinary_stream_closes_before_rejection(self):
  client,ref,_=retained();stream=io.BytesIO(b'bad')
  client.get_object=Mock(return_value={'Body':stream,'ContentLength':True,'LastModified':STAMP})
  reader=H['PublicOriginals'](client,'invented',A)
  with self.assertRaises(ValueError):reader.get_object(Bucket='invented',Key=ref['capture']['key'])
  self.assertTrue(stream.closed)

if __name__=='__main__':unittest.main(verbosity=2)
