import csv,io,json,unittest,urllib.error
from datetime import datetime,timezone,timedelta
import oecd_series as m
class StoreError(Exception):
 def __init__(self,code):self.response={'Error':{'Code':code}}
class Store:
 def __init__(self):self.objects={};self.puts=[]
 def get_object(self,Bucket,Key):
  if Key not in self.objects:raise StoreError('NoSuchKey')
  return {'Body':io.BytesIO(self.objects[Key])}
 def put_object(self,**kw):
  if kw.get('IfNoneMatch')=='*' and kw['Key'] in self.objects:raise StoreError('PreconditionFailed')
  self.objects[kw['Key']]=kw['Body'];self.puts.append(kw['Key'])
NOW=datetime(2026,10,4,12,tzinfo=timezone.utc)
EXTRA=[k for k in m._CONTEXTS if k!=m.FLOW]
def sid(flow):return 'oecd:'+flow+':'+next(k for k,d in m._context(flow).CATALOG['series'].items() if d['freq']=='M')
def raw(flow,value='100',**changes):
 d=m.definition(sid(flow));r=dict(DATAFLOW=m._context(flow).CATALOG['dataflow'],**d['dimensions'],TIME_PERIOD='2026-01',OBS_VALUE=value,OBS_STATUS='A',UNIT_MULT=d['unit_mult'],DECIMALS='2',BASE_PER=d['base_period']);r.update(changes);out=io.StringIO();w=csv.DictWriter(out,fieldnames=list(r));w.writeheader();w.writerow(r);return out.getvalue().encode()
class Tests(unittest.TestCase):
 def setUp(self):m._memory_index=None;m._memory_source=None;self.s=Store()
 def test_complete_exact_four_flow_directory(self):
  expected={m.FLOW:1511,EXTRA[0]:875,EXTRA[1]:1050,EXTRA[2]:413}
  ids=[]
  for flow,count in expected.items():
   self.assertEqual(m.directory(flow=flow)['total'],count);self.assertEqual(m.reviewed_flow(flow.split(',')[1]),flow)
   for offset in range(0,count,500):ids.extend(r['id'] for r in m.directory(flow=flow,offset=offset,limit=500)['rows'])
  self.assertEqual(len(set(ids)),3849);all_ids=[]
  for offset in range(0,3849,500):all_ids.extend(r['id'] for r in m.all_directory(offset=offset,limit=500)['rows'])
  self.assertEqual(all_ids,ids);self.assertEqual(m.directory()['total'],1511)
 def test_exact_version_and_dimensions_only_no_growth_for_additional_flows(self):
  for flow in EXTRA:
   self.assertEqual(m.definition(sid(flow).upper())['id'],sid(flow))
   for bad in [sid(flow)+':G1',sid(flow)+':GY',sid(flow).replace(flow,flow[:-1]+'9'),sid(flow)+':EVIL','oecd:'+flow+':UNKNOWN']:
    with self.assertRaises(ValueError):m.fetch(bad,self.s,'fixture',reader=lambda u:self.fail('source must not run'))
  with self.assertRaises(ValueError):m.read_http('https://evil.test')
 def test_each_flow_has_independent_daily_gate_and_memory_index(self):
  calls=[]
  def read(url):
   calls.append(url);flow=next(f for f,c in m._CONTEXTS.items() if c.SOURCE_URL==url);return raw(flow,str(10+list(m._CONTEXTS).index(flow))),{}
  for repetition in range(3):
   for i,flow in enumerate(m._CONTEXTS):
    packet=m.fetch(sid(flow),self.s,'fixture',reader=read,now=NOW);self.assertEqual(packet['obs'],[['2026-01-01',10+i]]);self.assertTrue(m.cache_valid(packet,sid(flow)));self.assertEqual(packet['source_receipts'][0]['flow'],flow)
  self.assertEqual(len(calls),4);self.assertEqual(len(set(calls)),4);self.assertEqual(len(m._memory_index),4);self.assertEqual(len(m._memory_source),4)
  gates=[p for p in self.s.puts if '/download-claims/' in p];self.assertEqual(len(gates),4);self.assertEqual(len(set(gates)),4)
 def test_denied_flow_never_stops_or_supplies_another_flow(self):
  bad,good=EXTRA[:2];calls=[]
  def reader(url):
   calls.append(url)
   if url==m._context(bad).SOURCE_URL:raise urllib.error.HTTPError(url,403,'invented denial',{},None)
   return raw(good,'23'),{}
  for at in [NOW,NOW+timedelta(days=1)]:self.assertEqual(m.fetch(sid(bad),self.s,'fixture',reader=reader,now=at)['n'],0)
  self.assertEqual(m.fetch(sid(good),self.s,'fixture',reader=reader,now=NOW)['obs'],[['2026-01-01',23]])
  self.assertEqual(len(calls),2)
 def test_wrong_flow_cannot_pass_unit_valid_values(self):
  for flow in EXTRA:
   for change in [{'DATAFLOW':m.CATALOG['dataflow']},{'UNIT_MULT':'3'},{'BASE_PER':'1900'}]:
    with self.assertRaises(ValueError):m.parse(raw(flow,**change),m.definition(sid(flow)))
 def test_balance_and_unspecified_index_units_not_invented_or_rebased(self):
  bts=m._context(EXTRA[1]);cli=m._context(EXTRA[2]);self.assertEqual({d['unit'] for d in bts.CATALOG['series'].values()},{'Percentage balance'})
  self.assertEqual({d['unit'] for d in cli.CATALOG['series'].values()},{'Index'});self.assertEqual({d['base_period'] for d in cli.CATALOG['series'].values()},{''})
  packet=m.fetch(sid(EXTRA[1]),self.s,'fixture',reader=lambda u:(raw(EXTRA[1],'-8.25'),{}),now=NOW);self.assertEqual(packet['obs'],[['2026-01-01',-8.25]]);self.assertEqual(packet['unit'],'Percentage balance');self.assertFalse(packet['calls_eligible'])
if __name__=='__main__':unittest.main()
