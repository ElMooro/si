"""Invented event inputs through the actual shared SDK; no native/provider access."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime,timezone
from decimal import Decimal
import hashlib,importlib.util,math,socket,sys,types,unittest
from unittest.mock import Mock,patch
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'aws/shared'))
from signal_event_inputs import CONTRACT,prepare,exact
SDK=ROOT/'aws/shared/signals_emit.py'
OLD=ROOT/'tests/fixtures/signals-emit-pre-research-20261002.py'

class Clock(datetime):
 @classmethod
 def now(cls,tz=None):return cls(2026,10,2,12,0,0,tzinfo=timezone.utc)

class Tests(unittest.TestCase):
 def setUp(self):
  guard=patch.object(socket.socket,'connect',side_effect=AssertionError('Invented storage only'));guard.start();self.addCleanup(guard.stop)
 def sdk(self,path=None):
  fake=types.SimpleNamespace(client=Mock(side_effect=AssertionError('No actual clients')))
  guard=patch.dict(sys.modules,{'boto3':fake});guard.start();self.addCleanup(guard.stop)
  spec=importlib.util.spec_from_file_location('invented_event_sdk',path or SDK);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
  m._suppress_set=Mock(return_value=set());m._regime_snapshot=Mock(return_value={});m._fabric_ctx=Mock(return_value={});m.datetime=Clock
  return m
 def emit(self,m,**changes):
  kw=dict(signal_type='invented-event',ticker='QAONLY',direction='UP',windows=[5,21],baseline_price=100,confidence=.55,metadata={'engine':'invented'})
  kw.update(changes);table=Mock();ok=m.log_signal(table,**kw);return ok,table
 def test_retained_predecessor_reproduces_seven_event_failures(self):
  self.assertEqual(hashlib.sha256(OLD.read_bytes()).hexdigest(),'0d456b9231fde0479a6594f64d94c29ce7e0c32feeb7056fddb12c3703144199')
  cases=[({'confidence':0},'confidence',Decimal('.05')),({'confidence':float('nan')},'confidence',Decimal('.95')),({'windows':'21'},'check_windows',['2','1']),({'baseline_price':True},'baseline_price',Decimal('1.0')),({'direction':'UNKNOWN'},'predicted_direction','UNKNOWN'),({'windows':[5.9]},'check_windows',['5'])]
  for changes,key,expected in cases:
   m=self.sdk(OLD);ok,table=self.emit(m,**changes);self.assertTrue(ok);self.assertEqual(table.put_item.call_args.kwargs['Item'][key],expected)
  m=self.sdk(OLD);ok,table=self.emit(m,metadata={'tiny':1e-7});self.assertTrue(ok);self.assertEqual(table.put_item.call_args.kwargs['Item']['metadata']['tiny'],Decimal(0))
 def test_confidence_endpoints_and_precision_survive_without_clipping(self):
  for val in (0,1,.123456789,Decimal('0.1234567890123456789')):
   m=self.sdk();ok,table=self.emit(m,confidence=val);self.assertTrue(ok);self.assertEqual(table.put_item.call_args.kwargs['Item']['confidence'],Decimal(str(val)))
 def test_invalid_confidence_rejected_before_reads_and_writes(self):
  for val in (None,True,'0.5',-.01,1.01,float('nan'),float('inf'),Decimal('NaN')):
   m=self.sdk();ok,table=self.emit(m,confidence=val);self.assertFalse(ok);table.put_item.assert_not_called();m._suppress_set.assert_not_called();m._regime_snapshot.assert_not_called();m._fabric_ctx.assert_not_called()
 def test_positive_tiny_price_cannot_turn_into_zero_in_storage(self):
  old=self.sdk(OLD);ok,table=self.emit(old,baseline_price=0.0000001);self.assertTrue(ok);self.assertEqual(table.put_item.call_args.kwargs['Item']['baseline_price'],Decimal(0))
  for price in (0.0000001,Decimal('0.0000001')):
   new=self.sdk();ok,table=self.emit(new,baseline_price=price);self.assertTrue(ok);self.assertEqual(table.put_item.call_args.kwargs['Item']['baseline_price'],Decimal('0.0000001'))
 def test_actual_ai_logging_adapter_uses_only_invented_prices_and_table(self):
  spec=importlib.util.spec_from_file_location('invented_ai_market_read',ROOT/'aws/lambdas/justhodl-ai/source/market_read.py');mr=importlib.util.module_from_spec(spec);spec.loader.exec_module(mr);mr.datetime=Clock
  sdk=self.sdk();table=Mock();price=Mock(return_value=0.0000001)
  calls=[{'ticker':'QAONLY','direction':'UP','horizon_days':21,'confidence':0.65,'thesis':'Invented callback compatibility only'}]
  out=mr.log_calls(table,'invented-read',calls,sdk.log_signal,price)
  self.assertTrue(out[0]['logged']);price.assert_called_once_with('QAONLY');item=table.put_item.call_args.kwargs['Item'];self.assertEqual(item['baseline_price'],Decimal('0.0000001'));self.assertEqual(item['confidence'],Decimal('0.65'));self.assertEqual(item['check_windows'],[str(v) for v in mr.WINDOWS]);self.assertEqual(item['metadata']['read_id'],'invented-read');self.assertFalse(item['metadata']['event_input_contract']['entry_mark_qualified'])
 def test_positive_exact_price_and_invalid_price(self):
  m=self.sdk();ok,table=self.emit(m,baseline_price=Decimal('123.1234567890123456789'));self.assertTrue(ok);self.assertEqual(table.put_item.call_args.kwargs['Item']['baseline_price'],Decimal('123.1234567890123456789'))
  for val in (None,True,False,'100',0,-1,float('nan'),float('inf')):
   m=self.sdk();ok,table=self.emit(m,baseline_price=val);self.assertFalse(ok);table.put_item.assert_not_called()
 def test_windows_are_whole_positive_distinct_before_expiry(self):
  for val in ('21',21,[],[0],[-1],[True],[5.9],[5.0],['05'],['5.0'],['day_5'],[5,5],[5,'5'],[365],[1000000000]):
   m=self.sdk();ok,table=self.emit(m,windows=val);self.assertFalse(ok);table.put_item.assert_not_called()
  m=self.sdk();ok,table=self.emit(m,windows=[5,'21',364]);self.assertTrue(ok);item=table.put_item.call_args.kwargs['Item'];self.assertEqual(item['check_windows'],['5','21','364']);self.assertEqual(item['horizon_days_primary'],364)
  self.assertEqual(item['check_timestamps']['day_5'],'2026-10-07T12:00:00+00:00');self.assertTrue(all(datetime.fromisoformat(v).timestamp()<item['ttl'] for v in item['check_timestamps'].values()))
 def test_direction_aliases_are_explicit_and_received_spelling_retained(self):
  for received,wanted in [('UP','UP'),('down','DOWN'),('bullish','UP'),('bearish','DOWN'),('NEUTRAL','NEUTRAL')]:
   m=self.sdk();ok,table=self.emit(m,direction=received);self.assertTrue(ok);item=table.put_item.call_args.kwargs['Item'];self.assertEqual(item['predicted_direction'],wanted);self.assertEqual(item['metadata']['event_input_contract']['received']['direction'],received)
  for val in ('UNKNOWN','',None,True,1,' UP '):
   m=self.sdk();ok,table=self.emit(m,direction=val);self.assertFalse(ok);table.put_item.assert_not_called()
 def test_relative_direction_requires_explicit_benchmark_not_a_pretend_price(self):
  m=self.sdk();ok,table=self.emit(m,direction='OUTPERFORM');self.assertFalse(ok);table.put_item.assert_not_called()
  m=self.sdk();ok,table=self.emit(m,direction='UNDERPERFORM',benchmark='SPY');self.assertTrue(ok);item=table.put_item.call_args.kwargs['Item'];self.assertIsNone(item['baseline_benchmark_price']);self.assertFalse(item['metadata']['event_input_contract']['entry_mark_qualified'])
 def test_metadata_is_copied_and_tiny_numbers_remain_exact(self):
  value={'tiny':1e-7,'nested':[0,False,None,Decimal('0.1234567890123456789')],'event_input_contract':{'forecast_qualified':True}}
  old=deepcopy(value);m=self.sdk();ok,table=self.emit(m,metadata=value);self.assertTrue(ok);self.assertEqual(value,old)
  md=table.put_item.call_args.kwargs['Item']['metadata'];self.assertEqual(md['tiny'],Decimal('0.0000001'));self.assertEqual(md['nested'],value['nested']);c=md['event_input_contract'];self.assertEqual(c['contract'],CONTRACT);self.assertEqual(c['caller_supplied_contract'],value['event_input_contract'])
  for field in ('entry_mark_qualified','forecast_qualified','sizing_eligible','confidence_calibrated'):self.assertIs(c[field],False)
 def test_invalid_metadata_including_nonfinite_regime_never_reaches_write(self):
  for val in ([],[('engine','invented')],{'a':float('nan')},{'a':Decimal('Infinity')},{1:'ambiguous'}, {'a':object()}):
   m=self.sdk();ok,table=self.emit(m,metadata=val);self.assertFalse(ok);table.put_item.assert_not_called()
  m=self.sdk();m._regime_snapshot.return_value={'value':float('nan')};ok,table=self.emit(m);self.assertFalse(ok);table.put_item.assert_not_called()
 def test_invalid_identity_types_do_not_escape_or_write(self):
  for changes in ({'ticker':1},{'ticker':'qaonly'},{'signal_type':'bad#family'},{'signal_type':''},{'benchmark':'ticker_vs_spy'},{'signal_type':None}):
   m=self.sdk();ok,table=self.emit(m,**changes);self.assertFalse(ok);table.put_item.assert_not_called()
 def test_conditional_dedupe_unchanged_and_storage_failure_not_success(self):
  m=self.sdk();ok,table=self.emit(m);self.assertTrue(ok);self.assertEqual(table.put_item.call_args.kwargs['ConditionExpression'],'attribute_not_exists(signal_id)');self.assertEqual(table.put_item.call_args.kwargs['Item']['signal_id'],'invented-event#QAONLY#2026-10-02')
  table=Mock();table.put_item.side_effect=RuntimeError('invented outage');self.assertFalse(m.log_signal(table,'invented-event','QAONLY','UP',[5],100))

if __name__=='__main__':unittest.main(verbosity=2)
