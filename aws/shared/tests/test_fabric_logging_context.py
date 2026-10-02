"""Actual SDK/consumer code against invented storage only, with sockets denied."""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import socket
import sys
import types
import unittest
from unittest.mock import Mock, patch
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/shared'),str(Path(__file__).resolve().parent)]
from fabric_logging_context import FIELDS,CONTRACT,separate,read,select
from ciss_vintage_test_support import load
from test_compound_numeric import Storage
from test_holdings_derived_boundary import Storage as ConsumerStorage

SDK_PATHS=[ROOT/'aws/shared/signals_emit.py',ROOT/'aws/lambdas/justhodl-shadow-lab/source/signals_emit.py']
PREDECESSOR=ROOT/'tests/fixtures/signals-emit-pre-research-20261002.py'


class FixedClock(datetime):
    @classmethod
    def now(cls,tz=None):return cls(2026,10,2,8,0,0,tzinfo=timezone.utc)


class Tests(unittest.TestCase):
    def setUp(self):
        block=patch.object(socket.socket,'connect',side_effect=AssertionError('Invented storage only'))
        block.start();self.addCleanup(block.stop)

    def packet(self,flag=False):
        return {'generated_at':'2001-01-01T00:00:00Z','learning_weight_eligible':flag,
                'tickers':{'QAONLY':{'agreement_pct':0,'fabric_score':0.123456789,'conflict':False,
                                    'peer_fabric_score':None,'learning_weight_eligible':flag}}}

    def sdk(self,packet=None,path=None,client=None):
        db=client or Storage({'data/feature-bus.json':self.packet() if packet is None else packet})
        fake=types.SimpleNamespace(client=lambda *a,**k:db)
        # The retained predecessor imports boto3 again inside _fabric_ctx.
        # Keep that import bound to invented storage for the whole test.
        guard=patch.dict(sys.modules,{'boto3':fake});guard.start();self.addCleanup(guard.stop)
        spec=importlib.util.spec_from_file_location('invented_actual_sdk',path or SDK_PATHS[0])
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        module._suppress_set=lambda:set();module._regime_snapshot=lambda:{};module.datetime=FixedClock
        return module,db

    def emit(self,module,metadata=None):
        table=Mock()
        ok=module.log_signal(table,'invented-signal','QAONLY','UP',[5,21],100,
                             confidence=.76543219,metadata=metadata or {'engine':'invented'})
        return ok,table

    def test_retained_predecessor_reproduces_false_stamp_and_new_copies_keep_core_event(self):
        self.assertEqual(hashlib.sha256(PREDECESSOR.read_bytes()).hexdigest(),'0d456b9231fde0479a6594f64d94c29ce7e0c32feeb7056fddb12c3703144199')
        old,_=self.sdk(path=PREDECESSOR);ok,table=self.emit(old);self.assertTrue(ok)
        before=table.put_item.call_args.kwargs['Item'];self.assertEqual(before['metadata']['fabric_agreement'],0)
        for path in SDK_PATHS:
            new,_=self.sdk(path=path);ok,table=self.emit(new);self.assertTrue(ok)
            after=table.put_item.call_args.kwargs['Item']
            self.assertEqual({k:v for k,v in before.items() if k!='metadata'}, {k:v for k,v in after.items() if k!='metadata'})
            self.assertEqual(after['metadata']['engine'],'invented');self.assertEqual(after['metadata']['regime'],{})
            self.assertFalse(set(FIELDS)&set(after['metadata']))
        self.assertEqual(SDK_PATHS[0].read_bytes(),SDK_PATHS[1].read_bytes())

    def test_both_copies_preserve_zero_precision_and_false_qualification_despite_forged_flags(self):
        for path in SDK_PATHS:
            for flag in (False,True,None):
                module,db=self.sdk(self.packet(flag),path);ok,table=self.emit(module);self.assertTrue(ok)
                md=table.put_item.call_args.kwargs['Item']['metadata'];self.assertFalse(set(FIELDS)&set(md))
                context=md['fabric_research'];self.assertEqual(context['contract'],CONTRACT)
                for field in ('learning_weight_eligible','calls_eligible','sizing_eligible','forecast_qualified'):self.assertIs(context[field],False)
                received=context['received'];values=received['selected_values']
                self.assertEqual(values['agreement_pct'],0);self.assertEqual(values['fabric_score'],Decimal('0.123456789'))
                self.assertIs(values['conflict'],False);self.assertIsNone(values['peer_fabric_score'])
                self.assertEqual(received['source_publication_clock'],'2001-01-01T00:00:00Z')
                self.assertFalse(received['source_freshness_qualified']);self.assertFalse(received['original_bytes_retained'])
                self.assertEqual(db.reads,['data/feature-bus.json'])

    def test_caller_legacy_and_existing_context_retained_separately_on_source_failure(self):
        caller={'engine':'invented','fabric_agreement':0,'fabric_score':0.0000001,
                'fabric_conflict':False,'fabric_peer':None,'fabric_research':{'learning_weight_eligible':True}}
        original=deepcopy(caller)
        module,_=self.sdk(client=Storage({'data/feature-bus.json':PermissionError('PRIVATE_CANARY')}))
        ok,table=self.emit(module,caller);self.assertTrue(ok);self.assertEqual(caller,original)
        md=table.put_item.call_args.kwargs['Item']['metadata'];self.assertFalse(set(FIELDS)&set(md))
        context=md['fabric_research'];self.assertFalse(context['learning_weight_eligible'])
        self.assertEqual(context['caller_legacy_fields']['fabric_agreement'],0)
        self.assertEqual(context['caller_legacy_fields']['fabric_score'],Decimal('0.0000001'))
        self.assertEqual(context['caller_supplied_research'],original['fabric_research'])
        self.assertNotIn('PRIVATE_CANARY',repr(md))

    def test_unexpected_projection_failure_never_falls_back_to_old_learning_keys(self):
        module,_=self.sdk()
        with patch.object(module,'separate_fabric_metadata',side_effect=ValueError('invented failure')):
            ok,table=self.emit(module,{'fabric_agreement':100})
        self.assertFalse(ok);table.put_item.assert_not_called()

    def test_nonfinite_caller_diagnostic_cannot_reach_a_write(self):
        for value in (float('nan'),float('inf'),Decimal('NaN')):
            module,_=self.sdk();ok,table=self.emit(module,{'fabric_score':value})
            self.assertFalse(ok);table.put_item.assert_not_called()

    def test_cache_reuse_disclosed_and_failed_refresh_cannot_reuse_last_good(self):
        module,db=self.sdk()
        with patch.object(module.time,'monotonic',side_effect=[0,0,1,901,901]):
            first=module._fabric_ctx('QAONLY');second=module._fabric_ctx('QAONLY')
            self.assertEqual(first['selected_values'],second['selected_values']);self.assertEqual(second['cache_age_s'],1)
            db.objects['data/feature-bus.json']=PermissionError('invented denial')
            third=module._fabric_ctx('QAONLY')
        self.assertEqual(len(db.reads),2);self.assertIsNone(third['selected_values'])
        self.assertEqual(third['row_status'],'source_unavailable');self.assertFalse(third['source_freshness_qualified'])

    def test_malformed_whole_source_and_bad_length_stay_unavailable_and_close_stream(self):
        class Captured(Storage):
            def get_object(self,**kwargs):
                response=super().get_object(**kwargs);self.body=response['Body'];return response
        for value in (b'{"tickers":{},"tickers":{"QAONLY":{}}}',b'{"tickers":{"QAONLY":{"fabric_score":NaN}}}',{'tickers':None}):
            db=Captured({'data/feature-bus.json':value});out=read(db,'invented')
            self.assertEqual(out['status'],'source_unavailable_or_invalid');self.assertTrue(db.body.closed)
        class Cut(Captured):
            def get_object(self,**kwargs):
                response=super().get_object(**kwargs);response['ContentLength']+=1;return response
        db=Cut({'data/feature-bus.json':self.packet()});self.assertEqual(read(db,'invented')['status'],'source_unavailable_or_invalid');self.assertTrue(db.body.closed)

    def test_absent_invalid_empty_and_explicit_zero_rows_remain_distinct(self):
        cases=[({},'not_reported',False),({'QAONLY':None},'invalid_record',True),({'QAONLY':{}},'selected_diagnostics',True),({'QAONLY':{'fabric_score':0}},'selected_diagnostics',True)]
        for rows,status,present in cases:
            snapshot=read(Storage({'data/feature-bus.json':{'tickers':rows}}),'invented');out=select(snapshot,'QAONLY')
            self.assertEqual(out['row_status'],status);self.assertIs(out['row_present'],present)
            if rows.get('QAONLY')=={'fabric_score':0}:self.assertEqual(out['selected_values'],{'fabric_score':0})

    def test_source_hash_binds_received_body_without_claiming_original_retention(self):
        raw=json.dumps(self.packet(),separators=(',',':')).encode();out=read(Storage({'data/feature-bus.json':raw}),'invented')
        self.assertEqual(out['source_sha256'],hashlib.sha256(raw).hexdigest());self.assertEqual(out['source_bytes'],len(raw))
        self.assertFalse(out['original_bytes_retained']);self.assertFalse(out['source_replay_performed'])

    def test_missing_client_snapshot_has_explicit_unavailable_row(self):
        result=select(None,'QAONLY')
        self.assertEqual(result['row_status'],'source_unavailable')
        self.assertFalse(result['row_present']);self.assertIsNone(result['selected_values'])
        self.assertFalse(result['source_freshness_qualified'])
        module,_=self.sdk()
        with patch.object(module.boto3,'client',side_effect=RuntimeError('PRIVATE_CANARY')):
            ok,table=self.emit(module)
        self.assertTrue(ok)
        received=table.put_item.call_args.kwargs['Item']['metadata']['fabric_research']['received']
        self.assertEqual(received['row_status'],'source_unavailable');self.assertNotIn('PRIVATE_CANARY',repr(received))

    def test_real_fabric_best_setups_and_sdk_chain(self):
        fabric=load('justhodl-signal-fabric');producer=Storage({'data/trend-reversal.json':{'rows':[{'ticker':'QAONLY','reversal_score':30,'direction':'BOTTOM_FORMING'}]}})
        with patch.object(fabric,'s3',producer),contextlib.redirect_stdout(io.StringIO()):fabric.lambda_handler({},None)
        packet=producer.writes['data/feature-bus.json'];sdk,_=self.sdk(packet);sdk.yprice=lambda *a:100
        m=load('justhodl-best-setups');table=Mock();db=ConsumerStorage({'data/feature-bus.json':packet,'data/insider-clusters.json':{'clusters':[{'ticker':'QAONLY','n_insiders':4,'total_value':1000000}]}})
        with patch.object(m,'s3',db),patch.object(m.boto3,'resource',return_value=types.SimpleNamespace(Table=lambda *a:table),create=True),patch.object(m,'_risk_gate_doc',return_value={}),patch.object(m,'load_constitution',return_value={'ok':False}),patch.dict(sys.modules,{'signals_emit':sdk,'wl_fusion':types.SimpleNamespace(load=lambda:{},context=lambda *a:None,multiplier=lambda *a:(1,None))}),contextlib.redirect_stdout(io.StringIO()):
            m.lambda_handler({},None)
        self.assertTrue(table.put_item.called)
        for call in table.put_item.call_args_list:
            md=call.kwargs['Item']['metadata'];self.assertFalse(set(FIELDS)&set(md));self.assertFalse(md['fabric_research']['learning_weight_eligible'])
            self.assertEqual(md['fabric_research']['received']['selected_values']['fabric_score'],Decimal('0.3'))
        self.assertTrue(all(row['fabric_mult']==1 for row in db.writes[m.OUTPUT_KEY]['top_setups']))


if __name__=='__main__':unittest.main(verbosity=2)
