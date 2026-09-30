from pathlib import Path
from copy import deepcopy
import importlib.util,json,sys,unittest
R=Path(__file__).resolve().parents[2]
p=R/'aws/ops/staged/ops_6391_china_safe_failure_context.py'
spec=importlib.util.spec_from_file_location('safe_china_context',p);op=importlib.util.module_from_spec(spec);spec.loader.exec_module(op)

def event():
    return {'eventId':'invented-only','timestamp':int(op.START.timestamp()*1000)+15000,
      'message':op.PREFIX+' '+json.dumps({'contract':'china-publication-failure.v1','stage':'calculate_sources','exception_class':'EvidenceError',
       'reason':'Complete attempt retention failed','started_at':'2026-09-30T14:30:00Z','retained_attempts':5,'staged_outputs':[], 'last_attempt_sha256':'a'*64})+'\n'}
class Tests(unittest.TestCase):
    def test_whole_safe_context_remains_typed_and_bound_to_fixed_original_window(self):
        e=event();r=op.parse_event(e);self.assertEqual(r['diagnostic']['retained_attempts'],5)
        self.assertEqual(r['timestamp_ms'],e['timestamp']);self.assertEqual(r['message_bytes'],len(e['message'].encode()))
        self.assertEqual(len(r['message_sha256']),64)
    def test_extra_fields_arbitrary_reasons_bad_dates_and_untyped_counts_are_refused_without_echo(self):
        for key,value in [('private_payload','invented-sensitive'),('reason','invented-sensitive'),('started_at','invented-sensitive'),('retained_attempts',True),
                          ('retained_attempts',65),('staged_outputs',['audit-private/invented-sensitive']),('service_error_code','invented-sensitive'),
                          ('last_attempt_sha256','invented-sensitive'),('publication_key','portfolio/snapshot.json'),('stage','invented-sensitive')]:
            e=event();d=json.loads(e['message'].split(op.PREFIX+' ',1)[1]);d[key]=value;e['message']=op.PREFIX+' '+json.dumps(d)
            with self.subTest(key=key),self.assertRaises(ValueError) as raised:op.parse_event(e)
            self.assertNotIn('invented-sensitive',str(raised.exception))
        for mutate in [lambda e:e.update(timestamp=True),lambda e:e.update(timestamp=int(op.END.timestamp()*1000)),lambda e:e.update(message=e['message']+' extra')]:
            e=event();mutate(e)
            with self.assertRaises(ValueError):op.parse_event(e)
    def test_complete_paginated_events_dedupe_identical_ids_but_conflicts_refuse(self):
        first=event();second=deepcopy(first);second.update(eventId='invented-second',timestamp=first['timestamp']+1)
        class Logs:
            def get_paginator(self,name):self.name=name;return self
            def paginate(self,**kw):self.args=kw;return [{'events':[first]},{'events':[first,second]}]
        client=Logs();result=op.collect(client);self.assertEqual(len(result),2)
        self.assertEqual(client.name,'filter_log_events');self.assertEqual(client.args['logGroupName'],'/aws/lambda/'+op.FN)
        self.assertEqual(client.args['filterPattern'],'"'+op.PREFIX+'"')
        second['eventId']=first['eventId']
        with self.assertRaisesRegex(ValueError,'Conflicting'):op.collect(client)
    def test_no_matching_diagnostic_never_becomes_publication_success_and_only_receipt_is_readable(self):
        class Logs:
            def get_paginator(self,name):return self
            def paginate(self,**kw):return [{'events':[]}]
        self.assertEqual(op.collect(Logs()),[])
        class Client:
            def get_object(self,**kw):return kw
        receipt=op.ReceiptOnly(Client())
        self.assertEqual(receipt.get_object(Bucket=op.BUCKET,Key='data/ops/releases/'+op.FN+'.json')['Bucket'],op.BUCKET)
        for key in ('data/china-liquidity.json','audit-private/source.bin','portfolio/snapshot.json'):
            with self.assertRaises(ValueError):receipt.get_object(Bucket=op.BUCKET,Key=key)
    def test_population_bound_refuses_instead_of_truncating(self):
        rows=[]
        for i in range(129):e=event();e['eventId']=str(i);rows.append(e)
        class Logs:
            def get_paginator(self,name):return self
            def paginate(self,**kw):return [{'events':rows}]
        with self.assertRaisesRegex(ValueError,'population'):op.collect(Logs())


def test_whole_safe_context_remains_typed_and_bound_to_fixed_original_window():
    Tests().test_whole_safe_context_remains_typed_and_bound_to_fixed_original_window()

def test_extra_fields_arbitrary_reasons_bad_dates_and_untyped_counts_are_refused_without_echo():
    Tests().test_extra_fields_arbitrary_reasons_bad_dates_and_untyped_counts_are_refused_without_echo()

def test_complete_paginated_events_dedupe_identical_ids_but_conflicts_refuse():
    Tests().test_complete_paginated_events_dedupe_identical_ids_but_conflicts_refuse()

def test_no_matching_diagnostic_never_becomes_publication_success_and_only_receipt_is_readable():
    Tests().test_no_matching_diagnostic_never_becomes_publication_success_and_only_receipt_is_readable()

def test_population_bound_refuses_instead_of_truncating():
    Tests().test_population_bound_refuses_instead_of_truncating()

def test_native_identity_and_original_resources_must_match_before_log_acceptance():
    from copy import deepcopy
    baseline=json.loads((R/'docs/audit/2026-09-27/china-original-baseline.json').read_bytes())['actual_producer'];cfg=baseline['runtime']
    actual={key:cfg[name] for key,name in {'function_name':'FunctionName','runtime':'Runtime','handler':'Handler','timeout':'Timeout','memory_mb':'MemorySize','architectures':'Architectures','role':'Role'}.items()}
    actual.update(ephemeral_storage_mb=cfg['EphemeralStorage']['Size'],schedules=baseline['schedules'],source_files_checked=4,receipt={'status':'matched','commit':op.SOURCE_COMMIT})
    op.validate(actual)
    for change in ({'source_files_checked':4.0},{'source_files_checked':3},{'timeout':120.0},{'memory_mb':257},{'receipt':{'status':'matched','commit':'a'*40}}):
        with Tests().assertRaises((ValueError,KeyError,TypeError)):op.validate({**actual,**change})
    changed=deepcopy(actual);changed['schedules'][0]['expression']='rate(1 minute)'
    with Tests().assertRaises(ValueError):op.validate(changed)
