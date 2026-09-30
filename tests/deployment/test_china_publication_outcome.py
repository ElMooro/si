from pathlib import Path
from copy import deepcopy
from datetime import timedelta
import importlib.util,json
R=Path(__file__).resolve().parents[2]
p=R/'aws/ops/staged/ops_6394_china_publication_outcome_acceptance.py'
spec=importlib.util.spec_from_file_location('china_publication_outcome',p);op=importlib.util.module_from_spec(spec);spec.loader.exec_module(op)
COMPILERS={name:'a'*64 for name in op.store.COMPILERS}


def event():
    row={'contract':'china-publication-outcome.v1','status':'producer_readbacks_verified','calculation_at':op.START.isoformat(),
      'verified_at':(op.START+timedelta(seconds=10)).isoformat(),'request_id':'12345678-1234-1234-1234-123456789abc','function_version':'$LATEST',
      'compiler_sha256':dict(COMPILERS),'provider_attempts':30,
      'outputs':[{'key':key,'bytes':2,'sha256':op.store.sha(b'{}')} for key in sorted((op.store.HEAD,op.store.KEYS[1]))],
      **dict.fromkeys(op.FLAGS,False)}
    return {'eventId':'invented','timestamp':int((op.START+timedelta(seconds=11)).timestamp()*1000),'message':op.PREFIX+' '+op.store.encode(row).decode()+'\n'}


def change_event(**changes):
    e=event();row=op.store.strict(e['message'].split(op.PREFIX+' ',1)[1].encode());row.update(changes);e['message']=op.PREFIX+' '+op.store.encode(row).decode();return e


def refuse(fn):
    try:fn()
    except (ValueError,TypeError,KeyError) as exc:
        assert 'invented-sensitive' not in str(exc);return
    raise AssertionError('Unreviewed witness accepted')


def test_whole_witness_binds_all_outputs_clocks_and_compilers_without_independent_authority():
    e=event();row=op.parse_event(e,COMPILERS)
    assert row['message_bytes']==len(e['message'].encode()) and row['message_sha256']==op.store.sha(e['message'].encode())
    assert row['witness']['compiler_sha256']==COMPILERS and len(row['witness']['outputs'])==2
    assert all(row['witness'][key] is False for key in op.FLAGS)
    optional=change_event(request_id=None,function_version=None);assert op.parse_event(optional,COMPILERS)['witness']['request_id'] is None


def test_added_sensitive_fields_missing_duplicate_and_wrong_output_references_refuse():
    for key,value in [('private','invented-sensitive'),('provider_attempts',True),('investment_authority',True),('request_id','invented-sensitive'),('function_version','invented-sensitive')]:
        refuse(lambda:op.parse_event(change_event(**{key:value}),COMPILERS))
    row=op.parse_event(event(),COMPILERS)['witness'];outputs=row['outputs']
    for bad in (outputs[:1],outputs+[outputs[0]],[{**outputs[0],'key':'portfolio/snapshot.json'},outputs[1]],
                [{**outputs[0],'bytes':True},outputs[1]],[{**outputs[0],'sha256':'invalid'},outputs[1]]):
        refuse(lambda:op.parse_event(change_event(outputs=bad),COMPILERS))


def test_wrong_clock_native_identity_and_malformed_json_never_echo_payloads():
    for changes in ({'calculation_at':(op.START-timedelta(seconds=1)).isoformat()}, {'verified_at':op.END.isoformat()},
                    {'verified_at':(op.START+timedelta(seconds=12)).isoformat()}, {'verified_at':'invented-sensitive'},
                    {'compiler_sha256':{**COMPILERS,'china_store.py':'b'*64}}):
        refuse(lambda:op.parse_event(change_event(**changes),COMPILERS))
    for e in ({**event(),'timestamp':True},{**event(),'timestamp':int(op.END.timestamp()*1000)},
              {**event(),'message':op.PREFIX+' invented-sensitive'},{**event(),'message':event()['message']+' unexpected'}):
        refuse(lambda:op.parse_event(e,COMPILERS))


def test_all_matching_pages_preserved_and_duplicate_event_conflicts_refused():
    first=event();second=deepcopy(first);second.update(eventId='second',timestamp=first['timestamp']+1)
    class Logs:
        def get_paginator(self,name):assert name=='filter_log_events';return self
        def paginate(self,**kw):self.args=kw;return [{'events':[first]},{'events':[first,second]}]
    client=Logs();rows=op.collect(client,COMPILERS);assert len(rows)==2
    assert client.args=={'logGroupName':'/aws/lambda/'+op.FN,'startTime':int(op.START.timestamp()*1000),'endTime':int(op.END.timestamp()*1000),'filterPattern':'"'+op.PREFIX+'"'}
    second['eventId']='invented';refuse(lambda:op.collect(client,COMPILERS))


def test_before_window_close_no_log_client_or_query_is_created_and_absence_is_unknown():
    def forbidden():raise AssertionError('Premature actual log query')
    result=op.observe(op.END-timedelta(seconds=1),forbidden,COMPILERS)
    assert result=={'status':'pending_original_window','complete_matching_witnesses':[],'application_log_query_count':0}
    class Logs:
        def get_paginator(self,name):return self
        def paginate(self,**kw):return [{'events':[]}]
    result=op.observe(op.END,lambda:Logs(),COMPILERS)
    assert result=={'status':'no_reviewed_witness_found','complete_matching_witnesses':[],'application_log_query_count':1}


def test_complete_population_bound_refuses_instead_of_truncating():
    rows=[{**event(),'eventId':str(i)} for i in range(129)]
    class Logs:
        def get_paginator(self,name):return self
        def paginate(self,**kw):return [{'events':rows}]
    refuse(lambda:op.collect(Logs(),COMPILERS))


def test_only_exact_receipt_and_original_typed_runtime_settings_are_accepted():
    class Client:
        def get_object(self,**kw):return kw
    client=op.ReceiptOnly(Client());assert client.get_object(Bucket=op.BUCKET,Key='data/ops/releases/'+op.FN+'.json')['Bucket']==op.BUCKET
    for key in ('data/china-liquidity.json','audit-private/original.bin','portfolio/snapshot.json'):
        refuse(lambda:client.get_object(Bucket=op.BUCKET,Key=key))
    baseline=json.loads((R/'docs/audit/2026-09-27/china-original-baseline.json').read_bytes())['actual_producer'];cfg=baseline['runtime']
    actual={k:cfg[v] for k,v in {'function_name':'FunctionName','runtime':'Runtime','handler':'Handler','timeout':'Timeout','memory_mb':'MemorySize','architectures':'Architectures','role':'Role'}.items()}
    actual.update(ephemeral_storage_mb=cfg['EphemeralStorage']['Size'],schedules=baseline['schedules'],source_files_checked=4,receipt={'status':'matched','commit':'b'*40})
    op.validate_native(actual,'b'*40)
    for change in ({'source_files_checked':4.0},{'timeout':120.0},{'memory_mb':257},{'receipt':{'status':'matched','commit':'c'*40}}):
        refuse(lambda:op.validate_native({**actual,**change},'b'*40))


def test_logging_scope_is_only_this_native_function_and_never_reports_environment():
    value={'FunctionName':op.FN,'CodeSha256':'invented-hash','LoggingConfig':{'LogGroup':'/aws/lambda/'+op.FN,'LogFormat':'Text'},'Environment':{'Variables':{'secret':'invented-sensitive'}}}
    class Lambda:
        def get_function_configuration(self,**kw):assert kw=={'FunctionName':op.FN};return value
    result=op.logging_scope(Lambda(),{'code_sha256':'invented-hash'});assert result['group']=='/aws/lambda/'+op.FN and 'invented-sensitive' not in str(result)
    for change in ({'FunctionName':'wrong'},{'CodeSha256':'wrong'},{'LoggingConfig':{'LogGroup':'unreviewed','LogFormat':'Text'}}):
        before=deepcopy(value);value.update(change);refuse(lambda:op.logging_scope(Lambda(),{'code_sha256':'invented-hash'}));value.clear();value.update(before)
