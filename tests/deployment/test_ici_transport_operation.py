"""Read-only exact native ICI operation gates, using invented control evidence."""
from pathlib import Path
from copy import deepcopy
from unittest.mock import patch
from types import SimpleNamespace
import importlib.util,sys
R=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('ici_transport_acceptance',R/'aws/ops/staged/ops_6395_ici_complete_transport_acceptance.py');op=importlib.util.module_from_spec(spec);spec.loader.exec_module(op)

def actual():return {**op.baseline(),'receipt':{'status':'matched','commit':'b'*40},'source_files_checked':5,'code_sha256':'invented-safe-package'}
def refuse(fn):
    try:fn()
    except (ValueError,TypeError,KeyError):return
    raise AssertionError('Invalid native evidence accepted')

def test_original_enabled_cadence_and_resources_are_required():
    d=actual();op.validate(d,'b'*40)
    assert d['memory_mb']==256 and d['timeout']==120 and d['ephemeral_storage_mb']==512
    assert d['schedules']==[{'kind':'EventBridge rule','name':'justhodl-ici-flows-weekly','state':'ENABLED','expression':'cron(30 16 ? * WED,THU *)','native_targets':1}]
    for change in ({'memory_mb':257},{'timeout':121},{'schedules':[]},{'schedules':[{**d['schedules'][0],'state':'DISABLED'}]}):refuse(lambda:op.validate({**d,**change},'b'*40))

def test_exact_receipt_commit_complete_source_population_and_types_are_required():
    d=actual()
    for change in ({'source_files_checked':5.0},{'source_files_checked':4},{'timeout':120.0},{'memory_mb':True},{'receipt':{'status':'matched','commit':'c'*40}}):refuse(lambda:op.validate({**d,**change},'b'*40))
    for expected in (None,True,'main','b'*39):refuse(lambda:op.validate(d,expected))

def test_only_this_exact_public_receipt_can_be_read():
    seen=[]
    class Client:
        def get_object(self,**kw):seen.append(kw);return kw
    client=op.ReceiptOnly(Client());client.get_object(Bucket=op.BUCKET,Key='data/ops/releases/'+op.FN+'.json')
    for key in ('data/ici-flows.json','data/ici-research/runs/'+'a'*64+'.json','audit-private/original.bin','portfolio/snapshot.json'):
        refuse(lambda:client.get_object(Bucket=op.BUCKET,Key=key))
    refuse(lambda:client.get_object(Bucket=op.BUCKET,Key='data/ops/releases/'+op.FN+'.json',Range='bytes=0-99'))
    assert len(seen)==1

class Report:
    def __init__(self):self.rows=[]
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def kv(self,**kwargs):self.rows.append(kwargs)

def execute(states,commit='b'*40):
    reporter=Report();clients=[]
    def client(name,**kw):clients.append((name,kw));return SimpleNamespace(name=name)
    with patch.dict(sys.modules,{'boto3':SimpleNamespace(client=client),'ops_report':SimpleNamespace(report=lambda name:reporter)}),patch.object(op.subprocess,'check_output',return_value=commit),patch.object(op,'runtime',side_effect=states):op.main()
    return reporter,clients

def test_whole_native_verification_never_constructs_log_or_provider_clients():
    d=actual();report,clients=execute([d,deepcopy(d)])
    assert [name for name,kw in clients]==['lambda','s3','events','scheduler']
    e=report.rows[0]['evidence'];assert e['native_before']==e['native_after']==d
    for key in ('normal_publication_verified','source_replay_verified','investment_authority'):assert e[key] is False
    for key in ('native_invocations','provider_requests','current_packet_reads','private_reads','account_reads','consumer_reads','native_writes','schedule_changes','application_log_queries'):assert type(e[key]) is int and e[key]==0

def test_control_plane_change_during_verification_cannot_produce_acceptance():
    d=actual();after=deepcopy(d);after['code_sha256']='different'
    refuse(lambda:execute([d,after]))

def test_predecessor_release_cannot_be_mislabeled_as_the_transport_repair():
    refuse(lambda:execute([],commit='0935a93566034d57ee4069161378ce13f62e907d'))
