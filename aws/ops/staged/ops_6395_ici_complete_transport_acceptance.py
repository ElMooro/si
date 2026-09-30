"""Read-only ICI complete-transport native package/resource acceptance.

Only exact public release receipts and native package/control metadata are read.
No current/private/account/consumer objects, provider requests, native invokes,
data writes, schedule changes or normal-publication claims.
"""
from pathlib import Path
import json,re,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import runtime,BUCKET
FN='justhodl-ici-flows'

class ReceiptOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**kw):
        if kw!={'Bucket':BUCKET,'Key':'data/ops/releases/'+FN+'.json'}:raise ValueError('Exact public native receipt only')
        return self.client.get_object(**kw)

def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)

def baseline():
    prior=json.loads((ROOT/'docs/audit/2026-09-26/ici-native-acceptance.json').read_bytes())['aws']
    before=prior['actual_runtime'];schedule=prior['schedule_after']
    fields=('function_name','runtime','handler','timeout','memory_mb','architectures','role','ephemeral_storage_mb')
    wanted={key:before[key] for key in fields}
    old=before['schedules'];assert len(old)==1 and old[0]['name']==schedule['Name'] and old[0]['expression']==schedule['ScheduleExpression']
    wanted['schedules']=[{**old[0],'state':schedule['State']}]
    return wanted

def validate(actual,expected):
    if type(expected) is not str or not re.fullmatch('[a-f0-9]{40}',expected):raise ValueError('Exact native source commit required')
    if actual['receipt']!={'status':'matched','commit':expected} or type(actual['source_files_checked']) is not int or actual['source_files_checked']!=5:
        raise ValueError('Exact five-source native package required')
    wanted=baseline()
    if encoded({key:actual[key] for key in wanted})!=encoded(wanted):raise ValueError('Original runtime or schedule differs')

def main():
    import boto3
    from ops_report import report
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN+'/source'],cwd=ROOT,text=True).strip()
    if expected=='0935a93566034d57ee4069161378ce13f62e907d':raise ValueError('New complete-transport source release required')
    lam,s3,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    clients=(lam,ReceiptOnly(s3),events,scheduler)
    with report('ops_6395_ici_complete_transport_acceptance') as out:
        before=runtime(*clients,FN);validate(before,expected)
        after=runtime(*clients,FN);validate(after,expected)
        if encoded(before)!=encoded(after):raise ValueError('Native producer changed during verification')
        out.kv(evidence={'expected_commit':expected,'native_before':before,'native_after':after,
          'normal_publication_verified':False,'source_replay_verified':False,'investment_authority':False,
          'native_invocations':0,'provider_requests':0,'current_packet_reads':0,'private_reads':0,'account_reads':0,
          'consumer_reads':0,'native_writes':0,'schedule_changes':0,'application_log_queries':0,
          'scope':'Exact deployed package and original resources/schedule only. No actual packet, archive or private journal is read. Deployment does not establish the next scheduled publication or original-source qualification.'})

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
