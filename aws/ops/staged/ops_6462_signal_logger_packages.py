"""Read-only predecessor package reconciliation for the named SDK importers.

Only named code packages, their release receipts and control/schedule metadata
are read. No producer, application packet, ledger, provider or settings write.
Never deploy the shared SDK over an unexplained native/source mismatch.
"""
from pathlib import Path
import hashlib,json,runpy,sys

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import runtime,BUCKET
ScheduleInventory=runpy.run_path(str(ROOT/'aws/ops/staged/ops_6461_signal_logger_controls.py'))['ScheduleInventory']
BASELINE=json.loads((ROOT/'docs/audit/2026-10-02/signal-logger-baseline.json').read_bytes())['functions']
SOURCES=json.loads((ROOT/'docs/audit/2026-10-02/signal-logger-predecessor-sources.json').read_bytes())['functions']


class ReceiptOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**kwargs):
        if kwargs not in [{'Bucket':BUCKET,'Key':'data/ops/releases/'+fn+'.json'} for fn in SOURCES]:
            raise ValueError('Named release receipt only')
        return self.client.get_object(**kwargs)


def validate_sources():
    if set(BASELINE)!=set(SOURCES) or len(SOURCES)!=17:
        raise ValueError('Complete named 17-function predecessor scope required')
    for members in SOURCES.values():
        if not members:raise ValueError('Tracked predecessor members required')
        for path,expected in members.items():
            if hashlib.sha256((ROOT/path).read_bytes()).hexdigest()!=expected:
                raise ValueError('Reviewed predecessor source changed')


def inspect(lam,s3,events,scheduler):
    result={}
    inventory=ScheduleInventory(scheduler)
    for fn,members in SOURCES.items():
        try:
            value=runtime(lam,ReceiptOnly(s3),events,inventory,fn)
            if (value.get('function_name')!=fn or value.get('code_sha256')!=BASELINE[fn]['CodeSha256']
                or type(value.get('source_files_checked')) is not int or value['source_files_checked']!=len(members)):
                raise ValueError('Named stable predecessor identity differs')
            after=lam.get_function_configuration(FunctionName=fn)
            if (after.get('FunctionName')!=fn or after.get('State')!='Active'
                or after.get('LastUpdateStatus')!='Successful' or after.get('CodeSha256')!=value['code_sha256']):
                raise ValueError('Named predecessor changed during comparison')
            result[fn]={'matched':True,'evidence':value}
        except Exception as exc:
            # Signed package URLs and native error bodies must never escape.
            result[fn]={'matched':False,'error_type':type(exc).__name__}
    return {'status':'named_predecessor_source_comparison',
            'all_matched':len(result)==len(SOURCES) and all(row['matched'] for row in result.values()),
            'functions':result,'source_members_planned':sum(len(v) for v in SOURCES.values()),
            'native_invocations':0,'provider_requests':0,'application_packet_reads':0,'private_reads':0,
            'account_reads':0,'ledger_reads':0,'native_writes':0,'schedule_changes':0,
            'normal_publication_verified':False,'source_qualified':False,'investment_authority':False}


def main():
    import boto3
    from ops_report import report
    with report('ops_6462_signal_logger_packages') as out:
        validate_sources()
        clients=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
        evidence=inspect(*clients);out.kv(evidence=evidence)
        if not evidence['all_matched']:
            raise ValueError('At least one named predecessor package requires reconciliation')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
