"""Read exact native auction code/resources/cadence before source repairs.

No current packet, archive history, account, outcome, provider, secret or native
invocation is read or executed. Only the two release receipts are permitted S3 reads.
"""
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from market_runtime_evidence import runtime,BUCKET

FUNCTIONS=('justhodl-auction-crisis-detector','justhodl-auction-desk')


class ReceiptsOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**request):
        if set(request)!={'Bucket','Key'} or request['Bucket']!=BUCKET or request['Key'] not in {
                'data/ops/releases/'+fn+'.json' for fn in FUNCTIONS}:
            raise ValueError('Only the two reviewed auction release receipts may be read')
        return self.client.get_object(**request)


def validate(value,function):
    if value.get('function_name')!=function or type(value.get('source_files_checked')) is not int or value['source_files_checked']<3:
        raise ValueError('Whole reviewed native auction source required')
    if type(value.get('memory_mb')) is not int or type(value.get('timeout')) is not int or value['memory_mb']<=0 or value['timeout']<=0:
        raise ValueError('Actual native resources required')
    if not any(row.get('state')=='ENABLED' and row.get('expression') and row.get('native_targets')==1 for row in value.get('schedules',[])):
        raise ValueError('An existing native schedule must be recorded')


def main():
    import boto3
    from ops_report import report
    lam,s3,events,scheduler=[boto3.client(service,region_name='us-east-1') for service in ('lambda','s3','events','scheduler')]
    clients=(lam,ReceiptsOnly(s3),events,scheduler)
    with report('ops_6329_auction_source_runtime_baseline') as r:
        before={fn:runtime(*clients,fn) for fn in FUNCTIONS}
        r.kv(actual_runtimes=before)
        for fn,value in before.items():validate(value,fn)
        after={fn:runtime(*clients,fn) for fn in FUNCTIONS}
        if before!=after:raise ValueError('Auction runtime changed during read-only baseline')
        r.kv(exact_existing_source_packages_verified=True,native_publication_verified=False,
            native_invocations=0,current_packet_reads=0,archive_history_reads=0,private_reads=0,
            provider_requests=0,schedule_changes=0,
            scope='Existing actual source packages, resources, release receipts and all native schedule bindings only. No measurement or forecasting qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
