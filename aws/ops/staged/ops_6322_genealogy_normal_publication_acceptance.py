"""Read-only whole Genealogy replay after its original scheduled publication.

All original reads are the reviewed producer's new public retained namespace.
No upstream output, learning ledger, account data, provider or native invocation.
"""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,re,subprocess,sys,tempfile
ROOT=Path(__file__).resolve().parents[3]
FN='justhodl-signal-genealogy'
sys.path[:0]=[str(ROOT/'aws/lambdas'/FN/'source'),str(ROOT/'aws/shared'),str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
import genealogy_native_publication as publication
import genealogy_cache_checkpoint as checkpoint
import genealogy_public_archive as archive
from market_runtime_evidence import runtime


class PublicRetainedOnly:
    def __init__(self,client):self.client=client;self.keys=set();self.head_metadata=None
    def get_object(self,**kw):
        key=kw.get('Key')
        cached=type(key) is str and re.fullmatch(re.escape(checkpoint.PREFIX)+r'(chunks|snapshots)/[a-f0-9]{64}\.json',key)
        if kw.get('Bucket')!=archive.BUCKET or not (publication.allowed(key) or cached):
            raise ValueError('Only reviewed public Genealogy artifacts permitted')
        value=self.client.get_object(**kw);self.keys.add(key)
        if key==publication.CURRENT:self.head_metadata={k:value.get(k) for k in ('LastModified','ContentLength','ETag')}
        return value


def validate_head(head,modified,now):
    if type(head) is not dict or head.get('contract')!='genealogy-research-head.v1' or head.get('version')!='2.0.0':
        raise ValueError('Original scheduled research publication pending')
    at=archive.clock(head['generated_at']);first=datetime(2026,9,29,6,40,tzinfo=timezone.utc)
    if not 0<=(at-at.replace(hour=6,minute=40,second=0,microsecond=0)).total_seconds()<60:
        raise ValueError('Publication outside original scheduled collection window')
    if at<first or at>now or not isinstance(modified,datetime) or modified.tzinfo is None or modified<at or modified>now:
        raise ValueError('Normal publication clocks differ')
    if (now-at).total_seconds()>26*3600 or (modified-at).total_seconds()>600:
        raise ValueError('Native publication age/runtime window exceeded')
    expected={'calls_eligible':False,'execution_eligible':False,'forecast_qualified':False,'sizing_eligible':False}
    if head.get('authority')!=expected or head.get('independent_evidence_count') is not None or head.get('original_engine_replay_verified') is not False:
        raise ValueError('Unexpected investment or source qualification')
    publication.typed_ref(head['replay'],'runs')
    return {'cutoff':at.isoformat(),'storage_last_modified':modified.isoformat(),'publication_elapsed_upper_bound_s':(modified-at).total_seconds()}


def main():
    import boto3
    from ops_report import report
    baseline=json.loads((ROOT/'docs/audit/2026-09-28/genealogy-native-runtime-acceptance.json').read_bytes())['evidence']['actual_runtime']
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
    clients=[boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')]
    with report('ops_6322_genealogy_normal_publication_acceptance') as r:
        before=runtime(*clients,FN);r.kv(actual_runtime=before,expected_commit=expected)
        if before!=baseline or before['receipt']!={'status':'matched','commit':expected}:
            raise ValueError('Reviewed complete native runtime differs')
        client=PublicRetainedOnly(clients[1]);raw,_=publication.read_current(client)
        head=archive.strict_json(raw);clocks=validate_head(head,client.head_metadata['LastModified'],datetime.now(timezone.utc))
        with tempfile.TemporaryDirectory(prefix='genealogy-native-acceptance-') as directory:
            reproduced=publication.replay(client,head['replay'],Path(directory)/'replayed')
        if publication.canonical(reproduced)!=raw:raise ValueError('Whole public head differs from complete retained replay')
        if publication.read_current(client)[0]!=raw:raise ValueError('Current publication changed during acceptance')
        if runtime(*clients,FN)!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(status='complete_normal_publication_retained_originals_and_calculation_replayed',
            publication={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),**clocks},
            coverage=head['coverage'],comparison_status_counts=head['comparison_status_counts'],
            retained_public_keys_read=len(client.keys),complete_native_storage_replay_verified=True,
            future_archive_growth_qualified=False,original_engine_replay_verified=False,
            native_invocations=0,provider_requests=0,learning_ledger_reads=0,private_account_reads=0,
            downstream_output_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
            calls_eligible=False,sizing_eligible=False,
            scope='Exact original-cadence native package and whole new public research head reproduce from all typed retained originals and reviewed compiler bytes. Publication fits its reviewed 600-second window. Peak native resource measurements, future archive growth, original model replay, independence and investment qualification are not inferred.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
