"""Qualify post-release public original FI/FX API populations, read-only.

No current/private/account/consumer reads, provider probes, native invocations,
data writes or schedule changes. A pending publication remains pending.
"""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/lambdas/justhodl-fifx-vol-migration/source','aws/shared')]
from ops_6390_fifx_fred_api_acceptance import FN,ReceiptOnly,validate,origin,jsonable
from market_runtime_evidence import runtime,BUCKET
from fifx_archive_acceptance import inspect
from fifx_api_delivery import qualify
import fifx_store as store
import fifx_model as model
SOURCE_COMMIT='1129b6d6a66f4afe9ff0c35f368fc45530a5c964'
CUTOFF='2026-09-30T21:37:13Z'


def main():
    import boto3
    from ops_report import report
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN+'/source'],cwd=ROOT,text=True).strip()
    if expected!=SOURCE_COMMIT:raise ValueError('Reviewed API source release changed')
    clients=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    lam,s3,events,scheduler=clients;scoped=(lam,ReceiptOnly(s3),events,scheduler)
    with report('ops_6392_fifx_api_original_qualification') as report:
        before=runtime(*scoped,FN);validate(before,SOURCE_COMMIT);configured=origin(lam,before)
        compilers={m.__name__:hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest() for m in store.COMPILERS}
        archive=inspect(s3,BUCKET,store,model,cutoff=CUTOFF,checked_at=datetime.now(timezone.utc).isoformat())
        qualification=qualify(archive,compilers,CUTOFF)
        after=runtime(*scoped,FN);validate(after,SOURCE_COMMIT)
        if not store.same_json(before,after) or origin(lam,after)!=configured:raise ValueError('Producer changed during original qualification')
        report.kv(evidence={'source_commit':SOURCE_COMMIT,'cutoff':CUTOFF,'native_before':before,'native_after':after,
          'origin':configured,'expected_compilers':compilers,'complete_original_archive':jsonable(archive),'qualification':qualification,
          'native_invocations':0,'provider_requests':0,'current_packet_reads':0,'private_reads':0,'account_reads':0,'consumer_reads':0,
          'native_writes':0,'archive_writes':0,'schedule_changes':0,
          'scope':'Complete content-addressed public original archive, exact package/resources/schedule and independent calculation replay only. Before a post-release run exists, inspect complete original manifest inventory only. No current pointer, causal trigger, first-release, forecast or investment authority claim.'})


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
