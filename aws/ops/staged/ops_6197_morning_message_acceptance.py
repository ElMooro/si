"""Read-only complete-message acceptance; no recipient or notification access."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
import morning_free_research as morning
BUCKET='justhodl-dashboard-live';FN='justhodl-morning-intelligence'
BASELINE='ccb4e3108429c31638a2af1b20d86fc5623e964e4deea939bb7ac27ab6ef4dbf'
PRIVATE='audit-private/20260909-originals/cycle-consumer-research/'


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6197_morning_message_acceptance') as r:
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN,'aws/shared/morning_free_research.py'],cwd=ROOT,text=True).strip()
        before=runtime(lam,s3,events,scheduler,FN)
        if before['receipt']!={'status':'matched','commit':expected}:raise ValueError('Exact source/helper receipt required')
        obj=s3.get_object(Bucket=BUCKET,Key=PRIVATE+BASELINE+'.bin');raw=bounded(obj['Body'])
        if obj['ContentLength']!=len(raw) or hashlib.sha256(raw).hexdigest()!=BASELINE:raise ValueError('Protected baseline differs')
        original=json.loads(raw)['consumers'][FN]
        for old,new in {'Runtime':'runtime','Handler':'handler','MemorySize':'memory_mb','Timeout':'timeout','Architectures':'architectures','Role':'role'}.items():
            if original['runtime'][old]!=before[new]:raise ValueError('Native runtime changed: '+old)
        if original['runtime']['EphemeralStorage']['Size']!=before['ephemeral_storage_mb'] or original['schedules']!=before['schedules']:
            raise ValueError('Native storage or cadence changed')
        subprocess.run([sys.executable,str(ROOT/'tests/synthetic_cycle_consumer_tests.py')],cwd=ROOT,check=True)
        # Only the fixed public research object is read. The actual native
        # handler is never invoked; no account/recipient/log is inspected.
        packet=morning.read_public(s3,BUCKET)
        message=morning.build(lambda key:packet,datetime.now(timezone.utc))
        units=len(message.encode('utf-16-le'))//2
        if units>4096 or '**DECISIVE CALL: WAIT**' not in message:raise ValueError('Complete message loses abstention or exceeds limit')
        if runtime(lam,s3,events,scheduler,FN)!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,baseline_sha256=BASELINE,
             public_brief={'generated_at':packet.get('generated_at'),'source_chars':len(packet.get('brief_md') or ''),
                           'delivery_utf16_units':units,'delivery_limit_utf16_units':4096,
                           'status':'WAIT_public_brief_unavailable' if message==morning.UNAVAILABLE else 'source_backed_public_brief'},
             synthetic_regressions='Complete final-message limit, astral Unicode, exact 4096 boundary, over-limit link, malformed/oversized publication timestamp, stale/paid/actionable packet rejection.',
             model_requests=0,native_invocations=0,account_reads=0,learning_log_reads=0,recipient_reads=0,
             notifications_sent=0,public_writes=0,history_writes=0,schedules_changed=0,
             scope='Exact deployed package and isolated complete-message construction. No live notification or recipient delivery is claimed.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
