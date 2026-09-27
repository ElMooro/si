"""Read-only actual package/alias/schedule and public brief acceptance.

No native invocation, account/learning-log/recipient read, notification,
provider call, schedule change or public/history mutation.
"""
from pathlib import Path
from datetime import datetime, timezone
import hashlib,json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
import morning_free_research as morning
BUCKET='justhodl-dashboard-live'
BASELINE='ccb4e3108429c31638a2af1b20d86fc5623e964e4deea939bb7ac27ab6ef4dbf'
PRIVATE='audit-private/20260909-originals/cycle-consumer-research/'
FUNCTIONS=('justhodl-katlin','justhodl-allocator','justhodl-morning-intelligence')


def unchanged(before,after):
    fields={'Runtime':'runtime','Handler':'handler','MemorySize':'memory_mb','Timeout':'timeout',
            'Architectures':'architectures','Role':'role'}
    for old,new in fields.items():
        if before['runtime'][old]!=after[new]:raise ValueError('Native runtime changed: '+old)
    if before['runtime']['EphemeralStorage']['Size']!=after['ephemeral_storage_mb']:raise ValueError('Native storage changed')
    if before['schedules']!=after['schedules']:raise ValueError('Native schedule changed')


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6194_cycle_consumer_acceptance') as r:
        obj=s3.get_object(Bucket=BUCKET,Key=PRIVATE+BASELINE+'.bin');raw=bounded(obj['Body'])
        if obj['ContentLength']!=len(raw) or hashlib.sha256(raw).hexdigest()!=BASELINE:raise ValueError('Whole protected baseline differs')
        baseline=json.loads(raw);runtimes={}
        for fn in FUNCTIONS:
            expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+fn],cwd=ROOT,text=True).strip()
            actual=runtime(lam,s3,events,scheduler,fn)
            if actual['receipt']!={'status':'matched','commit':expected}:raise ValueError('Exact receipt differs: '+fn)
            unchanged(baseline['consumers'][fn],actual);runtimes[fn]=actual
        subprocess.run([sys.executable,str(ROOT/'tests/synthetic_cycle_consumer_tests.py')],cwd=ROOT,check=True)
        # Fixed public packet only; never execute the notification-producing handler.
        packet=morning.read_public(s3,BUCKET)
        brief=morning.build(lambda key:packet,datetime.now(timezone.utc))
        brief_status='source_backed_public_brief' if brief!=morning.UNAVAILABLE else 'WAIT_public_brief_unavailable'
        if '**DECISIVE CALL: WAIT**' not in brief or len(brief.encode('utf-16-le'))//2>4096:raise ValueError('Delivery loses abstention')
        for fn in FUNCTIONS:
            if runtime(lam,s3,events,scheduler,fn)!=runtimes[fn]:raise ValueError('Runtime changed during acceptance')
        r.kv(baseline_sha256=BASELINE,actual_runtimes=runtimes,
             public_brief={'status':brief_status,'generated_at':packet.get('generated_at'),
                           'generation_method':packet.get('generation_method'),'source_chars':len(packet.get('brief_md') or ''),
                           'delivery_chars':len(brief),'paid_api_calls_by_acceptance':0,'notifications_sent':0},
             synthetic_cycle_qualified_votes=0,model_requests=0,native_invocations=0,
             account_reads=0,learning_log_reads=0,recipient_reads=0,provider_requests=0,
             notifications_sent=0,public_writes=0,history_writes=0,schedules_changed=0,
             scope='Exact deployment and isolated consumer behavior only. Native notification/consumer publication is not forced or read; other legacy allocation rules remain unqualified and under review.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
