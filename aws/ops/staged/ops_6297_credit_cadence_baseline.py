"""Read-only credit package/cadence and complete retained-source baseline.

No native invocation, provider request, credential lookup, private account read,
history write, notification or schedule change. Licensed originals stay private.
"""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,sys

ROOT=Path(__file__).resolve().parents[3]
FN='justhodl-credit-stress'
EXPECTED='9cb4a62b116865bbf9695493bcff820c87da9277'
CRON='cron(10 22 ? * MON-FRI *)'


def stamp(value):
    if not isinstance(value,str):raise ValueError('Explicit clock required')
    at=datetime.fromisoformat(value.replace('Z','+00:00'))
    if at.tzinfo is None:raise ValueError('Timezone required')
    return at.astimezone(timezone.utc)


def diagnosis(packet,at):
    generated=stamp(packet['generated_at']);due=stamp(packet['freshness']['pipeline_check_due_at'])
    valid=stamp(packet['freshness']['valid_until'])
    if generated>at or due<=generated or valid>due:raise ValueError('Publication clock ordering differs')
    rows=packet['measurements'];dates=[stamp(m['observation_date']+'T00:00:00Z').date() for m in rows.values() if m.get('observation_date')]
    return {'generated_at':packet['generated_at'],'as_of':packet.get('as_of'),'evaluated_at':at.isoformat(),
        'pipeline_check_due_at':due.isoformat(),'valid_until':valid.isoformat(),
        'pipeline_due_weekday_utc':due.weekday(),'declared_native_weekdays_utc':[0,1,2,3,4],
        'pipeline_deadline_on_unscheduled_day':due.weekday()>4,
        'pipeline_expired_at_evaluation':at>=due,'packet_expired_at_evaluation':at>=valid,
        'collection_age_hours':round((at-generated).total_seconds()/3600,6),
        'measurement_rows':len(rows),'dated_rows':len(dates),
        'rows_within_existing_five_calendar_day_ceiling':sum(0<=(at.date()-day).days<=5 for day in dates),
        'quality_status_at_publication':packet.get('quality',{}).get('status'),
        'currentness_not_extended':True,'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False}


def validate_runtime(value):
    if value['receipt']!={'status':'matched','commit':EXPECTED}:raise ValueError('Exact predecessor receipt required')
    if value['function_name']!=FN or value['timeout']!=300 or value['memory_mb']!=512:raise ValueError('Original credit resources differ')
    active=[row for row in value['schedules'] if row['state']=='ENABLED']
    if len(active)!=1 or active[0]['expression']!=CRON or active[0]['native_targets']!=1 or active[0].get('timezone','UTC')!='UTC':
        raise ValueError('One original weekday 22:10 UTC schedule required')


def main():
    import boto3
    sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/lambdas/justhodl-credit-stress/source','aws/shared','scripts')]
    from ops_report import report
    from market_runtime_evidence import runtime
    import credit_research_store as store
    from replay_credit_research import verify
    clients={name:boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')}
    lam,s3,events,scheduler=(clients[name] for name in ('lambda','s3','events','scheduler'))
    with report('ops_6297_credit_cadence_baseline') as r:
        before=runtime(lam,s3,events,scheduler,FN);validate_runtime(before)
        read=store.reader(s3,'justhodl-dashboard-live');raw=read(store.CURRENT);packet=json.loads(raw)
        reproduced=verify(packet,read)
        finding=diagnosis(packet,datetime.now(timezone.utc))
        if read(store.CURRENT)!=raw:raise ValueError('Public head changed during baseline; no mixed snapshot accepted')
        if runtime(lam,s3,events,scheduler,FN)!=before:raise ValueError('Credit runtime changed during baseline')
        r.kv(expected_commit=EXPECTED,actual_runtime=before,public_head={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()},
             retained_original_replay=reproduced,cadence_diagnosis=finding,
             native_invocations=0,provider_requests=0,credential_reads=0,private_account_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
             scope='Exact predecessor code/cadence and replay of its retained original public-provider inputs. No expiry extension, new release, independent evidence or investment authority.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
