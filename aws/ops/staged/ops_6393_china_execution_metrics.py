"""Read-only, complete AWS execution metrics for China's original daily window.

No application logs, current/private/account/consumer packets, provider requests,
native invokes, writes or schedule changes. AWS execution is not publication.
"""
from pathlib import Path
from datetime import datetime,timedelta,timezone
import hashlib,json,math,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/lambdas/justhodl-china-liquidity/source')]
from ops_6391_china_safe_failure_context import FN,SOURCE_COMMIT,START,END,ReceiptOnly,validate
from market_runtime_evidence import runtime,BUCKET
import china_store as store
METRICS={'Invocations':('Count',('Sum',)),'Errors':('Count',('Sum',)),'Throttles':('Count',('Sum',)),
         'Duration':('Milliseconds',('Minimum','Maximum','Sum','SampleCount'))}
PERIOD=60


def request(name):
    if name not in METRICS:raise ValueError('Reviewed native execution metric required')
    unit,stats=METRICS[name]
    return {'Namespace':'AWS/Lambda','MetricName':name,'Dimensions':[{'Name':'FunctionName','Value':FN}],
            'StartTime':START,'EndTime':END,'Period':PERIOD,'Statistics':list(stats),'Unit':unit}


def number(value,integer=False):
    if type(value) not in (int,float) or not math.isfinite(value) or value<0:
        raise ValueError('Finite nonnegative metric required')
    if integer and (value!=int(value) or value>2**53-1):raise ValueError('Exact metric count required')
    return value


def normalize(name,response):
    unit,stats=METRICS[name]
    if (type(response) is not dict or set(response)!={'Label','Datapoints','ResponseMetadata'} or response.get('Label')!=name
        or type(response.get('Datapoints')) is not list
        or type(response.get('ResponseMetadata')) is not dict
        or type(response['ResponseMetadata'].get('HTTPStatusCode')) is not int
        or response['ResponseMetadata']['HTTPStatusCode']!=200):raise ValueError('Exact successful metric response required')
    rows=response['Datapoints'];bins=int((END-START).total_seconds())//PERIOD
    if len(rows)>bins:raise ValueError('Complete metric population exceeds fixed window')
    seen=set();out=[]
    for row in rows:
        if type(row) is not dict or set(row)!={'Timestamp','Unit',*stats} or row['Unit']!=unit:
            raise ValueError('Exact metric fields and units required')
        at=row['Timestamp']
        if not isinstance(at,datetime) or at.tzinfo is None:raise ValueError('Zoned metric timestamp required')
        at=at.astimezone(timezone.utc)
        if not START<=at<END or at.second or at.microsecond or at in seen:raise ValueError('Unique whole-minute metric population required')
        seen.add(at);parsed={'timestamp':at.isoformat(),'unit':unit}
        for stat in stats:parsed[stat]=number(row[stat],unit=='Count' or stat=='SampleCount')
        if name=='Duration':
            if parsed['SampleCount']<=0 or parsed['Minimum']>parsed['Maximum']:raise ValueError('Complete duration sample required')
            low=parsed['Minimum']*parsed['SampleCount'];high=parsed['Maximum']*parsed['SampleCount']
            if not math.isfinite(low) or not math.isfinite(high):raise ValueError('Finite duration arithmetic required')
            tolerance=1e-9*max(1,abs(low),abs(high),parsed['Sum'])
            if not low-tolerance<=parsed['Sum']<=high+tolerance:raise ValueError('Duration sum conflicts with its sample')
        out.append(parsed)
    out.sort(key=lambda row:row['timestamp'])
    missing=[(START+timedelta(seconds=i*PERIOD)).isoformat() for i in range(bins) if START+timedelta(seconds=i*PERIOD) not in seen]
    total=math.fsum(row['Sum'] for row in out) if out else None
    if total is not None:number(total,unit=='Count')
    result={'metric':name,'unit':unit,'statistics':list(stats),'period_seconds':PERIOD,'datapoints':out,
            'returned_minutes':len(out),'window_minutes':bins,'unreported_minutes':missing,
            'sum_of_reported_points':total,'unreported_minutes_are_zero':False,
            'sdk_datapoints_sha256':hashlib.sha256(store.encode(out)).hexdigest()}
    if name=='Duration':
        samples=math.fsum(row['SampleCount'] for row in out) if out else None
        if samples is not None:number(samples,True)
        result.update(reported_samples=samples,mean_of_reported_samples_ms=total/samples if samples else None)
    return result


def collect(client):
    return {name:normalize(name,client.get_metric_statistics(**request(name))) for name in METRICS}


def check_window(now):
    # The reviewed period is one minute; never silently query rolled-up history.
    if now.tzinfo is None or not END<=now.astimezone(timezone.utc)<=START+timedelta(days=14):
        raise ValueError('Complete original window within one-minute metric retention required')


def summarize(metrics):
    invocations=metrics['Invocations']['sum_of_reported_points']
    return {'execution_observed':True if invocations is not None and invocations>0 else None,
            'publication_verified':False,'schedule_causation_verified':False,'executing_code_identity_verified':False,
            'metric_scope':'FunctionName aggregates versions; counts cannot identify a trigger, executing code revision or successful application publication.',
            'missing_metric_meaning':'Unreported, never inferred as zero. Sum and mean cover returned points only.',
            'caught_error_limit':'The reviewed handler may return statusCode 503 after catching an exception; Lambda Errors alone does not certify application success.'}


def main():
    import boto3
    from ops_report import report
    check_window(datetime.now(timezone.utc))
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN+'/source'],cwd=ROOT,text=True).strip()
    if expected!=SOURCE_COMMIT:raise ValueError('Native source changed; review scope before inspection')
    lam,s3,events,scheduler,cw=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler','cloudwatch')]
    clients=(lam,ReceiptOnly(s3),events,scheduler)
    with report('ops_6393_china_execution_metrics') as report:
        before=runtime(*clients,FN);validate(before);metrics=collect(cw);after=runtime(*clients,FN);validate(after)
        if store.encode(before)!=store.encode(after):raise ValueError('Producer changed during metric verification')
        report.kv(evidence={'source_commit':SOURCE_COMMIT,'native_before':before,'native_after':after,
          'window_start':START.isoformat(),'window_end':END.isoformat(),'namespace':'AWS/Lambda',
          'dimensions':[{'Name':'FunctionName','Value':FN}],'metrics':metrics,'interpretation':summarize(metrics),
          'native_invocations':0,'provider_requests':0,'application_log_reads':0,'current_packet_reads':0,'private_reads':0,
          'account_reads':0,'consumer_reads':0,'native_writes':0,'schedule_changes':0,
          'scope':'Four complete fixed-window AWS execution-statistic responses plus exact native package/resources/schedule controls. No logs or application/source/account/consumer objects.'})


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
