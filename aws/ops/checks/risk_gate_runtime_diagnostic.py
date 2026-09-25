"""Read-only Risk Gate operation evidence; logs remain private, no invocation."""
from datetime import datetime, timedelta, timezone
import hashlib, json, re

FUNCTION='justhodl-risk-gate'
CONFIG_FIELDS=('FunctionName','FunctionArn','Runtime','Handler','Timeout','MemorySize','CodeSha256','Version','State','LastUpdateStatus','LastModified')
KNOWN=(
 ('source_compiler_mismatch','source compiler differs from reviewed code'),
 ('retained_compiler_mismatch','retained source compiler differs'),
 ('original_reconstruction_mismatch','Original reconstruction differs'),
 ('publication_race','publication race retry limit'),
 ('timeout','Task timed out after'),
 ('import_failure','Runtime.ImportModuleError'),
 ('memory_limit','Runtime.OutOfMemory'),
 ('access_denied','AccessDenied'),
 ('missing_object','NoSuchKey'),
 ('source_binding_mismatch','source content differs'),
)

def summary(events):
    counts={}; hashes=[]; latest=None; reports=[]
    for item in events:
        message=item.get('message',''); stamp=item.get('timestamp')
        if isinstance(stamp,int):latest=max(latest or stamp,stamp)
        for kind,needle in KNOWN:
            if needle in message:counts[kind]=counts.get(kind,0)+1
        if '[ERROR]' in message or 'Traceback' in message or any(n in message for _,n in KNOWN):
            hashes.append({'timestamp':stamp,'message_sha256':hashlib.sha256(message.encode()).hexdigest()})
        if message.startswith('REPORT RequestId:'):
            row={'timestamp':stamp}
            for label,key in (('Duration','duration_ms'),('Memory Size','memory_mb'),('Max Memory Used','max_memory_mb')):
                match=re.search(r'(?:^|\s)'+re.escape(label)+r': ([0-9.]+)',message)
                if match:row[key]=float(match.group(1))
            reports.append(row)
    return {'sampled_events':len(events),'latest_event_timestamp_ms':latest,'known_failure_counts':counts,
            'error_message_hashes':hashes,'runtime_reports':reports,
            'scope':'Bounded recent log sample, not a count of all failures. Raw messages are retained only in the private evidence artifact.'}


def collect(lam,events,scheduler,logs,metrics,now):
    configuration=lam.get_function_configuration(FunctionName=FUNCTION)
    cfg={k:configuration.get(k) for k in CONFIG_FIELDS};arn=cfg['FunctionArn']
    aliases=[];versions=[]
    for page in lam.get_paginator('list_aliases').paginate(FunctionName=FUNCTION):
        aliases.extend({k:a.get(k) for k in ('Name','FunctionVersion','RoutingConfig')} for a in page['Aliases'])
        if len(aliases)>100:raise ValueError('Alias inventory bound exceeded')
    for page in lam.get_paginator('list_versions_by_function').paginate(FunctionName=FUNCTION):
        versions.extend({k:v.get(k) for k in CONFIG_FIELDS} for v in page['Versions'])
        if len(versions)>500:raise ValueError('Version inventory bound exceeded')
    targets={arn}|{arn+':'+a['Name'] for a in aliases}|{arn+':'+v['Version'] for v in versions if v['Version']!='$LATEST'}
    rules=[];seen=set()
    for target in sorted(targets):
        for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=target):
            for name in page['RuleNames']:
                if name in seen:continue
                seen.add(name);rule=events.describe_rule(Name=name);bound=[]
                for ts in events.get_paginator('list_targets_by_rule').paginate(Rule=name):
                    bound.extend({k:t.get(k) for k in ('Id','Arn','RetryPolicy','DeadLetterConfig')} for t in ts['Targets'] if t.get('Arn') in targets)
                rules.append({'name':name,'state':rule['State'],'expression':rule.get('ScheduleExpression'),'targets':bound})
    schedules=[]
    for page in scheduler.get_paginator('list_schedules').paginate(NamePrefix=FUNCTION):
        for item in page.get('Schedules',[]):
            actual=scheduler.get_schedule(Name=item['Name'],GroupName=item['GroupName'])
            schedules.append({k:actual.get(k) for k in ('Name','GroupName','State','ScheduleExpression','ScheduleExpressionTimezone') }|
                {'target_arn':actual.get('Target',{}).get('Arn')})
    streams=logs.describe_log_streams(logGroupName='/aws/lambda/'+FUNCTION,orderBy='LastEventTime',descending=True,limit=12)['logStreams']
    raw=[];streams_meta=[]
    for stream in streams:
        packet=logs.get_log_events(logGroupName='/aws/lambda/'+FUNCTION,logStreamName=stream['logStreamName'],limit=200,startFromHead=False)
        raw.extend(packet['events']);streams_meta.append({'stream_sha256':hashlib.sha256(stream['logStreamName'].encode()).hexdigest(),
            'last_event_timestamp_ms':stream.get('lastEventTimestamp'),'sampled_events':len(packet['events'])})
    metric={}
    for name in ('Invocations','Errors','Throttles'):
        result=metrics.get_metric_statistics(Namespace='AWS/Lambda',MetricName=name,Dimensions=[{'Name':'FunctionName','Value':FUNCTION}],
            StartTime=now-timedelta(days=7),EndTime=now,Period=86400,Statistics=['Sum'])
        metric[name]=sorted([{'timestamp':p['Timestamp'].isoformat(),'sum':p['Sum'],'unit':p['Unit']} for p in result['Datapoints']],key=lambda p:p['timestamp'])
    return {'observed_at':now.isoformat(),'configuration':cfg,'aliases':aliases,'versions':versions,'eventbridge_rules':rules,
        'scheduler_name_prefix':FUNCTION,'schedules':schedules,'metric_window_days':7,'metrics':metric,'log_streams':streams_meta,
        'log_summary':summary(raw)},raw
