"""Read-only trigger discovery without exposing target payloads or policies."""


def matches(arn, target):
    return isinstance(target,str) and (target==arn or target.startswith(arn+':'))


def collect(lam,scheduler,s3,arn,bucket):
    groups=[]
    for page in scheduler.get_paginator('list_schedule_groups').paginate():
        groups.extend(row['Name'] for row in page.get('ScheduleGroups',[]))
        if len(groups)>100:raise ValueError('Schedule group inventory bound exceeded')
    scanned=0;schedules=[]
    for group in groups:
        for page in scheduler.get_paginator('list_schedules').paginate(GroupName=group):
            for row in page.get('Schedules',[]):
                scanned+=1
                if scanned>10000:raise ValueError('Complete schedule inventory bound exceeded')
                if not matches(arn,(row.get('Target') or {}).get('Arn')):continue
                detail=scheduler.get_schedule(Name=row['Name'],GroupName=group)
                if not matches(arn,(detail.get('Target') or {}).get('Arn')):
                    raise ValueError('Schedule target changed during inventory')
                schedules.append({key:detail.get(key) for key in ('Name','GroupName','State','ScheduleExpression','ScheduleExpressionTimezone')}|
                    {'target_arn':detail['Target']['Arn']})
    mappings=[]
    for page in lam.get_paginator('list_event_source_mappings').paginate(FunctionName=arn):
        mappings.extend({key:row.get(key) for key in ('UUID','EventSourceArn','State','FunctionArn')} for row in page.get('EventSourceMappings',[]))
        if len(mappings)>1000:raise ValueError('Event source inventory bound exceeded')
    notifications=s3.get_bucket_notification_configuration(Bucket=bucket)
    direct=[{'id':row.get('Id'),'lambda_arn':row['LambdaFunctionArn'],'events':row.get('Events')}
        for row in notifications.get('LambdaFunctionConfigurations',[]) if matches(arn,row.get('LambdaFunctionArn'))]
    return {'schedule_groups_scanned':len(groups),'schedules_scanned':scanned,'matching_schedules':schedules,
        'event_source_mappings':mappings,'direct_bucket_notifications':direct,
        'indirect_lambda_stepfunction_and_eventbus_callers_verified':False}
