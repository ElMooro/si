#!/usr/bin/env python3
"""Preserve an existing direct Lambda schedule and never print its private Input."""
import copy
import json
import re
import sys
from scheduler_payload import WRITABLE


def payload(config, current, target_arn):
    spec = config['eventbridge_scheduler']
    if not re.fullmatch(r'arn:aws:lambda:[a-z0-9-]+:\d{12}:function:[A-Za-z0-9_-]+', target_arn):
        raise ValueError('direct_target_invalid')
    name, group = spec['schedule_name'], spec.get('group_name', 'default')
    if current:
        if current.get('Name') != name or current.get('GroupName', 'default') != group:
            raise ValueError('schedule_identity_mismatch')
        if current.get('Target', {}).get('Arn') not in (target_arn, target_arn + ':$LATEST'):
            raise ValueError('unrelated_or_qualified_target')
        result = {k: copy.deepcopy(v) for k, v in current.items() if k in WRITABLE}
    else:
        result = {'Name':name,'GroupName':group,'State':'ENABLED','FlexibleTimeWindow':{'Mode':'OFF'},
                  'ScheduleExpressionTimezone':'UTC','Target':{'Input':'{}','RetryPolicy':{'MaximumRetryAttempts':2,'MaximumEventAgeInSeconds':3600}}}
    result['ScheduleExpression'] = spec['cron']
    result['Target'].update(Arn=target_arn, RoleArn=spec['role_arn'])
    for source,destination in (('timezone','ScheduleExpressionTimezone'),('description','Description'),('state','State'),
            ('flexible_time_window','FlexibleTimeWindow'),('start_date','StartDate'),('end_date','EndDate'),
            ('kms_key_arn','KmsKeyArn'),('action_after_completion','ActionAfterCompletion')):
        if source in spec: result[destination] = copy.deepcopy(spec[source])
    for source,destination in (('retry_policy','RetryPolicy'),('dead_letter_config','DeadLetterConfig')):
        if source in spec: result['Target'][destination] = copy.deepcopy(spec[source])
    if 'input' in spec:
        value=spec['input']; result['Target']['Input']=value if isinstance(value,str) else json.dumps(value,separators=(',',':'))
    return result


def apply(client, config, target_arn):
    spec=config['eventbridge_scheduler']; args={'Name':spec['schedule_name'],'GroupName':spec.get('group_name','default')}
    try: current=client.get_schedule(**args)
    except Exception as exc:
        if getattr(exc,'response',{}).get('Error',{}).get('Code') != 'ResourceNotFoundException': raise
        current={}
    request=payload(config,current,target_arn)
    if current:
        observed=client.get_schedule(**args)
        if {k:v for k,v in observed.items() if k in WRITABLE} != {k:v for k,v in current.items() if k in WRITABLE}:
            raise RuntimeError('schedule_changed_before_update')
        client.update_schedule(**request)
    else:
        client.create_schedule(**request)
    readback=client.get_schedule(**args)
    if any(readback.get(k)!=v for k,v in request.items()):
        raise RuntimeError('schedule_readback_mismatch')
    return {'phase':'scheduler_configuration','status':'VERIFIED','function':target_arn.split(':function:')[1]}


if __name__ == '__main__':
    path,target,region=sys.argv[1:]
    try:
        import boto3
        with open(path) as stream: config=json.load(stream)
        print(json.dumps(apply(boto3.client('scheduler',region_name=region),config,target),sort_keys=True))
    except Exception as exc:
        code=getattr(exc,'response',{}).get('Error',{}).get('Code')
        code=code if isinstance(code,str) and re.fullmatch(r'[A-Za-z]{1,70}',code) else type(exc).__name__
        print(json.dumps({'phase':'scheduler_configuration','status':'FAILED','error_type':code}),file=sys.stderr)
        raise SystemExit(1) from None
