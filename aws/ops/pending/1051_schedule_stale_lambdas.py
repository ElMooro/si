#!/usr/bin/env python3
"""ops 1051 — check Scheduler v2 + create schedules for stale Lambdas

For both stale Lambdas:
  1. Check if a Scheduler v2 schedule exists targeting them
  2. If no Scheduler v2 schedule AND no EB Rule, create an EB Rule (Rules v1)
     since that's what deploy-lambdas.yml manages

Schedules:
  - justhodl-activist-13d: daily 14:00 UTC weekdays (after morning data)
  - justhodl-13f-price-divergence: weekly Tuesday 06:00 UTC (per description)
"""
import json, boto3, os, time
from datetime import datetime, timezone

REGION = 'us-east-1'
ACCOUNT = '857687956942'
lam = boto3.client('lambda', region_name=REGION)
events = boto3.client('events', region_name=REGION)
sched = boto3.client('scheduler', region_name=REGION)

report = {'started_at': datetime.now(timezone.utc).isoformat(), 'lambdas': {}}

TARGETS = [
    {
        'lambda': 'justhodl-activist-13d',
        'eb_rule_name': 'activist-13d-daily',
        'cron': 'cron(0 14 ? * MON-FRI *)',
        'description': 'Daily 14 UTC weekdays — activist 13D scanner',
    },
    {
        'lambda': 'justhodl-13f-price-divergence',
        'eb_rule_name': 'justhodl-13f-price-divergence-weekly',
        'cron': 'cron(0 6 ? * TUE *)',
        'description': 'Weekly Tuesday 06 UTC — 13F vs price divergence (per config description)',
    },
]

for target in TARGETS:
    fn = target['lambda']
    info = {'lambda': fn}
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{fn}"
    
    # 1. Check Scheduler v2
    sched_schedules = []
    try:
        # list_schedules in default group
        paginator = sched.get_paginator('list_schedules')
        for page in paginator.paginate():
            for s in page.get('Schedules', []):
                # describe to get target arn
                try:
                    full = sched.get_schedule(Name=s['Name'], GroupName=s.get('GroupName','default'))
                    target_arn = full.get('Target', {}).get('Arn', '')
                    if fn in target_arn or target_arn == fn_arn:
                        sched_schedules.append({
                            'name': s['Name'],
                            'group': s.get('GroupName','default'),
                            'state': full.get('State'),
                            'expression': full.get('ScheduleExpression'),
                            'target_arn': target_arn,
                        })
                except Exception:
                    pass
    except Exception as e:
        info['scheduler_v2_error'] = str(e)[:200]
    info['scheduler_v2_schedules'] = sched_schedules
    
    # 2. Check EB Rules
    eb_rules = []
    try:
        for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=fn_arn):
            for rn in page.get('RuleNames', []):
                rd = events.describe_rule(Name=rn)
                eb_rules.append({
                    'name': rn,
                    'state': rd.get('State'),
                    'expression': rd.get('ScheduleExpression'),
                })
    except Exception as e:
        info['eb_rules_error'] = str(e)[:200]
    info['eb_rules'] = eb_rules
    
    info['has_any_schedule'] = bool(sched_schedules or eb_rules)
    
    # 3. If no schedule of any kind, create EB Rule
    if not info['has_any_schedule']:
        rule_name = target['eb_rule_name']
        cron = target['cron']
        desc = target['description']
        
        print(f"\n[{fn}] No schedule found — creating EB Rule {rule_name} → {cron}")
        try:
            # Create the rule
            events.put_rule(
                Name=rule_name,
                ScheduleExpression=cron,
                State='ENABLED',
                Description=desc,
            )
            # Add Lambda as target
            events.put_targets(
                Rule=rule_name,
                Targets=[{'Id': '1', 'Arn': fn_arn}],
            )
            # Grant EB permission to invoke Lambda
            rule_arn = f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{rule_name}"
            stmt_id = f"AllowExecutionFromEB_{rule_name}"[:100]
            try:
                lam.add_permission(
                    FunctionName=fn,
                    StatementId=stmt_id,
                    Action='lambda:InvokeFunction',
                    Principal='events.amazonaws.com',
                    SourceArn=rule_arn,
                )
                info['permission_added'] = True
            except lam.exceptions.ResourceConflictException:
                info['permission_already_exists'] = True
            info['rule_created'] = {'name': rule_name, 'cron': cron, 'arn': rule_arn}
            print(f"  ✅ Rule + target + permission created")
        except Exception as e:
            info['create_error'] = str(e)[:300]
            print(f"  ❌ {e}")
    else:
        print(f"[{fn}] Already has schedule(s) — no action needed")
        print(f"  Scheduler v2: {sched_schedules}")
        print(f"  EB Rules:     {eb_rules}")
    
    report['lambdas'][fn] = info

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1051.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print("\n=== SUMMARY ===")
for fn, info in report['lambdas'].items():
    had_any = info.get('has_any_schedule')
    created = bool(info.get('rule_created'))
    status = '✅ ALREADY HAD SCHEDULE' if had_any else ('✅ CREATED' if created else '❌ FAILED TO CREATE')
    print(f"  {fn}: {status}")
