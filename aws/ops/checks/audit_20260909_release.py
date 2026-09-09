"""Metadata-only release gate; AWS access exists only behind injected clients.

Source parity is independent of output provenance. No payload/position/note text
is returned. Unknown outputs and absent source-version markers stay explicit.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(ROOT / 'aws/ops/checks'), str(ROOT / 'scripts')]
from donor_contract import numeric, parse_timestamp
from release_package_evidence import check_packages, release_config, shared_imports
from scheduler_payload import scheduler_payload
from audit_20260909_accounting import inspect_payload
from audit_20260909_risk import check_artifact

BASE = '125a68a89268c5ff8303dfba4177026dcf1e3c21'
BUCKET = 'justhodl-dashboard-live'
REGION = 'us-east-1'
ACCOUNT = '857687956942'
PRIMARY = {
    'justhodl-backtest-engine':['backtest/results.json'],
    'justhodl-research-backtest':['analytics/backtest_results.json'],
    'justhodl-calibration-snapshotter':['calibration/latest.json'],
    'justhodl-portfolio-snapshot':['portfolio/snapshot.json'],
    'justhodl-fleet-freshness-monitor':['data/_freshness-monitor.json'],
    'justhodl-stock-screener':['screener/data.json'],
    'justhodl-conviction-engine':['data/conviction.json'],
    'justhodl-sizing-engine':['data/sizing.json'],
    'justhodl-etf-constituents':['etf-flows/constituent-pressure.json'],
    'justhodl-risk-sizer':['data/risk-sizer.json','risk/recommendations.json'],
}
# Reviewed regular handlers publish research/data only and have no notification
# path. No generic discovery-based invocation is permitted. Portfolio snapshot's
# ordinary watchlist synchronization is part of its authorized normal refresh.
QUIET_STAGES = (
    ('tradingview','short-interest','liquidity-profile','etf-true-flows'),
    ('factor-risk','liquidity-capacity','conviction-engine'),
    ('risk-gate',),
    ('engine-fusion',),
    ('khalid-risk',),
    ('portfolio-snapshot',),
    ('katlin','risk-sizer','squeeze-fuel','trade-tickets','crypto-basis','firm-risk-board'),
    ('sizing-engine',),
)
QUIET_FUNCTIONS = {'justhodl-'+name for stage in QUIET_STAGES for name in stage}
RISK_KINDS = {'katlin','risk-sizer','khalid-risk','risk-gate','engine-fusion'}
ACCOUNTING_KEYS = {'portfolio/snapshot.json','backtest/results.json','analytics/backtest_results.json',
                   'calibration/latest.json','data/_freshness-monitor.json'}
MAX_AGE_H = {'justhodl-engine-fusion':2,'justhodl-khalid-risk':2,'justhodl-risk-sizer':2,
             'justhodl-risk-gate':48,'justhodl-katlin':36,'justhodl-portfolio-snapshot':3,
             'justhodl-crypto-funding':2,'justhodl-crypto-basis':2,'justhodl-factor-risk':48,
             'justhodl-short-interest':72,'justhodl-calibration-snapshotter':192}
SAFE_STATE = re.compile(r'^[A-Za-z0-9_.-]{1,64}$')


def utcnow(): return datetime.now(timezone.utc)


def safe_label(value):
    return value if isinstance(value,str) and SAFE_STATE.fullmatch(value) else None


def git(root, *args):
    return subprocess.check_output(['git',*args],cwd=root,text=True,stderr=subprocess.DEVNULL).strip()


def changed_scope(root, base=BASE):
    """Exact source/config changes and transitive shared importers; archived excluded."""
    git(root,'cat-file','-e',base+'^{commit}')  # Missing history is a gate failure.
    changed=git(root,'diff','--name-only',base,'HEAD','--','aws/lambdas','aws/shared').splitlines()
    shared_changed={Path(p).stem for p in changed if p.startswith('aws/shared/') and len(Path(p).parts)==3 and p.endswith('.py')}
    reasons={}
    for path in changed:
        parts=Path(path).parts
        if len(parts)>=4 and parts[:2]==('aws','lambdas') and parts[2]!='_archived' and (parts[3]=='source' or parts[3]=='config.json'):
            reasons.setdefault(parts[2],set()).add('source_or_config_changed')
    for directory in sorted((root/'aws/lambdas').iterdir()):
        if not directory.is_dir() or directory.name=='_archived': continue
        source=directory/'source'
        if not source.exists(): continue
        files=list(source.rglob('*.py'))
        # Shared imports are inspected transitively; source text is never reported.
        try:
            imported={p.stem for p in shared_imports(root,files)}
        except (SyntaxError,UnicodeError):
            if any(any(name in p.read_text(errors='replace') for name in shared_changed) for p in files):
                reasons.setdefault(directory.name,set()).add('shared_import_analysis_failed')
            continue
        hits=imported & shared_changed
        if hits: reasons.setdefault(directory.name,set()).update('shared:'+name for name in hits)
    for name in QUIET_FUNCTIONS:
        reasons.setdefault(name,set()).add('reviewed_dependency_refresh')
    return {name:sorted(why) for name,why in sorted(reasons.items()) if (root/'aws/lambdas'/name/'source').exists()}


def artifact_map(root, functions):
    manifest=json.loads((root/'engine-manifest.json').read_text())
    engines={row['engine']:row for row in manifest['engines']}
    pages=json.loads((root/'config/page-data-contracts.json').read_text()).get('pages',{})
    result={}
    for function in functions:
        row=engines.get(function,{})
        keys=row.get('keys',[])
        conventional='data/'+function.removeprefix('justhodl-')+'.json'
        chosen=PRIMARY.get(function) or ([conventional] if conventional in keys else keys if len(keys)==1 else [])
        result[function]={'primary_keys':chosen,'other_source_bound_keys':[key for key in keys if key not in chosen],
                          'unresolved_write_count':len(row.get('unresolved_writes',[])),
                          'primary_resolution':'EXPLICIT_OR_SOURCE_BOUND' if chosen else 'API_OR_PRIMARY_UNRESOLVED',
                          'pages':sorted(page for page,contract in pages.items() if function in contract.get('producers',[])),
                          'page_mapping_scope':'checked-in ownership contract; browser rendering verified separately'}
    return result


def strict_document(raw):
    def reject(value): raise ValueError('non_finite_json')
    doc=json.loads(raw,parse_constant=reject)
    if not isinstance(doc,dict): raise ValueError('primary_output_not_object')
    json.dumps(doc,allow_nan=False)
    return doc


def donor_checks(function, doc):
    """D27-D30 semantic checks, returning only fixed codes and aggregate counts."""
    name=function.removeprefix('justhodl-');errors=[];requirements=[];counts={}
    if name=='short-interest':
        if doc.get('measurement_contract')!='short-positioning.v2': errors.append('SHORT_MEASUREMENT_CONTRACT_MISSING')
        rows=doc.get('by_ticker',{})
        if not isinstance(rows,dict): errors.append('SHORT_ROWS_INVALID');rows={}
        for row in rows.values():
            if not isinstance(row,dict): errors.append('SHORT_ROW_INVALID');continue
            if row.get('latest_short_pct')!=row.get('daily_short_volume_pct'):errors.append('DAILY_VOLUME_ALIAS_MISMATCH')
            if row.get('days_to_cover_source')=='Finviz short ratio' and row.get('days_to_cover_as_of') is not None: errors.append('UNDATED_FALLBACK_FALSE_DATE')
            if row.get('short_interest') is not None and (row.get('short_interest_source')!='FINRA consolidated short interest' or row.get('short_interest_as_of')!=row.get('settlement_date')):errors.append('SHORT_INVENTORY_PROVENANCE_INVALID')
        counts['position_rows']=sum(isinstance(r,dict) and r.get('short_interest') is not None for r in rows.values())
        if not counts['position_rows']:requirements.append('DATED_FINRA_INVENTORY_UNAVAILABLE')
    elif name=='crypto-funding':
        rows=doc.get('by_coin',{})
        if not isinstance(rows,dict):errors.append('FUNDING_ROWS_INVALID');rows={}
        for row in rows.values():
            if not isinstance(row,dict):errors.append('FUNDING_ROW_INVALID');continue
            rate=numeric(row.get('current_funding_rate'));hours=numeric(row.get('funding_interval_hours'))
            if row.get('funding_rate_units')!='fraction_per_observed_interval':errors.append('FUNDING_UNITS_INVALID')
            if hours is None or row.get('interval_source')=='unknown':
                if row.get('annualized_pct') is not None:errors.append('UNKNOWN_INTERVAL_ANNUALIZED')
                requirements.append('OBSERVED_FUNDING_INTERVAL_UNAVAILABLE')
            elif rate is None or hours<=0 or row.get('interval_source') not in ('venue_schedule','observed_settlement_interval'):errors.append('FUNDING_INTERVAL_INVALID')
            elif numeric(row.get('annualized_pct')) is None or abs(row['annualized_pct']-rate*24/hours*365*100)>.011:errors.append('FUNDING_ANNUALIZATION_MISMATCH')
            if not row.get('venue_observed_at'):requirements.append('VENUE_OBSERVATION_TIMESTAMP_UNAVAILABLE')
    elif name in ('squeeze-fuel','trade-tickets'):
        rows=doc.get('board' if name=='squeeze-fuel' else 'tickets',[])
        if 'donor_health' not in doc:errors.append('DONOR_HEALTH_MISSING')
        if not isinstance(rows,list):errors.append('RECIPIENT_ROWS_INVALID');rows=[]
        counts['rows']=len(rows)
        for row in rows:
            if row.get('execution_eligible') is not False:errors.append('RESEARCH_EXECUTION_GUARD_MISSING')
            short=row.get('short_positioning') or {}
            if not short.get('positioning_usable'):requirements.append('DATED_POSITIONING_UNAVAILABLE')
            if short.get('locate_verified') is True or short.get('borrow_fee') is not None:errors.append('PUBLIC_SI_INVENTED_BORROW_TERMS')
            if name=='squeeze-fuel':
                if row.get('pct_of_float') is not None and (row.get('float_evidence') or {}).get('status')!='DATED':errors.append('UNDATED_FLOAT_USED')
            else:
                options=row.get('options_context') or {}
                if options.get('executable_option_quote') is not False:errors.append('OPTION_EXECUTION_GUARD_MISSING')
                if not options.get('usable'):requirements.append('CURRENT_OPTION_ANALYTICS_UNAVAILABLE')
                elif numeric((options.get('record') or {}).get('atm_iv_front')) is None or numeric(options.get('atm_iv_annualized_pct')) is None:errors.append('OPTION_VOLATILITY_UNITS_MISSING')
                elif abs(options['atm_iv_annualized_pct']-options['record']['atm_iv_front']*100)>1e-6:errors.append('OPTION_IV_SCALE_MISMATCH')
    elif name=='crypto-basis':
        if 'donor_health' not in doc:errors.append('DONOR_HEALTH_MISSING')
        for coin in ('btc','eth'):
            row=doc.get(coin) or {};ctx=row.get('perpetual_funding_context') or {};comparison=row.get('funding_basis_comparison') or {}
            if row.get('execution_eligible') is not False:errors.append('CARRY_EXECUTION_GUARD_MISSING')
            if comparison.get('locked_carry') is True:errors.append('FLOATING_FUNDING_TREATED_AS_LOCKED')
            if not ctx.get('usable'):requirements.append('OBSERVED_PERPETUAL_FUNDING_UNAVAILABLE')
            else:
                source=ctx.get('record') or {};rate=numeric(source.get('current_funding_rate'));hours=numeric(source.get('funding_interval_hours'));ann=numeric(ctx.get('annualized_from_observed_interval_pct'))
                if rate is None or hours is None or hours<=0 or ann is None or abs(ann-rate*24/hours*365*100)>1e-6:errors.append('CARRY_FUNDING_INTERVAL_MISMATCH')
    elif name=='sizing-engine':
        if not all(key in doc for key in ('donor_inputs','donor_constraint_summary')):errors.append('SIZING_DONOR_CONTRACT_MISSING')
        if any(r.get('execution_eligible') is not False for r in doc.get('recommendations',[])):errors.append('SIZING_EXECUTION_GUARD_MISSING')
    elif name=='firm-risk-board':
        if not all(key in doc for key in ('data_contract_status','factor_model_detail','liquidity_detail','book_linkage')):errors.append('FIRM_RISK_CONTRACT_MISSING')
        if doc.get('allows_new_entries') is not False:errors.append('MODELED_BOOK_GRANTED_EXECUTION')
    elif name=='liquidity-profile':
        if doc.get('method')!='liquidity_profile_v2_volume_capacity':errors.append('LIQUIDITY_PROFILE_OLD_METHOD')
    return {'errors':sorted(set(errors)),'requirements':sorted(set(requirements)),'counts':counts}


def inspect_output(s3, function, key, code, bucket=BUCKET, now=None):
    now=now or utcnow();result={'key':key,'status':'PENDING_OUTPUT','errors':[],'requirements':[]}
    try:
        response=s3.get_object(Bucket=bucket,Key=key);raw=response['Body'].read();doc=strict_document(raw)
        modified=response.get('LastModified');modified=parse_timestamp(modified.isoformat() if isinstance(modified,datetime) else modified)
        deployment=parse_timestamp(code.get('last_modified'))
        generation_field='generated_at' if doc.get('generated_at') else 'updated_at' if doc.get('updated_at') else 'as_of' if function in ('justhodl-risk-sizer','justhodl-calibration-snapshotter') else None
        generated=parse_timestamp(doc.get(generation_field)) if generation_field else None
        result.update(bytes=len(raw),last_modified=modified.isoformat() if modified else None,
                      generated_at=generated.isoformat() if generated else None,version_id=response.get('VersionId'),
                      schema_version=safe_label(doc.get('schema_version')),producer_version=safe_label(doc.get('version') or doc.get('audit_version')),
                      producer=safe_label(doc.get('engine')),source_code_version_marker_present=bool(doc.get('source_commit') or doc.get('code_sha256')),
                      attribution='publication timestamp after verified deployed package; no embedded source SHA asserted')
        if not deployment or not modified or modified<deployment or (now-modified).total_seconds() < -300:
            result['requirements'].append('FIRST_POST_RELEASE_PUBLICATION_PENDING');return result
        if not generated or generated < deployment-timedelta(seconds=5):
            result['requirements'].append('POST_RELEASE_GENERATION_TIMESTAMP_UNPROVEN');return result
        if (now-generated).total_seconds()>MAX_AGE_H.get(function,72)*3600:
            result['requirements'].append('FRESH_GENERATION_PENDING');return result
        if generated and ((now-generated).total_seconds() < -300 or generated>modified.replace(microsecond=0)+timedelta(minutes=5)):
            result['errors'].append('GENERATION_TIMESTAMP_INVALID')
        if key in ACCOUNTING_KEYS:
            result['errors'].extend(inspect_payload(key,doc))
            if key=='portfolio/snapshot.json':result['requirements'].append('BROKER_RECONCILED_CAPITAL_LEDGER_REQUIRED')
            if key=='backtest/results.json':result['requirements'].append('TRADABLE_PORTFOLIO_NAV_AND_VINTAGES_REQUIRED')
        kind=function.removeprefix('justhodl-')
        if kind in RISK_KINDS and key=='data/'+kind+'.json':
            try:
                checked=check_artifact(kind,doc)
                result['risk_contract_status']=safe_label(checked.get('status'))
                if checked.get('status') in ('DATA_HOLD','BLOCKED'):result['requirements'].append('CAPITAL_OR_SOURCE_REQUIREMENTS_BLOCK_PERMISSION')
            except Exception as exc:
                result['errors'].append('RISK_CONTRACT_'+type(exc).__name__)
        checks=donor_checks(function,doc)
        result['errors'].extend(checks['errors']);result['requirements'].extend(checks['requirements']);result['counts']=checks['counts']
        if doc.get('ok') is False or doc.get('error') or doc.get('_err'):result['requirements'].append('PRODUCER_REPORTED_DATA_UNAVAILABLE')
        result['status']='CONTRACT_FAILED' if result['errors'] else 'VERIFIED_BLOCKED_REQUIREMENTS' if result['requirements'] else 'VERIFIED'
    except Exception as exc:
        result.update(status='PENDING_OUTPUT' if type(exc).__name__ in ('NoSuchKey','ClientError') else 'CONTRACT_FAILED',error_type=type(exc).__name__)
    return result


def configure_katlin_refresh(scheduler, root):
    config=release_config(root,'justhodl-katlin');primary=config['eventbridge_scheduler']
    extras=config.get('eventbridge_schedulers_extra',[])
    matches=[spec for spec in extras if spec.get('schedule_name')=='justhodl-katlin-permission-refresh']
    if len(matches)!=1:raise ValueError('refresh_schedule_spec_missing_or_duplicate')
    spec={**matches[0],'cron':matches[0].get('expression') or matches[0].get('cron'),
          'role_arn':matches[0].get('role_arn') or primary['role_arn'],'state':'ENABLED'}
    if spec['cron']!='rate(15 minutes)' or spec.get('input')!={'mode':'permission_refresh'} or spec.get('target_alias')!='live':raise ValueError('refresh_schedule_contract_invalid')
    group=spec.get('group_name','default');name=spec['schedule_name']
    try:current=scheduler.get_schedule(Name=name,GroupName=group)
    except scheduler.exceptions.ResourceNotFoundException:current={}
    weekly_spec=config.get('eventbridge_scheduler_extra') or {}
    weekly=None
    if weekly_spec.get('schedule_name'):
        try:weekly=scheduler.get_schedule(Name=weekly_spec['schedule_name'],GroupName=weekly_spec.get('group_name','default'))
        except scheduler.exceptions.ResourceNotFoundException:pass
    target=f'arn:aws:lambda:{REGION}:{ACCOUNT}:function:justhodl-katlin:live'
    payload=scheduler_payload({'eventbridge_scheduler':spec},current,target)
    if current:scheduler.update_schedule(**payload)
    else:scheduler.create_schedule(**payload)
    observed=scheduler.get_schedule(Name=name,GroupName=group)
    if observed.get('State')!='ENABLED' or observed.get('Target',{}).get('Arn')!=target or json.loads(observed.get('Target',{}).get('Input','null'))!={'mode':'permission_refresh'} or observed.get('ScheduleExpression')!='rate(15 minutes)':raise ValueError('refresh_schedule_readback_failed')
    weekly_preserved=None
    if weekly is not None:
        after=scheduler.get_schedule(Name=weekly_spec['schedule_name'],GroupName=weekly_spec.get('group_name','default'))
        fields=('ScheduleExpression','ScheduleExpressionTimezone','State','Target','FlexibleTimeWindow','KmsKeyArn','StartDate','EndDate')
        weekly_preserved=all(weekly.get(k)==after.get(k) for k in fields)
        if not weekly_preserved:raise ValueError('weekly_backtest_schedule_changed')
    return {'name':name,'group':group,'expression':observed['ScheduleExpression'],'target_alias':'live',
            'state':observed.get('State'),'input_mode':'permission_refresh','weekly_schedule_present':weekly is not None,
            'weekly_backtest_preserved':weekly_preserved,'schedule_input_bodies_reported':False}


def input_matches(actual, expected):
    try:
        return json.loads(actual) == (json.loads(expected) if isinstance(expected,str) else expected)
    except (ValueError,TypeError):return actual==expected


def privacy_receipt_summary(root, receipt):
    """Reuse ops5230 evidence; never copy mirror contents or secret values."""
    result={'verified':False,'privacy_ops':5230,'reason':'SUCCESSFUL_PRIVACY_MIGRATION_RECEIPT_REQUIRED'}
    if not isinstance(receipt,dict) or receipt.get('ops')!=5230 or receipt.get('ok') is not True:return result
    from audit_20260909_privacy_migration import READINESS, PUBLISHERS, desired_members, digest, encoded
    checks=receipt.get('checks',[])
    configured={row.get('function') for row in checks if row.get('check')=='private_publisher_config'}
    code=[row for row in checks if row.get('check')=='exact_deployed_code']
    mismatched=[]
    for short in READINESS:
        name='justhodl-'+short
        try:
            expected=digest(encoded({key:digest(value) for key,value in desired_members(root,short).items()}))
            rows=[row for row in code if row.get('function')==name]
            if not rows or any(row.get('source_sha256')!=expected for row in rows):mismatched.append(name)
        except Exception:mismatched.append(name)
    missing_config=sorted({'justhodl-'+name for name in PUBLISHERS}-configured)
    policies=[row for row in checks if row.get('check')=='bucket_policy']
    policy=policies[-1] if policies else {}
    restored=policy.get('private_deny') is True and policy.get('historical_deny') is True and policy.get('temporary_current_deny') is False
    verified=not mismatched and not missing_config and restored
    result.update(verified=verified,reason='VERIFIED' if verified else 'PRIVACY_RECEIPT_INCOMPLETE_OR_DIFFERENT_SOURCE_RELEASE',
                  expected_readiness_functions=len(READINESS),expected_private_publishers=len(PUBLISHERS),
                  source_mismatches=mismatched,missing_publisher_config=missing_config,final_policy_restored=restored,
                  receipt_checkout_sha=receipt.get('checkout_sha'),privacy_epoch=receipt.get('epoch'))
    return result


def observe_schedules(clients, root, functions):
    """Read configured schedules; report target metadata without Input bodies."""
    result=[]
    for function in functions:
        config=release_config(root,function);specs=[]
        for field in ('eventbridge_scheduler','eventbridge_scheduler_extra'):
            if isinstance(config.get(field),dict):specs.append(('scheduler',config[field]))
        specs.extend(('scheduler',spec) for spec in config.get('eventbridge_schedulers_extra',[]) if isinstance(spec,dict))
        if isinstance(config.get('schedule'),dict):specs.append(('events',config['schedule']))
        specs.extend(('events',{'name':name}) for name in config.get('eventbridge_rules',[]) if isinstance(name,str))
        seen=set()
        for kind,spec in specs:
            name=spec.get('schedule_name') or spec.get('name')
            if not name or (kind,name) in seen:continue
            seen.add((kind,name));row={'function':function,'service':kind,'name':name,'status':'PENDING_CONFIGURATION'}
            try:
                if kind=='scheduler':
                    current=clients['scheduler'].get_schedule(Name=name,GroupName=spec.get('group_name','default'))
                    targets=[current.get('Target',{})]
                else:
                    current=clients['events'].describe_rule(Name=name)
                    targets=clients['events'].list_targets_by_rule(Rule=name).get('Targets',[])
                expected=spec.get('cron') or spec.get('expression')
                matching=[target for target in targets if target.get('Arn','').split(':function:')[-1].split(':')[0]==function]
                governed=bool(config.get('release_validation')) or function in ('justhodl-engine-fusion','justhodl-khalid-risk')
                qualified=all(':' in target['Arn'].split(':function:')[-1] and not target['Arn'].endswith(':$LATEST') for target in matching)
                row.update(expression=current.get('ScheduleExpression'),expected_expression=expected,state=current.get('State'),
                           target_count=len(targets),matching_target_arns=[target['Arn'] for target in matching],
                           governed_target_qualified=qualified if governed else None,
                           input_configured='input' in spec,input_preserved_or_matches=all(input_matches(target.get('Input'),spec['input']) for target in matching) if 'input' in spec else None)
                if matching and current.get('State')=='ENABLED' and (not expected or expected==current.get('ScheduleExpression')) and (not governed or qualified) and row['input_preserved_or_matches'] is not False:row['status']='VERIFIED'
            except Exception as exc:row['error_type']=type(exc).__name__
            result.append(row)
    return result


class ReleaseVerifier:
    def __init__(self, root, clients, *, parity_seconds=1800, publication_seconds=2100,
                 sleeper=time.sleep, clock=time.monotonic, package_check=check_packages, privacy_receipt=None):
        self.root=root;self.clients=clients;self.parity_seconds=min(parity_seconds,1800)
        self.publication_seconds=min(publication_seconds,2100);self.sleep=sleeper;self.clock=clock;self.package_check=package_check
        self.privacy_receipt=privacy_receipt
        self.report={'ops':5231,'ok':False,'status':'INITIALIZING','code':{},'outputs':{},'invocations':[],
                     'requirements':[],'private_payloads_reported':0,'raw_lambda_responses_reported':0}

    def checkpoint(self):
        path=self.root/'aws/ops/reports/5231_audit_release_verify.json';path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text(json.dumps(self.report,indent=2,allow_nan=False)+'\n')

    def parity(self, functions):
        deadline=self.clock()+self.parity_seconds;pending=set(functions)
        while pending:
            with ThreadPoolExecutor(max_workers=6) as pool:
                rows=list(pool.map(lambda name:self.package_check(self.clients['lambda'],self.root,[name])[0],sorted(pending)))
            for row in rows:self.report['code'][row['function']]=row
            pending={name for name in pending if not self.report['code'][name].get('pass')}
            self.report['source_parity_pending']=sorted(pending);self.checkpoint()
            print(json.dumps({'ops':5231,'phase':'package_parity','matched':len(functions)-len(pending),'pending':len(pending)}),flush=True)
            if not pending:return True
            if self.clock()>=deadline:return False
            self.sleep(min(30,max(0,deadline-self.clock())))
        return True

    def inspect_function(self, name):
        outputs=self.report['artifact_scope'][name]['primary_keys'];code=self.report['code'][name]
        rows=[inspect_output(self.clients['s3'],name,key,code) for key in outputs]
        self.report['outputs'][name]=rows
        return bool(rows) and all(row['status'].startswith('VERIFIED') for row in rows)

    def invoke_once(self, name):
        if name not in QUIET_FUNCTIONS:raise ValueError('unreviewed_manual_invocation')
        source_files=(self.root/'aws/lambdas'/name/'source').rglob('*.py')
        if any(re.search(r'api\.telegram\.org|sendMessage|send_telegram|send_email|\.invoke\(',path.read_text(errors='replace')) for path in source_files):
            self.report['requirements'].append({'function':name,'reason':'NOTIFICATION_OR_CASCADE_PATH_REQUIRES_NORMAL_SCHEDULE'})
            return False
        # A fresh package check immediately precedes every invocation. Pin the
        # verified numbered version where the governed alias provides one.
        code=self.package_check(self.clients['lambda'],self.root,[name])[0]
        self.report['code'][name]=code
        if not code.get('pass'):raise ValueError('code_changed_before_invocation')
        args={'FunctionName':name,'InvocationType':'Event','Payload':b'{}'}
        if code.get('qualifier')=='live':args['Qualifier']=code['version']
        response=self.clients['lambda'].invoke(**args)
        self.report['invocations'].append({'function':name,'qualifier':args.get('Qualifier','$LATEST'),
            'request_status':response.get('StatusCode'),'requested_at':utcnow().isoformat(),'mode':'regular_quiet_publish'})
        if response.get('StatusCode')!=202:raise ValueError('async_invocation_not_accepted')
        self.checkpoint();return True

    def run(self):
        self.report.update(checkout_sha=git(self.root,'rev-parse','HEAD'),baseline_sha=BASE,started_at=utcnow().isoformat())
        scope=changed_scope(self.root);self.report['engine_scope']=scope
        self.report['artifact_scope']=artifact_map(self.root,scope)
        self.report['status']='WAITING_FOR_SOURCE_PARITY'
        if not self.parity(scope):
            self.report['status']='SOURCE_PARITY_FAILED';self.checkpoint();return self.report
        # No schedule or invocation mutation has happened before this gate.
        self.report['status']='SOURCE_PARITY_VERIFIED';self.checkpoint()
        self.report['privacy_prerequisite']=privacy_receipt_summary(self.root,self.privacy_receipt)
        if not self.report['privacy_prerequisite']['verified']:
            self.report['status']='PRIVACY_PREREQUISITE_BLOCKED';self.checkpoint();return self.report
        katlin=self.package_check(self.clients['lambda'],self.root,['justhodl-katlin'])[0]
        if not katlin.get('pass'):raise ValueError('katlin_changed_before_schedule_update')
        self.report['katlin_refresh_schedule']=configure_katlin_refresh(self.clients['scheduler'],self.root)
        deadline=self.clock()+self.publication_seconds
        for stage in QUIET_STAGES:
            stage_timeout=max(release_config(self.root,'justhodl-'+short).get('timeout',300) for short in stage)+120
            stage_deadline=min(deadline,self.clock()+stage_timeout)
            waiting=[]
            for short in stage:
                name='justhodl-'+short
                if self.inspect_function(name):continue
                if self.clock()>=deadline:break
                if self.invoke_once(name):waiting.append(name)
            while waiting and self.clock()<stage_deadline:
                waiting=[name for name in waiting if not self.inspect_function(name) and not any(row['status']=='CONTRACT_FAILED' for row in self.report['outputs'][name])]
                self.checkpoint()
                if waiting:
                    print(json.dumps({'ops':5231,'phase':'publication','stage':list(stage),'pending':len(waiting)}),flush=True)
                    self.sleep(min(20,max(0,stage_deadline-self.clock())))
            # Later engines may safely publish explicit DATA_HOLD if an upstream
            # source remains unavailable; no donor failure is recast as neutral.
        for name in scope:self.inspect_function(name)
        self.report['schedules']=observe_schedules(self.clients,self.root,scope)
        self.report['schedule_discovery_scope']='declarative config schedules; no claim that unconfigured fleet rules were exhaustively discovered'
        pending=[];failed=[];blocked=[]
        pending.extend({'function':row['function'],'reason':'SCHEDULE_CONFIGURATION_PENDING','schedule':row['name']} for row in self.report['schedules'] if row['status']!='VERIFIED')
        if self.report['katlin_refresh_schedule'].get('weekly_schedule_present') is False:
            pending.append({'function':'justhodl-katlin','reason':'WEEKLY_BACKTEST_SCHEDULE_MISSING'})
        for name,rows in self.report['outputs'].items():
            if not rows:pending.append({'function':name,'reason':'API_REQUEST_FIXTURE_OR_PRIMARY_OUTPUT_MAPPING_REQUIRED'})
            for row in rows:
                if row['status']=='PENDING_OUTPUT':pending.append({'function':name,'key':row['key'],'reason':'FIRST_SCHEDULED_RUN_PENDING' if name not in QUIET_FUNCTIONS else 'POST_RELEASE_OUTPUT_PENDING'})
                elif row['status']=='CONTRACT_FAILED':failed.append({'function':name,'key':row['key'],'errors':row.get('errors',[]),'error_type':row.get('error_type')})
                elif row['requirements']:blocked.append({'function':name,'key':row['key'],'requirements':row['requirements']})
        self.report.update(pending_outputs=pending,contract_failures=failed,blocked_requirements=blocked,
                           finished_at=utcnow().isoformat(),ok=not pending and not failed,
                           status='CONTRACT_FAILURE' if failed else 'PENDING_SCHEDULED_OUTPUTS' if pending else 'VERIFIED_WITH_BLOCKED_REQUIREMENTS' if blocked else 'VERIFIED')
        self.checkpoint();return self.report
