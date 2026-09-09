"""Ops5231 mutation gates, strict output evidence and schedule preservation; no AWS."""
import copy
import io
import json
import runpy
import sys
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'aws/ops/checks'))
import audit_20260909_release as release


def test_scope_includes_transitive_shared_importers_and_excludes_archived():
    with tempfile.TemporaryDirectory() as temp:
        root=Path(temp);(root/'aws/shared').mkdir(parents=True)
        (root/'aws/shared/direct.py').write_text('import indirect\n')
        (root/'aws/shared/indirect.py').write_text('VALUE=1\n')
        for name,source in (('justhodl-recipient','import direct\n'),('justhodl-changed','VALUE=2\n'),('justhodl-unrelated','import math\n')):
            path=root/'aws/lambdas'/name/'source';path.mkdir(parents=True);(path/'lambda_function.py').write_text(source)
        archived=root/'aws/lambdas/_archived/old/source';archived.mkdir(parents=True);(archived/'lambda_function.py').write_text('import indirect\n')
        changes='aws/shared/indirect.py\naws/lambdas/justhodl-changed/config.json\naws/lambdas/_archived/old/source/lambda_function.py'
        with patch.object(release,'git',side_effect=lambda root,*args:changes if args[0]=='diff' else ''):
            scope=release.changed_scope(root)
        assert set(scope)=={'justhodl-recipient','justhodl-changed'}
        assert 'shared:indirect' in scope['justhodl-recipient']


def test_explicit_primary_outputs_are_source_bound_and_do_not_select_control_state():
    manifest=json.loads((ROOT/'engine-manifest.json').read_text())
    engines={row['engine']:set(row['keys']) for row in manifest['engines']}
    for name,keys in release.PRIMARY.items():
        assert set(keys)<=engines[name], (name,set(keys)-engines[name])
    assert release.PRIMARY['justhodl-contract-gate']==['data/contract-violations.json']
    assert release.PRIMARY['justhodl-fleet-monitor']==['_health/fleet.json']
    assert release.PRIMARY['justhodl-portfolio-risk']==['portfolio/risk.json']
    assert release.PRIMARY['justhodl-theme-classifier']==['data/momentum-themes.json']
    assert release.PRIMARY['justhodl-theme-rotation-engine']==['data/theme-momentum.json']


def test_any_source_mismatch_aborts_before_schedule_or_lambda_mutation():
    class NoMutation:
        def __getattr__(self,name):raise AssertionError('Unexpected AWS operation '+name)
    checker=lambda client,root,names:[{'function':name,'pass':False} for name in names]
    verifier=release.ReleaseVerifier(ROOT,dict.fromkeys(('lambda','scheduler','s3'),NoMutation()),parity_seconds=0,package_check=checker)
    verifier.checkpoint=lambda:None
    with patch.object(release,'changed_scope',return_value={'justhodl-risk-gate':['changed']}), \
         patch.object(release,'artifact_map',return_value={'justhodl-risk-gate':{'primary_keys':['data/risk-gate.json']}}), \
         patch.object(release,'git',return_value='0'*40):
        result=verifier.run()
    assert result['status']=='SOURCE_PARITY_FAILED' and result['ok'] is False
    assert result['invocations']==[] and 'katlin_refresh_schedule' not in result


class ScheduleClient:
    class exceptions:
        class ResourceNotFoundException(Exception):pass
    def __init__(self):
        self.schedules={
            'justhodl-katlin-permission-refresh':{'Name':'justhodl-katlin-permission-refresh','GroupName':'default','ScheduleExpression':'rate(1 hour)','State':'ENABLED',
                'Target':{'Arn':f'arn:aws:lambda:{release.REGION}:{release.ACCOUNT}:function:justhodl-katlin',
                          'RoleArn':'existing-role','Input':'{"old":"not printed"}',
                          'RetryPolicy':{'MaximumRetryAttempts':8},'DeadLetterConfig':{'Arn':'dlq'}},'FlexibleTimeWindow':{'Mode':'OFF'}},
            'justhodl-katlin-backtest-weekly':{'Name':'justhodl-katlin-backtest-weekly','ScheduleExpression':'cron(30 9 ? * SUN *)','State':'ENABLED',
                'Target':{'Arn':'weekly:live','Input':'{"mode":"backtest","private":"never report"}'},'FlexibleTimeWindow':{'Mode':'OFF'}}}
        self.calls=[]
    def get_schedule(self,Name,GroupName='default'):
        if Name not in self.schedules:raise self.exceptions.ResourceNotFoundException()
        return copy.deepcopy(self.schedules[Name])
    def update_schedule(self,**payload):self.calls.append(payload['Name']);self.schedules[payload['Name']]=copy.deepcopy(payload)
    create_schedule=update_schedule


def test_katlin_refresh_provision_preserves_weekly_and_retry_dlq_without_payload_report():
    client=ScheduleClient();before=copy.deepcopy(client.schedules['justhodl-katlin-backtest-weekly'])
    result=release.configure_katlin_refresh(client,ROOT)
    refresh=client.schedules['justhodl-katlin-permission-refresh']
    assert refresh['ScheduleExpression']=='rate(15 minutes)'
    assert refresh['Target']['Arn'].endswith(':live')
    assert refresh['Target']['RetryPolicy']=={'MaximumRetryAttempts':8}
    assert refresh['Target']['DeadLetterConfig']=={'Arn':'dlq'}
    assert json.loads(refresh['Target']['Input'])=={'mode':'permission_refresh'}
    assert client.schedules['justhodl-katlin-backtest-weekly']==before
    assert result['weekly_backtest_preserved'] is True
    assert 'never report' not in json.dumps(result)


def test_katlin_refresh_refuses_unrelated_existing_schedule_target():
    client=ScheduleClient();client.schedules['justhodl-katlin-permission-refresh']['Target']['Arn']='unrelated-function'
    try:release.configure_katlin_refresh(client,ROOT)
    except ValueError:pass
    else:raise AssertionError('Unrelated target overwritten')
    assert not client.calls


def fixture_output(doc, modified=None):
    now=datetime.now(timezone.utc)
    return SimpleNamespace(get_object=lambda **kw:{'Body':io.BytesIO(json.dumps(doc).encode()),'LastModified':modified or now,'VersionId':'fixture-version'})


def test_rewriting_old_payload_does_not_prove_new_code_generated_it():
    now=datetime.now(timezone.utc)
    client=fixture_output({'generated_at':(now-timedelta(days=3)).isoformat(),'secret_note':'DO_NOT_REPORT'})
    result=release.inspect_output(client,'justhodl-example','data/example.json',{'last_modified':(now-timedelta(hours=1)).isoformat()},now=now)
    assert result['status']=='PENDING_OUTPUT'
    assert 'POST_RELEASE_GENERATION_TIMESTAMP_UNPROVEN' in result['requirements']
    assert 'DO_NOT_REPORT' not in json.dumps(result)


def test_nonfinite_payload_fails_contract_and_never_prints_source_body():
    now=datetime.now(timezone.utc)
    result=release.inspect_output(fixture_output({'generated_at':now.isoformat(),'private':float('nan')}),
        'justhodl-example','data/example.json',{'last_modified':(now-timedelta(hours=1)).isoformat()},now=now)
    assert result['status']=='CONTRACT_FAILED' and result['error_type']=='ValueError'
    assert 'private' not in json.dumps(result)


def test_blocked_accounting_contract_is_verified_requirement_not_false_permission():
    now=datetime.now(timezone.utc)
    doc={'generated_at':now.isoformat(),'audit_version':'2026-09-09.1','positions':[],
         'capital_book':{'status':'BLOCKED','equity_nav':None,'allows_new_entries':False},'private_note':'DO_NOT_REPORT'}
    result=release.inspect_output(fixture_output(doc),'justhodl-portfolio-snapshot','portfolio/snapshot.json',{'last_modified':(now-timedelta(hours=1)).isoformat()},now=now)
    assert result['status']=='VERIFIED_BLOCKED_REQUIREMENTS' and not result['errors']
    assert 'BROKER_RECONCILED_CAPITAL_LEDGER_REQUIRED' in result['requirements']
    assert 'DO_NOT_REPORT' not in json.dumps(result)


def test_donor_checker_rejects_locked_funding_and_mixed_short_volume():
    doc={'measurement_contract':'short-positioning.v2','by_ticker':{'PRIVATE_NAME':{'latest_short_pct':90,'daily_short_volume_pct':20}}}
    result=release.donor_checks('justhodl-short-interest',doc)
    assert 'DAILY_VOLUME_ALIAS_MISMATCH' in result['errors'] and 'PRIVATE_NAME' not in json.dumps(result)
    result=release.donor_checks('justhodl-crypto-basis',{'donor_health':{},'btc':{'execution_eligible':False,'funding_basis_comparison':{'locked_carry':True}},'eth':{'execution_eligible':False}})
    assert 'FLOATING_FUNDING_TREATED_AS_LOCKED' in result['errors']


def test_governed_refresh_invokes_exact_verified_number_not_moving_live_alias():
    calls=[];client=SimpleNamespace(invoke=lambda **kw:calls.append(kw) or {'StatusCode':202})
    checker=lambda client,root,names:[{'function':names[0],'pass':True,'qualifier':'live','version':'17'}]
    verifier=release.ReleaseVerifier(ROOT,{'lambda':client},package_check=checker);verifier.checkpoint=lambda:None
    assert verifier.invoke_once('justhodl-risk-gate')
    assert calls[0]['Qualifier']=='17' and calls[0]['InvocationType']=='Event'
    assert calls[0]['Payload']==b'{}'


def test_unreviewed_notification_engine_cannot_be_manually_invoked():
    verifier=release.ReleaseVerifier(ROOT,{});verifier.checkpoint=lambda:None
    try:verifier.invoke_once('justhodl-crypto-funding')
    except ValueError:pass
    else:raise AssertionError('Notification-capable engine was invoked')
    assert verifier.report['invocations']==[]


def test_package_drift_immediately_before_invoke_aborts():
    calls=[];client=SimpleNamespace(invoke=lambda **kw:calls.append(kw))
    verifier=release.ReleaseVerifier(ROOT,{'lambda':client},package_check=lambda client,root,names:[{'function':names[0],'pass':False}])
    verifier.checkpoint=lambda:None
    try:verifier.invoke_once('justhodl-risk-gate')
    except ValueError:pass
    else:raise AssertionError('Mismatched code invoked')
    assert calls==[]


def test_schedule_metadata_rejects_latest_and_matches_json_without_reporting_input():
    target=f'arn:aws:lambda:{release.REGION}:{release.ACCOUNT}:function:justhodl-risk-gate'
    current={'ScheduleExpression':'rate(1 hour)','State':'ENABLED','Target':{'Arn':target,'Input':'{ "private": "DO_NOT_REPORT" }'}}
    client=SimpleNamespace(get_schedule=lambda **kw:copy.deepcopy(current))
    config={'release_validation':{'schema_version':'1'},'eventbridge_scheduler':{'schedule_name':'hourly','cron':'rate(1 hour)','input':{'private':'DO_NOT_REPORT'}}}
    with patch.object(release,'release_config',return_value=config):
        rows=release.observe_schedules({'scheduler':client},ROOT,['justhodl-risk-gate'])
        assert rows[0]['status']=='PENDING_CONFIGURATION'
        current['Target']['Arn']+=':live'
        rows=release.observe_schedules({'scheduler':client},ROOT,['justhodl-risk-gate'])
    assert rows[0]['status']=='VERIFIED' and rows[0]['input_preserved_or_matches'] is True
    assert 'DO_NOT_REPORT' not in json.dumps(rows)


def test_code_only_api_engine_keeps_overall_release_pending():
    checker=lambda client,root,names:[{'function':name,'pass':True,'last_modified':'2026-09-09T00:00:00Z'} for name in names]
    verifier=release.ReleaseVerifier(ROOT,{'lambda':None,'scheduler':None,'s3':None},package_check=checker)
    verifier.checkpoint=lambda:None
    with patch.object(release,'changed_scope',return_value={'justhodl-ask':['source_changed']}), \
         patch.object(release,'artifact_map',return_value={'justhodl-ask':{'primary_keys':[]}}), \
         patch.object(release,'configure_katlin_refresh',return_value={'verified':True}), \
         patch.object(release,'privacy_receipt_summary',return_value={'verified':True}), \
         patch.object(release,'observe_schedules',return_value=[]), \
         patch.object(release,'observe_function_urls',return_value=[]), \
         patch.object(release,'QUIET_STAGES',()),patch.object(release,'git',return_value='0'*40):
        result=verifier.run()
    assert result['ok'] is False and result['status']=='PENDING_SCHEDULED_OUTPUTS'
    assert result['pending_outputs'][0]['reason']=='API_REQUEST_FIXTURE_OR_PRIMARY_OUTPUT_MAPPING_REQUIRED'


def test_missing_privacy_receipt_blocks_every_mutation_after_source_parity():
    class NoMutation:
        def __getattr__(self,name):raise AssertionError('Unexpected AWS operation '+name)
    checker=lambda client,root,names:[{'function':name,'pass':True} for name in names]
    verifier=release.ReleaseVerifier(ROOT,dict.fromkeys(('lambda','scheduler','s3'),NoMutation()),package_check=checker)
    verifier.checkpoint=lambda:None
    with patch.object(release,'changed_scope',return_value={'justhodl-risk-gate':['changed']}), \
         patch.object(release,'artifact_map',return_value={'justhodl-risk-gate':{'primary_keys':['data/risk-gate.json']}}), \
         patch.object(release,'git',return_value='0'*40):
        result=verifier.run()
    assert result['status']=='PRIVACY_PREREQUISITE_BLOCKED' and result['ok'] is False
    assert result['invocations']==[] and 'katlin_refresh_schedule' not in result


def test_privacy_receipt_must_match_every_expected_source_and_publisher():
    import audit_20260909_privacy_migration as privacy
    members={'lambda_function.py':b'reviewed_source'}
    fingerprint=privacy.digest(privacy.encoded({key:privacy.digest(value) for key,value in members.items()}))
    receipt={'ops':5230,'ok':True,'checks':[
        {'check':'exact_deployed_code','function':'justhodl-private','source_sha256':fingerprint},
        {'check':'private_publisher_config','function':'justhodl-private'},
        {'check':'bucket_policy','private_deny':True,'historical_deny':True,'temporary_current_deny':False}]}
    with patch.object(privacy,'READINESS',('private',)),patch.object(privacy,'PUBLISHERS',('private',)), \
         patch.object(privacy,'desired_members',return_value=members):
        assert release.privacy_receipt_summary(ROOT,receipt)['verified'] is True
        receipt['checks'][0]['source_sha256']='wrong_source'
        assert release.privacy_receipt_summary(ROOT,receipt)['verified'] is False
        receipt['checks'][0]['source_sha256']=fingerprint
        receipt['checks'].pop(1)
        assert release.privacy_receipt_summary(ROOT,receipt)['verified'] is False


def test_exact_package_with_weighted_or_different_live_alias_fails_release_gate():
    import release_package_evidence as packages
    with tempfile.TemporaryDirectory() as temp:
        root=Path(temp);source=root/'aws/lambdas/justhodl-test/source';source.mkdir(parents=True)
        (source/'lambda_function.py').write_text('VALUE=1\n')
        (source.parent/'config.json').write_text('{"release_validation":{"schema_version":"1"}}')
        archive=io.BytesIO()
        with zipfile.ZipFile(archive,'w') as zipfile_out:zipfile_out.writestr('lambda_function.py','VALUE=1\n')
        alias={'FunctionVersion':'17','RoutingConfig':{}}
        client=SimpleNamespace(get_function=lambda **kw:{'Configuration':{'Version':'17','State':'Active','LastUpdateStatus':'Successful'},'Code':{'Location':'fixture-url'}},
                               get_alias=lambda **kw:copy.deepcopy(alias))
        with patch.object(packages.urllib.request,'urlopen',side_effect=lambda *a,**kw:io.BytesIO(archive.getvalue())), \
             patch.object(packages.subprocess,'check_output',return_value=b'aws/lambdas/justhodl-test/source/lambda_function.py\0'):
            assert packages.check_packages(client,root,['justhodl-test'])[0]['pass'] is True
            alias['RoutingConfig']={'AdditionalVersionWeights':{'16':0.1}}
            weighted=packages.check_packages(client,root,['justhodl-test'])[0]
            assert weighted['pass'] is False and weighted['alias_verified'] is False
            verifier=release.ReleaseVerifier(root,{'lambda':client},parity_seconds=0,package_check=packages.check_packages)
            verifier.checkpoint=lambda:None
            with patch.object(release,'changed_scope',return_value={'justhodl-test':['changed']}), \
                 patch.object(release,'artifact_map',return_value={'justhodl-test':{'primary_keys':[]}}), \
                 patch.object(release,'git',return_value='0'*40):
                result=verifier.run()
            assert result['status']=='SOURCE_PARITY_FAILED' and result['invocations']==[]
            alias.update(FunctionVersion='16',RoutingConfig={})
            assert packages.check_packages(client,root,['justhodl-test'])[0]['pass'] is False


def test_dependency_stages_publish_donors_before_their_actual_receivers():
    stage={name:i for i,names in enumerate(release.QUIET_STAGES) for name in names}
    assert stage['tradingview'] < stage['risk-gate'] < stage['engine-fusion'] < stage['khalid-risk']
    assert stage['khalid-risk'] < stage['portfolio-snapshot'] < stage['risk-sizer']
    assert stage['short-interest'] < stage['squeeze-fuel'] and stage['short-interest'] < stage['trade-tickets']
    assert stage['crypto-basis'] < stage['sizing-engine'] and stage['factor-risk'] < stage['firm-risk-board']
    assert stage['calibration-snapshotter'] < stage['backtest-engine']


def test_calibration_outputs_have_distinct_producer_contracts_and_readiness_checks():
    mapping=release.artifact_map(ROOT,['justhodl-calibration-snapshotter','justhodl-calibrator'])
    assert mapping['justhodl-calibration-snapshotter']['primary_keys']==['calibration/model-latest.json']
    assert mapping['justhodl-calibrator']['primary_keys']==['calibration/latest.json']
    assert mapping['justhodl-calibrator']['expected_output_contracts']['calibration/latest.json']=='calibrator_horizon_and_accuracy_report'
    assert 'calibration/model-latest.json' in release.ACCOUNTING_KEYS
    assert 'calibration/latest.json' not in release.ACCOUNTING_KEYS


def test_calibrator_old_snapshot_cannot_satisfy_report_contract():
    now=datetime.now(timezone.utc)
    code={'last_modified':(now-timedelta(hours=1)).isoformat()}
    doc={'generated_at':now.isoformat(),'snapshot_id':'cal-old','model_version':'calibrator-ssm-weights','weights':{}}
    result=release.inspect_output(fixture_output(doc),'justhodl-calibrator','calibration/latest.json',code,now=now)
    assert result['status']=='CONTRACT_FAILED'
    assert 'CALIBRATOR_REPORT_REPLACED_BY_MODEL_SNAPSHOT' in result['errors']
    assert result['expected_output_contract']=='calibrator_horizon_and_accuracy_report'
    doc={'generated_at':now.isoformat(),'total_outcomes':0,'signal_types_tracked':0,'weights':{},
         'accuracy_by_type':{},'window_accuracy':{},'window_weights':{},'recommended_horizon':{},
         'khalid_component_weights':{},'recommendations':[]}
    result=release.inspect_output(fixture_output(doc),'justhodl-calibrator','calibration/latest.json',code,now=now)
    assert result['status']=='VERIFIED' and not result['errors']


def test_calibration_snapshot_and_backtests_are_reviewed_quiet_but_calibrator_is_not():
    for name in ('calibration-snapshotter','backtest-engine','research-backtest','options-flow-scanner','bloomberg-v8','ecb-derived'):
        calls=[]
        checker=lambda client,root,names:[{'function':names[0],'pass':True,'qualifier':'live','version':'17'}]
        verifier=release.ReleaseVerifier(ROOT,{'lambda':SimpleNamespace(invoke=lambda **kw:calls.append(kw) or {'StatusCode':202})},package_check=checker)
        verifier.checkpoint=lambda:None
        assert verifier.invoke_once('justhodl-'+name)
        assert calls[0]['Qualifier']=='17' and calls[0]['Payload']==b'{}'
        assert 'justhodl-'+name in verifier.requested_after
    for name in ('calibrator',):
        verifier=release.ReleaseVerifier(ROOT,{})
        try:verifier.invoke_once('justhodl-'+name)
        except ValueError:pass
        else:raise AssertionError('Notification or weights-changing handler was manually invoked')


def test_post_deployment_output_must_also_be_generated_after_requested_refresh():
    now=datetime.now(timezone.utc)
    doc={'generated_at':(now-timedelta(minutes=3)).isoformat()}
    code={'last_modified':(now-timedelta(hours=1)).isoformat()}
    result=release.inspect_output(fixture_output(doc),'justhodl-example','data/example.json',code,now=now,not_before=now-timedelta(minutes=1))
    assert result['status']=='PENDING_OUTPUT' and 'POST_REFRESH_GENERATION_PENDING' in result['requirements']
    doc['generated_at']=now.isoformat()
    result=release.inspect_output(fixture_output(doc),'justhodl-example','data/example.json',code,now=now,not_before=now-timedelta(minutes=1))
    assert result['status']=='VERIFIED'


def test_backtest_generation_must_follow_latest_verified_model_publication():
    now=datetime.now(timezone.utc)
    verifier=release.ReleaseVerifier(ROOT,{'s3':None})
    verifier.report['artifact_scope']={'justhodl-backtest-engine':{'primary_keys':['backtest/results.json']}}
    verifier.report['code']={'justhodl-backtest-engine':{'last_modified':(now-timedelta(hours=1)).isoformat()}}
    verifier.report['outputs']['justhodl-calibration-snapshotter']=[{'status':'VERIFIED','last_modified':now.isoformat()}]
    captured=[]
    def inspect(*args,**kw):
        captured.append(kw['not_before'])
        return {'status':'PENDING_OUTPUT'}
    with patch.object(release,'inspect_output',side_effect=inspect):
        assert verifier.inspect_function('justhodl-backtest-engine') is False
        verifier.requested_after['justhodl-backtest-engine']=now+timedelta(seconds=1)
        verifier.inspect_function('justhodl-backtest-engine')
    assert captured==[now,now+timedelta(seconds=1)]


def test_colliding_outputs_have_explicit_distinct_canonical_keys_and_schemas():
    expected={'justhodl-options-flow-scanner':'data/options-flow-scanner.json','justhodl-options-flow':'flow-data.json',
              'justhodl-bloomberg-v8':'data/bloomberg-report.json','justhodl-daily-report-v3':'data/report.json',
              'justhodl-ecb-derived':'data/ecb-derived.json'}
    mapping=release.artifact_map(ROOT,expected)
    for name,key in expected.items():
        assert mapping[name]['primary_keys']==[key]
        assert key in mapping[name]['expected_output_contracts']
        assert release.donor_checks(name,{'generated_at':'2026-09-09T00:00:00Z'})['errors']


def test_bloomberg_uses_its_actual_utc_generation_field_and_rejects_wrong_report():
    now=datetime.now(timezone.utc)
    doc={'engine':'justhodl-bloomberg-v8','schema_version':'bloomberg-report.v8.1','execution_eligible':False,
         'utc':now.isoformat(),'fred':{},'stocks':{},'stats':{},'signals':{},'yield_curve':[]}
    code={'last_modified':(now-timedelta(hours=1)).isoformat()}
    result=release.inspect_output(fixture_output(doc),'justhodl-bloomberg-v8','data/bloomberg-report.json',code,now=now)
    assert result['status']=='VERIFIED_BLOCKED_REQUIREMENTS' and result['generated_at']==now.isoformat()
    assert release.donor_checks('justhodl-daily-report-v3',doc)['errors']==['DAILY_REPORT_V10_OUTPUT_OWNERSHIP_INVALID']


def test_function_url_identity_uses_only_reviewed_names_and_hostname_metadata():
    calls=[]
    lookup={function:host for _,function,host in release.FUNCTION_URL_BINDINGS}
    def config(**kw):
        calls.append(kw)
        return {'FunctionUrl':'https://'+lookup[kw['FunctionName']]+'/?ignored=DO_NOT_REPORT',
                'AuthType':'NONE','OtherSensitiveMetadata':'DO_NOT_REPORT'}
    rows=release.observe_function_urls(SimpleNamespace(get_function_url_config=config),ROOT)
    assert all(row['status']=='VERIFIED' for row in rows if row['function'] is not None)
    assert not any(row['function'] is None for row in rows)
    assert lookup['fedliquidityapi']=='mjqyipzzwjcmx44irtvijecswm0nkikf.lambda-url.us-east-1.on.aws'
    assert calls==[{'FunctionName':function} for _,function,_ in release.FUNCTION_URL_BINDINGS]
    assert 'DO_NOT_REPORT' not in json.dumps(rows)
    lookup['fmp-fundamentals-agent']='different.lambda-url.us-east-1.on.aws'
    rows=release.observe_function_urls(SimpleNamespace(get_function_url_config=config),ROOT)
    assert rows[0]['status']=='PENDING_IDENTITY' and rows[0]['reason']=='FUNCTION_URL_HOSTNAME_MISMATCH'


def test_unreviewed_page_url_change_blocks_identity_without_cloud_lookup():
    with tempfile.TemporaryDirectory() as temp:
        root=Path(temp)
        for page,_,_ in release.FUNCTION_URL_BINDINGS:(root/page).write_text('https://changed.example.invalid/')
        class NoLookup:
            def get_function_url_config(self,**kw):raise AssertionError('Changed page triggered guessed discovery')
        rows=release.observe_function_urls(NoLookup(),root)
        assert all(row['reason']=='PAGE_URL_CHANGED_SINCE_REVIEW' for row in rows if row['function'] is not None)


def snapshot_index_fixture():
    now=datetime.now(timezone.utc)
    day=now.date().isoformat()
    return {'schema_version':'daily-snapshot-index.v1','generated_at':now.isoformat(),'complete':True,
            'coverage_scope':'all listed source-owned public daily copies',
            'semantics':'MUTABLE_DAILY_COPY; filename date is not certified decision-time availability',
            'n_snapshots':1,'dates':[day],
            'snapshots':[{'key':'data/snapshots/data_yield-curve-'+day+'.json','source_key':'data/yield-curve.json',
                          'capture_date':day,'object_last_modified':now.isoformat(),'size_bytes':0,
                          'immutable':False,'point_in_time_certified':False,'content_status':'LISTED_NOT_CONTENT_VALIDATED'}]}


def test_daily_snapshot_index_is_primary_and_quiet_publish_follows_other_donors():
    mapping=release.artifact_map(ROOT,['justhodl-whats-changed'])
    assert mapping['justhodl-whats-changed']['primary_keys']==['data/whats-changed.json','data/snapshots-index.json']
    assert 'whats-changed' in release.QUIET_STAGES[-1]
    checker=lambda client,root,names:[{'function':names[0],'pass':True,'qualifier':'$LATEST'}]
    calls=[]
    verifier=release.ReleaseVerifier(ROOT,{'lambda':SimpleNamespace(invoke=lambda **kw:calls.append(kw) or {'StatusCode':202})},package_check=checker)
    verifier.checkpoint=lambda:None
    assert verifier.invoke_once('justhodl-whats-changed') and calls[0]['Payload']==b'{}'


def test_daily_snapshot_index_valid_metadata_preserves_explicit_mutable_availability_limit():
    doc=snapshot_index_fixture()
    now=datetime.now(timezone.utc)
    result=release.inspect_output(fixture_output(doc),'justhodl-whats-changed','data/snapshots-index.json',
                                  {'last_modified':(now-timedelta(hours=1)).isoformat()},now=now)
    assert result['status']=='VERIFIED_BLOCKED_REQUIREMENTS' and not result['errors']
    assert result['counts']=={'snapshots':1,'dates':1}
    assert result['requirements']==['MUTABLE_DAILY_COPIES_NOT_POINT_IN_TIME_CERTIFIED']


def test_daily_snapshot_index_rejects_private_or_fabricated_dates_counts_and_certification():
    changes=[lambda d:d.update(complete=False),lambda d:d.update(schema_version='legacy'),
             lambda d:d.update(n_snapshots=2),lambda d:d.update(dates=['1999-01-01']),
             lambda d:d['snapshots'][0].update(source_key='portfolio/snapshot.json'),
             lambda d:d['snapshots'][0].update(key='data/snapshots/../private.json'),
             lambda d:d['snapshots'][0].update(capture_date='2026-02-30'),
             lambda d:d['snapshots'][0].update(object_last_modified=None),
             lambda d:d['snapshots'][0].update(size_bytes=-1),
             lambda d:d['snapshots'][0].update(point_in_time_certified=True),
             lambda d:d['snapshots'][0].update(immutable=True)]
    for change in changes:
        doc=snapshot_index_fixture();change(doc)
        assert release.donor_checks('justhodl-whats-changed',doc,'data/snapshots-index.json')['errors']


def actual_archive_index_fixture():
    scope=runpy.run_path(str(ROOT/'aws/lambdas/justhodl-public-archive-index/tests/run_tests.py'))
    fixture=scope['HandlerTests']('test_actual_handler_collects_all_pages_zero_bytes_and_exact_metadata')
    fixture.setUp()
    fixture.test_actual_handler_collects_all_pages_zero_bytes_and_exact_metadata()
    return fixture


def test_archive_index_primary_scope_matches_the_exact_anchored_registry():
    registry=release.public_archive_registry(ROOT)
    mapping=release.artifact_map(ROOT,['justhodl-public-archive-index'])['justhodl-public-archive-index']
    assert mapping['primary_keys']==['data/archive-indexes/catalog.json']+['data/archive-indexes/'+engine+'.json' for engine in registry]
    calls=[]
    checker=lambda client,root,names:[{'function':names[0],'pass':True,'qualifier':'live','version':'17'}]
    verifier=release.ReleaseVerifier(ROOT,{'lambda':SimpleNamespace(invoke=lambda **kw:calls.append(kw) or {'StatusCode':202})},package_check=checker)
    verifier.checkpoint=lambda:None
    assert verifier.invoke_once('justhodl-public-archive-index') and calls[0]['Qualifier']=='17'


def test_actual_archive_handler_outputs_pass_metadata_contract_without_reading_archives():
    fixture=actual_archive_index_fixture()
    for key,doc in fixture.s3.writes.items():
        checked=release.donor_checks('justhodl-public-archive-index',doc,key)
        assert not checked['errors'],(key,checked['errors'])
        assert checked['requirements']==['ARCHIVE_METADATA_DOES_NOT_CERTIFY_CONTENT_OR_POINT_IN_TIME_AVAILABILITY']


def test_archive_checker_rejects_forged_membership_partial_rows_and_availability_claims():
    fixture=actual_archive_index_fixture()
    original=fixture.s3.writes[fixture.key]
    changes=[lambda d:d.update(families=['data/archive/*.json']),lambda d:d.update(engine='unreviewed'),
             lambda d:d.update(n_snapshots=0),lambda d:d.update(complete=False,status='INDEX_UNAVAILABLE'),
             lambda d:d['snapshots'][0].update(key='backtest/ledger/private.json'),
             lambda d:d['snapshots'][0].update(last_modified=None),lambda d:d['snapshots'][0].update(size_bytes=-1),
             lambda d:d['snapshots'][0].update(immutable=True),lambda d:d['snapshots'][0].update(point_in_time_certified=True),
             lambda d:d['snapshots'][0].update(content_status='VALIDATED')]
    for change in changes:
        doc=copy.deepcopy(original);change(doc)
        assert release.donor_checks('justhodl-public-archive-index',doc,fixture.key)['errors']
    catalog=copy.deepcopy(fixture.s3.writes['data/archive-indexes/catalog.json'])
    catalog['indexes'].pop()
    assert 'ARCHIVE_CATALOG_MEMBERSHIP_INVALID' in release.donor_checks('justhodl-public-archive-index',catalog,'data/archive-indexes/catalog.json')['errors']


def test_incomplete_archive_listing_is_explicit_blocked_requirement_without_partial_rows():
    fixture=actual_archive_index_fixture()
    fixture.test_access_denial_discards_partial_keys_and_publishes_unavailable()
    for key in (fixture.key,'data/archive-indexes/catalog.json'):
        checked=release.donor_checks('justhodl-public-archive-index',fixture.s3.writes[key],key)
        assert not checked['errors'],checked
        assert 'REVIEWED_ARCHIVE_LISTING_UNAVAILABLE' in checked['requirements']


def test_registry_scope_is_read_from_passed_release_root_without_loading_sdk_clients():
    with tempfile.TemporaryDirectory() as temp:
        root=Path(temp);source=root/'aws/lambdas/justhodl-public-archive-index/source/lambda_function.py'
        source.parent.mkdir(parents=True)
        source.write_text("raise AssertionError('must not execute module')\nREGISTRY=(('justhodl-reviewed','data/archive/reviewed/*.json'),)\n")
        assert release.public_archive_registry(root)=={'justhodl-reviewed':'data/archive/reviewed/*.json'}


def test_cadence_only_schedule_readback_uses_recorded_exact_identity_and_preserves_private_input():
    expected={'liquidity-profile-fri':'justhodl-liquidity-profile','justhodl-retail-sentiment-30min':'justhodl-retail-sentiment'}
    calls=[]
    def describe(**kw):
        calls.append(('describe',kw['Name']))
        assert kw['Name'] in expected
        return {'ScheduleExpression':'cron(0 22 ? * FRI *)','State':'DISABLED'}
    def targets(**kw):
        function=expected[kw['Rule']]
        return {'Targets':[{'Arn':f'arn:aws:lambda:us-east-1:123:function:{function}:live','Input':'DO_NOT_REPORT'},
                           {'Arn':'arn:aws:sqs:us-east-1:123:other','Input':'DO_NOT_REPORT'}]}
    client=SimpleNamespace(describe_rule=describe,list_targets_by_rule=targets)
    rows=release.observe_schedules({'events':client},ROOT,list(expected.values()))
    assert len(rows)==2 and all(row['status']=='OBSERVED_CADENCE_ONLY' for row in rows)
    assert all(row['observation_status']=='VERIFIED_IDENTITY' and row['mutation_requested'] is False for row in rows)
    assert all(row['cadence_matches'] is False for row in rows)
    assert 'DO_NOT_REPORT' not in json.dumps(rows)
    assert {name for _,name in calls}==set(expected)


def test_recorded_cadence_target_mismatch_is_explicit_information_without_rebinding():
    client=SimpleNamespace(describe_rule=lambda **kw:{'ScheduleExpression':'rate(1 hour)','State':'ENABLED'},
                           list_targets_by_rule=lambda **kw:{'Targets':[{'Arn':'arn:aws:lambda:us-east-1:123:function:other'}]})
    rows=release.observe_schedules({'events':client},ROOT,['justhodl-liquidity-profile'])
    assert len(rows)==1 and rows[0]['observation_status']=='UNPROVEN_IDENTITY'
    assert rows[0]['status']=='OBSERVED_CADENCE_ONLY' and rows[0]['matching_target_arns']==[]


def test_notification_monitors_receive_only_reviewed_explicit_quiet_modes_and_verified_versions():
    expected={'justhodl-fleet-freshness-monitor':{'mode':'quiet_refresh'},'justhodl-fleet-error-monitor':{'mode':'audit_refresh'}}
    for function,payload in expected.items():
        calls=[]
        checker=lambda client,root,names:[{'function':names[0],'pass':True,'qualifier':'live','version':'23'}]
        verifier=release.ReleaseVerifier(ROOT,{'lambda':SimpleNamespace(invoke=lambda **kw:calls.append(kw) or {'StatusCode':202})},package_check=checker)
        verifier.checkpoint=lambda:None
        assert verifier.invoke_once(function)
        assert json.loads(calls[0]['Payload'])==payload and calls[0]['Qualifier']=='23'
        assert verifier.report['invocations'][0]['mode']==payload['mode']
        assert calls[0]['InvocationType']=='Event'
        assert set(release.APPROVED_REFRESH_MODES)==set(expected)


def test_quiet_mode_does_not_bypass_immediate_source_and_alias_parity_guard():
    for function in release.APPROVED_REFRESH_MODES:
        class NoInvoke:
            def invoke(self,**kw):raise AssertionError('Source drift must reject before any invocation')
        verifier=release.ReleaseVerifier(ROOT,{'lambda':NoInvoke()},package_check=lambda client,root,names:[{'function':names[0],'pass':False}])
        try:verifier.invoke_once(function)
        except ValueError as error:assert str(error)=='code_changed_before_invocation'
        else:raise AssertionError('Old notification-capable source was invoked')


def test_metric_shared_rule_binding_observer_paginates_and_never_claims_dedicated_cadence():
    calls=[]
    def targets(**kw):
        calls.append(kw)
        function='justhodl-ka-metrics' if kw.get('NextToken') else 'justhodl-khalid-metrics'
        return {'Targets':[{'Arn':'arn:aws:lambda:us-east-1:123:function:'+function+':live','Input':'PRIVATE_CONFIG_BODY'}],**({} if kw.get('NextToken') else {'NextToken':'page2'})}
    events=SimpleNamespace(describe_rule=lambda **kw:{'State':'ENABLED','ScheduleExpression':'rate(1 hour)'},list_targets_by_rule=targets)
    with patch.object(release,'release_config',return_value={'eventbridge_rules':['legacy-metric-refresh']}):
        rows=release.observe_schedules({'events':events},ROOT,['justhodl-ka-metrics','justhodl-khalid-metrics'])
    assert len(rows)==2 and len(calls)==2
    assert all(row['status']=='PENDING_CONFIGURATION' and row['reason']=='SHARED_RULE_OWNERSHIP_REVIEW_REQUIRED' for row in rows)
    assert all(len(row['observed_lambda_target_arns'])==2 and len(row['matching_target_arns'])==1 and row['dedicated_cadence_verified'] is False for row in rows)
    assert 'PRIVATE_CONFIG_BODY' not in json.dumps(rows)
    assert all(row['mutation_requested'] is False for row in rows)


def test_metric_rule_missing_one_producer_target_is_explicit_not_a_verified_shared_alias():
    events=SimpleNamespace(describe_rule=lambda **kw:{'State':'ENABLED','ScheduleExpression':'rate(1 hour)'},list_targets_by_rule=lambda **kw:{'Targets':[{'Arn':'arn:aws:lambda:us-east-1:123:function:justhodl-ka-metrics:7'}]})
    with patch.object(release,'release_config',return_value={'eventbridge_rules':['legacy-metric-refresh']}):
        rows=release.observe_schedules({'events':events},ROOT,['justhodl-ka-metrics','justhodl-khalid-metrics'])
    khalid=next(row for row in rows if row['function']=='justhodl-khalid-metrics')
    assert khalid['reason']=='DEDICATED_METRIC_TARGET_MISSING' and khalid['matching_target_arns']==[]


def test_archive_registry_accepts_only_anchored_single_basename_families_and_rejects_private_or_traversal():
    with tempfile.TemporaryDirectory() as temp:
        root=Path(temp);source=root/'aws/lambdas/justhodl-public-archive-index/source/lambda_function.py';source.parent.mkdir(parents=True)
        rows=(('justhodl-reviewed','data/activity-nowcast/snapshots/*.json'),('justhodl-reviewed2','screener/snapshots/*.json'))
        source.write_text('REGISTRY='+repr(rows)+'\n')
        assert release.public_archive_registry(root)==dict(rows)
        for pattern in ('data/../secret/*.json','data/*/snapshots/*.json','data/brain.json','backtest/ledger/*.json'):
            source.write_text('REGISTRY='+repr((('justhodl-reviewed',pattern),))+'\n')
            try:release.public_archive_registry(root)
            except ValueError:pass
            else:raise AssertionError('Unsafe archive family accepted '+pattern)


def test_owned_metric_and_public_metadata_contracts_reject_previous_writer_payloads():
    doc={'schema_version':'macro-analysis.v1','engine':'justhodl-ka-metrics','input_artifact':'data/ka-metrics.json','llm_status':'available'}
    assert not release.donor_checks('justhodl-ka-metrics',doc,'data/ka-analysis.json')['errors']
    assert 'METRIC_OUTPUT_OWNER_OR_SCHEMA_INVALID' in release.donor_checks('justhodl-khalid-metrics',doc,'data/khalid-analysis.json')['errors']
    public=release.source_map_public({})
    assert not release.donor_checks('justhodl-source-map',public)['errors']
    assert 'PRIVATE_ATTRIBUTION_INPUT_UNAVAILABLE' in release.donor_checks('justhodl-source-map',public)['requirements']
    public['raw_description']='DO_NOT_REPORT'
    checked=release.donor_checks('justhodl-source-map',public)
    assert checked['errors']==['PUBLIC_SOURCE_MAP_PROJECTION_INVALID'] and 'DO_NOT_REPORT' not in json.dumps(checked)
    fleet=release.fleet_errors_public({'version':'1.1.0','n_alerts_detected':0,'alerts':[],'dlq_status':{'visible':0,'inflight':0,'total':0},'notification_mode':'suppressed_for_audit'})
    assert not release.donor_checks('justhodl-fleet-error-monitor',fleet)['errors']
    fleet['telegram_sent']=True
    assert 'QUIET_MONITOR_SENT_NOTIFICATION' in release.donor_checks('justhodl-fleet-error-monitor',fleet)['errors']


def test_dedicated_metric_schedulers_require_correct_target_expression_input_and_state():
    functions=['justhodl-ka-metrics','justhodl-khalid-metrics']
    for failure in (None,'target','expression','input','state','timezone'):
        def get_schedule(**kw):
            function=kw['Name'].removesuffix('-hourly')
            return {'ScheduleExpressionTimezone':'America/New_York' if failure=='timezone' else 'UTC',
                    'State':'DISABLED' if failure=='state' else 'ENABLED',
                    'ScheduleExpression':'rate(1 day)' if failure=='expression' else 'rate(1 hour)',
                    'Target':{'Arn':'arn:aws:lambda:us-east-1:857687956942:function:'+('other' if failure=='target' else function),
                              'Input':'{"mode":"unreviewed"}' if failure=='input' else '{}'}}
        rows=release.observe_schedules({'scheduler':SimpleNamespace(get_schedule=get_schedule)},ROOT,functions)
        assert len(rows)==2 and len({row['name'] for row in rows})==2
        assert all((row['status']=='VERIFIED')==(failure is None) for row in rows)


def test_metric_generation_uses_the_source_defined_generated_field_and_two_hour_age_limit():
    now=release.utcnow();code={'last_modified':(now-timedelta(hours=3)).isoformat()}
    for function in ('justhodl-ka-metrics','justhodl-khalid-metrics'):
        doc={'engine':function,'schema_version':'macro-metrics.v1','generated':now.isoformat()}
        result=release.inspect_output(fixture_output(doc),function,'data/'+function.removeprefix('justhodl-')+'.json',code,now=now)
        assert result['status']=='VERIFIED'
        doc['generated']=(now-timedelta(hours=2,minutes=1)).isoformat()
        result=release.inspect_output(fixture_output(doc),function,'data/'+function.removeprefix('justhodl-')+'.json',code,now=now)
        assert result['status']=='PENDING_OUTPUT' and 'FRESH_GENERATION_PENDING' in result['requirements']


def test_source_bound_alternate_generation_fields_preserve_future_and_age_guards():
    now=release.utcnow();code={'last_modified':(now-timedelta(hours=1)).isoformat()}
    for name,field in release.GENERATION_FIELDS.items():
        doc={field:now.isoformat()}
        with patch.object(release,'donor_checks',return_value={'errors':[],'requirements':[],'counts':{}}):
            row=release.inspect_output(fixture_output(doc),name,'data/example.json',code,now=now)
            assert row['status']=='VERIFIED'
            doc[field]=(now+timedelta(hours=1)).isoformat()
            row=release.inspect_output(fixture_output(doc),name,'data/example.json',code,now=now)
            assert row['status']=='CONTRACT_FAILED' and 'GENERATION_TIMESTAMP_INVALID' in row['errors']


def test_governed_schedule_cannot_pass_on_an_unverified_numeric_or_other_named_alias():
    config={'release_validation':{'schema_version':'1.1'},'eventbridge_scheduler':{'schedule_name':'risk-schedule','cron':'rate(15 minutes)'}}
    for qualifier in ('live','7','canary','$LATEST',''):
        arn='arn:aws:lambda:us-east-1:857687956942:function:justhodl-risk-gate'+(':'+qualifier if qualifier else '')
        client=SimpleNamespace(get_schedule=lambda **kw:{'State':'ENABLED','ScheduleExpression':'rate(15 minutes)','Target':{'Arn':arn}})
        with patch.object(release,'release_config',return_value=config):rows=release.observe_schedules({'scheduler':client},ROOT,['justhodl-risk-gate'])
        assert (rows[0]['status']=='VERIFIED')==(qualifier=='live')
