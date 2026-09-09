import importlib.util
import io
import json
import ssl
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

SOURCE = Path(__file__).resolve().parents[2] / 'aws/lambdas/justhodl-cot-extremes-scanner/source'
spec = importlib.util.spec_from_file_location('cot_data', SOURCE / 'cot_data.py')
data = importlib.util.module_from_spec(spec); spec.loader.exec_module(data)


def handler():
    spec = importlib.util.spec_from_file_location('cot_handler_test', SOURCE / 'lambda_function.py')
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {'cot_data': data, 'boto3': types.SimpleNamespace(client=lambda *a, **k: MagicMock())}):
        spec.loader.exec_module(module)
    return module


def provider_rows(count=30):
    today = datetime.now(timezone.utc).date() - timedelta(days=3)
    return [{'cftc_contract_market_code': '13874A', 'report_date_as_yyyy_mm_dd': (today-timedelta(weeks=i)).isoformat(),
             'asset_mgr_positions_long': str(100-i), 'asset_mgr_positions_short': '10',
             'lev_money_positions_long': '20', 'lev_money_positions_short': '10', 'open_interest_all': '1000'} for i in range(count)]


def document():
    return {'history': data.parse_rows(provider_rows(), 'tff', '13874A'), 'report_type': 'tff', 'refresh_status': 'FETCHED'}


def test_real_cftc_api_fields_and_zero_are_preserved_without_missing_zero_imputation():
    rows = provider_rows(1)
    result = data.parse_rows(rows, 'tff', '13874A')[0]
    assert result['spec_net'] == 100 and result['ratio'] == .1
    for name in data.FIELDS['tff']: rows[0][name] = '0'
    assert data.parse_rows(rows, 'tff', '13874A')[0]['ratio'] == 0
    del rows[0]['asset_mgr_positions_long']
    assert data.parse_rows(rows, 'tff', '13874A')[0]['ratio'] is None
    assert all(data.integer(value) is None for value in (None, '', True, 'NaN', '1.5', '1e309'))
    assert data.integer('1,000.00') == 1000


def test_cftc_identity_duplicates_and_impossible_positions_fail_explicitly():
    for rows in (provider_rows(1)*2, [dict(provider_rows(1)[0], cftc_contract_market_code='WRONG')],
                 [dict(provider_rows(1)[0], report_date_as_yyyy_mm_dd='2099-01-01')]):
        try: data.parse_rows(rows, 'tff', '13874A')
        except ValueError: pass
        else: raise AssertionError('invalid identity or clock accepted')
    for value in ('0', '-1', None):
        row = dict(provider_rows(1)[0], open_interest_all=value)
        assert data.parse_rows([row], 'tff', '13874A')[0]['ratio'] is None
    row = dict(provider_rows(1)[0], asset_mgr_positions_long='10000')
    assert data.parse_rows([row], 'tff', '13874A')[0]['ratio'] is None


def test_percentiles_exclude_current_and_stale_or_incomplete_inputs_cannot_signal():
    today = datetime.now(timezone.utc).date(); info = handler().COT_CONTRACTS['ES']
    doc = document(); row = data.summarize('ES', info, doc, today)
    assert row['percentile'] == 100 and row['n_prior_observations'] == 29 and row['execution_eligible'] is False
    assert row['trend_baseline_date'] == (today-timedelta(days=31)).isoformat()
    assert data.percentile_rank([1, 1, 1], 1) == 50
    for changed in (dict(doc, refresh_status='FAILED'), dict(doc, history=doc['history'][:10])):
        assert data.summarize('ES', info, changed, today)['percentile'] is None
    assert data.summarize('ES', info, doc, today+timedelta(days=11))['status'] == 'stale_report'
    doc['history'][0]['status'] = 'INVALID_POSITION_OR_OPEN_INTEREST'
    assert data.summarize('ES', info, doc, today)['status'] == 'invalid_history'


def test_calendar_trend_does_not_call_four_observations_four_weeks():
    doc=document(); info=handler().COT_CONTRACTS['ES']; today=datetime.now(timezone.utc).date()
    # Remove the target baseline and every nearby older observation.
    doc['history']=[r for r in doc['history'] if not 28 <= (today-timedelta(days=3)-datetime.fromisoformat(r['date']).date()).days <= 49]
    doc['history']=data.parse_rows(provider_rows(40), 'tff', '13874A')[:-8]+doc['history'][-4:]
    row=data.summarize('ES',info,doc,today)
    assert row['status']=='ok' and row['trend_4w'] is None


def test_cftc_transport_keeps_tls_verification_rejects_redirects_and_caps_bodies():
    module=handler(); assert module.ctx.check_hostname and module.ctx.verify_mode == ssl.CERT_REQUIRED
    assert module.NoRedirect().redirect_request(None,None,302,'',{},'https://elsewhere.invalid') is None
    for raw in (b'[]', b'{"x":NaN}', b'x'*(module.MAX_PROVIDER_BYTES+1)):
        with patch.object(module.urllib.request,'build_opener') as opener:
            response=opener.return_value.open.return_value.__enter__.return_value;response.read.return_value=raw
            result=module.fetch_url('https://publicreporting.cftc.gov/resource/gpe5-46if.json')
            assert result == ([] if raw==b'[]' else None)
            response.read.assert_called_once_with(module.MAX_PROVIDER_BYTES+1)
    with patch.object(module.urllib.request,'build_opener') as opener:
        assert module.fetch_url('https://publicreporting.cftc.gov.evil.invalid/resource/gpe5-46if.json') is None
        opener.assert_not_called()


def test_universe_extension_cannot_inject_s3_paths_or_cftc_query_expressions():
    module=handler()
    ext={'contracts': {'../private': {'cftc_code':'123456'}, 'XX': {'cftc_code':"' OR 1=1"}, 'GOOD': {'cftc_code':'123456','category':'currency','name':'Additional'}}}
    module.s3.get_object.return_value={'Body':io.BytesIO(json.dumps(ext).encode())}
    universe,status=module.load_universe()
    assert status=='PARTIAL_INVALID_EXTENSION_ROWS' and 'GOOD' in universe and '../private' not in universe and 'XX' not in universe


def test_quiet_refresh_retains_failed_contracts_and_all_histories_without_notifications():
    module=handler(); universe={key:dict(module.COT_CONTRACTS['ES']) for key in ('ES','NQ','YM','MISSING')}
    module.load_universe=lambda:(universe,'LOADED')
    def maintained(contract,info,deadline):
        return dict(document(),schema_version=module.HISTORY_SCHEMA,engine=module.ENGINE,contract=contract,execution_eligible=False,point_in_time_certified=False) if contract!='MISSING' else {'history':[],'refresh_status':'PROVIDER_UNAVAILABLE'}
    module.maintain_history=maintained; module.send_telegram=MagicMock(side_effect=AssertionError('notification forbidden'))
    result=module.lambda_handler({'mode':'audit_refresh'},None)
    doc=json.loads(module.s3.put_object.call_args.kwargs['Body'])
    assert set(doc['histories'])==set(universe) and len(doc['contracts'])==4 and doc['summary']['n_processed']==3
    assert doc['summary']['n_cluster_alerts']==1 and doc['summary']['n_errors']==1
    assert doc['notification_status']=='SUPPRESSED_AUDIT_REFRESH' and len(doc['histories']['ES']['history'])==30
    module.send_telegram.assert_not_called();module.ssm.get_parameter.assert_not_called()
    assert json.loads(result['body'])['summary']['n_returned']==4


def test_selected_contract_does_not_replace_global_output_and_primary_failure_retries():
    module=handler(); module.load_universe=lambda:({'ES':module.COT_CONTRACTS['ES']},'LOADED')
    module.maintain_history=lambda *a:document()
    assert module.lambda_handler({'contract':'WRONG'},None)['statusCode']==400
    result=module.lambda_handler({'contract':'ES'},None)
    assert json.loads(result['body'])['scope']=='SELECTED_CONTRACT';module.s3.put_object.assert_not_called()
    module.s3.put_object.side_effect=RuntimeError('write failed')
    try:module.lambda_handler({'mode':'audit_refresh'},None)
    except RuntimeError:pass
    else:raise AssertionError('primary failure reported success')


def test_full_history_refetch_does_not_reuse_bad_legacy_cache_or_mix_report_types():
    module=handler();module.fetch_url=lambda url:provider_rows()
    module.s3.get_object.side_effect=AssertionError('v1 cache cannot be reused')
    doc=module.maintain_history('ES',module.COT_CONTRACTS['ES'],float('inf'))
    assert doc['report_type']=='tff' and doc['history'][-1]['spec_net']==100 and doc['archive_status']=='PUBLISHED'
    module.s3.get_object.assert_not_called()
    assert module.s3.put_object.call_args.kwargs['Key']=='cot/history/ES.json'
