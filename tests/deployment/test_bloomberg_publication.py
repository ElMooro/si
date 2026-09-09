import importlib.util
import json
import sys
import types
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / 'aws/lambdas/justhodl-bloomberg-v8/source'
spec = importlib.util.spec_from_file_location('bloomberg_math', SOURCE / 'bloomberg_math.py')
maths = importlib.util.module_from_spec(spec); spec.loader.exec_module(maths)


def handler():
    secret = types.SimpleNamespace(managed_secret=lambda *a: 'SECRET_CANARY')
    boto = types.SimpleNamespace(client=lambda *a, **k: MagicMock())
    alias = types.SimpleNamespace(add_ka_aliases=lambda value: value)
    spec = importlib.util.spec_from_file_location('bloomberg_test', SOURCE / 'lambda_function.py')
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {'boto3': boto, 'managed_secret': secret, 'ka_aliases': alias,
                                 '_fred_shim': types.ModuleType('_fred_shim'), 'bloomberg_math': maths}):
        spec.loader.exec_module(module)
    return module


def test_weekly_macro_month_comparison_uses_calendar_dates_and_keeps_missing():
    rows = [{'date': (date(2026, 9, 2)-timedelta(weeks=i)).isoformat(), 'value': str(200-i*10)} for i in range(12)]
    data = maths.fred_statistics(rows, 'weekly')
    assert data['change_baselines']['1m']['date'] == '2026-07-29'
    assert data['chg_1m'] == maths.percent(200, 150)
    assert data['chg_1y'] is None and len(data['history']) == 12
    rows[0]['value'] = '.'
    data = maths.fred_statistics(rows, 'weekly')
    assert data['value'] is None and data['chg_1m'] is None
    assert maths.percent(0, 100) == -100 and maths.percent(10, 0) is None


def test_monthly_series_has_no_weekly_return_and_rejects_duplicate_dates():
    rows = [{'date': f'2026-{month:02}-01', 'value': month} for month in range(1, 10)]
    data = maths.fred_statistics(rows, 'monthly')
    assert data['chg_1w'] is None and data['chg_1m'] == maths.percent(9, 8)
    data = maths.fred_statistics(rows + [rows[-1]], 'monthly')
    assert data['invalid_or_duplicate_rows'] == 1 and data['chg_1m'] is None


def bars(values):
    return [{'t': int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()*1000) + i*86400000,
             'c': v, 'h': v+1, 'l': v-1, 'v': 100} for i, v in enumerate(values)]


def test_rsi_extremes_missing_sma_and_open_session_exclusion():
    for values, expected in ((range(10, 40), 100), (range(40, 10, -1), 0), ([10]*30, 50)):
        data = maths.stock_statistics(bars(values), today=date(2026, 3, 1))
        assert data['rsi'] == expected and data['sma200'] is None and data['above_sma200'] is None
        assert data['chg_1y'] is None and data['high_52w'] is None
    data = maths.stock_statistics(bars(range(10, 40)), today=date(2026, 1, 30))
    assert data['close'] == 38 and len(data['history']) == 30 and data['completed_bar_count'] == 29
    assert data['change_pct'] == data['chg_1d']


def test_liquidity_converts_billions_and_preserves_zero_without_fake_growth():
    data = maths.liquidity({'WALCL': {'value': 1000000}, 'WTREGEN': {'value': 100000}, 'RRPONTSYD': {'value': 20}})
    assert data['net_liquidity'] == 880000 and data['rrp'] == 20000 and data['net_liquidity_chg_1m'] is None
    assert maths.liquidity({'WALCL': {'value': 100}, 'WTREGEN': {'value': 0}, 'RRPONTSYD': {'value': 0}})['net_liquidity'] == 100
    assert maths.liquidity({})['net_liquidity'] is None


def test_absent_market_inputs_cannot_produce_neutral_index_or_buy_signal():
    module = handler()
    score = module.calculate_khalid_index({}, {})
    assert score['score'] is None and score['regime'] == 'UNAVAILABLE' and not score['execution_eligible']
    assert module.generate_signals({}, {'SPY': {'rsi': None}}) == {'buys': [], 'sells': [], 'warnings': [], 'reversals': []}


def test_primary_publication_failure_is_retryable_and_archive_failure_is_explicit():
    module = handler()
    for name, value in [('fetch_fred_batch', {}), ('fetch_polygon_stocks', {}), ('fetch_crypto', {}), ('fetch_news', [])]:
        setattr(module, name, lambda *a, v=value: v)
    module.s3.get_object.side_effect = RuntimeError('private-read-error')
    module.S3_BUCKET = 'justhodl-dashboard-live'
    def fail_archive(**kw):
        if '/bloomberg-archive/' in kw['Key']:
            raise RuntimeError('SECRET_CANARY')
    module.s3.put_object.side_effect = fail_archive
    result = module.lambda_handler({}, None); doc = json.loads(result['body'])
    assert doc['archive_status'] == 'UNAVAILABLE' and doc['khalid_index']['score'] is None
    assert 'SECRET_CANARY' not in result['body']
    writes = module.s3.put_object.call_args_list
    assert all(call.kwargs['Bucket'] == 'justhodl-dashboard-live' for call in writes)
    assert writes[-1].kwargs['Key'] == 'data/bloomberg-report.json'
    module.s3.put_object.side_effect = RuntimeError('SECRET_CANARY')
    try:
        module.lambda_handler({}, None)
    except RuntimeError as error:
        assert str(error) == 'BLOOMBERG_PRIMARY_PUBLICATION_FAILED'
    else:
        raise AssertionError('publication failure reported success')


def test_provider_errors_nan_redirects_and_size_limits_cannot_leak_credentials():
    module = handler()
    with patch.object(module.urllib.request, 'build_opener') as opener:
        opener.return_value.open.side_effect = RuntimeError('SECRET_CANARY')
        assert module.fetch_url('https://api.polygon.io/?key=SECRET_CANARY') == {'error': 'PROVIDER_DATA_UNAVAILABLE'}
    for raw in (b'{"x":NaN}', b'{"x":1e999}', b'x'*5_000_001):
        with patch.object(module.urllib.request, 'build_opener') as opener:
            response = opener.return_value.open.return_value.__enter__.return_value
            response.read.return_value = raw
            assert module.fetch_url('https://api.polygon.io/') == {'error': 'PROVIDER_DATA_UNAVAILABLE'}
            response.read.assert_called_once_with(5_000_001)
    with patch.object(module.urllib.request, 'build_opener') as opener:
        module.fetch_url('https://api.polygon.io.evil.invalid/')
        opener.assert_not_called()
    assert module.NoRedirect().redirect_request(None, None, 302, '', {}, 'https://evil.invalid') is None
