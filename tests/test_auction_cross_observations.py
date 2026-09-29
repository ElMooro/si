"""Dated cross-source arithmetic and actual native adapters; synthetic only."""
from pathlib import Path
from datetime import date,datetime,timezone,timedelta
from copy import deepcopy
from unittest.mock import patch
from urllib.parse import urlsplit,parse_qs
import io,json,runpy,sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import auction_cross_observations as model
TODAY=date(2026,9,29)


def frame(sid,rows):
    return {'series_id':sid,'requested_limit':model.LIMITS[sid],'read_status':'received',
            'response':{'observations':[{'date':at,'value':value} for at,value in rows]},
            'adapter_read_at':'2026-09-29T07:00:00Z','http_acquisition_time_verified':False}


def fixture():
    data={'SOFR':[('2026-09-29','4.95')],'IORB':[('2026-09-29','4.90')],
          'DTWEXBGS':[('2026-09-29','110'),('2026-08-30','100')],
          'T10Y2Y':[('2026-09-29','0.25')],'T5YIFR':[('2026-09-29','2.6')]}
    return {sid:frame(sid,rows) for sid,rows in data.items()}


def engine():return runpy.run_path(str(ROOT/'tests/test_auction_benchmarks.py'))['engine']()


def test_missing_sources_cannot_become_zero_calm_or_anchored():
    out=model.build({},TODAY)
    for key in ('repo_stress','dollar_strength','curve_slope','inflation_expectations'):
        assert out[key]['measurement_status']=='unavailable' and out[key]['regime']=='UNAVAILABLE'
    assert out['repo_stress']['spread_bp'] is None and out['dollar_strength']['change_30d_pct'] is None
    assert all(out[k] is False for k in model.PERMISSIONS)


def test_reproduced_mismatched_dates_single_dollar_print_and_anchoring_claim_are_removed():
    data=fixture();data['SOFR']=frame('SOFR',[('2026-09-20','5')])
    data['DTWEXBGS']=frame('DTWEXBGS',[('2026-09-29','100')]);out=model.build(data,TODAY)
    assert out['repo_stress']['spread_bp'] is None
    assert out['dollar_strength']['level']==100 and out['dollar_strength']['change_30d_pct'] is None
    assert out['inflation_expectations']['rate_pct']==2.6 and out['inflation_expectations']['regime']=='DESCRIPTIVE'
    assert 'Above Fed target' not in json.dumps(out)


def test_rates_use_latest_common_date_and_preserve_each_sources_latest_date():
    data=fixture();data['SOFR']=frame('SOFR',[('2026-09-28','5'),('2026-09-25','4.8')])
    data['IORB']=frame('IORB',[('2026-09-29','4.9'),('2026-09-28','4.9')]);out=model.build(data,TODAY)['repo_stress']
    assert out['spread_bp']==10 and out['observation_date']=='2026-09-28'
    assert out['trace']['iorb_latest_source_date']=='2026-09-29' and out['trace']['sofr_latest_source_date']=='2026-09-28'
    assert out['regime']=='POSITIVE' and out['legacy_threshold_band']=='ACUTE'
    assert 'Collateral squeeze' not in out['interpretation'] and out['trace']['age_calendar_days']==1


def test_latest_common_missing_value_cannot_silently_fall_back_to_older_pair():
    data=fixture();data['SOFR']=frame('SOFR',[('2026-09-29','5'),('2026-09-27','.'),('2026-09-26','4')])
    data['IORB']=frame('IORB',[('2026-09-28','4.9'),('2026-09-27','4.9'),('2026-09-26','4')])
    out=model.build(data,TODAY)['repo_stress']
    assert out['spread_bp'] is None and out['err']=='latest_common_observation_missing'


def test_legacy_threshold_boundaries_use_the_displayed_basis_points_without_float_noise():
    for sofr,expected,band in [('4.91',1,'CALM'),('4.94',4,'WATCH'),('4.98',8,'ELEVATED')]:
        data=fixture();data['SOFR']=frame('SOFR',[('2026-09-29',sofr)])
        out=model.build(data,TODAY)['repo_stress']
        assert out['spread_bp']==expected and out['legacy_threshold_band']==band


def test_current_sources_without_a_current_common_date_withhold_the_spread():
    data=fixture();data['SOFR']=frame('SOFR',[('2026-09-29','5'),('2026-09-15','4')])
    data['IORB']=frame('IORB',[('2026-09-28','4.9'),('2026-09-15','4')])
    out=model.build(data,TODAY)['repo_stress'];assert out['spread_bp'] is None and out['err']=='common_observation_too_old'


def test_boolean_nonfinite_missing_latest_and_malformed_dates_do_not_get_numeric_authority():
    for value in (True,False,None,'','.','nan','inf','1_000'):
        data=fixture();data['SOFR']=frame('SOFR',[('2026-09-29',value),('2026-09-28','5')])
        assert model.build(data,TODAY)['repo_stress']['spread_bp'] is None
    for at in ('2026-09-30','2026-02-30','20260929',None):
        data=fixture();data['IORB']=frame('IORB',[(at,'4.9')]);assert model.build(data,TODAY)['repo_stress']['spread_bp'] is None
    data=fixture();data['SOFR']['response']['observations'][0]['value']=float('nan')
    try:model.build(data,TODAY)
    except ValueError:pass
    else:raise AssertionError('Nonfinite retained response serialized')


def test_duplicate_dates_wrong_series_and_over_limit_populations_are_explicitly_unavailable():
    for kind in ('duplicate','wrong_id','wrong_limit','excess'):
        data=fixture();f=data['SOFR']
        if kind=='duplicate':f['response']['observations']*=2
        elif kind=='wrong_id':f['series_id']='OTHER'
        elif kind=='wrong_limit':f['requested_limit']=99
        else:f['response']['observations']*=6
        out=model.build(data,TODAY);assert out['repo_stress']['spread_bp'] is None
        assert out['source_frames']['SOFR']==f


def test_exact_thirty_day_change_has_exact_dates_and_compatible_legacy_key():
    out=model.build(fixture(),TODAY)['dollar_strength']
    assert out['level']==110 and out['change_30d_pct']==out['change_30d_target_pct']==10
    trace=out['comparison'];assert trace['target_date']==trace['start']['date']=='2026-08-30'
    assert trace['end']['date']=='2026-09-29' and trace['elapsed_calendar_days']==30


def test_nonbusiness_target_uses_bounded_prior_observation_and_does_not_mislabel_legacy_key():
    data=fixture();data['DTWEXBGS']=frame('DTWEXBGS',[('2026-09-29','110'),('2026-08-28','100')])
    out=model.build(data,TODAY)['dollar_strength'];trace=out['comparison']
    assert out['change_30d_target_pct']==10 and out['change_30d_pct'] is None
    assert trace['target_date']=='2026-08-30' and trace['start']['date']=='2026-08-28'
    assert trace['elapsed_calendar_days']==32 and trace['baseline_delay_calendar_days']==2


def test_dollar_gap_nonpositive_basis_and_missing_closest_baseline_do_not_turn_into_zero():
    negative=fixture();negative['DTWEXBGS']=frame('DTWEXBGS',[('2026-09-29','-1'),('2026-08-30','100')])
    assert model.build(negative,TODAY)['dollar_strength']['level'] is None
    for rows in ([('2026-09-29','110'),('2026-08-21','100')],
                 [('2026-09-29','110'),('2026-08-30','0')],
                 [('2026-09-29','110'),('2026-08-30','.'),('2026-08-29','100')]):
        data=fixture();data['DTWEXBGS']=frame('DTWEXBGS',rows);out=model.build(data,TODAY)['dollar_strength']
        assert out['change_30d_target_pct'] is None and out['regime']=='UNAVAILABLE'


def test_reused_observation_age_policies_distinguish_weekly_batch_dollar_from_daily_rates():
    assert model.MAX_AGE['DTWEXBGS']==10 and model.MAX_AGE['SOFR']==model.MAX_AGE['IORB']==5
    data=fixture();data['DTWEXBGS']=frame('DTWEXBGS',[('2026-09-20','110'),('2026-08-21','100')])
    assert model.build(data,TODAY)['dollar_strength']['change_30d_pct']==10
    data['DTWEXBGS']=frame('DTWEXBGS',[('2026-09-18','110'),('2026-08-19','100')])
    assert model.build(data,TODAY)['dollar_strength']['level'] is None


def test_measured_zero_survives_and_does_not_become_an_economic_regime():
    data=fixture()
    for sid in ('SOFR','IORB','T10Y2Y','T5YIFR'):data[sid]=frame(sid,[('2026-09-29','0')])
    out=model.build(data,TODAY)
    assert out['repo_stress']['spread_bp']==0 and out['repo_stress']['regime']=='ZERO'
    assert out['curve_slope']['spread_bp']==0 and out['inflation_expectations']['rate_pct']==0
    assert out['inflation_expectations']['regime']=='DESCRIPTIVE'


def test_unit_conversion_and_overflow_are_checked():
    out=model.build(fixture(),TODAY);assert out['curve_slope']['spread_bp']==25
    assert out['curve_slope']['trace']['source_unit']=='percentage_points'
    data=fixture();data['SOFR']=frame('SOFR',[('2026-09-29','1e308')]);data['IORB']=frame('IORB',[('2026-09-29','-1e308')])
    data['T10Y2Y']=frame('T10Y2Y',[('2026-09-29','1e308')])
    data['DTWEXBGS']=frame('DTWEXBGS',[('2026-09-29','1e308'),('2026-08-30','1e-308')])
    out=model.build(data,TODAY);json.dumps(out,allow_nan=False)
    assert out['repo_stress']['spread_bp'] is out['curve_slope']['spread_bp'] is out['dollar_strength']['change_30d_target_pct'] is None


def test_whole_response_and_all_sixty_observations_remain_inspectable_without_input_mutation():
    data=fixture();data['DTWEXBGS']=frame('DTWEXBGS',[(str(TODAY-timedelta(days=n)),str(100+n)) for n in range(60)])
    data['DTWEXBGS']['response']['provider_metadata']={'count':6000,'limit':60,'realtime_start':'2026-09-29'}
    before=deepcopy(data);out=model.build(data,TODAY)
    assert out['source_frames']==before==data and len(out['source_observations']['DTWEXBGS']['observations'])==60
    out['source_frames']['DTWEXBGS']['response']['observations'].clear();assert data==before


def test_native_adapter_uses_exactly_the_same_five_series_limits_and_preserves_dates():
    m=engine();data=fixture();requests=[]
    class FixedDatetime(datetime):
        @classmethod
        def now(cls,tz=None):return cls(2026,9,29,7,tzinfo=timezone.utc)
    def response(url,**kwargs):
        q=parse_qs(urlsplit(url).query);sid=q['series_id'][0];requests.append((sid,int(q['limit'][0])))
        body=io.BytesIO(json.dumps(data[sid]['response']).encode());body.status=200;return body
    with patch.object(m,'datetime',FixedDatetime),patch.object(m.urllib.request,'urlopen',side_effect=response):out=m.compute_cross_signals()
    assert requests==list(model.LIMITS.items()) and out['repo_stress']['spread_bp']==5
    assert out['source_frames']['SOFR']['response']==data['SOFR']['response']
    assert out['source_frames']['SOFR']['http_acquisition_time_verified'] is False


def test_native_bad_transport_json_and_secrets_in_errors_cannot_escape_into_a_packet():
    m=engine()
    for raw,status in ((b'{',200),(b'{"observations":[],"observations":[]}',200),(b'[NaN]',200),
                       (b'{"x":1e999}',200),(b' '*(2*1024*1024+1),200),(b'{}',206)):
        response=io.BytesIO(raw);response.status=status
        with patch.object(m.urllib.request,'urlopen',return_value=response):out=m._fred_cross_source('SOFR',5)
        assert out['response'] is None and out['read_status']=='unavailable'
    with patch.object(m.urllib.request,'urlopen',side_effect=RuntimeError('api_key=DO_NOT_PRINT')):
        out=m._fred_cross_source('SOFR',5)
    assert 'DO_NOT_PRINT' not in json.dumps(out)
    with patch.object(m.urllib.request,'urlopen',side_effect=AssertionError('Unreviewed request')):
        try:m._fred_cross_source('OTHER',5)
        except ValueError:pass
        else:raise AssertionError('Unreviewed series requested')


def test_supply_heuristic_cannot_convert_unavailable_cross_context_to_calm():
    m=engine();values=json.loads((ROOT/'tests/fixtures/auction-concerns.json').read_bytes())['cases']['complete']['inputs']
    class FixedConcernDate(datetime):
        @classmethod
        def now(cls,tz=None):return datetime(2026,9,29,tzinfo=timezone.utc)
    m.datetime=FixedConcernDate
    good=model.build(fixture(),TODAY)
    out=m.compute_tail_risk(*values[:4],good)['p_supply_volatility_30d']
    assert out['heuristic_score'] is not None and out['probability'] is None and out['drivers']['repo_observation_date']=='2026-09-29'
    for change in ('missing_repo','missing_usd','legacy'):
        data=deepcopy(good)
        if change=='missing_repo':data['repo_stress']['measurement_status']='unavailable'
        elif change=='missing_usd':data['dollar_strength']['change_30d_target_pct']=None
        else:data.pop('measurement_contract')
        out=m.compute_tail_risk(*values[:4],data)['p_supply_volatility_30d']
        assert out['heuristic_score'] is None and out['status']=='unavailable'


if __name__=='__main__':
    tests=[fn for name,fn in list(globals().items()) if name.startswith('test_')]
    for test in tests:test()
    print('Auction cross-observation regressions passed:',len(tests))
