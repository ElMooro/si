"""Current reviewed return compiler and native code; synthetic inputs only."""
from pathlib import Path
from datetime import date, timedelta, datetime, timezone
from copy import deepcopy
from contextlib import redirect_stdout
from unittest.mock import patch
import io
import json
import math
import runpy
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/shared'))
import auction_reactions as model
support = runpy.run_path(str(ROOT/'tests/deployment/test_auction_output_ownership.py'))


def series(n=30):
    return {'dates': [(date(2026, 8, 20)+timedelta(days=i)).isoformat() for i in range(n)],
            'closes': [100+i for i in range(n)]}


def test_exact_unique_endpoint_returns_and_full_formula_inputs():
    frame = model.PriceFrame(series(), '2026-09-29', 'SPY')
    out = frame.returns('2026-08-22')
    assert out['same_day'] == float(model.Fraction(102, 101)-1)
    assert out['d1'] == float(model.Fraction(103, 102)-1)
    assert out['d5'] == float(model.Fraction(107, 102)-1)
    assert out['d20'] == float(model.Fraction(122, 102)-1)
    for key, n in model.HORIZONS:
        trace = out['calculation_inputs']['horizons'][key]
        assert trace['status'] == 'complete' and trace['value'] == out[key]
        assert trace['start_date'] < trace['end_date']


def test_duplicate_or_nonascending_dates_withhold_without_deduplicating():
    for dates in (['2026-09-21','2026-09-22','2026-09-22','2026-09-23'],
                  ['2026-09-21','2026-09-23','2026-09-22','2026-09-24']):
        source = {'dates': dates, 'closes': [100,101,202,203]}
        frame = model.PriceFrame(source, '2026-09-29')
        assert not frame.valid and frame.document['input'] == source
        assert all(frame.returns('2026-09-22')[key] is None for key, _ in model.HORIZONS)


def test_mismatched_arrays_and_invalid_prices_cannot_crash_or_produce_returns():
    for closes in ([100], [100,101,102], [100,True], [100,None], [100,0], [100,-1],
                   [100,float('nan')], [100,float('inf')], [100,'101']):
        frame = model.PriceFrame({'dates':['2026-09-21','2026-09-22'],'closes':closes}, '2026-09-29')
        result = frame.returns('2026-09-22')
        assert not frame.valid and all(result[h] is None for h, _ in model.HORIZONS)
        json.dumps(frame.document, allow_nan=False)
    repeated={'x':float('nan')};safe=model.safe([repeated,repeated]);assert safe==[{'x':{'invalid_numeric':'nan'}},{'x':{'invalid_numeric':'nan'}}]
    cycle=[];cycle.append(cycle);assert model.safe(cycle)==[{'invalid_cycle':True}]


def test_first_observation_only_withholds_same_day_not_valid_forward_endpoints():
    out = model.PriceFrame(series(), '2026-09-29').returns('2026-08-20')
    assert out['same_day'] is None and out['d1'] == .01 and out['d5'] == .05 and out['d20'] == .2
    assert out['calculation_inputs']['horizons']['same_day']['reason'] == 'prior_observation_absent'


def test_missing_event_invalid_dates_future_and_partial_are_distinct():
    source = {'dates':['2026-09-25','2026-09-28','2026-09-29'],'closes':[100,101,102]}
    frame = model.PriceFrame(source, '2026-09-29')
    out = frame.returns('2026-09-28')
    assert out['same_day'] == .01 and out['d1'] is not None and out['partial'] == ['d1']
    assert frame.returns('2026-09-27')['calculation_inputs']['horizons']['same_day']['reason'] == 'event_date_absent'
    for invalid in ('2026-02-30','2026-9-28',None,True):
        assert frame.returns(invalid)['d1'] is None
    for dates in (['2026-09-25','2026-09-30'], ['2026-09-25','2026-02-30']):
        assert not model.PriceFrame({'dates':dates,'closes':[100,101]}, '2026-09-29').valid


def test_complete_actual_bar_population_and_raw_consumed_values_survive_normalization():
    bars = [{'time':'1994-01-01','close':100}, {'time':'2026-09-22','close':None},
            {'time':'2026-09-22','close':101}, {'time':'bad-date','close':102}, None]
    out = model.retain_bars(bars)
    assert len(out['dates']) == len(out['closes']) == len(out['input_observations']) == 5
    assert out['dates'][0] == '1994-01-01' and out['dates'][3] is None
    assert out['input_observations'][3]['time'] == 'bad-date' and out['input_observations'][4]['row_type'] == 'NoneType'
    assert model.bar_day(True) is None
    assert model.bar_day('2026-09-22garbage') is None and model.bar_day('2026-09-22T13:00:00') is None
    assert model.bar_day('2026-09-22T23:00:00-04:00') == '2026-09-23'


def compiled():
    events = [{'date':d,'classes':['bills_only']} for d in ('2026-09-25','2026-09-26','2026-09-27','2026-09-28','2026-09-29')]
    source = {'dates':['2026-09-24','2026-09-25','2026-09-26','2026-09-27','2026-09-28','2026-09-29'], 'closes':[100,101,102,103,104,105]}
    return model.compile_reactions(events, {'SPY':source}, '2026-09-29', '2026-09-29', {'bills_only':'bills only'})


def test_every_event_retained_and_partial_endpoints_excluded_with_coverage():
    result = compiled();inputs = result['comparison_inputs']
    assert len(inputs['events']) == 5 and result['n_events']['bills_only'] == 4
    assert result['baseline']['SPY']['same_day']['n'] == 4
    assert result['baseline']['SPY']['d1']['n'] == 3
    assert inputs['coverage']['SPY']['d1'] == {'candidate_events':4,'complete':3,'partial':1,'unavailable':0}
    assert inputs['coverage']['SPY']['d20']['unavailable'] == 4
    assert model.replay(inputs) == result


def test_full_replay_rejects_changed_input_identity_coverage_and_permissions():
    inputs = compiled()['comparison_inputs']
    for mutate in (lambda d:d['frames']['SPY']['input']['closes'].__setitem__(1,999),
                   lambda d:d['frames']['SPY'].update(frame_id='0'*64),
                   lambda d:d['coverage']['SPY']['d1'].update(complete=99),
                   lambda d:d.update(sizing_eligible=True), lambda d:d['horizons'].append(['extra',2])):
        bad=deepcopy(inputs);mutate(bad)
        try:model.replay(bad)
        except ValueError:pass
        else:raise AssertionError('Corrupt replay accepted')


def test_zero_returns_and_extreme_numbers_remain_finite_or_unavailable():
    source={'dates':['2026-09-21','2026-09-22'],'closes':[100,100]}
    assert model.PriceFrame(source,'2026-09-29').returns('2026-09-22')['same_day'] == 0
    out=model.PriceFrame({**source,'closes':[1e-308,1e308]},'2026-09-29').returns('2026-09-22')
    assert out['same_day'] is None and out['calculation_inputs']['horizons']['same_day']['reason']=='nonfinite_return'
    assert model.PriceFrame({**source,'closes':[1e-308,1e308]},'2026-09-22').returns('2026-09-22')['partial']==[]
    assert model.distribution([0,0,0]) == {'n':3,'median':0.0,'mean':0.0,'hit':0,'excluded_nonfinite_or_invalid':0,'unit':'percent_return'}
    assert model.distribution([.01,.02,float('nan'),True])['n'] == 2
    assert model.distribution([1e308,1e308,1e308])['mean'] is None


def test_actual_native_wrapper_and_refresh_keep_bad_rows_without_new_requests():
    store=support['Store']();scope=support['load']('auction-desk',store);env=scope['load_assets'].__globals__
    calls=[];bars=[{'time':'2026-09-21','close':100},{'time':'2026-09-22','close':None},{'time':'2026-09-22','close':102}]
    env.update(ASSETS=[('SPY','Stocks','equity')],_get_json=lambda url,**kw:(calls.append(url) or {'bars':bars,'symbol':'SPY','count':3,'source':'fixture'}))
    with patch('urllib.request.urlopen',side_effect=AssertionError('No network')):
        doc=scope['load_assets']()
    assert len(calls)==1 and len(doc['series']['SPY']['dates'])==3
    assert doc['series']['SPY']['closes'][1] is None
    assert scope['fwd_returns'](doc['series']['SPY'],'2026-09-22')['d1'] is None
    assert store.writes == [scope['ASSETS_KEY']]
    assert scope['Date_parse']('2099-01-01T00:00:00').tzinfo is not None
    acquired=doc['series']['SPY']['acquired_at']
    env['_get_json']=lambda *a,**kw:{'bars':[],'symbol':'SPY','count':0}
    with patch('urllib.request.urlopen',side_effect=AssertionError('No network')):retained=scope['load_assets'](force=True)
    assert retained['series']['SPY']['acquired_at']==acquired
    assert retained['series']['SPY']['latest_acquisition_attempt']['status']=='empty_or_invalid_response'
    assert retained['series']['SPY']['input_observations']==doc['series']['SPY']['input_observations']


def test_response_identity_and_count_bind_newly_captured_proxy_populations():
    prices=series()
    source={**prices, 'reaction_input_contract':model.CONTRACT,'requested_symbol':'SPY','response_symbol':'SPY','response_count':30,
            'input_observations':[{'ordinal':i,'time':d,'close':c,'row_type':'dict'} for i,(d,c) in enumerate(zip(prices['dates'],prices['closes']))]}
    assert model.PriceFrame(source,'2026-09-29','SPY').valid
    for updates in ({'response_symbol':'QQQ'},{'requested_symbol':'QQQ'},{'response_count':True},{'response_count':29},{'response_count':None}):
        frame=model.PriceFrame({**source,**updates},'2026-09-29','SPY')
        assert not frame.valid and frame.returns('2026-08-22')['same_day'] is None
    for mutate in (lambda d:d['input_observations'][0].update(close=999),lambda d:d['input_observations'][0].update(ordinal=True),
                   lambda d:d['input_observations'].pop(),lambda d:d.update(input_observations=None)):
        bad=deepcopy(source);mutate(bad);assert not model.PriceFrame(bad,'2026-09-29','SPY').valid
    for events in ([{'date':'bad','classes':['x']}],[{'date':'2026-09-22','classes':[['x']]}],
                   [{'date':'2026-09-22','classes':['x']},{'date':'2026-09-22','classes':['x']}]):
        try:model.compile_reactions(events,{},'2026-09-29','2026-09-29',{'x':'test'})
        except ValueError:pass
        else:raise AssertionError('Malformed event population accepted')


def test_actual_handler_publishes_whole_replay_ledger_without_price_authority():
    store=support['Store']();scope=support['load']('auction-desk',store);env=scope['lambda_handler'].__globals__
    raw=support['auction']();env.update(fetch_fd=lambda endpoint,*a,**kw:[raw] if endpoint=='auctions_query' else [],
        fetch_td=lambda *a,**kw:[],load_assets=lambda **kw:{'series':{'SPY':series()}},load_full_bank=lambda **kw:{'rows':{}},fetch_fred_daily=lambda *a:{})
    with patch('urllib.request.urlopen',side_effect=AssertionError('No network')),redirect_stdout(io.StringIO()):scope['lambda_handler']({},None)
    packet=store.docs['data/auction-desk.json']['reactions']
    assert packet['return_input_contract']==model.CONTRACT and packet['supplied_input_replay_available']
    assert not packet['price_lineage_verified'] and not packet['original_response_replay_available']
    replayed=model.replay(packet['comparison_inputs'])
    for key in ('stats','baseline','n_events'):assert replayed[key]==packet[key]
    assert all(packet['comparison_inputs'][key] is False for key in model.FLAGS)


def test_offline_cli_replays_saved_packet_and_rejects_mismatched_reported_stats():
    with tempfile.TemporaryDirectory() as directory:
        path=Path(directory)/'saved.json';packet={'reactions':compiled()};path.write_text(json.dumps(packet),encoding='utf-8')
        command=[sys.executable,str(ROOT/'scripts/replay_auction_reactions.py'),str(path)]
        result=subprocess.run(command,capture_output=True,text=True);assert result.returncode==0,result.stderr
        assert json.loads(result.stdout)==packet['reactions']
        packet['reactions']['baseline']['SPY']['d1']['n']=999;path.write_text(json.dumps(packet),encoding='utf-8')
        result=subprocess.run(command,capture_output=True,text=True);assert result.returncode==1 and 'does not reproduce' in result.stderr


if __name__=='__main__':
    tests=[value for name,value in list(globals().items()) if name.startswith('test_')]
    for test in tests:test()
    print('Auction reaction input regressions passed:',len(tests))
