"""Buyback source-field, population and chronology regressions; no external I/O."""
from pathlib import Path
from copy import deepcopy
import json
import sys
import runpy
import subprocess
import tempfile
import io
from contextlib import redirect_stdout
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
import auction_buybacks as model


def raw(day='2026-09-28',**changes):
    return {'operation_date':day,'settlement_date':'2026-09-30','operation_type':'Liquidity support',
            'security_type':'Nominal coupon','maturity_bucket':'7-10 years','max_par_amt':'1000',
            'par_amt_offered':'2000','par_amt_accepted':'900','nbr_issues_eligible':'10','nbr_issues_accepted':'5',**changes}


def analyzed(rows,as_of='2026-09-29'):
    normalized=[model.normalize(row) for row in rows]
    return [model.analyze(row,normalized,as_of) for row in normalized]


def test_zero_par_does_not_fall_through_to_issue_count_or_other_units():
    op=model.normalize(raw(par_amt_accepted='0',nbr_issues_accepted='5',accepted_price='99',accepted_percentage='50'))
    assert op['accepted']==0 and op['n_issues_accepted']==5
    candidates=op['normalization_inputs']['fields']['accepted']['candidates']
    assert len(candidates)==1 and candidates[0]['field']=='par_amt_accepted'
    assert candidates[0]['raw']=='0'


def test_count_only_fields_cannot_supply_missing_par_amount():
    source=raw();source.pop('par_amt_accepted')
    out=model.normalize(source)
    assert out['accepted'] is None and out['n_issues_accepted']==5
    assert out['normalization_inputs']['fields']['accepted']['status']=='missing_field'


def test_conflicting_or_invalid_aliases_never_silently_win_by_field_order():
    for changes in ({'total_par_amt_accepted':'800'}, {'total_par_amt_accepted':None}, {'total_par_amt_accepted':True}):
        source=raw(**changes)
        for ordered in (source,dict(reversed(list(source.items())))):
            assert model.normalize(ordered)['accepted'] is None
    assert model.normalize(raw(total_par_amt_accepted='900.0'))['accepted']==900


def test_number_parser_rejects_booleans_nonfinite_negative_and_bad_commas():
    for value in (True,False,None,'nan','Infinity',float('nan'),float('inf'),-1,'1,2','$100','1e999','1e-999'):
        assert model.numeric(value) is None,value
    assert model.numeric('1,234.5')==1234.5 and model.numeric('0')==0
    assert model.numeric('2.5',True) is None and model.numeric('2.0',True)==2
    assert model.numeric(str(2**53),True) is None


def test_all_source_fields_and_invalid_dates_remain_inspectable():
    source=raw(operation_date='2026-09-28garbage',unrecognized_101='retained',par_amt_accepted=float('nan'))
    out=model.normalize(source)
    assert out['operation_date'] is None and set(out['raw_fields'])==set(source)
    assert out['normalization_inputs']['source_fields']['unrecognized_101']=='retained'
    assert out['normalization_inputs']['source_fields']['par_amt_accepted']=={'invalid_numeric':'nan'}
    json.dumps(out,allow_nan=False)


def test_operation_identity_keeps_different_purposes_and_time_slots_separate():
    rows=[raw(),raw(operation_type='Cash management'),raw(operation_start_time_est='13:00'),raw(security_type='TIPS')]
    assert len({model.normalize(row)['operation_identity'] for row in rows})==4


def test_valid_zero_fill_remains_zero_and_no_offered_per_zero_division():
    out=analyzed([raw(par_amt_accepted='0')])[0]
    assert out['fill_pct']==0 and out['coverage'] is None and out['liquidity_signal']=='light'
    assert out['measurement_inputs']['status']=='complete' and 'MAX FILL' not in out['tags']


def test_incomplete_impossible_future_and_unverified_legacy_measurements_withhold_tags():
    cases=[raw(par_amt_accepted=None),raw(par_amt_accepted='1001'),raw(par_amt_offered='899'),
           raw(nbr_issues_accepted='11'),raw('2026-10-01')]
    for op in analyzed(cases):
        assert op['measurement_inputs']['status']=='unavailable'
        assert op['fill_pct'] is None and op['liquidity_signal']=='unavailable'
        assert 'MAX FILL' not in op['tags'] and op['call'] is None
    legacy={'operation_date':'2026-09-28','accepted':900,'max_par':1000}
    assert model.analyze(legacy,[legacy],'2026-09-29')['measurement_inputs']['status']=='unavailable'


def test_max_fill_requires_exact_equality_and_large_fill_is_not_easing():
    below=analyzed([raw(max_par_amt='10000',par_amt_offered='20000',par_amt_accepted='9999')])[0]
    assert below['fill_pct']==100.0 and 'MAX FILL' not in below['tags']
    large=analyzed([raw(max_par_amt='10000000000',par_amt_offered='20000000000',par_amt_accepted='9000000000')])[0]
    assert large['liquidity_signal']=='strong' and large['cash_settlement_usd'] is None
    assert not large['monetary_easing_inferred'] and large['call'] is None


def test_size_rank_uses_whole_strict_prior_comparable_sample_with_midrank_ties():
    rows=[raw('2026-09-23',par_amt_accepted='0'),raw('2026-09-24',par_amt_accepted='500'),
          raw('2026-09-25',par_amt_accepted='1000'),raw('2026-09-26',par_amt_accepted='500'),
          raw('2026-09-27',par_amt_accepted='900'),raw('2026-09-24',operation_type='Other',par_amt_accepted='10')]
    out=analyzed(rows)[3];trace=out['measurement_inputs']['size_comparison']
    assert out['size_pctile']==50 and trace['n']==3
    assert all(row['operation_date']<'2026-09-26' for row in trace['prior'])
    bad=deepcopy(rows);bad[1]['par_amt_accepted']=None
    assert analyzed(bad)[3]['size_pctile'] is None
    impossible=deepcopy(rows);impossible[1]['par_amt_accepted']='5000'
    assert analyzed(impossible)[3]['size_pctile'] is None
    duplicated=rows[:3]+[rows[0],rows[3]]
    assert analyzed(duplicated)[-1]['size_pctile'] is None


def test_aggregate_keeps_missing_rows_without_faking_zero_or_full_totals():
    ops=analyzed([raw('2026-09-25',par_amt_accepted='0'),raw('2026-09-26',par_amt_accepted=None)])
    result=model.program_summary(ops,'2026-09-29','2025-10-01','2026-09-01')
    assert result['n_ops']==2 and result['total_accepted'] is None and result['avg_fill_pct'] is None
    window=result['aggregate_inputs']['windows']['program']
    assert window['known_subtotal_usd_par']==0 and len(window['inputs'])==2
    assert model.program_summary([],'2026-09-29','2025-10-01','2026-09-01')['total_accepted'] is None


def test_complete_zero_totals_survive_and_future_or_undated_rows_are_separate():
    rows=[raw('2026-09-25',par_amt_accepted='0'),raw('2026-09-26',par_amt_accepted='0'),raw('2026-10-01')]
    out=model.program_summary(analyzed(rows),'2026-09-29','2025-10-01','2026-09-01')
    assert out['total_accepted']==out['avg_fill_pct']==0 and out['n_ops']==2
    assert out['aggregate_inputs']['future_operations']==1
    rows.append(raw(operation_date='bad'))
    out=model.program_summary(analyzed(rows),'2026-09-29','2025-10-01','2026-09-01')
    assert out['total_accepted'] is None and out['aggregate_inputs']['unknown_date_operations']==1
    duplicated=analyzed([raw(),raw()]);out=model.program_summary(duplicated,'2026-09-29','2025-10-01','2026-09-01')
    assert out['total_accepted'] is None and out['aggregate_inputs']['windows']['program']['duplicate_identity_count']==1


def test_measurements_and_aggregates_never_grant_forecast_or_sizing_permission():
    rows=analyzed([raw()]);documents=[rows[0]['measurement_inputs'],rows[0]['normalization_inputs'],
                                    model.program_summary(rows,'2026-09-29','2025-10-01','2026-09-01')['aggregate_inputs']]
    for document in documents:assert all(document[key] is False for key in model.FLAGS)


def test_bank_refresh_preserves_legacy_evidence_unknown_dates_and_all_duplicate_occurrences():
    legacy={key:value for key,value in model.normalize(raw()).items() if key not in ('normalization_inputs','operation_identity')}
    legacy['accepted']=5
    unrefreshed={**legacy,'operation_date':'2026-09-10'}
    updated,retired=model.merge_operations({'legacy':legacy,'older':unrefreshed},[raw(par_amt_accepted='0'),raw(operation_date='bad')])
    assert len(updated)==3 and len(retired)==1 and list(retired.values())[0]['accepted']==5
    assert updated['older']==unrefreshed and any(row['operation_date'] is None for row in updated.values())
    next_rows,next_retired=model.merge_operations(updated,[raw(par_amt_accepted='0'),raw(operation_date='bad')],retired)
    assert next_rows==updated and next_retired==retired
    duplicate,_=model.merge_operations({},[raw(),raw()])
    assert len(duplicate)==2
    for row in duplicate.values():assert not model.measurement_complete(model.analyze(row,list(duplicate.values()),'2026-09-29'))


def test_offline_replay_rejects_tampered_fields_totals_dates_and_permissions():
    rows=analyzed([raw('2026-09-25',par_amt_accepted='0'),raw('2026-09-26')])
    packet={'operations':rows,'program':model.program_summary(rows,'2026-09-29','2025-10-01','2026-09-01')}
    assert model.replay(packet)['status']=='identical_supplied_arithmetic'
    for mutate in (lambda p:p['operations'][0].update(accepted=5),
                   lambda p:p['program'].update(total_accepted=999),
                   lambda p:p['operations'][0]['measurement_inputs'].update(sizing_eligible=True),
                   lambda p:p['operations'][0]['measurement_inputs']['input']['normalization_inputs']['source_fields'].update(par_amt_accepted='12')):
        bad=deepcopy(packet);mutate(bad)
        try:model.replay(bad)
        except ValueError:pass
        else:raise AssertionError('Tampered buyback replay accepted')
    with tempfile.TemporaryDirectory() as folder:
        path=Path(folder)/'saved.json';path.write_text(json.dumps(packet),encoding='utf-8')
        command=[sys.executable,str(ROOT/'scripts/replay_auction_buybacks.py'),str(path)]
        result=subprocess.run(command,capture_output=True,text=True)
        assert result.returncode==0,result.stderr
        packet['program']['aggregate_inputs']['calls_eligible']=True
        path.write_text(json.dumps(packet),encoding='utf-8')
        assert subprocess.run(command,capture_output=True).returncode==1


def native_packet(rows, legacy=None):
    support=runpy.run_path(str(ROOT/'tests/deployment/test_auction_output_ownership.py'))
    store=support['Store']();scope=support['load']('auction-desk',store);env=scope['lambda_handler'].__globals__
    if legacy is not None:store.docs[scope['BUY_KEY']]={'operations':legacy}
    env.update(fetch_fd=lambda endpoint,*a,**k:rows if endpoint=='buybacks_operations' else [],fetch_td=lambda *a,**k:[],
               load_assets=lambda **k:{'series':{}},load_full_bank=lambda **k:{'rows':{}},fetch_fred_daily=lambda *a:{})
    from datetime import datetime,timezone
    env['_now']=lambda:datetime(2026,9,29,12,0,tzinfo=timezone.utc)
    with patch('urllib.request.urlopen',side_effect=AssertionError('No network')),redirect_stdout(io.StringIO()):scope['lambda_handler']({},None)
    assert 'data/auction-crisis.json' not in store.writes
    return store.docs[scope['OUT_KEY']],store.docs[scope['BUY_KEY']]


def test_actual_handler_preserves_missing_future_zero_and_undated_rows_without_current_day_leak():
    source=[raw('2026-09-25',par_amt_accepted='0'),raw('2026-09-26',par_amt_accepted=None),raw('2026-10-02'),raw(operation_date='bad')]
    out,bank=native_packet(source)
    assert len(out['buybacks']['operations'])==len(bank['operations'])==4
    assert out['today']['date']=='2026-09-26' and out['freshness']['newest_buyback']=='2026-09-26'
    assert out['buybacks']['program']['total_accepted'] is None
    assert out['today']['verdict']['buyback_accepted'] is None
    assert out['calendar']['buybacks'][0]['operation_date']=='2026-10-02'
    assert model.replay(out)['status']=='identical_supplied_arithmetic'
    assert out['reactions']['classification_inputs']['2026-09-26']['cohort_classes']==['unclassified']
    assert out['reactions']['scoreboard'][0]['buyback_accepted'] is None
    assert out['reactions']['scoreboard'][1]['buyback_accepted']==0
    json.dumps(out,allow_nan=False)


def test_original_source_mapping_rejects_tampered_trace_and_duplicate_average():
    row=analyzed([raw()])[0];row['normalization_inputs']['fields']['accepted']['candidates'][0]['value']=5
    assert not model.source_status(row,'accepted') and not model.measurement_complete(row)
    rows=analyzed([raw(),raw()]);summary=model.program_summary(rows,'2026-09-29','2025-10-01','2026-09-01')
    assert summary['avg_fill_pct'] is None


def test_fill_classification_is_exact_descriptive_par_arithmetic_not_cash():
    for accepted, maximum, expected in [('4499999999','5000000000','accepted_par_at_least_2bn'),
                                      ('4500000000','5000000000','large_high_fill'),
                                      ('1999999999','5000000000','other_valid_operation'),
                                      ('2000000000','5000000000','accepted_par_at_least_2bn'),
                                      ('0','0','other_valid_operation')]:
        row=analyzed([raw(max_par_amt=maximum,par_amt_accepted=accepted,par_amt_offered='6000000000',settlement_date=None)])[0]
        trace=row['fill_classification'];cash=row['cash_effect']
        assert trace['contract']==model.FILL_CONTRACT and trace['value']==expected
        assert trace['accepted_usd_par']==float(accepted) and trace['maximum_usd_par']==float(maximum)
        assert all(trace[key] is False and cash[key] is False for key in model.FLAGS)
        assert cash['status']=='unmeasured' and cash['reported_settlement_date'] is None
        assert cash['cash_settlement_usd'] is cash['net_reserve_change_usd'] is cash['duration_removed_dv01_usd'] is None
        assert row['liquidity_signal_semantics']['deprecated'] is True
        assert not any('TGA' in tag or 'EASING' in tag for tag in row['tags'])


def test_actual_handler_keeps_high_fill_separate_from_cash_and_day_verdict():
    packet,_=native_packet([raw(max_par_amt='5000000000',par_amt_accepted='4500000000',par_amt_offered='6000000000',settlement_date=None)])
    assert packet['buybacks']['operations'][0]['fill_classification']['value']=='large_high_fill'
    assert packet['today']['verdict']['tags']==['BUYBACK','LARGE HIGH-FILL BUYBACK']
    assert packet['today']['verdict']['risk_assets']=='neutral'
    assert model.replay(packet)['legacy_classification_rows']==0
    assert packet['decision']['sizing_eligible'] is False and packet['decision']['call'] is None


def test_legacy_replay_preserves_arithmetic_without_qualifying_old_cash_labels():
    packet=json.loads((ROOT/'tests/fixtures/auction-buyback-pre-fill-classification.json').read_bytes())
    result=model.replay(packet)
    assert result['legacy_classification_rows']==1 and result['legacy_cash_labels_qualified'] is False
    for mutate in (lambda p:p['buybacks']['operations'][0].update(fill_classification={}),
                   lambda p:p['buybacks']['operations'][0]['tags'].append('EASING'),
                   lambda p:p['buybacks']['operations'][0].update(liquidity_signal='light')):
        bad=deepcopy(packet);mutate(bad)
        try:model.replay(bad)
        except ValueError:pass
        else:raise AssertionError('Altered legacy semantics accepted')


def test_current_classification_and_cash_metadata_are_replay_bound():
    ops=analyzed([raw()]);packet={'operations':ops,'program':model.program_summary(ops,'2026-09-29','2025-10-01','2026-09-01')}
    for mutate in (lambda p:p['operations'][0]['fill_classification'].update(value='large_high_fill'),
                   lambda p:p['operations'][0]['cash_effect'].update(cash_settlement_usd=900),
                   lambda p:p['operations'][0].pop('cash_effect'),
                   lambda p:p['operations'][0]['liquidity_signal_semantics'].update(deprecated=False)):
        bad=deepcopy(packet);mutate(bad)
        try:model.replay(bad)
        except ValueError:pass
        else:raise AssertionError('Altered fill or cash metadata accepted')


if __name__=='__main__':
    tests=[fn for name,fn in list(globals().items()) if name.startswith('test_')]
    for test in tests:test()
    print('Auction buyback input regressions passed:',len(tests))
