"""Captured public Radar at 2026-09-30 ~19:06 UTC; its economic end is Sep 28.
Tests freeze time before expiry. No private originals, cloud or vendor requests.
"""
from datetime import datetime, timezone, timedelta
import copy
import gzip
import json
from pathlib import Path
import sys
import types
import importlib.util

SOURCE = Path(__file__).parents[1] / 'source'
sys.path.insert(0, str(SOURCE))
from provider_flow_evidence import project
NOW = datetime(2026, 9, 30, 19, 15, tzinfo=timezone.utc)
FIXTURE = Path(__file__).parent / 'fixtures/capital-flow-radar-20260929.json.gz'


def packet(): return json.loads(gzip.decompress(FIXTURE.read_bytes()))
def row(result, ticker='XLU'): return next(r for r in result['funds'] if r['ticker'] == ticker)
def unavailable(result, ticker='XLU'): return all(w['flow_usd_decimal'] is None for w in row(result, ticker)['windows'].values())


def test_captured_real_observations_visible_without_mutating_packet():
    p = packet(); before = copy.deepcopy(p); out = project(p, NOW)
    assert p == before and out['status'] == 'partial' and out['available_funds'] == 289
    assert len(out['funds']) == 300 and len(out['baskets']) == 46
    assert row(out)['windows']['1']['flow_usd_decimal'] == '-21738274.8'
    assert row(out,'SMH')['windows']['5']['flow_usd_decimal'] == '867505866.5'
    assert unavailable(out,'NVDL') and unavailable(out,'ARKB')
    assert row(out,'NVDL')['windows']['5']['reasons'] == ['source_expired']
    assert len(json.dumps(out,separators=(',',':')).encode()) < 350_000
    assert out['independent_investment_votes'] == 0 and out['informational_only']


def test_old_missing_and_bad_native_contract_are_unavailable():
    for p in [None, {}, {'complexes': []}, [], {'contract':'capital-flow-native-research.v1'}]:
        out=project(p,NOW);assert out['status']=='unavailable' and out['funds']==[]
    for field,value in [('calls_eligible',True),('portfolio_action','BUY')]:
        p=packet();p[field]=value;assert project(p,NOW)['status']=='unavailable'


def test_packet_dates_cannot_refresh_source_or_travel_in_time():
    p=packet();assert project(p,datetime(2026,10,1,tzinfo=timezone.utc))['reasons']==['packet_source_expired']
    for field,value in [('generated_at','2026-10-01T00:00:00Z'),('source_generated_at','2026-10-01T00:00:00Z'),('source_valid_until','2027-01-01T00:00:00Z'),('generated_at','2026-09-29T22:30:05')]:
        p=packet();p[field]=value;assert project(p,NOW)['status']=='unavailable'


def test_per_fund_dates_identity_permissions_and_missingness():
    mutations=[('ticker','SPY'),('unit','EUR'),('measure','turnover'),('source_acquired_at','2026-10-01T00:00:00Z'),('source_valid_until','2026-09-30T18:00:00Z'),('latest_effective_date','2026-10-01'),('latest_processed_date','2026-10-01'),('calls_eligible',True)]
    for field,value in mutations:
        p=packet();p['funds']['XLU'][field]=value;assert unavailable(project(p,NOW)),field
    p=packet();p['funds']['XLU']['latest_observation']['effective_date']='2026-09-27';assert unavailable(project(p,NOW))
    p=packet();p['funds']['XLU']['history']['sha256']='0'*64;assert unavailable(project(p,NOW))


def test_null_zero_and_finite_decimal_strings():
    for bad in [None,0,False,'','NaN','Infinity','1e6','1e999','9'*25,{},[]]:
        p=packet();p['funds']['XLU']['aligned_windows']['1']['flow_usd_decimal']=bad
        assert row(project(p,NOW))['windows']['1']['flow_usd_decimal'] is None,repr(bad)
    p=packet();p['funds']['XLU']['aligned_windows']['1']['flow_usd_decimal']='0'
    p['funds']['XLU']['latest_observation']['reported_flow_usd_decimal']='0.00'
    assert row(project(p,NOW))['windows']['1']['flow_usd_decimal']=='0'


def test_conflicting_windows_and_reference_grid_rejected():
    mutations=[('end_date','2026-09-27'),('dates',['2026-09-27']),('available_observations',True),('available_observations',0),('missing_dates',['2026-09-28']),('reasons',['conflicting_version'])]
    for field,value in mutations:
        p=packet();p['funds']['XLU']['aligned_windows']['1'][field]=value
        assert row(project(p,NOW))['windows']['1']['flow_usd_decimal'] is None,field
    p=packet();p['funds']['XLU']['aligned_windows']['21']['dates'][0]='2026-08-27'
    assert row(project(p,NOW))['windows']['21']['flow_usd_decimal'] is None


def test_partial_baskets_and_leveraged_funds_stay_separate():
    p=packet();out=project(p,NOW)
    tech=next(g for g in out['baskets'] if g['name']=='Technology')['windows']['21']
    assert tech['status']=='partial' and tech['flow_usd_decimal'] is None and 'FTEC' in tech['excluded']
    p['complexes'][0]['windows']['5']['flow_usd_decimal']='0'
    g=project(p,NOW)['baskets'][0]['windows']['5'];assert g['status']=='unavailable' and g['observed_subset_flow_usd_decimal'] is None
    p=packet();p['complexes'][0]['windows']['5']['required']=['SOXL','SOXS']
    assert project(p,NOW)['baskets'][0]['windows']['5']['reasons']==['leveraged_inverse_separate']
    assert row(out,'SOXL')['category']=='leveraged' and row(out,'SOXS')['category']=='leveraged'
    assert 'configured_stock_context' not in json.dumps(out)


def test_bounded_inventory_and_malformed_containers_fail_closed():
    for value in [None,[],{'bad ticker':{}}]:
        p=packet();p['funds']=value;assert project(p,NOW)['status']=='unavailable'
    p=packet();p['funds']['XLU']['aligned_windows']=[]
    assert project(p,NOW)['status'] in ('partial','unavailable')


def test_full_output_decision_parity_against_preintegration_handler():
    # Execute baseline handler source with only the two additive integration lines removed.
    # Risk/scoring/discovery modules and all inputs remain identical. This catches any
    # additional edit to the handler via the retained source digest assertion below.
    import lambda_function as candidate
    text=(SOURCE/'lambda_function.py').read_text(encoding='utf-8')
    baseline_text=text.replace('from provider_flow_evidence import project as project_provider_flow_evidence\n','').replace('    output["provider_flow_research"] = project_provider_flow_evidence(None if (metas.get("capital_flow") or {}).get("error") else feeds.get("capital_flow"), now)\n','')
    import hashlib
    assert hashlib.sha256(baseline_text.encode()).hexdigest() == 'e399a30dcfbb4526317d88836539e15a2b0a574d5d1d4f4c14ea3cf8a71adc04'
    module=types.ModuleType('pre_provider_evidence');module.__file__=str(SOURCE/'lambda_function.py')
    exec(compile(baseline_text,module.__file__,'exec'),module.__dict__)
    from test_scoring import valid_khalid_risk_artifact, base_row
    for radar in [packet(),{}, {'complexes':[{'primary':'SPY','flow_zscore_90d':2}]}]:
        for risk in [valid_khalid_risk_artifact(NOW.isoformat()),{}]:
            feeds={'capital_flow':radar,'khalid_risk':risk,'fortress':{'board':[base_row()],'etfs':[],'ledger':[]}}
            metas={k:{'last_modified':NOW.isoformat(),'error':None} for k in feeds}
            before=copy.deepcopy(feeds);a=module.build_output(feeds,metas,NOW,[]);b=candidate.build_output(feeds,metas,NOW,[])
            del b['provider_flow_research'];assert a==b and feeds==before


def test_loader_error_with_retained_payload_is_unavailable():
    import lambda_function as candidate
    output=candidate.build_output({'capital_flow':packet()},{'capital_flow':{'error':'read unavailable'}},NOW,[])
    assert output['provider_flow_research']['status']=='unavailable'


def single_member(p, ticker='XLU', n='5'):
    w=p['complexes'][0]['windows'][n]
    w.update(required=[ticker], included=[ticker], required_count=1, included_count=1,
             excluded={}, status='complete_matched_group',
             flow_usd_decimal=p['funds'][ticker]['aligned_windows'][n]['flow_usd_decimal'])
    return w


def test_latest_observation_reconciles_same_date_decimal_value():
    p=packet();p['funds']['XLU']['aligned_windows']['1']['flow_usd_decimal']='1000'
    w=row(project(p,NOW))['windows']['1']
    assert w['status']=='unavailable' and w['reasons']==['conflicting_latest_observation']
    for value in ['-21738274.8000', '-021738274.8']:
        p=packet();p['funds']['XLU']['latest_observation']['reported_flow_usd_decimal']=value
        assert row(project(p,NOW))['windows']['1']['status']=='available'
    for value in [None, 'NaN', 0]:
        p=packet();p['funds']['XLU']['latest_observation']['reported_flow_usd_decimal']=value
        assert row(project(p,NOW))['windows']['1']['status']=='unavailable'


def test_cross_ticker_history_identity_conflicts_but_basket_overlap_is_valid():
    p=packet();p['funds']['XLU']['history']=copy.deepcopy(p['funds']['SPY']['history'])
    out=project(p,NOW)
    for ticker in ['XLU','SPY']:
        assert unavailable(out,ticker)
        assert row(out,ticker)['windows']['5']['reasons']==['cross_ticker_history_conflict']
    p=packet();single_member(p);p['complexes'].append(copy.deepcopy(p['complexes'][0]))
    out=project(p,NOW)
    assert out['baskets'][0]['windows']['5']['status']=='available'
    assert out['baskets'][-1]['windows']['5']['status']=='available'


def test_basket_sum_matches_canonical_fifty_digit_precision():
    p=packet();value='123456789012345678.123456789012345678'
    p['funds']['XLU']['aligned_windows']['5']['flow_usd_decimal']=value
    single_member(p)
    w=project(p,NOW)['baskets'][0]['windows']['5']
    assert w['status']=='available' and w['flow_usd_decimal']==value


def test_basket_classification_requires_known_unleveraged_category():
    for classification,reason in [(None,'unverified_basket_classification'),({},'unverified_basket_classification'),({'category':'unknown'},'unverified_basket_classification'),({'category':'inverse'},'leveraged_inverse_separate'),({'category':'leveraged'},'leveraged_inverse_separate')]:
        p=packet();p['funds']['SOXL']['classification']=classification;single_member(p,'SOXL')
        w=project(p,NOW)['baskets'][0]['windows']['5']
        assert w['status']=='unavailable' and w['observed_subset_flow_usd_decimal'] is None
        assert w['reasons']==[reason]
    p=packet();single_member(p)
    assert project(p,NOW)['baskets'][0]['windows']['5']['status']=='available'
