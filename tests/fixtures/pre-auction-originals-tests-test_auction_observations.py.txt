"""Real detector and vendored desk primitive regressions; no provider requests."""
import ast
from copy import deepcopy
from datetime import datetime, timezone
import io,json,runpy,sys,types,urllib.parse,urllib.request
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
SOURCE=ROOT/'aws/lambdas/justhodl-auction-crisis-detector/source/lambda_function.py'
NAMES={'parse_float','parse_term_to_days','classify_tenor_bucket','compute_record_metrics','score_indicators','fetch_fiscal_auctions'}


def detector():
    tree=ast.parse(SOURCE.read_text(encoding='utf-8'))
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in NAMES
           or isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in ('NORMAL_2024_BASELINE','FISCAL_BASE') for t in n.targets)]
    scope={'json':json,'urllib':types.SimpleNamespace(request=urllib.request,parse=urllib.parse)}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(SOURCE),'exec'),scope)
    return scope


def fixture(**changes):
    return {'auction_date':'2026-09-17','security_type':'Note','security_term':'10-Year','cusip':'TEST00001',
        'inflation_index_security':'No','floating_rate':'No','bid_to_cover_ratio':'2.4',
        'high_yield':'4','median_yield':'3.98','low_yield':'3.9','allocation_pctage':'70',
        'primary_dealer_accepted':'2','direct_bidder_accepted':'1','indirect_bidder_accepted':'7',
        'total_accepted':'10',**changes}


def test_numeric_strings_booleans_nonfinite_and_overflow():
    p=detector()['parse_float']
    for bad in (True,False,None,'','null','NaN','Inf','-Infinity','1_000','1,000','1e999',float('nan'),float('inf'),10**1000,{},[]):
        assert p(bad) is None,repr(bad)
    for value in (0,0.0,'0','0.000',' -2.1 ','1e2'):
        assert p(value)==float(value)


def test_each_missing_bidder_withholds_every_share():
    compute=detector()['compute_record_metrics']
    keys=('primary_dealer_accepted','direct_bidder_accepted','indirect_bidder_accepted')
    for key in keys:
        for missing in (None,'',True,'nan','-1'):
            out=compute(fixture(**{key:missing}))
            assert out['primary_dealer_pct'] is out['direct_pct'] is out['indirect_pct'] is None
            assert key in out['bidder_missing_fields'] and out['bidder_denominator_usd'] is None


def test_zero_is_preserved_without_inventing_denominator():
    compute=detector()['compute_record_metrics']
    out=compute(fixture(primary_dealer_accepted='0',total_accepted='0',allocation_pctage='0',bid_to_cover_ratio='0'))
    assert out['primary_dealer_pct']==out['accepted_billions']==out['allocated_at_high_pct']==out['btc']==0
    assert out['direct_pct']==12.5 and out['indirect_pct']==87.5 and out['bidder_denominator_usd']==8
    empty=compute(fixture(primary_dealer_accepted='0',direct_bidder_accepted='0',indirect_bidder_accepted='0'))
    assert empty['bidder_denominator_usd']==0 and empty['primary_dealer_pct'] is None
    assert empty['bidder_share_status']=='zero_denominator'


def test_nonfinite_sum_and_out_of_range_measurements():
    compute=detector()['compute_record_metrics']
    out=compute(fixture(primary_dealer_accepted='1e308',direct_bidder_accepted='1e308',indirect_bidder_accepted='1e308',
        allocation_pctage='101',bid_to_cover_ratio='-1',total_accepted='-2'))
    assert all(out[k] is None for k in ('bidder_denominator_usd','primary_dealer_pct','allocated_at_high_pct','btc','accepted_billions'))


def test_explicit_identity_unknown_conflicting_flags_and_three_year_boundary():
    scope=detector();classify=scope['classify_tenor_bucket']
    for changes,kind in (({'inflation_index_security':'Yes'},'tips'),({'floating_rate':'Yes'},'frn'),
       ({'inflation_index_security':None,'floating_rate':None},'unknown'),
       ({'inflation_index_security':'Yes','floating_rate':'Yes'},'unknown'),
       ({'security_term':'3-Year'},'coupons_lt_3y'),({'security_term':None},'unknown'),
       ({'security_type':'Bill','security_term':'13-Week'},'bills_gte_90d')):
        out=scope['compute_record_metrics'](fixture(**changes));assert out['tenor_bucket']==kind
        if kind in ('unknown','frn'):assert scope['score_indicators'](out,4.25)=={}
    p=scope['parse_term_to_days'];assert p('9-Year 10-Month')==3585
    for bad in ('-1-Year','0-Year','10-Year garbage','2-Year 1-Year',True,'Cash Management'):
        assert p(bad) is None,bad


def test_quote_basis_never_falls_back_between_discount_nominal_real_and_margin():
    compute=detector()['compute_record_metrics']
    out=compute(fixture(security_type='Bill',security_term='4-Week'))
    assert out['quote_basis']=='discount_rate_pct' and out['high_rate'] is None
    out=compute(fixture(inflation_index_security='Yes',high_discnt_rate='20'))
    assert out['quote_basis']=='real_yield_pct' and out['high_rate']==4
    out=compute(fixture(floating_rate='Yes',high_discount_margin='0.055'))
    assert out['quote_basis']=='discount_margin_pct' and out['high_rate']==.055


def test_within_auction_dispersion_never_becomes_when_issued_tail():
    out=detector()['compute_record_metrics'](fixture())
    assert abs(out['high_minus_median_bp']-2)<1e-12
    assert out['tail_bp'] is out['wi_tail_bp'] is None
    assert not out['calls_eligible'] and not out['sizing_eligible']


def test_weak_bill_threshold_is_reachable_and_severity_monotonic():
    scope=detector();values=[]
    for btc in ('2.4','1.85','1.3'):
        out=scope['compute_record_metrics'](fixture(security_type='Bill',security_term='4-Week',bid_to_cover_ratio=btc))
        values.append(scope['score_indicators'](out,4.25)['btc_extreme'])
    assert values==[0,50,80]


def test_vendored_scoring_matches_full_functions_and_declarations():
    expected=detector();actual=runpy.run_path(str(ROOT/'aws/lambdas/justhodl-auction-desk/source/crisis_scoring.py'))
    source=ast.parse(SOURCE.read_text(encoding='utf-8'))
    vendor=ast.parse((ROOT/'aws/lambdas/justhodl-auction-desk/source/crisis_scoring.py').read_text(encoding='utf-8'))
    for name in NAMES-{'fetch_fiscal_auctions'}:
        a=next(n for n in source.body if isinstance(n,ast.FunctionDef) and n.name==name)
        b=next(n for n in vendor.body if isinstance(n,ast.FunctionDef) and n.name==name)
        assert ast.dump(a)==ast.dump(b),name
    assert actual['NORMAL_2024_BASELINE']==expected['NORMAL_2024_BASELINE']


def page(rows,total=2,pages=2,**changes):
    return io.BytesIO(json.dumps({'data':rows,'meta':{'total-pages':pages,'total-count':total},**changes}).encode())


def acquire(responses,size=1):
    fn=detector()['fetch_fiscal_auctions']
    with patch('urllib.request.urlopen',side_effect=responses) as transport:
        rows=fn('2026-09-01','2026-09-17',page_size=size)
    return rows,transport.call_count


def test_complete_pages_and_explicit_empty_set():
    first=fixture();second=fixture(cusip='TEST00002')
    rows,n=acquire([page([first]),page([second])]);assert rows==[first,second] and n==2
    assert acquire([page([],total=0,pages=1)])[0]==[]


def test_failed_later_page_never_returns_partial_history():
    try:acquire([page([fixture()]),OSError('synthetic transport failure')])
    except RuntimeError as exc:assert 'page 2' in str(exc) and 'previous publication preserved' in str(exc)
    else:raise AssertionError('Partial history accepted')


def test_inconsistent_repeated_short_and_missing_pagination_rejected():
    cases=[lambda:[page([fixture()]),page([fixture(cusip='OTHER0001')],total=3,pages=3)],
           lambda:[page([fixture()]),page([fixture()])],
           lambda:[page([],total=2,pages=2)],
           lambda:[page([fixture()],meta={})],
           lambda:[page([fixture()],meta={'total-pages':True,'total-count':1})],
           lambda:[page([fixture()],total=31,pages=31)],
           lambda:[page([fixture()],total=3,pages=2)]]
    for case in cases:
        try:acquire(case())
        except RuntimeError:pass
        else:raise AssertionError('Incomplete or inconsistent page set accepted')


def test_invalid_auction_identity_date_and_oversized_page_rejected():
    for row in (fixture(cusip=None),fixture(auction_date='2026-09-18'),fixture(auction_date='2026-09-01T01:00:00Z')):
        try:acquire([page([row],total=1,pages=1)])
        except RuntimeError:pass
        else:raise AssertionError('Invalid identity accepted')
    try:acquire([io.BytesIO(b' '*(4*1024*1024+1))])
    except RuntimeError:pass
    else:raise AssertionError('Oversized page accepted')


def test_actual_handler_failed_acquisition_preserves_all_published_bytes():
    support=runpy.run_path(str(ROOT/'tests/deployment/test_auction_output_ownership.py'))
    store=support['Store']({'data/auction-crisis.json':{'preexisting':'unchanged'}})
    before=deepcopy(store.docs);scope=support['load']('auction-crisis-detector',store)
    complete_first=[fixture(cusip=f'TEST{i:05d}') for i in range(200)]
    with patch('urllib.request.urlopen',side_effect=[page(complete_first,total=201,pages=2),OSError('synthetic page two failure')]):
        try:scope['lambda_handler']({},None)
        except RuntimeError:pass
        else:raise AssertionError('Incomplete acquisition did not fail')
    assert store.docs==before and store.writes==[] and store.reads==[]


def test_fresh_observation_never_grants_heuristic_authority():
    scope=runpy.run_path(str(ROOT/'aws/lambdas/justhodl-auction-crisis-detector/source/auction_quality.py'))
    doc={'freshness':{'latest_auction_date':'2026-09-17'},'recent_auctions':[fixture()],'composite_score':0}
    out=scope['stamp_quality'](doc,datetime(2026,9,17,tzinfo=timezone.utc))
    assert out['composite_score']==0 and out['quality']['status']=='fresh'
    assert all(out[k] is False for k in ('calls_eligible','sizing_eligible','forecast_eligible','execution_eligible','source_capture_verified'))
    assert out['score_validation']['status']=='unqualified_legacy_heuristic'


def test_complete_weighted_arithmetic_ignores_zero_without_inventing_weight():
    from auction_weighting import weighted_summary
    rows=[{'composite_score':100,'accepted_billions':0},
          {'composite_score':20,'accepted_billions':2},{'composite_score':80,'accepted_billions':1}]
    out=weighted_summary(rows)
    assert out['composite']==40 and out['known_weight_total_usd_bn']==3 and out['n_zero_weights']==1
    assert out['status']=='complete' and not out['sizing_eligible']
    assert weighted_summary(list(reversed(rows)))==out


def test_missing_negative_boolean_nonfinite_weights_never_enter_composite():
    from auction_weighting import weighted_summary
    for weight in (None,-1,True,'1',float('nan'),float('inf')):
        out=weighted_summary([{'composite_score':100,'accepted_billions':weight},
                              {'composite_score':20,'accepted_billions':2}])
        assert out['composite'] is None and out['status']=='missing_weight' and out['n_missing_weights']==1
    for score in (None,-1,True,101,float('nan')):
        out=weighted_summary([{'composite_score':score,'accepted_billions':2}])
        assert out['composite'] is None and out['status']=='missing_score'
    assert weighted_summary([])['status']=='empty'
    assert weighted_summary([{'accepted_billions':0}])['status']=='no_positive_weight'
    assert weighted_summary([{'accepted_billions':1e308,'composite_score':100}])['status']=='nonfinite_aggregate'


def test_actual_detector_tenor_and_history_share_missing_size_gate():
    support=runpy.run_path(str(ROOT/'tests/deployment/test_auction_output_ownership.py'))
    store,scope=support['detector_fixture']();handler=scope['lambda_handler'];env=handler.__globals__
    day=datetime.now(timezone.utc).date().isoformat()
    base=fixture(auction_date=day,total_accepted='2000000000')
    for unknown in (None,'0'):
        row={**base,'total_accepted':unknown}
        env['fetch_fiscal_auctions']=lambda *a,**kw:[row]
        with patch('urllib.request.urlopen',side_effect=AssertionError('No network')):
            handler({},None)
        doc=store.docs['data/auction-crisis.json']
        assert doc['composite_score'] is None and doc['regime']=='UNAVAILABLE' and not doc['triggers']
        assert doc['tenor_decomposition']['coupons_gt_3y']['composite'] is None
        assert doc['composite_history']['current']['composite'] is None
        assert doc['weighting_quality']['status']==('missing_weight' if unknown is None else 'no_positive_weight')
    row={**base,'bid_to_cover_ratio':'0'}
    env['fetch_fiscal_auctions']=lambda *a,**kw:[row]
    with patch('urllib.request.urlopen',side_effect=AssertionError('No network')):handler({},None)
    doc=store.docs['data/auction-crisis.json']
    assert doc['recent_auctions'][0]['btc']==0 and doc['weighting_quality']['status']=='complete'


def test_actual_desk_history_withholds_unknown_size_and_keeps_explicit_zero():
    support=runpy.run_path(str(ROOT/'tests/deployment/test_auction_output_ownership.py'))
    scope=support['load']('auction-desk',support['Store']())
    day=datetime.now(timezone.utc).date().isoformat()
    for amount,status in ((None,'missing_weight'),('0','no_positive_weight')):
        raw=fixture(auction_date=day,total_accepted=amount,bid_to_cover_ratio='0')
        out=scope['build_composite_history']({'row':raw},{day:4.25})
        assert out['series'][-1]['composite'] is None
        assert out['series'][-1]['weighting_quality']['status']==status


def test_unqualified_thresholds_do_not_publish_position_instructions():
    support=runpy.run_path(str(ROOT/'tests/deployment/test_auction_output_ownership.py'))
    store,scope=support['detector_fixture']();handler=scope['lambda_handler']
    for btc in ('2.4','0'):
        row={**support['auction'](),'bid_to_cover_ratio':btc}
        handler.__globals__['fetch_fiscal_auctions']=lambda *a,**kw:[row]
        with patch('urllib.request.urlopen',side_effect=AssertionError('No network')):handler({},None)
        doc=store.docs['data/auction-crisis.json']
        text=' '.join([doc['interpretation']]+[v.get('action','') for v in doc['triggers']]).lower()
        for phrase in ('tighten stops','reduce risk now','immediate action','tlt puts','hedge equity','defensive posture','tighten exposure'):
            assert phrase not in text,phrase
        assert 'no validated' in text or 'uncalibrated' in text


if __name__=='__main__':
    tests=[v for k,v in list(globals().items()) if k.startswith('test_')]
    for test in tests:test()
    print('Auction observation regressions passed:',len(tests))
