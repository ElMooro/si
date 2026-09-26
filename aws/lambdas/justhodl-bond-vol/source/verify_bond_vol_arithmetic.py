"""Independent rational variance and 100-digit root verification; no candidate import."""
from datetime import date,datetime,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
from fractions import Fraction
from zoneinfo import ZoneInfo
import base64,io
import bond_vol_timezone as pinned_timezone
import hashlib,json,math
from bond_vol_catalog import SERIES,SPECS,MOVE_URL,MOVE_NAMES

FLAGS=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','point_in_time_backtest_qualified','publication_eligible')
def quote_zone():
    raw=base64.b64decode(pinned_timezone.TZIF_BASE64,validate=True)
    assert hashlib.sha256(raw).hexdigest()==pinned_timezone.SHA256
    return ZoneInfo.from_file(io.BytesIO(raw),key='America/New_York')
def clock(value):
    out=datetime.fromisoformat(value.replace('Z','+00:00'));assert out.tzinfo is not None
    return out.astimezone(timezone.utc)
def rational(value):return None if value in (None,'.','') else Fraction(value)
def decimal(value):return Decimal(value.numerator)/Decimal(value.denominator)
def var(values):
    n=len(values)
    return (sum(v*v for v in values)-sum(values)**2/n)/(n-1)
def scalar(actual,expected):
    assert set(actual)=={'value','calculated_decimal'}
    if expected is None:assert actual=={'value':None,'calculated_decimal':None};return
    if isinstance(expected,Fraction):expected=decimal(expected)
    assert abs(Decimal(actual['calculated_decimal'])-expected)<=max(Decimal('1e-60'),abs(expected)*Decimal('1e-60'))
    assert type(actual['value']) in (int,float) and math.isfinite(actual['value'])
    assert abs(actual['value']-float(expected))<=max(1e-12,abs(float(expected))*1e-14)

def verify_quote(actual,raw,receipt,stamp):
    assert all(actual[k] is False for k in FLAGS) and actual['is_proxy'] is False
    assert actual['official_feed_parity_verified'] is False
    if raw is None:
        assert actual['status']=='unavailable' and actual['current'] is None and actual['history']==[]
        assert actual['original'] is None and actual['receipt'] is None;return 0
    assert receipt['source_url']==MOVE_URL and receipt['bytes']==len(raw) and receipt['sha256']==hashlib.sha256(raw).hexdigest()
    assert actual['receipt']==receipt and actual['unit']=='index_points'
    try:doc=json.loads(raw,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
    except (ValueError,UnicodeDecodeError):doc=None
    assert actual['original']==doc
    valid=isinstance(doc,dict) and isinstance(doc.get('chart'),dict)
    results=doc['chart'].get('result') if valid else None
    valid=valid and doc['chart'].get('error') is None and isinstance(results,list) and len(results)==1
    res=results[0] if valid else None
    valid=valid and isinstance(res,dict) and isinstance(res.get('meta'),dict) and isinstance(res.get('indicators'),dict)
    stamps=res.get('timestamp') if valid else None;quotes=res['indicators'].get('quote') if valid else None
    valid=valid and isinstance(stamps,list) and isinstance(quotes,list) and len(quotes)==1 and isinstance(quotes[0],dict)
    closes=quotes[0].get('close') if valid else None
    valid=valid and isinstance(closes,list) and all(isinstance(v,list) and len(v)==len(stamps) for v in quotes[0].values())
    valid=valid and all(type(t) is int and 0<=t<=253402214400 for t in stamps) and len(set(stamps))==len(stamps)
    valid=valid and all(c is None or type(c) in (int,float) and math.isfinite(c) and c>=0 for c in closes)
    if valid:
        days=[datetime.fromtimestamp(t,timezone.utc).astimezone(quote_zone()).date() for t in stamps]
        valid=len(set(days))==len(days)
    if not valid:
        assert actual['status']=='schema_mismatch'
        assert actual['current'] is None and actual['history']==[]
        return 0
    assert actual['status']!='schema_mismatch'
    res=doc['chart']['result'][0];meta=res['meta'];stamps=res['timestamp'];closes=res['indicators']['quote'][0]['close']
    identity=(meta.get('symbol')=='^MOVE' and meta.get('instrumentType')=='INDEX'
        and meta.get('exchangeTimezoneName')=='America/New_York' and meta.get('dataGranularity')=='1d'
        and all(meta.get(k) in MOVE_NAMES for k in ('longName','shortName')))
    assert actual['identity_reviewed']==identity and actual['provider_name']==meta.get('longName')
    zone=quote_zone()
    assert actual['timezone']=={'name':'America/New_York','iana_version':pinned_timezone.VERSION,'tzif_sha256':pinned_timezone.SHA256}
    expected=[{'original_row':i,'timestamp':t,'session_date':datetime.fromtimestamp(t,timezone.utc).astimezone(zone).date().isoformat(),
        'reported_close':closes[i]} for i,t in enumerate(stamps)]
    assert actual['history']==expected and len(stamps)==len(closes)
    if not identity:
        assert actual['status']=='identity_mismatch' and actual['current'] is None;return len(expected)
    if not expected:assert actual['status']=='unavailable' and actual['current'] is None;return 0
    latest=max(expected,key=lambda r:r['timestamp']);assert actual['last_observed']==latest
    now=clock(stamp);acquired=clock(receipt['acquired_at']);age=(now.astimezone(zone).date()-date.fromisoformat(latest['session_date'])).days
    expected_status=('provisional_or_future_session' if latest['session_date']>=str(acquired.astimezone(zone).date()) else
        'missing_latest_close' if latest['reported_close'] is None else 'stale_source' if (now-acquired).total_seconds()>26*3600 else
        'stale_observation' if not 0<=age<=7 else 'quoted_previous_session')
    assert actual['status']==expected_status and actual['current']==(latest if expected_status=='quoted_previous_session' else None)
    return len(expected)

def verify(output,source,originals,quote_raw=None,quote_receipt=None):
    assert output['contract']=='bond-vol-candidate.v1' and output['candidate_only'] is True
    assert set(output['series'])==set(SERIES)==set(originals)
    assert output['source_replay']==source['replay'] and output['source_generated_at']==source['generated_at']
    assert all(output[k] is False for k in FLAGS)
    assert output['composite_z_score'] is None and output['regime'] is None and output['call'] is None and output['signals']==[]
    assert output['decision']=={'verb':'WAIT','meaning':'abstain'} and output['portfolio_consequences']['target_weights'] is None
    assert output['dependency_graph']['independent_votes']==0 and output['dependency_graph']['statistical_independence_qualified'] is False
    assert output['dependency_graph']['series_to_engine']==list(SERIES) and output['funding_context']['independent_votes']==0
    assert output['dependency_graph']['series_roots']=={s:SPECS[s]['dependency_roots'] for s in SERIES}
    now=clock(output['generated_at']);source_time=clock(source['generated_at']);assert now>=source_time
    checks=total=windows=current=available=0
    with localcontext() as arithmetic:
        arithmetic.prec=100;arithmetic.rounding=ROUND_HALF_EVEN
        for sid in SERIES:
            row=output['series'][sid];original=originals[sid]
            assert row['series_id']==sid and all(row[k] is False for k in FLAGS)
            if original is None:
                assert row['original_rows']==[] and row['rolling_history']==[] and row['current'] is None
                assert row['quality']['status']=='unavailable';continue
            available+=1;raw=original['observations']['observations'];total+=len(raw)
            assert row['original_rows']==raw and row['source_definition']==original['definition']['seriess'][0]
            assert row['evidence']==original['evidence'] and row['acquired_at']==original['acquired_at']
            definition=original['definition']['seriess'][0]
            reviewed=tuple(definition.get(k) for k in ('units','frequency_short','frequency','seasonal_adjustment'))==SPECS[sid]['reviewed_definition']
            assert row['definition_reviewed']==reviewed and row['provider_updated_at']==definition.get('last_updated')
            acquired=clock(original['acquired_at']);assert acquired<=source_time
            eligible=sorted([(i,r) for i,r in enumerate(raw) if r['date']<=str(acquired.date())],key=lambda pair:pair[1]['date'])
            latest=eligible[-1][1] if eligible else None;assert row['latest_observation']==latest
            obs_age=(now.date()-date.fromisoformat(latest['date'])).days if latest else None;acq_age=(now-acquired).total_seconds()
            status=('definition_mismatch' if not reviewed else 'unavailable' if latest is None or rational(latest.get('value')) is None else
                'stale_observation' if not 0<=obs_age<=7 else 'stale_source' if not 0<=acq_age<=26*3600 or (now-source_time).total_seconds()>26*3600 else
                'source_unusable' if source['measurements'][sid]['quality']['status']!='fresh' else 'within_age_ceiling')
            assert row['quality']['status']==status and row['quality']['observation_age_days']==obs_age and row['quality']['acquisition_age_seconds']==acq_age
            numeric=[(position,i,r,rational(r.get('value'))) for position,(i,r) in enumerate(eligible) if rational(r.get('value')) is not None]
            history=row['rolling_history'];assert len(history)==(max(0,len(numeric)-30) if reviewed else 0)
            vols=[]
            for index,actual in enumerate(history):
                batch=numeric[index:index+31];first,last=batch[0],batch[-1]
                gaps=[(date.fromisoformat(batch[j][2]['date'])-date.fromisoformat(batch[j-1][2]['date'])).days for j in range(1,31)]
                deltas=[100*(batch[j][3]-batch[j-1][3]) for j in range(1,31)]
                variance=var(deltas);rv=decimal(variance*252).sqrt();vols.append(rv)
                assert actual['start_date']==first[2]['date'] and actual['end_date']==last[2]['date']
                assert actual['start_original_row']==first[1] and actual['end_original_row']==last[1]
                assert actual['elapsed_calendar_days']==(date.fromisoformat(last[2]['date'])-date.fromisoformat(first[2]['date'])).days
                assert actual['numeric_observations']==31 and actual['change_count']==30
                assert actual['missing_rows_inside_window']==last[0]-first[0]-30 and actual['max_interval_days']==max(gaps)
                assert actual['comparable_interval_ceiling_met']==(max(gaps)<=7)
                scalar(actual['sample_variance_bp2'],variance);scalar(actual['step_dispersion_bp'],decimal(variance).sqrt())
                scalar(actual['annualized_assuming_252_steps_bp'],rv);checks+=3;windows+=1
            last=history[-1] if history else None
            assert row['last_calculated']==last
            usable=bool(status=='within_age_ceiling' and last and last['end_date']==latest['date'] and last['comparable_interval_ceiling_met'])
            assert row['current']==(last if usable else None) and row['quality']['current_dispersion_available']==usable
            current+=usable
            distribution=row['historical_distribution'];prior=history[-253:-1] if history else []
            assert distribution['prior_window_count']==len(prior) and distribution['required_prior_windows']==252 and distribution['excludes_current'] is True
            assert distribution['first_prior_end_date']==(prior[0]['end_date'] if prior else None)
            assert distribution['last_prior_end_date']==(prior[-1]['end_date'] if prior else None)
            bad=sum(not r['comparable_interval_ceiling_met'] for r in prior)
            assert distribution['invalid_interval_windows']==bad and distribution['independent_sample_size'] is None
            if len(prior)<252:
                assert distribution['status']=='insufficient_prior_windows'
                expected=[None]*4
            elif bad or not last['comparable_interval_ceiling_met']:
                assert distribution['status']=='noncomparable_intervals';expected=[None]*4
            else:
                baseline=vols[-253:-1];flat=len(set(baseline))==1
                mean=baseline[0] if flat else sum(baseline)/252
                sd=Decimal(0) if flat else (sum((x-mean)**2 for x in baseline)/251).sqrt();current_rv=vols[-1]
                assert distribution['status']==('flat_baseline' if sd==0 else 'available')
                rank=Decimal(100)*(sum(v<current_rv for v in baseline)+Decimal('.5')*sum(v==current_rv for v in baseline))/252
                expected=[mean,sd,(current_rv-mean)/sd if sd else None,rank]
            for key,value in zip(('mean_bp','sample_sd_bp','z_score','midrank_percentile'),expected):scalar(distribution[key],value);checks+=1
            assert row['current_distribution']==(distribution if usable else None)
        quote_rows=verify_quote(output['move'],quote_raw,quote_receipt,output['generated_at'])
    assert output['quality']['current_series']==current and output['quality']['original_rows']==total
    return {'requested_series':len(SERIES),'reconstructed_series':available,'current_series':current,'original_rows':total,
        'rolling_windows':windows,'independent_scalar_checks':checks,'quote_rows':quote_rows,'quote_status':output['move']['status'],
        'forecast_qualified':False,'sizing_qualified':False}
