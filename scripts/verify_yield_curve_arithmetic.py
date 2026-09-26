"""Independent Fraction/date verifier. Does not import the candidate compiler."""
from datetime import date,datetime,timezone
from fractions import Fraction
import math
from yield_curve_catalog import SERIES,SPECS,NOMINAL,REAL,LAGS

# These formulas are deliberately written independently of the candidate catalog.
METRICS = {
    name:{'coefficients':{long:100,short:-100},'divisor':1,'unit':'basis_points'}
    for name,long,short in (
        ('2s10s','DGS10','DGS2'),('3M10Y','DGS10','DGS3MO'),('5s30s','DGS30','DGS5'),
        ('2s5s','DGS5','DGS2'),('10s30s','DGS30','DGS10'),('dff_to_10y','DGS10','DFF'),
        ('real_5s30s','DFII30','DFII5'),('target_band_width','DFEDTARU','DFEDTARL'))}
METRICS.update({
    'butterfly_2_5_10':{'coefficients':{'DGS2':-100,'DGS5':200,'DGS10':-100},'divisor':2,'unit':'basis_points'},
    'curvature_2_5_10':{'coefficients':{'DGS2':-100,'DGS5':200,'DGS10':-100},'divisor':1,'unit':'basis_points'},
    'nominal_mean':{'coefficients':dict.fromkeys(('DGS1MO','DGS3MO','DGS6MO','DGS1','DGS2','DGS3','DGS5','DGS7','DGS10','DGS20','DGS30'),1),'divisor':11,'unit':'percent'},
    'nominal_real_breakeven_residual_10y':{'coefficients':{'DGS10':100,'DFII10':-100,'T10YIE':-100},'divisor':1,'unit':'basis_points'},
})


def clock(value):
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    assert result.tzinfo is not None
    return result.astimezone(timezone.utc)


def number(value):
    return None if value in (None,'','.') or isinstance(value,bool) else Fraction(str(value))


def scalar(actual,expected):
    if expected is None:
        assert actual=={'value':None,'exact_decimal':None}
    else:
        assert actual['exact_decimal'] is not None
        assert abs(number(actual['exact_decimal'])-expected)<=max(Fraction(1,10**28),abs(expected)*Fraction(1,10**32))
        assert math.isfinite(actual['value']) and abs(actual['value']-float(expected))<=max(1e-12,abs(float(expected))*1e-14)


def verify(output,source,originals):
    assert output['contract']=='yield-curve-candidate.v1' and output['candidate_only'] is True
    assert set(output['series'])==set(SERIES)==set(originals)
    assert output['source_replay']==source['replay'] and output['source_generated_at']==source['generated_at']
    assert output['call'] is None and output['signals']==[] and output['decision']=={'verb':'WAIT','meaning':'abstain'}
    assert output['portfolio_consequences']['target_weights'] is None and output['qualified_term_premium_bps'] is None
    flags=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','publication_eligible','point_in_time_backtest_qualified')
    assert all(output[k] is False for k in flags)
    assert output['dependency_graph']['independent_votes']==0 and output['dependency_graph']['series_to_engine']==list(SERIES)
    assert output['term_premium_context']['independent_votes']==0
    now=clock(output['generated_at']);source_time=clock(source['generated_at']);assert now>=source_time
    maps={};latest_dates={};reviewed={};usable={};total=0;checks=0
    for sid in SERIES:
        original=originals[sid]; actual=output['series'][sid]
        assert all(actual[k] is False for k in flags)
        assert actual['series_id']==sid and actual['published_at'] is None
        if original is None:
            assert actual['history']==[] and actual['evidence']=={} and actual['quality']['status']=='unavailable'
            scalar(actual['current'],None);scalar(actual['last_observed'],None)
            maps[sid]={};latest_dates[sid]=None;reviewed[sid]=usable[sid]=False
            continue
        definition=original['definition']['seriess'][0];rows=original['observations']['observations'];total+=len(rows)
        assert actual['source_definition']==definition and actual['evidence']==original['evidence']
        assert actual['acquired_at']==original['acquired_at']
        assert actual['provider_updated_at']==definition.get('last_updated')
        assert len(actual['history'])==len(rows)
        for i,(raw,retained) in enumerate(zip(rows,actual['history'])):
            assert retained=={'original_row':i,'observation_date':raw['date'],'native_value':raw.get('value'),
                'realtime_start':raw.get('realtime_start'),'realtime_end':raw.get('realtime_end')}
        maps[sid]={r['date']:(i,number(r.get('value'))) for i,r in enumerate(rows) if r['date']<=str(source_time.date())}
        assert len({r['date'] for r in rows})==len(rows)
        latest_dates[sid]=max(maps[sid]) if maps[sid] else None
        latest=maps[sid].get(latest_dates[sid]);value=latest[1] if latest else None
        defined=(definition.get('units'),definition.get('frequency_short'),definition.get('frequency'),definition.get('seasonal_adjustment'))
        reviewed[sid]=defined==SPECS[sid]['reviewed_definition']
        assert actual['definition_reviewed']==reviewed[sid]
        assert actual['latest_date']==latest_dates[sid] and actual['original_row']==(latest[0] if latest else None)
        observation_age=(now.date()-date.fromisoformat(latest_dates[sid])).days if latest else None
        acquisition_age=(now-clock(original['acquired_at'])).total_seconds()
        usable[sid]=(reviewed[sid] and value is not None and 0<=observation_age<=7
            and 0<=acquisition_age<=26*3600 and (now-source_time).total_seconds()<=26*3600
            and source['measurements'][sid]['quality']['status']=='fresh')
        assert actual['quality']['observation_age_days']==observation_age
        assert actual['quality']['acquisition_age_seconds']==acquisition_age
        assert (actual['quality']['status']=='within_age_ceiling')==usable[sid]
        scalar(actual['current'],value if usable[sid] else None);scalar(actual['last_observed'],value);checks+=2
    comparison_count=0

    def test_metric(coefficients,divisor,unit,actual=None,row=None):
        nonlocal checks,comparison_count
        common=set.intersection(*(set(d for d,(_,v) in maps[s].items() if v is not None) for s in coefficients))
        matched=sorted(common);all_dates=set.union(*(set(maps[s]) for s in coefficients))
        end=matched[-1] if matched else None;valid=all(reviewed[s] for s in coefficients)
        live=bool(end and valid and all(usable[s] for s in coefficients) and end==min(latest_dates[s] for s in coefficients))
        def expected(day):
            return sum(c*maps[s][day][1] for s,c in coefficients.items())/divisor if day and valid else None
        def point(value,day):
            nonlocal checks
            if day is None:assert value is None;return
            assert value['observation_date']==day and set(value['legs'])==set(coefficients)
            for s in coefficients:
                assert value['legs'][s]['original_row']==maps[s][day][0]
                assert number(value['legs'][s]['native_decimal'])==maps[s][day][1]
            scalar(value['measurement'],expected(day));checks+=1
        comparisons=row['historical_observation_comparisons'] if row else actual['historical_comparisons']
        assert set(comparisons)==set(map(str,LAGS))
        if actual:
            assert actual['coefficients']==coefficients and actual['divisor']==divisor and actual['unit']==unit
            assert actual['latest_source_dates']=={s:latest_dates[s] for s in coefficients}
            assert actual['matched_numeric_dates']==len(matched)
            point(actual['last_matched'],end);point(actual['current'],end if live else None)
            assert all(actual[k] is False for k in flags)
        for lag in LAGS:
            comp=comparisons[str(lag)];start=matched[-lag-1] if len(matched)>lag else None
            assert comp['matched_observation_steps']==lag and comp['current_date']==end
            point(comp['baseline'],start)
            assert comp['elapsed_calendar_days']==((date.fromisoformat(end)-date.fromisoformat(start)).days if start else None)
            assert comp['unmatched_or_missing_dates']==(len([d for d in all_dates if start<d<=end and d not in common]) if start else None)
            scalar(comp['change'],expected(end)-expected(start) if valid and end and start else None)
            assert comp['change_unit']==('percentage_points' if unit=='percent' else 'basis_points')
            comparison_count+=1;checks+=1
        assert (row['current_observation_comparisons'] if row else actual['current_comparisons'])==(comparisons if live else None)
        return matched,live

    assert set(output['derived'])==set(METRICS)
    for name,spec in METRICS.items():test_metric(**spec,actual=output['derived'][name])
    for sid in SERIES:test_metric({sid:100},1,'basis_points',row=output['series'][sid])
    for group,members in (('nominal',NOMINAL),('real',REAL)):
        curve=output['curves'][group]
        common=set.intersection(*(set(d for d,(_,v) in maps[s].items() if v is not None) for s in members))
        end=max(common) if common else None
        live=bool(end and all(usable[s] for s in members) and end==min(latest_dates[s] for s in members))
        assert curve['complete']==live and curve['interpolated'] is False and curve['requested_series']==list(members)
        assert curve['observation_date']==(end if live else None) and curve['unit']=='percent'
        assert len(curve['points'])==(len(members) if live else 0)
        for sid,point in zip(members,curve['points']):
            assert point['series_id']==sid and point['tenor_months']==SPECS[sid]['tenor_months']
            assert point['original_row']==maps[sid][end][0]
            scalar({k:point[k] for k in ('value','exact_decimal')},maps[sid][end][1]);checks+=1
    shape=output['shape'];spread=output['derived']['2s10s'];latest=spread['current']
    base=(spread['current_comparisons'] or {}).get('5',{}).get('baseline')
    assert shape['inverted_2s10s']==(number(latest['measurement']['exact_decimal'])<0 if latest else None)
    assert all(shape[k] is False for k in flags)
    if latest and base:
        end,start=latest['observation_date'],base['observation_date']
        short=100*(maps['DGS2'][end][1]-maps['DGS2'][start][1]);long=100*(maps['DGS10'][end][1]-maps['DGS10'][start][1])
        slope=long-short;mean=(short+long)/2
        label=('RISING' if mean>0 else 'FALLING' if mean<0 else 'UNCHANGED_MEAN')+'_'+('STEEPENING' if slope>0 else 'FLATTENING' if slope<0 else 'PARALLEL' if mean else 'UNCHANGED')
        assert shape['label']==label and shape['status']=='descriptive'
        assert shape['current_date']==end and shape['baseline_date']==start
        for key,value in (('short_change_bps',short),('long_change_bps',long),('slope_change_bps',slope),('mean_endpoint_change_bps',mean)):
            scalar(shape[key],value);checks+=1
    else:
        assert shape['status']=='unavailable' and shape['label'] is None
        for key in ('short_change_bps','long_change_bps','slope_change_bps','mean_endpoint_change_bps'):scalar(shape[key],None)
    assert output['quality']['current_series']==sum(usable.values())
    assert output['quality']['missing_series']==[s for s in SERIES if originals[s] is None]
    return {'requested_series':len(SERIES),'reconstructed_series':sum(o is not None for o in originals.values()),
        'original_rows':total,'rational_checks':checks,'matched_observation_comparisons':comparison_count,
        'current_series':sum(usable.values()),'missing_series':[s for s in SERIES if originals[s] is None],
        'all_original_rows_retained':True,'forecast_qualified':False,'sizing_qualified':False}
