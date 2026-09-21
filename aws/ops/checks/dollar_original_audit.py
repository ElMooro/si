"""Independent arithmetic checks against retained original provider rows.

No Dollar calculation helpers are used. Raw provider strings and row ordinals
are the arithmetic inputs; the production compiler is the object being checked.
"""
from calendar import monthrange
from datetime import date,datetime,timedelta
from decimal import Decimal,localcontext,ROUND_HALF_EVEN


def number(value):return None if value in (None,'.','') else Decimal(str(value))
def shift(day,months):
    total=day.year*12+day.month-1-months;year,month=divmod(total,12);month+=1
    return date(year,month,min(day.day,monthrange(year,month)[1]))
def scalar(actual,expected):
    assert set(actual)=={'value','exact_value'}
    assert actual['exact_value']==(str(expected) if expected is not None else None)
    assert actual['value']==(float(round(expected,10)) if expected is not None else None)
def point(actual,expected):
    if expected is None:assert actual is None;return
    day,value,index=expected
    assert actual['date']==day and actual['original_row_index']==index
    scalar({k:actual[k] for k in ('value','exact_value')},value)
def check_change(actual,current,baseline,inverse=False):
    point(actual['current'],current);point(actual['baseline'],baseline)
    a=current[1] if current else None;b=baseline[1] if baseline else None
    if inverse:a=1/a if a is not None else None;b=1/b if b is not None else None
    difference=a-b if a is not None and b is not None else None
    relative=100*(a/b-1) if difference is not None and b>0 else None
    scalar(actual['difference'],difference);scalar(actual['relative_percent'],relative)
    assert actual['available']==(difference is not None) and actual['inverted']==inverse


def independent(output,originals,canonical,catalog):
    counts={'original_series':0,'rendered_points':0,'calendar_comparisons':0,'reciprocal_comparisons':0,'matched_months':0}
    with localcontext() as context:
        context.prec=34;context.rounding=ROUND_HALF_EVEN
        source_day=datetime.fromisoformat(canonical['generated_at']).date();compiled=datetime.fromisoformat(output['generated_at'])
        assert compiled>=datetime.fromisoformat(canonical['generated_at'])
        assert output['source_generated_at']==canonical['generated_at'] and output['source_replay']==canonical['replay']
        assert set(output['series'])==set(catalog.SERIES)
        histories={};families={}
        for sid,row in output['series'].items():
            original=originals.get(sid);label,unit,freq,family=catalog.SPECS[sid];families.setdefault(family,[]).append(sid)
            rows=[]
            if original:
                meta=original['definition']['seriess'][0]
                assert (meta['id'],meta['units'],meta['frequency_short'],meta['seasonal_adjustment_short'])==(sid,unit,freq,'NSA')
                rows=sorted((r['date'],number(r['value']),i) for i,r in enumerate(original['observations']['observations']) if date.fromisoformat(r['date'])<=source_day)
                assert len({r[0] for r in rows})==len(rows)
                assert row['definition']==meta and row['evidence']==original['evidence'] and row['acquired_at']==original['acquired_at']
                counts['original_series']+=1
            histories[sid]=rows;latest=rows[-1] if rows else None;point(row['latest_observation'],latest)
            assert (row['series_id'],row['label'],row['unit'],row['frequency'],row['family'])==(sid,label,unit,freq,family)
            cutoff=shift(date.fromisoformat(latest[0]),13 if freq=='D' else 60) if latest else None
            displayed=[r for r in rows if cutoff and r[0]>=str(cutoff)]
            assert len(row['history'])==len(displayed)
            for shown,raw in zip(row['history'],displayed):point(shown,raw);counts['rendered_points']+=1
            assert row['history_scope']['displayed_rows']==len(displayed) and row['history_scope']['eligible_original_rows']==len(rows)
            if not original:state='unavailable'
            elif latest[1] is None:state='unavailable'
            elif (compiled.date()-date.fromisoformat(latest[0])).days>{'D':10,'W':21,'M':100}[freq]:state='stale_observation'
            elif (compiled-datetime.fromisoformat(original['acquired_at'])).total_seconds()>26*3600 or (compiled-datetime.fromisoformat(canonical['generated_at'])).total_seconds()>26*3600:state='stale_source'
            elif canonical['measurements'][sid]['quality']['status']!='fresh':state='source_ineligible'
            else:state='within_age_ceiling'
            assert row['quality']['status']==state and row['quality']['release_calendar_verified'] is False
            assert row['current_value']==(float(round(latest[1],10)) if latest and state=='within_age_ceiling' else None)
            assert all(row[k] is False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'))
            if not rows:assert row['comparisons']=={};continue
            for label,months,days in (('week',0,7),('month',1,0),('quarter',3,0),('year',12,0)):
                current_day=date.fromisoformat(latest[0]);target=current_day-timedelta(days=days) if days else shift(current_day,months)
                eligible=[r for r in rows if r[0]<=str(target)];base=eligible[-1] if eligible else None
                if freq=='M':base=base if not days and base and base[0]==str(target) else None
                elif base and (target-date.fromisoformat(base[0])).days>(4 if freq=='D' else 6):base=None
                result=row['comparisons'][label];check_change(result,latest,base)
                assert result['target_date']==str(target)
                assert result['baseline_lag_days']==((target-date.fromisoformat(base[0])).days if base else None)
                counts['calendar_comparisons']+=1
                if sid in catalog.FX:
                    currency,num,den,quote_unit=catalog.FX[sid]
                    check_change(result['dollar_strength'],latest,base,num=='USD');counts['reciprocal_comparisons']+=1
            if sid in catalog.FX:
                currency,num,den,quote_unit=catalog.FX[sid];quote=row['quote'];value=latest[1]
                assert (quote['currency'],quote['numerator'],quote['denominator'],quote['source_quote_unit'])==(currency,num,den,quote_unit)
                inv=1/value if value is not None else None
                scalar(quote['foreign_units_per_usd'],inv if num=='USD' else value)
                scalar(quote['usd_per_foreign_unit'],value if num=='USD' else inv)
                assert quote['observation_date']==latest[0] and quote['cnh_substitution_performed'] is False
        rates=output['derived']['us_germany_monthly'];us=histories['DGS10'];de=histories['IRLTLT01DEM156N']
        germans=[r for r in de[-36:] if date.fromisoformat(r[0])<compiled.date().replace(day=1)]
        assert len(rates['trail'])==len(germans)
        for result,german in zip(rates['trail'],germans):
            start=date.fromisoformat(german[0]);end=start.replace(day=monthrange(start.year,start.month)[1])
            members=[r for r in us if str(start)<=r[0]<=str(end)];values=[r[1] for r in members if r[1] is not None]
            covered=bool(us and us[0][0]<=str(start) and us[-1][0]>=str(end));mean=sum(values)/len(values) if covered and len(values)>=15 else None
            delta=(mean-german[1])*100 if mean is not None and german[1] is not None else None
            assert result['reference_month']==str(start) and result['month_end']==str(end);point(result['german'],german)
            assert len(result['us_daily_members'])==len(members)
            for actual,member in zip(result['us_daily_members'],members):point(actual,member)
            scalar(result['us_daily_mean'],mean);scalar(result['difference_bps'],delta)
            assert result['returned_us_rows']==len(members) and result['numeric_us_rows']==len(values)
            assert result['returned_span_covered']==covered and result['calendar_completeness_verified'] is False and result['available']==(delta is not None)
            counts['matched_months']+=1
        assert rates['latest']==(rates['trail'][-1] if rates['trail'] else None)
        curve=output['derived']['treasury_curve'];a=histories['DGS10'][-1] if histories['DGS10'] else None;b=histories['DGS2'][-1] if histories['DGS2'] else None
        delta=100*(a[1]-b[1]) if a and b and a[0]==b[0] and all(output['series'][s]['quality']['status']=='within_age_ceiling' for s in ('DGS10','DGS2')) else None
        point(curve['left'],a);point(curve['right'],b);scalar(curve['difference_bps'],delta);assert curve['available']==(delta is not None)
        liquidity=output['derived']['net_liquidity'];values={};missing=[]
        for sid,factor in (('WALCL',1),('WTREGEN',1),('RRPONTSYD',1000)):
            row=histories[sid][-1] if histories[sid] else None;leg=liquidity['components'][sid];value=row[1] if row else None
            assert leg['value_decimal']==(str(value) if value is not None else None) and leg['multiplier_to_usd_millions']==factor
            assert leg['date']==(row[0] if row else None) and leg['row_index']==(row[2] if row else None)
            valid=output['series'][sid]['quality']['status']=='within_age_ceiling';assert leg['eligible']==valid
            if valid:values[sid]=value*factor
            else:missing.append(sid)
        value=values['WALCL']-values['WTREGEN']-values['RRPONTSYD'] if not missing else None
        assert liquidity['net_decimal']==(str(value) if value is not None else None) and liquidity['net']==(float(value) if value is not None else None)
        assert liquidity['missing_or_ineligible']==missing and liquidity['unit']=='USD_millions'
        assert output['dependency_graph']['series_families']==families and output['dependency_graph']['independent_votes']==0
        assert all(v['independent_votes']==0 for v in output['retained_contexts'].values())
        assert set(output['retained_contexts'])==set(catalog.CONTEXT_KEYS)
        assert all(output[k] is False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'))
        assert output['call'] is output['score'] is output['regime'] is output['dollar_pressure'] is None
        assert output['portfolio_action']=='WAIT' and output['independent_investment_votes']==0
        assert output['benchmark_identity']['benchmark_replication_qualified'] is False
    return counts
