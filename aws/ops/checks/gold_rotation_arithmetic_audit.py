"""Independent rational arithmetic checked directly against retained vendor rows."""
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
from fractions import Fraction
import hashlib,json

def independent(output,inputs,read):
    def original(ref):
        body=read(ref['key']);assert len(body)==ref['bytes'] and hashlib.sha256(body).hexdigest()==ref['sha256']
        return json.loads(body,parse_float=Decimal,parse_int=Decimal)
    def rational(v):return Fraction(Decimal(str(v)))
    def quantized(v):
        with localcontext() as ctx:
            ctx.prec=100
            if isinstance(v,Fraction):v=Decimal(v.numerator)/Decimal(v.denominator)
            return v.quantize(Decimal('0.000000000001'),rounding=ROUND_HALF_EVEN)
    def check(metric,value,dates,n):
        assert metric['observations_available']==len(dates) and metric['observations_required']==n
        assert metric['start_date']==(dates[0] if dates else None) and metric['end_date']==(dates[-1] if dates else None)
        assert (metric['value_decimal'] is None) if value is None else Decimal(metric['value_decimal'])==quantized(value)
        assert all(metric[k] is False for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'))
    def returns(actual,dates,values):
        for n in (5,20,63,252):
            selected=dates[-n-1:];ready=len(selected)==n+1 and all(values.get(d) is not None for d in selected)
            value=(values[selected[-1]]/values[selected[0]]-1)*100 if ready else None
            check(actual[str(n)],value,selected,n+1);assert actual[str(n)]['unit']=='percent'
    kinds={'light':'price','full':'close','dividend-adjusted':'adjClose'}
    price_maps={};n_source_rows=0;n_source_fields=0;n_windows=0
    for symbol,instrument in output['instruments'].items():
        profile=original(inputs['captures'][symbol+':profile']['original']);assert len(profile)==1
        assert profile[0]['isEtf'] is True
        for field in ('symbol','isin','exchange','currency'):assert profile[0][field]==instrument[field]
        ledgers={kind:original(inputs['captures'][symbol+':'+kind]['original']) for kind in kinds}
        maps={kind:{r['date']:(i,r) for i,r in enumerate(rows)} for kind,rows in ledgers.items()}
        dates=sorted(set().union(*(set(rows) for rows in maps.values())))
        assert [r['date'] for r in instrument['history']]==dates
        assert instrument['latest']==instrument['history'][-1]
        price_maps[symbol]={}
        for kind,price_field in kinds.items():
            n_source_rows+=len(ledgers[kind]);values={}
            for row in instrument['history']:
                day=row['date'];source=maps[kind].get(day);reported=row['source_rows'][kind]
                if source is None:
                    assert reported is None and row[kind] is None;values[day]=None;continue
                ordinal,raw=source;assert reported['source_row_index']==ordinal and reported['date']==day and raw['symbol']==symbol
                for field,value in reported['values'].items():
                    original_value=raw.get(field)
                    assert value is None if original_value is None else Decimal(value)==Decimal(str(original_value))
                    n_source_fields+=1
                original_value=raw.get(price_field);values[day]=rational(original_value) if original_value is not None else None
                assert row[kind] is None if values[day] is None else rational(row[kind])==values[day]
            price_maps[symbol][kind]=values
            returns(instrument['returns'][kind],dates,values);n_windows+=4
        matched=sorted(set(maps['light'])&set(maps['full']))
        different=[d for d in matched if rational(maps['light'][d][1]['price'])!=rational(maps['full'][d][1]['close'])]
        assert instrument['legacy_light_reconciliation']['matched_dates']==len(matched)
        assert instrument['legacy_light_reconciliation']['different_close_dates']==different
    ratio_points=0
    for kind,actual in output['ratios'].items():
        left=price_maps['SPY'][kind];right=price_maps['GLD'][kind];dates=sorted(set(left)|set(right));values={}
        assert [r['date'] for r in actual['history']]==dates and actual['latest']==actual['history'][-1]
        for row in actual['history']:
            day=row['date'];a=left.get(day);b=right.get(day);values[day]=a/b if a is not None and b is not None else None
            if values[day] is None:assert row['value_decimal'] is None
            else:
                assert rational(row['numerator_decimal'])==a and rational(row['denominator_decimal'])==b
                with localcontext() as ctx:
                    ctx.prec=60
                    assert Decimal(row['value_decimal'])==Decimal(values[day].numerator)/Decimal(values[day].denominator)
                ratio_points+=1
        returns(actual['returns'],dates,values);n_windows+=4
        for n in (50,200):
            selected=dates[-n:];ready=len(selected)==n and all(values[d] is not None for d in selected)
            value=sum((values[d] for d in selected),Fraction(0))/n if ready else None
            check(actual['moving_averages'][str(n)],value,selected,n)
        selected=dates[-252:];z=None
        if len(selected)==252 and all(values[d] is not None for d in selected):
            refs=[values[d] for d in selected[:-1]];mean=sum(refs,Fraction(0))/len(refs)
            variance=sum(((v-mean)**2 for v in refs),Fraction(0))/(len(refs)-1)
            if variance:
                with localcontext() as ctx:
                    ctx.prec=100;delta=values[selected[-1]]-mean
                    z=(Decimal(delta.numerator)/Decimal(delta.denominator))/(Decimal(variance.numerator)/Decimal(variance.denominator)).sqrt()
        check(actual['zscore'],z,selected,252)
        recent=dates[-6:];direction=None
        if len(recent)==6 and all(values[d] is not None for d in recent):
            signs=[(values[b]>values[a])-(values[b]<values[a]) for a,b in zip(recent,recent[1:])]
            direction=signs[0] if len(set(signs))==1 else 0
        assert actual['persistence']['direction']==direction and actual['persistence']['observation_dates']==recent
    assert output['trade_tickets']==[] and output['n_tickets']==0 and output['independent_investment_votes']==0
    assert output['state'] is None and output['signal_strength'] is None and all(v is None for v in output['current_metrics'].values())
    assert all(output[k] is False for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'))
    return {'instruments':len(output['instruments']),'source_endpoints':len(inputs['captures']),'original_price_rows':n_source_rows,
        'original_numeric_fields':n_source_fields,'matched_ratio_points':ratio_points,'return_windows':n_windows,
        'ratio_moving_averages':4,'ratio_zscores':2,'provider_original_arithmetic_checked':True}
