"""Independent decimal checks against every privately retained original row."""
from decimal import Decimal,localcontext
from datetime import date
import math


def verify(full,originals):
    checked=0;source_rows=0;values={};dates={}
    for sid,source in originals.items():
        rows=source['observations'];source_rows+=len(rows)
        pairs=[(r['date'],Decimal(r['value'])) for r in rows if r['value']!='.']
        if len(pairs)!=len(full['histories'][sid]):raise ValueError('Whole numeric source population differs')
        for (day,value),actual in zip(pairs,full['histories'][sid]):
            if actual[0]!=day or Decimal(str(actual[1]))!=value:raise ValueError('Original numeric row differs')
            checked+=1
        values[sid]=dict(pairs);dates[sid]=[p[0] for p in pairs]
    def close(actual,expected,tolerance='0.00000001'):
        nonlocal checked
        if type(actual) not in (int,float) or not math.isfinite(actual) or abs(Decimal(str(actual))-expected)>Decimal(tolerance):
            raise ValueError('Independent original arithmetic differs')
        checked+=1
    nominal=values['DGS10'];ordered=dates['DGS10'];latest=nominal[ordered[-1]]
    if full['quality']['status']=='fresh':
        close(full['level'],latest)
        close(full['distance_to_5pct_bps'],(Decimal(5)-latest)*100,'0.051')
        population=[v for day,v in nominal.items() if day>='1990-01-01']
        if population:close(full['pct_rank_since_1990'],Decimal(sum(v<=latest for v in population))*100/len(population),'0.051')
    for n in (20,60):
        row=full['velocity']['comparisons'][str(n)+'_observations']
        if len(ordered)>=n+1:
            start,end=ordered[-n-1],ordered[-1]
            if (row['start_date'],row['end_date'])!=(start,end):raise ValueError('Yield comparison endpoints differ')
            close(row['value'],(nominal[end]-nominal[start])*100,'0.051')
    prices=values['SP500'];price_dates=dates['SP500'];indices={d:i for i,d in enumerate(price_dates)}
    valid_returns=0;episodes=0
    with localcontext() as arithmetic:
        arithmetic.prec=40
        for cohort in full['episode_study'].values():
            if cohort['n']!=len(cohort['episodes']):raise ValueError('Episode population truncated')
            episodes+=len(cohort['episodes'])
            for episode in cohort['episodes']:
                close(episode['y'],nominal[episode['date']],'0.0051')
                for label,n in (('1w',5),('1m',21),('3m',63)):
                    trace=episode['return_inputs'][label]
                    if trace is None:
                        if episode['spx_'+label] is not None:raise ValueError('Return lacks original endpoints')
                        continue
                    start,end=trace['start_date'],trace['end_date']
                    if indices[end]-indices[start]!=n:raise ValueError('Price horizon differs from original observation count')
                    if trace['elapsed_calendar_days']!=(date.fromisoformat(end)-date.fromisoformat(start)).days:raise ValueError('Price calendar span differs')
                    close(trace['start_value'],prices[start]);close(trace['end_value'],prices[end])
                    close(episode['spx_'+label],(prices[end]/prices[start]-1)*100,'0.0051');valid_returns+=1
        pairs=full['correlation_trace']['observations'];rs=[];ys=[]
        for row in pairs:
            start,end=row['start_date'],row['end_date']
            close(row['start_price'],prices[start]);close(row['end_price'],prices[end])
            close(row['start_yield_percent'],nominal[start]);close(row['end_yield_percent'],nominal[end])
            r=prices[end]/prices[start]-1;y=nominal[end]-nominal[start]
            close(row['price_return_fraction'],r);close(row['yield_change_percentage_points'],y)
            rs.append(r);ys.append(y)
        if full['corr60_spx_vs_dy'] is not None:
            if len(rs)<20:raise ValueError('Correlation lacks minimum pairs')
            mr=sum(rs)/len(rs);my=sum(ys)/len(ys)
            cov=sum((r-mr)*(y-my) for r,y in zip(rs,ys))
            denominator=(sum((r-mr)**2 for r in rs)*sum((y-my)**2 for y in ys)).sqrt()
            close(full['corr60_spx_vs_dy'],cov/denominator,'0.00051')
    return {'original_rows':source_rows,'independent_scalar_checks':checked,'complete_episode_rows':episodes,
        'valid_original_price_returns':valid_returns,'matched_correlation_pairs':len(pairs),
        'arithmetic_verified':True,'forecast_qualified':False,'point_in_time_backtest_qualified':False}
