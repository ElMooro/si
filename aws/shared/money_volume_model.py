"""Matched-session price and turnover research. No inference of investor net flows."""
from collections import defaultdict
from datetime import datetime,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import json,re
import money_volume_sessions as native
import money_volume_source as source

CONTRACT='money-volume-research.v1'
PREFIX='data/money-volume-research/'
CURRENT='data/money-flow-state.json'
PRIVATE='audit-private/20260909-originals/sector-flow-research/'
PERMISSIONS={k:False for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')}
encoded=native.encoded;sha=native.sha;clock=native.stamp
SECTOR_ALIASES={'Basic Materials':'Materials','Financial Services':'Financials','Consumer Cyclical':'Consumer Discretionary',
    'Consumer Defensive':'Consumer Staples','Healthcare':'Health Care','Technology':'Technology','Energy':'Energy',
    'Industrials':'Industrials','Utilities':'Utilities','Real Estate':'Real Estate','Communication Services':'Communication Services'}


def numeric(value):
    if value is None:return None
    return float(value.quantize(Decimal('0.000001')))


def exact(value):return None if value is None else native.decimal_text(value)


def universe_rows(packet):
    if not isinstance(packet,dict) or not isinstance(packet.get('stocks'),list):raise ValueError('Reviewed universe shape required')
    rows=packet['stocks'];seen=set();result=[]
    if not 1<=len(rows)<=15000:raise ValueError('Universe size outside bound')
    for index,row in enumerate(rows):
        ticker=row.get('symbol')
        if not isinstance(ticker,str) or not re.fullmatch(r'[A-Za-z0-9.\-]{1,20}',ticker) or ticker in seen:raise ValueError('Missing or duplicate universe identity')
        seen.add(ticker)
        sector=row.get('sector');industry=row.get('industry');name=row.get('name')
        if any(v is not None and (not isinstance(v,str) or len(v)>300) for v in (sector,industry,name)):raise ValueError('Invalid universe label')
        result.append({'ticker':ticker,'name':name,'sector':SECTOR_ALIASES.get(sector),'source_sector':sector,
            'industry':industry or None,'universe_row_index':index})
    return result


def build(packet,universe,at,read,context,refs):
    with localcontext() as ctx:
        ctx.prec=34;ctx.rounding=ROUND_HALF_EVEN
        return _build(packet,universe,at,read,context,refs)


def _build(packet,universe,at,read,context,refs):
    plan=source.prepare(packet,read);stamp=clock(at);source_stamp=clock(plan['generated_at']);u_stamp=clock(universe['generated_at'])
    if not source_stamp<=stamp or u_stamp>stamp:raise ValueError('Future source publication')
    configured=universe_rows(universe);symbols={r['ticker'] for r in configured};history={t:[] for t in symbols};source_days={}
    for day in plan['days']:
        rows,meta=source.restore_day(plan,day,read);source_days[day]=meta
        for ticker in sorted(symbols):history[ticker].append(rows.get(ticker) if rows is not None else None)
    last_close=datetime.combine(datetime.fromisoformat(plan['days'][-1]).date()+timedelta(days=1),datetime.min.time(),native.EASTERN).astimezone(timezone.utc)
    acquisitions=[clock(v['acquired_at']) for v in source_days.values() if v['status']=='original_replayed']
    due=min([source_stamp+timedelta(hours=26),u_stamp+timedelta(days=7),last_close+timedelta(hours=96),*[x+timedelta(hours=26) for x in acquisitions]])
    fresh=stamp<due;rows=[];decimals={}
    for member in configured:
        ticker=member['ticker'];observations=history[ticker]
        missing=[day for day,row in zip(plan['days'],observations) if row is None]
        missing_turnover=[day for day,row in zip(plan['days'],observations) if row is not None and row['turnover'] is None]
        complete=not missing;turnover_complete=complete and not missing_turnover
        start=observations[0];end=observations[-1]
        ret=end['close']/start['close']-1 if complete else None
        avg=sum((r['turnover'] for r in observations),Decimal(0))/6 if turnover_complete else None
        pressure=ret*avg if ret is not None and avg is not None else None
        up=down=flat=None
        if turnover_complete:
            up=down=flat=Decimal(0)
            for before,after in zip(observations,observations[1:]):
                if after['close']>before['close']:up+=after['turnover']
                elif after['close']<before['close']:down+=after['turnover']
                else:flat+=after['turnover']
        reasons=[]
        if missing:reasons.append('missing_expected_session')
        if missing_turnover:reasons.append('missing_positive_volume_vwap')
        if not fresh:reasons.append('source_check_due')
        measurements={'price_return_pct':ret*100 if ret is not None else None,'mean_session_turnover_usd_proxy':avg,
            'price_volume_pressure_usd_proxy':pressure,'up_close_turnover_usd_proxy':up,
            'down_close_turnover_usd_proxy':down,'unchanged_close_turnover_usd_proxy':flat}
        decimals[ticker]=measurements
        rows.append({**member,**{k:numeric(v) for k,v in measurements.items()},'exact':{k:exact(v) for k,v in measurements.items()},
            'start_close':numeric(start['close']) if start else None,'end_close':numeric(end['close']) if end else None,
            'source_row_indices':[r['source_row_index'] if r else None for r in observations],
            'available_sessions':6-len(missing),'missing_sessions':missing,'missing_turnover_sessions':missing_turnover,
            'quality':{'status':'available' if not reasons else 'unavailable' if missing or missing_turnover else 'source_check_due','reasons':reasons},
            'calculation_complete':pressure is not None,'within_age_ceiling':fresh})
    rows.sort(key=lambda r:r['ticker'])
    def aggregate(members,label,sector=None):
        available=[r for r in members if decimals[r['ticker']]['price_volume_pressure_usd_proxy'] is not None]
        included_symbols={r['ticker'] for r in available}
        sums={k:sum((decimals[r['ticker']][k] for r in available),Decimal(0)) if available else None for k in ('price_volume_pressure_usd_proxy','mean_session_turnover_usd_proxy','up_close_turnover_usd_proxy','down_close_turnover_usd_proxy','unchanged_close_turnover_usd_proxy')}
        denominator=sums['mean_session_turnover_usd_proxy'];pressure=sums['price_volume_pressure_usd_proxy']
        intensity=pressure/denominator*10000 if denominator is not None and denominator>0 else None
        ranked=sorted(available,key=lambda r:(-decimals[r['ticker']]['mean_session_turnover_usd_proxy'],r['ticker']))
        largest=sum((decimals[r['ticker']]['mean_session_turnover_usd_proxy'] for r in ranked[:5]),Decimal(0))/denominator*100 if denominator is not None and denominator>0 else None
        return {'label':label,'sector':sector,'configured_count':len(members),'included_count':len(available),
            'included_tickers':[r['ticker'] for r in available],'excluded_tickers':[r['ticker'] for r in members if r['ticker'] not in included_symbols],
            'coverage_pct':numeric(Decimal(len(available))/len(members)*100) if members else None,
            'scope':'covered_constituent_subtotal','complete_universe':len(available)==len(members),
            **{k:numeric(v) for k,v in sums.items()},'exact':{k:exact(v) for k,v in sums.items()},
            'pressure_intensity_bps':numeric(intensity),'top_five_turnover_share_pct':numeric(largest),
            'top_five_turnover_tickers':[r['ticker'] for r in ranked[:5]],'within_age_ceiling':fresh}
    sectors=defaultdict(list);industries=defaultdict(list)
    for row in rows:
        sectors[row['sector'] or 'Unclassified'].append(row)
        industries[(row['sector'] or 'Unclassified',row['industry'] or 'Unclassified')].append(row)
    sector_rows=[aggregate(members,label,label) for label,members in sorted(sectors.items())]
    industry_rows=[aggregate(members,label,sector) for (sector,label),members in sorted(industries.items())]
    total=aggregate(rows,'Configured universe')
    included=sum(r['included_count'] for r in sector_rows)
    if included!=total['included_count']:raise ValueError('Sector population does not reconcile')
    for key in total['exact']:
        parts=[Decimal(r['exact'][key]) for r in sector_rows if r['exact'][key] is not None]
        whole=Decimal(total['exact'][key]) if total['exact'][key] is not None else None
        if parts and abs(sum(parts,Decimal(0))-whole)>Decimal('0.000001'):raise ValueError('Sector subtotals do not reconcile')
    quality={'status':'covered_sample' if fresh and total['included_count'] else 'source_check_due' if not fresh else 'no_complete_constituents',
        'configured_stocks':len(rows),'complete_stocks':total['included_count'],'excluded_stocks':len(rows)-total['included_count'],
        'source_sessions':len(source_days),'replayed_sessions':len(acquisitions),'independent_investment_votes':0,
        'historical_security_identity_verified':False,'historical_classification_verified':False,'first_availability_verified':False}
    contexts=[{'source_key':key,'available':value is not None,'generated_at':(value or {}).get('generated_at') or (value or {}).get('as_of'),
        'snapshot_sha256':refs[key]['sha256'] if refs.get(key) else None,'role':'retained_context_not_calculation_input'} for key,value in sorted(context.items())]
    return {'contract':CONTRACT,'engine':'justhodl-money-flow-state','version':'2.0.0','generated_at':at,
        'source_generated_at':plan['generated_at'],'source_valid_until':due.isoformat(),'as_of':plan['days'][-1],
        'period':{'start_date':plan['days'][0],'end_date':plan['days'][-1],'session_dates':plan['days'],'price_intervals':5,'turnover_sessions':6},
        'source_replay':plan['replay'],'source_evidence':source_days,'universe':{'generated_at':universe['generated_at'],
            'snapshot_sha256':refs['data/universe.json']['sha256'],'count':len(rows),'classification':'Retained current vendor labels mapped through an explicit alias table; not an independently licensed or historical GICS security master.','sector_aliases':SECTOR_ALIASES},
        'stocks':rows,'sector_measurements':sector_rows,'industry_measurements':industry_rows,'covered_universe':total,'quality':quality,
        'field_units':{'price_return_pct':'percent_price_return','start_close':'USD_per_share_split_adjusted','end_close':'USD_per_share_split_adjusted',
            'price_volume_pressure_usd_proxy':'USD_proxy_not_net_capital_flow','mean_session_turnover_usd_proxy':'USD_per_session_turnover_proxy',
            'up_close_turnover_usd_proxy':'USD_turnover_proxy','down_close_turnover_usd_proxy':'USD_turnover_proxy',
            'unchanged_close_turnover_usd_proxy':'USD_turnover_proxy','pressure_intensity_bps':'basis_points_of_turnover_weighted_past_price_return',
            'coverage_pct':'percent_of_configured_constituents','top_five_turnover_share_pct':'percent_of_covered_sample_mean_turnover'},
        'definitions':{'price_return_pct':'100 × (last close / first close − 1), requiring all six expected sessions; split-adjusted price return, no dividends.',
            'mean_session_turnover_usd_proxy':'Mean of reported VWAP × volume over six exact sessions; zero volume contributes zero without inventing VWAP. This is a split-adjusted turnover proxy, not independently reconciled cash turnover.',
            'price_volume_pressure_usd_proxy':'Five-interval decimal price return × mean six-session turnover proxy. Signed price-volume statistic; no buyer identity, order direction or investor net capital flow.',
            'up_down_unchanged':'Later-session turnover assigned by the change in consecutive closes. Equal closes have their own bucket. The first session supplies the starting price and is excluded from these five-interval buckets.',
            'aggregate':'Unrounded constituent values summed only across the disclosed covered common-period sample. Missing constituents are excluded with named coverage; the subtotal is not whole-market capital flow.',
            'pressure_intensity_bps':'10,000 × sum(pressure proxy) / sum(mean turnover proxy), a turnover-weighted past price return. No cross-sectional z-score or confidence interpretation.',
            'institutional':'13F source retained separately. Reported holdings and their changes do not establish contemporaneous trade flow or independent confirmation.'},
        'methodology_sources':['https://massive.com/docs/rest/stocks/aggregates/daily-market-summary','https://www.sec.gov/divisions/investment/13ffaq'],
        'dependency_graph':{'roots':['MASSIVE:GROUPED_DAILY','CURRENT:CONFIGURED_UNIVERSE'],
            'shared_roots_with':['market-internals','sector-rotation'],'independent_votes':0},
        'context_inventory':contexts,'portfolio_action':'WAIT','call':None,**PERMISSIONS,
        'portfolio_consequences':{'role':'User-entered exposure can be compared with retained price return and observed average turnover. No trade size, liquidation horizon or execution capacity is prescribed.'},
        'compatibility':{'retired_ambiguous_fields':['flow_usd','net_flow_usd','stocks_in','stocks_out','institutional_sector_tilt'],
            'predecessor':'Whole previous packets and the complete old handler are retained; a proxy is not copied into an actual-capital-flow key.'}}
