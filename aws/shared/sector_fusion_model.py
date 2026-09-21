"""A typed sector evidence matrix; incomparable measurements are never averaged.

Returns use exactly the issuer window endpoints. Stock price-volume subtotals
retain their own dates and current universe. No allocator, forecast or trade
signal is inferred from matching signs, and no source gains an extra vote.
"""
from copy import deepcopy
from datetime import datetime,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import hashlib,json,math
from sector_research_catalog import SECTORS

CONTRACT='sector-fusion-research.v1'
CAPITAL_CONTRACT='sector-capital-research.v1'
PREFIX='data/sector-fusion-research/';CURRENT='data/sector-flow-state.json'
CAPITAL_PREFIX='data/sector-capital-research/';CAPITAL_CURRENT='data/sector-capital-fusion.json'
PRIVATE='audit-private/20260909-originals/sector-flow-research/'
PERMISSIONS={k:False for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')}


def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def clock(value):
    dt=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if dt.tzinfo is None:raise ValueError('Aware source clock required')
    return dt.astimezone(timezone.utc)
def number(value):
    if value is None:return None
    out=float(value)
    if not math.isfinite(out):raise ValueError('Finite measurement required')
    return 0.0 if out==0 else out
def dec(value):
    if isinstance(value,bool):raise ValueError('Boolean is not a measurement')
    out=Decimal(str(value))
    if not out.is_finite():raise ValueError('Finite decimal required')
    return out
def exact(value):return None if value is None else format(value,'f')


def price_window(observation,benchmark,window):
    a,b=window.get('start_date'),window.get('end_date')
    rows={r['date']:r for r in observation.get('history',[])};base={r['date']:r for r in benchmark.get('history',[])}
    positions=[('fund_start',rows.get(a)),('fund_end',rows.get(b)),('benchmark_start',base.get(a)),('benchmark_end',base.get(b))]
    missing=[label for label,row in positions if row is None]
    out={'status':'missing_exact_endpoint' if missing else 'available','start_date':a,'end_date':b,'missing_endpoints':missing,
        'price_return_pct':None,'benchmark_return_pct':None,'excess_percentage_points':None,'ratio_return_pct':None,
        'exact':{},'endpoints':{label:{k:row[k] for k in ('date','close','original_row_index')} if row else None for label,row in positions},
        'unit':'percent_price_return; excess in percentage_points','benchmark':'SPY','dividends_included':False,'exchange_calendar_verified':False}
    if missing:return out
    with localcontext() as ctx:
        ctx.prec=40;ctx.rounding=ROUND_HALF_EVEN
        closes=[dec(row['close']) for _,row in positions]
        if any(v<=0 for v in closes):raise ValueError('Positive matched endpoint required')
        x,y=closes[1]/closes[0],closes[3]/closes[2]
        values={'price_return_pct':(x-1)*100,'benchmark_return_pct':(y-1)*100,'excess_percentage_points':(x-y)*100,'ratio_return_pct':(x/y-1)*100}
        out.update({k:number(v) for k,v in values.items()});out['exact']={k:exact(v) for k,v in values.items()}
    return out


def sign_comparison(price,window,comparison):
    value=window.get('value_decimal');sensitivity=window.get('precision_sensitivity_decimal')
    result={'status':'unavailable','directional_inference_qualified':False,'independent_confirmation':False,
        'reason':'A dated price return and NAV-valued share change do not identify informed buying or future returns.'}
    if price['status']!='available' or value is None or sensitivity is None:return result
    if comparison.get('status')!='within_published_precision':return {**result,'status':'issuer_catalogue_conflict'}
    value,precision=dec(value),dec(sensitivity)
    if precision<0:raise ValueError('Nonnegative display sensitivity required')
    if abs(value)<=precision:return {**result,'status':'issuance_not_larger_than_display_sensitivity'}
    p=dec(price['exact']['price_return_pct'])
    if not p or not value:return {**result,'status':'zero_measurement'}
    return {**result,'status':'same_sign_descriptive' if (p>0)==(value>0) else 'different_sign_descriptive'}


def build(rotation,volume,issuer,at,contexts,refs):
    if rotation.get('contract')!='sector-native-research.v1' or volume.get('contract')!='money-volume-research.v1':raise ValueError('Native sector and volume sources required')
    if set(issuer['sectors'])!=set(SECTORS):raise ValueError('Exact sector issuer inventory required')
    for packet in (rotation,volume):
        if any(packet.get(k) is not False for k in PERMISSIONS) or packet.get('call') is not None:raise ValueError('Descriptive source authority required')
    stamp=clock(at);roots={'prices':rotation,'stock_volume':volume,'issuer':issuer}
    if any(clock(p['generated_at'])>stamp or clock(p['source_generated_at'])>clock(p['generated_at']) for p in roots.values()):raise ValueError('Future source publication')
    volume_rows={r['label']:r for r in volume['sector_measurements']}
    if len(volume_rows)!=len(volume['sector_measurements']):raise ValueError('Unique sector volume labels required')
    rows=[];all_due=[clock(rotation['source_valid_until']),clock(volume['source_valid_until'])]
    benchmark=rotation['observations']['SPY']
    for ticker,label in SECTORS.items():
        issue=issuer['sectors'][ticker];observation=rotation['observations'][ticker]
        windows={};lookup={r['date']:r for r in issue['history']}
        if issue.get('source_valid_until'):all_due.append(clock(issue['source_valid_until']))
        for horizon in ('1d','5d','20d'):
            window=deepcopy(issue['windows'].get(horizon,{'status':'issuer_source_unavailable','value_decimal':None}))
            price=price_window(observation,benchmark,window)
            start,end=window.get('start_date'),window.get('end_date')
            members=[deepcopy(r) for r in issue['history'] if start and end and start<=r['date']<=end]
            starting=lookup.get(start);assets=dec(starting['net_assets_decimal']) if starting and starting.get('net_assets_decimal') is not None else None
            value=dec(window['value_decimal']) if window.get('value_decimal') is not None else None
            with localcontext() as ctx:
                ctx.prec=40;ctx.rounding=ROUND_HALF_EVEN
                relative=value/assets*100 if value is not None and assets is not None and assets>0 else None
            window.update(unit='USD_nav_valued_share_change_estimate',value_usd=number(value),
                precision_sensitivity_usd=number(window.get('precision_sensitivity_decimal')),
                starting_reported_net_assets_decimal=exact(assets),issuance_as_pct_starting_assets=number(relative),
                issuance_as_pct_starting_assets_decimal=exact(relative),source_rows=members,
                definition='Sum each dated change in reported shares × that date’s issuer NAV. Not actual cash subscriptions, investor identity or secondary-market purchases.',
                precision_definition='Propagation of one last displayed unit; a sensitivity, not a confidence interval or guaranteed error bound.')
            windows[horizon]={'issuer':window,'price':price,'comparison':sign_comparison(price,window,issue.get('comparison',{}))}
        stock=deepcopy(volume_rows.get(label))
        rows.append({'symbol':ticker,'sector':label,'issuer_identity':deepcopy(issue['identity']),'windows':windows,
            'issuer_source':{k:deepcopy(issue.get(k)) for k in ('status','source','history_ref','latest_observation_date','acquired_at','source_valid_until','comparison')},
            'price_source':{k:deepcopy(observation.get(k)) for k in ('source','request','acquired_at','observed_at','source_valid_until','adjustment')},
            'stock_volume':{'period':deepcopy(volume['period']),'measurement':stock,'source_generated_at':volume['source_generated_at'],
                'source_valid_until':volume['source_valid_until'],'replay':deepcopy(volume['replay']),
                'universe_matches_ETF_holdings':False,'same_period_as_issuer_5d':all(volume['period'].get(k)==windows['5d']['issuer'].get(k) for k in ('start_date','end_date')),
                'scope':'Current vendor sector-labelled stocks, not the holdings of this ETF. Price-volume pressure is a proxy, not net investor capital.'},
            'independent_investment_votes':0,'call':None,'posture':None,'conviction':None,'net_score':None,**PERMISSIONS})
    source_clocks={k:{'generated_at':p['generated_at'],'source_generated_at':p['source_generated_at'],'replay':deepcopy(p['replay'])} for k,p in roots.items()}
    due=min(all_due);complete=sum(r['issuer_source']['status']=='original_replayed' and r['windows']['5d']['price']['status']=='available'
        and r['windows']['5d']['issuer']['status']=='complete_descriptive_estimate' and (r['issuer_source']['comparison'] or {}).get('status')=='within_published_precision' for r in rows)
    context_rows=[{'source_key':key,'status':'retained_context' if ref else 'missing','source_generated_at':(contexts.get(key) or {}).get('generated_at') or (contexts.get(key) or {}).get('as_of'),
        'snapshot_sha256':ref['sha256'] if ref else None,'bytes':ref['bytes'] if ref else None,'role':'Outside this calculation; no independent vote inferred.'} for key,ref in sorted(refs.items())]
    return {'contract':CONTRACT,'engine':'justhodl-sector-flow-state','version':'2.0.0','generated_at':at,
        'source_generated_at':min(clock(p['source_generated_at']) for p in roots.values()).isoformat(),'source_valid_until':due.isoformat(),
        'source_clocks':source_clocks,'sectors':rows,'n_sectors':len(rows),'quality':{'status':'source_check_due' if stamp>=due else 'complete_selected_sources' if complete==len(SECTORS) else 'partial_selected_sources',
            'configured_sectors':len(SECTORS),'issuer_price_five_window_available':complete,'independent_investment_votes':0,
            'historical_first_availability_verified':False,'historical_constituents_verified':False,'exchange_calendar_verified':False},
        'dependency_graph':{'roots':['ISSUER:StateStreet','ISSUER:iShares:IVV_REFERENCE_DATES','MARKET:ETF_DAILY_AGGREGATES','MARKET:GROUPED_STOCK_AGGREGATES','CURRENT:VENDOR_UNIVERSE'],
            'shared_roots_with':['sector-rotation','sector-tilt','market-internals','money-flow-state','etf-true-flows'],
            'independent_votes':0,'capital_fusion_role':'Alternate view of this evidence; not another independent source.'},
        'methodology':{'time_alignment':'ETF price and SPY endpoints use each issuer window’s exact dates. No nearest-date substitution. Stock-volume dates remain separate.',
            'measurement_separation':'Issuer NAV-valued share changes, price returns and stock turnover are separate measurements with separate units. They are not averaged, z-scored or counted as corroborating signals.',
            'classification':'Current ETF sector wrappers and vendor stock labels do not establish identical holdings or historical sector membership.',
            'vintage':'Current retrieved historical source vintages. Retained acquisition clocks do not prove historical first publication.',
            'portfolio':'Entered signed exposure × retained price return gives retrospective price P&L, before dividends, fees, tax, financing and execution costs. Issuance is not a forecast coefficient.'},
        'context_inventory':context_rows,'issuer_reference':{'source':issuer['reference_source'],'calendar':issuer['reference_calendar']},
        'overweight':[],'underweight':[],'top_inflow':[],'top_outflow':[],'divergences':[],
        'call':None,'portfolio_action':'WAIT',**PERMISSIONS}


def capital_projection(canonical,at,previous_ref=None):
    if canonical.get('contract')!=CONTRACT or any(canonical.get(k) is not False for k in PERMISSIONS):raise ValueError('Canonical evidence matrix required')
    if clock(canonical['generated_at'])>clock(at):raise ValueError('Future canonical publication')
    result=deepcopy({k:v for k,v in canonical.items() if k!='replay'})
    result.update(contract=CAPITAL_CONTRACT,engine='justhodl-sector-capital-fusion',generated_at=at,
        canonical_generated_at=canonical['generated_at'],canonical_replay=deepcopy(canonical['replay']),
        projection_scope='The same canonical sector evidence presented for capital research. No new measurement or independent vote.',
        predecessor_snapshot_sha256=previous_ref['sha256'] if previous_ref else None)
    if clock(at)>=clock(result['source_valid_until']):result['quality']['status']='source_check_due'
    return result
