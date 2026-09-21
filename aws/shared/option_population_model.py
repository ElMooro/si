"""Exact captured option-population coefficients, with no inferred ownership.

This pure model consumes the retained canonical option record graph. It never
collects prices, estimates dealer positions or assigns investment authority.
"""
from collections import Counter,defaultdict
from decimal import Decimal,localcontext
import json,re
import option_flow_research as upstream
import option_flow_store as evidence
import option_contract_research as contracts
import option_research_rows as codec

CONTRACT='option-population-research.v1'
PREFIX='data/option-population-research/'
MAX_ARTIFACT=16*1024*1024
GROUP_BLOCK_ROWS=100
UNDERLYINGS=('SPY','QQQ','IWM','NVDA','TSLA','AAPL','META','AMZN','MSFT','GOOGL')
PERMISSIONS=upstream.PERMISSIONS
FIELDS={'reported_open_interest':('contracts','open_interest'),
    'gamma_oi_shares':('shares_per_USD_underlying_move','vendor_gamma'),
    'delta_oi_shares':('equivalent_underlying_shares','vendor_delta')}
UNITS={'open_interest':'contracts','shares_per_contract':'shares_per_contract',
    'vendor_gamma':'per_USD_underlying_price',
    'vendor_delta':'option_price_change_per_underlying_price_change'}


def text(value):return '0' if value==0 else format(value,'f')


def numeric(cell,unit):
    if not isinstance(cell,dict) or cell.get('unit')!=unit:raise ValueError('Exact source unit required')
    if cell.get('state') not in ('reported','reported_zero'):
        if cell.get('value') is not None:raise ValueError('Unqualified value must remain null')
        return None
    value=cell.get('value')
    if not isinstance(value,str) or len(value)>260 or not re.fullmatch(r'-?\d+(?:\.\d+)?',value):
        raise ValueError('Exact qualified numeric text required')
    number=contracts.decimal(Decimal(value))
    if (number==0)!=(cell['state']=='reported_zero'):raise ValueError('Source zero state differs')
    return number


def row_values(row):
    """Require the exact declared standard contract; missing fields stay local."""
    if not row.get('identity_eligible'):return None
    if row.get('contract_type') not in ('call','put') or any(row.get(k) is not False for k in PERMISSIONS):
        raise ValueError('Qualified descriptive row required')
    m=row['metrics'];mult=numeric(m.get('shares_per_contract'),UNITS['shares_per_contract'])
    if mult!=100:raise ValueError('Upstream standard deliverable contract differs')
    oi=numeric(m.get('open_interest'),UNITS['open_interest'])
    if oi is not None and (oi<0 or oi!=oi.to_integral_value()):raise ValueError('Upstream OI domain differs')
    out={'reported_open_interest':oi}
    with localcontext() as ctx:
        ctx.prec=180
        for name,lower,upper in (('gamma',Decimal(0),None),
                ('delta',Decimal(-1) if row['contract_type']=='put' else Decimal(0),
                    Decimal(0) if row['contract_type']=='put' else Decimal(1))):
            metric=numeric(m.get('vendor_'+name),UNITS['vendor_'+name])
            if metric is not None and (metric<lower or upper is not None and metric>upper):
                raise ValueError('Upstream Greek domain differs')
            out[name+'_oi_shares']=None if metric is None or oi is None else metric*oi*mult
    return out


def aggregate(rows):
    counts=Counter(returned_rows=0,identity_eligible_rows=0,call_identity_rows=0,put_identity_rows=0)
    identity_reasons=Counter();values=defaultdict(list);exclusions=defaultdict(Counter)
    with localcontext() as ctx:
        ctx.prec=180
        for row in rows:
            kind=row.get('contract_type');counts['returned_rows']+=1
            if not row.get('identity_eligible'):
                identity_reasons.update(row.get('identity_reasons') or ['identity_unqualified'])
                continue
            counts['identity_eligible_rows']+=1;counts[str(kind)+'_identity_rows']+=1
            terms=row_values(row)
            for field,value in terms.items():
                if value is not None:values[kind,field].append(value)
                else:
                    oi=row['metrics']['open_interest']
                    reason='open_interest:'+oi['state'] if oi['value'] is None else FIELDS[field][1]+':'+row['metrics'][FIELDS[field][1]]['state']
                    exclusions[kind,field][reason]+=1
        out={}
        for kind in ('call','put'):
            out[kind]={}
            for field,(unit,_) in FIELDS.items():
                included=values[kind,field];population=counts[kind+'_identity_rows']
                out[kind][field]={'value':text(sum(included,Decimal(0))) if included else None,'unit':unit,
                    'included_rows':len(included),'identity_population_rows':population,'excluded_rows':population-len(included),
                    'exclusion_reasons':dict(sorted(exclusions[kind,field].items())),
                    'complete_field_coverage':population>0 and len(included)==population,
                    'observation_time':None,'meaning':'Captured standard-contract population; common observation time and ownership unknown'}
    counts['identity_excluded_rows']=counts['returned_rows']-counts['identity_eligible_rows']
    return {'counts':dict(counts),'identity_exclusion_reasons':dict(sorted(identity_reasons.items())),'sides':out}


def verified_packet(raw,read):
    packet=json.loads(raw)
    if (packet.get('contract')!=upstream.CONTRACT or any(packet.get(k) is not False for k in PERMISSIONS)
            or packet.get('call') is not None or packet.get('independent_investment_votes')!=0):
        raise ValueError('Typed canonical option research required')
    run=evidence.verified_run(packet['replay'],read)
    retained=upstream.checked(run['output'],read,'outputs')
    if {k:v for k,v in packet.items() if k!='replay'}!=retained:raise ValueError('Canonical current body differs from retained output')
    return packet


def chain(packet,symbol,read):
    verified_packet(upstream.encoded(packet),read)
    if symbol not in UNDERLYINGS:raise ValueError('Declared GEX continuity universe required')
    item=packet['chains'][symbol];summary=upstream.checked(item['chain'],read,'chains');rows=[];seen=set()
    if (summary.get('underlying')!=symbol or summary.get('contract')!='option-research-chain.v1'
            or summary.get('coverage')!=item['coverage']):raise ValueError('Underlying chain differs')
    for part in summary['record_blocks']:
        block=upstream.checked(part['artifact'],read,'rows');restored=codec.unpack(block)
        if (len(restored)!=part['rows'] or block['expanded_sha256']!=part['expanded_sha256']
                or block['source']['page']!=part['source_page']):raise ValueError('Record block inventory differs')
        for row in restored:
            position=(row['evidence']['page'],row['evidence']['row_index'])
            if row.get('underlying')!=symbol or position in seen:raise ValueError('Duplicated or mismatched source row')
            seen.add(position);rows.append(row)
    if len(rows)!=summary['coverage']['returned_rows']:raise ValueError('Captured row coverage differs')
    groups=defaultdict(list)
    for row in rows:
        if row.get('identity_eligible'):
            strike=row['metrics']['strike']['value']
            strike=strike.rstrip('0').rstrip('.') if '.' in strike else strike
            groups[row['expiration_date'],strike].append(row)
    return {'contract':CONTRACT,'underlying':symbol,'source_run':packet['replay'],'source_chain':item['chain'],
        'source_capture_completed_at':packet['generated_at'],'record_blocks':summary['record_blocks'],
        'capture_status':summary['research_status'],'source_coverage':summary['coverage'],
        'totals':aggregate(rows),
        'by_expiry_strike':[{'expiration_date':expiry,'strike_usd_per_share':strike,**aggregate(group)}
            for (expiry,strike),group in sorted(groups.items(),key=lambda pair:(pair[0][0],Decimal(pair[0][1])))],
        'formulas':{'reported_open_interest':'sum(reported open interest)',
            'gamma_oi_shares':'sum(vendor gamma * reported open interest * reported shares per contract)',
            'delta_oi_shares':'sum(vendor delta * reported open interest * reported shares per contract)'},
        'observed_dealer_inventory':None,'zero_gamma_flip':None,'dealer_hedging_flow':None,
        'call':None,'score':None,'portfolio_action':'WAIT','independent_investment_votes':0,**PERMISSIONS,
        'model_scope':'Separate call/put captured-population coefficients. No inferred ownership sign, live observation claim or fixed-Greek zero-gamma flip.'}


def reference(raw,kind):
    if kind not in ('groups','chains'):raise ValueError('Reviewed coefficient artifact kind required')
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX_ARTIFACT:raise ValueError('Bounded coefficient artifact required')
    digest=upstream.sha(raw)
    return {'key':PREFIX+kind+'/'+digest+'.json','sha256':digest,'bytes':len(raw)}


def checked(ref,kind,read):
    if not isinstance(ref,dict) or not isinstance(ref.get('key'),str) or not re.fullmatch(
            re.escape(PREFIX+kind+'/')+r'[a-f0-9]{64}\.json',ref['key']):raise ValueError('Reviewed coefficient reference required')
    raw=read(ref['key'])
    if reference(raw,kind)!=ref:raise ValueError('Coefficient artifact bytes differ')
    return json.loads(raw)


def deliver(output,emit):
    """Keep every strike/expiry group while bounding each downloadable object."""
    if output.get('contract')!=CONTRACT or any(output.get(k) is not False for k in PERMISSIONS):
        raise ValueError('Descriptive coefficient output required')
    groups=output['by_expiry_strike'];blocks=[]
    for offset in range(0,len(groups),GROUP_BLOCK_ROWS):
        block={'contract':'option-population-groups.v1','underlying':output['underlying'],
            'source_run':output['source_run'],'source_chain':output['source_chain'],
            'offset':offset,'groups':groups[offset:offset+GROUP_BLOCK_ROWS]}
        raw=upstream.encoded(block);ref=reference(raw,'groups');emit(ref['key'],raw)
        blocks.append({'artifact':ref,'offset':offset,'groups':len(block['groups'])})
    summary={k:v for k,v in output.items() if k!='by_expiry_strike'}
    summary.update(group_blocks=blocks,group_count=len(groups),expanded_sha256=upstream.sha(upstream.encoded(output)))
    raw=upstream.encoded(summary);ref=reference(raw,'chains');emit(ref['key'],raw)
    return ref


def restore(ref,read):
    summary=checked(ref,'chains',read);groups=[]
    if summary.get('contract')!=CONTRACT or any(summary.get(k) is not False for k in PERMISSIONS):
        raise ValueError('Descriptive coefficient summary required')
    for item in summary['group_blocks']:
        block=checked(item['artifact'],'groups',read)
        if (block.get('contract')!='option-population-groups.v1' or block.get('underlying')!=summary['underlying']
                or block.get('source_run')!=summary['source_run'] or block.get('source_chain')!=summary['source_chain']
                or block.get('offset')!=len(groups) or item['offset']!=len(groups)
                or not 0<len(block['groups'])==item['groups']<=GROUP_BLOCK_ROWS):
            raise ValueError('Coefficient block inventory differs')
        groups.extend(block['groups'])
    if len(groups)!=summary['group_count']:raise ValueError('Coefficient group coverage differs')
    output={k:v for k,v in summary.items() if k not in ('group_blocks','group_count','expanded_sha256')}
    output['by_expiry_strike']=groups
    if upstream.sha(upstream.encoded(output))!=summary['expanded_sha256']:raise ValueError('Expanded coefficient output differs')
    return output
