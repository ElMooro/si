"""Reproduce whole donor observations without manufacturing a scarcity forecast."""
from datetime import datetime,timezone
from decimal import Decimal
import hashlib,json,math

CONTRACT='scarcity-donor-observations.v1'
SOURCES={
 'data/supply-inflection.json':('Supply proxy observations',('signals','by_theme')),
 'data/bottleneck-boom.json':('Bottleneck research',('ranks',)),
 'data/chokepoint.json':('Company research',('cheap_chokepoint_book','hidden_chokepoint_book','highest_conviction_book','all_chokepoints')),
 'data/supply-chain-graph.json':('Reported supply-chain relationships',('supply_chain_laggards',)),
 'data/narrative-vs-tape.json':('Narrative and price-screen context',('quiet_accumulation',)),
 'data/themes-detected.json':('ETF theme observations',('themes',)),
 'data/inventory-drawdown.json':('Inventory observations',('sector_drawdown','stock_drawdown_board','boom_setups')),
}
PRIVATE='audit-private/20260909-originals/scarcity-donor-observations/'
MAX=4*1024*1024


def sha(raw):return hashlib.sha256(raw).hexdigest()


def strict(raw):
    def pairs(values):
        out={}
        for key,value in values:
            if key in out:raise ValueError('Duplicate original JSON member')
            out[key]=value
        return out
    def real(value):
        out=float(value)
        if not math.isfinite(out) or (out==0 and Decimal(value)!=0):raise ValueError('Unrepresentable original number')
        return out
    def bad(_):raise ValueError('Nonfinite original number')
    return json.loads(raw.decode('utf-8'),parse_float=real,parse_constant=bad,object_pairs_hook=pairs)


def clock(value):
    try:
        if not isinstance(value,str):return None
        stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
        return stamp.astimezone(timezone.utc) if stamp.tzinfo else None
    except ValueError:return None


def pointer(value):return str(value).replace('~','~0').replace('/','~1')


def reference(raw):return {'key':PRIVATE+sha(raw)+'.bin','sha256':sha(raw),'bytes':len(raw)}


def validate_capture(key,capture,raw):
    if key not in SOURCES:raise ValueError('Undeclared donor')
    if not isinstance(raw,bytes) or len(raw)>MAX or capture.get('original')!=reference(raw):raise ValueError('Whole retained donor differs')
    if clock(capture.get('received_at')) is None or not capture.get('etag'):raise ValueError('Source object identity required')
    parsed=strict(raw)
    if not isinstance(parsed,dict):raise ValueError('Whole donor object required')
    return parsed


def observations(key,packet,ref):
    """Every occurrence in every consumed subset; absent fields stay absent in raw."""
    if key not in SOURCES or not isinstance(packet,dict):raise ValueError('Declared whole donor required')
    out=[];populations=[]
    for field in SOURCES[key][1]:
        values=packet.get(field)
        if values is None:
            populations.append({'field':field,'status':'not_provided','occurrences':None});continue
        if isinstance(values,list):items=list(enumerate(values))
        elif isinstance(values,dict) and field=='by_theme':items=list(values.items())
        else:
            populations.append({'field':field,'status':'invalid_population_type','occurrences':None});continue
        populations.append({'field':field,'status':'received','occurrences':len(items)})
        for index,raw in items:
            row=raw if isinstance(raw,dict) else {}
            path='/'+pointer(field)+'/'+pointer(index)
            ticker=row.get('ticker') if isinstance(row.get('ticker'),str) else None
            symbol=row.get('symbol') if isinstance(row.get('symbol'),str) else None
            theme=(str(index) if field=='by_theme' else row.get('theme_etf',row.get('etf',row.get('theme'))))
            # Preserve source prose verbatim. A screen does not become buying, a shortage or an UP call.
            out.append({'occurrence_id':sha((ref['sha256']+'\n'+key+'\n'+path).encode()),'source_key':key,
                        'source_pointer':path,'source_original':ref,'ticker':ticker,'declared_symbol':symbol,
                        'declared_theme':theme,'raw':raw,'source_meaning_verified':False,
                        'independence_verified':False,'call':None,'calls_eligible':False,'sizing_eligible':False})
    return out,populations


def compile_packet(captures,originals,generated_at):
    now=clock(generated_at)
    if now is None or set(captures)!=set(SOURCES):raise ValueError('Complete declared donor inventory and clock required')
    if set(originals)!={key for key,c in captures.items() if c.get('status') in ('retained','invalid_original')}:
        raise ValueError('Original inventory differs from acquisition statuses')
    source_rows=[];occurrences=[];received_inputs=0
    for key,(label,_) in SOURCES.items():
        capture=captures[key];status=capture.get('status');entry={'key':key,'label':label,'status':status,
             'capture':capture,'producer_generated_at':None,'source_period_freshness':'unverified','model_qualification':'unverified',
             'independence_verified':False,'may_vote':False,'populations':[]}
        if status=='retained':
            packet=validate_capture(key,capture,originals[key])
            received=clock(capture['received_at'])
            if received>now:raise ValueError('Source receipt after publication')
            producer=packet.get('generated_at',packet.get('as_of'))
            stamp=clock(producer);entry['producer_generated_at']=producer
            entry['producer_clock_status']='unknown' if stamp is None else 'future' if stamp>received else 'reported'
            entry['upstream_calls_eligible']=packet.get('calls_eligible')
            entry['upstream_quality_status']=packet.get('quality',{}).get('status') if isinstance(packet.get('quality'),dict) else None
            rows,populations=observations(key,packet,capture['original']);occurrences.extend(rows);entry['populations']=populations
            received_inputs+=1
        elif status=='invalid_original':
            raw=originals[key]
            if capture.get('original')!=reference(raw) or len(raw)>MAX or clock(capture.get('received_at')) is None:
                raise ValueError('Invalid donor original identity differs')
            if clock(capture['received_at'])>now:raise ValueError('Invalid original receipt after publication')
            try:
                parsed=strict(raw)
                if isinstance(parsed,dict):raise RuntimeError('Valid donor incorrectly declared invalid')
            except (ValueError,UnicodeError):pass
        elif status not in ('missing','unavailable','size_bound','time_bound'):
            raise ValueError('Explicit donor acquisition status required')
        source_rows.append(entry)
    if not received_inputs:raise ValueError('No complete donor received; preserve previous publication')
    tickers=sorted({r['ticker'] for r in occurrences if r['ticker']})
    return {'engine':'scarcity-radar','version':'1.2.0','measurement_contract':CONTRACT,'generated_at':generated_at,
            'status':'RESEARCH_ONLY','source_inventory':source_rows,'donor_occurrences':occurrences,
            'snapshot_atomic':False,'call':None,'calls_eligible':False,'forecast_qualified':False,'sizing_eligible':False,'execution_eligible':False,
            'signals_logged':0,'notifications_sent':0,'independent_votes':None,'source_count':received_inputs,
            'declared_source_count':len(SOURCES),'occurrence_count':len(occurrences),'received_tickers':tickers,
            'vertical_tightness':[],'stealth_shortage_board':[],'prime_setups':[],
            'counts':{'names':len(tickers),'prime':None,'candidates':None,'verticals_tightening':None},
            'quality':{'status':'partial','reason':'Donor timing, units, independent ancestry and forecast qualification remain unverified.'},
            'method':'Whole donor observations with exact original identities and source coordinates. No score averaging or keyword category assignment.',
            'caveats':['Repeated ticker or theme occurrences are retained, not counted as independent evidence.',
                       'ETF price changes and producer scores do not establish physical supply shortages.',
                       'Reported producer generation clocks are not original observation dates or first-release vintages.',
                       'Narrative absence and unqualified price screens do not establish institutional buying.',
                       'Complete donor originals are retained by identity; they are not recursively embedded into downstream packets.',
                       'Complete donor replay does not validate upstream measurements, original-provider histories or portfolio consequences.']}
