"""Pure reported stablecoin stock observations, not cash flow or trade signals."""
from base64 import b64encode
from decimal import Decimal,InvalidOperation,localcontext,ROUND_HALF_EVEN
import hashlib,json,math,re

CONTRACT='crypto-reported-stablecoin-stock.v1'
DENIED={'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'forecast_qualified':False,'independent_investment_votes':0}
SNAPSHOTS={'current':'circulating','reported_previous_day':'circulatingPrevDay',
           'reported_previous_week':'circulatingPrevWeek','reported_previous_month':'circulatingPrevMonth'}
UNIT='provider_current_price_valued_circulating_stock_usd'
LIMIT=16*1024*1024
MAX_ROWS=5000

def pairs(items):
    out={}
    for key,value in items:
        if key in out:raise ValueError('Duplicate JSON member')
        out[key]=value
    return out


def context(packet):
    """Whole-original descriptive projection for consumers; no network or vote."""
    result={'contract':'crypto-stablecoin-consumer-context.v1','status':'unavailable',
            'reported_rows':None,'identified_rows':None,'unresolved_rows':None,
            'acquisition_completed_at':None,'source_response_sha256':None,
            'cash_flow_verified':False,'source_observation_clocks_available':False,
            'source_authenticity_verified':False,'point_in_time_qualified':False,
            'note':'Reported current-price-valued stock snapshots only; flow, mint/burn, observation dates and source freshness are unqualified.',**DENIED}
    try:
        from crypto_stablecoin_transport import project
        from types import SimpleNamespace
        if not isinstance(packet,dict):return result
        model=SimpleNamespace(LIMIT=LIMIT,CONTRACT=CONTRACT,DENIED=DENIED,build=build)
        expected=project(packet.get('source_attempt'),model)
        if set(packet)!=set(expected)|{'original_capture'}:return result
        if json.dumps(expected,sort_keys=True,allow_nan=False)!=json.dumps({k:packet[k] for k in expected},sort_keys=True,allow_nan=False):return result
        proof=packet['original_capture']
        if not isinstance(proof,dict) or proof.get('contract')!='crypto-stablecoin-original-replay.v1' or proof.get('complete_capture_replayed') is not True:return result
        for key in ('source_qualified','point_in_time_qualified','investment_authority'):
            if proof.get(key) is not False:return result
        for name,kind in (('manifest','runs'),('capture','captures')):
            ref=proof.get(name)
            if not isinstance(ref,dict) or type(ref.get('bytes')) is not int or not 0<ref['bytes']<=64*1024*1024:return result
            if not isinstance(ref.get('sha256'),str) or not re.fullmatch('[a-f0-9]{64}',ref['sha256']):return result
            if ref.get('key')!='data/crypto-stablecoin-research/'+kind+'/'+ref['sha256']+'.json':return result
        canonical=(json.dumps(expected,sort_keys=True,ensure_ascii=True,allow_nan=False,separators=(',',':'))+'\n').encode()
        if proof['capture']['sha256']!=hashlib.sha256(canonical).hexdigest() or proof['capture']['bytes']!=len(canonical):return result
        if expected['status']!='descriptive':return result
        return {**result,'status':'descriptive','reported_rows':expected['reported_rows'],
                'identified_rows':expected['identified_rows'],'unresolved_rows':expected['unresolved_rows'],
                'acquisition_completed_at':expected['source_attempt']['acquisition_completed_at'],
                'source_response_sha256':expected['source_attempt']['received_sha256'],
                'original_manifest_reference':proof['manifest'],'original_projection_checked':True,
                'stored_archive_read_verified':False}
    except (ValueError,TypeError,KeyError,UnicodeError,OverflowError,RecursionError):
        return result

def quantity(value):
    if not isinstance(value,Decimal) or not value.is_finite() or value<0:
        return {'status':'unavailable','value':None,'value_decimal':None,'unit':UNIT}
    number=float(value)
    if not math.isfinite(number) or Decimal(str(number))!=value:
        return {'status':'unavailable','value':None,'value_decimal':None,'unit':UNIT}
    return {'status':'reported','value':number,'value_decimal':str(value),'unit':UNIT}

def build(raw):
    if not isinstance(raw,bytes) or not 0<len(raw)<=LIMIT:
        raise ValueError('Whole bounded original response required')
    out={'contract':CONTRACT,'status':'unavailable','stablecoins':[],
         'original_response_base64':b64encode(raw).decode('ascii'),
         'original_response_bytes':len(raw),'original_response_sha256':hashlib.sha256(raw).hexdigest(),
         'total_mcap':None,'total_mcap_fmt':None,'minting_count':None,'burning_count':None,
         'stable_count':None,'net_signal':'UNAVAILABLE',
         'reported_rows':None,'identified_rows':0,'unresolved_rows':None,
         'source_observation_clocks_available':False,'observation_freshness_verified':False,
         'population_comparability_verified':False,'cash_flow_verified':False,
         'note':'Reported current-price-valued stocks and comparison snapshots; not mint/burn transactions, cash flows or dated historical market values.',**DENIED}
    try:
        doc=json.loads(raw.decode('utf-8'),parse_float=Decimal,parse_int=Decimal,object_pairs_hook=pairs,
                       parse_constant=lambda _ : (_ for _ in ()).throw(ValueError('Nonfinite JSON')))
        if not isinstance(doc,dict) or not isinstance(doc.get('peggedAssets'),list):
            raise ValueError('Whole pegged asset population required')
    except (ValueError,TypeError,UnicodeError,InvalidOperation,RecursionError):
        return {**out,'reason':'invalid_original_response'}
    source=doc['peggedAssets'];identities={}
    if len(source)>MAX_ROWS:
        return {**out,'reason':'complete_population_exceeds_reviewed_projection_budget',
                'source_rows_received':len(source),'projection_complete':False,
                'projection_row_limit':MAX_ROWS}
    for index,row in enumerate(source):
        ident=row.get('id') if isinstance(row,dict) else None
        if isinstance(ident,str) and re.fullmatch(r'[A-Za-z0-9_-]{1,100}',ident):
            identities.setdefault(ident,[]).append(index)
    for index,row in enumerate(source):
        entry={'source_row':'/peggedAssets/'+str(index),'status':'unresolved_identity',
               'id':None,'name':None,'symbol':None,'peg_type':None,'snapshots':{},'comparisons':{},**DENIED}
        if not isinstance(row,dict):out['stablecoins'].append(entry);continue
        ident=row.get('id');peg=row.get('pegType')
        for key in ('name','symbol'):
            entry[key]=row.get(key) if isinstance(row.get(key),str) else None
        if not isinstance(ident,str) or len(identities.get(ident,[]))!=1:
            out['stablecoins'].append(entry);continue
        entry.update(id=ident,peg_type=peg if isinstance(peg,str) else None)
        if not isinstance(peg,str) or not re.fullmatch(r'pegged[A-Za-z0-9]{1,20}',peg):
            out['stablecoins'].append(entry);continue
        entry['status']='reported_identity';out['identified_rows']+=1
        entry['provider_flags']={k:row.get(k) if type(row.get(k)) is bool else None for k in ('delisted','deprecated','yieldBearing')}
        for name,key in SNAPSHOTS.items():
            values=row.get(key);value=values.get(peg) if isinstance(values,dict) else None
            entry['snapshots'][name]={**quantity(value),'source_field':key+'/'+peg,'observation_at':None}
        current=entry['snapshots']['current']
        for label in list(SNAPSHOTS)[1:]:
            prior=entry['snapshots'][label]
            compare={'difference_usd_decimal':None,'percent_decimal':None,
                     'percent_numerator_usd_decimal':None,'percent_denominator_usd_decimal':None,
                     'percent_scale':100,'percent_precision_digits':34,'percent_rounding':'ROUND_HALF_EVEN',
                     'period_dates_verified':False,'cash_flow_verified':False,'reason':'missing_reported_quantity'}
            if current['value_decimal'] is not None and prior['value_decimal'] is not None:
                with localcontext() as ctx:
                    # Accepted operands round-trip through finite binary64:
                    # 1024 decimal digits cover their full exponent span.
                    ctx.prec=1024;ctx.rounding=ROUND_HALF_EVEN
                    now,then=Decimal(current['value_decimal']),Decimal(prior['value_decimal'])
                    difference=now-then
                    compare.update(difference_usd_decimal=str(difference),
                                   reason='reported_snapshot_arithmetic_only' if then else 'zero_comparison_denominator')
                    if then:
                        compare.update(percent_numerator_usd_decimal=str(difference),percent_denominator_usd_decimal=str(then))
                        ctx.prec=34
                        compare['percent_decimal']=str(difference/then*100)
            entry['comparisons'][label]=compare
        out['stablecoins'].append(entry)
    out.update(status='descriptive',reported_rows=len(source),unresolved_rows=len(source)-out['identified_rows'],
               reason='all_returned_rows_retained_without_global_flow_or_mint_burn_inference')
    return out
