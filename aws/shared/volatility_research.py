"""Typed volatility-index context; no return forecast or hedge-cost authority."""
from datetime import date,datetime,timezone
from decimal import Decimal
import hashlib,json,math,re

CONTRACT='volatility-native-research.v1'
PERMISSIONS={'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False}
FRED={'VIX_30D':'VIXCLS','VIX_3M':'VXVCLS','NDX_VOL':'VXNCLS','RUT_VOL':'RVXCLS','DJIA_VOL':'VXDCLS',
    'EEM_VOL':'VXEEMCLS','EWZ_VOL':'VXEWZCLS','OIL_VOL':'OVXCLS','GOLD_VOL':'GVZCLS',
    'AAPL_VOL':'VXAPLCLS','GOOG_VOL':'VXGOGCLS','AMZN_VOL':'VXAZNCLS','GS_VOL':'VXGSCLS','IBM_VOL':'VXIBMCLS'}


def context(packet,at=None):
    absent={'available':False,'reason':'verified_current_volatility_unavailable','measurements':{},'tenor_comparison':None,**PERMISSIONS}
    try:
        if not isinstance(packet,dict) or packet.get('contract')!=CONTRACT or packet.get('call') is not None:return absent
        if any(packet.get(k) is not False for k in PERMISSIONS):return absent
        at=at or datetime.now(timezone.utc)
        generated=datetime.fromisoformat(packet['generated_at'].replace('Z','+00:00'))
        deadline=datetime.fromisoformat(packet['freshness']['pipeline_check_due_at'].replace('Z','+00:00'))
        if generated.tzinfo is None or deadline.tzinfo is None or not generated<=at<deadline or (at-generated).total_seconds()>36*3600:return absent
        ref=packet['replay']
        if not re.fullmatch(r'data/volatility-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']):return absent
        raw=json.dumps({k:v for k,v in packet.items() if k!='replay'},sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
        if hashlib.sha256(raw).hexdigest()!=ref['output_sha256']:return absent
        sources={}
        for source in packet['source_evidence']:
            label,kind,digest=source['label'],source['kind'],source['sha256']
            if not re.fullmatch('[a-f0-9]{64}',digest) or source['key']!='audit-private/20260909-originals/volatility-research/'+digest+'.bin':return absent
            if label in FRED:
                if source['series_id']!=FRED[label] or kind not in ('definition','observations') or source['provider']!='fred':return absent
                expected='https://api.stlouisfed.org/fred/'+('series?' if kind=='definition' else 'series/observations?')+'series_id='+FRED[label]+'&'
                if not source['request_url'].startswith(expected) or 'api_key' in source['request_url']:return absent
            elif label in ('VVIX','SKEW'):
                if source['series_id']!=label or kind!='observations' or source['provider']!='Cboe':return absent
                if source['request_url']!='https://cdn-api.cboe.com/api/global/us_indices/daily_prices/'+label+'_History.csv':return absent
            else:return absent
            if (label,kind) in sources:return absent
            clock=datetime.fromisoformat(source['acquired_at'].replace('Z','+00:00'))
            if clock.tzinfo is None or not generated.timestamp()-240<=clock.timestamp()<=generated.timestamp():return absent
            sources[(label,kind)]=source
        measurements={}
        for label,m in packet['measurements'].items():
            if m.get('label')!=label or m.get('quality',{}).get('status')!='within_age_ceiling':continue
            kinds=('definition','observations') if label in FRED else ('observations',)
            if any((label,k) not in sources for k in kinds):continue
            if not 0<=(at.date()-date.fromisoformat(m['observation_date'])).days<=5:continue
            value=m['value']
            if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value):continue
            exact=Decimal(m['exact']['value'])
            if not exact.is_finite() or value!=float(exact) or not 0<=exact<=10000:continue
            if m['unit']!='index_points' or m['source_unit']!='Index' or any(m.get(k) is not False for k in PERMISSIONS):continue
            measurements[label]={k:m[k] for k in ('label','series_id','definition','value','unit','observation_date','source_url','underlying','tenor_days','change_points','previous_observation_date','descriptive_statistics','exact')}
            measurements[label].update(PERMISSIONS)
        comparison=None;t=packet['tenor_comparison'];a=measurements.get('VIX_30D');b=measurements.get('VIX_3M')
        if a and b and t['current_comparison_available'] and a['observation_date']==b['observation_date']==t['observation_date']:
            expected=float(Decimal(a['exact']['value'])-Decimal(b['exact']['value']))
            if t['difference_points']==expected:
                comparison={k:t[k] for k in ('observation_date','difference_points','ratio','relation','interpretation')}
        return {**absent,'available':bool(measurements),'reason':'descriptive_volatility_only','measurements':measurements,
            'tenor_comparison':comparison,'generated_at':packet['generated_at'],'replay':ref,
            'interpretation':'Index measurements and same-date relationships. No qualified return forecast, tail probability, correlation estimate or position-specific hedge cost.'}
    except (KeyError,ValueError,TypeError,ArithmeticError,OverflowError):return absent


def qualified_signal(packet):
    # A descriptive packet cannot grant forecasting authority to itself.
    return None


def decision_view(packet):
    return {'research_context':context(packet),'regime':None,'composite_stress_score':None,
        'skew':{'value':None,'pctile_252d':None,'regime':None},'term_structure':{'inverted':None},**PERMISSIONS}


def describe(doc):
    if not doc.get('available'):return 'Verified current volatility-index measurements are unavailable; research abstains.'
    parts=[]
    for label,name in (('VIX_30D','30-day VIX'),('VIX_3M','3-month VIX'),('VVIX','VVIX'),('SKEW','SKEW')):
        m=doc['measurements'].get(label)
        if m:parts.append(f"{name} {m['value']:g} index points observed {m['observation_date']}; {m['source_url']}")
    return '; '.join(parts)+'. Descriptive indices; no qualified trade, hedge price or crash probability.'


def cost_context(packet,dealer=None,at=None):
    doc=context(packet,at);dealer=dealer if isinstance(dealer,dict) else {}
    raw_gamma=dealer.get('market_composite') or {}
    raw_gamma=raw_gamma if isinstance(raw_gamma,dict) else {}
    return {'volatility_research':doc,'skew_pctile_252d':None,'skew_regime':None,
        'vix_term_ratio_30d_3m':None,'term_inverted':None,'vol_surface_regime':None,
        'gamma_regime':raw_gamma.get('composite_regime'),'gamma_role':'unverified_context_only',
        'hedge_cost_read':'UNCLASSIFIED','note':'Dated Cboe index observations do not price a hedge. Contract identity, executable option prices, expiry, Greeks, costs and a reviewed payoff model are required.',**PERMISSIONS}


def risk_context(packet,dealer=None,at=None):
    out=cost_context(packet,dealer,at)
    raw=(dealer or {}).get('market_composite') if isinstance(dealer,dict) else {}
    raw=raw if isinstance(raw,dict) else {}
    return {**out,'spy_gamma_regime':raw.get('spy_regime'),'vol_surface_stress':None,
        'concentration_warning':None,
        'carry_justification':'Unclassified: volatility and gamma labels do not establish a crash probability or whether a hedge pays for its cost.',
        'note':'Use the portfolio holdings and a reviewed covariance/scenario model for concentration risk. Index levels do not establish safe diversification or justify hedge carry.'}
