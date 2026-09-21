"""Typed price-volume context cannot become net-capital-flow or allocation input."""
from datetime import datetime,timedelta,timezone
import hashlib,json,re
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}


def context(packet,at=None):
    absent={'available':False,'reason':'verified_price_volume_research_unavailable',**PERMISSIONS}
    try:
        if not isinstance(packet,dict) or packet.get('contract')!='money-volume-research.v1' or any(packet.get(k) is not False for k in PERMISSIONS):return absent
        if packet.get('call') is not None or packet.get('portfolio_action')!='WAIT' or packet.get('quality',{}).get('independent_investment_votes')!=0:return absent
        def clock(s):
            value=datetime.fromisoformat(s.replace('Z','+00:00'))
            if value.tzinfo is None:raise ValueError('Aware clock required')
            return value
        at=at or datetime.now(timezone.utc);source=clock(packet['source_generated_at']);generated=clock(packet['generated_at']);due=clock(packet['source_valid_until'])
        if not source<=generated<=at<due<=source+timedelta(hours=26):return absent
        ref=packet['replay']
        if not re.fullmatch(r'data/money-volume-research/runs/[a-f0-9]{64}\.json',ref.get('manifest_key','')):return absent
        body={k:v for k,v in packet.items() if k!='replay'}
        if hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()!=ref.get('output_sha256'):return absent
        return {**absent,'available':True,'reason':'dated_price_volume_proxy_only','generated_at':packet['generated_at'],
            'reference_date':packet.get('as_of'),'source_valid_until':packet['source_valid_until'],'replay':ref,
            'note':'Return-times-turnover is not net capital flow, institutional order direction, a forecast or independent confirmation.'}
    except (KeyError,ValueError,TypeError,AttributeError,ArithmeticError):return absent


def decision_view(packet):
    return {'research_context':context(packet),'sectors':[],'institutional_sector_tilt':[],'stocks_in':[],'stocks_out':[],
        'industries_in':[],'industries_out':[],'call':None,'portfolio_action':'WAIT',**PERMISSIONS}
