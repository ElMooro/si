"""Bounded descriptive consumer view; no numeric-key discovery or extra countries."""
from datetime import date,datetime,timezone
from decimal import Decimal
from math import isfinite
from research_brief_model import clock,digest


def context(packet,now=None):
    now=now or datetime.now(timezone.utc)
    result={'status':'unavailable','components_usd_bn':{},'three_bank_total_usd_bn':None,
            'calls_eligible':False,'sizing_eligible':False,'independent_votes':0,
            'scope':'Fed, Eurosystem and Bank of Japan subtotal only. China M2 is not PBOC assets.'}
    try:
        if not isinstance(packet,dict) or packet.get('contract')!='global-liquidity-research.v1':return result
        for field in ('generated_at','source_generated_at'):
            age=(now-clock(packet[field])).total_seconds()
            if not 0<=age<=26*3600:return result
        if digest({k:v for k,v in packet.items() if k!='replay'})!=packet['replay']['output_sha256']:return result
        current=packet['three_bank_subtotal']
        if current['status']!='descriptive' or current['unit']!='USD_millions' or current['missing_components']:return result
        if current['valuation_date']!=clock(packet['generated_at']).date().isoformat():return result
        if set(current['components'])!={'WALCL','ECBASSETSW','JPNASSETS'}:return result
        for sid,component in current['components'].items():
            for selected in (component['balance'],component.get('fx')):
                if selected is None:continue
                age=(now.date()-date.fromisoformat(selected['selected']['effective_observation_date'])).days
                if selected['status']!='descriptive' or not 0<=age<=selected['max_carry_age_days']:return result
            for identity in (sid,component.get('fx_series_id')):
                if identity is None:continue
                native=packet['series'][identity]
                if native.get('available') is not True or not 0<=(now-clock(native['acquired_at'])).total_seconds()<=26*3600:return result
        amounts={sid:Decimal(component['usd_millions_decimal']) for sid,component in current['components'].items()}
        total=Decimal(current['total_usd_millions_decimal'])
        if any(not v.is_finite() or v<0 or not isfinite(float(v)) for v in (*amounts.values(),total)) or sum(amounts.values())!=total:return result
        labels={'WALCL':'Fed','ECBASSETSW':'Eurosystem','JPNASSETS':'BOJ'}
        result.update(status='descriptive',components_usd_bn={labels[sid]:float(v/1000) for sid,v in amounts.items()},
            three_bank_total_usd_bn=float(total/1000),valuation_date=current['valuation_date'],
            generated_at=packet['generated_at'],source_replay=packet['replay'],call=None)
    except (KeyError,ValueError,TypeError,ArithmeticError,OverflowError):
        return result
    return result
