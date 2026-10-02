"""Typed legacy event inputs; validation grants no price/forecast qualification."""
from copy import deepcopy
from decimal import Decimal
import math,re

CONTRACT='signal-event-inputs.v1'
DIRECTIONS={'UP':'UP','DOWN':'DOWN','NEUTRAL':'NEUTRAL','OUTPERFORM':'OUTPERFORM','UNDERPERFORM':'UNDERPERFORM','BULLISH':'UP','BEARISH':'DOWN'}
SYMBOL=re.compile(r'[A-Z0-9.\-\^=]{1,10}')


def number(value):
    if type(value) not in (int,float,Decimal):
        raise ValueError('Typed numeric event value required')
    if type(value) is float and not math.isfinite(value):
        raise ValueError('Finite event value required')
    out=Decimal(str(value))
    if not out.is_finite():
        raise ValueError('Finite event value required')
    return out


def exact(value):
    if type(value) in (float,Decimal):return number(value)
    if value is None or type(value) in (str,bool,int):return value
    if isinstance(value,(list,tuple)):return [exact(item) for item in value]
    if isinstance(value,dict):
        if not all(type(key) is str for key in value):raise ValueError('String metadata keys required')
        return {key:exact(item) for key,item in value.items()}
    raise ValueError('JSON-shaped event metadata required')


def prepare(signal_type,ticker,direction,windows,baseline_price,confidence,metadata,benchmark):
    if type(signal_type) is not str or not signal_type or signal_type!=signal_type.strip() or '#' in signal_type:
        raise ValueError('Unambiguous event family required')
    if type(ticker) is not str or SYMBOL.fullmatch(ticker) is None:
        raise ValueError('Explicit ticker required')
    if benchmark is not None and (type(benchmark) is not str or SYMBOL.fullmatch(benchmark) is None):
        raise ValueError('Explicit benchmark symbol required')
    if type(direction) is not str or direction.upper() not in DIRECTIONS:
        raise ValueError('Recognized explicit direction required')
    canonical=DIRECTIONS[direction.upper()]
    if canonical in ('OUTPERFORM','UNDERPERFORM') and benchmark is None:
        raise ValueError('Relative direction requires a benchmark')
    if not isinstance(windows,(list,tuple)) or not windows:
        raise ValueError('Explicit nonempty window collection required')
    days=[]
    for item in windows:
        if type(item) is int:day=item
        elif type(item) is str and re.fullmatch(r'[1-9][0-9]*',item):day=int(item)
        else:raise ValueError('Positive whole calendar-day windows required')
        # Legacy event rows expire one year after logging. Do not accept a
        # declared evaluation at/after that expiry as a gradeable pending event.
        if not 0<day<365 or day in days:raise ValueError('Distinct windows before legacy expiry required')
        days.append(day)
    price=number(baseline_price);conf=number(confidence)
    if price<=0:raise ValueError('Positive baseline price required')
    if not 0<=conf<=1:raise ValueError('Confidence must be within the declared unit interval')
    if metadata is None:metadata={}
    if not isinstance(metadata,dict):raise ValueError('Metadata object required')
    md=exact(metadata)
    received={'direction':direction,'windows':deepcopy(windows),'confidence':conf,'baseline_price':price}
    contract={'contract':CONTRACT,'received':received,'window_unit':'calendar_day','canonical_direction':canonical,
              'confidence_calibrated':False,'entry_mark_qualified':False,'forecast_qualified':False,'sizing_eligible':False,
              'reason':'Type validation is not source-time, instrument, corporate-action or forecast qualification.'}
    if 'event_input_contract' in md:contract['caller_supplied_contract']=md.pop('event_input_contract')
    md['event_input_contract']=contract
    return {'direction':canonical,'windows':days,'baseline_price':price,'confidence':conf,'metadata':md}
