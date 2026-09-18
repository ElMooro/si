"""Deterministic forward price observations from a registered research record.

SPY observed sessions are the declared measurement clock, not an assertion of
an independently verified exchange calendar. Dividends, costs and fills are
not present and cannot be reported as performance or portfolio PnL.
"""
from datetime import datetime, timedelta, timezone
import json

from instrument_identity import resolve_instrument
from outcome_price_evidence import EASTERN, finite, validate_daily_mark
from prospective_journal import validate_record, stamp

CONTRACT='forward-price-measurement.v1'


def marks(packet, symbol, now):
    raw, receipt = packet['raw'], packet['evidence']
    received=stamp(receipt.get('first_received_at'))
    if received is None or received>now: raise ValueError('price source was not yet available at evaluation')
    doc=json.loads(raw)
    identity=resolve_instrument(symbol,'equity')
    if not identity or not isinstance(doc,dict) or not isinstance(doc.get('results',[]),list):
        raise ValueError('invalid price response')
    if doc.get('ticker')!=symbol or doc.get('adjusted') is not True or doc.get('status') not in ('OK','DELAYED') or doc.get('next_url'):
        raise ValueError('incomplete or incompatible price response')
    result={}
    for bar in doc.get('results',[]):
        ts=finite(bar.get('t')) if isinstance(bar,dict) else None
        if ts is None: raise ValueError('bar has no observation period')
        start=datetime.fromtimestamp(ts/1000,timezone.utc).astimezone(EASTERN)
        end=(start+timedelta(days=1)).astimezone(timezone.utc)
        # Current/future daily periods are not final; do not consume them.
        if end>now: continue
        date=start.date().isoformat()
        if date in result: raise ValueError('duplicate daily session')
        mark={'symbol':symbol,'instrument_id':identity['instrument_id'],'currency':'USD',
              'provider':'polygon','price':finite(bar.get('c')),'bar_timestamp_ms':ts,
              'as_of':date,'observed_at':end.isoformat(),
              'observation_time_basis':'aggregate_period_end_not_trade_time',
              'adjustment_basis':'split_adjusted_price','adjustment_vintage':receipt['sha256'],
              'evidence_parser':'polygon-us-daily-close.v1','evidence_sha256':receipt['sha256'],'evidence':receipt}
        errors=validate_daily_mark(mark,receipt,raw)
        if errors: raise ValueError(errors[0])
        result[date]=mark
    return result


def evaluate(record, horizon, asset_packet, benchmark_packet, now, verify_mark):
    validate_record(record)
    if horizon not in record['protocol']['horizons_sessions'] or isinstance(horizon,bool):
        raise ValueError('unregistered horizon')
    if now.tzinfo is None or now<stamp(record['registered_at']): raise ValueError('invalid evaluation clock')
    base={'contract':CONTRACT,'forecast_id':record['forecast_id'],'horizon_sessions':horizon,
          'measured_at':now.isoformat(),'direction':record['observation']['direction'],
          'symbol':record['observation']['instrument']['symbol'],
          'sizing_eligible':False,'promotion_eligible':False,'out_of_sample_validation':False,
          'net_return_pct':None,'portfolio_pnl':None,'total_return_pct':None,
          'calendar_basis':'SPY observed completed daily periods; exchange calendar completeness unverified'}
    if asset_packet is None or benchmark_packet is None:
        return {**base,'status':'PENDING_SOURCE','reason':'retained price response unavailable'}
    bm=marks(benchmark_packet,'SPY',now)
    sessions=sorted(date for date in bm if date>record['registration_date_et'])
    if len(sessions)<=horizon:
        return {**base,'status':'PENDING_FORWARD_WINDOW','completed_sessions':len(sessions),
                'required_sessions_including_entry':horizon+1}
    entry_date,exit_date=sessions[0],sessions[horizon]
    # A truncated request could otherwise move entry to a conveniently later bar.
    start_request=benchmark_packet['evidence']['source_url'].split('/range/1/day/')[-1].split('/')[0]
    next_date=(datetime.fromisoformat(record['registration_date_et'])+timedelta(days=1)).date().isoformat()
    if start_request != next_date: raise ValueError('benchmark request does not cover the registered entry window')
    asset=marks(asset_packet,base['symbol'],now)
    if entry_date not in asset or exit_date not in asset:
        return {**base,'status':'MISSING_MATCHING_ENDPOINT','entry_session':entry_date,'exit_session':exit_date,
                'reason':'asset endpoint missing; the registered window was not moved'}
    selected={'entry_asset':asset[entry_date],'exit_asset':asset[exit_date],
              'entry_benchmark':bm[entry_date],'exit_benchmark':bm[exit_date]}
    if verify_mark is None: raise ValueError('original source archive verification required')
    for mark in selected.values():
        errors=verify_mark(mark)
        if errors: raise ValueError('retained price evidence rejected: '+errors[0])
    asset_return=(selected['exit_asset']['price']/selected['entry_asset']['price']-1)*100
    benchmark_return=(selected['exit_benchmark']['price']/selected['entry_benchmark']['price']-1)*100
    sign=1 if base['direction']=='UP' else -1
    signed=sign*asset_return
    return {**base,'status':'MEASURED_PRICE_ONLY','entry_session':entry_date,'exit_session':exit_date,
            'entry_marks':{'asset':selected['entry_asset'],'benchmark':selected['entry_benchmark']},
            'exit_marks':{'asset':selected['exit_asset'],'benchmark':selected['exit_benchmark']},
            'asset_price_return_pct':asset_return,'benchmark_price_return_pct':benchmark_return,
            'asset_minus_benchmark_pct':asset_return-benchmark_return,
            'direction_adjusted_price_move_pct':signed,'direction_hit':None if signed==0 else signed>0,
            'meaning':'Descriptive split-adjusted price observation. No executable fill, borrow, dividends, net return, independent sample or allocation authority.'}
