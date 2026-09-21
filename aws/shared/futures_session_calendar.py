"""Reconcile original product session events without asserting final provider bars."""
from collections import defaultdict
from datetime import date,datetime,timezone,timedelta
from zoneinfo import ZoneInfo
TRADING_TIMEZONE=ZoneInfo('America/Chicago')


def instant(value):
    if not isinstance(value,str):raise ValueError('Aware event clock required')
    out=datetime.fromisoformat(value.replace('Z','+00:00'))
    if out.tzinfo is None:raise ValueError('Timezone required')
    return out.astimezone(timezone.utc)


def reconcile(rows,product,venue,start,end,complete):
    groups=defaultdict(lambda:defaultdict(list));invalid=[]
    for index,row in enumerate(rows):
        try:
            if not isinstance(row,dict) or row.get('product_code')!=product or row.get('trading_venue')!=venue:raise ValueError('Identity mismatch')
            session=row.get('session_end_date');day=date.fromisoformat(session)
            if session!=day.isoformat() or not start<=session<=end:raise ValueError('Session outside requested dates')
            event=row.get('event')
            if event not in ('pre_open','open','close'):raise ValueError('Unreviewed event')
            stamp=instant(row.get('timestamp'))
            if not day-timedelta(days=7)<=stamp.date()<=day+timedelta(days=1):raise ValueError('Event outside bounded session window')
            groups[session][(event,stamp.isoformat())].append(index)
        except (ValueError,TypeError,OverflowError):invalid.append(index)
    sessions={};repeats=0
    for session,events in sorted(groups.items()):
        opens=sorted(stamp for event,stamp in events if event=='open')
        closes=sorted(stamp for event,stamp in events if event=='close')
        eligible=bool(complete and not invalid and len(closes)==1 and opens
            and instant(closes[0]).astimezone(TRADING_TIMEZONE).date().isoformat()==session
            and all(instant(x)<instant(closes[0]) for x in opens))
        grouped=[{'event':event,'timestamp_utc':stamp,'source_row_ordinals':indices} for (event,stamp),indices in sorted(events.items())]
        repeats+=sum(len(indices)-1 for indices in events.values())
        sessions[session]={'session_end_date':session,'event_identities':grouped,'unique_open_times':opens,'unique_close_times':closes,
            'scheduled_close_qualified':eligible,'scheduled_close_utc':closes[0] if eligible else None,
            'reason':None if eligible else 'incomplete_scope_or_ambiguous_session_events',
            'bar_finality_independently_verified':False}
    return {'product_code':product,'venue':venue,'dataset':product+':schedules','pagination_complete':complete,
        'exchange_calendar_timezone':'America/Chicago',
        'returned_rows':len(rows),'invalid_source_row_ordinals':invalid,'repeated_event_identity_rows':repeats,
        'sessions':sessions,'qualified_session_closes':sum(s['scheduled_close_qualified'] for s in sessions.values()),
        'meaning':'A matched scheduled end is a calendar fact, not proof that all trades or provider revisions have arrived.'}


def status(calendar,session,acquired_at):
    record=calendar['sessions'].get(session,{})
    close=record.get('scheduled_close_utc') if record.get('scheduled_close_qualified') else None
    ended=instant(close)<=instant(acquired_at) if close is not None else None
    return {'calendar_dataset':calendar['dataset'],'session_end_date':session,'scheduled_close_utc':close,
        'scheduled_session_ended_by_capture':ended,'status':'scheduled_session_ended' if ended is True else
            'session_still_scheduled_open' if ended is False else 'session_calendar_unqualified',
        'bar_finality_independently_verified':False,'source_capture_at':acquired_at}
