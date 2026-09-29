"""Whole TreasuryDirect calendar response and deterministic, dated selection.

Current acquisition evidence only. No prediction, first-release availability,
provider-universe completeness, network access or portfolio authority.
"""
from datetime import date, datetime, timedelta
from copy import deepcopy
import auction_benchmarks
from auction_originals import encoded, sha, clock, strict_json

CONTRACT = 'treasury-upcoming-measurements.v1'
REPLAY_CONTRACT = 'treasury-upcoming-replay.v1'
PREFIX = 'data/auction-calendar-originals/'
URL = 'https://www.treasurydirect.gov/TA_WS/securities/upcoming?format=json'
MAX_BYTES = 4 * 1024 * 1024
MAX_ROWS = 5000
PERMISSIONS = dict.fromkeys(('historical_point_in_time_verified', 'provider_universe_complete',
                            'forecast_eligible', 'calls_eligible', 'sizing_eligible', 'execution_eligible'), False)
ALIASES = {'auctionDate':'auction_date', 'issueDate':'issue_date', 'securityType':'security_type',
           'securityTerm':'security_term', 'offeringAmount':'offering_amount',
           'floatingRate':'floating_rate', 'tips':'inflation_index_security',
           'originalSecurityTerm':'original_security_term'}


def source_day(value):
    """Provider calendar field, not an availability timestamp or time-zone conversion."""
    if not isinstance(value, str): return None
    try:
        if len(value) == 10:
            parsed = date.fromisoformat(value)
            return parsed if parsed.isoformat() == value else None
        if len(value) <= 35 and len(value) > 10 and value[10] == 'T':
            return datetime.fromisoformat(value.replace('Z', '+00:00')).date()
    except ValueError:
        pass
    return None


def normalize(record, index, today, end):
    """Keep every original occurrence, including duplicates and rejected records."""
    entry = {'source_row_index': index, 'provider_record': deepcopy(record), 'issues': [], 'selected': None}
    if not isinstance(record, dict):
        return {**entry, 'status':'invalid_record'}
    values = dict(record)
    for camel, snake in ALIASES.items():
        if camel in record and snake in record and encoded(record[camel]) != encoded(record[snake]):
            values[camel] = None
            entry['issues'].append('conflicting_alias:'+camel)
        elif camel not in record:
            values[camel] = record.get(snake)
    auctioned = source_day(values.get('auctionDate'))
    if auctioned is None:
        return {**entry, 'status':'invalid_auction_date'}
    if not today <= auctioned <= end:
        return {**entry, 'status':'outside_window'}
    issued = source_day(values.get('issueDate'))
    if issued is None or issued < auctioned:
        issued = None
        entry['issues'].append('invalid_issue_date')
    amount = auction_benchmarks.number(values.get('offeringAmount'))
    if amount is None or amount < 0:
        amount = None
        entry['issues'].append('invalid_offering_amount')
    identity = auction_benchmarks.calendar_identity(values)
    if identity['instrument_classification_status'] != 'verified':
        entry['issues'].append('unverified_instrument')
    selected = {'auction_date':auctioned.isoformat(), 'issue_date':issued.isoformat() if issued else None,
                'security_type':values.get('securityType'), 'security_term':values.get('securityTerm'),
                'offering_amount_billions':amount/1e9 if amount is not None else None,
                'cusip':record.get('cusip'), 'days_ahead':(auctioned-today).days,
                **identity, 'source_row_index':index, 'provider_record':deepcopy(record),
                'calendar_field_issues':entry['issues'][:], 'source_capture_verified':False}
    return {**entry, 'status':'selected', 'selected':selected}


def build(frame, as_of, days_ahead, generated_at):
    generated = clock(generated_at)
    today = date.fromisoformat(as_of)
    if today.isoformat() != as_of or today != generated.date():
        raise ValueError('Calendar selection must use the publication UTC date')
    if type(days_ahead) is not int or not 0 <= days_ahead <= 30:
        raise ValueError('Calendar window exceeds reviewed bounds')
    if not isinstance(frame, dict) or frame.get('request_url') != URL or frame.get('response_url') != URL:
        raise ValueError('Calendar request or final response identity differs')
    if type(frame.get('http_status')) is not int or frame['http_status'] != 200:
        raise ValueError('Complete HTTP 200 response required')
    started, acquired = clock(frame['started_at']), clock(frame['acquired_at'])
    if not started <= acquired <= generated or (generated-started).total_seconds() > 180:
        raise ValueError('Calendar acquisition clocks invalid or too old')
    if started.date() != today:
        raise ValueError('Calendar request crosses the selection date')
    raw = frame.get('raw')
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('Whole calendar response outside byte bound')
    declared_length = frame.get('content_length')
    if declared_length is not None and (type(declared_length) is not int or declared_length != len(raw)):
        raise ValueError('Calendar response is shorter or longer than Content-Length')
    rows = strict_json(raw)
    if not isinstance(rows, list) or len(rows) > MAX_ROWS:
        raise ValueError('Whole calendar must be a bounded array')
    end = today+timedelta(days=days_ahead)
    observations = [normalize(row, index, today, end) for index, row in enumerate(rows)]
    selected = sorted((row['selected'] for row in observations if row['status']=='selected'),
                      key=lambda row:(row['auction_date'], row['source_row_index']))
    invalid = sum(row['status'] in ('invalid_record','invalid_auction_date') for row in observations)
    coverage = {'response_complete':True, 'records_received':len(rows), 'selected':len(selected),
                'outside_window':sum(row['status']=='outside_window' for row in observations),
                'invalid_records':invalid, 'selected_with_field_issues':sum(bool(row['calendar_field_issues']) for row in selected),
                'duplicate_occurrences':len(rows)-len({sha(encoded(row)) for row in rows}),
                'all_occurrences_retained':True, 'selection_complete':invalid==0, 'provider_universe_complete':False}
    return {'contract':CONTRACT, 'generated_at':generated_at, 'status':'partial' if invalid else 'complete',
            'request_window':{'start':as_of,'end':end.isoformat(),'days_ahead':days_ahead},
            'acquisition':{key:value for key,value in frame.items() if key!='raw'},
            'response_sha256':sha(raw), 'response_bytes':len(raw), 'coverage':coverage,
            'observations':observations, 'selected_auctions':selected, **PERMISSIONS,
            'limitations':['Every received occurrence is retained; duplicate occurrences are not independent evidence.',
                          'Complete response transport and deterministic selection do not prove the provider listed every upcoming auction.',
                          'Provider calendar dates are not first-publication clocks. Size, flags and dates are not forecasts.']}


def unavailable(reason):
    return {'contract':REPLAY_CONTRACT, 'status':'unavailable', 'reason':reason,
            'original_bytes_replayed':False, 'selection_replayed':False, **PERMISSIONS}
