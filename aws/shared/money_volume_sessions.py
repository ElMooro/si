"""Reviewed market-session parsing reused by native price-volume research.

Every window uses consecutive reviewed market sessions and one collection of
split-adjusted provider responses. Historical provider-symbol continuity is not
an independently reconstructed issuer/security master.
"""
from collections import deque
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
import hashlib
import json
from zoneinfo import ZoneInfo

CONTRACT = 'breadth-native-research.v1'
EASTERN = ZoneInfo('America/New_York')
MIN_DAY = date(2025, 9, 1)
MAX_DAY = date(2028, 12, 31)
CALENDAR_SOURCES = (
    'https://ir.theice.com/press/news-details/2024/NYSE-Group-Announces-2025-2026-and-2027-Holiday-and-Early-Closings-Calendar/default.aspx',
    'https://www.nyse.com/trade/hours-calendars',
)
# Reviewed published closures. The scope starts after the earlier 2025 closures.
# Early-close days remain sessions; a complete daily aggregate is accepted only
# after the next Eastern midnight, including any qualifying late-session trades.
HOLIDAYS = frozenset(('2025-09-01 2025-11-27 2025-12-25 '
    '2026-01-01 2026-01-19 2026-02-16 2026-04-03 2026-05-25 2026-06-19 2026-07-03 2026-09-07 2026-11-26 2026-12-25 '
    '2027-01-01 2027-01-18 2027-02-15 2027-03-26 2027-05-31 2027-06-18 2027-07-05 2027-09-06 2027-11-25 2027-12-24 '
    '2028-01-17 2028-02-21 2028-04-14 2028-05-29 2028-06-19 2028-07-04 2028-09-04 2028-11-23 2028-12-25').split())
FIELDS = ('ADVANCERS', 'DECLINERS', 'UNCHANGED', 'ADVDEC_LINE', 'UP_VOLUME', 'DOWN_VOLUME',
          'TRIN', 'NEW_HIGHS', 'NEW_LOWS', 'PCT_ABOVE_50DMA', 'PCT_ABOVE_200DMA')


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def stamp(value):
    if not isinstance(value, str): raise ValueError('clock must be an explicit timestamp')
    out = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if out.tzinfo is None: raise ValueError('clock needs a timezone')
    return out.astimezone(timezone.utc)


def number(value):
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, float)):
        raise ValueError('provider numeric field has an invalid type')
    out = Decimal(str(value))
    if not out.is_finite(): raise ValueError('nonfinite provider number')
    return out


def decimal_text(value):
    out = format(value, 'f')
    if '.' in out: out = out.rstrip('0').rstrip('.')
    return '0' if out in ('', '-0') else out


def ratio(numerator, denominator, scale=1):
    if denominator == 0: return None
    with localcontext() as ctx:
        ctx.prec = 28; ctx.rounding = ROUND_HALF_EVEN
        return float((Decimal(numerator)*Decimal(scale)/Decimal(denominator)).quantize(Decimal('0.000001')))


def sessions(at, count=253):
    if isinstance(count, bool) or not isinstance(count, int) or not 2 <= count <= 253:
        raise ValueError('bounded complete-session window required')
    day = stamp(at).astimezone(EASTERN).date()-timedelta(days=1)
    if not MIN_DAY <= day <= MAX_DAY: raise ValueError('date is outside the reviewed exchange calendar')
    found = []
    while day >= MIN_DAY and len(found) < count:
        if day.weekday() < 5 and day.isoformat() not in HOLIDAYS: found.append(day.isoformat())
        day -= timedelta(days=1)
    if len(found) != count: raise ValueError('insufficient reviewed calendar history')
    return list(reversed(found))


def parse_session(raw, day, acquired_at):
    """Reject a partial/ambiguous session rather than silently shrinking its universe."""
    if not isinstance(raw, bytes) or len(raw) > 24*1024*1024: raise ValueError('bounded original response required')
    requested = date.fromisoformat(day)
    if requested.isoformat() != day: raise ValueError('canonical session date required')
    close = datetime.combine(requested+timedelta(days=1), datetime.min.time(), EASTERN).astimezone(timezone.utc)
    if stamp(acquired_at) < close: raise ValueError('aggregate day was still in progress at acquisition')
    doc = json.loads(raw, parse_float=Decimal)
    if not isinstance(doc, dict) or doc.get('status') not in ('OK', 'DELAYED') or doc.get('adjusted') is not True:
        raise ValueError('successful split-adjusted source required')
    rows = doc.get('results')
    if not isinstance(rows, list) or not rows or doc.get('next_url'):
        raise ValueError('empty or incomplete expected session')
    for field in ('queryCount', 'resultsCount'):
        if type(doc.get(field)) is not int or doc[field] != len(rows): raise ValueError('provider count mismatch')
    selected = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict): raise ValueError('invalid provider row')
        ticker = row.get('T')
        if not isinstance(ticker, str) or not ticker or ticker in selected or row.get('otc') is True:
            raise ValueError('missing/duplicate identity or excluded OTC row')
        price, volume, instant = (number(row.get(key)) for key in ('c', 'v', 't'))
        if price <= 0 or volume < 0 or instant != instant.to_integral_value(): raise ValueError('invalid price, volume or clock')
        native = datetime.fromtimestamp(int(instant)/1000, EASTERN)
        # Grouped-daily t denotes the END of the aggregate window, not midnight.
        # Bind its Eastern session date without inventing a uniform close time.
        if native.date().isoformat() != day:
            raise ValueError('native aggregate date differs from the request')
        # Preserve every returned price for history, even when that session's
        # price/volume filter would exclude the security from the current census.
        selected[ticker] = {'close': price, 'volume': volume, 'source_row_index': index}
    return selected
