"""Native daily breadth arithmetic; no scores, forecasts, or portfolio policy.

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


def compute(expected_days, source_reader, generated_at):
    with localcontext() as ctx:
        ctx.prec = 28; ctx.rounding = ROUND_HALF_EVEN
        return _compute(expected_days, source_reader, generated_at)


def _compute(expected_days, source_reader, generated_at):
    """Stream original daily responses in calendar order; retain metric-specific populations.

    source_reader(day) returns {raw: bytes, acquired_at: ISO timestamp, evidence: dict}
    or {error: stable_reason}. Original bodies remain with the authorized source
    archive; this output contains aggregate statistics and their original hashes.
    """
    if expected_days != sessions(generated_at, len(expected_days)):
        raise ValueError('source dates do not match the reviewed complete-session window')
    now = stamp(generated_at)
    histories = {}; previous = {}; previous_day = None; series = {key: {} for key in FIELDS}
    evidence = {}; coverage = {}; failures = {}; ad_line = 0; ad_base = None; ad_broken = False; constituents = []
    for ordinal, day in enumerate(expected_days):
        item = source_reader(day)
        try:
            if not isinstance(item, dict) or item.get('error'): raise ValueError('source unavailable')
            acquired = stamp(item.get('acquired_at'))
            if acquired > now or (now-acquired).total_seconds() > 24*3600:
                raise ValueError('source collection clock outside the accepted batch')
            raw = item['raw']; ref = item.get('evidence')
            if not isinstance(ref, dict) or ref.get('sha256') != sha(raw) or ref.get('bytes') != len(raw):
                raise ValueError('original source binding mismatch')
            expected_url = 'https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/'+day+'?adjusted=true&include_otc=false'
            if ref.get('source_url') != expected_url or ref.get('provider') != 'massive':
                raise ValueError('original source request identity mismatch')
            rows = parse_session(raw, day, item['acquired_at'])
        except (ValueError, TypeError, KeyError, OverflowError, OSError) as exc:
            failures[day] = str(exc) if isinstance(exc, ValueError) else 'source contract rejected'
            coverage[day] = {'status': 'unavailable', 'reason': failures[day]}
            for key in FIELDS: series[key][day] = None
            previous = {}; previous_day = None; ad_broken = True
            continue
        evidence[day] = {**ref, 'acquired_at': item['acquired_at']}
        population = {ticker: row for ticker, row in rows.items() if row['close'] >= 1 and row['volume'] >= 50000}
        paired = {}; adv = dec = unch = highs = lows = above50 = above200 = n50 = n200 = n252 = 0
        up_volume = Decimal(0); down_volume = Decimal(0)
        prior_is_adjacent = ordinal > 0 and previous_day == expected_days[ordinal-1]
        for ticker, row in population.items():
            price = row['close']; prior = previous.get(ticker) if prior_is_adjacent else None
            if prior is not None:
                paired[ticker] = prior
                if price > prior['close']: adv += 1; up_volume += row['volume']
                elif price < prior['close']: dec += 1; down_volume += row['volume']
                else: unch += 1
            history = histories.get(ticker)
            tail = list(history[1]) if history and history[0] == ordinal-1 else []
            # Each SMA includes today's close. Earlier low-volume/low-price days
            # remain observations rather than silently disappearing from its window.
            if len(tail) >= 49:
                n50 += 1; above50 += price*50 > sum(tail[-49:], Decimal(0))+price
            if len(tail) >= 199:
                n200 += 1; above200 += price*200 > sum(tail[-199:], Decimal(0))+price
            if len(tail) >= 252:
                n252 += 1; highs += price >= max(tail[-252:]); lows += price <= min(tail[-252:])
            if day == expected_days[-1]:
                constituents.append({'symbol': ticker, 'source_row_index': row['source_row_index'],
                    'previous_source_row_index': prior['source_row_index'] if prior else None,
                    'change': ('up' if price > prior['close'] else 'down' if price < prior['close'] else 'unchanged') if prior else None,
                    'above_50': price*50 > sum(tail[-49:], Decimal(0))+price if len(tail) >= 49 else None,
                    'above_200': price*200 > sum(tail[-199:], Decimal(0))+price if len(tail) >= 199 else None,
                    'closing_high': price >= max(tail[-252:]) if len(tail) >= 252 else None,
                    'closing_low': price <= min(tail[-252:]) if len(tail) >= 252 else None,
                    'prior_consecutive_sessions': len(tail)})
        for ticker, row in rows.items():
            history = histories.get(ticker)
            values = history[1] if history and history[0] == ordinal-1 else deque(maxlen=252)
            values.append(row['close']); histories[ticker] = (ordinal, values)
        # Drop absent identities; returning tickers restart their consecutive window.
        histories = {ticker: histories[ticker] for ticker in rows}
        values = dict.fromkeys(FIELDS)
        if paired:
            values.update(ADVANCERS=adv, DECLINERS=dec, UNCHANGED=unch,
                          UP_VOLUME=ratio(up_volume, 1000000), DOWN_VOLUME=ratio(down_volume, 1000000))
            if not ad_broken:
                if ad_base is None: ad_base = previous_day
                ad_line += adv-dec; values['ADVDEC_LINE'] = ad_line
            values['TRIN'] = ratio(Decimal(adv)*down_volume, Decimal(dec)*up_volume) if adv and dec else None
        if n50: values['PCT_ABOVE_50DMA'] = ratio(above50, n50, 100)
        if n200: values['PCT_ABOVE_200DMA'] = ratio(above200, n200, 100)
        if n252: values.update(NEW_HIGHS=highs, NEW_LOWS=lows)
        for key, value in values.items(): series[key][day] = value
        coverage[day] = {'status': 'available', 'provider_rows': len(rows), 'current_filter_population': len(population),
            'matched_previous_session': len(paired), 'missing_previous_session': len(population)-len(paired),
            'previous_session': previous_day if prior_is_adjacent else None,
            'sma50_denominator': n50, 'sma50_numerator': above50, 'sma200_denominator': n200, 'sma200_numerator': above200,
            'prior252_denominator': n252, 'prior252_highs': highs, 'prior252_lows': lows,
            'up_volume_shares_decimal': decimal_text(up_volume) if paired else None,
            'down_volume_shares_decimal': decimal_text(down_volume) if paired else None}
        previous, previous_day = rows, day
    latest_day = expected_days[-1]
    return {'contract': CONTRACT, 'generated_at': generated_at, 'as_of': latest_day,
        'series': series, 'latest': {key: [latest_day, series[key][latest_day]] for key in FIELDS},
        'metrics': list(FIELDS), 'coverage': coverage, 'source_evidence': evidence, 'source_errors': failures,
        'current_constituents': sorted(constituents, key=lambda row: row['symbol']),
        'field_units': {key: ('percent' if key.startswith('PCT_') else 'million_reported_shares' if key in ('UP_VOLUME', 'DOWN_VOLUME')
                            else 'ratio' if key == 'TRIN' else 'net_provider_symbol_count' if key == 'ADVDEC_LINE' else 'provider_symbol_count')
                        for key in FIELDS},
        'calendar': {'first_reviewed_date': MIN_DAY.isoformat(), 'last_reviewed_date': MAX_DAY.isoformat(),
                     'sources': list(CALENDAR_SOURCES), 'requested_sessions': expected_days, 'reviewed_on': '2026-09-20',
                     'limitation': 'Published scheduled closures; extraordinary closures require calendar review. An empty expected session remains a source error.'},
        'advdec_line_origin': {'zero_base_session': ad_base, 'broken_by_source_gap': ad_broken},
        'quality': {'status': 'unavailable' if latest_day in failures else 'degraded' if failures else 'fresh',
                    'basis': 'native_session_completeness_and_original_source_collection', 'failed_sessions': len(failures)},
        'universe': {'provider': 'Massive US stocks grouped daily', 'include_otc': False,
            'current_price_minimum': 1, 'current_volume_shares_minimum': 50000,
            'price_unit': 'provider quote price; currency is not returned by this grouped endpoint',
            'historical_filter_applied': False, 'security_types': 'provider mixed listed-security population; not an exchange or common-stock index',
            'identity_limit': 'provider-symbol continuity; historical issuer/security identity is not independently reconstructed',
            'adjustment_basis': 'split-adjusted provider price response collection; no dividends or total-return inference',
            'native_timestamp_role': 'Provider aggregate-window end in Unix milliseconds, bound to the requested Eastern session date; no uniform close time assumed.',
            'timestamp_definition_source': 'https://massive.com/docs/rest/stocks/aggregates/daily-market-summary.md',
            'vintage_limit': 'Individually acquired responses in one bounded collection; the provider does not supply an atomic revision snapshot or historical availability timestamps.'},
        'calls_eligible': False, 'forecast_eligible': False, 'sizing_eligible': False, 'execution_eligible': False,
        'breadth_score': None, 'state': 'RESEARCH_ONLY', 'decision': {'verb': 'WAIT', 'abstain': True},
        'portfolio_consequences': {'target_weights': None, 'proposed_trades': [], 'forced_liquidation': False,
            'interpretation': 'Descriptive participation research. No return forecast, position change or risk limit is justified by this packet alone.',
            'scenario_workspace': '/position-sizer.html'},
        'definitions': {'ADVDEC_LINE': 'Rebased cumulative advancers minus decliners; unavailable after any missing source session.',
            'TRIN': '(advancers / decliners) / (up-volume / down-volume), using the same matched population; zero legs are undefined.',
            'NEW_HIGHS': 'Current close at or above all preceding 252 consecutive session closes, among current-filter eligible symbols.',
            'NEW_LOWS': 'Current close at or below all preceding 252 consecutive session closes, among current-filter eligible symbols.',
            'PCT_ABOVE_50DMA': '100 times symbols strictly above the inclusive 50-session arithmetic mean divided by their own complete-window population.',
            'PCT_ABOVE_200DMA': '100 times symbols strictly above the inclusive 200-session arithmetic mean divided by their own complete-window population.',
            'UP_VOLUME': 'Millions of reported shares in matched advancing symbols; not dollars or capital inflows.',
            'DOWN_VOLUME': 'Millions of reported shares in matched declining symbols; not dollars or capital outflows.'}}
