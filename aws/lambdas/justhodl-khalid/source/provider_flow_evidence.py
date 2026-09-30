"""Bounded informational projection of the existing Radar input; no I/O or votes."""
from datetime import datetime, timezone, timedelta, date
from decimal import Decimal, InvalidOperation, localcontext
import re

CONTRACT = 'khalid-provider-flow-evidence.v1'
FLAGS = ('forecast_qualified', 'calls_eligible', 'sizing_eligible', 'execution_eligible')
PERIODS = ('1', '5', '21')
# Explicit native catalog categories; unknown classifications cannot certify separation.
BASKET_CATEGORIES = frozenset(('broad', 'commodity', 'country', 'credit', 'crypto',
                               'factor', 'fx', 'sector', 'thematic', 'treasury'))
TICKER = re.compile(r'[A-Z][A-Z0-9.-]{0,14}')
MONEY = re.compile(r'-?\d{1,24}(?:\.\d{1,50})?')
REF = re.compile(r'data/(?:provider-flow|capital-radar)-research/(?:runs|histories)/[a-f0-9]{64}\.json')


def stamp(value):
    if not isinstance(value, str) or len(value) > 40 or 'T' not in value:
        raise ValueError('invalid_timestamp')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('timezone_required')
    return result.astimezone(timezone.utc)


def day(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise ValueError('invalid_effective_date')
    return date.fromisoformat(value)


def amount(value):
    if not isinstance(value, str) or not MONEY.fullmatch(value):
        raise ValueError('invalid_decimal')
    parsed = Decimal(value)
    if not parsed.is_finite() or abs(parsed) > Decimal('1e18'):
        raise ValueError('decimal_out_of_bounds')
    return parsed


def authority(row):
    return (isinstance(row, dict) and all(row.get(f) is False for f in FLAGS)
            and row.get('call') is None and row.get('portfolio_action') == 'WAIT')


def safe_ref(value):
    return value if isinstance(value, str) and REF.fullmatch(value) else None


def blank(reason):
    return {'status': 'unavailable', 'flow_usd_decimal': None,
            'available_observations': 0, 'requested_observations': 0,
            'dates': [], 'reasons': [reason]}


def window(raw, n, end, now, unavailable=None):
    out = blank(unavailable or 'missing_window'); out['requested_observations'] = int(n)
    if not isinstance(raw, dict):
        return out
    try:
        dates = raw.get('dates')
        count = raw.get('available_observations')
        if (not isinstance(dates, list) or len(dates) != int(n)
                or not all(isinstance(d, str) for d in dates)
                or dates != sorted(set(dates)) or dates[-1] != end
                or raw.get('end_date') != end or type(raw.get('requested_observations')) is not int or raw.get('requested_observations') != int(n)
                or type(count) is not int or not 0 <= count <= int(n)
                or any(day(d) > now.date() for d in dates)):
            raise ValueError('conflicting_window')
        missing = raw.get('missing_dates')
        if (not isinstance(missing, list) or not all(isinstance(d, str) for d in missing)
                or len(set(missing)) != len(missing) or not set(missing) <= set(dates)
                or count != int(n) - len(missing)):
            raise ValueError('conflicting_coverage')
        out.update(dates=dates[:], available_observations=count)
        if unavailable:
            return out
        if raw.get('status') != 'matched_reporting_window' or count != int(n) or missing:
            out['reasons'] = ['incomplete_window']
            return out
        if raw.get('reasons') != []:
            raise ValueError('conflicting_window_reasons')
        amount(raw.get('flow_usd_decimal'))
        out.update(status='available', flow_usd_decimal=raw['flow_usd_decimal'], reasons=[])
    except (ValueError, TypeError, InvalidOperation, OverflowError):
        out.update(status='unavailable', flow_usd_decimal=None, reasons=['invalid_or_conflicting_window'])
    return out


def fund(ticker, raw, end, generated, now):
    out = {'ticker': ticker, 'asset_type': 'ETF', 'category': 'unverified',
           'subcategory': None, 'source_acquired_at': None, 'source_valid_until': None,
           'effective_date': None, 'processed_date': None, 'history_key': None}
    reason = None
    try:
        if not isinstance(raw, dict) or raw.get('ticker') != ticker or not authority(raw):
            raise ValueError('identity_or_authority_mismatch')
        if raw.get('unit') != 'USD' or raw.get('measure') != 'provider_reported_creation_redemption_flow':
            raise ValueError('measurement_mismatch')
        acquired = stamp(raw.get('source_acquired_at')); due = stamp(raw.get('source_valid_until'))
        effective = day(raw.get('latest_effective_date')); processed = day(raw.get('latest_processed_date'))
        if not effective <= processed <= acquired.date() or acquired > generated or generated > now:
            raise ValueError('future_or_conflicting_dates')
        if due > min(acquired + timedelta(hours=26), datetime.combine(effective, datetime.min.time(), timezone.utc) + timedelta(days=6)):
            raise ValueError('invalid_source_expiry')
        observation = raw.get('latest_observation')
        if (not isinstance(observation, dict) or observation.get('effective_date') != effective.isoformat()
                or observation.get('processed_date') != processed.isoformat()):
            raise ValueError('observation_identity_mismatch')
        history = raw.get('history') or {}
        key = safe_ref(history.get('key'))
        if key != 'data/provider-flow-research/histories/' + str(history.get('sha256')) + '.json':
            raise ValueError('history_identity_mismatch')
        out.update(source_acquired_at=raw['source_acquired_at'], source_valid_until=raw['source_valid_until'],
                   effective_date=effective.isoformat(), processed_date=processed.isoformat(), history_key=key)
        tags = raw.get('classification') or {}
        for key in ('category', 'subcategory'):
            value = tags.get(key)
            if isinstance(value, str) and len(value) <= 80:
                out[key] = value
        quality = raw.get('quality') or {}
        if now >= due:
            reason = 'source_expired'
        elif (raw.get('acquisition_status') != 'retained' or quality.get('status') != 'complete_acquired_history'
              or quality.get('pagination_complete') is not True or quality.get('invalid_rows') != 0):
            reason = 'source_incomplete_or_invalid'
    except (ValueError, TypeError, AttributeError, OverflowError):
        reason = 'invalid_identity_dates_or_source'
    windows = raw.get('aligned_windows') if isinstance(raw, dict) else None
    out['windows'] = {n: window((windows or {}).get(n), n, end, now, reason) for n in PERIODS}
    for w in out['windows'].values():
        if w['status'] == 'available' and (not out['effective_date'] or end > out['effective_date']):
            w.update(status='unavailable', flow_usd_decimal=None, reasons=['window_after_latest_observation'])
    one = out['windows']['1']
    if one['status'] == 'available' and end == out['effective_date']:
        try:
            if amount(raw['latest_observation'].get('reported_flow_usd_decimal')) != amount(one['flow_usd_decimal']):
                raise ValueError('conflicting_latest_observation')
        except (ValueError, TypeError, InvalidOperation):
            one.update(status='unavailable', flow_usd_decimal=None, reasons=['conflicting_latest_observation'])
    # All aligned windows must describe one nested reporting grid.
    valid_dates = [out['windows'][n]['dates'] for n in PERIODS]
    if any(valid_dates) and any(valid_dates[i] and valid_dates[i + 1] and valid_dates[i] != valid_dates[i + 1][-len(valid_dates[i]):] for i in range(2)):
        for w in out['windows'].values():
            w.update(status='unavailable', flow_usd_decimal=None, reasons=['conflicting_window_dates'])
    return out


def project(packet, now):
    """No mutation of packet; malformed or legacy packets produce an explicit hold."""
    result = {'contract': CONTRACT, 'status': 'unavailable', 'reasons': [], 'unit': 'USD',
              'source_artifact': 'data/capital-flow-radar.json',
              'canonical_artifact': 'data/provider-fund-flow-research.json',
              'independent_investment_votes': 0, 'informational_only': True,
              'portfolio_action': 'WAIT', 'call': None, **{f: False for f in FLAGS},
              'generated_at': None, 'source_generated_at': None, 'source_valid_until': None,
              'reference_end_date': None, 'replay_key': None, 'funds': [], 'baskets': [],
              'scope': 'Provider-reported ETF creations/redemptions. Configured baskets are not complete industries; no constituent purchases or investor identity inferred.'}
    try:
        if (not authority(packet) or packet.get('contract') != 'capital-flow-native-research.v1'
                or packet.get('engine') != 'justhodl-capital-flow-radar'
                or (packet.get('dependency_graph') or {}).get('additional_independent_investment_votes') != 0):
            raise ValueError('native_radar_required')
        quality = packet.get('quality') or {}
        funds = packet.get('funds')
        if (quality.get('independent_investment_votes') != 0 or not isinstance(funds, dict)
                or not 0 < len(funds) <= 500 or quality.get('configured_funds') != len(funds)
                or not all(isinstance(t, str) and TICKER.fullmatch(t) for t in funds)):
            raise ValueError('invalid_fund_inventory')
        generated = stamp(packet.get('generated_at')); source = stamp(packet.get('source_generated_at'))
        due = stamp(packet.get('source_valid_until')); end = day((packet.get('reference') or {}).get('end_date'))
        if not source <= generated <= now or end > source.date() or due <= source or due > source + timedelta(hours=26):
            raise ValueError('future_or_conflicting_packet_dates')
        ref = safe_ref((packet.get('replay') or {}).get('manifest_key'))
        if not ref or not ref.startswith('data/capital-radar-research/runs/'):
            raise ValueError('invalid_replay_reference')
        result.update(generated_at=packet['generated_at'], source_generated_at=packet['source_generated_at'],
                      source_valid_until=packet['source_valid_until'], reference_end_date=end.isoformat(), replay_key=ref)
        if now >= due:
            raise ValueError('packet_source_expired')
        rows = [fund(t, funds[t], end.isoformat(), source, now) for t in sorted(funds)]
        # Histories contain ticker identity. A shared key/hash across funds is a
        # conflict, unlike a fund legitimately occurring in several named baskets.
        history_owners = {}
        for row in rows:
            if row['history_key']:
                history_owners.setdefault(row['history_key'], []).append(row)
        for owners in history_owners.values():
            if len(owners) > 1:
                for row in owners:
                    for w in row['windows'].values():
                        w.update(status='unavailable', flow_usd_decimal=None, reasons=['cross_ticker_history_conflict'])
        grids = {n: next((r['windows'][n]['dates'] for r in rows if r['ticker'] == 'SPY'), []) for n in PERIODS}
        if any(len(grids[n]) != int(n) for n in PERIODS):
            raise ValueError('reference_grid_unavailable')
        for row in rows:
            for n, w in row['windows'].items():
                if w['dates'] != grids[n]:
                    w.update(status='unavailable', flow_usd_decimal=None, reasons=['reference_grid_mismatch'])
        result['reporting_dates'] = grids
        result['funds'] = rows
        available = sum(any(w['status'] == 'available' for w in r['windows'].values()) for r in rows)
        live_due = [stamp(r['source_valid_until']) for r in rows if any(w['status'] == 'available' for w in r['windows'].values())]
        if live_due: result['source_valid_until'] = min([due] + live_due).isoformat()
        result.update(configured_funds=len(rows), available_funds=available,
                      status='available' if available == len(rows) else 'partial' if available else 'unavailable')
        if available < len(rows): result['reasons'] = ['incomplete_fund_coverage']
        # Retain named membership only. Reconcile totals against accepted fund windows;
        # never use stock-context arrays, leveraged comparison arithmetic or old scores.
        indexed = {r['ticker']: r for r in rows}
        groups = packet.get('complexes')
        if not isinstance(groups, list) or len(groups) > 100:
            result['reasons'].append('invalid_basket_inventory'); groups = []
        for group in groups:
            if not authority(group) or not isinstance(group.get('name'), str) or len(group['name']) > 100:
                continue
            projected = {'name': group['name'], 'windows': {}}
            for n in PERIODS:
                raw = (group.get('windows') or {}).get(n) or {}
                members = raw.get('required')
                w = {'status': 'unavailable', 'flow_usd_decimal': None, 'observed_subset_flow_usd_decimal': None,
                     'required_count': 0, 'included_count': 0, 'excluded': [], 'dates': [], 'reasons': ['invalid_basket']}
                if isinstance(members, list) and 0 < len(members) <= 500 and all(isinstance(t, str) and t in indexed for t in members) and len(set(members)) == len(members):
                    accepted = [t for t in sorted(members) if indexed[t]['windows'][n]['status'] == 'available']
                    grids = {tuple(indexed[t]['windows'][n]['dates']) for t in accepted}
                    # Leveraged/inverse funds remain individually labelled; never blend into industry baskets.
                    special = any(indexed[t]['category'] in ('leveraged', 'inverse') for t in members)
                    unverified = any(indexed[t]['category'] not in BASKET_CATEGORIES | {'leveraged', 'inverse'} for t in members)
                    excluded = sorted(set(members) - set(accepted))
                    w.update(required_count=len(members), included_count=len(accepted), excluded=excluded)
                    if len(grids) == 1 and not special and not unverified:
                        with localcontext() as ctx:
                            ctx.prec = 50
                            subtotal = sum((amount(indexed[t]['windows'][n]['flow_usd_decimal']) for t in accepted), Decimal(0))
                        dates = list(next(iter(grids)))
                        w.update(dates=dates, observed_subset_flow_usd_decimal=format(subtotal, 'f'), reasons=['partial_configured_basket'] if excluded else [])
                        if excluded:
                            w['status'] = 'partial'
                        else:
                            try:
                                if (raw.get('status') != 'complete_matched_group' or raw.get('dates') != dates
                                        or raw.get('end_date') != end.isoformat() or raw.get('included_count') != len(members)
                                        or raw.get('required_count') != len(members) or set(raw.get('included', [])) != set(members)
                                        or raw.get('excluded') != {} or amount(raw.get('flow_usd_decimal')) != subtotal):
                                    raise ValueError('conflicting_basket')
                                w.update(status='available', flow_usd_decimal=format(subtotal, 'f'))
                            except (ValueError, TypeError, InvalidOperation):
                                w.update(status='unavailable', observed_subset_flow_usd_decimal=None, reasons=['conflicting_basket'])
                    elif unverified:
                        w['reasons'] = ['unverified_basket_classification']
                    elif special:
                        w['reasons'] = ['leveraged_inverse_separate']
                projected['windows'][n] = w
            result['baskets'].append(projected)
    except (ValueError, TypeError, AttributeError, InvalidOperation, OverflowError) as exc:
        result.update(status='unavailable', funds=[], baskets=[], reasons=[str(exc) if isinstance(exc, ValueError) else 'malformed_native_packet'])
    for row in result['funds'] + result['baskets']:
        for w in row['windows'].values():
            dates = w.pop('dates', [])
            w['start_date'] = dates[0] if dates else None
            w['end_date'] = dates[-1] if dates else None
    return result
