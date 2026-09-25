"""Reconstruct bounded daily research from complete FINRA originals and listings.

The same literal symbol across dates is not a verified security identity.
Source files acquired now do not establish point-in-time historical availability.
No directional, short-interest, squeeze or portfolio authority is granted.
"""
from collections import defaultdict
from datetime import datetime, timezone, date
import hashlib, json, re
import offexchange_measurements as source
import short_volume_source_index as index
import short_volume_measurements as measurements

CONTRACT = 'short-volume-original-research.v1'
PREFIX = 'data/short-volume-research/'
PRIVATE = 'audit-private/20260909-originals/short-volume-research/'
CURRENT = 'data/finra-short.json'
MAX = 8 * 1024 * 1024
FLAGS = ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified')
PERMISSIONS = dict.fromkeys(FLAGS, False)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return sha(encoded(value))


def strict(raw):
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise ValueError('Duplicate JSON key')
            out[key] = value
        return out
    def invalid(_):
        raise ValueError('Nonfinite JSON')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)


def clock(value):
    if not isinstance(value, str):
        raise ValueError('Explicit acquisition clock required')
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('Timezone required')
    return stamp.astimezone(timezone.utc)


def original(ref, read):
    if (not isinstance(ref, dict) or set(ref) != {'key', 'sha256', 'bytes'}
            or not isinstance(ref.get('sha256'), str) or not re.fullmatch('[a-f0-9]{64}', ref['sha256'])
            or ref['key'] != PRIVATE + ref['sha256'] + '.bin'
            or type(ref['bytes']) is not int or not 0 < ref['bytes'] <= MAX):
        raise ValueError('Complete protected original identity required')
    raw = read(ref['key'])
    if not isinstance(raw, bytes) or len(raw) != ref['bytes'] or sha(raw) != ref['sha256']:
        raise ValueError('Original source identity differs')
    return raw


def capture(row, url, generated, read):
    if not isinstance(row, dict) or row.get('url') != url or row.get('http_status') != 200 or row.get('status') != 'response_retained':
        raise ValueError('Exact successful source capture required')
    if not clock(row.get('requested_at')) <= clock(row.get('received_at')) <= clock(generated):
        raise ValueError('Acquisition chronology differs')
    raw = original(row['original'], read)
    headers = row.get('headers')
    if not isinstance(headers, dict):
        raise ValueError('Explicit captured header map required')
    length = headers.get('content-length')
    if length is not None and (not isinstance(length, str) or not length.isdigit() or int(length) != len(raw)):
        raise ValueError('HTTP source length differs')
    return raw


def selected_sources(inputs, read):
    if not isinstance(inputs, dict) or inputs.get('contract') != 'short-volume-original-inputs.v1':
        raise ValueError('Original-input contract required')
    generated = inputs['generated_at']
    cutoff = date.fromisoformat(source.day(inputs['selection_cutoff']))
    if cutoff > clock(generated).date():
        raise ValueError('Source selection is after generation')
    captures = inputs.get('captures')
    if not isinstance(captures, dict):
        raise ValueError('Complete original captures required')
    discovery = captures['index:discovery']
    parsed = index.parse(capture(discovery, index.URL, generated, read), cutoff)
    plan = index.month_requests(cutoff, parsed)
    if inputs.get('month_plan') != plan:
        raise ValueError('Source month plan differs')
    indexes = []
    for spec in plan:
        row = captures['index:' + spec['period']]
        if clock(row['requested_at']) < clock(discovery['received_at']):
            raise ValueError('Filtered listing predates source filter discovery')
        indexes.append(index.parse(capture(row, spec['url'], generated, read), cutoff, spec['period']))
    selected = index.selected_files(indexes, cutoff)
    if selected != inputs.get('selected_files'):
        raise ValueError('Latest 61 published file selections differ')
    expected = {'index:discovery', *('index:' + v['period'] for v in plan),
                *('daily:' + v['observation_date'] for v in selected)}
    if set(captures) != expected:
        raise ValueError('Exactly the selected source captures required')
    return selected


def bucket(symbol):
    return sha(symbol.encode())[:2]


def record_identity(value):
    raw = encoded(value)
    if len(raw) > MAX:
        raise ValueError('Public record shard exceeds bound')
    return {'key': PREFIX + 'records/' + sha(raw) + '.json', 'sha256': sha(raw), 'bytes': len(raw)}


def compile_output(inputs, read):
    selected = selected_sources(inputs, read)
    generated = inputs['generated_at']
    dates = [row['observation_date'] for row in selected]
    histories = defaultdict(list)
    sources = []
    for i, selected_row in enumerate(selected):
        stamp = selected_row['observation_date']
        captured = inputs['captures']['daily:' + stamp]
        raw = capture(captured, selected_row['url'], generated, read)
        if stamp > clock(captured['received_at']).date().isoformat():
            raise ValueError('Daily observation is after acquisition')
        rows = source.cnms(raw, stamp)
        if not rows:
            raise ValueError('Listed source contains no observations')
        for row in rows:
            histories[row['symbol']].append(measurements.point(row, i))
        sources.append({'observation_date': stamp, 'original': captured['original'], 'url': captured['url'],
                        'source_requested_at': captured['requested_at'], 'source_received_at': captured['received_at'],
                        'source_headers': captured['headers'], 'rows': len(rows), 'trailer_reconciled': True,
                        'historical_availability_verified': False})
        del rows
    shards = {}
    symbols = []
    complete = {str(size): 0 for size in measurements.WINDOWS}
    latest_count = 0
    for name in sorted(histories):
        record = measurements.comparisons(name, histories[name], dates)
        shard = shards.setdefault(bucket(name), {'contract': 'short-volume-record-shard.v1', 'records': {}})
        shard['records'][name] = record
        latest_count += record['latest_point_present']
        for size in complete:
            complete[size] += record['windows'][size]['daily_ratios_complete']
        symbols.append({'symbol': name, 'bucket': bucket(name)})
    refs = {key: record_identity(value) for key, value in sorted(shards.items())}
    packet = {
        'contract': CONTRACT, 'engine': 'justhodl-finra-short', 'version': '2.0.0', 'generated_at': generated,
        'data_date': dates[-1], 'selection_cutoff': inputs['selection_cutoff'],
        'quality': {'status': 'dated_observations', 'source_originals_reconstructed': True,
                    'market_coverage_complete': False, 'historical_availability_verified': False,
                    'security_identity_continuity_verified': False},
        'dates': dates, 'point_fields': list(measurements.POINT_FIELDS), 'daily_sources': sources,
        'record_shards': refs, 'symbols': symbols,
        'counts': {'source_files': len(dates), 'source_rows': sum(v['rows'] for v in sources),
                   'source_symbols': len(symbols), 'latest_symbols': latest_count,
                   'symbols_with_complete_prior_daily_ratios': complete},
        'scope': 'FINRA disseminated NMS regular-session TRF/ADF volume; not all-market volume or outstanding short interest.',
        'methodology': {
            'short_volume_includes_exempt': True, 'windows': list(measurements.WINDOWS),
            'window_basis': 'preceding_published_files_excluding_latest',
            'missing_observations': 'no_fill_no_shortened_baseline',
            'daily_mean': 'equal_weight_mean_of_daily_short_to_reported_total_percent',
            'pooled_ratio': 'sum_short_divided_by_sum_reported_total_percent',
            'standard_deviation': 'sample_daily_percentage_points_n_minus_1',
            'z_score': 'descriptive_latest_minus_prior_mean_divided_by_sample_sd',
            'numeric_rounding': 'decimal_128_precision_centered_variance_half_even_12_decimal_places',
            'histories': 'literal_symbol_not_verified_security_identity',
            'probability_interpretation': None,
        },
        'evidence_dependencies': {'family': 'finra_reported_equity_activity',
                                  'independent_of_offexchange_daily_evidence': False},
        'call': None, 'signal': None, 'score': None, 'tickets': [], **PERMISSIONS,
        'decision': {'verb': 'WAIT', 'abstain': True, 'eligible_votes': 0,
                     'reason': 'Daily trade-reporting comparisons have no qualified directional or sizing authority.'},
        'squeeze_candidates': [], 'high_short_volume': [], 'stocks': [], 'top_picks': [], 'board': [],
        'short_interest_shares': None, 'days_to_cover': None,
    }
    if len(encoded(packet)) > MAX:
        raise ValueError('Public research index exceeds byte bound')
    return {'packet': packet, 'shards': shards}
