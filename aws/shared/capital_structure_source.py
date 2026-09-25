"""Read-only reconstruction of the complete retained share-structure campaign."""
import re
import urllib.parse
import statement_research_source as common
import capital_structure_measurements as measurements

PRIVATE = 'audit-private/20260909-originals/share-structure-research/'
PREFIX = 'data/capital-structure-research/'
MAX = 32 * 1024 * 1024
sha, encoded, strict, clock = common.sha, common.encoded, common.strict, common.clock
STATEMENTS = (measurements.INCOME, measurements.CASH)
SNAPSHOTS = ('quote', 'shares-float', 'splits')


def original(ref, read):
    if (not isinstance(ref, dict) or set(ref) != {'key', 'sha256', 'bytes'}
            or not isinstance(ref.get('sha256'), str) or not re.fullmatch('[a-f0-9]{64}', ref['sha256'])
            or ref['key'] != PRIVATE + ref['sha256'] + '.bin'
            or type(ref['bytes']) is not int or not 0 < ref['bytes'] <= MAX):
        raise ValueError('Complete protected capital-structure original identity required')
    body = read(ref['key'])
    if not isinstance(body, bytes) or len(body) != ref['bytes'] or sha(body) != ref['sha256']:
        raise ValueError('Retained capital-structure original bytes differ')
    return body


def spec(symbol, endpoint, period=None):
    if (not isinstance(symbol, str) or not re.fullmatch('[A-Z0-9][A-Z0-9.-]{0,15}', symbol)
            or endpoint not in STATEMENTS + SNAPSHOTS):
        raise ValueError('Exact reviewed capital-structure request required')
    params = {'symbol': symbol}
    if endpoint in STATEMENTS:
        if period not in ('annual', 'quarter'):
            raise ValueError('Explicit statement period required')
        params.update(period=period, limit=5 if period == 'annual' else 13)
    elif period is not None:
        raise ValueError('Snapshot request cannot claim a fiscal period')
    return {'symbol': symbol, 'endpoint': endpoint, 'period': period, 'limit': params.get('limit'),
        'url': 'https://financialmodelingprep.com/stable/' + endpoint + '?' + urllib.parse.urlencode(params)}


def labels(value):
    if (not isinstance(value, list) or not 1 <= len(value) <= 5000
            or not all(isinstance(v, str) and re.fullmatch('[A-Z0-9][A-Z0-9.-]{0,15}', v) for v in value)
            or value != sorted(set(value))):
        raise ValueError('Whole sorted unique reported population required')
    return value


def plan(ref, read):
    manifest = strict(original(ref, read))
    if (not isinstance(manifest, dict) or manifest.get('contract') != 'capital-structure-complete-sources.v1'
            or manifest.get('status') != 'complete'):
        raise ValueError('Complete original-source campaign required')
    names = labels(manifest.get('reported_symbols'))
    document = strict(original(manifest['plan'], read))
    if (document.get('contract') != 'capital-structure-source-plan.v1'
            or document.get('reported_symbols') != names or document.get('planned_sources') != len(names)*7
            or document.get('requests_per_batch') != 2000
            or document.get('batches') != (len(names)*7 + 1999)//2000):
        raise ValueError('Whole disjoint campaign plan required')
    clock(document['generated_at'])
    baseline = strict(original(document['baseline'], read))
    inventory = strict(original(baseline['inventory'], read))
    if (baseline.get('status') != 'complete'
            or inventory.get('contract') != 'share-structure-baseline-inventory.v1'
            or inventory.get('candidate_provider_labels') != names
            or inventory.get('candidate_provider_label_count') != len(names)
            or inventory.get('all_current_rows_conserved') is not True):
        raise ValueError('Full retained predecessor and source population required')
    specs = [spec(symbol, endpoint, period) for symbol in names
        for endpoint, period in [(e, p) for p in ('annual', 'quarter') for e in STATEMENTS] + [(e, None) for e in SNAPSHOTS]]
    if set(manifest.get('captures', {})) != {v['url'] for v in specs}:
        raise ValueError('Every one of the seven sources per label is required')
    batch_refs = manifest.get('batch_manifests')
    if not isinstance(batch_refs, list) or len(batch_refs) != document['batches']:
        raise ValueError('All whole disjoint source batches required')
    seen, captures, completed = set(), {}, []
    counts = dict.fromkeys(('complete_sources', 'provider_requests', 'reused_sources', 'provider_rows', 'provider_bytes', 'empty_arrays'), 0)
    for batch_ref in batch_refs:
        batch = strict(original(batch_ref, read)); part = batch.get('part')
        if (type(part) is not int or not 1 <= part <= document['batches'] or part in seen
                or batch.get('status') != 'complete' or batch.get('plan') != manifest['plan']):
            raise ValueError('Distinct completed original source batch required')
        if set(batch.get('captures', {})) != {v['url'] for v in specs[(part-1)*2000:part*2000]}:
            raise ValueError('Source batch differs from its exact planned slice')
        if not clock(batch['generated_at']) <= clock(batch['completed_at']):
            raise ValueError('Source batch chronology differs')
        seen.add(part); captures.update(batch['captures']); completed.append(batch['completed_at'])
        for key in counts:
            value = batch['counts'].get(key)
            if type(value) is not int or value < 0:
                raise ValueError('Exact source-batch counts required')
            counts[key] += value
    if captures != manifest['captures'] or counts != manifest['counts'] or counts['complete_sources'] != len(specs):
        raise ValueError('Complete source-batch union or counts differ')
    return manifest, specs, inventory, max(completed, key=clock)


def response(ref, request, completed_at, read):
    cap = strict(original(ref, read))
    if (cap.get('spec') != request or cap.get('http_status') != 200
            or cap.get('status') not in ('claimed', 'response_retained')
            or (cap.get('status') == 'claimed' and cap.get('reused_original') is not True)):
        raise ValueError('Exact successful captured or adopted response required')
    if not clock(cap['requested_at']) <= clock(cap['received_at']) <= clock(completed_at):
        raise ValueError('Original response acquisition chronology differs')
    raw = original(cap['original'], read); headers = cap.get('headers')
    if not isinstance(headers, dict) or headers.get('content-length') not in (None, str(len(raw))):
        raise ValueError('Original HTTP response length differs')
    if headers.get('content-encoding', 'identity').lower() not in ('', 'identity'):
        raise ValueError('Unreviewed source response encoding')
    rows = strict(raw); bound = request['limit'] or (10 if request['endpoint'] == 'quote' else 2000)
    if not isinstance(rows, list) or len(rows) > bound or not all(isinstance(row, dict) for row in rows):
        raise ValueError('Complete bounded original provider array required')
    return cap, rows


def prefetch(manifest, read):
    method = getattr(read, 'prefetch', None)
    if callable(method):
        method([ref['key'] for ref in manifest['captures'].values()])
        method([strict(original(ref, read))['original']['key'] for ref in manifest['captures'].values()])
