"""Canonical disclosure products. All data belongs to one immutable source run.

The four historical keys are compatibility views, not a transaction ledger.
The positions key is the commit pointer; readers of several views must use its
immutable products, never join independently fetched mutable aliases.
"""
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

import holdings_native as model
import holdings_store as store

CURRENT = 'data/13f-positions.json'
KEYS = (CURRENT, 'data/13f-flows-by-ticker.json', 'data/13f-by-ticker.json', 'data/13f-desk.json')
CONTRACT = 'holdings-canonical.v1'
NO_TRANSACTIONS = {
    'status': 'not_observed_in_13f', 'purchases_usd': None, 'sales_usd': None,
    'net_capital_flow_usd': None, 'direction': None, 'score': None,
    'reason': 'Quarter-end reported quantities and market values do not establish executed transactions or net capital flows.',
}
NO_TICKERS = {
    'status': 'unavailable_without_reviewed_security_master',
    'reason': 'CUSIP, reported class, SH/PRN and PUT/CALL are retained. Legacy issuer-name guesses cannot authorize ticker joins.',
    'mapped_positions': 0,
}


def retain(kind, value, artifacts):
    raw = model.encoded(value)
    if not 0 < len(raw) <= model.MAX_BYTES:
        raise ValueError('Canonical complete artifact exceeds bound; partition explicitly')
    key = model.PREFIX + 'canonical/' + kind + '/' + hashlib.sha256(raw).hexdigest() + '.json'
    artifacts[key] = raw
    return store.reference(key, raw)


def verified(ref, read, kind):
    sha = ref.get('sha256', '')
    if (not re.fullmatch('[a-f0-9]{64}', sha)
            or ref.get('key') != model.PREFIX + 'canonical/' + kind + '/' + sha + '.json'):
        raise ValueError('Canonical artifact identity differs')
    raw = read(ref['key'])
    if hashlib.sha256(raw).hexdigest() != sha or len(raw) != ref.get('bytes'):
        raise ValueError('Canonical artifact bytes differ')
    return model.decode(raw)


def source(run_key, read):
    if not re.fullmatch(re.escape(model.PREFIX) + r'runs/[a-f0-9]{64}\.json', run_key or ''):
        raise ValueError('Complete retained research run required')
    manifest = model.decode(read(run_key))
    if run_key != model.PREFIX + 'runs/' + model.digest(manifest) + '.json':
        raise ValueError('Native research run hash differs')
    # Executes only these reviewed local compilers, never downloaded Python.
    output = store.replay(manifest, read)
    return output, {'manifest_key': run_key, 'output_sha256': model.digest(output)}


def build(research, binding, read):
    if research.get('contract') != model.CONTRACT or model.digest(research) != binding['output_sha256']:
        raise ValueError('Canonical research output binding differs')
    artifacts, funds, indexes = {}, {}, defaultdict(dict)
    comparisons = 0
    for name, summary in sorted(research['funds'].items()):
        detail = store.verified(summary['detail'], read, 'funds')
        if (detail['contract'] != 'holdings-native-fund.v1' or detail['fund'] != name
                or detail['cik'] != summary['cik'] or detail['official_name'] != summary['official_name']):
            raise ValueError('Native manager identity differs')
        current, prior = detail['current_holdings_period'], detail['prior_holdings_period']
        current_data = detail['periods'].get(current, {})
        prior_data = detail['periods'].get(prior, {})
        compared = model.compare(current_data, prior_data)
        if compared != detail['comparison'] or compared['counts'] != summary['comparison_counts']:
            raise ValueError('Complete canonical comparison differs')
        funds[name] = {
            **summary, 'fund_key': name, 'name': detail['official_name'],
            'period_of_report': current, 'positions_ref': summary['detail'],
            'positions_count': len(current_data.get('positions', {})),
            'comparison_count': len(compared['rows']),
            'current_chain_status': current_data.get('status', 'not_acquired'),
            'effective_accessions': current_data.get('effective_accessions', []),
            'valuation_reviews': current_data.get('valuation_reviews', []),
            'positions_storage': 'Complete immutable fund artifact; periods[report_date].positions and comparison.rows',
            'transaction_inference': NO_TRANSACTIONS,
        }
        for row in compared['rows']:
            identity = row['position_id']
            if identity != model.digest(row['identity']):
                raise ValueError('Native security identity differs')
            group = indexes[current or 'unavailable'].setdefault(identity, {
                'identity': row['identity'], 'reported_managers': [],
            })
            if group['identity'] != row['identity']:
                raise ValueError('Security identity collision')
            # Full quantities, native row IDs, amendment history and originals are
            # retained once in the referenced fund document, not truncated here.
            group['reported_managers'].append({
                'fund': name, 'cik': detail['cik'], 'fund_detail': summary['detail'],
                'position_id': identity, 'current_period': current, 'prior_period': prior,
                'current_present': row['current_present'], 'prior_present': row['prior_present'],
                'disclosure_status': row['status'],
            })
            comparisons += 1
    refs = {}
    for period, securities in sorted(indexes.items()):
        index = {'contract': 'holdings-security-index.v1', 'report_period': period,
                 'security_count': len(securities),
                 'manager_comparison_count': sum(len(v['reported_managers']) for v in securities.values()),
                 'securities': securities, 'research': binding,
                 'scope': 'All native comparisons in this report-period cohort, including prior-only disclosures. No summation across managers or period cohorts.',
                 **model.PERMISSION}
        refs[period] = retain('indexes', index, artifacts)
    common = {
        'contract': CONTRACT, 'version': '3.0.0', 'generated_at': research['generated_at'],
        'source_generated_at': research['source_generated_at'], 'research': binding,
        'funds_total': len(funds), 'funds_parsed': len(funds),
        'funds': research['funds'], 'by_fund': funds,
        'report_period_cohorts': research['report_period_cohorts'],
        'current_cohort_count': research['current_cohort_count'],
        'required_period_for_current_cohort': research['required_period_for_current_cohort'],
        'security_indexes': refs, 'manager_comparison_count': comparisons,
        'quality': {**research['quality'], 'canonical_source': True, 'complete_native_comparisons': True},
        'transaction_inference': NO_TRANSACTIONS, 'ticker_identity': NO_TICKERS,
        'research_url': '/holdings-research.html', 'methodology_url': research['methodology_url'],
        'limitations': research['limitations'], 'legacy_snapshot': research['legacy_snapshot'],
        'compatibility': {'status': 'legacy_transaction_fields_retired',
            'empty_ticker_views_mean': 'Unqualified mapping; not zero ownership or no activity.',
            'consumer_rule': 'Use immutable products from canonical_replay. Missing transaction evidence must abstain and leave score denominators.',
            'consumer_migration_complete': False},
        **model.PERMISSION,
    }
    # Existing readers can no longer recover invented trades from these fields.
    # Complete real holdings live in by_fund and the cohort/security indexes.
    products = {
        CURRENT: {**common, 'view': 'positions', 'aggregate_by_ticker': {},
                  'most_bought': [], 'most_sold': [], 'consensus_holds': [], 'rare_picks': []},
        KEYS[1]: {**common, 'view': 'transaction_compatibility', 't': {}, 'net_flow_usd': None},
        KEYS[2]: {**common, 'view': 'ticker_compatibility', 'by_ticker': {}},
        KEYS[3]: {**common, 'view': 'disclosure_desk', 'total_aum_usd': None,
                  'most_bought': [], 'most_sold': [], 'risk_appetite': None},
    }
    return products, artifacts


def replay(manifest, read, replay_sources=True):
    if manifest.get('contract') != 'holdings-canonical-replay.v1':
        raise ValueError('Canonical replay contract differs')
    code = Path(__file__).read_bytes(); sha = hashlib.sha256(code).hexdigest()
    if manifest['compiler'] != {'key': model.PREFIX + 'compilers/' + sha + '.py', 'sha256': sha}:
        raise ValueError('Reviewed canonical compiler differs')
    if read(manifest['compiler']['key']) != code:
        raise ValueError('Retained canonical compiler bytes differ')
    if replay_sources:
        research, binding = source(manifest['research']['manifest_key'], read)
    else:
        native = model.decode(read(manifest['research']['manifest_key']))
        research = store.verified(native['output'], read, 'outputs')
        binding = {'manifest_key': manifest['research']['manifest_key'], 'output_sha256': model.digest(research)}
    if binding != manifest['research']:
        raise ValueError('Canonical source binding differs')
    products, artifacts = build(research, binding, read)
    if set(manifest['products']) != set(KEYS):
        raise ValueError('Complete canonical product set required')
    for key, product in products.items():
        if product != verified(manifest['products'][key], read, 'products'):
            raise ValueError('Canonical product replay differs')
    for key, raw in artifacts.items():
        if read(key) != raw:
            raise ValueError('Complete canonical index replay differs')
    return products


def run(client, bucket):
    read = store.reader(client, bucket)
    pointer = model.decode(read(model.CURRENT))
    research, binding = source(pointer.get('replay', {}).get('manifest_key'), read)
    if {k: v for k, v in pointer.items() if k != 'replay'} != research or pointer['replay'] != binding:
        raise ValueError('Current source pointer differs from immutable research')
    products, artifacts = build(research, binding, read)
    code = Path(__file__).read_bytes(); sha = hashlib.sha256(code).hexdigest()
    compiler = {'key': model.PREFIX + 'compilers/' + sha + '.py', 'sha256': sha}
    store.immutable(client, bucket, compiler['key'], code, 'text/x-python')
    refs = {key: retain('products', value, artifacts) for key, value in products.items()}
    for key, raw in artifacts.items():
        store.immutable(client, bucket, key, raw)
    manifest = {'contract': 'holdings-canonical-replay.v1', 'research': binding,
                'compiler': compiler, 'products': refs, 'generated_at': research['generated_at']}
    # Source replay above already checked complete originals. Product replay here
    # verifies serialization without doing that expensive source work twice.
    if replay(manifest, read, replay_sources=False) != products:
        raise ValueError('Canonical prepublication replay differs')
    run_ref = retain('runs', manifest, {})
    store.immutable(client, bucket, run_ref['key'], model.encoded(manifest))
    before = {}
    for key in KEYS:
        try:
            value = client.get_object(Bucket=bucket, Key=key)
            raw = store.bounded(value['Body']); old = model.decode(raw)
            if old.get('contract') == CONTRACT and model.clock(old['source_generated_at']) > model.clock(research['source_generated_at']):
                return {'published': False, 'reason': 'newer_source_already_published'}
            before[key] = {'raw': raw, 'condition': {'IfMatch': value['ETag']}}
            digest = hashlib.sha256(raw).hexdigest()
            store.immutable(client, bucket, store.PRIVATE + digest + '.bin', raw, 'application/octet-stream')
        except Exception as exc:
            if not store.missing(exc):
                raise
            before[key] = {'raw': None, 'condition': {'IfNoneMatch': '*'}}
    preservation = {'contract': 'holdings-canonical-preservation.v1', 'candidate': run_ref,
        'objects': [{'source': key, 'sha256': hashlib.sha256(v['raw']).hexdigest() if v['raw'] is not None else None,
                     'bytes': len(v['raw']) if v['raw'] is not None else None} for key, v in before.items()]}
    retained = retain('preservation', preservation, {})
    store.immutable(client, bucket, retained['key'], model.encoded(preservation))
    # S3 does not have a multi-key transaction. Immutable products are prepared
    # first; the authoritative positions pointer commits last. On failure the
    # old pointer remains usable; a newer sidecar alias is never join authority.
    written = []
    for key in (*KEYS[1:], CURRENT):
        body = model.encoded({**products[key], 'canonical_replay': run_ref})
        if before[key]['raw'] != body:
            client.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json',
                              CacheControl='no-store', **before[key]['condition'])
            if read(key) != body:
                raise ValueError('Canonical public alias readback differs')
            written.append(key)
    return {'published': True, 'canonical_replay': run_ref, 'preservation': retained,
            'source_replay': binding, 'generated_at': research['generated_at'],
            'funds': len(research['funds']), 'manager_comparisons': products[CURRENT]['manager_comparison_count'],
            'changed_aliases': written, 'transaction_inferences': 0}


def handle(event, client, bucket, user_agent, request_id=None):
    event = dict(event or {})
    action = event.get('action')
    if action == 'holdings_research_read':
        return store.handle(event, client, bucket, user_agent)
    if action not in (None, 'holdings_canonical_refresh', 'holdings_research_collect', 'holdings_research_refresh'):
        raise ValueError('Unknown holdings action; legacy writer is retired')
    collect = action in ('holdings_research_collect', 'holdings_research_refresh') or event.get('trigger') == 'new_filing'
    if action is None and not collect:
        try:
            current = model.decode(store.reader(client, bucket)(model.CURRENT))
            age = (datetime.now(timezone.utc) - model.clock(current['source_generated_at'])).total_seconds()
            collect = age > 2 * 3600
            if age < -60:
                raise ValueError('Future source clock')
        except Exception as exc:
            if not store.missing(exc):
                raise
            collect = True
    source_result = None
    if collect:
        event['action'] = action if action == 'holdings_research_refresh' else 'holdings_research_collect'
        if not event.get('request_id') and request_id:
            event['request_id'] = 'holdings-' + request_id
        response = store.handle(event, client, bucket, user_agent)
        if response['statusCode'] != 200:
            raise ValueError('Native collection did not complete')
        source_result = json.loads(response['body'])
    result = run(client, bucket)
    return {'statusCode': 200, 'headers': {'Content-Type': 'application/json', 'Cache-Control': 'no-store'},
            'body': json.dumps({**result, 'source_collection': source_result, 'paid_ai_calls': 0,
                'notifications_sent': 0, 'private_account_reads': 0, 'portfolio_writes': 0})}
