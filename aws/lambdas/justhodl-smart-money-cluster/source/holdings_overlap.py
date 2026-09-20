"""Reproducible overlap of positive reported quantities, never inferred trades."""
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from itertools import combinations
import hashlib
import json
import re

PREFIX = 'data/holdings-overlap/'
CURRENT = 'data/smart-money-clusters.json'
NATIVE = 'data/holdings-research/'
CANONICAL = NATIVE+'canonical/'
CONTRACT = 'holdings-disclosure-overlap.v1'
MAX_BYTES = 64*1024*1024
PERMISSION = {'call': None, 'calls_eligible': False, 'sizing_eligible': False,
              'execution_eligible': False, 'additional_independent_votes': 0}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value): return hashlib.sha256(encoded(value)).hexdigest()


def decode(raw):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('Complete bounded JSON required')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result: raise ValueError('Duplicate JSON key')
            result[key] = value
        return result
    def reject(value): raise ValueError('Nonfinite JSON: '+value)
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=reject)


def clock(value):
    value = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if value.tzinfo is None: raise ValueError('Timezone-aware clock required')
    return value.astimezone(timezone.utc)


def reference(key, raw):
    return {'key': key, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}


def verified(ref, read, prefix, kind):
    sha = ref.get('sha256', '')
    if not re.fullmatch('[a-f0-9]{64}', sha) or ref.get('key') != prefix+kind+'/'+sha+'.json':
        raise ValueError('Immutable reference identity differs')
    raw = read(ref['key'])
    if len(raw) != ref.get('bytes') or hashlib.sha256(raw).hexdigest() != sha:
        raise ValueError('Immutable reference bytes differ')
    return decode(raw)


def source(read, run=None):
    pointer = decode(read('data/13f-positions.json')) if run is None else None
    ref = run if run is not None else pointer.get('canonical_replay', {})
    manifest = verified(ref, read, CANONICAL, 'runs')
    if manifest.get('contract') != 'holdings-canonical-replay.v1':
        raise ValueError('Canonical manifest required')
    product_ref = manifest['products']['data/13f-positions.json']
    product = verified(product_ref, read, CANONICAL, 'products')
    if product.get('contract') != 'holdings-canonical.v1' or product['research'] != manifest['research']:
        raise ValueError('Canonical research binding differs')
    if pointer is not None and {k: v for k, v in pointer.items() if k != 'canonical_replay'} != product:
        raise ValueError('Mutable source differs from immutable canonical product')
    return product, {'canonical_replay': ref, 'canonical_product': product_ref,
                     'native_research': manifest['research']}


def ratio(numerator, denominator):
    if not denominator: return None
    with localcontext() as ctx:
        ctx.prec = 40
        return format((Decimal(numerator)*100/Decimal(denominator)).quantize(Decimal('0.000001')), 'f')


def build(product, binding, read, generated_at):
    if product.get('contract') != 'holdings-canonical.v1' or digest(product) != binding['canonical_product']['sha256']:
        raise ValueError('Complete canonical product binding required')
    now = clock(generated_at); age = (now-clock(product['source_generated_at'])).total_seconds()
    if age < -60 or clock(product['generated_at']) > now:
        raise ValueError('Future source cannot be published')
    if len(product['by_fund']) > 256:
        raise ValueError('Partition the complete manager roster explicitly before expansion')
    funds, groups, members, ciks = {}, defaultdict(dict), defaultdict(list), set()
    for name, summary in sorted(product['by_fund'].items()):
        detail = verified(summary['positions_ref'], read, NATIVE, 'funds')
        if (detail.get('contract') != 'holdings-native-fund.v1' or detail['fund'] != name
                or detail['cik'] != summary['cik'] or detail['official_name'] != summary['official_name']
                or detail['current_holdings_period'] != summary['period_of_report']):
            raise ValueError('Complete native manager identity differs')
        if detail['cik'] in ciks: raise ValueError('Duplicate manager CIK requires an alias review')
        ciks.add(detail['cik'])
        period = detail['current_holdings_period']; current = detail['periods'].get(period, {})
        if period and (not re.fullmatch(r'\d{4}-\d{2}-\d{2}', period) or datetime.fromisoformat(period).date() > now.date()):
            raise ValueError('Unambiguous nonfuture report period required')
        complete = current.get('status') == 'complete_selected_public_chain'
        positions = current.get('positions', {})
        if len(positions) != summary['positions_count'] or current.get('status', 'not_acquired') != summary['current_chain_status']:
            raise ValueError('Complete current-period counts or chain differ')
        effective = current.get('effective_accessions', [])
        if any(a not in detail['filings'] for a in effective):
            raise ValueError('Effective filing reference missing')
        if complete: members[period].append(name)
        funds[name] = {'fund': name, 'official_name': detail['official_name'], 'cik': detail['cik'],
            'period_of_report': period, 'current_chain_status': current.get('status', 'not_acquired'),
            'eligible_for_overlap': complete, 'detail': summary['positions_ref'],
            'effective_filings': {a: detail['filings'][a] for a in effective},
            'latest_submission': detail.get('latest_submission'),
            'reported_identity_count': len(positions) if complete else None,
            'positive_quantity_identity_count': None, 'zero_quantity_identity_count': None,
            'observation_age_days': (now.date()-datetime.fromisoformat(period).date()).days if period else None,
            'confidential_omissions_declared': current.get('confidential_omissions_declared'),
            'confidential_omissions_unknown': current.get('confidential_omissions_unknown'),
            'valuation_reviews': current.get('valuation_reviews', []),
            'current_cohort': complete and period == product['required_period_for_current_cohort']}
        if not complete: continue
        positive = 0
        for position_id, position in sorted(positions.items()):
            identity = position['identity']
            if set(identity) != {'cusip', 'class', 'quantity_type', 'put_call'} or position_id != digest(identity):
                raise ValueError('Native security identity differs')
            kind, option = identity['quantity_type'], identity['put_call']
            if kind not in ('SH', 'PRN') or option not in (None, 'PUT', 'CALL'):
                raise ValueError('Instrument scope requires review')
            quantity = position['reported_quantity']
            if not isinstance(quantity, str) or not re.fullmatch(r'\d{1,30}(?:\.\d{1,12})?', quantity):
                raise ValueError('Exact nonnegative reported quantity required')
            present = Decimal(quantity) > 0; positive += int(present)
            scope = kind+'|'+(option or 'NONE')
            group = groups[(period, scope)].setdefault(position_id, {'position_id': position_id,
                'identity': identity, 'issuer_names': set(), 'reporters': []})
            group['issuer_names'].update(position['issuer_names'])
            group['reporters'].append({'fund': name, 'reported_quantity': quantity,
                'positive_reported_quantity': present, 'native_rows': position['native_rows'],
                'detail_pointer': '/periods/'+period+'/positions/'+position_id})
        funds[name]['positive_quantity_identity_count'] = positive
        funds[name]['zero_quantity_identity_count'] = len(positions)-positive
    if len(funds) != product['funds_total']:
        raise ValueError('Complete roster count differs')
    artifacts, cohorts = {}, {}
    for period, names in sorted(members.items()):
        refs = {}
        for (p, scope), securities in sorted(groups.items()):
            if p != period: continue
            sets = {name: set() for name in names}
            rows = []
            for key, row in sorted(securities.items()):
                for reporter in row['reporters']:
                    if reporter['positive_reported_quantity']: sets[reporter['fund']].add(key)
                rows.append({**row, 'issuer_names': sorted(row['issuer_names']),
                    'reported_manager_count': len(row['reporters']),
                    'positive_quantity_manager_count': sum(v['positive_reported_quantity'] for v in row['reporters'])})
            pairs = []
            for a, b in combinations(sorted(names), 2):
                shared = sorted(sets[a] & sets[b]); union = len(sets[a] | sets[b])
                pairs.append({'fund_a': a, 'fund_b': b, 'count_a': len(sets[a]), 'count_b': len(sets[b]),
                    'shared_count': len(shared), 'union_count': union, 'shared_position_ids': shared,
                    'jaccard_pct': ratio(len(shared), union), 'coverage_a_pct': ratio(len(shared), len(sets[a])),
                    'coverage_b_pct': ratio(len(shared), len(sets[b]))})
            doc = {'contract': 'holdings-overlap-scope.v1', 'report_period': period, 'instrument_scope': scope,
                'funds': sorted(names), 'source': binding, 'securities': rows, 'pairs': pairs,
                'security_count': len(rows), 'manager_disclosure_count': sum(v['reported_manager_count'] for v in rows),
                'positive_manager_disclosure_count': sum(len(v) for v in sets.values()), **PERMISSION}
            raw = encoded(doc)
            if len(raw) > MAX_BYTES: raise ValueError('Complete scope exceeds bound; partition explicitly')
            key = PREFIX+'scopes/'+hashlib.sha256(raw).hexdigest()+'.json'; artifacts[key] = raw
            refs[scope] = {**reference(key, raw), 'security_count': len(rows), 'pair_count': len(pairs)}
        cohorts[period] = {'funds': sorted(names), 'fund_count': len(names), 'scopes': refs,
                           'current_cohort': period == product['required_period_for_current_cohort']}
    missing = sorted(k for k, v in funds.items() if not v['eligible_for_overlap'])
    output = {'contract': CONTRACT, 'version': '2.0.0', 'generated_at': generated_at,
        'source_generated_at': product['source_generated_at'], 'source_research_generated_at': product['generated_at'],
        'source': binding, 'funds': funds, 'cohorts': cohorts,
        'required_period_for_current_cohort': product['required_period_for_current_cohort'],
        'counts': {'funds_in_roster': len(funds), 'funds_with_complete_current_disclosures': len(funds)-len(missing),
                   'current_cohort_managers': sum(v['current_cohort'] for v in funds.values()), 'period_cohorts': len(cohorts)},
        'quality': {'status': 'stale' if age > 48*3600 else ('partial' if missing else 'fresh'),
                    'source_age_seconds': round(age, 3), 'fresh_until': (clock(product['source_generated_at'])+timedelta(hours=48)).isoformat(), 'unavailable_managers': missing,
                    'scope': 'Acquisition freshness only. Holdings recency is the reported quarter and observation_age_days, not this publication clock.'},
        'methodology': {'identity': 'Exact CUSIP, reported class, SH/PRN and PUT/CALL; no ticker guessing.',
            'membership': 'Positive reported quantity in a complete current public amendment chain. Zero-quantity disclosures remain inspectable but are excluded from overlap sets.',
            'jaccard': 'Number of shared identities divided by union of identities within one report period and instrument scope. An empty denominator is null.',
            'coverage': 'Shared identities divided by each manager set size. Missing public chains are excluded, not treated as empty holdings.',
            'interpretation': 'Descriptive co-disclosure only. No trades, independence, investment conviction, exposure correlation or portfolio diversification are inferred.'},
        'limitations': ['Class text differences can split an economic security; an exact match is not a reviewed security-master mapping.',
            'Shared discretion, manager relationships, short positions and public/confidential omissions prevent counting disclosures as independent capital.',
            'Option quantities describe reported underlying shares; principal amounts are not share counts. Scopes are never blended.',
            'Only the retained manager roster is covered; it is not the institutional market universe.',
            'Comparisons describe quarter-end public disclosures and are not live ownership or executed transactions.'],
        'clusters': [], 'stats': {'n_clusters_scored': None},
        'compatibility': {'legacy_trade_scoring': 'retired', 'empty_clusters_mean': 'Unqualified transaction inference, not no holdings or no activity.',
                          'consumer_migration_complete': False},
        'research_url': '/smart-money.html', 'original_disclosures_url': '/holdings-research.html', **PERMISSION}
    return output, artifacts
