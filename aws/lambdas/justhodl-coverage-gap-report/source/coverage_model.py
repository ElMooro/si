"""Source-scoped inventory counts, never a market-coverage certification.

Pure projection of complete supplied source documents. No I/O at import or call.
The acquisition layer retains exact originals and binds the compiler separately.
"""
from copy import deepcopy
from datetime import datetime, timezone
import re

CONTRACT = 'coverage-inventory.v1'
INPUTS = {
    'symbology': 'data/symbology/master.json',
    'edgar': 'data/warm/edgar-filings/latest-summary.json',
    'nyfed': 'data/warm/nyfed/latest-summary.json',
    'rollup': 'data/audit/data-source-rollup.json',
}
RATE_NAMES = ('SOFR', 'EFFR', 'OBFR', 'TGCR', 'BGCR')
# Character patterns only, not checksum/assignment/relationship validation.
# FIGI allocation rules v30 (August 2025), section 1.1.2:
# https://www.openfigi.com/docs/figi-allocation-rules.pdf
# CUSIP/CINS structure: https://www.cusip.com/identifiers.html
IDENTIFIER_PATTERNS = {
    'figi': r'[B-DF-HJ-NP-TV-Z]{2}G[B-DF-HJ-NP-TV-Z0-9]{8}[0-9]',
    'cusip': r'[A-Z0-9*@#]{8}[0-9]',
}


def count(value):
    return value if type(value) is int and value >= 0 else None


def timestamp(value):
    if not isinstance(value, str) or len(value) > 80:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed.astimezone(timezone.utc).isoformat() if parsed.tzinfo else None
    except (ValueError, OverflowError):
        return None


def source_state(name, attempt, doc):
    if not isinstance(attempt, dict) or attempt.get('source_key') != INPUTS[name]:
        raise ValueError('Exact declared source identity required')
    received = attempt.get('status') == 'received'
    valid = received and isinstance(doc, dict)
    reported = {}
    if valid:
        for key in ('generated_at', 'as_of', 'updated_at'):
            value = doc.get(key)
            if isinstance(value, str) and len(value) <= 80:
                reported[key] = {'value': value, 'aware_clock': timestamp(value)}
    return {
        'source_key': INPUTS[name], 'read_status': attempt.get('status'),
        'document_status': 'object_received' if valid else 'invalid_document' if received else 'unavailable',
        'requested_at': attempt.get('requested_at'), 'received_at': attempt.get('received_at'),
        'original_ref': deepcopy(attempt.get('original_ref')),
        'reported_clocks': reported,
        'observation_time_verified': False, 'freshness_verified': False,
        'source_replay_verified': False,
    }, doc if valid else None


def metric(name, actual, target, method, source, *, unit, grain, status, fields, **extras):
    if count(actual) is None and actual is not None:
        raise ValueError('Inventory count must be an integer or unknown')
    if count(target) is None and target is not None:
        raise ValueError('Inventory denominator must be an integer or unknown')
    return {
        'metric': name, 'actual': actual, 'target': target,
        'pct_of_target': round(100 * actual / target, 2) if actual is not None and target else None,
        'method': method, 'unit': unit, 'grain': grain, 'status': status,
        'source': source, 'source_fields': fields,
        'market_coverage_qualified': False, 'investment_authority': False, **extras,
    }


def project(attempts, documents, generated_at):
    if set(attempts) != set(INPUTS) or set(documents) != set(INPUTS) or timestamp(generated_at) is None:
        raise ValueError('Whole declared input set and aware generation clock required')
    sources, docs = {}, {}
    for name in INPUTS:
        sources[name], docs[name] = source_state(name, attempts[name], documents[name])
    sym = docs['symbology']; population = sym.get('by_ticker') if sym is not None else None
    rows_valid = isinstance(population, dict) and all(
        isinstance(t, str) and t and t.strip() == t and not any(c.isspace() or ord(c) < 32 for c in t)
        and isinstance(row, dict) for t, row in population.items())
    n = len(population) if rows_valid else None
    row_state = 'observed_inventory' if rows_valid else 'invalid_population' if sym is not None else 'unavailable'
    declared = sym.get('n_tickers') if sym is not None else None
    declared_count = count(declared)
    reconciliation = {
        'reported_n_tickers': declared_count,
        'reported_count_valid': declared_count is not None,
        'matches_population': declared_count == n if declared_count is not None and n is not None else None,
    }
    identifiers = {}
    for field, pattern in IDENTIFIER_PATTERNS.items():
        values, present, invalid, row_count = set(), 0, 0, 0
        if rows_valid:
            for row in population.values():
                value = row.get(field)
                if value is None:
                    continue
                present += 1
                if isinstance(value, str) and re.fullmatch(pattern, value):
                    values.add(value); row_count += 1
                else:
                    invalid += 1
        identifiers[field] = {
            'actual': len(values) if rows_valid else None,
            'rows_with_non_null_values': present if rows_valid else None,
            'rows_matching_character_pattern': row_count if rows_valid else None,
            'rows_not_matching_character_pattern': invalid if rows_valid else None,
            'identifier_relationships_qualified': False, 'checksum_verified': False, 'assignment_verified': False,
        }
    metrics = [metric('us_tickers', n, None,
        'Count of complete received by_ticker entries; neither a whole SEC registrant census nor global market coverage.',
        'symbology', unit='ticker_entries', grain='one entry in the supplied ticker dictionary', status=row_state,
        fields=['by_ticker'], legacy_unqualified_target=320000, count_reconciliation=reconciliation)]
    for name, field, old_target in (('figi_ids', 'figi', None), ('cusips', 'cusip', 500000)):
        info = identifiers[field]
        metrics.append(metric(name, info.pop('actual'), None,
            'Distinct stored values matching the identifier character pattern; no checksum, assignment, issuer/security or historical qualification. Row counts are separate.',
            'symbology', unit='distinct_identifier_values', grain='one distinct stored '+field,
            status=row_state, fields=['by_ticker.*.'+field],
            legacy_unqualified_target=old_target, **info))
    edgar = docs['edgar']; filings = count(edgar.get('n_filings')) if edgar is not None else None
    metrics.append(metric('edgar_filings_qtd', filings, None,
        'Reported n_filings only. The field does not prove current-quarter scope or completeness.',
        'edgar', unit='reported_filing_count', grain='reported summary count',
        status='reported_unqualified_count' if filings is not None else 'unavailable_or_invalid_count',
        fields=['n_filings'], quarter_scope_verified=False, completeness_verified=False))
    nyfed = docs['nyfed']; rates = nyfed.get('rates') if nyfed is not None else None
    valid_rates = isinstance(rates, dict) and all(isinstance(k,str) and isinstance(v,dict) for k,v in rates.items())
    rate_states = {}
    for name in RATE_NAMES:
        aliases=[key for key in (name.lower(),name) if valid_rates and key in rates]
        row = rates[aliases[0]] if len(aliases)==1 else None
        observations = count(row.get('n_obs')) if isinstance(row, dict) else None
        flagged = isinstance(row,dict) and row.get('data_unavailable',False) is not False
        known = observations is not None and not flagged and len(aliases)==1
        rate_states[name] = {'reported_observations': observations, 'source_keys': aliases,
            'count_known': known,
            'status': 'ambiguous_source_aliases' if len(aliases)>1 else 'source_reported_unavailable' if flagged else
                      'reported_nonempty' if known and observations>0 else 'reported_empty' if known else 'unavailable_or_invalid'}
    known_positive=sum(r['count_known'] and r['reported_observations']>0 for r in rate_states.values()) if valid_rates else None
    complete_slots=valid_rates and all(r['count_known'] for r in rate_states.values())
    covered=known_positive if complete_slots else None
    metrics.append(metric('nyfed_reference_rates', covered, len(RATE_NAMES),
        'Named summary slots with positive integer n_obs, only when all five slots are known. Lowercase producer keys and uppercase aliases are recognized; conflicting aliases remain unknown. No history/date completeness claim.',
        'nyfed', unit='named_rate_slots', grain='one of SOFR/EFFR/OBFR/TGCR/BGCR',
        status='observed_summary_slots' if complete_slots else 'partial_or_unavailable_summary_slots',
        fields=['rates.<named_slot>.n_obs'], slot_states=rate_states,
        known_nonempty_slot_count=known_positive,
        unrecognized_rate_keys=sorted(k for k in rates if k.upper() not in RATE_NAMES) if valid_rates else None,
        denominator_definition='five explicitly named summary slots', history_completeness_verified=False))
    roll = docs['rollup']; feed_counts = roll.get('global_feed_counts') if roll is not None else None
    feeds = count(feed_counts.get('fred')) if isinstance(feed_counts, dict) else None
    metrics.append(metric('fred_feeds_in_use', feeds, None,
        'Reported FRED feed count, not a unique series census. No external series denominator is applicable.',
        'rollup', unit='reported_feed_count', grain='reported feed-count summary',
        status='reported_unqualified_count' if feeds is not None else 'unavailable_or_invalid_count',
        fields=['global_feed_counts.fred'], legacy_unqualified_target=45000, series_census_verified=False))
    return {'measurement_contract': CONTRACT, 'generated_at': generated_at, 'as_of': generated_at,
        'as_of_basis': 'report generation, not source observation time', 'spec': 'Source-scoped inventory report',
        'metrics': metrics, 'sources': sources,
        'quality': {'status': 'inventory_only', 'unavailable_metric_count': sum(m['actual'] is None for m in metrics),
                    'source_freshness_verified': False, 'market_coverage_qualified': False, 'investment_authority': False},
        'note': 'Counts describe supplied artifacts at acquisition. Null means unavailable or invalid; a scoped zero is not proof of no market coverage. No Bloomberg/Refinitiv parity or forecast claim.'}
