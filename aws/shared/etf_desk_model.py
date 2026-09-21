"""One dated ETF desk built from profiles and existing canonical source families."""
from collections import Counter
from datetime import timedelta
from decimal import Decimal, localcontext
import json
import etf_desk_catalog as catalog
import etf_profile_native as profile
import provider_flow_native as flow
import provider_flow_model as flow_model
import etf_holdings_native as holdings
import etf_holdings_model as holdings_model

CONTRACT = 'etf-desk-original-research.v1'
PREFIX = 'data/etf-desk-research/'
CURRENT = 'data/etf-desk-research.json'
MAX_PUBLIC_ARTIFACT = 16 * 1024 * 1024
sha, encoded, clock = profile.sha, profile.encoded, profile.clock
PERMISSIONS = flow_model.PERMISSIONS
permissions = flow_model.permissions


def artifact(doc, kind, emit):
    raw = encoded(doc)
    if not 0 < len(raw) <= MAX_PUBLIC_ARTIFACT: raise ValueError('Desk artifact byte bound')
    digest = sha(raw); key = PREFIX + kind + '/' + digest + '.json'; emit(key, raw)
    return {'key': key, 'sha256': digest, 'bytes': len(raw)}


def checked(ref, read):
    raw = read(ref['key'])
    if len(raw) != ref['bytes'] or sha(raw) != ref['sha256']: raise ValueError('Desk source reference differs')
    return json.loads(raw)


def retained_profile(snapshot, emit):
    ref = artifact({'contract': 'etf-desk-profile-snapshot.v1', **snapshot}, 'profiles', emit)
    current = snapshot['profiles'][0] if snapshot['quality']['single_profile_unambiguous'] else None
    return {key: snapshot[key] for key in ('ticker', 'cutoff', 'acquisition_status', 'processed_date',
        'effective_date', 'source_acquired_at', 'source_valid_until', 'quality')} | {
        'snapshot': ref, 'selected_profile': snapshot['selected_profile'],
        'summary': {'text': {k: current['text'][k] for k in ('description','issuer','primary_benchmark','asset_class','product_type','leverage_style')},
                    'numeric': current['numeric']} if current else None}


def profile_changes(current, prior):
    a, b = current.get('summary'), prior.get('summary')
    ready = (a is not None and b is not None and current['effective_date'] > prior['effective_date'])
    out = []
    for field in profile.UNITS:
        left = a['numeric'][field] if a else None; right = b['numeric'][field] if b else None
        usable = ready and all(v['status'] == 'reported' for v in (left, right))
        with localcontext() as ctx:
            ctx.prec = 180
            delta = Decimal(left['value_decimal']) - Decimal(right['value_decimal']) if usable else None
        out.append({'field': field, 'status': 'raw_reported_difference' if usable else 'not_comparable',
            'current_raw_decimal': left['value_decimal'] if left else None,
            'prior_raw_decimal': right['value_decimal'] if right else None,
            'difference_raw_decimal': profile.ds(delta), 'unit': profile.UNITS[field][0],
            'unit_certified': profile.UNITS[field][1],
            'current_source': left['source'] if left else None, 'prior_source': right['source'] if right else None})
    return {'current_effective_date': current['effective_date'], 'prior_effective_date': prior['effective_date'],
        'distinct_effective_dates': ready, 'fields': out,
        'scope': 'Profile field differences on their own effective dates. No annualization, issuer transaction, fee conversion, or holdings change is inferred.'}


def extra_holdings(pair, read, generated_at, emit):
    snapshots = [holdings.reconstruct_or_reject(pair[role], read, generated_at) for role in ('current', 'prior')]
    a, b = [holdings_model.retain_snapshot(s, emit) for s in snapshots]
    comparison = holdings.compare(*snapshots); parts = []
    for start in range(0, len(comparison['rows']), holdings_model.CHUNK_ROWS):
        parts.append(holdings_model.artifact({'contract': 'etf-holdings-position-comparison-rows.v1',
            'ticker': a['ticker'], 'row_offset': start, 'rows': comparison['rows'][start:start+holdings_model.CHUNK_ROWS]}, 'comparisons', emit))
    body = {k: v for k, v in comparison.items() if k != 'rows'}
    body.update(contract='etf-holdings-position-comparison.v1', parts=parts,
        current_snapshot=a['snapshot'], prior_snapshot=b['snapshot'], compared_identities=len(comparison['rows']))
    return {'ticker': a['ticker'], 'current': a, 'prior': b,
        'comparison': holdings_model.artifact(body, 'comparisons', emit),
        'comparable_snapshots': body['comparable_snapshots'], 'comparison_status_counts': body['identity_status_counts'],
        **permissions()}


def extra_flow(ticker, collection, read, generated_at, dates, end, emit):
    fund = flow.reconstruct(ticker, collection, read, generated_at)
    history = {'contract': 'provider-flow-history.v1', 'ticker': ticker,
        'history': fund['history'], 'originals': fund['originals'], 'rejected_rows': fund['rejected_rows'],
        'reconciliation': flow.reconcile(fund, dates), 'quality': fund['quality'],
        'vintage': 'Latest processed version within this acquisition. Acquisition time is known; historical first public availability is not certified.'}
    raw = encoded(history); digest = sha(raw); key = flow_model.PREFIX + 'histories/' + digest + '.json'; emit(key, raw)
    return {**flow_model.fund_measurements(fund, dates, end),
        'history': {'key': key, 'sha256': digest, 'bytes': len(raw)}, 'originals': fund['originals']}


def legacy_history(ticker, contexts, read):
    key = 'data/etf-flow-hist/' + ticker + '.json'; ref = contexts.get(key)
    if not ref: return {'status': 'no_retained_predecessor', 'source_key': key}
    doc = checked(ref, read)
    dates = doc.get('d') if isinstance(doc.get('d'), list) else []
    if not dates and isinstance(doc.get('rows'), list):
        dates = [r.get('d') or r.get('processed_date') or r.get('effective_date') for r in doc['rows'] if isinstance(r, dict)]
    dates = [d for d in dates if isinstance(d, str)]
    return {'status': 'unqualified_preserved_history', 'source_key': key, 'retained_original': ref,
        'reported_rows': len(dates), 'reported_first_date': min(dates) if dates else None,
        'reported_last_date': max(dates) if dates else None,
        'original_generation_clock': doc.get('generated_at'), 'merged_into_verified_history': False,
        'scope': 'Complete predecessor bytes retained. Legacy processing-date semantics, original availability and completeness remain unverified.'}


def build(inputs, read, emit, canonical_flow, canonical_holdings, previous=None):
    if inputs.get('contract') != 'etf-desk-inputs.v1': raise ValueError('Reviewed desk inputs required')
    generated = inputs['generated_at']; now = clock(generated); query = profile.day(inputs['query_date'])
    if query > now.date(): raise ValueError('Desk query cutoff after compilation')
    if canonical_flow.get('contract') != flow_model.CONTRACT or canonical_holdings.get('contract') != holdings_model.CONTRACT:
        raise ValueError('Canonical native flow and holdings sources required')
    for source in (canonical_flow, canonical_holdings):
        if clock(source['generated_at']) > now or any(source.get(k) is not False for k in PERMISSIONS):
            raise ValueError('Canonical source clock or authority differs')
    if set(canonical_flow['funds']) != set(canonical_holdings['funds']): raise ValueError('Canonical source universes differ')
    universe = set(catalog.DESK); extras = universe - set(canonical_flow['funds'])
    if set(inputs['profiles']) != universe or set(inputs['extra_flows']) != extras or set(inputs['extra_holdings']) != extras:
        raise ValueError('Complete desk source coverage required, including every additional fund')
    grid = checked(canonical_flow['reference'], read)
    if grid['contract'] != 'provider-reporting-reference.v1' or grid['ticker'] != 'SPY': raise ValueError('Canonical SPY reporting grid required')
    dates = grid['dates']; end = canonical_flow['reference']['end_date']; funds = {}
    counts = Counter(); flow_funds = {}
    for ticker in sorted(universe):
        pair = inputs['profiles'][ticker]
        if set(pair) != {'current','prior'}: raise ValueError('Both profile acquisition cutoffs required')
        profiles = {}
        for role in ('current', 'prior'):
            cutoff = query if role == 'current' else query - timedelta(days=30)
            if pair[role]['ticker'] != ticker or pair[role]['cutoff'] != cutoff.isoformat(): raise ValueError('Exact fund profile cutoff required')
            profiles[role] = retained_profile(profile.reconstruct_or_reject(pair[role], read, generated), emit)
        profiles['comparison'] = profile_changes(profiles['current'], profiles['prior'])
        old = (previous or {}).get('funds', {}).get(ticker, {})
        if profiles['current']['quality']['status'] != 'complete_returned_profile_snapshot':
            retained = old.get('profiles', {}).get('current')
            if not retained or retained['quality']['status'] != 'complete_returned_profile_snapshot':
                retained = old.get('profiles', {}).get('retained_previous_current')
            if retained:
                profiles['retained_previous_current'] = retained
                profiles['retained_previous_eligible_as_current'] = False
        if ticker in extras:
            flow_collection = inputs['extra_flows'][ticker]; holdings_pair = inputs['extra_holdings'][ticker]
            if flow_collection['ticker'] != ticker or flow_collection['query_date'] != query.isoformat(): raise ValueError('Additional flow identity/cutoff differs')
            if set(holdings_pair) != {'current','prior'}: raise ValueError('Both additional holdings cutoffs required')
            for role in ('current','prior'):
                expected = query if role == 'current' else query - timedelta(days=30)
                if holdings_pair[role]['ticker'] != ticker or holdings_pair[role]['cutoff'] != expected.isoformat(): raise ValueError('Additional holdings identity/cutoff differs')
            f = extra_flow(ticker, flow_collection, read, generated, dates, end, emit)
            h = extra_holdings(holdings_pair, read, generated, emit); basis = 'additional_desk_originals'
        else:
            f = canonical_flow['funds'][ticker]; h = canonical_holdings['funds'][ticker]; basis = 'canonical_original_replay'
        profile_current = profiles['current']; holdings_current = h['current']
        counts['profiles_current_eligible'] += bool(profile_current['quality']['current_profile_eligible'])
        counts['holdings_complete'] += holdings_current['quality']['status'] == 'complete_returned_snapshot'
        flow_current = f['quality']['status'] == 'complete_acquired_history' and now < clock(f['source_valid_until'])
        counts['flows_current_eligible'] += flow_current
        counts['funds_with_current_profiles_flows_holdings'] += bool(flow_current and profile_current['quality']['current_profile_eligible'] and holdings_current['quality']['status'] == 'complete_returned_snapshot' and now < clock(holdings_current['source_valid_until']))
        flow_funds[ticker] = f
        profile_count = (profile_current.get('summary') or {}).get('numeric',{}).get('num_holdings',{}).get('value_decimal')
        dates_h = list(holdings_current['effective_dates'])
        funds[ticker] = {'ticker': ticker, 'source_basis': basis, 'profiles': profiles, 'flows': f, 'holdings': h,
            'legacy_history': legacy_history(ticker, inputs['contexts'], read),
            'quality': {'flow_current_eligible': flow_current, 'profile_current_eligible': profile_current['quality']['current_profile_eligible'],
                'holdings_source_check_current': bool(holdings_current['source_valid_until'] and now < clock(holdings_current['source_valid_until'])),
                'independent_investment_votes': 0},
            'reported_count_comparison': {'profile_count_decimal': profile_count,
                'profile_effective_date': profile_current['effective_date'], 'holdings_effective_dates': dates_h,
                'returned_constituent_rows': holdings_current['quality'].get('returned_rows'),
                'same_effective_date': dates_h == [profile_current['effective_date']], 'equivalent_counting_scope_verified': False,
                'scope': 'Profile holdings count and returned constituent rows can differ in date and counting convention; no missing-position claim follows.'}, **permissions()}
    # Canonical rows keep their original clocks. Even a formerly complete window
    # cannot become current again when merely copied into a newer desk packet.
    totals = {}
    for n in flow_model.WINDOWS:
        eligible = {t: f for t, f in flow_funds.items() if funds[t]['quality']['flow_current_eligible']}
        totals[str(n)] = flow_model.aggregate(sorted(universe), eligible, n, end)
    return {'contract': CONTRACT, 'version': '2.0.0', 'engine': 'justhodl-etf-global-desk',
        'generated_at': generated, 'query_date': query.isoformat(), 'funds': funds,
        'canonical_sources': {'flows': {'generated_at': canonical_flow['generated_at'], 'replay': canonical_flow['replay'], 'retained': inputs['canonical_flows']},
            'holdings': {'generated_at': canonical_holdings['generated_at'], 'replay': canonical_holdings['replay'], 'retained': inputs['canonical_holdings']}},
        'reference': canonical_flow['reference'], 'matched_desk_totals': totals,
        'quality': {'status': 'partial' if any(counts.values()) else 'unavailable', 'configured_funds': len(funds),
            'canonical_overlap': len(universe)-len(extras), 'additional_funds': sorted(extras), **dict(counts),
            'independent_investment_votes': 0, 'weight_unit_certified': False, 'profile_fee_scale_certified': False},
        'legacy_contexts': inputs['contexts'], 'retained_prior_publication': inputs.get('previous'),
        'provider_requests': inputs['provider_requests'], 'original_provider_bytes': inputs['original_provider_bytes'],
        'methodology': {'profile': 'Original profile rows with separate effective, processing and acquisition clocks. Profile dates never replace constituent dates.',
            'flows': 'Canonical fund-flow originals reused once; additional desk funds use the same SPY reporting grid. Reporting windows are not an independently verified exchange calendar.',
            'holdings': 'Complete returned constituent rows, including cash, bonds and derivatives. Raw weights and market-value currency remain unqualified; no trades or underlying stock purchases are inferred.',
            'history': 'All predecessor histories retained whole and kept distinct from original-source verified observations. No shortened tape is presented as complete lifetime history.',
            'units': 'Only endpoint-specific documented units are certified. No magnitude-based fee conversion, normalization or automatic currency inference.',
            'portfolio': 'Descriptive research supplies no calibrated return, allocation or sizing instruction. Portfolio scenarios require explicit user assumptions and costs.',
            'source_documentation': profile.DOCUMENTATION},
        'dependency_graph': {'provider_roots': ['ETF Global via Massive/Polygon'],
            'measurement_families': ['fund-flow originals','constituent originals','profile originals'],
            'canonical_views_reused': ['provider-fund-flow-research','etf-holdings-research'], 'additional_independent_investment_votes': 0},
        'private_account_reads': 0, 'paid_ai_calls': 0, 'notifications_sent': 0, 'signals_emitted': 0, 'portfolio_writes': 0,
        **permissions()}
