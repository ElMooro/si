"""Dated provider flow measurements and deduplicated, matched-period group views."""
from collections import Counter
from decimal import Decimal, localcontext
import provider_flow_native as native
import provider_flow_catalog as catalog

CONTRACT = 'provider-fund-flow-research.v1'
PREFIX = 'data/provider-flow-research/'
CURRENT = 'data/provider-fund-flow-research.json'
RADAR_CONTRACT = 'capital-flow-native-research.v1'
RADAR_PREFIX = 'data/capital-radar-research/'
RADAR_CURRENT = 'data/capital-flow-radar.json'
PERMISSIONS = {key: False for key in ('forecast_qualified', 'calls_eligible', 'sizing_eligible', 'execution_eligible')}
WINDOWS = (1, 5, 21)
sha = native.sha
encoded = native.encoded
clock = native.clock
ds = native.ds


def amount(value): return Decimal(value) if value is not None else None


def permissions(): return {'call': None, 'portfolio_action': 'WAIT', **PERMISSIONS}


def fund_measurements(fund, reference_dates, reference_end):
    latest = fund['history'][-1] if fund['history'] else None
    valid = fund['quality']['status'] == 'complete_acquired_history'
    with localcontext() as ctx:
        ctx.prec = 50
        assets = (amount(latest['nav_decimal']) * amount(latest['shares_decimal'])
                  if valid and latest and latest['nav_decimal'] and latest['shares_decimal'] else None)
    windows = {str(n): native.window(fund, reference_dates, reference_end, n) for n in WINDOWS}
    own = {str(n): native.window(fund, reference_dates, fund['latest_effective_date'], n) for n in WINDOWS}
    return {key: fund[key] for key in ('ticker', 'acquisition_status', 'source_acquired_at', 'source_valid_until',
        'latest_effective_date', 'latest_processed_date', 'quality', 'revision_versions_selected')} | {
        'unit': 'USD', 'measure': 'provider_reported_creation_redemption_flow',
        'latest_observation': {'effective_date': latest['date'], 'processed_date': latest['processed_date'],
            'reported_flow_usd_decimal': latest['flow_decimal'] if valid else None,
            'nav_usd_decimal': latest['nav_decimal'] if valid else None,
            'shares_decimal': latest['shares_decimal'] if valid else None,
            'reported_assets_usd_decimal': ds(assets), 'source_rows': latest['source_rows']} if latest else None,
        'aligned_windows': windows, 'latest_windows': own, **permissions()}


def aggregate(members, funds, observations, end):
    requested = sorted(set(members)); eligible = {}; excluded = {}
    for ticker in requested:
        window = funds.get(ticker, {}).get('aligned_windows', {}).get(str(observations), {})
        if (window.get('status') == 'matched_reporting_window' and window.get('end_date') == end
                and window.get('flow_usd_decimal') is not None): eligible[ticker] = window
        else: excluded[ticker] = window.get('reasons') or ['fund_not_collected']
    grids = {tuple(w['dates']) for w in eligible.values()}
    if len(grids) > 1: raise ValueError('Aligned group date grids differ')
    complete = bool(requested) and not excluded and len(grids) == 1
    with localcontext() as ctx:
        ctx.prec = 50
        subtotal = sum((amount(w['flow_usd_decimal']) for w in eligible.values()), Decimal(0)) if eligible else None
        aums = [amount(w['end_reported_assets_usd_decimal']) for w in eligible.values()]
        assets = sum(aums, Decimal(0)) if complete and all(v is not None and v > 0 for v in aums) else None
        ratio = subtotal / assets * 100 if assets is not None else None
    return {'status': 'complete_matched_group' if complete else 'incomplete' if requested else 'not_applicable',
        'required': requested, 'included': sorted(eligible), 'excluded': excluded,
        'required_count': len(requested), 'included_count': len(eligible), 'end_date': end,
        'dates': list(next(iter(grids))) if grids else [], 'observations': observations,
        'flow_usd_decimal': ds(subtotal) if complete else None,
        'observed_subset_flow_usd_decimal': ds(subtotal),
        'reported_assets_usd_decimal': ds(assets), 'flow_to_end_assets_pct_decimal': ds(ratio),
        'scope': 'Unique configured funds on identical effective dates. A partial subtotal is not the full group; overlapping groups cannot be added.'}


def comparison(name, kind, members, funds, end):
    overlap = set(members.get('bull', [])) & set(members.get('bear', []))
    if overlap: raise ValueError('Same fund appears in both comparison legs')
    periods = {}
    for n in WINDOWS:
        bull = aggregate(members.get('bull', []), funds, n, end)
        bear = aggregate(members.get('bear', []), funds, n, end)
        matched = (bull['status'] == bear['status'] == 'complete_matched_group' and bull['dates'] == bear['dates'])
        with localcontext() as ctx:
            ctx.prec = 50
            a = amount(bull['flow_usd_decimal']); b = amount(bear['flow_usd_decimal'])
            total = a + b if matched else None; difference = a - b if matched else None
        periods[str(n)] = {'bull': bull, 'bear': bear, 'status': 'matched' if matched else 'incomplete_or_not_applicable',
            'total_reported_fund_flow_usd_decimal': ds(total), 'bull_minus_bear_flow_usd_decimal': ds(difference)}
    return {'name': name, 'kind': kind, 'windows': periods,
        'scope': 'Configured strategy labels. Bull plus inverse measures fund-level creations/redemptions; bull minus inverse is a separate unscaled comparison. Neither measures underlying exposure, investor identity or a forecast.'}


def groups(funds, end):
    complexes = []
    for name, members in catalog.COMPLEXES.items():
        windows = {str(n): aggregate(members['core'], funds, n, end) for n in WINDOWS}
        pace = None; five = windows['5']; twentyone = windows['21']
        if (five['status'] == twentyone['status'] == 'complete_matched_group'
                and five['dates'] == twentyone['dates'][-5:]):
            with localcontext() as ctx:
                ctx.prec = 50
                recent = amount(five['flow_usd_decimal']); full = amount(twentyone['flow_usd_decimal'])
                pace = recent / 5 - (full - recent) / 16
        complexes.append({'name': name, 'primary': members['primary'], 'windows': windows,
            'recent_minus_prior_pace_usd_per_observation_decimal': ds(pace),
            'configured_stock_context': members.get('stocks', []),
            'stock_context_scope': 'Configured related names, not verified fund holdings or recipients of the measured flow.',
            **permissions()})
    comparisons = [comparison(name, 'complex', members, funds, end) for name, members in catalog.COMPLEXES.items()
                   if members.get('bull') or members.get('bear')]
    comparisons.extend(comparison(name, 'single_stock', members, funds, end) for name, members in catalog.SINGLE_STOCK_LEV.items())
    membership = list(catalog.COMPLEXES.values()) + list(catalog.SINGLE_STOCK_LEV.values())
    unique = {leg: sorted({ticker for group in membership for ticker in group.get(leg, [])}) for leg in ('bull', 'bear')}
    leveraged = comparison('Unique configured leveraged funds', 'deduplicated', unique, funds, end)
    categories = {}
    for category in sorted({r['category'] for r in catalog.ETF_UNIVERSE.values()}):
        members = [t for t, row in catalog.ETF_UNIVERSE.items() if row['category'] == category]
        categories[category] = {str(n): aggregate(members, funds, n, end) for n in WINDOWS}
    return {'complexes': complexes, 'comparisons': comparisons, 'unique_leveraged': leveraged, 'categories': categories}


def build(inputs, read):
    if inputs.get('contract') != 'provider-flow-inputs.v1': raise ValueError('Original provider input contract required')
    at = inputs['generated_at']; clock(at)
    if set(inputs.get('collections', {})) != set(catalog.ETF_UNIVERSE):
        raise ValueError('Complete configured fund inventory required')
    reconstructed = {ticker: native.reconstruct(ticker, inputs['collections'][ticker], read, at)
                     for ticker in sorted(catalog.ETF_UNIVERSE)}
    spy = reconstructed['SPY']
    reference_dates = ([r['date'] for r in spy['history']]
                       if spy['quality']['status'] == 'complete_acquired_history' else [])
    end = reference_dates[-1] if reference_dates else None
    funds = {}; histories = {}
    for ticker, fund in reconstructed.items():
        history = {'contract': 'provider-flow-history.v1', 'ticker': ticker,
            'history': fund['history'], 'originals': fund['originals'], 'rejected_rows': fund['rejected_rows'],
            'reconciliation': native.reconcile(fund, reference_dates), 'quality': fund['quality'],
            'vintage': 'Latest processed version within this acquisition. Acquisition time is known; historical first public availability is not certified.'}
        raw = encoded(history); digest = sha(raw); key = PREFIX + 'histories/' + digest + '.json'; histories[key] = raw
        row = fund_measurements(fund, reference_dates, end)
        classification = catalog.ETF_UNIVERSE[ticker]
        row.update(classification={k: classification.get(k) for k in ('category', 'subcategory', 'region', 'ref_sector')},
            classification_scope='Configured current research tags; no investor identity or historical membership is inferred.',
            history={'key': key, 'sha256': digest, 'bytes': len(raw)}, originals=fund['originals'])
        funds[ticker] = row
    grid = {'contract': 'provider-reporting-reference.v1', 'ticker': 'SPY', 'dates': reference_dates,
            'source_history': funds['SPY']['history'], 'exchange_calendar_verified': False,
            'scope': 'Observed SPY provider effective dates, not a complete exchange or issuer reporting calendar.'}
    raw = encoded(grid); digest = sha(raw); key = PREFIX + 'histories/' + digest + '.json'; histories[key] = raw
    reference = {'key': key, 'sha256': digest, 'bytes': len(raw), 'end_date': end, 'observations': len(reference_dates)}
    valid = [f for f in funds.values() if f['quality']['status'] == 'complete_acquired_history']
    due = min((f['source_valid_until'] for f in valid), key=clock) if valid else at
    quality = dict(Counter(f['quality']['status'] for f in funds.values()))
    group_views = groups(funds, end)
    return {'contract': CONTRACT, 'engine': 'justhodl-etf-fund-flows', 'version': '2.0.0',
        'generated_at': at, 'source_valid_until': due, 'reference': reference, 'funds': funds, **group_views,
        'quality': {'status': 'complete_sources' if len(valid) == len(funds) else 'partial' if valid else 'unavailable',
            'configured_funds': len(funds), 'source_status_counts': quality,
            'matched_five_observation_funds': sum(f['aligned_windows']['5']['status'] == 'matched_reporting_window' for f in funds.values()),
            'independent_investment_votes': 0, 'exchange_calendar_verified': False,
            'historical_first_availability_verified': False},
        'unique_configured_universe': {str(n): aggregate(list(funds), funds, n, end) for n in WINDOWS},
        'dependency_graph': {'root': 'ETF Global fund-flow response via Massive/Polygon',
            'views': ['fund histories', 'category groups', 'complexes', 'leveraged comparisons', 'Capital Flow Radar'],
            'additional_independent_investment_votes': 0},
        'legacy_contexts': inputs['contexts'], 'retained_prior_publication': inputs.get('previous'),
        'provider_requests': inputs.get('provider_requests', 0),
        'methodology': {'flow': 'Provider-reported creation/redemption flow, not exchange turnover or identified investor purchases.',
            'dates': 'Effective date is the observation; processed date identifies the acquired provider version. Missing dates are never replaced by processing dates.',
            'windows': 'Exact listed SPY reporting dates required, separately at the common reference end and each fund latest end. Missing flow stays null; reported zero stays zero.',
            'revisions': 'Latest processed version per effective date; a conflicting same-version observation is withheld, never replaced with an older value.',
            'reconciliation': 'Prior-NAV and current-NAV share-change arithmetic are shown separately; neither substitutes for provider-reported flows.',
            'groups': 'All requested funds and exact matching dates required for a group total. Observed subset sums and excluded funds remain explicit.',
            'authority': 'No flow-to-return coefficient, smart/dumb money classification, calibrated probability or allocation authority.'},
        'notifications_sent': 0, 'signals_emitted': 0, 'paid_ai_calls': 0, 'private_account_reads': 0, 'portfolio_writes': 0,
        **permissions()}, histories


def radar(packet, at, retained_current, previous):
    if packet.get('contract') != CONTRACT or any(packet.get(k) is not False for k in PERMISSIONS):
        raise ValueError('Reviewed provider research required')
    if clock(at) < clock(packet['generated_at']): raise ValueError('Radar compilation precedes its source')
    return {'contract': RADAR_CONTRACT, 'engine': 'justhodl-capital-flow-radar', 'version': '5.0.0',
        'generated_at': at, 'source_generated_at': packet['generated_at'], 'source_valid_until': packet['source_valid_until'],
        'canonical_replay': packet['replay'], 'reference': packet['reference'],
        'complexes': packet['complexes'], 'comparisons': packet['comparisons'],
        'unique_leveraged': packet['unique_leveraged'], 'unique_configured_universe': packet['unique_configured_universe'],
        'funds': packet['funds'], 'quality': {**packet['quality'], 'independent_investment_votes': 0},
        'dependency_graph': packet['dependency_graph'], 'methodology': packet['methodology'],
        'retained_canonical_source': retained_current, 'retained_predecessor': previous, **permissions()}


def compatibility(packet, target):
    """Retire ambiguous legacy score shapes; point to the complete native data.

    These small views contain no synthetic neutral metrics or missing-as-zero
    values. The complete old bytes are retained in legacy_contexts.
    """
    if packet.get('contract') != CONTRACT or 'replay' not in packet:
        raise ValueError('Retained native publication required for compatibility')
    return {'contract': 'provider-flow-compatibility.v1', 'source_key': target,
        'generated_at': packet['generated_at'], 'source_valid_until': packet['source_valid_until'],
        'status': 'superseded_by_native_research',
        'canonical': {'key': CURRENT, 'replay': packet['replay']},
        'quality': {'status': 'research_only', 'independent_investment_votes': 0},
        'reason': 'Use the canonical dated observations. Old scores, investor labels, implied stock purchases and unvalidated event-study calls are unavailable.',
        'metrics': [], 'composite': {}, 'by_category': {}, 'context': {}, 'events': [],
        'inventory': {t: {'ticker': t, **r['classification']} for t, r in packet['funds'].items()},
        'retained_predecessor': packet['legacy_contexts'].get(target),
        **permissions()}
