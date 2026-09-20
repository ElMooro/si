"""Keep holdings-value bridges, fund issuance estimates and TIC transactions distinct."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re

import capital_bridge as bridge

PREFIX = 'data/capital-research/'
CURRENT = 'data/capital-flow.json'
NATIVE = 'data/holdings-research/'
CANONICAL = NATIVE+'canonical/'
FLOW = 'data/flow-desk-research/'
CONTRACT = 'capital-evidence-research.v1'
MAX_BYTES = 64*1024*1024
PERMISSION = bridge.PERMISSION
encoded = bridge.encoded


def digest(value): return hashlib.sha256(encoded(value)).hexdigest()


def decode(raw):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BYTES: raise ValueError('Complete bounded JSON required')
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value: raise ValueError('Duplicate JSON key')
            value[key] = item
        return value
    def reject(value): raise ValueError('Nonfinite JSON token: '+value)
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=reject)


def clock(value):
    value = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if value.tzinfo is None: raise ValueError('Timezone-aware source clock required')
    return value.astimezone(timezone.utc)


def reference(key, raw): return {'key': key, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}


def verified(ref, read, prefix, kind):
    sha = ref.get('sha256', '')
    if not re.fullmatch('[a-f0-9]{64}', sha) or ref.get('key') != prefix+kind+'/'+sha+'.json':
        raise ValueError('Immutable artifact identity differs')
    raw = read(ref['key'])
    if len(raw) != ref.get('bytes') or hashlib.sha256(raw).hexdigest() != sha:
        raise ValueError('Immutable artifact bytes differ')
    return decode(raw)


def source(read, pinned=None):
    holding_pointer = decode(read('data/13f-positions.json')) if pinned is None else None
    holding_ref = holding_pointer.get('canonical_replay', {}) if pinned is None else pinned['holdings']['manifest']
    holding_manifest = verified(holding_ref, read, CANONICAL, 'runs')
    if holding_manifest.get('contract') != 'holdings-canonical-replay.v1': raise ValueError('Native canonical holdings required')
    holding_output = holding_manifest['products']['data/13f-positions.json']
    holdings = verified(holding_output, read, CANONICAL, 'products')
    if holdings.get('contract') != 'holdings-canonical.v1' or holdings['research'] != holding_manifest['research']:
        raise ValueError('Native holdings binding differs')
    if holding_pointer is not None and {k: v for k, v in holding_pointer.items() if k != 'canonical_replay'} != holdings:
        raise ValueError('Current holdings differ from immutable evidence')
    flow_pointer = decode(read('data/global-flow-desk.json')) if pinned is None else None
    if flow_pointer is not None:
        key = flow_pointer.get('replay', {}).get('manifest_key', '')
        if not re.fullmatch(re.escape(FLOW)+r'runs/[a-f0-9]{64}\.json', key): raise ValueError('Native fund-flow manifest required')
        raw = read(key); flow_ref = reference(key, raw)
    else: flow_ref = pinned['fund_flows']['manifest']
    flow_manifest = verified(flow_ref, read, FLOW, 'runs')
    if flow_manifest.get('contract') != 'flow-desk-replay.v1': raise ValueError('Fund-flow research manifest required')
    flows = verified(flow_manifest['output'], read, FLOW, 'outputs')
    if flows.get('contract') != 'global-flow-research.v1' or flow_manifest['output_sha256'] != digest(flows):
        raise ValueError('Fund-flow output binding differs')
    if flow_pointer is not None and (flows != {k: v for k, v in flow_pointer.items() if k != 'replay'}
            or flow_pointer['replay']['output_sha256'] != flow_manifest['output']['sha256']):
        raise ValueError('Current fund research differs from retained evidence')
    bindings = {'holdings': {'manifest': holding_ref, 'output': holding_output, 'native_research': holding_manifest['research']},
                'fund_flows': {'manifest': flow_ref, 'output': flow_manifest['output']}}
    if pinned is not None and bindings != pinned: raise ValueError('Pinned source bindings differ')
    return holdings, flows, bindings


def freshness(holdings, flows, at):
    now = clock(at)
    sources = {}
    for name, packet in (('holdings', holdings), ('fund_flows', flows)):
        generation = clock(packet['generated_at']); observation = clock(packet['source_generated_at'])
        if generation > now or observation > generation: raise ValueError('Future or inconsistent source clocks')
        sources[name] = {'source_generated_at': packet['source_generated_at'], 'calculated_at': packet['generated_at'],
                         'source_age_hours': (now-observation).total_seconds()/3600,
                         'fresh_until': (observation+timedelta(hours=48)).isoformat(),
                         'collection_stale': now-observation > timedelta(hours=48)}
    return sources


def build(holdings, flows, bindings, read, at, legacy):
    if digest(holdings) != bindings['holdings']['output']['sha256'] or digest(flows) != bindings['fund_flows']['output']['sha256']:
        raise ValueError('Complete source output binding required')
    clocks = freshness(holdings, flows, at)
    if set(legacy) != {CURRENT, 'data/capital-flow-history.json'}:
        raise ValueError('Both whole legacy publications must remain retained')
    for key, item in legacy.items():
        if item.get('qualification') != 'unqualified_legacy_calculation': raise ValueError('Legacy authority differs')
        if not isinstance(verified(item['artifact'], read, PREFIX, 'legacy'), dict): raise ValueError('Whole legacy packet required')
    if len(holdings['by_fund']) > 256: raise ValueError('Partition the complete manager roster before expansion')
    artifacts, funds, ciks = {}, {}, set()
    for name, summary in sorted(holdings['by_fund'].items()):
        detail = verified(summary['positions_ref'], read, NATIVE, 'funds')
        if (detail['fund'] != name or detail['cik'] != summary['cik'] or detail['official_name'] != summary['official_name']
                or detail['current_holdings_period'] != summary['period_of_report']): raise ValueError('Manager binding differs')
        if detail['cik'] in ciks: raise ValueError('Duplicate manager requires alias review')
        ciks.add(detail['cik'])
        current = detail['periods'].get(detail['current_holdings_period'], {})
        if len(current.get('positions', {})) != summary['positions_count'] or current.get('status', 'not_acquired') != summary['current_chain_status']:
            raise ValueError('Complete manager count or chain differs')
        for date in (detail['current_holdings_period'], detail['prior_holdings_period']):
            if date and (not re.fullmatch(r'\d{4}-\d{2}-\d{2}', date) or datetime.fromisoformat(date).date() > clock(at).date()):
                raise ValueError('Unambiguous nonfuture disclosure date required')
        if detail['current_holdings_period'] and detail['prior_holdings_period'] and detail['prior_holdings_period'] >= detail['current_holdings_period']:
            raise ValueError('Disclosure period ordering differs')
        doc = {**bridge.build_manager(detail), 'source': {'native_fund': summary['positions_ref'], **bindings['holdings']}}
        raw = encoded(doc)
        if len(raw) > MAX_BYTES: raise ValueError('Complete manager bridge exceeds bound; partition explicitly')
        key = PREFIX+'managers/'+hashlib.sha256(raw).hexdigest()+'.json'; artifacts[key] = raw
        funds[name] = {k: doc[k] for k in ('fund', 'cik', 'official_name', 'current_report_period', 'prior_report_period',
                    'current_chain_status', 'prior_chain_status', 'comparison_available', 'value_reviews', 'record_count', 'bridge_status_counts')}
        funds[name].update(bridge=reference(key, raw), native_fund=summary['positions_ref'],
                          current_cohort=detail['current_holdings_period'] == holdings['required_period_for_current_cohort'])
    count = sum(v['record_count'] for v in funds.values())
    if count != holdings['manager_comparison_count'] or len(funds) != holdings['funds_total']:
        raise ValueError('Complete holdings universe differs')
    # These are references to distinct measured products, never blended votes.
    funds_flow = {'measurement': 'nav_valued_share_change_estimate', 'unit': 'usd',
        'period': flows['period'], 'source': bindings['fund_flows'], 'json_pointer': '/funds',
        'funds': flows['funds'], 'quality': flows['quality'], 'source_generated_at': flows['source_generated_at'],
        'interpretation': 'Changes in issuer-reported fund shares valued at NAV. This does not measure stock purchases, investor identity or national capital flows.',
        'research_snapshot': '/global-flow-desk.html?run='+bindings['fund_flows']['manifest']['sha256']}
    tic = {'measurement': 'monthly_treasury_securities_transactions', 'source': bindings['fund_flows'],
           'json_pointer': '/tic_context', 'context': flows['tic_context'],
           'interpretation': 'Keep the Treasury series definition, monthly period and USD-million unit. Securities holdings are separate from transactions; no sum with ETF estimates or manager values.'}
    unavailable = [name for name, fund in funds.items() if not fund['comparison_available']]
    status = 'stale' if any(v['collection_stale'] for v in clocks.values()) else 'partial'
    return {'contract': CONTRACT, 'engine': 'capital-flow', 'version': '3.0.0', 'generated_at': at,
        'source': bindings, 'source_clocks': clocks, 'managers': funds,
        'required_period_for_current_cohort': holdings['required_period_for_current_cohort'],
        'counts': {'managers': len(funds), 'disclosure_comparisons': count,
                   'managers_with_comparisons': len(funds)-len(unavailable),
                   'current_cohort_managers': sum(v['current_cohort'] for v in funds.values()),
                   'configured_etfs': len(flows['funds'])},
        'quality': {'status': status, 'unavailable_manager_comparisons': unavailable,
                    'scope': 'Source collection age, holdings report periods and fund-issuance observation dates remain separate. Coverage is partial; no market-wide total or stock cash flow is measured.'},
        'fund_issuance': funds_flow, 'foreign_transactions': tic, 'legacy_contexts': legacy,
        'measurement_families': ['reported_holdings_value_bridge', 'nav_valued_fund_share_change', 'monthly_treasury_transactions'],
        'dependency_roots': sorted(set(['US_SEC:13F']+flows['dependency_roots'])),
        'retired_model_inputs': {'screener/data.json': 'Ownership percentage scales and independent investor identity were not established.',
            'data/etf-flows.json': 'Trading volume and returns are not fund issuance or redemptions.',
            'data/etf-fund-flows.json': 'Unqualified fallback is not substituted for original issuer observations.',
            'provider_symbol_positions_summary': 'Whole legacy enrichment remains in retained context; historical API originals were not retained, so valuation changes are not qualified as transactions.'},
        'compatibility': {'legacy_stock_flow_scores': 'retired', 'empty_rankings_mean': 'No qualified stock cash-flow ranking, not no institutional activity.',
                          'consumer_migration_complete': False},
        'accumulating': [], 'distributing': [], 'by_ticker': {}, 'dollar_flow_in': [], 'dollar_flow_out': [],
        'sector_dollar_flows': [], 'etf_flows_in': [], 'etf_flows_out': [], 'category_rotation': [],
        'lens_conflicts': [], 'top_new_positions': [], 'summary': {'n_scored': None},
        'methodology': 'Separate measured quantities and periods, with an exact per-security accounting bridge. No guessed percentage scale, ticker join, trade inference or blend of correlated inputs.',
        'portfolio_impact': None, **PERMISSION}, artifacts
