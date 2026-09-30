"""Full dated holdings, exact-identity membership and inspectable source pages."""
from collections import Counter, defaultdict
import etf_holdings_native as native
import provider_flow_catalog as catalog

PREFIX = 'data/etf-holdings-research/'
CURRENT = 'data/etf-holdings-research.json'
CONTRACT = 'etf-holdings-original-research.v1'
LOOK_PREFIX = 'data/holdings-lookthrough-research/'
LOOK_CURRENT = 'data/flow-lookthrough.json'
LOOK_CONTRACT = 'etf-holdings-lookthrough.v1'
PERMISSIONS = {key: False for key in ('forecast_qualified', 'calls_eligible', 'sizing_eligible', 'execution_eligible')}
sha, encoded, clock = native.sha, native.encoded, native.clock
CHUNK_ROWS = 250
MAX_PUBLIC_ARTIFACT = 16 * 1024 * 1024


def permissions(): return {'call': None, 'portfolio_action': 'WAIT', **PERMISSIONS}


def artifact(doc, kind, emit):
    raw = encoded(doc)
    if not 0 < len(raw) <= MAX_PUBLIC_ARTIFACT: raise ValueError('Reviewed public artifact byte bound')
    digest = sha(raw);key = PREFIX + kind + '/' + digest + '.json'
    emit(key, raw)
    return {'key': key, 'sha256': digest, 'bytes': len(raw)}


def retain_snapshot(snapshot, emit):
    rows = snapshot['rows'];parts = [];index = []
    for start in range(0, len(rows), CHUNK_ROWS):
        chunk = rows[start:start + CHUNK_ROWS];number = len(parts)
        doc = {'contract': 'etf-holdings-rows.v1', 'ticker': snapshot['ticker'],
            'processed_date': snapshot['processed_date'], 'row_offset': start, 'rows': chunk}
        parts.append(artifact(doc, 'rows', emit))
        for row in chunk:
            index.append({key: row[key] for key in ('row_id', 'identity_key', 'constituent_ticker',
                'constituent_name', 'effective_date', 'asset_class', 'security_type', 'currency_traded',
                'constituent_rank', 'figi', 'isin', 'us_code', 'sedol')} | {'part': number})
    body = {k: v for k, v in snapshot.items() if k != 'rows'}
    index_parts = [artifact({'contract': 'etf-holdings-row-index.v1', 'ticker': snapshot['ticker'],
        'row_offset': start, 'rows': index[start:start + 500]}, 'snapshots', emit) for start in range(0, len(index), 500)]
    body.update(contract='etf-holdings-snapshot.v1', parts=parts, index_parts=index_parts, indexed_rows=len(index))
    ref = artifact(body, 'snapshots', emit)
    return {key: snapshot[key] for key in ('ticker', 'processed_date', 'effective_dates', 'quality',
        'source_acquired_at', 'source_valid_until', 'acquisition_status')} | {
        'weight_audit': snapshot.get('weight_audit'), 'reported_classifications': snapshot.get('reported_classifications'),
        'snapshot': ref}


# New inputs opt into this policy. Absence preserves the historical output bytes.
OWNERSHIP_POLICY = 'qualified-membership.v1'
OWNERSHIP_CONTRACT = 'etf-qualified-membership-summary.v1'
SUMMARY_MAX_RECORDS = 100000
SUMMARY_MAX_OBSERVATIONS = 300000
SUMMARY_MAX_BYTES = 48 * 1024 * 1024
SUMMARY_PAGE_BYTES = 256 * 1024
SUMMARY_MANIFEST_BYTES = 512 * 1024
SUMMARY_PAGE_ROWS = 200
SUMMARY_MAX_PAGES = 768


class SummaryBound(ValueError):
    pass


def summary_reason(snapshot, at):
    q = snapshot.get('quality', {})
    if q.get('status') != 'complete_returned_snapshot' or q.get('pagination_complete') is not True:
        return 'incomplete_returned_snapshot'
    try:
        acquired, expiry = clock(snapshot['source_acquired_at']), clock(snapshot['source_valid_until'])
        dates = list(snapshot['effective_dates'])
        if acquired > at or expiry <= acquired or at >= expiry: return 'source_check_not_current'
        if len(dates) != 1 or native.day(dates[0]) > at.date(): return 'mixed_or_future_effective_dates'
        if native.day(snapshot['processed_date']) > acquired.date(): return 'future_processing_date'
    except (KeyError, TypeError, ValueError): return 'invalid_source_clocks'
    if any(type(q.get(k)) is not int or q[k] != 0 for k in
           ('missing_identity_rows', 'duplicate_identity_rows', 'rows_with_field_errors')):
        return 'identity_or_field_coverage_unresolved'
    return None


class OwnershipSummary:
    """Only existing reconstructed rows; never a collector or an investment vote."""
    def __init__(self, generated_at, configured):
        self.at = clock(generated_at)
        self.configured = configured
        self.groups = {}
        self.funds = {}
        self.observations = 0
        self.records = 0
        self.failed = None

    def group(self, kind, dates):
        definition = {'kind': kind, 'effective_dates': dates}
        key = sha(encoded(definition))
        if key not in self.groups:
            self.groups[key] = {**definition, 'cohort_id': key, 'eligible_funds': [], 'source_valid_until': None, 'rows': {}}
        return self.groups[key]

    def record(self, group, row):
        identity = row['identity_key']
        if not isinstance(identity, str) or len(identity) != 64: raise ValueError('Summary exact identity required')
        records = group['rows']
        if identity not in records:
            self.records += 1
            if self.records > SUMMARY_MAX_RECORDS: raise SummaryBound('summary_record_bound')
            records[identity] = {'identity_key': identity, 'raw': set(), 'qualified': set(), 'lower': set(),
                                 'both': set(), 'current_only': set(), 'prior_only': set(), 'tickers': set(),
                                 'source_asset_class': row.get('asset_class'), 'source_security_type': row.get('security_type')}
        if row.get('constituent_ticker'): records[identity]['tickers'].add(row['constituent_ticker'])
        return records[identity]

    def eligible(self, group, fund, snapshots):
        group['eligible_funds'].append(fund)
        expiry = min(s['source_valid_until'] for s in snapshots)
        group['source_valid_until'] = min(group['source_valid_until'] or expiry, expiry)

    def add(self, fund, current, prior, comparison, current_ref, prior_ref, comparison_ref, tags):
        if self.failed: return
        try:
            self._add(fund, current, prior, comparison, current_ref, prior_ref, comparison_ref, tags)
        except SummaryBound as exc:
            self.failed = str(exc);self.groups.clear();self.funds.clear()

    def _add(self, fund, current, prior, comparison, current_ref, prior_ref, comparison_ref, tags):
        self.observations += len(current['rows']) + len(comparison['rows'])
        if self.observations > SUMMARY_MAX_OBSERVATIONS: raise SummaryBound('summary_observation_bound')
        reason = summary_reason(current, self.at)
        prior_reason = summary_reason(prior, self.at)
        pair_reason = reason or prior_reason or (None if comparison['comparable_snapshots'] else 'incompatible_effective_dates')
        self.funds[fund] = {'current_snapshot': current_ref, 'prior_snapshot': prior_ref, 'comparison': comparison_ref,
            'current_effective_dates': sorted(current['effective_dates']), 'prior_effective_dates': sorted(prior['effective_dates']),
            'source_acquired_at': current['source_acquired_at'], 'source_valid_until': current['source_valid_until'],
            'qualification_exclusion': reason, 'comparison_exclusion': pair_reason,
            'missing_identity_rows': current['quality'].get('missing_identity_rows'),
            'duplicate_identity_rows': current['quality'].get('duplicate_identity_rows'),
            'configured_tags_unverified': tags}
        if reason is None:
            group = self.group('current_membership', sorted(current['effective_dates']))
            self.eligible(group, fund, [current])
        lower_current = False
        try:
            lower_current = (clock(current['source_acquired_at']) <= self.at < clock(current['source_valid_until']))
        except (TypeError, ValueError): pass
        for row in current['rows']:
            if not row['identity_key']: continue
            group = self.group('current_membership', [row['effective_date']])
            rec = self.record(group, row);rec['raw'].add(fund)
            if reason is None: rec['qualified'].add(fund)
            # Known presence is not evidence of absence; never part of an exact rank.
            if lower_current and native.day(row['effective_date']) <= self.at.date(): rec['lower'].add(fund)
        if pair_reason is None:
            group = self.group('dated_membership_comparison',
                               [*sorted(prior['effective_dates']), *sorted(current['effective_dates'])])
            self.eligible(group, fund, [current, prior])
            source_rows = {r['identity_key']: r for r in prior['rows'] + current['rows']}
            mapping = {'observed_in_both': 'both', 'observed_only_in_current': 'current_only', 'observed_only_in_prior': 'prior_only'}
            for row in comparison['rows']:
                if row['status'] not in mapping: raise ValueError('Qualified comparison identity differs')
                self.record(group, source_rows[row['identity_key']])[mapping[row['status']]].add(fund)

    def finish(self, emit):
        if self.failed: return {'status': 'unavailable', 'policy': OWNERSHIP_POLICY, 'reason': self.failed}
        try: return self._finish(emit)
        except SummaryBound as exc:
            return {'status': 'unavailable', 'policy': OWNERSHIP_POLICY, 'reason': str(exc)}

    def _finish(self, emit):
        if len(self.funds) != self.configured: raise ValueError('Complete summary fund inventory required')
        pending = [];total = 0;cohorts = []
        def prepare(doc, limit):
            nonlocal total
            raw = encoded(doc)
            if len(raw) > limit: raise SummaryBound('summary_artifact_byte_bound')
            total += len(raw)
            if total > SUMMARY_MAX_BYTES: raise SummaryBound('summary_total_byte_bound')
            key = PREFIX + 'directories/' + sha(raw) + '.json'
            pending.append((key, raw))
            return {'key': key, 'sha256': sha(raw), 'bytes': len(raw)}
        page_count = 0
        for key, group in sorted(self.groups.items()):
            eligible = sorted(set(group['eligible_funds']))
            rows = []
            for identity, rec in group['rows'].items():
                rows.append({'identity_key': identity, 'reported_tickers': sorted(rec['tickers']),
                    'source_asset_class': rec['source_asset_class'], 'source_security_type': rec['source_security_type'],
                    'raw_observed_fund_count': len(rec['raw']) if group['kind'] == 'current_membership' else None,
                    'known_presence_lower_bound': len(rec['lower']) if group['kind'] == 'current_membership' else None,
                    'qualified_fund_count': len(rec['qualified']) if eligible and group['kind'] == 'current_membership' else None,
                    'observed_in_both_count': len(rec['both']) if group['kind'] == 'dated_membership_comparison' else None,
                    'observed_only_in_current_count': len(rec['current_only']) if group['kind'] == 'dated_membership_comparison' else None,
                    'observed_only_in_prior_count': len(rec['prior_only']) if group['kind'] == 'dated_membership_comparison' else None})
            rows.sort(key=lambda r: (-(r['qualified_fund_count'] or 0), r['identity_key']))
            refs = []
            for start in range(0, len(rows), SUMMARY_PAGE_ROWS):
                page_count += 1
                if page_count > SUMMARY_MAX_PAGES: raise SummaryBound('summary_page_bound')
                refs.append(prepare({'contract': 'etf-qualified-membership-rows.v1', 'cohort_id': key,
                                     'row_offset': start, 'rows': rows[start:start + SUMMARY_PAGE_ROWS]}, SUMMARY_PAGE_BYTES))
            cohorts.append({k: v for k, v in group.items() if k not in ('rows', 'eligible_funds')} | {
                'eligible_funds': eligible, 'eligible_fund_count': len(eligible), 'configured_fund_count': self.configured,
                'ranking_scope': 'exact_within_eligible_date_cohort' if eligible and group['kind'] == 'current_membership' else 'unranked_reported_observations',
                'record_count': len(rows), 'parts': refs})
        manifest = {'contract': OWNERSHIP_CONTRACT, 'policy': OWNERSHIP_POLICY, 'generated_at': self.at.isoformat(),
            'configured_fund_count': self.configured, 'funds': self.funds, 'cohorts': cohorts,
            'record_count': self.records, 'page_count': page_count,
            'limits': {'records': SUMMARY_MAX_RECORDS, 'observations': SUMMARY_MAX_OBSERVATIONS,
                       'total_bytes': SUMMARY_MAX_BYTES, 'page_bytes': SUMMARY_PAGE_BYTES, 'page_rows': SUMMARY_PAGE_ROWS,
                       'manifest_bytes': SUMMARY_MANIFEST_BYTES, 'pages': SUMMARY_MAX_PAGES},
            'scope': 'Reported rows, including zero/short positions; not current ownership, trades, daily changes or capital flows. '
                     'Known presence is a separate lower bound, never proof of absence or a global rank. '
                     'Quantity and raw-weight observations remain in the referenced comparison records. '
                     'No aggregate exposure, unit conversion, inferred asset class, fund-of-funds lookthrough or leveraged/inverse netting.',
            'source_classifications_verified': False, 'corporate_actions_verified': False, 'weight_unit_certified': False, 'market_value_currency_certified': False,
            'independent_investment_votes': 0, **permissions()}
        ref = prepare(manifest, SUMMARY_MANIFEST_BYTES)
        # Preflight all bytes before a single summary PUT; overflow cannot publish a prefix as complete.
        for key, raw in pending: emit(key, raw)
        return {'status': 'complete', 'policy': OWNERSHIP_POLICY, 'manifest': ref}


def build(inputs, read, emit, previous=None):
    if inputs.get('contract') != 'etf-holdings-inputs.v1' or inputs.get('kind') != 'holdings':
        raise ValueError('Canonical holdings input required')
    policy = inputs.get('ownership_summary_policy')
    if 'ownership_summary_policy' in inputs and policy != OWNERSHIP_POLICY: raise ValueError('Unreviewed ownership summary policy')
    generated = inputs['generated_at'];stamp = clock(generated)
    summary = OwnershipSummary(generated, len(catalog.ETF_UNIVERSE)) if policy else None
    query_date = native.day(inputs['query_date'])
    if query_date > stamp.date(): raise ValueError('Query date after compilation')
    collections = inputs['collections']
    if set(collections) != set(catalog.ETF_UNIVERSE): raise ValueError('Complete configured fund inventory required')
    funds = {};identities = {};row_count = 0
    for ticker, pair in sorted(collections.items()):
        if set(pair) != {'current', 'prior'}: raise ValueError('Current and prior source attempts required')
        current, prior = [native.reconstruct_or_reject(pair[key], read, generated) for key in ('current', 'prior')]
        if current['ticker'] != ticker or prior['ticker'] != ticker: raise ValueError('Fund map identity differs')
        if current['cutoff'] != query_date.isoformat() or prior['cutoff'] != (query_date - native.timedelta(days=30)).isoformat():
            raise ValueError('Reviewed snapshot cutoffs required')
        a, b = retain_snapshot(current, emit), retain_snapshot(prior, emit)
        comparison = native.compare(current, prior)
        compare_parts = []
        for start in range(0, len(comparison['rows']), CHUNK_ROWS):
            compare_parts.append(artifact({'contract': 'etf-holdings-position-comparison-rows.v1',
                'ticker': ticker, 'row_offset': start, 'rows': comparison['rows'][start:start + CHUNK_ROWS]}, 'comparisons', emit))
        compared = {k: v for k, v in comparison.items() if k != 'rows'}
        compared.update(contract='etf-holdings-position-comparison.v1', parts=compare_parts,
            current_snapshot=a['snapshot'], prior_snapshot=b['snapshot'], compared_identities=len(comparison['rows']))
        fund = {'ticker': ticker, 'current': a, 'prior': b, 'comparison': artifact(compared, 'comparisons', emit),
            'comparable_snapshots': compared['comparable_snapshots'], 'comparison_status_counts': compared['identity_status_counts'],
            'configured_tag_unverified': {k: catalog.ETF_UNIVERSE[ticker].get(k) for k in ('category', 'subcategory', 'region')},
            **permissions()}
        if a['quality']['status'] != 'complete_returned_snapshot' and (previous or {}).get('contract') == CONTRACT:
            old = previous.get('funds', {}).get(ticker, {})
            retained = old.get('current')
            if not retained or retained.get('quality', {}).get('status') != 'complete_returned_snapshot':
                retained = old.get('retained_previous_current')
            if retained:
                fund['retained_previous_current'] = retained
                fund['retention_note'] = 'Previously reconstructed snapshot, not part of this current collection. Original source clocks and units still apply.'
        if summary is not None:
            summary.add(ticker, current, prior, comparison, a['snapshot'], b['snapshot'], fund['comparison'], fund['configured_tag_unverified'])
        funds[ticker] = fund;row_count += len(current['rows'])
        for row in current['rows']:
            identity = row['identity_key']
            if not identity: continue
            rec = identities.setdefault(identity, {'identity_key': identity,
                'provider_identifiers': {k: row[k] for k in native.IDENTITY_FIELDS}, 'names': set(), 'tickers': set(), 'memberships': []})
            if row['constituent_name']: rec['names'].add(row['constituent_name'])
            if row['constituent_ticker']: rec['tickers'].add(row['constituent_ticker'])
            rec['memberships'].append({'fund': ticker, 'row_id': row['row_id'], 'snapshot': a['snapshot'],
                'effective_date': row['effective_date'], 'processed_date': row['processed_date'],
                'source_valid_until': current['source_valid_until'], 'source_check_overdue': current['quality'].get('source_check_overdue', True),
                'snapshot_complete': current['quality']['pagination_complete'],
                'weight_raw_decimal': row['weight_raw_decimal'], 'weight_unit_certified': False,
                'market_value_raw_decimal': row['market_value_raw_decimal'], 'market_value_currency_certified': False,
                'shares_held_raw_decimal': row['shares_held_raw_decimal'], 'source': row['source']})
    directory = [];buckets = defaultdict(list)
    if len(identities) > 300000: raise ValueError('Reviewed searchable identity bound exceeded')
    for identity, rec in sorted(identities.items()):
        rec['names'] = sorted(rec['names']);rec['tickers'] = sorted(rec['tickers'])
        rec['memberships'].sort(key=lambda row: (row['fund'], row['row_id']))
        unique_funds = sorted({row['fund'] for row in rec['memberships']})
        dates = sorted({row['effective_date'] for row in rec['memberships']})
        directory.append({'identity_key': identity, 'names': rec['names'], 'tickers': rec['tickers'],
            'identifiers': {k: rec['provider_identifiers'][k] for k in ('figi', 'isin', 'us_code', 'sedol')},
            'observed_fund_count': len(unique_funds), 'effective_dates': dates, 'bucket': identity[:2]})
        rec.update(observed_funds=unique_funds, effective_dates=dates, portfolio_weight=None,
            inferred_trade_usd=None, additional_independent_investment_votes=0)
        buckets[identity[:2]].append(rec)
    shards = {key: artifact({'contract': 'etf-holdings-memberships.v1', 'bucket': key, 'securities': rows}, 'memberships', emit)
              for key, rows in sorted(buckets.items())}
    directory_parts = [artifact({'contract': 'etf-holdings-directory-rows.v1', 'row_offset': start,
        'securities': directory[start:start + 500]}, 'directories', emit) for start in range(0, len(directory), 500)]
    directory_ref = artifact({'contract': 'etf-holdings-security-directory.v1', 'security_count': len(directory),
        'parts': directory_parts, 'buckets': shards}, 'directories', emit)
    states = Counter(f['current']['quality']['status'] for f in funds.values())
    complete = states.get('complete_returned_snapshot', 0)
    deadlines = [f['current']['source_valid_until'] for f in funds.values() if f['current']['source_valid_until']]
    result = {'contract': CONTRACT, 'version': '1.0.0', 'engine': 'justhodl-etf-constituents', 'generated_at': generated,
        'source_valid_until': min(deadlines) if deadlines else None, 'query_date': inputs['query_date'],
        'funds': funds, 'security_directory': directory_ref,
        'quality': {'status': 'partial' if complete else 'unavailable', 'configured_funds': len(funds),
            'source_status_counts': dict(sorted(states.items())), 'complete_returned_snapshots': complete,
            'reconstructed_current_rows': row_count, 'exact_provider_identities': len(directory),
            'independent_investment_votes': 0, 'portfolio_weight_qualified_funds': 0,
            'current_holdings_confirmed': False, 'weight_unit_certified': False, 'market_value_currency_certified': False},
        'methodology': {
            'snapshots': 'Latest processing date on or before each query cutoff; every returned row and pagination response retained. The earlier cutoff is 30 calendar days earlier, not an asserted 30-day holding period.',
            'clocks': 'Effective dates describe source holdings. Processing dates and original acquisition times stay separate. New compilation never refreshes those dates or proves current ownership.',
            'identity': 'Exact provider identifier tuples, listing currency, exchange and security classifications. Ticker equality alone cannot merge instruments; ambiguous or missing identities retain their individual rows.',
            'weights': 'Raw provider weight values are not rescaled, normalized to 100%, or certified as portfolio fractions. Published documentation and live examples need a reconciled unit contract. Leveraged and derivative books may exceed one.',
            'valuation': 'Reported market values do not become USD from a trading-currency label. Shares-held fields may describe bond or derivative position units.',
            'changes': 'Unadjusted reported position changes on distinct effective dates are not trades, investor identity, accumulation, rebalances or index events. Missing sides never become zero.',
            'portfolio': 'Fund membership counts are not portfolio weights. Actual look-through exposure requires user positions, qualified units, compatible holdings dates and derivative treatment.',
            'source_docs': 'https://massive.com/docs/rest/partners/etf-global/constituents'},
        'legacy_contexts': inputs['contexts'], 'retained_prior_publication': inputs.get('previous'),
        'provider_requests': inputs['provider_requests'], 'original_provider_bytes': inputs['original_provider_bytes'],
        'private_account_reads': 0, 'paid_ai_calls': 0, 'signals_emitted': 0, 'notifications_sent': 0, 'portfolio_writes': 0,
        'dependency_graph': {'roots': ['ETF Global constituents original responses'],
            'views': ['dated fund snapshots', 'exact-identity memberships', 'unadjusted position comparisons'],
            'independent_investment_votes': 0}, **permissions()}

    if summary is not None:
        result.update(version='1.1.0', ownership_summary=summary.finish(emit))
    return result


def lookthrough(packet, generated_at, retained, previous):
    if packet.get('contract') != CONTRACT or any(packet.get(k) is not False for k in PERMISSIONS):
        raise ValueError('Qualified descriptive holdings packet required')
    if clock(packet['generated_at']) > clock(generated_at): raise ValueError('Source compilation after projection')
    return {**{k: v for k, v in packet.items() if k not in ('replay', 'legacy_contexts', 'retained_prior_publication')},
        'contract': LOOK_CONTRACT, 'engine': 'justhodl-flow-lookthrough', 'generated_at': generated_at,
        'source_generated_at': packet['generated_at'], 'canonical_replay': packet['replay'],
        'retained_canonical_source': retained, 'retained_prior_publication': previous,
        'provider_requests': 0, 'original_provider_bytes': 0, 'additional_independent_investment_votes': 0,
        'top_picks': [], 'inflow_leaders': [], 'outflow_leaders': [], 'actual_accumulation': [], 'actual_distribution': []}
