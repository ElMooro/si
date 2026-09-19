"""Reproducible comparisons of retained fund evidence, without invented investors."""
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, localcontext
import hashlib
import json
import re

from evidence_store import public_source_url
import flow_desk_catalog as catalog

PREFIX = 'data/flow-desk-research/'
CURRENT = 'data/global-flow-desk.json'
CONTRACT = 'global-flow-research.v1'
ETF = 'data/etf-research/'
TIC = 'data/tic-view/'
MAX_BYTES = 64 * 1024 * 1024
PERMISSION = {'call': None, 'calls_eligible': False, 'sizing_eligible': False,
              'execution_eligible': False, 'additional_independent_votes': 0}
ISSUER_URL = ('https://www.ishares.com/us/product-screener/product-screener-v3.1.jsn'
              '?dcrPath=/templatedata/config/product-screener-v3/data/en/us-ishares/'
              'ishares-product-screener-backend-config&siteEntryPassthrough=true')


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def decode(raw):
    def reject(value):
        raise ValueError('Non-finite JSON token: ' + value)
    return json.loads(raw, parse_constant=reject)


def clock(value):
    parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('Timezone-aware source clock required')
    return parsed.astimezone(timezone.utc)


def dec(value):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError('Finite decimal amount required')
    result = Decimal(str(value))
    if not result.is_finite() or abs(result) > Decimal('1e30'):
        raise ValueError('Source amount outside bound')
    return result


def ds(value):
    return None if value is None else format(value, 'f')


def number(value):
    return None if value is None else float(value)


def verified_raw(ref, read, prefix, limit=MAX_BYTES):
    sha = ref.get('sha256', '')
    if not re.fullmatch('[a-f0-9]{64}', sha) or ref.get('key') != prefix + sha + '.json':
        raise ValueError('Retained artifact identity differs')
    raw = read(ref['key'])
    if not 0 < len(raw) <= limit or hashlib.sha256(raw).hexdigest() != sha:
        raise ValueError('Retained artifact hash or size differs')
    if 'bytes' in ref and ref['bytes'] != len(raw):
        raise ValueError('Retained artifact length differs')
    return raw


def load(ref, read, prefix, limit=MAX_BYTES):
    return decode(verified_raw(ref, read, prefix, limit))


def upstream(ref, read, prefix, replay_contract, output_contract, at):
    manifest = load(ref, read, prefix + 'runs/', 1024 * 1024)
    if manifest.get('contract') != replay_contract:
        raise ValueError('Upstream replay contract differs')
    output = load(manifest['output'], read, prefix + 'outputs/')
    if output.get('contract') != output_contract:
        raise ValueError('Upstream output contract differs')
    if output['generated_at'] != manifest['generated_at'] or clock(output['generated_at']) > clock(at):
        raise ValueError('Upstream generation binding differs')
    if manifest.get('output_sha256', manifest['output']['sha256']) != digest(output):
        raise ValueError('Upstream output binding differs')
    if output.get('call') is not None or any(output.get(k) is not False for k in
           ('calls_eligible', 'sizing_eligible', 'execution_eligible')):
        raise ValueError('Descriptive source permission differs')
    return manifest, output


def taxonomy(manifest, source, read, at):
    """Only reported issuer classifications, kept separate from configured baskets."""
    inputs = load(manifest['input'], read, ETF + 'inputs/')
    ref = inputs['originals']['ishares_catalog']
    ev = ref['evidence']
    source_url = public_source_url(ISSUER_URL)
    request_hash = hashlib.sha256(source_url.encode()).hexdigest()
    sha = ev.get('sha256', '')
    if (ref['url'] != ISSUER_URL or ev.get('provider') != 'etf_original'
            or ev.get('captured') is not True or ev.get('contract') != 'source-evidence.v1'
            or ev.get('source_url') != source_url or not re.fullmatch('[a-f0-9]{64}', sha)
            or ev.get('key') != f'data/evidence/etf_original/{request_hash}/{sha}.bin.gz'):
        raise ValueError('Issuer taxonomy source identity differs')
    if not clock(ev['first_received_at']) <= clock(ref['acquired_at']) <= clock(at):
        raise ValueError('Issuer taxonomy clock differs')
    raw = read(ev['key'])
    if len(raw) != ev['bytes'] or hashlib.sha256(raw).hexdigest() != sha:
        raise ValueError('Issuer taxonomy original differs')
    parsed = decode(raw)
    result = {}
    for ticker, value in source['by_etf'].items():
        identity = value['identity']
        if identity.get('issuer') != 'iShares':
            result[ticker] = {'status': 'not_reviewed', 'asset_class': None,
                              'sub_asset_class': None}
            continue
        pid = str(identity['portfolio_id'])
        row = parsed.get(pid, {})
        if (row.get('localExchangeTicker') != ticker or str(row.get('portfolioId')) != pid
                or row.get('isin') != identity.get('isin') or row.get('fundName') != identity['fund_name']):
            raise ValueError('Issuer taxonomy fund identity differs')
        asset, sub = row.get('aladdinAssetClass'), row.get('aladdinSubAssetClass')
        if not isinstance(asset, str) or not asset.strip() or not isinstance(sub, str) or not sub.strip():
            raise ValueError('Issuer classification missing')
        result[ticker] = {'status': 'reported_issuer_classification', 'asset_class': asset,
            'sub_asset_class': sub, 'source': ref, 'object_key': pid,
            'fields': ['aladdinAssetClass', 'aladdinSubAssetClass'],
            'scope': 'Current issuer classification. Not a historical mandate vintage or a holdings look-through.'}
    return result


def series_values(history, dates):
    """Do not bridge excluded dates or compress a five-observation window."""
    if history.get('contract') != 'etf-native-history.v1':
        raise ValueError('ETF history contract differs')
    rows = history['rows']
    if len(rows) > 18000 or any(a['date'] >= b['date'] for a, b in zip(rows, rows[1:])):
        raise ValueError('ETF history date order differs')
    mapping = {}
    for row in rows:
        value = dec(row.get('nav_valued_share_change_decimal'))
        if value is not None and row.get('flow_status') != 'descriptive_estimate':
            raise ValueError('Excluded ETF observation has a flow')
        mapping[row['date']] = value
    one = [mapping.get(day) for day in dates]
    five = []
    for index in range(len(dates)):
        window = one[max(0, index - 4):index + 1]
        five.append(sum(window, Decimal(0)) if index >= 5 and all(v is not None for v in window) else None)
    return one, five


def summary(members, values, index, dates, allowed=None):
    members = sorted(set(members))
    covered = [t for t in members if t in values and values[t][1][index] is not None
               and (allowed is None or t in allowed)]
    value = sum((values[t][1][index] for t in covered), Decimal(0)) if covered else None
    return {'value_decimal': ds(value), 'net_flow_5d_usd': number(value),
            'unit': 'usd', 'method': 'sum_of_nav_valued_share_changes',
            'period': {'start_date': dates[index - 5], 'end_date': dates[index]} if index >= 5 else None,
            'configured_members': members, 'covered_members': covered,
            'unavailable_members': [t for t in members if t not in covered],
            'coverage_count': len(covered), 'configured_count': len(members),
            'direction': None if value is None else 'POSITIVE' if value > 0 else 'NEGATIVE' if value < 0 else 'ZERO',
            'whole_market_total': False, **PERMISSION}


def history_document(identifier, members, values, dates, source_ref):
    rows = []
    for index, day in enumerate(dates):
        one_members = [t for t in members if t in values and values[t][0][index] is not None]
        five_members = [t for t in members if t in values and values[t][1][index] is not None]
        one = sum((values[t][0][index] for t in one_members), Decimal(0)) if one_members else None
        five = sum((values[t][1][index] for t in five_members), Decimal(0)) if five_members else None
        rows.append({'date': day, 'one_observation_decimal': ds(one), 'five_observation_decimal': ds(five),
                     'five_start_date': dates[index - 5] if index >= 5 else None,
                     'one_covered_members': one_members, 'five_covered_members': five_members})
    return {'contract': 'flow-desk-history.v1', 'id': identifier, 'rows': rows,
            'configured_members': sorted(set(members)), 'upstream_manifest': source_ref,
            'unit': 'usd', 'scope': 'Partial configured-fund subtotals; each row identifies its covered members.',
            'vintage_policy': 'Fixed current membership and current issuer historical vintage. Changing coverage and survivorship prevent treating this chart as a point-in-time strategy backtest.',
            **PERMISSION}


def retained_contexts(inputs, read, at):
    out = {}
    for key in catalog.CONTEXT_KEYS:
        ref = inputs['contexts'].get(key, {})
        if ref.get('status') not in ('retained', 'unparseable'):
            out[key] = {'source_status': 'unavailable', 'failure_class': ref.get('failure_class'),
                        'interpretation': 'No value or signal substituted.', **PERMISSION}
            continue
        raw = verified_raw(ref['artifact'], read, PREFIX + 'contexts/')
        if clock(ref['acquired_at']) > clock(at):
            raise ValueError('Future context receipt')
        if ref['status'] == 'unparseable':
            try:
                decode(raw)
            except (ValueError, UnicodeError):
                out[key] = {'source_status': 'retained_unparseable_context', 'artifact': ref['artifact'],
                            'acquired_at': ref['acquired_at'], 'interpretation': 'Original bytes retained; no measurement substituted.',
                            **PERMISSION}
                continue
            raise ValueError('Unparseable context classification differs')
        doc = decode(raw)
        out[key] = {'source_status': 'retained_unqualified_context', 'artifact': ref['artifact'],
            'acquired_at': ref['acquired_at'],
            'reported_generation': doc.get('generated_at') if isinstance(doc, dict) and isinstance(doc.get('generated_at'), str) else None,
            'interpretation': 'Complete upstream packet retained. Its measurements, investor labels and forecasts are not independently qualified by this synthesis.',
            **PERMISSION}
    return out


def build(inputs, read, at):
    if inputs.get('contract') != 'flow-desk-inputs.v1':
        raise ValueError('Flow desk input contract differs')
    clock(at)
    manifest, source = upstream(inputs['etf_manifest'], read, ETF, 'etf-original-replay.v1',
                                'etf-original-research.v1', at)
    calendar = load(source['reference_calendar'], read, ETF + 'histories/')
    if calendar.get('contract') != 'etf-issuer-reference-dates.v1' or calendar.get('reference_fund') != 'IVV':
        raise ValueError('Observed reference-date contract differs')
    dates = [v['date'] for v in calendar['rows']]
    if len(dates) < 6 or dates != sorted(set(dates)) or dates[-1] != source['reference_calendar']['latest_date']:
        raise ValueError('Reference dates differ')
    end = source['aggregation_period']['end_date']
    index = dates.index(end)
    if index < 5:
        raise ValueError('Five-observation comparison unavailable')
    classifications = taxonomy(manifest, source, read, at)
    source_age = (clock(at) - clock(source['generated_at'])).total_seconds() / 3600
    observation_age = (clock(at).date() - date.fromisoformat(dates[-1])).days
    current_source = 0 <= source_age <= 48 and 0 <= observation_age <= 4
    values, funds, allowed = {}, {}, set()
    with localcontext() as ctx:
        ctx.prec = 50
        for ticker, entry in source['by_etf'].items():
            funds[ticker] = {'ticker': ticker, 'identity': entry['identity'], 'history': entry.get('history'),
                'source_status': entry['source_status'], 'issuer_classification': classifications[ticker],
                'configured_category': entry['category'], 'observation_date': entry.get('observation_date'),
                'source': entry.get('source'), 'quality': entry['quality'],
                'source_snapshot': '/etf-research.html?run=' + inputs['etf_manifest']['sha256'] + '&ticker=' + ticker,
                **PERMISSION}
            if not entry.get('history'):
                continue
            history = load(entry['history'], read, ETF + 'histories/')
            if (history.get('ticker') != ticker or history.get('identity') != entry['identity']
                    or len(history['rows']) != entry['history']['rows']):
                raise ValueError('ETF history fund binding differs')
            values[ticker] = series_values(history, dates)
            claimed = entry.get('net_flow_5d_usd')
            if claimed is not None:
                window = entry['flow_windows']['5d']
                if (window.get('start_date') != dates[index - 5] or window.get('end_date') != end
                        or values[ticker][1][index] != dec(window['value_decimal'])
                        or number(values[ticker][1][index]) != claimed):
                    raise ValueError('Upstream five-observation estimate differs from full history')
                if current_source:
                    allowed.add(ticker)
        definitions = []
        for key, categories in catalog.CLASSES.items():
            members = [t for t, v in source['by_etf'].items() if v['category'] in categories]
            definitions.append(('configured:' + key, catalog.GROUP_LABELS[key], members, 'configured_basket'))
        for key, members in catalog.EXTRA_GROUPS.items():
            definitions.append(('configured:' + key, catalog.GROUP_LABELS[key], list(members), 'configured_basket'))
        for ticker in catalog.SECTORS:
            definitions.append(('sector:' + ticker, ticker + ' fund', [ticker], 'configured_sector_fund'))
        countries = defaultdict(list)
        for ticker, name in catalog.CTRY.items():
            countries[name].append(ticker)
        for name, members in sorted(countries.items()):
            definitions.append(('geography:' + name, name + ' fund research basket', members, 'configured_geography'))
        issuer_groups = defaultdict(list)
        for ticker, item in classifications.items():
            if item['status'] == 'reported_issuer_classification':
                issuer_groups[(item['asset_class'], item['sub_asset_class'])].append(ticker)
        for (asset, sub), members in sorted(issuer_groups.items()):
            definitions.append(('issuer:' + asset + ':' + sub, 'iShares · ' + asset + ' / ' + sub, members, 'issuer_classification'))
        groups, histories = {}, {}
        for identifier, label, members, kind in definitions:
            members = sorted(set(members))
            doc = history_document(identifier, members, values, dates, inputs['etf_manifest'])
            body = encoded(doc); sha = hashlib.sha256(body).hexdigest()
            ref = {'key': PREFIX + 'histories/' + sha + '.json', 'sha256': sha, 'bytes': len(body), 'rows': len(doc['rows'])}
            histories[ref['key']] = body
            item = summary(members, values, index, dates, allowed)
            groups[identifier] = {'id': identifier, 'label': label, 'kind': kind, 'history': ref, **item,
                'scope': 'Selected iShares funds in the issuer-reported class; not the whole market.' if kind == 'issuer_classification' else catalog.CONFIGURED_GROUP_SCOPE,
                'is_national_capital_flow': False}
        for ticker in funds:
            funds[ticker]['comparison'] = summary([ticker], values, index, dates, allowed)
    tic_context = {'source_status': 'unavailable', **PERMISSION}
    if inputs.get('tic_manifest'):
        tm, tv = upstream(inputs['tic_manifest'], read, TIC, 'tic-view-replay.v1', 'tic-holdings-view.v1', at)
        tic_context = {'source_status': 'verified_descriptive_snapshot', 'manifest': inputs['tic_manifest'],
            'output': tm['output'], 'foreign_manifest': tv['foreign_manifest'], 'foreign_output': tv['foreign_output'],
            'observation_date': tv['observation_date'], 'source_generated_at': tv['source_generated_at'],
            'quality': tv['quality'], 'net_purchases': tv['net_purchases'], 'holdings': tv['top_holders'],
            'source_snapshot': '/foreign-flows.html?run=' + tv['foreign_manifest']['sha256'],
            'interpretation': 'Monthly securities holdings and transactions remain distinct from daily ETF issuance and country fund mandates. No hot-money or investor-identity vote.',
            **PERMISSION}
    contexts = retained_contexts(inputs, read, at)
    research_members = set(funds).union(*(set(g['configured_members']) for g in groups.values()))
    shared_members = {t: [key for key, group in groups.items() if t in group['configured_members']]
                      for t in sorted(research_members)}
    quality = {'status': 'partial' if current_source else 'stale_source',
        'configured_funds': len(funds), 'source_histories': len(values), 'aligned_funds': len(allowed),
        'unavailable_funds': sorted(set(funds) - allowed), 'issuer_classified_funds': sum(v['status'] == 'reported_issuer_classification' for v in classifications.values()),
        'source_packet_age_hours': source_age, 'latest_observation_age_days': observation_age,
        'source_check_max_age_hours': 48, 'latest_observation_max_age_days': 4,
        'exchange_calendar_independently_complete': False, **PERMISSION}
    narrative = (f'{len(allowed)} of {len(funds)} configured funds have comparable NAV-valued share-change estimates '
                 f'over {dates[index - 5]} to {end}. Research groups show covered-fund subtotals; overlapping '
                 'groups are not independent flows. Investor identity, national capital flow and expected returns are not inferred.')
    return {'engine': 'justhodl-global-flow-desk', 'version': '2.0.0', 'contract': CONTRACT,
        'generated_at': at, 'source_generated_at': source['generated_at'], 'quality': quality,
        'period': {'start_date': dates[index - 5], 'end_date': end, 'issuer_observations': 5},
        'source_reference_date': dates[-1], 'etf_manifest': inputs['etf_manifest'], 'etf_output': manifest['output'],
        'groups': groups, 'funds': funds, 'overlapping_memberships': shared_members,
        'retained_contexts': contexts, 'tic_context': tic_context,
        'asset_classes': {key: groups['configured:' + key] for key in catalog.CLASSES},
        'sectors': {'ranked': [groups['sector:' + t] for t in catalog.SECTORS], 'leaders': [], 'laggards': [], **PERMISSION},
        'inst_vs_retail': {'institutional': None, 'retail': None, 'divergence': None,
                          'status': 'investor_identity_not_measured', **PERMISSION},
        'hot_money': {'countries': {}, 'n_scored': 0, 'top_inflows': [], 'top_outflows': [],
                      'status': 'national_hot_money_not_measured', 'tic_context': tic_context, **PERMISSION},
        'capex': {'status': 'unqualified_context', 'source': contexts['data/capex-pulse.json'], **PERMISSION},
        'ai_brief': narrative, 'ai_brief_src': 'verified_deterministic_description',
        'narrative_evidence': {'unit': 'usd', 'period': {'start_date': dates[index - 5], 'end_date': end},
                               'source_manifest': inputs['etf_manifest'], 'scope': 'dated_nav_valued_share_changes'},
        'history': [], 'legacy_preservation': inputs['legacy'],
        'method': 'Exact same-period fund observations, with preserved configured baskets separated from reported issuer classifications. No donor fallback or heuristic identity score.',
        'dependency_roots': ['ETF_ISSUER_OBSERVATIONS'] + (['US_TREASURY:TIC:CSLT'] if inputs.get('tic_manifest') else []),
        'portfolio_impact': None, 'paid_ai_calls': 0, 'notifications_sent': 0,
        'private_account_reads': 0, 'portfolio_writes': 0, **PERMISSION}, histories
