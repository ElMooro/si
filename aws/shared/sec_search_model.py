"""Complete SEC search-response research; keyword matches are not issuer events.

No network, clock, storage, notification or investment decision is performed.
All dates, recipes and originals are explicit inputs to deterministic replay.
"""
from collections import defaultdict
from copy import deepcopy
from datetime import date, timedelta
import re
from urllib.parse import urlencode
from sec_atom_model import accession, clock, encode, sha, strict

CONTRACT = 'sec-search-research.v1'
HEAD = 'data/sec-filings-intel.json'
QUERY_IDS = ('going_concern', 'material_weakness', 'restatement', 'auditor_change',
             'cfo_departure', 'investigation', 'bankruptcy', 'definitive_agreement',
             'share_buyback_authorized', 'going_private', 'exclusive_partnership',
             'fda_approval', 'atm_shelf', 'bought_deal')
FLAGS = {'call': None, 'calls_eligible': False, 'forecast_qualified': False,
         'sizing_eligible': False, 'event_verified': False, 'original_vintage_verified': False}


def recipe_check(recipe):
    if not isinstance(recipe, dict) or type(recipe.get('lookback_days')) is not int or not 1 <= recipe['lookback_days'] <= 366:
        raise ValueError('Explicit existing query window required')
    queries = recipe.get('queries')
    if not isinstance(queries, list) or [q.get('id') for q in queries if isinstance(q, dict)] != list(QUERY_IDS):
        raise ValueError('Complete original query set required')
    for query in queries:
        if (not all(isinstance(query.get(k), str) and query[k] for k in ('query', 'label', 'desc'))
                or not isinstance(query.get('forms'), list) or not query['forms']
                or any(not isinstance(f, str) or not f for f in query['forms'])
                or query.get('polarity') not in ('bullish', 'bearish', 'material')
                or query.get('severity') not in ('low', 'medium', 'high', 'critical')
                or type(query.get('weight')) is not int):
            raise ValueError('Whole original query definition required')


def query_url(query, recipe, started_at):
    stamp = clock(started_at)
    if stamp is None: raise ValueError('Explicit acquisition clock required')
    return 'https://efts.sec.gov/LATEST/search-index?' + urlencode({
        'q': query['query'], 'forms': ','.join(query['forms']), 'dateRange': 'custom',
        'startdt': (stamp.date() - timedelta(days=recipe['lookback_days'])).isoformat(),
        'enddt': stamp.date().isoformat(), 'from': 0})


def filing_date(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value): return None
    try: return date.fromisoformat(value)
    except ValueError: return None


def entities(source):
    """Only an explicit display-name association maps a ticker to a CIK.

    Parallel arrays are not zipped, a co-filer is not silently discarded and an
    unparsed name remains in the complete hit and explicit association list.
    """
    names = source.get('display_names')
    if not isinstance(names, list) or not names: return [{'raw': deepcopy(names), 'issue': 'display_names_missing_or_not_list'}]
    result = []
    for index, name in enumerate(names):
        row = {'index': index, 'raw': deepcopy(name)}
        match = re.fullmatch(r'(.+?)\s*\(CIK\s*(\d{1,10})\)\s*', name) if isinstance(name, str) else None
        if not match or int(match[2]) == 0:
            row['issue'] = 'unresolved_display_identity'
        else:
            prefix = match[1].strip()
            ticker = re.fullmatch(r'(.+?)\s*\(([A-Z][A-Z0-9.\-]{0,14})\)', prefix)
            row.update(cik=match[2].zfill(10), name=ticker[1].strip() if ticker else prefix,
                       ticker=ticker[2] if ticker else None)
            if not ticker: row['issue'] = 'ticker_not_explicit_in_display_name'
            if 'ciks' in source:
                ciks = source['ciks']
                valid = isinstance(ciks, list) and all(isinstance(c, str) and re.fullmatch(r'\d{1,10}', c) and int(c) > 0 for c in ciks)
                if not valid or row['cik'] not in {c.zfill(10) for c in ciks}:
                    row['issue'] = 'display_identity_conflicts_with_source_ciks'
        result.append(row)
    return result


def parse_response(raw):
    packet = strict(raw)
    if not isinstance(packet, dict) or not isinstance(packet.get('hits'), dict) or not isinstance(packet['hits'].get('hits'), list):
        raise ValueError('Whole SEC search hits envelope required')
    total = packet['hits'].get('total'); relation = None; count = None
    if type(total) is int and total >= 0: count, relation = total, 'eq'
    elif isinstance(total, dict) and type(total.get('value')) is int and total['value'] >= 0 and total.get('relation') in ('eq', 'gte'):
        count, relation = total['value'], total['relation']
    hits = packet['hits']['hits']
    if count is not None and relation == 'eq' and count < len(hits):
        raise ValueError('Reported hit count smaller than returned population')
    shards = packet.get('_shards', {})
    incomplete = packet.get('timed_out') is not False or not isinstance(shards, dict) or type(shards.get('failed')) is not int or shards['failed'] != 0
    return hits, {'returned_hits': len(hits), 'reported_total': count, 'total_relation': relation,
                  'provider_response_complete': not incomplete,
                  'query_population_complete': not incomplete and relation == 'eq' and count == len(hits)}


def build(inputs, read):
    recipe = inputs['recipe']; recipe_check(recipe)
    started = clock(inputs.get('started_at')); generated = clock(inputs.get('generated_at'))
    if started is None or generated is None or generated < started:
        raise ValueError('Ordered acquisition and publication clocks required')
    prior_ref = inputs.get('prior')
    if prior_ref and prior_ref.get('key') != HEAD: raise ValueError('Exact predecessor key required')
    prior = strict(read(prior_ref['original'])) if prior_ref else {}
    if not isinstance(prior, dict) or (prior and not isinstance(prior.get('all_tickers'), list)):
        raise ValueError('Whole predecessor research population required')
    if prior.get('contract') not in (None, CONTRACT): raise ValueError('Unknown predecessor contract')
    attempts = inputs.get('attempts')
    if not isinstance(attempts, list) or [a.get('query_id') for a in attempts] != list(QUERY_IDS):
        raise ValueError('All query outcomes must be accounted for')
    source_responses, matches, events = [], [], []
    start_date = started.date() - timedelta(days=recipe['lookback_days'])
    for query, attempt in zip(recipe['queries'], attempts):
        if attempt.get('url') != query_url(query, recipe, inputs['started_at']):
            raise ValueError('Exact original query required')
        requested, received = clock(attempt.get('requested_at')), clock(attempt.get('received_at'))
        if requested is None or received is None or not started <= requested <= received <= generated:
            raise ValueError('Attempt clocks outside acquisition')
        summary = {k: deepcopy(attempt.get(k)) for k in ('query_id', 'url', 'requested_at', 'received_at', 'status', 'http_status', 'original')}
        parsed = None
        if attempt.get('status') == 'http_response':
            raw = read(attempt['original'])
            if attempt.get('http_status') == 200:
                try: parsed, metadata = parse_response(raw)
                except (ValueError, TypeError, UnicodeError) as error:
                    summary.update(parse_status='invalid_search_response', parse_error_type=type(error).__name__)
                else: summary.update(metadata, parse_status='complete_response_parsed')
            else: summary['parse_status'] = 'http_error_body_retained'
        elif attempt.get('status') not in ('transport_error', 'rate_limit_not_attempted', 'budget_not_attempted', 'unsupported_encoding'):
            raise ValueError('Unknown acquisition status')
        source_responses.append(summary)
        if parsed is None: continue
        for index, hit in enumerate(parsed):
            source = hit.get('_source') if isinstance(hit, dict) else None
            source = source if isinstance(source, dict) else {}
            associations = entities(source)
            stamp, acc, form = filing_date(source.get('file_date')), accession(source.get('adsh')), source.get('form')
            issues = []
            if stamp is None: issues.append('invalid_filing_date')
            elif not start_date <= stamp <= started.date(): issues.append('outside_requested_date_window')
            if acc is None: issues.append('missing_or_invalid_accession')
            if not isinstance(form, str) or form not in query['forms']: issues.append('returned_form_outside_query')
            ref = {'original': attempt['original'], 'query_id': query['id'], 'hit_index': index, 'received_at': attempt['received_at']}
            match_id = sha(encode({'query_id': query['id'], 'hit': hit}))
            matches.append({'match_id': match_id, 'source_hit': deepcopy(hit), 'source_evidence': ref,
                            'entity_associations': associations, 'issues': issues, **FLAGS})
            if issues: continue
            seen = set()
            for entity in associations:
                pair = (entity.get('cik'), entity.get('ticker'))
                if entity.get('issue') or pair in seen: continue
                seen.add(pair)
                events.append({'ticker': entity['ticker'], 'name': entity['name'], 'cik': entity['cik'],
                    'form': form, 'filed_at': stamp.isoformat(), 'accession': acc,
                    'filing_url': 'https://www.sec.gov/Archives/edgar/data/' + str(int(entity['cik'])) + '/' + acc.replace('-', '') + '/' + acc + '-index.htm',
                    'signal_id': query['id'], 'signal_label': 'Keyword match: ' + query['label'],
                    'desc': 'Search candidate; issuer event and investment impact are unverified.',
                    'polarity': query['polarity'], 'severity': query['severity'], 'weight': query['weight'],
                    'snippet': None, 'match_id': match_id, 'source_evidence': ref, **FLAGS})
    parsed_count = sum(r.get('parse_status') == 'complete_response_parsed' for r in source_responses)
    if not parsed_count: raise ValueError('No parsed search response; preserve previous publication')
    grouped = defaultdict(list)
    for event in events: grouped[(event['cik'], event['ticker'])].append(event)
    ticker_ciks = defaultdict(set)
    for cik, ticker in grouped: ticker_ciks[ticker].add(cik)
    records = []
    severity_rank = {'low': 1, 'medium': 2, 'high': 3, 'critical': 4}
    for (cik, ticker), rows in sorted(grouped.items(), key=lambda item: (item[0][1], item[0][0])):
        rows.sort(key=lambda r: (r['filed_at'], r['accession'], r['signal_id'], r['match_id']), reverse=True)
        unique = {(r['signal_id'], r['accession']): r for r in rows}
        aliases = sorted({r['name'] for r in rows})
        records.append({'ticker': ticker, 'cik': cik, 'name': aliases[0] if len(aliases) == 1 else None,
            'source_names': aliases, 'ticker_identity_conflict': len(ticker_ciks[ticker]) > 1,
            'events': rows, 'n_events': len(rows), 'unique_query_filing_matches': len(unique),
            'bearish_signals': sum(r['polarity'] == 'bearish' for r in unique.values()),
            'bullish_signals': sum(r['polarity'] == 'bullish' for r in unique.values()),
            'highest_severity': max((r['severity'] for r in rows), key=severity_rank.get),
            'latest_filing': rows[0]['filed_at'], 'score': None, 'raw_score': None, 'verdict': 'RESEARCH_ONLY',
            'diagnostic_weight_sum': sum(r['weight'] for r in unique.values()), **FLAGS})
    out = deepcopy(prior); out.pop('publication_context', None)
    out.update(contract=CONTRACT, schema_version='2.0', generated_at=inputs['generated_at'],
        duration_s=round((generated-started).total_seconds(), 3), lookback_days=recipe['lookback_days'],
        n_signal_queries=len(recipe['queries']), n_events_total=len(events), n_tickers_with_signals=len(records),
        n_unique_tickers=len(ticker_ciks), n_entity_ticker_associations=len(records),
        events_by_signal={q['id']: sum(e['signal_id'] == q['id'] for e in events) for q in recipe['queries']},
        signal_definitions=deepcopy(recipe['queries']), all_tickers=records, search_matches=matches,
        source_responses=source_responses,
        highlights={'risks': [r for r in records if r['bearish_signals'] > 0],
                    'opportunities': [r for r in records if r['bullish_signals'] > 0],
                    'critical': [r for r in records if r['highest_severity'] == 'critical']}, **FLAGS)
    out['quality'] = {'status': 'partial', 'queries_parsed': parsed_count, 'queries_requested': len(QUERY_IDS),
        'complete_query_populations': sum(r.get('query_population_complete') is True for r in source_responses),
        'returned_hit_occurrences': len(matches), 'excluded_hit_occurrences': sum(bool(r['issues']) for r in matches),
        'unresolved_entity_associations': sum(bool(e.get('issue')) for r in matches for e in r['entity_associations']),
        'ticker_identity_conflicts': sum(len(ciks) > 1 for ciks in ticker_ciks.values()),
        'independent_investment_votes': 0, 'historical_universe_complete': False,
        'reason': 'Only the original first-page search queries are acquired; every returned hit is retained. Keyword matches do not establish current issuer events or predictive skill.'}
    out['method'] = {'date_basis': 'SEC file_date as returned; no filing acceptance time or event date is inferred.',
        'identity': 'Explicit display-name CIK/ticker associations only; co-filers and ticker conflicts remain separate.',
        'counts': 'Every hit occurrence is retained. Per-entity polarity counts and diagnostic weights deduplicate query/accession pairs, not independent economic events.',
        'qualifications': 'Query polarity, severity and weights are original search priorities, not validated risk, probabilities or investment recommendations.',
        'excerpt': '_search_id is an opaque source identifier, never a filing excerpt.',
        'history': 'Whole predecessor packet and source responses are retained privately; this publication describes only this acquisition.'}
    out['notes'] = 'Research candidates only. Full filing context, entity resolution, event verification, historical vintages and investment qualification remain required.'
    encode(out)
    return out
