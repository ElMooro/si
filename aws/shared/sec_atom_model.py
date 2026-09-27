"""Deterministic SEC Atom research, with source versions and explicit exclusions.

This module performs no network, storage, clock, account or notification work.
The SEC current-feed snapshot is not a complete historical filing universe.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
import hashlib
import json
import math
import re
import xml.etree.ElementTree as ET
from urllib.parse import urlencode

CONTRACT = 'sec-atom-research.v1'
FORMS = {'8k': ('8-K',), '10kq': ('10-K', '10-Q', '10-K/A', '10-Q/A')}
ALLOWED_FORMS = {'8-K', '8-K/A', '10-K', '10-Q', '10-K/A', '10-Q/A'}
HEADS = {'8k': 'data/8k-filings.json', '10kq': 'data/10kq-filings.json'}
NS = '{http://www.w3.org/2005/Atom}'
FLAGS = {'call': None, 'calls_eligible': False, 'forecast_qualified': False,
         'sizing_eligible': False, 'original_vintage_verified': False}


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def feed_url(form):
    if form not in ALLOWED_FORMS: raise ValueError('Unreviewed SEC form')
    return 'https://www.sec.gov/cgi-bin/browse-edgar?' + urlencode({'action': 'getcurrent', 'type': form, 'output': 'atom', 'count': 200})


def strict(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('Duplicate JSON key')
            result[key] = value
        return result
    def invalid(value):
        raise ValueError('Nonfinite JSON value')
    def number(value):
        n = float(value)
        if not math.isfinite(n) or (n == 0 and any(c in '123456789' for c in value.lower().split('e')[0])):
            invalid(value)
        return n
    return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_constant=invalid, parse_float=number)


def clock(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?(?:Z|[+-]\d{2}:\d{2})', value):
        return None
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return dt.astimezone(timezone.utc) if dt.utcoffset() is not None else None
    except ValueError:
        return None


def accession(value):
    if not isinstance(value, str):
        return None
    if re.fullmatch(r'\d{10}-\d{2}-\d{6}', value):
        return value
    if re.fullmatch(r'\d{18}', value):
        return value[:10] + '-' + value[10:12] + '-' + value[12:]
    return None


def version_id(record):
    return sha(encode({k: v for k, v in record.items()
                       if k not in ('source_evidence', 'source_status', 'version_id')}))


class Text(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True); self.parts = []
    def handle_data(self, value):
        self.parts.append(value)


def parse_atom(raw):
    text = raw.decode('utf-8-sig')
    if re.search(r'<!\s*(?:DOCTYPE|ENTITY)\b', text, re.I):
        raise ValueError('DTD/entity declarations are not research data')
    document = ET.fromstring(text)
    if document.tag != NS + 'feed':
        raise ValueError('SEC Atom feed root required')
    records = []
    for i, entry in enumerate(document.findall(NS + 'entry')):
        title = entry.findtext(NS + 'title', '')
        summary = entry.findtext(NS + 'summary', '')
        identities = [entry.findtext(NS + 'id', '')]
        links = [dict(link.attrib) for link in entry.findall(NS + 'link')]
        categories = [dict(category.attrib) for category in entry.findall(NS + 'category')]
        alternate = [link.get('href') for link in links if link.get('rel', 'alternate') == 'alternate' and link.get('href')]
        urls = list(dict.fromkeys(alternate))
        identities.extend(urls)
        found = sorted({m for value in identities for m in re.findall(r'(?<!\d)(\d{10}-\d{2}-\d{6})(?!\d)', value)})
        forms = {c.get('term') for c in categories if c.get('term') in ALLOWED_FORMS}
        match = re.match(r'^(8-K(?:/A)?|10-[KQ](?:/A)?)\s*[-–]\s*(.*?)\s*\((\d{10})\)(?:\s|$)', title)
        if match:
            forms.add(match[1])
        form = next(iter(forms)) if len(forms) == 1 else None
        parsed = Text(); parsed.feed(summary)
        item_text = ' '.join(parsed.parts) + ' ' + title
        items = sorted(set(re.findall(r'\bItem\s+(\d\.\d{2})\b', item_text, re.I)))
        issues = []
        if len(forms) != 1: issues.append('returned_form_missing_or_conflicting')
        if len(found) != 1: issues.append('accession_missing_or_conflicting')
        if len(urls) != 1: issues.append('document_link_missing_or_conflicting')
        updated = entry.findtext(NS + 'updated', '')
        if clock(updated) is None: issues.append('invalid_filing_clock')
        records.append({'company': match[2] if match else title, 'cik': match[3] if match else None,
                        'form': form, 'accession': found[0] if len(found) == 1 else None,
                        'filed_at': updated, 'items': items, 'filing_url': urls[0] if len(urls) == 1 else None,
                        'summary': summary, 'summary_snippet': summary,
                        'atom_title': title, 'atom_id': identities[0], 'atom_links': links,
                        'atom_categories': categories, 'source_issues': issues,
                        'atom_entry_index': i,
                        'atom_entry_xml': ET.canonicalize(ET.tostring(entry, encoding='unicode'), rewrite_prefixes=True)})
    return {'records': records, 'feed_updated': document.findtext(NS + 'updated', ''),
            'returned_entries': len(records), 'feed_attributes': dict(document.attrib)}


def recipe_check(recipe):
    if not isinstance(recipe, dict) or recipe.get('kind') not in FORMS:
        raise ValueError('Reviewed filing family required')
    if type(recipe.get('window_days')) is not int or not 1 <= recipe['window_days'] <= 366:
        raise ValueError('Explicit filing window required')
    if recipe['kind'] == '8k':
        if not isinstance(recipe.get('item_labels'), dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in recipe['item_labels'].items()):
            raise ValueError('Original item definitions required')
        for name in ('red_flag_items', 'high_impact_items'):
            if not isinstance(recipe.get(name), list) or any(not isinstance(k, str) for k in recipe[name]):
                raise ValueError('Explicit item classification required')


def build(inputs, read):
    recipe = inputs['recipe']; recipe_check(recipe); kind = recipe['kind']
    at = clock(inputs.get('generated_at')); started = clock(inputs.get('started_at'))
    if at is None or started is None or at < started:
        raise ValueError('Ordered explicit publication clocks required')
    prior = strict(read(inputs['prior']['original'])) if inputs.get('prior') else {}
    if not isinstance(prior, dict) or (prior and not isinstance(prior.get('filings'), list)):
        raise ValueError('Whole prior filing population required')
    if prior.get('contract') not in (None, CONTRACT):
        raise ValueError('Unknown prior filing contract')
    if prior.get('contract') == CONTRACT and prior.get('engine_family') != kind:
        raise ValueError('Prior filing family differs')
    if inputs.get('prior') and inputs['prior'].get('key') != HEADS[kind]:
        raise ValueError('Only the exact prior filing head may be used')
    attempts = inputs.get('attempts')
    if not isinstance(attempts, list) or [r.get('requested_form') for r in attempts] != list(FORMS[kind]):
        raise ValueError('Every declared feed attempt must be accounted for')
    responses, versions, invalid_prior = [], [], []
    previous = prior.get('filing_versions', prior.get('filings', []))
    if not isinstance(previous, list): raise ValueError('Prior versions must remain a complete list')
    for i, row in enumerate(previous):
        if not isinstance(row, dict):
            invalid_prior.append({'path': ('filing_versions' if 'filing_versions' in prior else 'filings') + '[' + str(i) + ']', 'value': deepcopy(row)})
            continue
        record = deepcopy(row)
        record['source_status'] = 'retained_prior_publication'
        # The prior original already retains its entire evidence graph. Link
        # that node once instead of expanding every ancestor into every row.
        record['source_evidence'] = [{'kind': 'prior_packet', 'original': inputs['prior']['original'],
                                      'population': 'filing_versions' if 'filing_versions' in prior else 'filings', 'row_index': i}]
        versions.append(record)
    for attempt in attempts:
        if attempt.get('url') != feed_url(attempt.get('requested_form')):
            raise ValueError('Exact declared SEC query required')
        requested = clock(attempt.get('requested_at')); received = clock(attempt.get('received_at'))
        if requested is None or received is None or not started <= requested <= received <= at:
            raise ValueError('Attempt clocks must lie within publication acquisition')
        public = {k: deepcopy(attempt.get(k)) for k in ('requested_form', 'url', 'requested_at', 'received_at', 'status', 'http_status', 'original')}
        if attempt.get('status') == 'http_response':
            raw = read(attempt['original'])
            if attempt.get('http_status') == 200:
                try:
                    feed = parse_atom(raw)
                except (ValueError, UnicodeError, ET.ParseError) as error:
                    public.update(parse_status='invalid_atom', parse_error_type=type(error).__name__)
                else:
                    public.update({k: v for k, v in feed.items() if k != 'records'}); public['parse_status'] = 'complete_response_parsed'
                    for record in feed['records']:
                        entry_index = record.pop('atom_entry_index')
                        record.update(source_status='current_atom_response', source_evidence=[{'kind': 'SEC_Atom',
                            'original': attempt['original'], 'url': attempt['url'], 'requested_form': attempt['requested_form'],
                            'received_at': attempt['received_at'], 'entry_index': entry_index}])
                        versions.append(record)
            else: public['parse_status'] = 'http_error_body_retained'
        elif attempt.get('status') not in ('transport_error', 'rate_limit_not_attempted', 'budget_not_attempted'):
            raise ValueError('Unknown acquisition attempt state')
        responses.append(public)
    if not any(r.get('parse_status') == 'complete_response_parsed' for r in responses):
        raise ValueError('No successfully parsed current feed; preserve last publication')

    # Identical semantic versions can share evidence. Conflicting field values
    # remain separate versions; a ticker or absent identifier is never a join key.
    unique = {}
    for record in versions:
        version = version_id(record)
        if version not in unique:
            unique[version] = {**record, 'version_id': version, 'source_evidence': []}
        existing = {sha(encode(x)) for x in unique[version]['source_evidence']}
        evidence = record.get('source_evidence')
        if not isinstance(evidence, list): raise ValueError('Whole source evidence list required')
        for item in evidence:
            digest = sha(encode(item))
            if digest not in existing:
                unique[version]['source_evidence'].append(deepcopy(item)); existing.add(digest)
        if record['source_status'] == 'current_atom_response': unique[version]['source_status'] = record['source_status']
    # Only a verified selection from our own previous contract can break a tie
    # between retained versions. An earlier current-feed conflict remains a
    # conflict until new source evidence resolves it; order is never evidence.
    prior_selected = {}
    if prior.get('contract') == CONTRACT:
        for row in prior['filings']:
            if not isinstance(row, dict) or row.get('version_id') != version_id(row):
                raise ValueError('Prior canonical record identity differs')
            acc = accession(row.get('accession'))
            if acc is None or acc in prior_selected:
                raise ValueError('Prior canonical accession is not unique')
            prior_selected[acc] = row['version_id']
    cutoff = at - timedelta(days=recipe['window_days'])
    eligible, exclusions, groups = [], [], {}
    for row in unique.values():
        stamp = clock(row.get('filed_at')); acc = accession(row.get('accession'))
        reason = ('invalid_filing_clock' if stamp is None else 'future_filing_clock' if stamp > at else
                  'outside_rolling_window' if stamp < cutoff else 'accession_missing_or_invalid' if acc is None else
                  'off_scope_or_unknown_form' if row.get('form') not in (('8-K', '8-K/A') if kind == '8k' else FORMS[kind])
                  and not (kind == '8k' and row.get('form') is None and 'atom_title' not in row and row['source_status'] == 'retained_prior_publication') else None)
        if reason:
            exclusions.append({'reason': reason, 'record': row}); continue
        groups.setdefault(acc, []).append(row)
    conflicts, selections = [], {}
    for acc, candidates in groups.items():
        current = [row for row in candidates if row['source_status'] == 'current_atom_response']
        if len(current) > 1:
            conflicts.append({'accession': acc, 'reason': 'conflicting_current_feed_records', 'version_ids': [row['version_id'] for row in candidates]})
            continue
        canonical = [row for row in candidates if row['version_id'] == prior_selected.get(acc)]
        if not current and len(candidates) > 1 and len(canonical) != 1:
            conflicts.append({'accession': acc, 'reason': 'conflicting_retained_records', 'version_ids': [row['version_id'] for row in candidates]})
            continue
        eligible.append(deepcopy((current or canonical or candidates)[0]))
        selections[acc] = 'current_returned_version' if current else 'prior_canonical_version' if canonical else 'sole_retained_version'
    eligible.sort(key=lambda row: (clock(row['filed_at']), row['accession']), reverse=True)
    complete = all(row.get('parse_status') == 'complete_response_parsed' for row in responses)
    out = deepcopy(prior)
    out.pop('publication_context', None)
    out.update(contract=CONTRACT, schema_version='2.0', version='2.0.0', engine_family=kind,
               generated_at=inputs['generated_at'], window_days=recipe['window_days'], filings=eligible,
               filing_versions=[r for r in unique.values() if clock(r.get('filed_at')) is not None and cutoff <= clock(r['filed_at']) <= at],
               excluded_records=exclusions, identity_conflicts=conflicts, selection_basis=selections,
               malformed_prior_records=invalid_prior, source_responses=responses, **FLAGS)
    out['quality'] = {'status': 'partial', 'ingestion_status': 'all_requested_responses_parsed' if complete else 'partial_source_responses',
        'publication_date': inputs['generated_at'], 'historical_universe_complete': False,
        'source_atomic': False, 'dated_unique_accessions': len(eligible),
        'latest_filing_updated_at': eligible[0]['filed_at'] if eligible else None,
        'oldest_filing_updated_at': eligible[-1]['filed_at'] if eligible else None,
        'current_eligible_accessions': sum(row['source_status'] == 'current_atom_response' for row in eligible),
        'retained_canonical_accessions': sum(row['source_status'] != 'current_atom_response' for row in eligible),
        'current_response_versions': sum(row['source_status'] == 'current_atom_response' for row in unique.values()),
        'retained_prior_versions': sum(row['source_status'] != 'current_atom_response' for row in unique.values()),
        'excluded_versions': len(exclusions), 'identity_conflicts': len(conflicts),
        'reason': 'Recent Atom snapshots and retained metadata do not establish complete SEC history or a qualified investment signal.'}
    out['method'] = {'form_basis': 'Returned category and filing title; requested form never substitutes for returned form.',
        'clock_basis': 'Atom updated timestamp as supplied, not an independently verified SEC acceptance or event timestamp.',
        'versions': 'Every distinct record version and source reference is retained; current conflicts cannot enter headline counts.',
        'history': 'No arbitrary row cap. Complete prior packets and acquired responses are retained in the publication manifest.',
        'amendments': 'An amendment is not automatically a restatement.',
        'item_tags': 'Regex matches in Atom summary/title; full filing text and market impact are unverified.',
        'source_limit': '200 most recent entries per requested feed; no claim of complete window acquisition.'}
    failures = [r['requested_form'] + ': ' + r.get('parse_status', r['status']) for r in responses if r.get('parse_status') != 'complete_response_parsed']
    stats = deepcopy(prior.get('stats', {})) if isinstance(prior.get('stats'), dict) else {}
    stats.update(fetch_errors=failures, fetch_duration_s=round((at - started).total_seconds(), 3))
    if kind == '8k':
        def items(row): return [x for x in row.get('items', []) if isinstance(x, str)] if isinstance(row.get('items'), list) else []
        red = [row for row in eligible if set(items(row)) & set(recipe['red_flag_items'])]
        high = [row for row in eligible if set(items(row)) & set(recipe['high_impact_items'])]
        counts = {}
        for row in eligible:
            for item in set(items(row)):
                if isinstance(item, str): counts[item] = counts.get(item, 0) + 1
        stats.update(total_filings=len(eligible), last_24h=sum(clock(r['filed_at']) > at - timedelta(hours=24) for r in eligible),
                     red_flag_filings=len(red), high_impact_filings=len(high))
        out.update(item_labels=deepcopy(recipe['item_labels']), by_item_counts=counts, red_flags=red, high_impact=high)
    else:
        counts = {form: sum(r.get('form') == form for r in eligible) for form in FORMS[kind]}
        stats.update(total=len(eligible), total_10k=counts['10-K'], total_10q=counts['10-Q'],
                     total_10k_amended=counts['10-K/A'], total_10q_amended=counts['10-Q/A'])
        out.update(by_form_count=counts, amended=[r for r in eligible if r.get('form') in ('10-K/A', '10-Q/A')])
    out['stats'] = stats
    encode(out)
    return out
