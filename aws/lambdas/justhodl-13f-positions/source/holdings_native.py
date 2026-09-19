"""Deterministic SEC disclosure research. A reported position is not a trade.

Inputs are complete retained SEC responses. No network, symbol guessing, price
heuristics, execution estimates or model-generated investment signals live here.
"""
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, localcontext
import hashlib
import json
import re
import xml.etree.ElementTree as ET

PREFIX = 'data/holdings-research/'
CURRENT = 'data/holdings-research.json'
CONTRACT = 'holdings-native-research.v1'
MAX_BYTES = 64 * 1024 * 1024
PERMISSION = {'call': None, 'calls_eligible': False, 'sizing_eligible': False,
              'execution_eligible': False, 'additional_independent_votes': 0}
GUIDANCE = ('https://www.sec.gov/rules-regulations/staff-guidance/'
            'division-investment-management-frequently-asked-questions/'
            'frequently-asked-questions-about-form-13f')
# Explicit reviewed anomalies, not a rule that guesses units from share prices.
VALUE_REVIEWS = {
    '0001061768-26-000007': 'Modern cover and table report 5115380; preceding platform used a price-based multiplier. Economic valuation requires review; no rescaling.',
    '0001061768-26-000010': 'Modern cover and table report 5415853; preceding platform used a price-based multiplier. Economic valuation requires review; no rescaling.',
}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def decode(raw):
    def reject(value):
        raise ValueError('Nonfinite JSON token: ' + value)
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('Duplicate JSON key')
            result[key] = value
        return result
    return json.loads(raw, parse_constant=reject, object_pairs_hook=pairs)


def clock(value):
    result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Timezone-aware receipt required')
    return result.astimezone(timezone.utc)


def day(value):
    value = str(value)
    if re.fullmatch(r'\d{2}-\d{2}-\d{4}', value):
        value = value[6:] + '-' + value[:2] + '-' + value[3:5]
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise ValueError('Unambiguous calendar date required')
    return date.fromisoformat(value).isoformat()


def decimal(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{1,30}(?:\.\d{1,12})?', value):
        raise ValueError('Nonnegative exact decimal required')
    return Decimal(value)


def ds(value):
    return format(value, 'f')


def total(values):
    with localcontext() as context:
        context.prec = 60
        return sum((Decimal(v) for v in values), Decimal(0))


def difference(left, right):
    with localcontext() as context:
        context.prec = 60
        return ds(Decimal(left) - Decimal(right))


def local(node):
    return node.tag.rsplit('}', 1)[-1]


def tree(raw):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('Complete XML bytes required within bound')
    if b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():
        raise ValueError('XML entities are not accepted')
    return ET.fromstring(raw)


def child(node, name, optional=False):
    found = [v for v in node if local(v) == name]
    if not found and optional:
        return None
    if len(found) != 1:
        raise ValueError('Expected exactly one XML child: ' + name)
    return found[0]


def path(node, names, optional=False):
    for name in names.split('/'):
        node = child(node, name, optional)
        if node is None:
            return None
    return node


def text(node, names, optional=False):
    node = path(node, names, optional)
    if node is None:
        return None
    if len(node):
        raise ValueError('Scalar XML field required: ' + names)
    value = (node.text or '').strip()
    if not value and not optional:
        raise ValueError('Empty required XML field: ' + names)
    return value or None


def native_fields(node):
    """Preserve repeated/unknown fields in order; original XML retains namespaces."""
    result = []
    def walk(current, prefix):
        here = prefix + '/' + local(current)
        if not len(current):
            result.append([here, (current.text or '').strip(), dict(sorted(current.attrib.items()))])
        for sub in current:
            walk(sub, here)
    walk(node, '')
    return result


def truth(value):
    if value is None:
        return None
    if value.lower() not in ('true', 'false', '1', '0'):
        raise ValueError('Invalid reported boolean')
    return value.lower() in ('true', '1')


def original(ref, read, url, at):
    ev = ref.get('evidence', {})
    sha = ev.get('sha256', '')
    request_sha = hashlib.sha256(url.encode()).hexdigest()
    if (ref.get('url') != url or ev.get('source_url') != url
            or ev.get('contract') != 'source-evidence.v1' or ev.get('captured') is not True
            or ev.get('provider') != 'sec_holdings_original' or not re.fullmatch('[a-f0-9]{64}', sha)
            or ev.get('key') != f'data/evidence/sec_holdings_original/{request_sha}/{sha}.bin.gz'):
        raise ValueError('Original SEC evidence identity differs')
    if not clock(ev['first_received_at']) <= clock(ref['acquired_at']) <= clock(at):
        raise ValueError('Original SEC evidence receipt differs')
    raw = read(ev['key'])
    if not 0 < len(raw) <= MAX_BYTES or len(raw) != ev.get('bytes') or hashlib.sha256(raw).hexdigest() != sha:
        raise ValueError('Original SEC evidence bytes differ')
    return raw


def submission_rows(recent):
    names = ('accessionNumber', 'form', 'reportDate', 'filingDate', 'primaryDocument', 'acceptanceDateTime')
    size = len(recent['accessionNumber'])
    if any(not isinstance(recent.get(k), list) or len(recent[k]) != size for k in names):
        raise ValueError('Submission column lengths differ')
    result = []
    for i in range(size):
        if recent['form'][i] not in ('13F-HR', '13F-HR/A', '13F-NT', '13F-NT/A'):
            continue
        accession = recent['accessionNumber'][i]
        if not re.fullmatch(r'\d{10}-\d{2}-\d{6}', accession):
            raise ValueError('Invalid SEC accession')
        filed, period = day(recent['filingDate'][i]), day(recent['reportDate'][i])
        accepted = recent['acceptanceDateTime'][i]
        # SEC filing date can be the following business day after acceptance.
        # Keep both clocks; never replace the reported business date with UTC.
        if filed < period or clock(accepted).date().isoformat() < period:
            raise ValueError('Filing chronology differs')
        result.append({'accession': accession, 'form': recent['form'][i], 'filed_at': filed,
                       'period_of_report': period, 'accepted_at': accepted,
                       'primary_doc': recent['primaryDocument'][i]})
    if len({v['accession'] for v in result}) != len(result):
        raise ValueError('Duplicate submission accession')
    return sorted(result, key=lambda v: (clock(v['accepted_at']), v['accession']))


def merge_submissions(catalogs):
    merged = {}
    for catalog in catalogs:
        for row in submission_rows(catalog):
            if row['accession'] in merged and merged[row['accession']] != row:
                raise ValueError('Conflicting official submission metadata')
            merged[row['accession']] = row
    return sorted(merged.values(), key=lambda v: (clock(v['accepted_at']), v['accession']))


def selected_periods(candidates):
    return sorted({v['period_of_report'] for v in candidates if v['form'] in ('13F-HR', '13F-HR/A')}, reverse=True)[:2]


def pending_archives(submissions, candidates, acquired):
    periods = selected_periods(candidates)
    files = submissions['filings']['files']
    if not isinstance(files, list) or len(files) > 48:
        raise ValueError('Review complete submission archive without truncation')
    for item in files:
        if (not re.fullmatch(r'CIK\d{10}-submissions-\d{3}\.json', item['name'])
                or day(item['filingFrom']) > day(item['filingTo'])):
            raise ValueError('Submission archive identity or bounds differ')
    if len({v['name'] for v in files}) != len(files):
        raise ValueError('Duplicate submission archive identity')
    return [v for v in sorted(files, key=lambda v: (v['filingTo'], v['name']), reverse=True)
            if v['name'] not in acquired and (len(periods) < 2 or day(v['filingTo']) >= periods[-1])]


def verified_catalog(name, submissions, refs, read, at):
    catalogs = [submissions['filings']['recent']]
    if any(clock(v['accepted_at']) > clock(refs[name + ':submissions']['acquired_at']) for v in submission_rows(catalogs[0])):
        raise ValueError('Future filing in acquired recent submissions')
    acquired, sources = set(), []
    for item in submissions['filings']['files']:
        label = name + ':submissions-archive:' + item['name']
        if label not in refs:
            continue
        source = refs[label]
        raw = original(source, read, 'https://data.sec.gov/submissions/' + item['name'], at)
        archive = decode(raw)
        if len(archive['accessionNumber']) != item['filingCount']:
            raise ValueError('Complete submission archive count differs')
        rows = submission_rows(archive)
        if any(clock(v['accepted_at']) > clock(source['acquired_at']) for v in rows):
            raise ValueError('Future filing in acquired archive')
        if any(not day(item['filingFrom']) <= v['filed_at'] <= day(item['filingTo']) for v in rows):
            raise ValueError('Submission archive filing bounds differ')
        catalogs.append(archive); acquired.add(item['name']); sources.append(source)
    candidates = merge_submissions(catalogs)
    pending = pending_archives(submissions, candidates, acquired)
    return candidates, acquired, sources, pending


def parse_cover(raw, filing, cik):
    root = tree(raw)
    if local(root) != 'edgarSubmission':
        raise ValueError('13F XML cover required')
    header, form = child(root, 'headerData'), child(root, 'formData')
    cover, summary = child(form, 'coverPage'), child(form, 'summaryPage')
    identity = text(header, 'filerInfo/filer/credentials/cik')
    if not re.fullmatch(r'\d{1,10}', identity) or int(identity) != int(cik):
        raise ValueError('Filing CIK differs from submissions')
    for reported in (text(header, 'filerInfo/periodOfReport'), text(cover, 'reportCalendarOrQuarter')):
        if day(reported) != filing['period_of_report']:
            raise ValueError('Cover report period differs')
    if text(header, 'submissionType') != filing['form']:
        raise ValueError('Cover submission type differs')
    amendment = filing['form'] == '13F-HR/A'
    declared = truth(text(cover, 'isAmendment', True))
    if declared is not None and declared != amendment:
        raise ValueError('Amendment declaration differs')
    number = text(cover, 'amendmentNo', True)
    kind = text(cover, 'amendmentInfo/amendmentType', True)
    if amendment:
        if not number or not re.fullmatch(r'[1-9]\d{0,3}', number) or kind not in ('RESTATEMENT', 'NEW HOLDINGS'):
            raise ValueError('Review amendment number or type')
    elif number is not None or kind is not None:
        raise ValueError('Original filing declares amendment metadata')
    count, value = text(summary, 'tableEntryTotal'), text(summary, 'tableValueTotal')
    if not re.fullmatch(r'\d{1,7}', count):
        raise ValueError('Integer cover count required')
    decimal(value)
    included = path(summary, 'otherManagers2Info', True)
    manager_rows = [] if included is None else [native_fields(v) for v in included]
    manager_count = text(summary, 'otherIncludedManagersCount')
    if not re.fullmatch(r'\d{1,5}', manager_count) or int(manager_count) != len(manager_rows):
        raise ValueError('Included manager count differs')
    multiplier = 1 if filing['filed_at'] >= '2023-01-03' else 1000
    return {'cik': identity.zfill(10), 'filing_manager': text(cover, 'filingManager/name'),
            'schema_version': text(root, 'schemaVersion'), 'report_type': text(cover, 'reportType'),
            'is_amendment': amendment, 'amendment_number': int(number) if number else None,
            'amendment_type': kind, 'reported_rows': int(count), 'reported_value_sum': value,
            'reported_value_unit': 'USD' if multiplier == 1 else 'USD_thousands',
            'usd_multiplier': multiplier, 'unit_basis': 'SEC filing-date convention, never implied share price',
            'confidential_omissions_declared': truth(text(summary, 'isConfidentialOmitted', True)),
            'included_managers': manager_rows, 'included_manager_count': int(manager_count),
            'valuation_review': VALUE_REVIEWS.get(filing['accession']),
            'valuation_verified': False, 'value_scope': 'Reported disclosure values; not NAV, net exposure or invested cash.'}


def parse_table(raw, ref, accession, multiplier):
    root = tree(raw)
    if local(root) != 'informationTable' or any(local(v) != 'infoTable' for v in root):
        raise ValueError('Complete 13F information table required')
    if len(root) > 100000:
        raise ValueError('Review large native filing without truncation')
    rows = []
    for number, node in enumerate(root, 1):
        cusip = text(node, 'cusip').upper()
        quantity, value = text(node, 'shrsOrPrnAmt/sshPrnamt'), text(node, 'value')
        decimal(quantity); decimal(value)
        if not re.fullmatch(r'[A-Z0-9*@#]{9}', cusip):
            raise ValueError('CUSIP identifier needs review')
        quantity_type = text(node, 'shrsOrPrnAmt/sshPrnamtType').upper()
        option = (text(node, 'putCall', True) or '').upper()
        if quantity_type not in ('SH', 'PRN') or option not in ('', 'PUT', 'CALL'):
            raise ValueError('Quantity type or option needs review')
        title = text(node, 'titleOfClass')
        identity = {'cusip': cusip, 'class': ' '.join(title.upper().split()),
                    'quantity_type': quantity_type, 'put_call': option or None}
        voting = {}
        for key in ('Sole', 'Shared', 'None'):
            voting[key.lower()] = text(node, 'votingAuthority/' + key)
            decimal(voting[key.lower()])
        with localcontext() as context:
            context.prec = 60
            usd = ds(Decimal(value) * multiplier)
        rows.append({'row_id': accession + ':' + ref['evidence']['sha256'] + ':' + str(number),
                     'accession': accession, 'source_sha256': ref['evidence']['sha256'], 'row_number': number,
                     'identity': identity, 'issuer_name': text(node, 'nameOfIssuer'), 'reported_class': title,
                     'reported_quantity': quantity, 'reported_value': value, 'reported_value_usd': usd,
                     'investment_discretion': text(node, 'investmentDiscretion'),
                     'other_manager': text(node, 'otherManager', True), 'voting_authority': voting,
                     'figi': text(node, 'figi', True), 'native_fields': native_fields(node)})
    return rows


def parse_filing(name, cik, filing, refs, read, at):
    accession = filing['accession']
    base = 'https://www.sec.gov/Archives/edgar/data/' + str(int(cik)) + '/' + accession.replace('-', '') + '/'
    label = name + ':' + accession + ':'
    directory_ref = refs[label + 'index']
    directory = decode(original(directory_ref, read, base + 'index.json', at))
    filenames = [v['name'] for v in directory['directory']['item'] if v.get('name', '').lower().endswith('.xml')]
    if (not filenames or len(filenames) > 12 or len(set(filenames)) != len(filenames)
            or any(not re.fullmatch(r'[A-Za-z0-9_.-]+\.xml', v) for v in filenames)):
        raise ValueError('Review complete XML directory without truncation')
    covers, tables = [], []
    for filename in sorted(filenames):
        ref = refs[label + filename]
        raw = original(ref, read, base + filename, at)
        kind = local(tree(raw))
        if kind == 'edgarSubmission':
            covers.append((ref, parse_cover(raw, filing, cik)))
        elif kind == 'informationTable':
            tables.append((ref, raw))
        else:
            raise ValueError('Unclassified XML document in filing')
    if len(covers) != 1 or not tables:
        raise ValueError('One cover and complete native table set required')
    cover_ref, cover = covers[0]
    rows = []
    for ref, raw in tables:
        rows.extend(parse_table(raw, ref, accession, cover['usd_multiplier']))
    if len(rows) != cover['reported_rows'] or total(v['reported_value'] for v in rows) != Decimal(cover['reported_value_sum']):
        raise ValueError('Native rows or value do not reconcile to cover')
    return {'contract': 'holdings-native-filing.v1', **filing, 'cover': cover, 'cover_source': cover_ref,
            'directory_source': directory_ref, 'table_sources': [v[0] for v in tables], 'rows': rows,
            'reconciliation': {'row_count_exact': True, 'value_sum_exact': True}}


def groups(rows):
    result = {}
    for row in rows:
        key = digest(row['identity'])
        target = result.setdefault(key, {'identity': row['identity'], 'issuer_names': set(), 'reported_classes': set(),
            'quantities': [], 'values': [], 'row_ids': [], 'discretion': set(), 'other_managers': set(), 'voting': []})
        target['issuer_names'].add(row['issuer_name']); target['reported_classes'].add(row['reported_class'])
        target['quantities'].append(row['reported_quantity']); target['values'].append(row['reported_value_usd'])
        target['row_ids'].append(row['row_id']); target['discretion'].add(row['investment_discretion'])
        if row['other_manager']:
            target['other_managers'].add(row['accession'] + ':' + row['other_manager'])
        target['voting'].append(row['voting_authority'])
    return {key: {'identity': v['identity'], 'issuer_names': sorted(v['issuer_names']),
                  'reported_classes': sorted(v['reported_classes']),
                  'reported_quantity': ds(total(v['quantities'])), 'reported_value_usd': ds(total(v['values'])),
                  'row_ids': v['row_ids'], 'native_rows': len(v['row_ids']),
                  'investment_discretion': sorted(v['discretion']), 'other_managers': sorted(v['other_managers']),
                  'voting_authority': {k: ds(total(x[k] for x in v['voting'])) for k in ('sole', 'shared', 'none')},
                  'symbol': None, 'symbol_status': 'Not inferred from issuer text or legacy cache'}
            for key, v in sorted(result.items())}


def resolve_period(filings, complete):
    """Keep history, replace restatements, and refuse ambiguous supplemental overlap."""
    if not complete or not filings:
        return {'status': 'incomplete_chain', 'positions': {}, 'effective_accessions': [], 'chain': []}
    filings = sorted(filings, key=lambda v: (clock(v['accepted_at']), v['accession']))
    if len({v['period_of_report'] for v in filings}) != 1:
        raise ValueError('Cannot combine report periods')
    rows, effective, chain, supplements = [], [], [], False
    expected = 0
    failure = None
    for i, filing in enumerate(filings):
        cover = filing['cover']
        if i == 0:
            if cover['is_amendment']:
                failure = 'missing_original'
                break
        else:
            expected += 1
            if not cover['is_amendment'] or cover['amendment_number'] != expected:
                failure = 'amendment_number_gap_or_duplicate_original'
                break
        action = cover['amendment_type'] or 'ORIGINAL'
        chain.append({'accession': filing['accession'], 'action': action, 'accepted_at': filing['accepted_at'],
                      'amendment_number': cover['amendment_number'], 'native_rows': len(filing['rows'])})
        if action in ('ORIGINAL', 'RESTATEMENT'):
            if action == 'RESTATEMENT' and supplements:
                # A later restatement's relation to earlier supplements needs explicit review.
                failure = 'restatement_after_supplement_requires_review'
                break
            rows = list(filing['rows']); effective = [filing['accession']]
        else:
            previous = {digest(v['identity']) for v in rows}
            if any(digest(v['identity']) in previous for v in filing['rows']):
                failure = 'supplemental_identity_overlap_requires_review'
                break
            rows.extend(filing['rows']); effective.append(filing['accession']); supplements = True
    if failure:
        return {'status': failure, 'positions': {}, 'effective_accessions': [], 'chain': chain}
    return {'status': 'complete_selected_public_chain', 'positions': groups(rows),
            'effective_accessions': effective, 'chain': chain,
            'effective_native_rows': len(rows), 'reported_value_usd': ds(total(v['reported_value_usd'] for v in rows)),
            'confidential_omissions_declared': any(v['cover']['confidential_omissions_declared'] is True for v in filings),
            'confidential_omissions_unknown': any(v['cover']['confidential_omissions_declared'] is None for v in filings),
            'valuation_reviews': [v['cover']['valuation_review'] for v in filings if v['cover']['valuation_review']],
            'scope': 'Complete selected public amendment chain at acquisition; public omission remains possible.'}


def compare(current, prior):
    eligible = current.get('status') == prior.get('status') == 'complete_selected_public_chain'
    a, b = current.get('positions', {}), prior.get('positions', {})
    rows = []
    for key in sorted(set(a) | set(b)):
        now, before = a.get(key), b.get(key)
        quantity, value = None, None
        if not eligible:
            status = 'comparison_unavailable'
        elif now is None:
            status = 'not_present_in_current_public_disclosure'
        elif before is None:
            status = 'newly_present_in_public_disclosure'
        else:
            quantity = difference(now['reported_quantity'], before['reported_quantity'])
            value = difference(now['reported_value_usd'], before['reported_value_usd'])
            status = 'reported_quantity_increased' if Decimal(quantity) > 0 else (
                'reported_quantity_decreased' if Decimal(quantity) < 0 else 'reported_quantity_unchanged')
        rows.append({'position_id': key, 'identity': (now or before)['identity'],
                     'current_present': now is not None, 'prior_present': before is not None, 'status': status,
                     'reported_quantity_change': quantity, 'reported_value_change_usd': value,
                     'inferred_purchase_usd': None, 'inferred_sale_usd': None,
                     'corporate_actions_adjusted': False, 'execution_inferred': False})
    return {'eligible_for_disclosure_comparison': eligible, 'rows': rows,
            'counts': dict(sorted(Counter(v['status'] for v in rows).items())),
            'interpretation': 'Reported quantity and value changes only. Corporate actions, discretion changes, omissions and interim trades are not resolved. Newly disclosed does not mean newly purchased.'}


def expected_report_period(at):
    """Last calendar quarter at least 45 days old; descriptive, not a legal deadline."""
    today = clock(at).date()
    quarter_start = date(today.year, ((today.month - 1) // 3) * 3 + 1, 1)
    end = quarter_start - timedelta(days=1)
    if (today - end).days < 45:
        start = date(end.year, ((end.month - 1) // 3) * 3 + 1, 1)
        end = start - timedelta(days=1)
    return end.isoformat()


def build(probe, read, generated_at):
    if probe.get('contract') not in ('holdings-original-source-probe.v1', 'holdings-original-acquisition.v1') or clock(probe['generated_at']) > clock(generated_at):
        raise ValueError('Retained holdings acquisition contract differs')
    refs, artifacts, funds = probe['refs'], {}, {}
    index_ev = probe['index_source']
    index_ref = {'evidence': index_ev, 'url': 'https://justhodl.ai/data/institutional-positions.json',
                 'acquired_at': index_ev['first_received_at']}
    index = decode(original(index_ref, read, index_ref['url'], generated_at))
    if (set(index.get('by_fund', {})) != set(probe['roster'])
            or any(int(index['by_fund'][name]['cik']) != int(row['cik']) for name, row in probe['roster'].items())):
        raise ValueError('Acquisition roster differs from retained detector index')
    def retain(kind, value):
        raw = encoded(value); sha = hashlib.sha256(raw).hexdigest()
        key = PREFIX + kind + '/' + sha + '.json'; artifacts[key] = raw
        return {'key': key, 'sha256': sha, 'bytes': len(raw)}
    required_period = expected_report_period(generated_at)
    for name, roster in sorted(probe['roster'].items()):
        cik = str(roster['cik'])
        if not re.fullmatch(r'\d{1,10}', cik):
            raise ValueError('Roster CIK invalid')
        url = 'https://data.sec.gov/submissions/CIK' + cik.zfill(10) + '.json'
        subref = refs[name + ':submissions']
        submissions = decode(original(subref, read, url, generated_at))
        if int(submissions['cik']) != int(cik):
            raise ValueError('Submission CIK differs from roster')
        candidates, acquired_archives, archive_sources, pending = verified_catalog(name, submissions, refs, read, generated_at)
        checked_at = max([clock(subref['acquired_at'])] + [clock(v['acquired_at']) for v in archive_sources])
        if any(clock(v['accepted_at']) > checked_at for v in candidates):
            raise ValueError('Future filing in acquired submissions')
        selected = {k: v for k, v in roster['filings_for_selected_periods'].items()}
        if probe['contract'] == 'holdings-original-acquisition.v1':
            required = {v['accession'] for v in candidates if v['form'] in ('13F-HR', '13F-HR/A')
                        and v['period_of_report'] in selected_periods(candidates)}
            if pending or set(selected) != required:
                raise ValueError('Current SEC acquisition omits required history or filings')
        by_accession = {v['accession']: v for v in candidates}
        filing_refs, filing_summaries, periods = {}, {}, defaultdict(list)
        # Candidate metadata from the detector never overrides the official submission.
        for accession in sorted(selected):
            if accession not in by_accession or by_accession[accession]['form'] not in ('13F-HR', '13F-HR/A'):
                raise ValueError('Selected filing missing from retained official submissions')
            filing = parse_filing(name, cik, by_accession[accession], refs, read, generated_at)
            periods[filing['period_of_report']].append(filing)
            filing_refs[accession] = retain('filings', filing)
            filing_summaries[accession] = {k: v for k, v in filing.items() if k != 'rows'}
        period_results = {}
        for period, filings in sorted(periods.items()):
            relevant = {v['accession'] for v in candidates if v['period_of_report'] == period and v['form'] in ('13F-HR', '13F-HR/A')}
            # An older archive ending before report date cannot contain that period's filing.
            archives_outside = all(day(v['filingTo']) < period for v in submissions['filings']['files'])
            archives_covered = all(day(v['filingTo']) < period or v['name'] in acquired_archives for v in submissions['filings']['files'])
            complete = relevant == {v['accession'] for v in filings} and archives_covered
            result = resolve_period(filings, complete)
            result['coverage'] = {'official_accessions': sorted(relevant), 'acquired_accessions': sorted(v['accession'] for v in filings),
                                  'older_archives_excluded_by_filing_dates': archives_outside,
                                  'required_archives_acquired': archives_covered}
            period_results[period] = result
        holdings_periods = selected_periods(candidates)
        latest = holdings_periods[0] if holdings_periods else None
        prior = holdings_periods[1] if len(holdings_periods) > 1 else None
        absent = {'status': 'not_acquired', 'positions': {}}
        compared = compare(period_results.get(latest, absent), period_results.get(prior, absent))
        latest_submission = max(candidates, key=lambda v: (v['period_of_report'], clock(v['accepted_at']))) if candidates else None
        detail = {'contract': 'holdings-native-fund.v1', 'fund': name, 'cik': cik.zfill(10),
                  'official_name': submissions['name'], 'configured_name': roster.get('configured_name'),
                  'submissions_source': subref, 'filings': filing_refs, 'filing_summaries': filing_summaries, 'periods': period_results,
                  'submissions_archives': archive_sources, 'unacquired_relevant_archives': [v['name'] for v in pending],
                  'current_holdings_period': latest, 'prior_holdings_period': prior, 'comparison': compared,
                  'latest_submission': latest_submission, **PERMISSION}
        funds[name] = {'cik': cik.zfill(10), 'official_name': submissions['name'], 'current_holdings_period': latest,
                       'prior_holdings_period': prior, 'latest_submission': latest_submission,
                       'observation_age_days': (clock(generated_at).date() - date.fromisoformat(latest)).days if latest else None,
                       'required_period_for_current_cohort': required_period,
                       'current_cohort_eligible': latest == required_period and period_results.get(latest, absent)['status'] == 'complete_selected_public_chain'
                           and (latest_submission is None or latest_submission['period_of_report'] <= latest),
                       'comparison_counts': compared['counts'], 'comparison_available': compared['eligible_for_disclosure_comparison'],
                       'detail': retain('funds', detail), **PERMISSION}
    cohorts = defaultdict(list)
    for name, fund in funds.items():
        cohorts[fund['current_holdings_period'] or 'unavailable'].append(name)
    output = {'contract': CONTRACT, 'generated_at': generated_at, 'source_generated_at': probe['generated_at'],
              'funds': funds, 'fund_count': len(funds), 'report_period_cohorts': dict(sorted(cohorts.items())),
              'current_cohort_count': sum(v['current_cohort_eligible'] for v in funds.values()),
              'required_period_for_current_cohort': required_period, 'source_originals': len(refs),
              'quality': {'status': 'descriptive_source_research', 'schema_validated': True,
                          'unit_inference': False, 'valuation_verified': False, 'trades_inferred': False},
              'limitations': ['Public long-security disclosures omit short positions and may omit confidential or small holdings.',
                              'Options report underlying positions; these values are not option premium or net exposure.',
                              'Managers can share discretion. Their records and market values are not independent capital flows.',
                              'Mixed quarters remain separate. Calendar freshness never makes a quarterly holding a current trade.',
                              'CUSIP, reported class, quantity type and put/call define comparison identity. Unreviewed ticker aliases are excluded.'],
              'methodology_url': GUIDANCE, **PERMISSION}
    return output, artifacts
