"""Pure, replayable SEC ownership-feed observations; no investment authority."""
from collections import defaultdict
from datetime import timedelta
from urllib.parse import quote, urlsplit
import re
import xml.etree.ElementTree as ET
from sec_atom_model import accession, clock, encode, sha, strict

CONTRACT = 'ownership-filing-observations.v1'
HEAD = 'data/activist-filings.json'
PREFIX = 'data/activist-filings/sources/'
LEGACY_FORMS = ('SC 13D', 'SC 13D/A', 'SC 13G', 'SC 13G/A')
FORMS = LEGACY_FORMS + ('SCHEDULE 13D', 'SCHEDULE 13D/A', 'SCHEDULE 13G', 'SCHEDULE 13G/A')
MAPPING = 'https://www.sec.gov/files/company_tickers.json'
NS = '{http://www.w3.org/2005/Atom}'
FLAGS = {k: False for k in ('calls_eligible','forecast_qualified','sizing_eligible',
    'execution_eligible','independent_evidence_eligible','private_state_read_or_written')}


def feed_url(form):
    if form not in FORMS:
        raise ValueError('Reviewed ownership form required')
    # Preserve all four original query bytes. Add the current XML submission
    # names explicitly; never silently substitute or discard the legacy scope.
    return ('https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type='
            + quote(form) + '&output=atom&count=100')


def source_ref(raw, kind):
    if not isinstance(raw, bytes) or len(raw) > 8*1024*1024 or kind not in ('json','xml'):
        raise ValueError('Whole bounded source required')
    digest = sha(raw)
    return {'key': PREFIX+digest+'.'+kind, 'bytes': len(raw), 'sha256': digest, 'format': kind}


def validate_ref(ref):
    if not isinstance(ref, dict) or set(ref) != {'key','bytes','sha256','format'}:
        raise ValueError('Exact source identity required')
    if (ref['format'] not in ('json','xml') or not isinstance(ref['sha256'], str)
            or not re.fullmatch('[a-f0-9]{64}', ref['sha256'])
            or type(ref['bytes']) is not int or not 0 <= ref['bytes'] <= 8*1024*1024
            or ref['key'] != PREFIX+ref['sha256']+'.'+ref['format']):
        raise ValueError('Declared source identity invalid')
    return ref


def content(attempt, sources, kind):
    if attempt.get('status') != 'received':
        if 'original_ref' in attempt:
            raise ValueError('Unavailable attempt has an undeclared original')
        return None
    ref = validate_ref(attempt.get('original_ref'))
    raw = sources.get(ref['key'])
    if not isinstance(raw, bytes) or ref['format'] != kind or source_ref(raw, kind) != ref:
        raise ValueError('Whole original source differs')
    return raw


def cik(value):
    if type(value) is int:
        value = str(value)
    if not isinstance(value, str) or not re.fullmatch(r'[0-9]{1,10}', value) or int(value) == 0:
        return None
    return value.zfill(10)


def canonical_form(value):
    return re.sub(r'^(?:SC|SCHEDULE) ', '', value) if value in FORMS else None


def index_identity(value):
    """Only reported official index links; path CIK never supplies entity role."""
    if not isinstance(value, str):
        return None
    try:
        parsed = urlsplit(value)
    except ValueError:
        return None
    if parsed.scheme != 'https' or parsed.netloc not in ('www.sec.gov','sec.gov') or parsed.query or parsed.fragment:
        return None
    found = re.fullmatch(r'/Archives/edgar/data/([0-9]{1,10})/(?:([0-9]{18})/)?([0-9]{10}-[0-9]{2}-[0-9]{6})-index\.html?', parsed.path)
    if not found or cik(found[1]) is None or found[2] and accession(found[2]) != found[3]:
        return None
    return found[3]


def parse_atom(raw, requested_form, checked_at, window_days):
    if requested_form not in FORMS or clock(checked_at) is None:
        raise ValueError('Explicit form and observation clock required')
    text = raw.decode('utf-8-sig')
    if re.search(r'<!\s*(?:DOCTYPE|ENTITY)\b', text, re.I):
        raise ValueError('DTD and entity declarations prohibited')
    root = ET.fromstring(text)
    if root.tag != NS+'feed':
        raise ValueError('SEC Atom namespace and feed root required')
    records = []
    for i, entry in enumerate(root.findall(NS+'entry')):
        issues = []
        # Every repeated field survives in the XML. Ambiguous identity fields
        # are excluded from grouping instead of selecting their first value.
        values = {name: [n.text or '' for n in entry.findall(NS+name)] for name in ('title','id','updated')}
        for name in values:
            if len(values[name]) != 1:
                issues.append(name+'_missing_or_repeated')
        title = values['title'][0] if len(values['title']) == 1 else ''
        identifier = values['id'][0] if len(values['id']) == 1 else ''
        updated = values['updated'][0] if len(values['updated']) == 1 else ''
        links = [dict(n.attrib) for n in entry.findall(NS+'link')]
        categories = [dict(n.attrib) for n in entry.findall(NS+'category')]
        match = re.fullmatch(r'((?:SC|SCHEDULE) 13[DG](?:/A)?)\s*[-–]\s*(.*?)\s*\(([0-9]{1,10})\)\s*\((Filer|Subject|Reporting)\)\s*', title)
        forms = {n.get('term') for n in categories if n.get('term')}
        if match:
            forms.add(match[1])
        if len(forms) != 1 or any(f not in FORMS for f in forms):
            issues.append('reported_form_missing_or_conflicting')
        form = next(iter(forms)) if len(forms) == 1 and next(iter(forms)) in FORMS else None
        if match is None or cik(match[3]) is None:
            issues.append('explicit_role_name_cik_missing')
        requested = canonical_form(requested_form)
        returned = canonical_form(form)
        if returned is None or (returned != requested and not ('/A' not in requested and returned == requested+'/A')):
            issues.append('outside_requested_form_family')
        urls = list(dict.fromkeys(n.get('href') for n in links if n.get('rel','alternate') == 'alternate' and n.get('href')))
        link_accessions = [index_identity(u) for u in urls]
        if len(urls) != 1 or any(a is None for a in link_accessions):
            issues.append('index_link_missing_conflicting_or_unreviewed')
        identifiers = set(re.findall(r'(?<![0-9])([0-9]{10}-[0-9]{2}-[0-9]{6})(?![0-9])', identifier))
        identifiers.update(a for a in link_accessions if a)
        if len(identifiers) != 1:
            issues.append('accession_missing_or_conflicting')
        stamp = clock(updated)
        if stamp is None:
            issues.append('invalid_feed_updated_clock')
        elif stamp > clock(checked_at):
            issues.append('future_feed_updated_clock')
        elif stamp < clock(checked_at)-timedelta(days=window_days):
            issues.append('outside_received_snapshot_window')
        records.append({'entry_index': i, 'requested_form': requested_form, 'reported_form': form,
            'canonical_form': returned, 'accession': next(iter(identifiers)) if len(identifiers)==1 else None,
            'name': match[2] if match else None, 'cik': cik(match[3]) if match else None,
            'role': match[4] if match else None, 'feed_updated_at': updated,
            'filing_accepted_at': None, 'filing_url': urls[0] if len(urls)==1 and link_accessions[0] else None,
            'atom_title': title, 'atom_id': identifier, 'atom_links': links, 'atom_categories': categories,
            'summaries': [ET.tostring(n,encoding='unicode') for n in entry.findall(NS+'summary')],
            'raw_entry_xml': ET.canonicalize(ET.tostring(entry,encoding='unicode'),rewrite_prefixes=True),
            'issues': issues, 'identity_eligible_for_grouping': not issues})
    return {'records': records, 'returned_entries': len(records),
            'feed_updated_at': root.findtext(NS+'updated',''), 'feed_attributes': dict(root.attrib),
            'feed_links': [dict(n.attrib) for n in root.findall(NS+'link')]}


def mapping(attempt, sources):
    raw = content(attempt,sources,'json')
    result = {'status': attempt.get('status'), 'records': [], 'historical_identity_verified': False}
    if raw is None or attempt.get('http_status') != 200:
        return result
    try:
        packet = strict(raw)
        if not isinstance(packet,dict):
            raise ValueError('Mapping object required')
    except (ValueError,UnicodeError,RecursionError):
        result['status']='invalid_mapping_original_retained';return result
    for key,value in packet.items():
        row=value if isinstance(value,dict) else {}
        ticker=row.get('ticker')
        valid=isinstance(ticker,str) and bool(re.fullmatch(r'[A-Z0-9][A-Z0-9.\-]{0,19}',ticker))
        result['records'].append({'source_key':key,'raw':value,'cik':cik(row.get('cik_str')),
            'ticker':ticker if valid else None,'mapping_row_valid':valid and cik(row.get('cik_str')) is not None})
    result['status']='whole_current_mapping_parsed'
    return result


def universe(attempt,sources):
    raw=content(attempt,sources,'json')
    result={'status':attempt.get('status'),'occurrences':[],'historical_membership_verified':False}
    if raw is None:return result
    try:
        p=strict(raw)
        if not isinstance(p,dict) or not isinstance(p.get('stocks'),list):raise ValueError('Whole stocks array required')
    except (ValueError,UnicodeError,RecursionError):
        result['status']='invalid_universe_original_retained';return result
    result.update(status='whole_current_stocks_array',occurrences=[{'source_index':i,'raw':r,'literal_symbol':r.get('symbol') if isinstance(r,dict) else None} for i,r in enumerate(p['stocks'])])
    return result


def build(acquisitions,sources,checked_at,window_days):
    if clock(checked_at) is None or type(window_days) is not int or not 1<=window_days<=366:
        raise ValueError('Explicit observation window required')
    if not isinstance(acquisitions,dict) or set(acquisitions)!= {'feeds','mapping','universe'}:
        raise ValueError('Complete declared acquisition plan required')
    if [a.get('endpoint') for a in acquisitions['feeds']] != [feed_url(f) for f in FORMS]:
        raise ValueError('Every original and modern feed must have one outcome in order')
    if acquisitions['mapping'].get('endpoint')!=MAPPING or acquisitions['universe'].get('endpoint')!='data/universe.json':
        raise ValueError('Declared mapping/universe required')
    references=set()
    for a in [acquisitions['mapping'],acquisitions['universe'],*acquisitions['feeds']]:
        if a.get('status') not in ('received','transport_unavailable','response_exceeds_bound',
                'not_attempted_rate_or_runtime_limit','universe_unavailable'):
            raise ValueError('Unknown source attempt state')
        if a.get('status')=='received' or 'requested_at' in a or 'received_at' in a:
            requested=clock(a.get('requested_at'));received=clock(a.get('received_at'))
            if requested is None or received is None or not requested<=received<=clock(checked_at):
                raise ValueError('Source attempt clocks invalid')
        if a.get('status')=='received':
            if a['endpoint']!='data/universe.json' and (type(a.get('http_status')) is not int or not 100<=a['http_status']<=599):
                raise ValueError('Explicit HTTP status required')
            references.add(validate_ref(a.get('original_ref'))['key'])
    if references!=set(sources):
        raise ValueError('Whole declared original population differs')
    lookup=mapping(acquisitions['mapping'],sources);members=universe(acquisitions['universe'],sources)
    coverage=[];entries=[]
    for fi,(form,a) in enumerate(zip(FORMS,acquisitions['feeds'])):
        row={'feed_index':fi,'requested_form':form,'status':a.get('status'),'returned_entries':None,
             'historical_completeness_verified':False}
        raw=content(a,sources,'xml')
        if raw is not None and a.get('http_status')==200:
            try:parsed=parse_atom(raw,form,checked_at,window_days)
            except (ValueError,UnicodeError,ET.ParseError,RecursionError) as exc:
                row.update(status='invalid_atom_original_retained',error_type=type(exc).__name__)
            else:
                row.update({k:v for k,v in parsed.items() if k!='records'});row['status']='whole_returned_atom_parsed'
                for record in parsed['records']:
                    record.update(occurrence_index=len(entries),feed_index=fi)
                    entries.append(record)
        elif raw is not None:row['status']='http_error_original_retained'
        coverage.append(row)
    grouped=defaultdict(list)
    for e in entries:
        if e['accession'] is not None:grouped[e['accession']].append(e)
    groups=[]
    for acc,rows in sorted(grouped.items()):
        valid=[r for r in rows if r['identity_eligible_for_grouping']]
        issues=[]
        if len(valid)!=len(rows):issues.append('one_or_more_occurrences_unresolved')
        forms=sorted(set(r['canonical_form'] for r in valid))
        if len(forms)!=1:issues.append('accession_form_missing_or_conflicting')
        subjects=sorted(set(r['cik'] for r in valid if r['role']=='Subject'))
        if len(subjects)!=1:issues.append('explicit_subject_missing_or_conflicting')
        parties=[]
        for role in ('Subject','Filer','Reporting'):
            for identifier in sorted(set(r['cik'] for r in valid if r['role']==role)):
                party_rows=[r for r in valid if r['role']==role and r['cik']==identifier]
                names=sorted(set(r['name'] for r in party_rows))
                if len(names)>1:issues.append('party_name_conflict')
                parties.append({'role':role,'cik':identifier,'names':names,'occurrence_indices':[r['occurrence_index'] for r in party_rows]})
        if not any(p['role'] in ('Filer','Reporting') for p in parties):issues.append('explicit_reporting_party_missing')
        subject=subjects[0] if len(subjects)==1 else None
        matches=[{'mapping_index':i,**r} for i,r in enumerate(lookup['records']) if r['mapping_row_valid'] and r['cik']==subject]
        tickers=sorted(set(r['ticker'] for r in matches))
        # Current mapping is display context only. Never pick one share class,
        # infer ownership percentage, or count amendments as independent buyers.
        groups.append({'accession':acc,'canonical_form':forms[0] if len(forms)==1 else None,
            'occurrence_indices':[r['occurrence_index'] for r in rows],'parties':parties,'subject_cik':subject,
            'current_mapping_matches':matches,'current_ticker_candidates':tickers,
            'current_universe_occurrences':[m for m in members['occurrences'] if m['literal_symbol'] in tickers],
            'identity_issues':sorted(set(issues)),'role_group_status':'resolved_from_explicit_feed_roles' if not issues else 'unresolved_feed_identity',
            'reported_beneficial_ownership_shares':None,'reported_beneficial_ownership_percent':None,
            'activist_intent_verified':False,'independent_reporting_groups_verified':False,'call':None,**FLAGS})
    if not any(r['status']=='whole_returned_atom_parsed' for r in coverage):
        raise ValueError('No current Atom feed parsed; preserve previous publication')
    return {'entry_occurrences':entries,'filing_groups':groups,'feed_coverage':coverage,
        'mapping_population':lookup,'universe_population':members,
        'quality':{'status':'partial','ingestion_status':'all_requested_feeds_parsed' if all(r['status']=='whole_returned_atom_parsed' for r in coverage) else 'partial_source_responses',
            'received_entry_occurrences':len(entries),'distinct_reported_accessions':len(groups),
            'unresolved_entry_occurrences':sum(not r['identity_eligible_for_grouping'] for r in entries),
            'explicit_role_groups':sum(not r['identity_issues'] for r in groups),
            'feed_snapshot_historical_completeness_verified':False,'original_release_vintages_verified':False,
            'document_contents_acquired':False,'investment_authority':False}}
