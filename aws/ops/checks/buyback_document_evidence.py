"""Reconstruct a complete document catalog from retained SEC submissions.

No network, storage mutation, amount extraction or investment classification.
Every document is linked to its complete-submission hash and raw byte ranges.
The result is an explicit public projection; capture journals stay private.
"""
from datetime import datetime, timezone
from pathlib import Path
import json, re
import buyback_filing_sources as source

CONTRACT = 'buyback-submission-document-evidence.v1'
INSPECTOR_SHA = '1dd2bdde40761cbcca40ab65b20865ae2185a297ebd0ca4c8d9d0c36e10b4c25'
FLAGS = {**source.FLAGS, 'ticker_identity_verified': False, 'calls_eligible': False,
         'execution_eligible': False, 'keyword_search_is_semantic_or_exhaustive': False}


def original(ref, read):
    if (not isinstance(ref, dict) or set(ref) != {'key', 'sha256', 'bytes'}
            or not isinstance(ref['sha256'], str) or not re.fullmatch('[a-f0-9]{64}', ref['sha256'])
            or ref['key'] != source.PRIVATE + ref['sha256'] + '.bin'
            or type(ref['bytes']) is not int or not 0 < ref['bytes'] <= source.MAX):
        raise ValueError('Exact retained original reference required')
    raw = read(ref)
    if not isinstance(raw, bytes) or len(raw) != ref['bytes'] or source.sha(raw) != ref['sha256']:
        raise ValueError('Retained original bytes differ')
    return raw


def clock(value):
    if not isinstance(value, str):
        raise ValueError('Explicit retained capture timestamp required')
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('Retained capture timestamp timezone required')
    return stamp.astimezone(timezone.utc)


def no_authority(value):
    if any(value.get(key) is not False for key in source.FLAGS):
        raise ValueError('Source retention cannot acquire interpretation authority')


def document_evidence(body, document):
    # Check both byte ranges independently of the inventory parser. All offsets
    # are into the complete submission, never into decoded/normalized HTML.
    for start, end, size, digest in (
            ('byte_start', 'byte_end', 'bytes', 'sha256'),
            ('text_byte_start', 'text_byte_end', 'text_bytes', 'text_sha256')):
        a, b = document[start], document[end]
        if (type(a) is not int or type(b) is not int or not 0 <= a <= b <= len(body)
                or len(body[a:b]) != document[size] or source.sha(body[a:b]) != document[digest]):
            raise ValueError('Original document byte range differs')
    if not (document['byte_start'] <= document['text_byte_start']
            <= document['text_byte_end'] <= document['byte_end']):
        raise ValueError('Document text must remain inside its raw container')
    words = []
    for word in document['literal_keyword_occurrences']:
        a, b = word['byte_start'], word['byte_end']
        if (type(a) is not int or type(b) is not int
                or not document['text_byte_start'] <= a < b <= document['text_byte_end']
                or body[a:b].decode('ascii') != word['reported_text']):
            raise ValueError('Literal keyword source coordinate differs')
        words.append({'byte_start': a, 'byte_end': b, 'reported_text': word['reported_text']})
    keys = ('source_document', 'type', 'sequence', 'filename', 'document_url',
            'byte_start', 'byte_end', 'bytes', 'sha256',
            'text_byte_start', 'text_byte_end', 'text_bytes', 'text_sha256')
    return {**{key: document[key] for key in keys}, 'literal_keyword_occurrences': words,
            'keyword_matches_establish_authorization': False,
            'byte_ranges_reference': 'complete_submission_original_bytes',
            'document_url_current_bytes_verified': False}


def compile_output(manifest_ref, read):
    manifest = json.loads(original(manifest_ref, read))
    if (manifest.get('status') != 'captured' or manifest.get('all_reported_rows_conserved') is not True
            or manifest.get('all_declared_documents_retained') is not True):
        raise ValueError('Complete accepted source capture required')
    no_authority(manifest)
    inspector = original(manifest['inspector_source'], read)
    if (source.sha(inspector) != INSPECTOR_SHA
            or Path(source.__file__).read_bytes() != inspector):
        raise ValueError('Exact accepted submission inspector required')
    packet = json.loads(original(manifest['whole_scanner'], read))
    planned = json.loads(original(manifest['plan'], read))
    if planned != source.plan(packet) or planned['unresolved_rows']:
        raise ValueError('Whole reported source-row plan differs')
    expected = {item['spec']['url']: item for item in planned['requests']}
    records = manifest['records']
    if (len(records) != len(expected) or set(manifest['captures']) != set(expected)
            or len({r['request']['url'] for r in records}) != len(records)):
        raise ValueError('Every distinct reported filing must be represented exactly once')
    started, completed = clock(manifest['started_at']), clock(manifest['completed_at'])
    if started > completed:
        raise ValueError('Capture completion precedes acquisition')
    filings, joined = [], {}
    for record in records:
        no_authority(record)
        request = record['request']; item = expected[request['url']]
        ref = manifest['captures'][request['url']]
        if request != item['spec'] or record['source_rows'] != item['source_rows'] or record['capture'] != ref:
            raise ValueError('Reported rows and retained filing identity differ')
        capture = json.loads(original(ref, read))
        if (capture['spec'] != request or capture['http_status'] != 200
                or capture['status'] != 'response_retained' or capture['original'] != record['original']
                or not started <= clock(capture['requested_at']) <= clock(capture['received_at']) <= completed):
            raise ValueError('Exact successful capture and acquisition clocks required')
        body = original(capture['original'], read)
        inspected = source.inspect(body, request)
        if inspected != capture['inventory']:
            raise ValueError('Complete original inventory replay differs')
        for key, value in (('received_at', capture['received_at']), ('filing_date', inspected['filing_date']),
                           ('documents', len(inspected['documents'])),
                           ('literal_keyword_occurrences', inspected['literal_keyword_occurrences'])):
            if record[key] != value:
                raise ValueError('Accepted filing summary differs from original')
        documents = [document_evidence(body, doc) for doc in inspected['documents']]
        identity = request['issuer_cik'] + ':' + request['accession'] + ':' + source.sha(body)
        filing_id = source.sha(identity.encode('ascii'))
        filings.append({'filing_id': filing_id, 'issuer_cik': request['issuer_cik'],
            'accession': request['accession'], 'submission_url': request['url'],
            'filing_date': inspected['filing_date'], 'form': inspected['form'],
            'header_issuer_ciks': inspected['header_issuer_ciks'],
            'requested_at': capture['requested_at'], 'received_at': capture['received_at'],
            'original_sha256': source.sha(body), 'original_bytes': len(body),
            'capture_sha256': ref['sha256'], 'source_rows': item['source_rows'],
            'documents': documents, 'all_declared_documents_retained': True, **FLAGS})
        for index in item['source_rows']:
            if index in joined:
                raise ValueError('Reported source row belongs to more than one filing')
            joined[index] = filing_id
    if set(joined) != set(range(planned['reported_rows'])):
        raise ValueError('Every reported source row must remain in the evidence catalog')
    rows = [{'source_row': i, 'filing_id': joined[i],
        'reported_label': row.get('ticker', row.get('symbol')),
        'reported_company': row.get('company'), 'reported_announcement_date': row.get('announcement_date'),
        'reported_authorization_usd': row.get('authorization_usd'),
        'reported_announcement_date_is_verified_event_date': False,
        'reported_authorization_amount_verified': False} for i, row in enumerate(packet['top_opportunities'])]
    return {'contract': CONTRACT, 'manifest_sha256': manifest_ref['sha256'],
        'inspector_sha256': INSPECTOR_SHA, 'generated_at': manifest['completed_at'],
        'capture_started_at': manifest['started_at'], 'capture_completed_at': manifest['completed_at'],
        'source_packet': {'key': 'data/buyback-scanner.json',
            'sha256': manifest['whole_scanner']['sha256'], 'bytes': manifest['whole_scanner']['bytes'],
            'reported_as_of': packet.get('as_of')},
        'reported_rows': len(rows), 'distinct_filings': len(filings),
        'documents': sum(len(f['documents']) for f in filings),
        'original_bytes': sum(f['original_bytes'] for f in filings),
        'literal_keyword_occurrences': sum(len(d['literal_keyword_occurrences']) for f in filings for d in f['documents']),
        'rows': rows, 'filings': sorted(filings, key=lambda f: f['filing_id']),
        'all_reported_rows_conserved': True, 'all_declared_documents_retained': True,
        'evidence_scope': 'All rows in the retained scanner packet; not a market-wide discovery census.',
        'document_link_scope': 'Link identifies the SEC document; byte evidence refers to its embedded copy in the retained complete submission.',
        'literal_keyword_scope': 'Literal matches only; absence or presence is not an authorization, execution or amount classification.',
        **FLAGS}
