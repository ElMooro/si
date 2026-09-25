"""Independently verify a document catalog against complete local SEC originals.

Offline: no AWS access, vendor request, code loading or investment inference.
The input directory holds <complete-submission-sha256>.bin files. Hashes of
the full catalog and every original must match before any evidence is accepted.
"""
from pathlib import Path
import argparse, hashlib, json, re

MAX = 64 * 1024 * 1024
sha = lambda raw: hashlib.sha256(raw).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(value):
    return isinstance(value, str) and re.fullmatch('[a-f0-9]{64}', value) is not None


def field(raw, label):
    matches = re.findall(rb'(?m)^' + re.escape(label) + rb'[ \t]*([^\r\n]*)', raw)
    require(len(matches) == 1, 'Exactly one original metadata field required')
    return matches[0].strip().decode('ascii')


def verify(catalog_bytes, expected_sha256, read_original):
    require(digest(expected_sha256) and sha(catalog_bytes) == expected_sha256, 'Whole catalog hash differs')
    catalog = json.loads(catalog_bytes)
    require(catalog.get('contract') == 'buyback-submission-document-evidence.v1', 'Reviewed catalog contract required')
    require(catalog.get('all_reported_rows_conserved') is True and catalog.get('all_declared_documents_retained') is True, 'Complete evidence required')
    flags = ('filing_population_complete','authorization_amount_qualified','buyback_execution_qualified',
        'ownership_dilution_qualified','forecast_qualified','sizing_qualified','ticker_identity_verified',
        'calls_eligible','execution_eligible','keyword_search_is_semantic_or_exhaustive')
    seen, covered = set(), {};documents = words = total_bytes = 0
    for value in [catalog, *catalog['filings']]:
        require(all(value.get(key) is False for key in flags), 'Source bytes cannot grant investment or interpretation authority')
    for filing in catalog['filings']:
        source_sha = filing['original_sha256']
        require(digest(source_sha), 'Exact whole-submission SHA required')
        identity = (filing['issuer_cik'], filing['accession'])
        require(identity not in seen, 'Distinct filings cannot repeat')
        seen.add(identity)
        body = read_original(source_sha)
        require(isinstance(body, bytes) and 0 < len(body) <= MAX, 'Whole bounded original required')
        require(sha(body) == source_sha and len(body) == filing['original_bytes'], 'Complete original bytes differ')
        require(body.startswith(b'<SEC-DOCUMENT>') and body.rstrip().endswith(b'</SEC-DOCUMENT>'), 'Complete submission envelope required')
        header_end = body.find(b'</SEC-HEADER>')
        require(header_end >= 0, 'Original SEC header required')
        header = body[:header_end]
        require(field(header,b'ACCESSION NUMBER:') == filing['accession'], 'Original accession differs')
        require(field(header,b'CONFORMED SUBMISSION TYPE:') == filing['form'], 'Original filing form differs')
        require(field(header,b'FILED AS OF DATE:') == filing['filing_date'].replace('-',''), 'Original filing date differs')
        ciks = {match.decode().zfill(10) for match in re.findall(rb'(?m)^[ \t]*CENTRAL INDEX KEY:[ \t]*([0-9]+)[ \t]*\r?$', header)}
        require(filing['issuer_cik'] in ciks and sorted(ciks) == filing['header_issuer_ciks'], 'Original header issuer list differs')
        starts = list(re.finditer(rb'(?m)^<DOCUMENT>[ \t]*\r?$', body))
        ends = list(re.finditer(rb'(?m)^</DOCUMENT>[ \t]*\r?$', body))
        require(len(starts) == len(ends) == len(filing['documents']) == int(field(header,b'PUBLIC DOCUMENT COUNT:')), 'Complete raw document population differs')
        require(not body[header_end+len(b'</SEC-HEADER>'):starts[0].start()].strip(), 'Unrepresented bytes before first document')
        for i, doc in enumerate(filing['documents']):
            a,b = doc['byte_start'],doc['byte_end'];ta,tb = doc['text_byte_start'],doc['text_byte_end']
            require(all(type(n) is int for n in (a,b,ta,tb)) and 0 <= a <= ta <= tb <= b <= len(body), 'Invalid document ranges')
            require(doc['source_document'] == i and a == starts[i].start() and ends[i].end() <= b, 'Document order or boundary differs')
            next_start = starts[i+1].start() if i+1 < len(starts) else len(body)
            require(b <= next_start and not body[ends[i].end():b].strip(), 'Raw document end differs')
            if i+1 < len(starts):require(not body[b:next_start].strip(), 'Unrepresented bytes between documents')
            else:require(body[b:].strip() == b'</SEC-DOCUMENT>', 'Unrepresented bytes after last document')
            block = body[a:b]
            require(len(block) == doc['bytes'] and sha(block) == doc['sha256'], 'Raw document hash differs')
            require(len(body[ta:tb]) == doc['text_bytes'] and sha(body[ta:tb]) == doc['text_sha256'], 'Embedded text hash differs')
            opening = re.search(rb'(?m)^<TEXT>[ \t]*\r?\n', block)
            closing = re.search(rb'(?m)^</TEXT>[ \t]*\r?\n[\s]*</DOCUMENT>', block)
            require(opening is not None and closing is not None and a+opening.end() == ta and a+closing.start() == tb, 'Embedded text boundaries differ from raw SGML')
            metadata = block[:opening.start()]
            for key, tag in (('type',b'<TYPE>'),('sequence',b'<SEQUENCE>'),('filename',b'<FILENAME>')):
                require(field(metadata,tag) == doc[key], 'Original document metadata differs')
            require(re.fullmatch(r'[A-Za-z0-9_.-]+',doc['filename']) and doc['filename'] not in ('.','..'), 'Safe exact document filename required')
            base = 'https://www.sec.gov/Archives/edgar/data/'+str(int(filing['issuer_cik']))+'/'+filing['accession'].replace('-','')+'/'
            require(filing['submission_url'] == base+filing['accession']+'.txt' and doc['document_url'] == base+doc['filename'], 'Canonical SEC source URL differs')
            expected_words = [{'byte_start':ta+m.start(),'byte_end':ta+m.end(),'reported_text':m[0].decode('ascii')}
                for m in re.finditer(rb'\b(?:repurchase|buyback)\b',body[ta:tb],re.I)]
            require(expected_words == doc['literal_keyword_occurrences'], 'Literal keyword population differs')
            require(doc['keyword_matches_establish_authorization'] is False and doc['document_url_current_bytes_verified'] is False
                and doc['byte_ranges_reference'] == 'complete_submission_original_bytes', 'Document evidence meaning differs')
            words += len(expected_words);documents += 1
        total_bytes += len(body)
        for source_row in filing['source_rows']:
            require(type(source_row) is int and source_row not in covered, 'Duplicate original scanner row')
            covered[source_row] = filing['filing_id']
    require(set(covered) == set(range(catalog['reported_rows'])), 'Complete source-row population differs')
    require(len(catalog['rows']) == catalog['reported_rows'], 'Missing reported source rows')
    for i,row in enumerate(catalog['rows']):
        require(row['source_row'] == i and row['filing_id'] == covered[i], 'Source-row filing join differs')
        require(row['reported_authorization_amount_verified'] is False and row['reported_announcement_date_is_verified_event_date'] is False, 'Scanner claim was promoted')
    require((len(seen),documents,total_bytes,words) == (catalog['distinct_filings'],catalog['documents'],catalog['original_bytes'],catalog['literal_keyword_occurrences']), 'Whole catalog totals differ')
    return {'catalog_sha256':expected_sha256,'verified_submissions':len(seen),'verified_documents':documents,
        'verified_original_bytes':total_bytes,'literal_coordinates_checked':words,'reported_rows_conserved':len(covered),
        'production_inspector_imported':False,'provider_requests':0,'authorization_amount_qualified':False,
        'buyback_execution_qualified':False,'forecast_qualified':False,'sizing_qualified':False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalog',type=Path,required=True)
    parser.add_argument('--catalog-sha256',required=True)
    parser.add_argument('--originals',type=Path,required=True)
    args=parser.parse_args()
    def read(digest):
        with (args.originals/(digest+'.bin')).open('rb') as handle:return handle.read(MAX+1)
    print(json.dumps(verify(args.catalog.read_bytes(),args.catalog_sha256,read),sort_keys=True))


if __name__=='__main__':main()
