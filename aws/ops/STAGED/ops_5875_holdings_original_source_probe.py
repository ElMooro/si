"""Retain SEC filing originals for the existing roster, without invoking engines."""
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit
import hashlib
import json
import re
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/ops')]
from evidence_store import capture, read_verified
from ops_report import report

BUCKET = 'justhodl-dashboard-live'
LIMIT = 64 * 1024 * 1024
PROVIDER = 'sec_holdings_original'


def bounded(stream):
    try:
        raw = stream.read(LIMIT + 1)
    finally:
        stream.close()
    if not raw or len(raw) > LIMIT:
        raise ValueError('Complete original exceeds explicit bound')
    return raw


def original(s3, url, user_agent):
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or parsed.hostname not in ('www.sec.gov', 'data.sec.gov') or parsed.query:
        raise ValueError('Official SEC source required')
    time.sleep(0.35)
    request = urllib.request.Request(url, headers={'User-Agent': user_agent, 'Accept': '*/*'})
    with urllib.request.urlopen(request, timeout=45) as response:
        headers = {k: response.headers.get(k) for k in ('Content-Type', 'ETag', 'Last-Modified', 'Date')}
        raw = bounded(response)
    at = datetime.now(timezone.utc)
    ref = {'url': url, 'acquired_at': at.isoformat(), 'response_headers': headers,
           'evidence': capture(s3, BUCKET, PROVIDER, url, raw, at)}
    assert read_verified(s3, BUCKET, ref['evidence']) == raw
    return ref, raw


def xml_summary(raw):
    if b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():
        raise ValueError('Unexpected XML declaration')
    root = ET.fromstring(raw)
    local = lambda node: node.tag.rsplit('}', 1)[-1]
    info = [node for node in root.iter() if local(node) == 'infoTable']
    if not info:
        wanted = {'schemaVersion', 'submissionType', 'reportCalendarOrQuarter', 'periodOfReport',
                  'isAmendment', 'amendmentNo', 'amendmentType', 'reportType',
                  'tableEntryTotal', 'tableValueTotal', 'isConfidentialOmitted'}
        fields = {name: [(node.text or '').strip() for node in root.iter() if local(node) == name] for name in sorted(wanted)}
        managers = [ET.tostring(node, encoding='unicode') for node in root.iter() if local(node) == 'filingManager']
        return {'kind': 'cover_or_other_xml', 'root': local(root), 'fields': fields, 'filing_managers': managers}
    options, types, samples = Counter(), Counter(), []
    total = Decimal(0)
    for row in info:
        fields = {local(node): (node.text or '').strip() for node in row.iter() if len(node) == 0}
        amount = Decimal(fields['value'])
        assert amount.is_finite() and amount >= 0
        total += amount
        options[fields.get('putCall') or 'NOT_OPTION'] += 1
        types[fields.get('sshPrnamtType') or 'MISSING'] += 1
        if fields.get('cusip') in ('037833100', '191216100', '247361702') and len(samples) < 18:
            samples.append(fields)
    return {'kind': 'information_table', 'rows': len(info), 'reported_value_sum_decimal': format(total, 'f'),
            'value_unit': 'Not inferred by this acquisition probe; inspect filing schema and cover.',
            'option_rows': dict(options), 'share_principal_types': dict(types), 'selected_raw_records': samples}


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    # Reuse the production collector's existing SEC identification, never log its environment.
    conf = boto3.client('lambda', region_name='us-east-1').get_function_configuration(FunctionName='justhodl-13f-positions')
    user_agent = conf.get('Environment', {}).get('Variables', {}).get('USER_AGENT') or 'JustHodl Research raafouis@gmail.com'
    index_raw = bounded(s3.get_object(Bucket=BUCKET, Key='data/institutional-positions.json')['Body'])
    index = json.loads(index_raw)
    index_ref = capture(s3, BUCKET, PROVIDER, 'https://justhodl.ai/data/institutional-positions.json', index_raw)
    assert read_verified(s3, BUCKET, index_ref) == index_raw
    refs, summaries, errors, roster = {}, {}, {}, {}
    def acquire(label, url, kind):
        try:
            ref, raw = original(s3, url, user_agent)
            refs[label] = ref
            doc = json.loads(raw) if kind == 'json' else None
            if kind == 'xml':
                summaries[label] = xml_summary(raw)
            return doc
        except Exception as exc:
            errors[label] = 'HTTP_' + str(exc.code) if isinstance(exc, urllib.error.HTTPError) else type(exc).__name__
            return None
    with report('ops_5875_holdings_original_source_probe') as r:
        r.kv(production_engines_invoked=0, paid_ai_calls=0, notifications_sent=0, private_account_reads=0, portfolio_writes=0,
             index_source=index_ref, index_generated_at=index.get('generated_at'))
        funds = index['by_fund']
        assert len(funds) == 18, 'Review changed roster before expanding acquisition'
        for name, fund in sorted(funds.items()):
            cik = str(fund['cik'])
            assert re.fullmatch(r'\d{1,10}', cik)
            submissions = acquire(name + ':submissions', 'https://data.sec.gov/submissions/CIK' + cik.zfill(10) + '.json', 'json')
            periods = {fund[k]['period_of_report'] for k in ('latest_filing', 'prior_filing') if fund.get(k)}
            candidates = {}
            if submissions:
                recent = submissions['filings']['recent']
                for i, accession in enumerate(recent['accessionNumber']):
                    if recent['form'][i] not in ('13F-HR', '13F-HR/A') or recent['reportDate'][i] not in periods:
                        continue
                    candidates[accession] = {'accession': accession, 'filed_at': recent['filingDate'][i],
                        'accepted_at': recent.get('acceptanceDateTime', [None] * len(recent['accessionNumber']))[i],
                        'period_of_report': recent['reportDate'][i], 'form': recent['form'][i], 'primary_doc': recent['primaryDocument'][i],
                        'origin': 'official_recent_submissions'}
            for kind in ('latest_filing', 'prior_filing'):
                if fund.get(kind):
                    candidate = fund[kind]
                    candidates.setdefault(candidate['accession'], {**candidate, 'origin': 'current_detector_index'})
            assert len(candidates) <= 24, 'Review unusually large amendment chain without truncation'
            roster[name] = {'configured_name': fund.get('name'), 'cik': cik,
                            'official_name': submissions.get('name') if submissions else None,
                            'submissions_cik': submissions.get('cik') if submissions else None,
                            'detector_latest': fund.get('latest_filing'), 'detector_prior': fund.get('prior_filing'),
                            'filings_for_selected_periods': candidates,
                            'history_scope': 'Recent official submissions plus detector-selected filings. Older submissions archive files not acquired in this probe.'}
            for accession, filing in sorted(candidates.items()):
                assert re.fullmatch(r'\d{10}-\d{2}-\d{6}', accession)
                base = 'https://www.sec.gov/Archives/edgar/data/' + str(int(cik)) + '/' + accession.replace('-', '') + '/'
                label = name + ':' + accession
                directory = acquire(label + ':index', base + 'index.json', 'json')
                if not directory:
                    continue
                names = [item['name'] for item in directory['directory']['item'] if str(item.get('name', '')).lower().endswith('.xml')]
                if not names or len(names) > 12 or any(not re.fullmatch(r'[A-Za-z0-9_.-]+\.xml', v) for v in names):
                    errors[label + ':xml_discovery'] = 'Review_complete_directory_no_truncation'
                    continue
                for filename in names:
                    acquire(label + ':' + filename, base + filename, 'xml')
        packet = {'contract': 'holdings-original-source-probe.v1', 'generated_at': datetime.now(timezone.utc).isoformat(),
                  'index_source': index_ref, 'roster': roster, 'refs': refs, 'summaries': summaries, 'source_status_codes': errors,
                  'production_engines_invoked': 0, 'paid_ai_calls': 0, 'notifications_sent': 0,
                  'private_account_reads': 0, 'portfolio_writes': 0}
        raw = json.dumps(packet, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
        sha = hashlib.sha256(raw).hexdigest()
        key = 'data/holdings-research/probes/' + sha + '.json'
        s3.put_object(Bucket=BUCKET, Key=key, Body=raw, ContentType='application/json', IfNoneMatch='*', CacheControl='public, max-age=31536000, immutable')
        assert bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body']) == raw
        counts = Counter(v['kind'] for v in summaries.values())
        r.kv(probe={'key': key, 'sha256': sha, 'bytes': len(raw)}, retained_responses=len(refs), xml_kinds=dict(counts),
             source_status_codes=errors, official_names={k: v['official_name'] for k, v in roster.items()},
             audit_scope='Retained original SEC submissions, filing directory and complete XML bytes. No production packet, cache, scores, classification or portfolio changed.')
        assert counts['information_table'] >= 2 and counts['cover_or_other_xml'] >= 2, 'Original filing evidence needs review'


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print('Holdings original-source probe failed; inspect sanitized committed report.')
        sys.exit(1)
