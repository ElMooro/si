"""Fetch complete official SEC originals for the configured institutional roster.

Network requests are paced and bounded. Any incomplete acquisition leaves the
previous publication intact; the failed attempt retains successfully read inputs.
"""
from datetime import datetime, timezone
import hashlib
import json
import re
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit

import evidence_store
import holdings_native as model

PROVIDER = 'sec_holdings_original'
INDEX = 'data/institutional-positions.json'


def now():
    return datetime.now(timezone.utc).isoformat()


def bounded(stream):
    try:
        raw = stream.read(model.MAX_BYTES + 1)
    finally:
        stream.close()
    if not 0 < len(raw) <= model.MAX_BYTES:
        raise ValueError('Complete source response exceeds bound or is empty')
    return raw


def validate_url(url):
    parsed = urlsplit(url)
    if (parsed.scheme != 'https' or parsed.hostname not in ('www.sec.gov', 'data.sec.gov')
            or parsed.query or parsed.fragment or parsed.username or parsed.password or parsed.port):
        raise ValueError('Official SEC source URL required')
    allowed = (re.fullmatch(r'/submissions/CIK\d{10}(?:-submissions-\d{3})?\.json', parsed.path)
               if parsed.hostname == 'data.sec.gov' else
               re.fullmatch(r'/Archives/edgar/data/\d{1,10}/\d{18}/(?:index\.json|[A-Za-z0-9_.-]+\.xml)', parsed.path))
    if not allowed:
        raise ValueError('SEC source path outside the reviewed collector')


def persist(client, bucket, category, packet):
    raw = model.encoded(packet); sha = hashlib.sha256(raw).hexdigest()
    key = model.PREFIX + category + '/' + sha + '.json'
    try:
        client.put_object(Bucket=bucket, Key=key, Body=raw, ContentType='application/json',
                          CacheControl='public, max-age=31536000, immutable', IfNoneMatch='*')
    except Exception as exc:
        if str((getattr(exc, 'response', {}) or {}).get('Error', {}).get('Code', '')) not in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
            raise
    if bounded(client.get_object(Bucket=bucket, Key=key)['Body']) != raw:
        raise ValueError('Retained SEC acquisition differs')
    return {'key': key, 'sha256': sha, 'bytes': len(raw)}


def acquire(client, bucket, user_agent, opener=urllib.request.urlopen, sleep=time.sleep, monotonic=time.monotonic, request_id=None):
    if not isinstance(user_agent, str) or not user_agent.strip() or '\n' in user_agent or '\r' in user_agent:
        raise ValueError('Existing SEC identification is required')
    if request_id is not None and (not isinstance(request_id, str) or not re.fullmatch(r'holdings-[a-z0-9-]{8,64}', request_id)):
        raise ValueError('Bounded holdings collection request identifier required')
    started = now(); refs, roster, errors = {}, {}, {}
    index_ref = None
    last_request = None

    def fetch(label, url):
        nonlocal last_request
        validate_url(url)
        if last_request is not None:
            sleep(max(0, 0.35 - (monotonic() - last_request)))
        last_request = monotonic()
        request = urllib.request.Request(url, headers={'User-Agent': user_agent, 'Accept': '*/*'})
        with opener(request, timeout=35) as response:
            validate_url(response.geturl())
            if response.geturl() != url or response.status != 200:
                raise ValueError('Unexpected SEC redirect or response status')
            headers = {k: response.headers.get(k) for k in ('Content-Type', 'ETag', 'Last-Modified', 'Date')}
            raw = bounded(response)
        stamp = now()
        ev = evidence_store.capture(client, bucket, PROVIDER, url, raw, model.clock(stamp))
        if evidence_store.read_verified(client, bucket, ev) != raw:
            raise ValueError('Original SEC retention differs')
        refs[label] = {'url': url, 'acquired_at': stamp, 'response_headers': headers, 'evidence': ev}
        return raw

    try:
        raw = bounded(client.get_object(Bucket=bucket, Key=INDEX)['Body'])
        index = model.decode(raw)
        index_ref = evidence_store.capture(client, bucket, PROVIDER, 'https://justhodl.ai/' + INDEX, raw)
        if evidence_store.read_verified(client, bucket, index_ref) != raw:
            raise ValueError('Retained roster differs')
        funds = index['by_fund']
        if not isinstance(funds, dict) or not 1 <= len(funds) <= 60:
            raise ValueError('Configured institutional roster requires review')
        for name, configured in sorted(funds.items()):
            if not re.fullmatch(r'[A-Z0-9_]{1,60}', name):
                raise ValueError('Configured manager identity invalid')
            cik = str(configured['cik'])
            if not re.fullmatch(r'\d{1,10}', cik):
                raise ValueError('Configured manager CIK invalid')
            submissions = model.decode(fetch(name + ':submissions', 'https://data.sec.gov/submissions/CIK' + cik.zfill(10) + '.json'))
            if int(submissions['cik']) != int(cik):
                raise ValueError('Official manager CIK differs')
            catalogs = [submissions['filings']['recent']]; acquired = set()
            candidates = model.merge_submissions(catalogs)
            while True:
                pending = model.pending_archives(submissions, candidates, acquired)
                if not pending:
                    break
                item = pending[0]
                if not item['name'].startswith('CIK' + cik.zfill(10) + '-'):
                    raise ValueError('Archive CIK identity differs')
                archive = model.decode(fetch(name + ':submissions-archive:' + item['name'],
                    'https://data.sec.gov/submissions/' + item['name']))
                if len(archive['accessionNumber']) != item['filingCount']:
                    raise ValueError('Submission archive is incomplete')
                catalogs.append(archive); acquired.add(item['name']); candidates = model.merge_submissions(catalogs)
            periods = model.selected_periods(candidates)
            selected = {v['accession']: v for v in candidates if v['form'] in ('13F-HR', '13F-HR/A') and v['period_of_report'] in periods}
            if len(selected) > 48:
                raise ValueError('Review full amendment chain without truncation')
            roster[name] = {'configured_name': configured.get('name'), 'cik': cik, 'official_name': submissions['name'],
                'detector_latest': configured.get('latest_filing'), 'detector_prior': configured.get('prior_filing'),
                'filings_for_selected_periods': selected,
                'history_scope': 'Latest two reported holdings periods, complete official recent/archive submissions and all visible amendments.'}
            for accession, filing in sorted(selected.items()):
                base = 'https://www.sec.gov/Archives/edgar/data/' + str(int(cik)) + '/' + accession.replace('-', '') + '/'
                label = name + ':' + accession + ':'
                directory = model.decode(fetch(label + 'index', base + 'index.json'))
                files = [v['name'] for v in directory['directory']['item'] if v.get('name', '').lower().endswith('.xml')]
                if not files or len(files) > 12 or len(set(files)) != len(files):
                    raise ValueError('Review complete SEC XML document set')
                for filename in sorted(files):
                    if not re.fullmatch(r'[A-Za-z0-9_.-]+\.xml', filename):
                        raise ValueError('Unsafe SEC XML document name')
                    model.tree(fetch(label + filename, base + filename))
        packet = {'contract': 'holdings-original-acquisition.v1', 'started_at': started, 'generated_at': now(), 'request_id': request_id,
                  'index_source': index_ref, 'roster': roster, 'refs': refs, 'source_status_codes': errors,
                  'source': 'Official SEC submissions, filing directories, complete cover and information-table XML',
                  'paid_ai_calls': 0, 'notifications_sent': 0, 'private_account_reads': 0, 'portfolio_writes': 0}
        return persist(client, bucket, 'probes', packet)
    except Exception as exc:
        failure = 'HTTP_' + str(exc.code) if isinstance(exc, urllib.error.HTTPError) else type(exc).__name__
        attempt = {'contract': 'holdings-acquisition-failed.v1', 'started_at': started, 'generated_at': now(),
                   'index_source': index_ref, 'retained_sources': refs, 'partial_roster': roster,
                   'failure_class': failure, 'published': False}
        ref = persist(client, bucket, 'acquisition-attempts', attempt)
        print(json.dumps({'holdings_acquisition_failure': ref['key'], 'failure_class': failure}))
        raise
