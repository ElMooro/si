"""Retain complete WGB acquisitions on the existing producer run.

Retention makes provider-reported fields reproducible, not economically qualified.
Original requests/responses stay in the existing protected sovereign prefix.
"""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import math
import re
import urllib.error
import urllib.request
import sovereign_history as storage

CONTRACT = 'sovereign-source-capture.v1'
ORIGIN = 'https://www.worldgovernmentbonds.com'
ENDPOINT = ORIGIN + '/wp-json/country/v1/main'
LIMIT = 4 * 1024 * 1024
FIELDS = {
    'bond10y_pct': 'bond10y', 'cds_bp': 'lastCds',
    'cds_default_prob_pct': 'lastCdsDefaultProb',
    'spread_vs_bund_bp': 'mainSpreadValue', 'cb_rate_pct': 'cbRateNumber',
    'rating': 'lastRatingValue', 'as_of': 'lastDataValDesc',
}


class RetentionError(RuntimeError):
    """Evidence storage failure must never become a missing-provider observation."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def utc():
    return datetime.now(timezone.utc).isoformat()


def parse_globals(raw):
    text = raw.decode('utf-8')
    match = re.search(r'var\s+jsGlobalVars\s*=\s*', text)
    if not match:
        raise ValueError('Provider request context unavailable')
    # raw_decode handles braces inside quoted strings; the old brace counter did not.
    start = text[match.end():].lstrip()
    _, end = json.JSONDecoder().raw_decode(start)
    value = storage.strict(start[:end].encode('utf-8'))
    if not isinstance(value, dict) or not value or not start[end:].lstrip().startswith(';'):
        raise ValueError('Provider request context invalid')
    return value


def project(raw):
    data = storage.strict(raw.decode('utf-8').encode('utf-8'))
    if not isinstance(data, dict) or data.get('success') is not True:
        raise ValueError('Provider response unsuccessful')
    result = {}
    for target, source in FIELDS.items():
        value = data.get(source)
        if target in ('rating', 'as_of'):
            result[target] = value if isinstance(value, str) and value.strip() else None
        else:
            try:
                number = float(value) if value not in (None, '', '----') and not isinstance(value, bool) else None
                result[target] = number if number is not None and math.isfinite(number) else None
            except (ValueError, TypeError):
                result[target] = None
    return result


def compiler_hashes():
    root = Path(__file__).parent
    return {name: storage.sha((root / name).read_bytes()) for name in
            ('lambda_function.py', 'sovereign_history.py', 'sovereign_sources.py')}


class Capture:
    def __init__(self, client, bucket, countries, user_agent, opener=None, now=utc):
        self.client, self.bucket, self.countries = client, bucket, dict(countries)
        self.user_agent = user_agent
        self.opener = opener or urllib.request.build_opener(NoRedirect()).open
        self.now = now
        self.started_at = now()
        self.requests, self.coverage = [], []
        self.slugs = {slug for slug, _ in countries.values()}

    def retain(self, raw):
        try:
            # Empty HTTP bodies have an exact hash too. A retained envelope holds
            # their identity without pretending an empty body is a quote.
            return storage.retain(self.client, self.bucket, raw) if raw else {
                'bytes': 0, 'sha256': storage.sha(b''), 'key': None}
        except Exception as exc:
            raise RetentionError('Sovereign source retention failed') from exc

    def request(self, slug, phase, body=None):
        if slug not in self.slugs or phase not in ('page', 'quote'):
            raise ValueError('Only configured sovereign provider routes allowed')
        if (phase == 'page') != (body is None):
            raise ValueError('Provider method/body mismatch')
        page = ORIGIN + '/country/' + slug + '/'
        url = page if phase == 'page' else ENDPOINT
        headers = {'User-Agent': self.user_agent, 'Accept-Encoding': 'identity'}
        if phase == 'quote':
            headers.update({'Content-Type': 'application/json', 'Accept': 'application/json, text/plain, */*',
                            'Referer': page, 'Origin': ORIGIN, 'X-Requested-With': 'XMLHttpRequest'})
        row = {'slug': slug, 'phase': phase, 'method': 'GET' if body is None else 'POST',
               'url': url, 'started_at': self.now(), 'request_body': self.retain(body) if body else None}
        self.requests.append(row)
        response = None
        try:
            try:
                response = self.opener(urllib.request.Request(url, data=body, headers=headers), timeout=25)
            except urllib.error.HTTPError as exc:
                response = exc  # Retain the complete failed response as evidence.
            status = response.getcode()
            row.update(http_status=status, resolved_url=response.geturl(),
                       response_headers={k: response.headers[k] for k in ('Content-Type', 'Content-Encoding', 'Content-Length', 'Date', 'ETag', 'Last-Modified') if k in response.headers})
            parts, size = [], 0
            while True:
                piece = response.read(min(65536, LIMIT + 1 - size))
                if not piece:
                    break
                parts.append(piece)
                size += len(piece)
                if size > LIMIT:
                    raise ValueError('Provider body exceeds whole-response limit')
            raw = b''.join(parts)
            length = response.headers.get('Content-Length')
            if length is not None and (not length.isdecimal() or int(length) != len(raw)):
                raise ValueError('Incomplete provider body')
            row['response_body'] = self.retain(raw)
            row['complete_body_retained'] = True
            if row['resolved_url'] != url or not 200 <= status < 300:
                row['status'] = 'http_or_route_rejected'
                return None
            encoding = response.headers.get('Content-Encoding', 'identity').strip().lower()
            if encoding not in ('', 'identity'):
                row['status'] = 'unsupported_encoding'
                return None
            row['status'] = 'complete_response'
            return raw
        except RetentionError:
            raise
        except Exception as exc:
            row.update(status='transport_incomplete', error_type=type(exc).__name__, complete_body_retained=False)
            return None
        finally:
            if response is not None:
                response.close()
            row['received_at'] = self.now()

    def country(self, slug):
        if slug not in self.slugs or any(r['slug'] == slug for r in self.coverage):
            raise ValueError('Unregistered or repeated country acquisition')
        row = {'slug': slug, 'status': 'page_unavailable', 'request_indices': []}
        self.coverage.append(row)
        start = len(self.requests)
        try:
            page = self.request(slug, 'page')
            if page is None:
                return None
            try:
                gv = parse_globals(page)
            except (ValueError, UnicodeError):
                row['status'] = 'request_context_invalid'
                return None
            raw = self.request(slug, 'quote', storage.encode({'GLOBALVAR': gv}))
            if raw is None:
                row['status'] = 'quote_unavailable'
                return None
            try:
                result = project(raw)
            except (ValueError, UnicodeError):
                row['status'] = 'quote_invalid'
                return None
            row.update(status='reported_fields' if result['bond10y_pct'] is not None else 'required_yield_unavailable',
                       projection_sha256=storage.sha(storage.encode(result)))
            return result
        finally:
            row['request_indices'] = list(range(start, len(self.requests)))

    def finish(self):
        if {r['slug'] for r in self.coverage} != self.slugs or len(self.coverage) != len(self.countries):
            raise ValueError('Complete configured source coverage required')
        manifest = {'contract': CONTRACT, 'started_at': self.started_at, 'completed_at': self.now(),
                    'compiler_sha256': compiler_hashes(), 'requests': self.requests, 'coverage': self.coverage,
                    'universe': [{'country': name, 'slug': slug, 'region': region} for name, (slug, region) in self.countries.items()],
                    'field_mapping': FIELDS, 'original_source_replay_verified': False,
                    'definitions_verified': False, 'observation_clocks_verified': False,
                    'scope': 'Original provider bytes and direct field extraction; units, benchmark, quote clocks and economic interpretation unqualified.'}
        ref = self.retain(storage.encode(manifest))
        return {'contract': CONTRACT, 'manifest': ref, 'compiler_sha256': manifest['compiler_sha256'],
                'configured_countries': len(self.countries), 'coverage': self.coverage,
                'requests_attempted': len(self.requests),
                'complete_responses_retained': sum(r.get('complete_body_retained') is True for r in self.requests),
                'original_source_replay_verified': False, 'definitions_verified': False, 'observation_clocks_verified': False,
                'scope': manifest['scope']}


def replay(manifest, read):
    """Read retained originals only. Never execute archived code or query providers."""
    if manifest.get('contract') != CONTRACT or manifest.get('compiler_sha256') != compiler_hashes() or manifest.get('field_mapping') != FIELDS:
        raise ValueError('Exact source compiler and field mapping required')
    if any(manifest.get(k) is not False for k in ('original_source_replay_verified', 'definitions_verified', 'observation_clocks_verified')):
        raise ValueError('Capture cannot certify its own source interpretation')
    started, ended = storage.clock(manifest['started_at']), storage.clock(manifest['completed_at'])
    if not 0 <= (ended - started).total_seconds() <= 300:
        raise ValueError('Acquisition outside native runtime')
    universe = manifest['universe']
    slugs = [r['slug'] for r in universe]
    if len(slugs) != len(set(slugs)) or sorted(slugs) != sorted(r['slug'] for r in manifest['coverage']):
        raise ValueError('Coverage identity differs')
    def original(ref):
        if ref == {'bytes': 0, 'sha256': storage.sha(b''), 'key': None}:
            return b''
        if not isinstance(ref, dict) or ref.get('key') != storage.PRIVATE + ref.get('sha256', '') + '.bin':
            raise ValueError('Protected content identity required')
        raw = read(ref['key'])
        if len(raw) != ref['bytes'] or storage.sha(raw) != ref['sha256']:
            raise ValueError('Retained source bytes differ')
        return raw
    results = {}
    requests = manifest['requests']
    assigned = []
    for country in manifest['coverage']:
        indices = country['request_indices']
        assigned.extend(indices)
        rows = [requests[i] for i in indices]
        if not 1 <= len(rows) <= 2 or [r['phase'] for r in rows] != ['page', 'quote'][:len(rows)] or any(r['slug'] != country['slug'] for r in rows):
            raise ValueError('Country request chain differs')
        bodies = []
        for row in rows:
            expected_url = ORIGIN + '/country/' + country['slug'] + '/' if row['phase'] == 'page' else ENDPOINT
            if row['url'] != expected_url or row['method'] != ('GET' if row['phase'] == 'page' else 'POST'):
                raise ValueError('Provider request identity differs')
            if not started <= storage.clock(row['started_at']) <= storage.clock(row['received_at']) <= ended:
                raise ValueError('Acquisition clock order differs')
            bodies.append(original(row['response_body']) if row.get('complete_body_retained') is True else None)
            if row['status'] == 'complete_response':
                if bodies[-1] is None or row.get('resolved_url') != expected_url or not 200 <= row.get('http_status', 0) < 300 or row.get('response_headers', {}).get('Content-Encoding', 'identity').strip().lower() not in ('', 'identity'):
                    raise ValueError('Accepted response identity differs')
        expected_status = 'page_unavailable'
        projected = None
        if rows[0]['status'] == 'complete_response':
            try:
                parse_globals(bodies[0])
            except (ValueError, UnicodeError):
                expected_status = 'request_context_invalid'
                if len(rows) != 1:
                    raise ValueError('Invalid context cannot produce POST')
            else:
                if len(rows) != 2:
                    raise ValueError('Missing quote request after usable context')
                expected_status = 'quote_unavailable'
                if rows[1]['status'] == 'complete_response':
                    try:
                        projected = project(bodies[1])
                        expected_status = 'reported_fields' if projected['bond10y_pct'] is not None else 'required_yield_unavailable'
                    except (ValueError, UnicodeError):
                        expected_status = 'quote_invalid'
        if country['status'] != expected_status:
            raise ValueError('Source gap classification differs')
        if len(rows) == 2:
            expected_body = storage.encode({'GLOBALVAR': parse_globals(bodies[0])})
            if original(rows[1]['request_body']) != expected_body:
                raise ValueError('POST does not match retained provider context')
        if country['status'] in ('reported_fields', 'required_yield_unavailable'):
            if len(rows) != 2 or any(r['status'] != 'complete_response' for r in rows):
                raise ValueError('Successful projection lacks complete requests')
            result = projected
            if storage.sha(storage.encode(result)) != country['projection_sha256'] or (result['bond10y_pct'] is None) != (country['status'] == 'required_yield_unavailable'):
                raise ValueError('Direct provider projection differs')
            results[country['slug']] = result
    if sorted(assigned) != list(range(len(requests))):
        raise ValueError('Orphaned or duplicate source requests')
    return results


def verify_publication(packet, read, countries):
    """Bind all published provider fields and all excluded countries to originals."""
    evidence = packet['source_evidence']
    ref = evidence['manifest']
    if ref['key'] != storage.PRIVATE + ref['sha256'] + '.bin':
        raise ValueError('Protected manifest identity required')
    raw = read(ref['key'])
    if len(raw) != ref['bytes'] or storage.sha(raw) != ref['sha256']:
        raise ValueError('Complete source manifest differs')
    manifest = storage.strict(raw)
    expected = [{'country': name, 'slug': slug, 'region': region} for name, (slug, region) in countries.items()]
    if manifest['universe'] != expected or evidence['coverage'] != manifest['coverage'] or evidence['compiler_sha256'] != manifest['compiler_sha256']:
        raise ValueError('Published universe, coverage or compiler differs')
    if evidence['contract'] != CONTRACT or evidence['configured_countries'] != len(countries) or evidence['requests_attempted'] != len(manifest['requests']) or evidence['complete_responses_retained'] != sum(r.get('complete_body_retained') is True for r in manifest['requests']):
        raise ValueError('Published capture counts differ')
    if any(evidence.get(k) is not False for k in ('original_source_replay_verified', 'definitions_verified', 'observation_clocks_verified')) or any(packet.get(k) is not False for k in ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified')):
        raise ValueError('Unqualified source cannot grant authority')
    if storage.clock(manifest['completed_at']) > storage.clock(packet['generated_at']):
        # Handler stamps the payload after capture completion, not before it.
        raise ValueError('Source capture completed after publication clock')
    results = replay(manifest, read)
    rows = {r['country']: r for r in packet['countries']}
    selected = {name for name, (slug, _) in countries.items() if slug in results and results[slug]['bond10y_pct'] is not None}
    if len(rows) != len(packet['countries']) or set(rows) != selected or packet['n_countries'] != len(selected) or packet['errors'] != [name for name in countries if name not in selected] or packet['n_errors'] != len(countries) - len(selected):
        raise ValueError('Published country coverage differs')
    for name in selected:
        slug, region = countries[name]
        if rows[name]['region'] != region:
            raise ValueError('Published country region differs')
        for field, value in results[slug].items():
            if rows[name].get('yield_10y_pct' if field == 'bond10y_pct' else field) != value:
                raise ValueError('Published provider field differs')
    return {'countries_with_reported_fields': len(selected), 'countries_with_gaps': len(countries) - len(selected),
            'retained_requests_checked': len(manifest['requests']), 'direct_provider_fields_replayed': sum(len(results[countries[name][0]]) for name in selected),
            'definitions_verified': False, 'observation_clocks_verified': False, 'model_qualified': False}
