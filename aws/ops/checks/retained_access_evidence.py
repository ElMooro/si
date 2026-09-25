"""Record anonymous HEAD outcomes; unknown transport states are never denials."""
from datetime import datetime, timezone
import re, time, urllib.error, urllib.request

BUCKET = 'justhodl-dashboard-live'
PREFIX = 'audit-private/20260909-originals/'


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def urls(key):
    if not isinstance(key, str) or not key.startswith(PREFIX) or not re.fullmatch(r'[A-Za-z0-9_./-]+', key):
        raise ValueError('Reviewed private research key required')
    if any(part in ('', '.', '..') for part in key.split('/')):
        raise ValueError('Canonical private research key required')
    return ('https://justhodl.ai/' + key, 'https://' + BUCKET + '.s3.amazonaws.com/' + key)


def check(key, transport=None, pause=time.sleep, clock=None):
    targets = urls(key)
    opener = transport or urllib.request.build_opener(NoRedirect()).open
    now = clock or (lambda: datetime.now(timezone.utc).isoformat())
    result = {'key': key, 'method': 'HEAD', 'origins': [], 'denied': True}
    for url in targets:
        attempts = []
        for number in range(3):
            outcome = {'requested_at': now(), 'http_status': None, 'error_type': None}
            req = urllib.request.Request(url, method='HEAD', headers={'User-Agent': 'justhodl-verify-release/1.0'})
            try:
                with opener(req, timeout=25) as response:
                    outcome['http_status'] = response.status
            except urllib.error.HTTPError as exc:
                outcome['http_status'] = exc.code
                exc.close()
            except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
                outcome['error_type'] = type(exc).__name__
            outcome['received_at'] = now()
            attempts.append(outcome)
            code = outcome['http_status']
            transient = code is None or code == 429 or code in (500, 502, 503, 504)
            if not transient or number == 2:
                break
            pause(number + 1)
        denied = code in (401, 403, 404)
        result['origins'].append({'url': url, 'attempts': attempts, 'denied': denied})
        result['denied'] = result['denied'] and denied
    return result


def summarize(rows):
    counts = {}
    for row in rows:
        for origin in row['origins']:
            for attempt in origin['attempts']:
                key = str(attempt['http_status']) if attempt['http_status'] is not None else attempt['error_type']
                counts[key] = counts.get(key, 0) + 1
    failures = [row for row in rows if not row['denied']]
    return {'protected_paths_checked': len(rows), 'anonymous_origins_checked': len(rows) * 2,
            'attempt_outcome_counts': counts, 'all_denied': not failures, 'failures': failures}
