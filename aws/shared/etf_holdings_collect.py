"""Bounded whole holdings responses from the existing provider subscription."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
import threading, time, urllib.request, urllib.error
import etf_holdings_native as native
from provider_flow_catalog import ETF_UNIVERSE


def now(): return datetime.now(timezone.utc).isoformat()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs): raise ValueError('Provider redirect rejected')


class Collector:
    def __init__(self, credential, save, deadline, query_date=None):
        self.credential = credential
        self.save = save
        self.deadline = deadline
        self.query_date = query_date or datetime.now(timezone.utc).date().isoformat()
        native.day(self.query_date)
        self.lock = threading.Lock()
        self.last = 0.0
        self.requests = 0
        self.source_bytes = 0

    def response(self, url):
        native.next_url(url)
        for attempt in range(2):
            with self.lock:
                delay = max(0, .125 - (time.monotonic() - self.last))
                if time.monotonic() + delay + 21 >= self.deadline:
                    return None, 'acquisition_deadline', None
                if delay: time.sleep(delay)
                self.last = time.monotonic();self.requests += 1
                if self.requests > len(ETF_UNIVERSE) * 2 * (native.MAX_PAGES + 1) * 2:
                    return None, 'response_rejected', None
            request = urllib.request.Request(url, headers={'User-Agent': 'JustHodl-OriginalETFHoldings/1.0',
                                                          'Authorization': 'Bearer ' + self.credential})
            try:
                response = urllib.request.build_opener(NoRedirect).open(request, timeout=20)
                try: raw = response.read(native.MAX_SOURCE_BYTES + 1)
                finally: response.close()
                if not 0 < len(raw) <= native.MAX_SOURCE_BYTES or self.credential.encode() in raw:
                    return None, 'response_rejected', None
                with self.lock:
                    if self.source_bytes + len(raw) > 256 * 1024 * 1024: return None, 'response_rejected', None
                    self.source_bytes += len(raw)
                return raw, None, None
            except urllib.error.HTTPError as exc:
                status = exc.code;exc.close()
                if attempt == 0 and status in (429, 500, 502, 503, 504) and time.monotonic() + 23 < self.deadline:
                    time.sleep(1);continue
                return None, 'provider_http_error', status
            except Exception:
                return None, 'provider_request_failed', None

    def page(self, url):
        raw, error, http_status = self.response(url)
        if error: return None, {'status': error, **({'http_status': http_status} if http_status else {})}
        stamp = now();ref = self.save(raw)
        entry = {'url': url, 'acquired_at': stamp, 'original': ref}
        try:
            doc = native.original(ref, lambda _: raw)
            if doc.get('next_url'): native.next_url(doc['next_url'])
        except Exception:
            return None, {'status': 'response_rejected', 'rejected_original': ref}
        return doc, entry

    def snapshot(self, ticker, cutoff):
        result = {'ticker': ticker, 'cutoff': cutoff, 'selection': None, 'pages': [],
                  'status': 'credential_unavailable'}
        if not self.credential: return result
        body, selection = self.page(native.selection_url(ticker, cutoff))
        if body is None: result.update(selection);return result
        result['selection'] = selection
        if not body['results']:
            result['status'] = 'unavailable';return result
        try:
            if len(body['results']) != 1: raise ValueError('One selected date required')
            row = body['results'][0];processed = row['processed_date']
            if row['composite_ticker'] != ticker or native.day(processed) > native.day(cutoff):
                raise ValueError('Selected snapshot identity differs')
            url = native.snapshot_url(ticker, processed)
        except (KeyError, TypeError, ValueError):
            result['status'] = 'response_rejected';return result
        seen = set()
        for _ in range(native.MAX_PAGES):
            if url in seen:
                result['status'] = 'response_rejected';break
            seen.add(url)
            body, ref = self.page(url)
            if body is None: result.update(ref);break
            result['pages'].append(ref)
            if not body.get('next_url'):
                result['status'] = 'complete_returned_snapshot';break
            url = body['next_url']
        else: result['status'] = 'pagination_bound'
        result['completed_at'] = now()
        return result

    def fund(self, ticker):
        prior = (native.day(self.query_date) - timedelta(days=30)).isoformat()
        return {'current': self.snapshot(ticker, self.query_date), 'prior': self.snapshot(ticker, prior)}

    def collect(self):
        result = {}
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(self.fund, ticker): ticker for ticker in sorted(ETF_UNIVERSE)}
            for future in as_completed(futures):
                result[futures[future]] = future.result()
        return result, self.requests, self.source_bytes
