"""Bounded original-response collection using the already configured provider key."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import threading, time, urllib.request, urllib.error
import provider_flow_native as native
import provider_flow_catalog as catalog


def now(): return datetime.now(timezone.utc).isoformat()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs): raise ValueError('Provider redirect rejected')


class Collector:
    def __init__(self, credential, save, deadline):
        self.credential = credential
        self.save = save
        self.deadline = deadline
        self.lock = threading.Lock()
        self.last = 0.0
        self.requests = 0
        self.source_bytes = 0
        self.query_date = datetime.now(timezone.utc).date().isoformat()

    def response(self, url):
        native.next_url(url)
        for attempt in range(2):
            with self.lock:
                delay = max(0, .125 - (time.monotonic() - self.last))
                if time.monotonic() + delay + 16 >= self.deadline: return None, 'acquisition_deadline', None
                if delay: time.sleep(delay)
                self.last = time.monotonic(); self.requests += 1
            request = urllib.request.Request(url, headers={'User-Agent': 'JustHodl-OriginalFundFlows/1.0',
                                                          'Authorization': 'Bearer ' + self.credential})
            try:
                response = urllib.request.build_opener(NoRedirect).open(request, timeout=15)
                try: raw = response.read(native.MAX_SOURCE_BYTES + 1)
                finally: response.close()
                if not 0 < len(raw) <= native.MAX_SOURCE_BYTES or self.credential.encode() in raw:
                    return None, 'response_rejected', None
                with self.lock:
                    if self.source_bytes + len(raw) > 64 * 1024 * 1024:
                        return None, 'response_rejected', None
                    self.source_bytes += len(raw)
                return raw, None, None
            except urllib.error.HTTPError as exc:
                status = exc.code; exc.close()
                if attempt == 0 and status in (429, 500, 502, 503, 504) and time.monotonic() + 18 < self.deadline:
                    time.sleep(1); continue
                return None, 'provider_http_error', status
            except Exception:
                return None, 'provider_request_failed', None

    def fund(self, ticker):
        doc = {'ticker': ticker, 'query_date': self.query_date, 'pages': [], 'status': 'credential_unavailable'}
        if self.credential:
            url = native.initial_url(ticker, self.query_date); seen = set()
            for _ in range(native.MAX_PAGES):
                if url in seen:
                    doc['status'] = 'response_rejected'; break
                seen.add(url)
                raw, error, http_status = self.response(url)
                if error:
                    doc['status'] = error
                    if http_status is not None: doc['http_status'] = http_status
                    break
                ref = self.save(raw)
                try:
                    body = native.original(ref, lambda _: raw)
                    following = native.next_url(body['next_url']) if body.get('next_url') else None
                except Exception:
                    doc.update(status='response_rejected', rejected_original=ref); break
                doc['pages'].append({'url': url, 'acquired_at': now(), 'original': ref})
                if following is None:
                    doc['status'] = 'retained'; break
                url = following
            else: doc['status'] = 'pagination_bound'
        doc['completed_at'] = now()
        return doc

    def collect(self):
        result = {}
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(self.fund, ticker): ticker for ticker in sorted(catalog.ETF_UNIVERSE)}
            for future in as_completed(futures):
                ticker = futures[future]
                # Storage failures are not disguised as provider absence.
                result[ticker] = future.result()
        return result, self.requests
