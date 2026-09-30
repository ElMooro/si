"""Bounded exact-byte private HTTP sender using only invented responses."""
from pathlib import Path
from email.message import Message
import io
import json
import os
import sys
import types
import unittest
import urllib.error
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT/'aws/shared'), str(Path(__file__).resolve().parents[1]/'source')]
import portfolio_publication as pub
import private_artifact


class Response(io.BytesIO):
    def __init__(self, raw, status=200, headers=(), url=None):
        super().__init__(raw); self.status = status; self.headers = Message(); self.url = url
        for key, value in headers: self.headers.add_header(key, value)
    def geturl(self): return self.url


class PublicationTransport(unittest.TestCase):
    def call(self, response, method='POST', raw=b'{"minimum_revision":0}'):
        self.requests = []
        def open_request(request, timeout):
            self.requests.append(request); self.assertEqual(timeout, 20)
            if response.url is None: response.url = request.full_url
            return response
        def opener(handler):
            self.assertIsInstance(handler, pub.NoRedirect)
            return types.SimpleNamespace(open=open_request)
        with patch.dict(os.environ, {'PRIVATE_ARTIFACT_PROXY': pub.ORIGIN}), patch.object(private_artifact, 'service_headers', return_value={'X-JH-Service-Token': 'invented-test-token'}), patch.object(pub.urllib.request, 'build_opener', side_effect=opener):
            return pub.publication_request(method, raw, **({'token': 'invented-reservation-header', 'sha256': 'a'*64} if method == 'PUT' else {}))
    def test_sender_preserves_exact_raw_body_and_fixed_authenticated_origin(self):
        raw = b' {"zero":-0.0,"null":null,"unknown":false}\n'
        response = Response(b'{"ok":true}', headers=[('Content-Length', '11')])
        self.assertEqual(self.call(response, 'PUT', raw), {'ok': True}); self.assertTrue(response.closed)
        request = self.requests[0]
        self.assertEqual(request.data, raw); self.assertEqual(request.full_url, pub.ORIGIN+'/private-artifact?kind=portfolio-risk')
        headers = {k.lower(): v for k, v in request.header_items()}
        self.assertEqual(headers['x-jh-service-token'], 'invented-test-token')
        self.assertEqual(headers['x-jh-publication-token'], 'invented-reservation-header')
        self.assertEqual(headers['x-jh-body-sha256'], 'a'*64)
    def test_status_conflicts_are_explicit_and_other_errors_are_redacted(self):
        response = Response(b'{"ok":false,"error":"superseded_publication"}', status=409)
        with self.assertRaises(pub.SupersededMirror): self.call(response, 'PUT')
        self.assertTrue(response.closed)
        for status in [302, 401, 403, 500]:
            response = Response(b'SYNTHETIC_PRIVATE_SECRET_IN_ERROR', status=status)
            with self.assertRaises(pub.PublicationUnavailable) as error: self.call(response)
            self.assertNotIn('SYNTHETIC_PRIVATE', str(error.exception)); self.assertTrue(response.closed)
    def test_malformed_partial_oversize_and_ambiguous_acknowledgements_close(self):
        cases = [(b'{"ok":true,"ok":false}', []), (b'{"x":1e999}', []), (b'\xff', []), (b'\xef\xbb\xbf{}', []),
                 (b'{"ok":', []), (b'{}', [('Content-Length', '3')]), (b'{}', [('Content-Length', 'true')]),
                 (b'{}', [('Content-Length', '2'), ('Content-Length', '2')]), (b'{}', [('Content-Encoding', 'gzip')]),
                 (b' '*65537, []), (b'{}', [('Content-Length', '65537')])]
        for raw, headers in cases:
            response = Response(raw, headers=headers)
            with self.assertRaises(pub.PublicationUnavailable): self.call(response)
            self.assertTrue(response.closed)
    def test_redirect_and_unreviewed_origin_cannot_forward_the_service_token(self):
        response = Response(b'{}', url='https://unreviewed.invalid/private')
        with self.assertRaises(pub.PublicationUnavailable): self.call(response)
        self.assertTrue(response.closed)
        with patch.dict(os.environ, {'PRIVATE_ARTIFACT_PROXY': 'https://unreviewed.invalid'}), patch.object(private_artifact, 'service_headers') as identity:
            with self.assertRaises(pub.PublicationUnavailable): pub.publication_request('POST', b'{}')
            identity.assert_not_called()
        handler = pub.NoRedirect(); self.assertIsNone(handler.redirect_request(None, None, 302, '', {}, 'https://unreviewed.invalid'))
    def test_http_error_body_and_transport_failure_never_leak_or_succeed(self):
        body = io.BytesIO(b'SYNTHETIC_PRIVATE_SECRET')
        url = pub.ORIGIN+'/private-artifact?kind=portfolio-risk&action=reserve'
        error = urllib.error.HTTPError(url, 302, 'SYNTHETIC_PRIVATE_SECRET', Message(), body)
        for failure in [error, OSError('SYNTHETIC_PRIVATE_SECRET')]:
            opener = types.SimpleNamespace(open=lambda *a, e=failure, **kw: (_ for _ in ()).throw(e))
            with patch.dict(os.environ, {'PRIVATE_ARTIFACT_PROXY': pub.ORIGIN}), patch.object(private_artifact, 'service_headers', return_value={}), patch.object(pub.urllib.request, 'build_opener', return_value=opener):
                with self.assertRaises(pub.PublicationUnavailable) as caught: pub.publication_request('POST', b'{}')
                self.assertNotIn('SYNTHETIC_PRIVATE', str(caught.exception))
        self.assertTrue(body.closed)
    def test_elapsed_deadline_is_checked_after_read_and_response_always_closes(self):
        response = Response(b'{}')
        with patch.object(pub.time, 'monotonic', side_effect=[0, 0, 21]):
            with self.assertRaisesRegex(pub.PublicationUnavailable, 'deadline'): self.call(response)
        self.assertTrue(response.closed)


if __name__ == '__main__': unittest.main()
