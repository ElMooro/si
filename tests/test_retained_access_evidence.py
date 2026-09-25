from pathlib import Path
import io, sys, unittest, urllib.error
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'aws/ops/checks'))
import retained_access_evidence as evidence

KEY = evidence.PREFIX + 'share-structure-research/' + 'a' * 64 + '.bin'


class Tests(unittest.TestCase):
    def run_codes(self, codes):
        pending = iter(codes); calls = []; waits = []
        def open_one(request, timeout):
            calls.append((request.full_url, request.method, timeout, request.headers))
            code = next(pending)
            if code is None:
                raise TimeoutError('not included in public evidence')
            raise urllib.error.HTTPError(request.full_url, code, 'test', {}, io.BytesIO(b'body must not be read'))
        result = evidence.check(KEY, open_one, waits.append, lambda: '2026-09-25T00:00:00+00:00')
        return result, calls, waits

    def test_explicit_denial_on_both_anonymous_origins(self):
        result, calls, waits = self.run_codes([403, 404])
        self.assertTrue(result['denied']); self.assertEqual(waits, [])
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(c[1] == 'HEAD' and 'Authorization' not in c[3] for c in calls))
        self.assertEqual(evidence.summarize([result])['attempt_outcome_counts'], {'403': 1, '404': 1})

    def test_transient_attempts_are_retained_and_bounded(self):
        result, calls, waits = self.run_codes([503, None, 403, 429, 403])
        self.assertTrue(result['denied']); self.assertEqual(waits, [1, 2, 1])
        self.assertEqual(len(calls), 5)
        self.assertEqual(result['origins'][0]['attempts'][1]['error_type'], 'TimeoutError')

    def test_success_redirect_and_exhausted_transport_do_not_count_as_denials(self):
        for first in (200, 204, 301, 302, 405, 500, None):
            codes = [first] * (3 if first in (500, None) else 1) + [403]
            result, _, _ = self.run_codes(codes)
            self.assertFalse(result['denied'])
            self.assertEqual(evidence.summarize([result])['failures'], [result])

    def test_paths_cannot_redirect_requests_or_escape_the_reviewed_prefix(self):
        for key in ('data/public.json', KEY + '?token=secret', KEY + '#fragment',
                    evidence.PREFIX + '../secret', evidence.PREFIX + '/x', 'https://example.com/x'):
            with self.assertRaises(ValueError): evidence.urls(key)
        self.assertIsNone(evidence.NoRedirect().redirect_request(None, None, 302, '', {}, 'https://example.com'))


if __name__ == '__main__': unittest.main(verbosity=2)
