from pathlib import Path
import sys,unittest,urllib.error
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/ops/staged'))
from ops_5998_option_population_retained_acceptance import denied_with_retry


class Tests(unittest.TestCase):
    def test_transport_error_requires_later_actual_denial(self):
        attempts=[];pauses=[]
        def check(url):
            attempts.append(url)
            if len(attempts)<3:raise urllib.error.URLError('synthetic reset')
            return True
        self.assertTrue(denied_with_retry('https://example.test/private',check,pauses.append))
        self.assertEqual(len(attempts),3);self.assertEqual(pauses,[1,2])
    def test_public_success_is_not_retried_until_hidden(self):
        calls=[]
        self.assertFalse(denied_with_retry('x',lambda url:calls.append(url) or False,lambda _:None))
        self.assertEqual(calls,['x'])
    def test_exhausted_transport_errors_are_not_denial(self):
        attempts=[]
        def check(url):attempts.append(url);raise ConnectionResetError('synthetic')
        with self.assertRaises(ConnectionResetError):denied_with_retry('x',check,lambda _:None)
        self.assertEqual(len(attempts),3)


if __name__=='__main__':unittest.main(verbosity=2)
