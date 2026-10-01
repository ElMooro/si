"""Complete invented resident generations and whole native search behaviour."""
from datetime import datetime,timedelta,timezone
from pathlib import Path
from unittest.mock import patch
import ast,json,pickle,sys,tempfile,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-symdir/source'),str(ROOT/'aws/shared')]
import test_directory_native as native
import test_directory_index as fixture
import directory_index as index
import directory_resident as resident
fixtures=fixture


class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.cache={};self.state={};self.seconds=0;self.wall=0
        self.store=fixture.Store(fixture.population('FIRST','2000-01-01T00:00:00+00:00'))
    def iso(self):return (datetime(2000,1,3,tzinfo=timezone.utc)+timedelta(seconds=self.wall)).isoformat()
    def mono(self):return self.seconds
    def advance(self,seconds):self.seconds+=seconds;self.wall+=seconds
    def load(self,**kw):
        return resident.load(self.cache,self.state,
            lambda:index.refresh(self.cache,self.store,'invented-bucket',fixture.PREFIX,fixture.derived,self.iso,temp_root=self.tmp.name),
            lambda:index.manifest(self.store,'invented-bucket',fixture.PREFIX),
            lambda head,cache:index.refresh_needed(head,cache,fixture.PREFIX),self.iso,self.mono,**kw)
    def evidence(self):return resident.evidence(self.cache,self.state,self.iso,self.mono)['head_check']
    def install_next(self,stamp='2000-01-02T00:00:00+00:00'):
        self.store=fixture.Store(fixture.population('SECOND',stamp))
    def fail(self):
        self.store.get_object=lambda **kw:(_ for _ in ()).throw(OSError('Invented unavailable manifest'))

    def test_new_generation_is_discovered_by_this_process_at_interval_boundary(self):
        self.load();self.install_next();before=pickle.dumps(self.cache)
        self.advance(299);self.load();self.assertEqual(pickle.dumps(self.cache),before);self.assertEqual(self.store.calls,[])
        self.advance(1);self.load();self.assertEqual(self.cache['ids'],[('SECOND',0)])
        self.assertEqual(self.evidence()['status'],'checked_within_interval')
    def test_same_clock_changed_generation_does_not_require_a_warm_event(self):
        self.load();self.install_next('2000-01-01T00:00:00+00:00');self.advance(300);self.load()
        self.assertEqual(self.cache['ids'],[('SECOND',0)])
    def test_outage_preserves_whole_cache_and_retries_after_bounded_delay(self):
        self.load();before=pickle.dumps(self.cache);self.install_next();original=self.store.get_object;calls=[]
        def fail(**kw):calls.append(kw);raise OSError('Invented head outage')
        self.store.get_object=fail;self.advance(300);self.load()
        self.assertEqual(pickle.dumps(self.cache),before);self.assertEqual(self.evidence()['status'],'unavailable')
        self.assertEqual(self.evidence()['http_max_age_s'],0);self.assertEqual(len(calls),1)
        self.advance(29);self.load();self.assertEqual(len(calls),1)
        self.store.get_object=original;self.advance(1);self.load();self.assertEqual(self.cache['ids'],[('SECOND',0)])
    def test_cold_outage_and_explicit_checks_raise_and_never_erase_cache(self):
        self.fail()
        with self.assertRaises(OSError):self.load()
        self.install_next();self.load();before=pickle.dumps(self.cache);self.fail()
        for mode in ({'force':True},{'check':True}):
            with self.assertRaises(OSError):self.load(**mode)
            self.assertEqual(pickle.dumps(self.cache),before)
        self.assertEqual(self.evidence()['status'],'unavailable')
    def test_explicit_warm_check_checks_unchanged_head_without_artifact_download(self):
        self.load();self.store.calls.clear();self.advance(1);self.load(check=True)
        self.assertEqual(self.store.calls,[fixture.PREFIX+'manifest.json']);self.assertFalse(self.state['reloaded'])
    def test_wall_clock_advancing_without_monotonic_expires_check(self):
        self.load();self.install_next();self.wall+=300;self.load();self.assertEqual(self.cache['ids'],[('SECOND',0)])
    def test_monotonic_advancing_without_wall_clock_expires_check(self):
        self.load();self.install_next();self.seconds+=300;self.load();self.assertEqual(self.cache['ids'],[('SECOND',0)])
    def test_clock_regression_does_not_extend_cached_success(self):
        self.load();self.store.calls.clear();self.seconds-=1;self.load();self.assertEqual(self.store.calls,[fixture.PREFIX+'manifest.json'])
        self.store.calls.clear();self.wall-=1;self.load();self.assertEqual(self.store.calls,[fixture.PREFIX+'manifest.json'])
    def test_future_and_regressing_heads_cannot_displace_the_verified_population(self):
        self.load();before=pickle.dumps(self.cache)
        for stamp in ('2000-01-04T00:00:00+00:00','1999-12-31T00:00:00+00:00'):
            self.install_next(stamp);self.advance(300);self.load()
            self.assertEqual(pickle.dumps(self.cache),before);self.assertEqual(self.evidence()['status'],'unavailable')
    def test_http_cache_cannot_extend_last_successful_check_window(self):
        self.load();self.assertEqual(self.evidence()['http_max_age_s'],120)
        self.advance(299);self.assertEqual(self.evidence()['http_max_age_s'],1)
        self.advance(1);self.assertEqual(self.evidence()['http_max_age_s'],0)
        self.assertFalse(self.evidence()['source_freshness_verified'])
    def test_long_download_does_not_claim_head_was_checked_at_finish(self):
        old=self.store.get_object
        def slow(**kw):
            result=old(**kw)
            if kw['Key'].endswith('docs.pkl.gz'):self.advance(301)
            return result
        self.store.get_object=slow;self.load()
        self.assertEqual(self.evidence()['status'],'check_due');self.assertEqual(self.evidence()['http_max_age_s'],0)
    def test_nonfinite_clock_cannot_authorize_a_resident_window(self):
        for bad in (float('nan'),float('inf'),True):
            self.seconds=bad
            with self.assertRaises(index.IndexIntegrityError):self.load()
            self.assertEqual(self.store.calls,[])
    def test_corrupt_new_artifact_preserves_the_entire_working_generation(self):
        self.load();before=pickle.dumps(self.cache);self.install_next()
        key=self.store.descriptor['files']['index']['key'];self.store.objects[key]=self.store.objects[key][:-4]
        self.advance(300);self.load()
        self.assertEqual(pickle.dumps(self.cache),before)
        self.assertEqual(self.evidence()['status'],'unavailable')
        self.assertEqual(self.evidence()['http_max_age_s'],0)
    def test_failure_details_are_typed_without_echoing_transport_messages(self):
        self.load();self.advance(300)
        self.store.get_object=lambda **kw:(_ for _ in ()).throw(OSError('invented-secret-token'))
        self.load();value=self.evidence()
        self.assertEqual(value['error_type'],'OSError');self.assertNotIn('invented-secret-token',str(value))
    def test_unrecoverable_refresh_without_a_population_cannot_return_a_cache_success(self):
        self.load();self.advance(300);self.install_next()
        def lost():self.cache.clear();raise OSError('Invented unrecoverable restoration failure')
        with self.assertRaises(OSError):
            resident.load(self.cache,self.state,lost,
                lambda:index.manifest(self.store,'invented-bucket',fixture.PREFIX),
                lambda head,cache:index.refresh_needed(head,cache,fixture.PREFIX),self.iso,self.mono)
        self.assertIsNone(self.cache.get('docs'));self.assertEqual(self.evidence()['http_max_age_s'],0)


class NativeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.module,*_=native.build();self.seconds=0
        self.module._iso=lambda:(datetime(2000,1,3,tzinfo=timezone.utc)+timedelta(seconds=self.seconds)).isoformat()
        self.module.refresh_index=lambda *args:index.refresh(*args,temp_root=self.tmp.name)
        self.module.s3=fixtures.Store(fixtures.population('FIRST'))
        self.timer=patch('time.monotonic',side_effect=lambda:self.seconds);self.timer.start();self.addCleanup(self.timer.stop)
    def search(self,name):
        result=self.module.lambda_handler({'mode':'search','q':name},None)
        self.assertEqual(result['statusCode'],200,result)
        return result,json.loads(result['body'])
    def test_full_native_search_notices_new_population_without_any_warm_invocation(self):
        _,initial=self.search('FIRST');self.assertTrue(any(row['id']=='FIRST' for row in initial['rows']))
        self.module.s3=fixtures.Store(fixtures.population('SECOND','2000-01-02T00:00:00+00:00'))
        self.seconds=299;_,before=self.search('SECOND');self.assertFalse(any(row['id']=='SECOND' for row in before['rows']))
        self.seconds=300;_,after=self.search('SECOND');self.assertTrue(any(row['id']=='SECOND' for row in after['rows']))
        self.assertEqual(after['index_integrity']['head_check']['status'],'checked_within_interval')
        self.assertFalse(after['index_integrity']['investment_authority'])
    def test_native_outage_keeps_searchable_rows_with_explicit_unavailable_check_and_zero_ttl(self):
        self.search('FIRST');before=pickle.dumps(self.module._IDX)
        self.module.s3.get_object=lambda **kw:(_ for _ in ()).throw(OSError('Invented outage'))
        self.seconds=300;response,body=self.search('FIRST')
        self.assertTrue(any(row['id']=='FIRST' for row in body['rows']))
        self.assertEqual(pickle.dumps(self.module._IDX),before)
        self.assertEqual(body['index_integrity']['head_check']['status'],'unavailable')
        self.assertIn('max-age=0',response['headers']['Cache-Control'])
        with self.assertRaises(OSError):self.module.lambda_handler({'mode':'warm'},None)
    def test_native_warm_uses_same_generation_check_and_preserves_same_clock_replacement(self):
        self.search('FIRST');self.module.s3=fixtures.Store(fixtures.population('SECOND'))
        result=self.module.lambda_handler({'mode':'warm'},None)
        self.assertTrue(result['reloaded']);self.assertEqual(self.module._IDX['ids'],[('SECOND',0)])
        again=self.module.lambda_handler({'mode':'warm'},None);self.assertFalse(again['reloaded'])


class Preservation(unittest.TestCase):
    def test_whole_predecessor_preserves_build_sources_series_and_nonsearch_handler_branches(self):
        old=ast.parse((ROOT/'tests/fixtures/symbol-directory/pre-resident-lambda.py.txt').read_bytes())
        new=ast.parse((ROOT/'aws/lambdas/justhodl-symdir/source/lambda_function.py').read_bytes())
        def functions(tree):return {n.name:n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
        def dump(node):return ast.dump(node,include_attributes=False)
        before,after=functions(old),functions(new)
        self.assertEqual(set(after)-set(before),{'index_cache_evidence'})
        self.assertEqual({key for key in before if dump(before[key])!=dump(after[key])},{'load_index','lambda_handler'})
        self.assertEqual(dump(before['lambda_handler'].args),dump(after['lambda_handler'].args))
        def rest(fn):
            return [dump(n) for n in fn.body if not (isinstance(n,ast.If) and
                ast.unparse(n.test) in ("mode == 'warm' or path == '/warm'","mode == 'search' or path == '/search'"))]
        self.assertEqual(rest(before['lambda_handler']),rest(after['lambda_handler']))
        def assignments(tree):return {ast.unparse(n.targets[0]):dump(n) for n in tree.body if isinstance(n,ast.Assign)}
        earlier,later=assignments(old),assignments(new)
        self.assertEqual(set(later)-set(earlier),{'_ICHECK'})
        for key,value in earlier.items():self.assertEqual(value,later[key],key)


if __name__=='__main__':
    with patch('urllib.request.urlopen',side_effect=AssertionError('Real HTTP forbidden')):unittest.main(verbosity=2)
