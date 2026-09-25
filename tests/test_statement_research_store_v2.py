from pathlib import Path
from copy import deepcopy
import io, sys, unittest
sys.path[:0] = [str(Path(__file__).resolve().parents[1] / path) for path in ('aws/shared','aws/ops/checks')]
import statement_research_store_v2 as store
import statement_research_source as source
import statement_research_v2 as model
import statement_research_arithmetic_v2 as independent
import statement_measurements as measurements
from test_statement_research_v2 import Fixture


class Conflict(Exception):
    response={'Error':{'Code':'PreconditionFailed'}}


class S3:
    def __init__(self, files): self.files=dict(files);self.writes=[]
    def get_object(self, Bucket, Key): return {'Body':io.BytesIO(self.files[Key])}
    def put_object(self, **kwargs):
        assert kwargs['IfNoneMatch']=='*'
        if kwargs['Key'] in self.files: raise Conflict()
        self.files[kwargs['Key']]=kwargs['Body'];self.writes.append(kwargs['Key'])


class Tests(unittest.TestCase):
    def test_parallel_prefetch_preserves_bounds_and_checks_deadline_on_cached_reads(self):
        import threading,time
        files={source.PRIVATE+source.sha(str(i).encode())+'.bin':str(i).encode() for i in range(24)}
        client=S3(files); original=client.get_object; lock=threading.Lock()
        active=maximum=0;seen=[]
        def get(**kwargs):
            nonlocal active,maximum
            with lock:active+=1;maximum=max(active,maximum);seen.append(kwargs['Key'])
            try:time.sleep(.002);return original(**kwargs)
            finally:
                with lock:active-=1
        client.get_object=get
        expired=False
        def deadline():
            if expired:raise TimeoutError('Deadline passed')
        read=store.reader(client,'test',deadline);read.prefetch(files)
        self.assertGreater(maximum,1);self.assertLessEqual(maximum,8)
        self.assertEqual(len(seen),len(files))
        for key,body in files.items():self.assertEqual(read(key),body)
        self.assertEqual(len(seen),len(files))
        with self.assertRaises(ValueError):read.prefetch(['data/portfolio.json'])
        expired=True
        with self.assertRaises(TimeoutError):read(next(iter(files)))

    def test_whole_original_replay_and_independent_fraction_check(self):
        fixture=Fixture(('ABC','XYZ'));compiled=fixture.compile();s3=S3(fixture.files)
        ref=store.retain(s3,'test',fixture.ref,fixture.identity_ref,compiled)
        self.assertEqual(store.replay(ref,store.reader(s3,'test')),compiled)
        self.assertEqual(independent.verify(fixture.ref,fixture.identity_ref,compiled,s3.files.__getitem__)['metric_comparisons'],68)
        self.assertTrue(all(key.startswith(model.PREFIX) and 'current' not in key for key in s3.writes))
        self.assertEqual(len(s3.writes),13)
        before=len(s3.writes);self.assertEqual(store.retain(s3,'test',fixture.ref,fixture.identity_ref,compiled),ref)
        self.assertEqual(len(s3.writes),before)

    def test_independent_check_finds_mutated_results_sources_and_missing_rows(self):
        fixture=Fixture();original=fixture.compile()
        mutations=(lambda c:c['shards']['ABC']['records'][0]['measurements']['metrics']['gross_margin_pct'].update(value='41.000000000000'),
            lambda c:c['shards']['ABC']['records'][0]['source_rows'].pop(),
            lambda c:c['shards']['ABC']['records'][0]['measurements']['metrics']['gross_margin_pct']['inputs'][0].update(reported_value='41'),
            lambda c:c['shards']['ABC']['records'].pop(),
            lambda c:c['shards']['ABC']['records'][0]['measurements'].update(calls_eligible=True))
        for mutation in mutations:
            candidate=deepcopy(original);mutation(candidate)
            with self.assertRaises((AssertionError,KeyError)):independent.verify(fixture.ref,fixture.identity_ref,candidate,fixture.files.__getitem__)

    def test_fraction_check_covers_missing_fields_duplicate_endpoints_and_invalid_rows(self):
        def modify(symbol, period, endpoint, rows):
            if period=='quarter' and endpoint==measurements.C:return []
            if period=='annual' and endpoint==measurements.B:return rows+deepcopy(rows)
            if period=='quarter' and endpoint==measurements.B:rows[0]['cik']='0'
            if endpoint==measurements.I:rows[0]['revenue']=0
            return rows
        fixture=Fixture(modify=modify);compiled=fixture.compile()
        result=independent.verify(fixture.ref,fixture.identity_ref,compiled,fixture.files.__getitem__)
        self.assertEqual(result['original_rows_checked'],6)
        self.assertEqual(result['invalid_or_uncorroborated_identity_rows_checked'],1)
        self.assertGreater(result['unavailable_metrics_checked'],10)

    def test_corrupt_record_or_source_cannot_replay(self):
        fixture=Fixture();compiled=fixture.compile();s3=S3(fixture.files)
        ref=store.retain(s3,'test',fixture.ref,fixture.identity_ref,compiled)
        key=compiled['packet']['issuers'][0]['record']['key'];s3.files[key]+=b' '
        with self.assertRaises(ValueError):store.replay(ref,store.reader(s3,'test'))
        s3.files[key]=source.encoded(compiled['shards']['ABC'])
        source_key=next(iter(fixture.capsules.values()))['original']['key'];s3.files[source_key]+=b' '
        with self.assertRaises(ValueError):store.replay(ref,store.reader(s3,'test'))

    def test_compiler_identity_is_part_of_the_recorded_run(self):
        fixture=Fixture();s3=S3(fixture.files);ref=store.retain(s3,'test',fixture.ref,fixture.identity_ref,fixture.compile())
        run=source.strict(s3.files[ref['manifest_key']]);compiler=next(iter(run['compilers'].values()))
        s3.files[compiler['key']]+=b'\n'
        with self.assertRaises(ValueError):store.verified_run(ref,store.reader(s3,'test'))

    def test_wrong_issuer_index_fails_before_any_write(self):
        fixture=Fixture();s3=S3(fixture.files);compiled=fixture.compile()
        compiled['packet']['issuers'][0]['record']['sha256']='0'*64
        with self.assertRaises(ValueError):store.retain(s3,'test',fixture.ref,fixture.identity_ref,compiled)
        self.assertEqual(s3.writes,[])

    def test_store_cannot_read_or_write_current_account_or_request_paths(self):
        fixture=Fixture();s3=S3(fixture.files);reader=store.reader(s3,'test')
        for key in ('data/forensic-screen.json','data/portfolio.json',source.PRIVATE+'requests/request.json',model.PREFIX+'runs/../current.json'):
            with self.assertRaises(ValueError):reader(key)
            ref={'key':key,'sha256':source.sha(b'{}'),'bytes':2}
            with self.assertRaises(ValueError):store.put_immutable(s3,'test',ref,b'{}')
        self.assertEqual(s3.writes,[])

    def test_negative_fraction_half_even_rounding(self):
        from fractions import Fraction
        self.assertEqual(independent.rounded(Fraction(-1,2*10**12)),'0.000000000000')
        self.assertEqual(independent.rounded(Fraction(-3,2*10**12)),'-0.000000000002')
        self.assertEqual(independent.rounded(Fraction(-2,3)),'-0.666666666667')


if __name__=='__main__':unittest.main(verbosity=2)
