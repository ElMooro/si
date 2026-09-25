from pathlib import Path
from copy import deepcopy
from io import BytesIO
from unittest.mock import Mock
import sys, threading, time, unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import capital_structure_source as source
import capital_structure_store as store
from test_capital_structure_research import fixture, compile_fixture


class Conflict(Exception): response={'Error':{'Code':'PreconditionFailed'}}
class S3:
    def __init__(self, files): self.files=dict(files);self.writes=[]
    def get_object(self, Bucket, Key): return {'Body':BytesIO(self.files[Key])}
    def put_object(self, **args):
        assert args['IfNoneMatch']=='*'
        if args['Key'] in self.files: raise Conflict()
        self.files[args['Key']]=args['Body'];self.writes.append(args['Key'])


class Tests(unittest.TestCase):
    def test_all_originals_compilers_and_issuer_histories_replay_without_acquisition(self):
        f=fixture();compiled=compile_fixture(f);client=S3(f['files'])
        ref=store.retain(client,'test',f['manifest'],f['identity'],compiled)
        self.assertEqual(store.replay(ref,store.reader(client,'test')),compiled)
        self.assertEqual(len(client.writes),12)
        self.assertTrue(all(key.startswith(source.PREFIX) for key in client.writes))
        count=len(client.writes)
        self.assertEqual(store.retain(client,'test',f['manifest'],f['identity'],compiled),ref)
        self.assertEqual(len(client.writes),count)

    def test_tampered_compiler_output_history_source_and_sec_index_are_detected(self):
        for kind in ('compiler','output','history','source','sec'):
            f=fixture();compiled=compile_fixture(f);client=S3(f['files'])
            ref=store.retain(client,'test',f['manifest'],f['identity'],compiled)
            run=source.strict(client.files[ref['manifest_key']])
            key = (next(iter(run['compilers'].values()))['key'] if kind=='compiler' else
                   run['output']['key'] if kind=='output' else
                   compiled['packet']['issuers'][0]['record']['key'] if kind=='history' else
                   next(iter(f['originals'].values()))['key'] if kind=='source' else
                   compiled['packet']['identity_index']['original']['key'])
            client.files[key]+=b' '
            with self.assertRaises(ValueError): store.replay(ref,store.reader(client,'test'))

    def test_validation_before_writing_and_closed_current_account_request_boundaries(self):
        f=fixture();client=S3(f['files']);compiled=compile_fixture(f)
        compiled['shards']['ABC']['records'].pop()
        with self.assertRaises(ValueError):store.retain(client,'test',f['manifest'],f['identity'],compiled)
        self.assertEqual(client.writes,[])
        read=store.reader(client,'test')
        for key in ('data/share-flows.json','data/portfolio.json','data/trade-tickets.json',
                    source.PRIVATE+'requests/'+'f'*64+'.json',source.PREFIX+'runs/../current.json'):
            with self.assertRaises(ValueError):read(key)
            with self.assertRaises(ValueError):store.put_immutable(client,'test',{'key':key,'sha256':source.sha(b'{}'),'bytes':2},b'{}')
        self.assertEqual(client.writes,[])

    def test_prefetch_is_bounded_parallel_and_deadlines_also_guard_cached_reads(self):
        files={source.PRIVATE+source.sha(str(i).encode())+'.bin':str(i).encode() for i in range(40)}
        client=S3(files);original=client.get_object;lock=threading.Lock();active=maximum=0;seen=[]
        def get(**args):
            nonlocal active,maximum
            with lock:active+=1;maximum=max(active,maximum);seen.append(args['Key'])
            try:time.sleep(.002);return original(**args)
            finally:
                with lock:active-=1
        client.get_object=get;expired=False
        def deadline():
            if expired:raise TimeoutError('Expired')
        read=store.reader(client,'test',deadline);read.prefetch(files)
        self.assertGreater(maximum,1);self.assertLessEqual(maximum,8)
        for key,body in files.items():self.assertEqual(read(key),body)
        self.assertEqual(len(seen),len(files))
        with self.assertRaises(ValueError):read.prefetch(['data/private-account.json'])
        expired=True
        with self.assertRaises(TimeoutError):read(next(iter(files)))


if __name__=='__main__':unittest.main(verbosity=2)
