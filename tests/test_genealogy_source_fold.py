from pathlib import Path
from collections import defaultdict
import random,sys,unittest,tracemalloc,tempfile,json
from datetime import timedelta
R=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(R/'tests'),str(R/'aws/shared'),str(R/'aws/ops/checks')]
import test_genealogy_capture_timing as fixtures
import genealogy_capture_timing as original
from genealogy_source_fold import fold_source
from genealogy_spooled_timing import compile_spooled


def projections(contexts):
    sources=defaultdict(list)
    for context in contexts:
        for s in context['sources']:
            sources[s['source_key']].append({'members':{(x['instrument_id'],x['direction']) for x in s['explicit_members']},
               'eligible':s['eligible'],'absence_eligible':s['unsupported_identity_count']==0,
               'receipt':{'capture_key':context['capture_key'],'capture_sha256':context['capture_sha256'],
                    'capture_source_index':s['capture_source_index'],'source_bytes_sha256':s['source_bytes_sha256'],
                    'source_generated_at':s['source_generated_at'],'source_received_at':s['source_received_at']}})
    return sources


class Folding(unittest.TestCase):
    def test_disk_spool_reproduces_the_entire_canonical_output(self):
        for seed in range(12):
            random.seed(seed);contexts=[]
            for n in range(45):
                sources=[fixtures.snapshot(name,n//2,tuple(k for k in ('AAA','BBB','CCC') if random.random()<.4),
                         random.random()>.1,random.choice((0,0,1))) for name in ('a','b','c')]
                contexts.append(fixtures.capture(n+1,*sources))
            expected=original.canonical(fixtures.compile(*contexts))
            with tempfile.TemporaryDirectory() as directory:
                path=Path(directory);report=compile_spooled(iter(contexts),'2026-09-20T00:00:00Z',path/'spool.sqlite',path/'output.json')
                self.assertEqual((path/'output.json').read_bytes(),expected,seed)
                self.assertEqual(report['output_bytes'],len(expected))
                with self.assertRaisesRegex(ValueError,'fresh_computation_paths'):
                    compile_spooled(iter(contexts),'2026-09-20T00:00:00Z',path/'spool.sqlite',path/'output.json')

    def test_disk_spool_keeps_all_4950_comparisons(self):
        contexts=[fixtures.capture(1,*[fixtures.snapshot(f'source-{n:03d}',n) for n in range(100)])]
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory);report=compile_spooled(iter(contexts),'2026-09-20T00:00:00Z',path/'db',path/'out')
            self.assertEqual(report['possible_comparisons'],4950)
            self.assertEqual((path/'out').read_bytes(),original.canonical(fixtures.compile(*contexts)))

    def test_disk_spool_rejects_duplicate_or_reordered_captures(self):
        a=fixtures.capture(1,fixtures.snapshot('a',1));b=fixtures.capture(2,fixtures.snapshot('a',2))
        for contexts in ([a,a],[b,a]):
            with tempfile.TemporaryDirectory() as directory:
                path=Path(directory)
                with self.assertRaisesRegex(ValueError,'capture_context_order'):
                    compile_spooled(iter(contexts),'2026-09-20T00:00:00Z',path/'db',path/'out')
                self.assertFalse((path/'out').exists())

    def test_seeded_whole_timeline_matches_all_intervals_first_observations_and_ambiguities(self):
        for seed in range(60):
            random.seed(seed);contexts=[]
            for n in range(100):
                minute=n//2 if seed%2 else n
                sources=[]
                for name in ('a','b','c'):
                    if random.random()<.08:continue
                    names=tuple(k for k in ('AAA','BBB','CCC') if random.random()<.4)
                    sources.append(fixtures.snapshot(name,minute,names,random.random()>.12,random.choice((0,0,1))))
                contexts.append(fixtures.capture(n+1,*sources))
            baseline=fixtures.compile(*contexts);intervals=[];ambiguities=[];first=[];counts=defaultdict(int)
            for name,rows in projections(contexts).items():
                result=fold_source(name,iter(rows),intervals.append,ambiguities.append)
                first+=result['first_observations']
                for k,v in result['counts'].items():counts[k]+=v
            key=lambda r:(r['instrument_id'],r['direction'],r['source_key'],r['upper_inclusive_utc'])
            self.assertEqual(sorted(intervals,key=key),baseline['intervals'],seed)
            self.assertEqual(sorted(first,key=key),baseline['first_observations'],seed)
            self.assertEqual(sorted(ambiguities,key=lambda r:(r['source_key'],r['source_received_at'])),baseline['same_timestamp_ambiguities'],seed)
            for k in ('valid_source_snapshots','ineligible_source_snapshots','identity_partial_source_snapshots','same_timestamp_duplicate_snapshots'):
                self.assertEqual(counts[k],baseline[k],(seed,k))

    def test_consumes_ordered_input_once_and_emits_before_the_end(self):
        contexts=[fixtures.capture(n+1,fixtures.snapshot('a',n,() if n%2==0 else ('AAA',))) for n in range(100)]
        rows=projections(contexts)['data/a.json'];consumed=[];emitted=[]
        def once():
            for i,row in enumerate(rows):consumed.append(i);yield row
        def emit(interval):emitted.append((len(consumed),interval))
        result=fold_source('data/a.json',once(),emit,lambda x:None)
        self.assertEqual(len(emitted),50);self.assertLess(emitted[0][0],5)
        self.assertEqual(len(consumed),100);self.assertEqual(len(result['first_observations']),1)
        self.assertEqual(result['counts']['total_membership_intervals'],50)

    def test_late_receipt_requires_rebuild_instead_of_incorrect_append(self):
        contexts=[fixtures.capture(1,fixtures.snapshot('a',2)),fixtures.capture(2,fixtures.snapshot('a',1))]
        rows=projections(contexts)['data/a.json']
        with self.assertRaisesRegex(ValueError,'requires_rebuild'):
            fold_source('data/a.json',rows,lambda x:None,lambda x:None)

    def test_long_single_source_history_has_bounded_working_memory(self):
        emitted=0
        def timeline():
            for i in range(100000):
                at=(fixtures.BASE+timedelta(microseconds=i)).isoformat()
                yield {'members':{('equity:US:AAA','UP')} if i%2 else set(),'eligible':True,'absence_eligible':True,
                    'receipt':{'capture_key':f'data/research-forecasts/captures/{i:064x}.json','capture_sha256':f'{i:064x}',
                         'capture_source_index':0,'source_bytes_sha256':'b'*64,'source_generated_at':at,'source_received_at':at}}
        def emit(row):
            nonlocal emitted
            emitted+=1
        tracemalloc.start()
        try:
            result=fold_source('data/a.json',timeline(),emit,lambda x:None)
            peak=tracemalloc.get_traced_memory()[1]
        finally:tracemalloc.stop()
        self.assertEqual(emitted,50000)
        self.assertEqual(len(result['first_observations']),1)
        self.assertLess(peak,4*1024*1024)


if __name__=='__main__':unittest.main()
