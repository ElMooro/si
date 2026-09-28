"""Original starvation and resumed native writer; isolated synthetic I/O only."""
from pathlib import Path
from copy import deepcopy
import ast,hashlib,importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('revenue_resume_native',ROOT/'aws/lambdas/justhodl-revenue-acceleration/tests/run_tests.py')
n=importlib.util.module_from_spec(spec);spec.loader.exec_module(n)
from revenue_observations import occurrence_keys,acquisition_plan,acquisition_progress,CONTRACT
SKIP='not_attempted_runtime_rate_or_size_limit'


def selected(names):return [{**n.member(name),'source_index':i} for i,name in enumerate(names)]
def prior(members,visited,plan=None,reason='source_byte_budget'):
    plan=plan or acquisition_plan(members,None)
    records=[{'request_index':i,'universe_member':m,'acquisitions':[{'endpoint':'income-statement','status':'received' if i in visited else SKIP}]} for i,m in enumerate(members)]
    return {'measurement_contract':CONTRACT,'universe_membership':{'selected':members},'request_records':records,
            'acquisition_progress':acquisition_progress(plan,visited,reason,1024)}


class Tests(unittest.TestCase):
    def test_complete_deployed_original_and_repeated_first_window_starvation(self):
        raw=(ROOT/'tests/fixtures/pre-revenue-resume-lambda_function.py.txt').read_bytes()
        self.assertEqual(len(raw),27571);self.assertEqual(hashlib.sha256(raw).hexdigest(),'14888da2ce95862fa617ba9b656d5b141b0e8afb28b18852710c979e259e3c03')
        h=next(x for x in ast.parse(raw).body if isinstance(x,ast.FunctionDef) and x.name=='lambda_handler')
        block=next(x for x in h.body if isinstance(x,ast.With))
        results=[]
        for _ in range(2):
            members=selected([f'T{i}' for i in range(12)]);seen=[]
            def company(m):seen.append(m['source_index']);return [{'original_bytes':2*1024*1024},{'original_bytes':1024*1024}]
            ns={'selected':members,'workers':6,'total_bytes':0,'remaining':lambda:200,'ThreadPoolExecutor':n.ThreadPoolExecutor,
                '_revenue_company':company,'captures':[[{'status':SKIP}] for _ in members]}
            exec(compile(ast.Module(body=[block],type_ignores=[]),'<original acquisition loop>','exec'),ns)
            results.append(sorted(seen));self.assertEqual(ns['captures'][6][0]['status'],SKIP)
        self.assertEqual(results,[[0,1,2,3,4,5],[0,1,2,3,4,5]])

    def test_bootstrap_from_original_unattempted_population_preserves_duplicates(self):
        members=selected(['A','A',None,'B']);p=prior(members,[0,1]);p.pop('acquisition_progress')
        self.assertEqual(occurrence_keys(members),['A#0','A#1','!invalid#0','B#0'])
        self.assertEqual(acquisition_plan(members,p)['planned_request_indices'],[2,3])
        self.assertEqual(acquisition_plan(members,p)['plan_reason'],'resume_prior_unattempted_occurrences')

    def test_progress_rotates_windows_then_restarts_only_after_cycle(self):
        members=selected(['A','B','C','D','E'])
        p=prior(members,[0,1]);plan=acquisition_plan(members,p)
        self.assertEqual(plan['planned_request_indices'],[2,3,4])
        p=prior(members,[2,3],plan);plan=acquisition_plan(members,p)
        self.assertEqual(plan['planned_request_indices'],[4])
        p=prior(members,[4],plan,'planned_window_complete')
        self.assertTrue(p['acquisition_progress']['cycle_complete'])
        self.assertEqual(acquisition_plan(members,p)['planned_request_indices'],[0,1,2,3,4])

    def test_changed_order_new_and_retired_occurrences_keep_remaining_order(self):
        old=selected(['A','A','B','C']);p=prior(old,[0])
        current=selected(['C','A','NEW','A'])
        current[1]['raw']['market_cap']=2e9
        plan=acquisition_plan(current,p)
        self.assertEqual(plan['planned_occurrence_keys'],['A#1','C#0','NEW#0'])
        self.assertEqual(plan['planned_request_indices'],[3,0,2])
        self.assertEqual(acquisition_plan(selected(['A']),p)['planned_request_indices'],[0])

    def test_corrupt_prior_progress_cannot_skip_or_relabel_occurrences(self):
        members=selected(['A','B','C']);base=prior(members,[0])
        edits=[lambda p:p.update(universe_membership=[]),lambda p:p['request_records'].pop(),
            lambda p:p['request_records'][0].update(request_index=True),lambda p:p['request_records'][1].update(acquisitions=[None]),
            lambda p:p['acquisition_progress'].update(remaining_occurrence_keys=['C#0']),
            lambda p:p['acquisition_progress'].update(visited_request_indices=[True]),
            lambda p:p['acquisition_progress'].update(visited_request_indices=[1]),
            lambda p:p['acquisition_progress'].update(planned_request_indices=[False,1,2]),
            lambda p:p['acquisition_progress'].update(pending_occurrences=0),
            lambda p:p['acquisition_progress'].update(planned_occurrence_keys=['A#0','A#0','C#0']),
            lambda p:p['request_records'][1]['acquisitions'][0].update(status='received')]
        for edit in edits:
            p=deepcopy(base);edit(p)
            with self.assertRaises(ValueError):acquisition_plan(members,p)
        for bad in ([True],[1],[0,0],[0,1,2,3]):
            with self.assertRaises(ValueError):acquisition_progress(acquisition_plan(members,None),bad,'source_byte_budget',1)
        with self.assertRaises(ValueError):acquisition_progress(acquisition_plan(members,None),[0],'planned_window_complete',1)

    def test_budget_rate_and_runtime_reasons_preserve_exact_remaining_partition(self):
        members=selected(['A','B','C']);plan=acquisition_plan(members,None)
        for reason in ('source_byte_budget','runtime_reserve','provider_rate_limit'):
            p=acquisition_progress(plan,[0],reason,128)
            self.assertEqual(p['remaining_occurrence_keys'],['B#0','C#0']);self.assertFalse(p['cycle_complete'])
            self.assertTrue(p['visited_is_not_successful_provider_response']);self.assertFalse(p['schedule_accelerated'])

    def test_real_writer_three_runs_resume_without_new_reads_and_keep_complete_history(self):
        memory=n.Memory();memory.data['data/universe.json']=json.dumps({'stocks':[n.member(f'T{i}')['raw'] for i in range(5)]}).encode()
        class Clock(n.datetime):
            tick=n.datetime(2026,9,28,8,tzinfo=n.timezone.utc)
            @classmethod
            def now(cls,tz=None):cls.tick+=n.timedelta(seconds=1);return cls.tick
        ns=n.native(memory,MAX_TICKERS=5,N_WORKERS=1,datetime=Clock);seen=[];all_seen=[];raws=[]
        def company(m):
            seen.append(m['ticker'])
            if len(seen)==2:return [{'endpoint':'income-statement','status':'rate_limited'}]
            captures=n.company(m)
            for a in captures:a['received_at']=Clock.now().isoformat()
            return captures
        ns['_revenue_company']=company
        for expected in ([0,1],[2,3],[4]):
            seen.clear();ns['lambda_handler']();raw=memory.data['data/revenue-acceleration.json'];raws.append(raw);p=json.loads(raw)
            self.assertEqual(p['acquisition_progress']['visited_request_indices'],expected)
            self.assertEqual([r['ticker'] for r in p['request_records']],[f'T{i}' for i in range(5)])
            self.assertEqual(p['acquisition_progress']['pending_occurrences'],5-expected[-1]-1)
            all_seen+=list(seen)
        self.assertEqual(all_seen,['T0','T1','T2','T3','T4'])
        for raw in raws:self.assertEqual(memory.data['data/revenue-acceleration/history/'+hashlib.sha256(raw).hexdigest()+'.json'],raw)
        self.assertTrue(all(k in ('data/revenue-acceleration.json','data/universe.json') or k.startswith('data/revenue-acceleration/history/') for k in memory.reads))
        plan=acquisition_plan(p['universe_membership']['selected'],p);self.assertEqual(plan['planned_request_indices'],[0,1,2,3,4])


if __name__=='__main__':unittest.main(verbosity=2)
