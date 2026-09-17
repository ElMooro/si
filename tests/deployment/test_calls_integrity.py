"""Calls consumer regressions, including real handler paths without AWS/network."""
import ast
import copy
import io
import json
import runpy
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws/shared'))
from calls_contract import (append_snapshot, candidate_verb, decision_eligibility,
                            finite, khalid_value, latest_snapshot, make_snapshot, timestamp)
REPLAY = runpy.run_path(str(ROOT / 'aws/lambdas/justhodl-calls-backtest/source/calls_replay.py'))
NOW = datetime(2026, 9, 20, 21, tzinfo=timezone.utc)


def extract(engine, names, scope=None):
    source = ROOT / f'aws/lambdas/{engine}/source/lambda_function.py'
    tree = ast.parse(source.read_text(encoding='utf-8'))
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(nodes) == len(names)
    env = dict(json=json, datetime=datetime, timezone=timezone, time=time,
               khalid_value=khalid_value, candidate_verb=candidate_verb, make_snapshot=make_snapshot,
               append_snapshot=append_snapshot, decision_eligibility=decision_eligibility,
               latest_snapshot=latest_snapshot, finite=finite, timestamp=timestamp)
    env.update(scope or {})
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), 'exec'), env)
    return env


def qualified(**changes):
    row = dict(schema_version='calls.v2', snapshot_id='fixture', timestamp='2026-09-17T12:00:00Z',
               expires_at='2026-09-21T12:00:00Z', decision_status='VALID', decision_eligible=True,
               sizing_eligible=True, validation_status='validated', model_version='fixture-only',
               call_verb='LONG', evidence_ids=['root:fixture'], asset='SPY', target_exposure=1)
    row.update(changes)
    return row


def observation_output(md=None, error=None):
    return make_snapshot({'as_of': '2026-09-17T20:00:00Z', 'intelligence': {'scores': {'khalid_score': 0}}},
                         {'brief_md': md if md is not None else 'x'*54, 'error': error, 'duration_s': 1})


class StorageError(Exception):
    def __init__(self, code): self.response = {'Error': {'Code': code}}


class Store:
    def __init__(self, rows=None):
        self.objects = {}
        self.version = 1
        self.deny = False
        self.race = False
        if rows is not None:
            self.objects['data/decisive-call-history.json'] = json.dumps({'snapshots': rows}).encode()
    def get_object(self, Bucket, Key):
        if self.deny and Key == 'data/decisive-call-history.json': raise StorageError('AccessDenied')
        if Key not in self.objects: raise StorageError('NoSuchKey')
        return {'Body': io.BytesIO(self.objects[Key]), 'ETag': str(self.version)}
    def put_object(self, Bucket, Key, Body, **kwargs):
        if kwargs.get('IfNoneMatch') == '*' and Key in self.objects: raise StorageError('PreconditionFailed')
        if self.race and Key == 'data/decisive-call-history.json':
            self.race = False
            old = json.loads(self.objects[Key]); old['snapshots'].append({'timestamp': '2026-09-17T19:00:00Z', 'snapshot_id': 'other-writer'})
            self.objects[Key] = json.dumps(old).encode(); self.version += 1
        if 'IfMatch' in kwargs and kwargs['IfMatch'] != str(self.version): raise StorageError('PreconditionFailed')
        self.objects[Key] = Body
        self.version += 1


def test_calls_khalid_zero_survives_actual_compressor_and_history():
    env=extract('justhodl-ai-brief', {'compress_intel'})
    compressed=env['compress_intel']({'scores': {'khalid_score': 0}})
    row=make_snapshot({'as_of':'2026-09-17T20:00:00Z','intelligence':compressed},{'brief_md':'x'*54})
    assert compressed['khalid_score'] == row['khalid_score'] == 0
    assert khalid_value({'khalid_score':False, 'scores':{'khalid_score':42}}) == 42


def test_calls_stub_and_generation_error_are_not_valid_waits():
    for row in (observation_output(), observation_output(error='synthetic failure')):
        assert row['decision_status']=='ERROR' and row['call_verb']=='UNKNOWN'
        assert not row['sizing_eligible'] and not decision_eligibility(row, NOW)[0]
    assert observation_output()['decision_reason']=='empty_or_stub_brief'


def test_calls_narrative_cannot_grant_itself_sizing_eligibility():
    row=observation_output('A long narrative. '*20+'\nDECISIVE CALL: LONG')
    assert row['candidate_verb']=='LONG'
    assert row['decision_status']=='ABSTAIN' and row['call_verb']=='WAIT' and not row['sizing_eligible']
    assert 'n_open_positions' not in row
    assert candidate_verb('WATCH TRIGGERS: EXIT ALL RISK if X\n'+'x'*200) is None


def test_calls_eligibility_rejects_legacy_expired_future_and_unqualified():
    assert decision_eligibility(qualified(), NOW)[0]
    for row in ({'call_verb':'LONG'}, qualified(sizing_eligible=False), qualified(evidence_ids=[]),
                qualified(expires_at='2026-09-17T13:00:00Z'), qualified(timestamp='2026-10-01T12:00:00Z'),
                qualified(validation_status='monitor_only'), qualified(call_verb='WAIT')):
        assert not decision_eligibility(row, NOW)[0]


def test_calls_history_idempotent_preserves_more_than_1000_rows():
    old=[{'timestamp':'2026-09-01T00:00:00Z','legacy_n':i} for i in range(1001)]
    store=Store(old);row=observation_output()
    append_snapshot(store,'fixture',row);doc=append_snapshot(store,'fixture',row)
    assert len(doc['snapshots'])==1002 and doc['snapshots'][:1001]==old
    assert len([k for k in store.objects if '/decisive-call-events/' in k])==1


def test_calls_history_read_failure_never_resets_ledger():
    store=Store([{'timestamp':'2026-09-01T00:00:00Z'}]);before=store.objects['data/decisive-call-history.json'];store.deny=True
    try: append_snapshot(store,'fixture',observation_output())
    except StorageError: pass
    else: raise AssertionError('Read failure was swallowed')
    assert store.objects['data/decisive-call-history.json']==before


def test_calls_history_concurrent_writer_is_retained():
    store=Store([]);store.race=True
    doc=append_snapshot(store,'fixture',observation_output())
    assert len(doc['snapshots'])==2 and doc['snapshots'][0]['snapshot_id']=='other-writer'


def test_calls_real_handler_publishes_failure_status_and_never_notifies():
    store=Store([])
    funcs={'lambda_handler','compress_intel'}
    noop=lambda *a,**k: None
    env=extract('justhodl-ai-brief',funcs,{'S3':store,'BUCKET':'fixture','private_http_denied':noop,
      'publish_private':noop,'load_json':lambda key:{'scores':{'khalid_score':0}},
      'get_anthropic_key':lambda:'TEST_ONLY','call_anthropic':lambda *a,**k:{'content':[{'type':'text','text':'x'*54}]},
      'ANTHROPIC_MODEL':'fixture','SKIP_TELEGRAM':False,
      **{name:noop for name in ('compress_calibration','compress_calibration_v2','compress_paper_portfolio',
         'compress_sectors','compress_momentum','compress_allocator','compress_asymmetric','compress_risk_sizer',
         'compress_auction','compress_ed_stress','compress_macro','compress_insiders','compress_earnings','compress_alerts')}})
    response=env['lambda_handler']({'suppress_alerts':True},None)
    row=json.loads(store.objects['data/decisive-call-history.json'])['snapshots'][-1]
    assert response['statusCode']==200 and row['decision_status']=='ERROR' and row['khalid_score']==0
    assert json.loads(store.objects['data/ai-brief.json'])['decision']==row


def test_calls_sizer_abstains_without_selling_and_retains_zero_risk_constraint():
    captured={}
    env=extract('justhodl-position-sizer-v2',{'lambda_handler'},
      {'load_json':lambda *a,**k:{'snapshots':[{'call_verb':'UNKNOWN','timestamp':'2026-09-17T20:00:00Z'}]},
       '_risk_gate_doc':lambda:{'posture':'SEVERE','sizing_multiplier':0},
       'write_json':lambda key,body:captured.update(body)})
    result=env['lambda_handler']({},None)
    assert result['statusCode']==200 and captured['status']=='ABSTAIN'
    assert captured['risk_constraints']['sizing_multiplier']==0
    assert captured['risk_multiplier'] is None and captured['positions']==captured['setups']==[]
    assert captured['summary']['total_recommended_exposure_pct'] is None


def test_calls_replay_unknown_wait_and_legacy_never_create_exposure():
    rows=[{'timestamp':'2026-09-17T12:00:00Z','call_verb':verb} for verb in ('UNKNOWN','WAIT','LONG','HOLD')]
    out=REPLAY['replay'](rows,{'2026-09-17':{'open':100,'close':200}},NOW)
    assert out['status']=='no_eligible_calls' and out['calls']==out['nav_curve']==[]
    assert out['summary']['final_nav'] is None and not out['calibration_eligible']


def test_calls_replay_after_close_cannot_capture_same_day_return():
    bars={'2026-09-17':{'open':100,'close':200},'2026-09-18':{'open':100,'close':110}}
    out=REPLAY['replay']([qualified(timestamp='2026-09-17T21:00:00Z')],bars,NOW)
    assert out['calls'][0]['executed_at']=='2026-09-18T13:30:00+00:00'
    assert out['summary']['total_return_pct']==10 and len(out['nav_curve'])==1


def test_calls_replay_abstention_retains_actual_simulated_holdings():
    bars={'2026-09-17':{'open':100,'close':110},'2026-09-18':{'open':110,'close':120}}
    rows=[qualified(target_exposure=.2),{'timestamp':'2026-09-17T20:30:00Z','call_verb':'UNKNOWN'}]
    out=REPLAY['replay'](rows,bars,NOW)
    assert out['summary']['final_nav']==104000 and len(out['calls'])==1
    assert out['summary']['net_return_pct'] is None and out['summary']['sharpe_proxy'] is None


def test_calls_replay_expired_at_execution_and_missing_allocation_excluded():
    bars={'2026-09-18':{'open':100,'close':110}}
    row=qualified(timestamp='2026-09-17T21:00:00Z',expires_at='2026-09-18T01:00:00Z')
    assert REPLAY['replay']([row],bars,NOW)['excluded']['expired_decision']==1
    for val in (None,True,float('nan')):
        out=REPLAY['replay']([qualified(target_exposure=val)],bars,NOW)
        assert not out['calls']


def test_calls_replay_dst_and_strictly_later_open():
    assert REPLAY['session_time']('2026-03-06',9,30).hour==14
    assert REPLAY['session_time']('2026-03-09',9,30).hour==13
    row=qualified(timestamp='2026-09-17T13:30:00Z')
    out=REPLAY['replay']([row],{'2026-09-17':{'open':100,'close':200},'2026-09-18':{'open':100,'close':110}},NOW)
    assert out['calls'][0]['execution_date']=='2026-09-18'


def test_calls_replay_zero_allocation_is_not_missing():
    out=REPLAY['replay']([qualified(call_verb='EXIT_ALL_RISK',target_exposure=0)],
                         {'2026-09-17':{'open':100,'close':200}},NOW)
    assert len(out['calls'])==1 and out['summary']['final_nav']==100000


def test_calls_replay_handler_no_qualified_calls_never_fetches_prices():
    store=Store([{'timestamp':'2026-09-17T12:00:00Z','call_verb':'UNKNOWN'}])
    def no_prices(*args): raise AssertionError('Unqualified history must not request prices')
    env=extract('justhodl-calls-backtest',{'lambda_handler','publish'},
                {'S3':store,'BUCKET':'fixture','replay':REPLAY['replay'],'fetch_spy_daily':no_prices})
    assert env['lambda_handler']({})['statusCode']==200
    public=json.loads(store.objects['data/calls-replay.json'])
    assert public['status']=='no_eligible_calls' and public['summary']['total_return_pct'] is None
    assert store.objects['backtest/calls-results.json']==store.objects['data/calls-replay.json']


def test_calls_replay_handler_read_failure_retires_previous_success():
    store=Store([]);store.deny=True
    store.objects['data/calls-replay.json']=b'{"status":"diagnostic_only"}'
    env=extract('justhodl-calls-backtest',{'lambda_handler','publish'},
                {'S3':store,'BUCKET':'fixture','replay':REPLAY['replay']})
    assert env['lambda_handler']({})['statusCode']==503
    public=json.loads(store.objects['data/calls-replay.json'])
    assert public['status']=='error' and public['summary']['total_return_pct'] is None


def test_calls_risk_constraint_zero_is_live_and_not_cached():
    store=Store([])
    doc={'generated_at':datetime.now(timezone.utc).isoformat(),'sizing_multiplier':0,'posture':'SEVERE'}
    store.objects['data/risk-gate.json']=json.dumps(doc).encode()
    env=extract('justhodl-position-sizer-v2',{'_risk_gate_doc'},{'S3':store,'BUCKET':'fixture'})
    assert env['_risk_gate_doc']()['sizing_multiplier']==0
    doc['generated_at']='2020-01-01T00:00:00Z'
    store.objects['data/risk-gate.json']=json.dumps(doc).encode()
    assert env['_risk_gate_doc']()['sizing_multiplier'] is None


if __name__=='__main__':
    tests=[f for n,f in list(globals().items()) if n.startswith('test_')]
    for test in tests: test()
    print(f'Calls integrity tests passed: {len(tests)}')
