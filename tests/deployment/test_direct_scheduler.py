"""Direct schedules preserve private execution settings and fail closed on races."""
import copy
import runpy
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
module=runpy.run_path(str(ROOT/'scripts/apply_direct_scheduler.py'))
apply=module['apply']
ARN='arn:aws:lambda:us-east-1:123456789012:function:fixture'
CONFIG={'eventbridge_scheduler':{'schedule_name':'daily','cron':'rate(1 hour)','role_arn':'arn:aws:iam::123456789012:role/scheduler'}}
CURRENT={'Name':'daily','GroupName':'default','State':'DISABLED','ScheduleExpression':'rate(2 hours)',
         'FlexibleTimeWindow':{'Mode':'OFF'},'Target':{'Arn':ARN,'RoleArn':CONFIG['eventbridge_scheduler']['role_arn'],
         'Input':'{"owner":"PRIVATE_CANARY"}','RetryPolicy':{'MaximumRetryAttempts':5},'DeadLetterConfig':{'Arn':'dlq'}}}

class Failure(Exception):
    def __init__(self,code): self.response={'Error':{'Code':code}}

class Client:
    def __init__(self,current=None,error=None,race=False,corrupt=False):
        self.current=copy.deepcopy(current); self.error=error; self.race=race; self.corrupt=corrupt; self.reads=0; self.writes=[]
    def get_schedule(self,**kwargs):
        self.reads+=1
        if self.error: raise Failure(self.error)
        if self.current is None: raise Failure('ResourceNotFoundException')
        result=copy.deepcopy(self.current)
        if (self.race and self.reads==2) or (self.corrupt and self.writes): result['State']='ENABLED'
        return result
    def update_schedule(self,**kwargs): self.writes.append('update'); self.current=copy.deepcopy(kwargs)
    def create_schedule(self,**kwargs): self.writes.append('create'); self.current=copy.deepcopy(kwargs)

def test_direct_schedule_preserves_input_retry_dlq_and_disabled_state():
    client=Client(CURRENT)
    assert apply(client,CONFIG,ARN)['status']=='VERIFIED'
    assert client.writes==['update']
    assert client.current['Target']==CURRENT['Target']
    assert client.current['State']=='DISABLED'
    assert client.current['ScheduleExpression']=='rate(1 hour)'

def test_only_confirmed_not_found_creates_schedule():
    client=Client()
    assert apply(client,CONFIG,ARN)['status']=='VERIFIED' and client.writes==['create']
    for code in ('AccessDeniedException','TooManyRequestsException','InternalServerException'):
        client=Client(error=code)
        try: apply(client,CONFIG,ARN)
        except Failure: pass
        else: raise AssertionError('Read error allowed a write')
        assert not client.writes

def test_qualified_and_unrelated_targets_never_get_demoted_or_reassigned():
    for target in (ARN+':live',ARN+':7',ARN+'-other'):
        current=copy.deepcopy(CURRENT);current['Target']['Arn']=target
        client=Client(current)
        try: apply(client,CONFIG,ARN)
        except ValueError: pass
        else: raise AssertionError('Protected target overwritten')
        assert not client.writes

def test_changed_schedule_stops_before_update():
    client=Client(CURRENT,race=True)
    try: apply(client,CONFIG,ARN)
    except RuntimeError as exc: assert str(exc)=='schedule_changed_before_update'
    else: raise AssertionError('Concurrent changes were overwritten')
    assert not client.writes

def test_readback_mismatch_cannot_report_success():
    client=Client(CURRENT,corrupt=True)
    try: apply(client,CONFIG,ARN)
    except RuntimeError as exc: assert str(exc)=='schedule_readback_mismatch'
    else: raise AssertionError('Incorrect deployed state accepted')
