"""Offline bridge: the actual frozen Calls producer and actual history contract."""
from pathlib import Path
from datetime import datetime,timezone
import json,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
from calls_period_replay_tests import packet,AT,KEY
from calls_research_replay import prepare,replay
from calls_free_brief import METHOD
from calls_contract import make_snapshot
from morning_free_research import build,UNAVAILABLE


def fixture():
    bundle=prepare(lambda key:packet() if key==KEY else {},AT)
    assert replay(bundle)['status']=='reproduced'
    public=bundle['payload']['output']
    assert public['generation_method']==METHOD
    row=make_snapshot({'as_of':AT},public)
    assert row['generation_method']==METHOD
    return {'public':public,'row':row,'at':AT}


def verify_morning(data):
    at=datetime.fromisoformat(data['at'].replace('Z','+00:00'))
    predecessor={}
    path=ROOT/'tests/fixtures/pre-calls-v2-consumers-morning_free_research.py.txt'
    exec(compile(path.read_text(encoding='utf-8'),str(path),'exec'),predecessor)
    assert predecessor['build'](lambda _:data['public'],at)==predecessor['UNAVAILABLE']
    for method in (data['public']['generation_method'],'warehouse_deterministic_v1'):
        p={**data['public'],'generation_method':method}
        seen=[];out=build(lambda key:seen.append(key) or p,at)
        assert seen==['data/ai-brief-public.json'] and out!=UNAVAILABLE
        assert p['generated_at'] in out and '**DECISIVE CALL: WAIT**' in out
        assert len(out.encode('utf-16-le'))//2<=4096
        assert p['brief_md'].rstrip() in out or 'complete brief exceeds' in out
    for method in ('warehouse_deterministic_v999','warehouse_deterministic_v2x',None,{},[]):
        assert build(lambda _:dict(data['public'],generation_method=method),at)==UNAVAILABLE


if __name__=='__main__':
    data=fixture();verify_morning(data)
    print(json.dumps(data,ensure_ascii=False,allow_nan=False))
