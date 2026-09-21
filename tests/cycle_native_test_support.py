"""Run actual Cycle Clock compilation with a deliberately unqualified upstream."""
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-cycle-clock/tests')]
from cycle_fixture import fixture,store,model

def synthesis_with(key,packet):
    client,inputs,_,_=fixture()
    client.objects[key]=model.encoded(packet)
    inputs['legacy'][key]=store.snapshot(client,'b',key)
    return store.compile_output(inputs,store.reader(client,'b'))
