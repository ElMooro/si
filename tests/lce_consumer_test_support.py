"""Exercise actual text and allocator consumers without any external action."""
import ast
from datetime import datetime,timezone
import json
from pathlib import Path
import sys
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
from test_lce_research_model import fixture,NOW
import lce_research_model as model
from research_brief_model import narrative_context
from tenor_research_model import public_summary


def run(engine):
    name='build_context' if engine=='ai-chat' else 'rule_liquidity_credit_engine'
    path=ROOT/'aws/lambdas'/('justhodl-'+engine)/'source/lambda_function.py'
    node=next(n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name==name)
    source,original=fixture();packet=model.build(source,None,None,original,NOW)
    packet['replay']={'manifest_key':'data/lce-research/runs/'+('a'*64)+'.json','output_sha256':model.digest(packet)}
    if engine=='ai-chat':
        class Frozen(datetime):
            @classmethod
            def now(cls,tz=None):return datetime.fromisoformat(NOW)
        env={'datetime':Frozen,'timezone':timezone,'json':json,'research_brief_context':narrative_context,
             'tenor_research_summary':public_summary,'_CALIBRATION_AVAILABLE':False,
             'detect_entities':lambda _:([],[]),'get_s3':lambda key:packet if key=='data/liquidity-credit-engine.json' else {}}
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),env)
        with patch.object(model,'datetime',Frozen):text=env[name]('Show liquidity observations')
        line=next(line for line in text.splitlines() if line.startswith('[LIQUIDITY/CREDIT RESEARCH'))
        context=json.loads(line.split('] ',1)[1]);row=next(r for r in context['measurements'] if r['series_id']=='WALCL')
        assert row['value']=='6746548' and row['unit']=='Millions of U.S. Dollars' and row['observation_date']=='2026-09-16'
        assert '[LCE BALANCE-SHEET]' not in text and context['sizing_eligible'] is False
    else:
        reads=[];env={'fs3':lambda key:reads.append(key) or {'regime':'CALM','sizing_eligible':True,'composite':{'score':0}}}
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),env)
        scores={'SPY':3};evidence=[];env[name](scores,evidence)
        assert scores=={'SPY':3} and evidence==[] and reads==['data/liquidity-credit-engine.json']
    print(engine+': actual LCE consumer preserves research boundary and units')


if __name__=='__main__':
    for engine in ('ai-chat','allocator'):run(engine)
