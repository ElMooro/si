"""Exercise actual consumer functions with legacy and forged actionable packets."""
import ast
from datetime import datetime
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
from tenor_research_model import public_summary

PACKET={'any_firing':True,'any_watch':True,'composite_score':100,'sizing_eligible':True,
        'signals':{k:{'state':'EXTREME','direction':'CUTS_PRICED','interpretation':'unvalidated'} for k in ('fed_path','eurodollar','qe_imminence')},
        'transitions':[{'channel':'qe_imminence','new_state':'EXTREME','prior_state':'OFF'}]}

def source(engine):return ROOT/'aws/lambdas'/('justhodl-'+engine)/'source/lambda_function.py'

def functions(engine,names,env):
    nodes=[n for n in ast.parse(source(engine).read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name in names]
    assert len(nodes)==len(names)
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(source(engine)),'exec'),env)
    return env

def run(engine):
    if engine=='allocator':
        reads=[];scope=functions(engine,{'rule_tenor_signals'},{'fs3':lambda key:reads.append(key) or PACKET})
        scores={'TLT':3};evidence=[];scope['rule_tenor_signals'](scores,evidence)
        assert scores=={'TLT':3} and evidence==[] and reads==['data/auction-tenor-signals.json']
    elif engine=='alert-router':
        alerts=[];scope=functions(engine,{'check_tenor_signals'},{'load_json':lambda key:PACKET})
        scope['check_tenor_signals'](alerts);assert alerts==[]
    elif engine=='daily-report-v3':
        env={'load_tenor_signals':lambda:PACKET,'datetime':datetime,'tenor_research_summary':public_summary}
        env.update({k:lambda:{} for k in ('load_lce','load_global_cycle','load_leading_markets')})
        env.update({k:lambda _: (0,None) for k in ('lce_ki_adjustment','gbc_ki_adjustment','leading_markets_ki_adjustment')})
        scope=functions(engine,{'compute_ki'},env);result=scope['compute_ki']({}, {})
        assert result['score']==50 and result['signals']==[] and result['tenor_state']=='UNAVAILABLE'
    elif engine in ('ai-chat','morning-intelligence'):
        tree=ast.parse(source(engine).read_text(encoding='utf-8'))
        calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='tenor_research_summary']
        assert calls,'narrative consumer bypasses bounded dated summary'
        assert public_summary(PACKET)['status']=='UNAVAILABLE'
        assert not any(isinstance(n,ast.Constant) and isinstance(n.value,str) and ('[TENOR SIGNALS]' in n.value or 'TENOR_SIGNALS: composite:' in n.value) for n in ast.walk(tree))
    else:raise ValueError(engine)
    print(engine+': Treasury research cannot acquire allocation/alert/score authority')

if __name__=='__main__':
    for engine in ('allocator','alert-router','daily-report-v3','ai-chat','morning-intelligence'):run(engine)
