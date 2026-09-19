"""Execute the production consumer boundaries without provider or AI calls."""
import ast
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]


def run(name):
    source=(ROOT/'aws/lambdas'/name/'source/lambda_function.py').read_text(encoding='utf-8');tree=ast.parse(source)
    if name=='justhodl-signal-board':
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='n_eurodollar_plumbing')
        scope={};exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-signal-boundary','exec'),scope)
        fn=scope[node.name]
        assert fn({'contract':'funding-original-research.v1','calls_eligible':False,'plumbing_health':100})[0] is None
        assert fn({'plumbing_health':None})[0] is None
        assert fn({'plumbing_health':0})[0]==-2
    elif name=='justhodl-bond-warroom':
        nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('eurodollar_shortage','heartbeat')]
        scope={};exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual-warroom-boundary','exec'),scope)
        for packet in ({},{'calls_eligible':False,'composite_score':100}):
            result=scope['eurodollar_shortage']({}, {'eurodollar_plumbing':packet})
            assert result['state']=='UNQUALIFIED' and result['score'] is None and result['points'] is None
            assert 'no eurodollar-shortage' not in result['text'].lower()
            assert scope['heartbeat']({}, {'score':0,'state':'CALM'}, result)['score']==0
    elif name=='justhodl-bond-desk':
        handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        node=next(n for n in handler.body if isinstance(n,ast.If) and 'unqualified_funding_vote' in ast.unparse(n))
        for health,packet in ((None,{}),(0,{'calls_eligible':False}),(100,{'calls_eligible':False})):
            scope={'health':health,'pl':packet,'GF':{'score':50,'fresh':True}}
            exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-desk-boundary','exec'),scope)
            assert scope['GF']['score'] is None and scope['GF']['fresh'] is False
    else:raise ValueError('unsupported consumer')
    print(name+': funding abstention boundary passed')
