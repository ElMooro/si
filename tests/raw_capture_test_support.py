"""Large-body witnesses exercise the actual capture expressions in collectors."""
import ast
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

def run(engine):
    path=ROOT/'aws/lambdas'/('justhodl-'+engine)/'source/lambda_function.py'
    tree=ast.parse(path.read_text(encoding='utf-8'))
    calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='snapshot']
    assert calls,'collector has no archive capture'
    original=b'{"observations":['+b'123.45,'*100000+b'0]}'
    for call in calls:
        expr=call.args[2];names={n.id for n in ast.walk(expr) if isinstance(n,ast.Name)}
        env={name:original for name in names}
        captured=eval(compile(ast.Expression(expr),str(path),'eval'),{'__builtins__':{}},env)
        assert captured==original,'collector archives only a partial response: '+engine
    print(engine+': '+str(len(calls))+' actual capture arguments retain the complete >400KB body')
