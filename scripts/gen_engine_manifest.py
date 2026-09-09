#!/usr/bin/env python3
"""Producer ownership from actual write arguments. Never infer ownership from nearby reads.
Unresolved writes and dynamic families are explicit; deployed env overrides remain a verification gate.
"""
import ast
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
LAMBDAS = ROOT / 'aws' / 'lambdas'
WRITE_POS = {'put_object': None, 'upload_file': 2, 'upload_fileobj': 2,
             'put_json': 0, 'write_json': 0, 'save_json': 0}
READ_POS = {'get_object': None, 'head_object': None, 'download_file': 1,
            'download_fileobj': 1, 'get_json': 0, 'read_json': 0}
KEY_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_./*{}-]*(?:\.json(?:\.gz)?|/\*)$')

def normalise(value):
    if not isinstance(value, str): return None
    value = re.sub(r'\*+', '*', value)
    if not KEY_RE.fullmatch(value) or '..' in value.split('/') or '{' in value: return None
    return value

class Scan:
    def __init__(self, code, environment=None):
        self.tree = ast.parse(code)
        self.environment = environment or {}
        self.functions = {n.name:n for n in self.tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))}
        self.writes, self.reads, self.unresolved, self.defaults = set(),set(),[],{}
        self.proofs = {}
        self.visited_write_lines=set()
        self.globals = {}
        self.containers={t.id:n.value for n in self.tree.body if isinstance(n,ast.Assign) and isinstance(n.value,(ast.List,ast.Tuple,ast.Dict)) for t in n.targets if isinstance(t,ast.Name)}
        for node in self.tree.body: self.assign(node, self.globals)

    def resolve(self, node, env):
        if isinstance(node,ast.Constant): return node.value if isinstance(node.value,str) else None
        if isinstance(node,ast.Name): return env.get(node.id)
        if isinstance(node,ast.JoinedStr):
            return ''.join(str(v.value) if isinstance(v,ast.Constant) else (self.resolve(v.value,env) or '*') for v in node.values)
        if isinstance(node,ast.BinOp):
            l,r=self.resolve(node.left,env),self.resolve(node.right,env)
            if isinstance(node.op,ast.Add): return (l or '*')+(r or '*')
            if isinstance(node.op,ast.Mod) and l: return re.sub(r'%(?:\([^)]*\))?[-+ 0#]*\d*(?:\.\d+)?[sdifrxX%]','*',l)
        if isinstance(node,ast.Call):
            if isinstance(node.func,ast.Attribute) and node.func.attr=='format':
                base=self.resolve(node.func.value,env)
                if base is not None:
                    import string
                    try: return ''.join(literal+('*' if field is not None else '') for literal,field,spec,conv in string.Formatter().parse(base))
                    except ValueError: return None
            func=ast.unparse(node.func)
            if func in ('os.environ.get','os.getenv','environ.get') and node.args:
                name=self.resolve(node.args[0],env)
                default=self.resolve(node.args[1],env) if len(node.args)>1 else None
                if name and normalise(default): self.defaults[name]=default
                return self.environment.get(name, default)
        if isinstance(node,ast.IfExp):
            a,b=self.resolve(node.body,env),self.resolve(node.orelse,env)
            return a if a==b else None
        return None

    def assign_target(self,target,value,env):
        if isinstance(target,ast.Name): env[target.id]=self.resolve(value,env)
        elif isinstance(target,(ast.Tuple,ast.List)) and isinstance(value,(ast.Tuple,ast.List)) and len(target.elts)==len(value.elts):
            vals=[self.resolve(v,env) for v in value.elts]
            for t,v in zip(target.elts,vals):
                if isinstance(t,ast.Name): env[t.id]=v
    def assign(self,node,env):
        if isinstance(node,ast.Assign):
            for t in node.targets:self.assign_target(t,node.value,env)
        if isinstance(node,ast.AnnAssign) and node.value:self.assign_target(node.target,node.value,env)

    def call(self,node,env,stack):
        if isinstance(node.func,ast.Name) and node.func.id in self.functions:
            name=node.func.id
            if name in stack or len(stack)>12:return
            fn=self.functions[name]; args=list(fn.args.posonlyargs)+list(fn.args.args)
            bound=dict(self.globals)
            for a in args+list(fn.args.kwonlyargs):bound[a.arg]=None
            for a,d in zip(args[-len(fn.args.defaults):],fn.args.defaults) if fn.args.defaults else []:bound[a.arg]=self.resolve(d,env)
            for a,v in zip(args,node.args):bound[a.arg]=self.resolve(v,env)
            for kw in node.keywords:
                if kw.arg:bound[kw.arg]=self.resolve(kw.value,env)
            self.block(fn.body,bound,stack+(name,))
            return
        if not isinstance(node.func,ast.Attribute):return
        attr=node.func.attr
        kind='write' if attr in WRITE_POS else 'read' if attr in READ_POS else None
        if not kind:return
        if kind=='write':self.visited_write_lines.add(node.lineno)
        keynode=next((kw.value for kw in node.keywords if kw.arg=='Key'),None)
        pos=(WRITE_POS if kind=='write' else READ_POS)[attr]
        if keynode is None and pos is not None and len(node.args)>pos:keynode=node.args[pos]
        if keynode is None:
            # boto3 keyword-only Key binding is intentionally not guessed from Bucket/Body.
            if kind=='write':self.unresolved.append({'line':node.lineno,'operation':attr,'reason':'unresolved key argument'})
            return
        key=normalise(self.resolve(keynode,env))
        if key:
            (self.writes if kind=='write' else self.reads).add(key)
            if kind=='write':self.proofs.setdefault(key,set()).add(node.lineno)
        elif kind=='write':self.unresolved.append({'line':node.lineno,'operation':attr,'reason':'dynamic or unresolved key'})

    def expr(self,node,env,stack):
        if isinstance(node,ast.Call):self.call(node,env,stack)
        for child in ast.iter_child_nodes(node):
            if not isinstance(child,(ast.FunctionDef,ast.AsyncFunctionDef,ast.Lambda)):self.expr(child,env,stack)

    def block(self,nodes,env,stack):
        for n in nodes:
            if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)):continue
            self.assign(n,env)
            if isinstance(n,ast.If):
                self.expr(n.test,env,stack)
                branches=[]
                for body in (n.body,n.orelse):
                    branch=dict(env);self.block(body,branch,stack);branches.append(branch)
                for key in set().union(*branches):env[key]=branches[0].get(key) if branches[0].get(key)==branches[1].get(key) else None
            elif isinstance(n,(ast.For,ast.AsyncFor)):
                self.expr(n.iter,env,stack)
                iterator=self.containers.get(n.iter.id,n.iter) if isinstance(n.iter,ast.Name) else n.iter
                if isinstance(iterator,(ast.List,ast.Tuple)):
                    for value in iterator.elts:
                        branch=dict(env);self.assign_target(n.target,value,branch);self.block(n.body,branch,stack)
                else:
                    branch=dict(env)
                    for a in ast.walk(n.target):
                        if isinstance(a,ast.Name):branch[a.id]=None
                    self.block(n.body,branch,stack)
                self.block(n.orelse,dict(env),stack)
            elif isinstance(n,(ast.Try,ast.With,ast.AsyncWith,ast.While)):
                for child in ast.iter_child_nodes(n):
                    if not isinstance(child,ast.stmt):self.expr(child,env,stack)
                self.block(n.body,dict(env),stack)
                for h in getattr(n,'handlers',[]):self.block(h.body,dict(env),stack)
                self.block(getattr(n,'orelse',[]),dict(env),stack);self.block(getattr(n,'finalbody',[]),dict(env),stack)
            else:self.expr(n,env,stack)

    def run(self):
        self.block(self.tree.body,dict(self.globals),())
        # All defined write-capable functions are potential producers; unresolved parameters are reported.
        for name,fn in self.functions.items():
            env=dict(self.globals)
            for a in fn.args.posonlyargs+fn.args.args+fn.args.kwonlyargs:env[a.arg]=None
            self.block(fn.body,env,(name,))
        for node in ast.walk(self.tree):
            if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute) and node.func.attr in WRITE_POS and node.lineno not in self.visited_write_lines:
                self.unresolved.append({'line':node.lineno,'operation':node.func.attr,'reason':'write site outside analyzed call graph'})
        return self

def scan_code(code,environment=None):
    return Scan(code,environment).run()

def ast_keys(code):
    try:s=scan_code(code);return sorted(s.writes),sorted(s.reads),True
    except SyntaxError:return [],[],False

def confirmed_write_keys(code):
    """Compatibility API. Never fall back to the unsafe window scanner."""
    return ast_keys(code)[0]

def build(root=ROOT):
    engines=[]
    for d in sorted((root/'aws/lambdas').iterdir()):
        if not (d/'source/lambda_function.py').exists():continue
        cfg={}
        try:cfg=json.loads((d/'config.json').read_text())
        except (OSError,ValueError):pass
        env=cfg.get('environment') or {}; env=env.get('Variables',env) if isinstance(env,dict) else {}
        keys,reads,proofs,unresolved,defaults=set(),set(),{},[],{}
        for src in sorted((d/'source').rglob('*.py')):
            if '__pycache__' in src.parts:continue
            rel=str(src.relative_to(d/'source'))
            try:
                s=scan_code(src.read_text(errors='replace'),env);keys.update(s.writes);reads.update(s.reads);defaults.update(s.defaults)
                for key,lines in s.proofs.items():proofs.setdefault(key,[]).extend({'file':rel,'line':line} for line in sorted(lines))
                unresolved.extend(dict(x,file=rel) for x in s.unresolved)
            except SyntaxError as exc:unresolved.append({'file':rel,'line':exc.lineno,'reason':'parse failure'})
        # Resolve parameter-only reports only when that exact write site has a concrete or family binding.
        proven={(x['file'],x['line']) for values in proofs.values() for x in values}
        unresolved=[dict(t) for t in sorted({tuple(sorted(x.items())) for x in unresolved})]  # A resolved invocation must not hide another unresolved invocation at the same write site.
        exact=sorted(k for k in keys if '*' not in k);patterns=sorted(k for k in keys if '*' in k)
        engines.append({'engine':d.name,'keys':exact,'n_keys':len(exact),'key_patterns':patterns,
                        'reads':sorted(reads),'method':'ast-call-binding-v3','write_evidence':proofs,
                        'unresolved_writes':unresolved,'environment_key_defaults':defaults,
                        'ownership_status':'incomplete' if unresolved else 'source_bound',
                        'deployment_overrides_verified':False,'description':str(cfg.get('description') or '')[:140]})
    return {'schema_version':'engine-manifest.v3','generated_at':datetime.now(timezone.utc).isoformat(),
            'source':'scripts/gen_engine_manifest.py; source-bound outputs, separate families and unresolved writes; runtime not certified',
            'n_engines':len(engines),'engines':engines}

def main():
    doc=build();path=ROOT/'engine-manifest.json';tmp=path.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(doc,separators=(',',':')));os.replace(tmp,path)
    print(f"[manifest] {len(doc['engines'])} engines; {sum(bool(e['keys']) for e in doc['engines'])} with concrete outputs; unresolved explicitly reported")
if __name__=='__main__':main()
