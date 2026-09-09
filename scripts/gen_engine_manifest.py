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
KEY_RE = re.compile(r'^[A-Za-z0-9_][A-Za-z0-9_./=*{}-]*(?:\.json(?:\.gz)?|/\*)$')

def normalise(value):
    if not isinstance(value, str): return None
    value = re.sub(r'\*+', '*', value)
    if not KEY_RE.fullmatch(value) or '..' in value.split('/') or '{' in value: return None
    return value

class FunctionBinding:
    def __init__(self,node,environment):
        self.node=node
        self.environment=environment

class Scan:
    def __init__(self, code, environment=None, initial_symbols=None):
        self.tree = ast.parse(code)
        self.environment = environment or {}
        self.executor_names=set()
        for item in ast.walk(self.tree):
            if isinstance(item,(ast.With,ast.AsyncWith)):
                for manager in item.items:
                    if isinstance(manager.optional_vars,ast.Name) and isinstance(manager.context_expr,ast.Call) and ast.unparse(manager.context_expr.func).split('.')[-1] in ('ThreadPoolExecutor','ProcessPoolExecutor'):self.executor_names.add(manager.optional_vars.id)
            if isinstance(item,ast.Assign) and isinstance(item.value,ast.Call) and ast.unparse(item.value.func).split('.')[-1] in ('ThreadPoolExecutor','ProcessPoolExecutor'):
                self.executor_names.update(t.id for t in item.targets if isinstance(t,ast.Name))
        self.functions={}
        for node in self.tree.body:
            if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)):self.functions[node.name]=node
            elif isinstance(node,ast.Assign) and isinstance(node.value,ast.Name) and node.value.id in self.functions:
                for target in node.targets:
                    if isinstance(target,ast.Name):self.functions[target.id]=self.functions[node.value.id]
        self.writes, self.reads, self.unresolved, self.defaults = set(),set(),[],{}
        self.proofs = {}
        self.visited_write_lines=set();self.called_functions=set();self.other_writes=[]
        self.globals = dict(initial_symbols or {})
        self.containers={t.id:n.value for n in self.tree.body if isinstance(n,ast.Assign) and isinstance(n.value,(ast.List,ast.Tuple,ast.Dict)) for t in n.targets if isinstance(t,ast.Name)}
        for node in self.tree.body: self.assign(node, self.globals)

    def resolve(self, node, env):
        if isinstance(node,ast.Constant): return node.value if isinstance(node.value,str) else None
        if isinstance(node,ast.Name): return env.get(node.id)
        if isinstance(node,ast.Attribute):
            base=self.resolve(node.value,env)
            return base.get(node.attr) if isinstance(base,dict) else None
        if isinstance(node,ast.Dict):
            result={}
            for key,value in zip(node.keys,node.values):
                if key is None:
                    unpack=self.resolve(value,env)
                    if not isinstance(unpack,dict):return None
                    result.update(unpack)
                else:
                    name=self.resolve(key,env)
                    if not isinstance(name,str):return None
                    result[name]=self.resolve(value,env)
            return result
        if isinstance(node,ast.Subscript):
            base=self.resolve(node.value,env);key=self.resolve(node.slice,env)
            return base.get(key) if isinstance(base,dict) else None
        if isinstance(node,ast.JoinedStr):
            return ''.join(str(v.value) if isinstance(v,ast.Constant) else (self.resolve(v.value,env) or '*') for v in node.values)
        if isinstance(node,ast.BinOp):
            l,r=self.resolve(node.left,env),self.resolve(node.right,env)
            if isinstance(node.op,ast.Add): return (l or '*')+(r or '*')
            if isinstance(node.op,ast.Mod) and isinstance(l,str):
                values=[self.resolve(v,env) for v in node.right.elts] if isinstance(node.right,ast.Tuple) else [self.resolve(node.right,env)]
                named=values[0] if len(values)==1 and isinstance(values[0],dict) else {};position=0
                def substitute(match):
                    nonlocal position
                    if match.group(0)=='%%':return '%'
                    field=match.group(1)
                    if field:value=named.get(field)
                    else:value=values[position] if position<len(values) else None;position+=1
                    return value if isinstance(value,str) else '*'
                return re.sub(r'%(?:\(([^)]*)\))?[-+ 0#]*\d*(?:\.\d+)?[sdifrxX%]',substitute,l)
        if isinstance(node,ast.Call):
            if isinstance(node.func,ast.Attribute) and node.func.attr in ('replace','strip','lstrip','rstrip','removeprefix','removesuffix','lower','upper'):
                value=self.resolve(node.func.value,env);args=[self.resolve(arg,env) for arg in node.args]
                if isinstance(value,str) and all(isinstance(arg,str) for arg in args):
                    try:return getattr(value,node.func.attr)(*args)
                    except (TypeError,ValueError):return None
            if isinstance(node.func,ast.Name) and node.func.id=='dict' and not node.args:
                return {kw.arg:self.resolve(kw.value,env) for kw in node.keywords if kw.arg}
            if isinstance(node.func,ast.Attribute) and node.func.attr=='format':
                base=self.resolve(node.func.value,env)
                if base is not None:
                    import string
                    try:
                        pieces=[];auto=0;values=[self.resolve(v,env) for v in node.args];named={kw.arg:self.resolve(kw.value,env) for kw in node.keywords if kw.arg}
                        for literal,field,spec,conv in string.Formatter().parse(base):
                            pieces.append(literal)
                            if field is None:continue
                            if field=='':value=values[auto] if auto<len(values) else None;auto+=1
                            elif field.isdigit():value=values[int(field)] if int(field)<len(values) else None
                            else:value=named.get(field)
                            pieces.append(value if isinstance(value,str) else '*')
                        return ''.join(pieces)
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
        elif isinstance(target,ast.Subscript) and isinstance(target.value,ast.Name):
            name=target.value.id;mapping=env.get(name);key=self.resolve(target.slice,env)
            if isinstance(mapping,dict) and isinstance(key,str):env[name]={**mapping,key:self.resolve(value,env)}
            else:env[name]=None
        elif isinstance(target,(ast.Tuple,ast.List)) and isinstance(value,(ast.Tuple,ast.List)) and len(target.elts)==len(value.elts):
            vals=[self.resolve(v,env) for v in value.elts]
            for t,v in zip(target.elts,vals):
                if isinstance(t,ast.Name): env[t.id]=v
    def assign(self,node,env):
        if isinstance(node,ast.Assign):
            for t in node.targets:self.assign_target(t,node.value,env)
        if isinstance(node,ast.AnnAssign) and node.value:self.assign_target(node.target,node.value,env)

    def call(self,node,env,stack):
        local_functions=(env.get('__jh_local_functions') or {})
        def known_function(name):return local_functions[name] if name in local_functions else self.functions.get(name)
        # Standard executor callbacks bind the callable's real argument positions.
        if isinstance(node.func,ast.Attribute) and isinstance(node.func.value,ast.Name) and node.func.value.id in self.executor_names and node.func.attr in ('submit','map') and node.args and isinstance(node.args[0],ast.Name) and known_function(node.args[0].id):
            callback=node.args[0]
            if node.func.attr=='submit':
                self.call(ast.Call(func=callback,args=node.args[1:],keywords=node.keywords),env,stack)
            else:
                iterables=[self.containers.get(arg.id,arg) if isinstance(arg,ast.Name) else arg for arg in node.args[1:]]
                if iterables and all(isinstance(arg,(ast.List,ast.Tuple)) for arg in iterables):
                    for values in zip(*(arg.elts for arg in iterables)):self.call(ast.Call(func=callback,args=list(values),keywords=[]),env,stack)
                else:self.call(ast.Call(func=callback,args=[],keywords=[]),env,stack)
            return
        if isinstance(node.func,ast.Name) and known_function(node.func.id):
            name=node.func.id;self.called_functions.add(name)
            if name in stack or len(stack)>12:return
            binding=known_function(name);fn=binding.node if isinstance(binding,FunctionBinding) else binding; args=list(fn.args.posonlyargs)+list(fn.args.args)
            # A nested function captures its lexical caller values. A module
            # function does not inherit arbitrary locals from its caller.
            bound=dict(binding.environment if isinstance(binding,FunctionBinding) else self.globals)
            for a in args+list(fn.args.kwonlyargs):bound[a.arg]=None
            for a,d in zip(args[-len(fn.args.defaults):],fn.args.defaults) if fn.args.defaults else []:bound[a.arg]=self.resolve(d,env)
            for a,d in zip(fn.args.kwonlyargs,fn.args.kw_defaults):
                if d is not None:bound[a.arg]=self.resolve(d,env)
            for a,v in zip(args,node.args):bound[a.arg]=self.resolve(v,env)
            for kw in node.keywords:
                if kw.arg:bound[kw.arg]=self.resolve(kw.value,env)
                else:
                    unpack=self.resolve(kw.value,env)
                    if isinstance(unpack,dict):bound.update(unpack)
            self.block(fn.body,bound,stack+(name,))
            return
        if not isinstance(node.func,ast.Attribute):return
        attr=node.func.attr
        kind='write' if attr in WRITE_POS else 'read' if attr in READ_POS else None
        if not kind:return
        if kind=='write':self.visited_write_lines.add(node.lineno)
        keynode=next((kw.value for kw in node.keywords if kw.arg=='Key'),None)
        unpacked={}
        for kw in node.keywords:
            if kw.arg is None:
                mapping=self.resolve(kw.value,env)
                if isinstance(mapping,dict):unpacked.update(mapping)
        keyvalue=unpacked.get('Key') if keynode is None else self.resolve(keynode,env)
        pos=(WRITE_POS if kind=='write' else READ_POS)[attr]
        if keynode is None and keyvalue is None and pos is not None and len(node.args)>pos:keynode=node.args[pos];keyvalue=self.resolve(keynode,env)
        if keynode is None and keyvalue is None:
            # boto3 keyword-only Key binding is intentionally not guessed from Bucket/Body.
            if kind=='write':self.unresolved.append({'line':node.lineno,'operation':attr,'reason':'unresolved key argument'})
            return
        key=normalise(keyvalue)
        if key:
            (self.writes if kind=='write' else self.reads).add(key)
            if kind=='write':self.proofs.setdefault(key,set()).add(node.lineno)
        elif kind=='write':
            if isinstance(keyvalue,str) and re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_./=*\-]*\.(?:md|txt|html|csv|parquet|jsonl|ndjson|sqlite|db|zip|png|pdf)(?:\.gz)?',keyvalue):self.other_writes.append({'line':node.lineno,'operation':attr,'key':keyvalue,'classification':'dynamic_non_json_output' if '*' in keyvalue else 'non_json_output'})
            else:self.unresolved.append({'line':node.lineno,'operation':attr,'reason':'dynamic or unresolved key'})

    def expr(self,node,env,stack):
        if isinstance(node,ast.Call):self.call(node,env,stack)
        for child in ast.iter_child_nodes(node):
            if not isinstance(child,(ast.FunctionDef,ast.AsyncFunctionDef,ast.Lambda)):self.expr(child,env,stack)

    def block(self,nodes,env,stack):
        for n in nodes:
            if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)):
                if stack:env['__jh_local_functions']={**(env.get('__jh_local_functions') or {}),n.name:FunctionBinding(n,env)}
                continue
            if isinstance(n,ast.ClassDef):continue
            self.assign(n,env)
            if isinstance(n,ast.If):
                self.expr(n.test,env,stack)
                branches=[]
                for body in (n.body,n.orelse):
                    branch=dict(env);self.block(body,branch,stack);branches.append(branch)
                for key in set().union(*branches):
                    left,right=branches[0].get(key),branches[1].get(key)
                    if isinstance(left,dict) and isinstance(right,dict):env[key]={k:left.get(k) if left.get(k)==right.get(k) else None for k in set(left)|set(right)}
                    else:env[key]=left if left==right else None
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

    def run(self,entrypoint=None):
        self.block(self.tree.body,dict(self.globals),())
        # Only externally callable roots receive unknown arguments. A helper called
        # with concrete arguments is not re-invoked with fabricated unknown inputs.
        roots=[entrypoint] if entrypoint in self.functions else [name for name in ('lambda_handler','handler','main') if name in self.functions]
        external_roots=set(roots)
        referenced={n.func.id for n in ast.walk(self.tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name)}
        # Dispatch-table targets can be referenced as values, so retain their
        # source-bound writes; do not invent a second invocation of called helpers.
        roots+= [name for name in self.functions if name not in roots and name not in referenced and name not in self.called_functions]
        for name in roots:
            if name in self.called_functions and name not in external_roots:continue
            fn=self.functions[name];env=dict(self.globals)
            for a in fn.args.posonlyargs+fn.args.args+fn.args.kwonlyargs:env[a.arg]=None
            self.block(fn.body,env,(name,))
        for node in ast.walk(self.tree):
            if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute) and node.func.attr in WRITE_POS and node.lineno not in self.visited_write_lines:
                self.unresolved.append({'line':node.lineno,'operation':node.func.attr,'reason':'write site outside analyzed entrypoint call graph'})
        return self

def scan_code(code,environment=None,entrypoint=None,initial_symbols=None):
    return Scan(code,environment,initial_symbols).run(entrypoint)

def imported_symbols(source,roots,environment=None,stack=(),cache=None):
    """Resolve checked-in module constants through AST only; never import code.

    Function calls across modules remain unresolved. Exact symbols can still bind
    an actual caller write such as S3Store.put_json(STATE_KEY, payload).
    """
    source=source.resolve()
    if cache is None:cache={}
    if source in stack:return {}
    try:tree=ast.parse(source.read_text(errors='replace'))
    except (OSError,SyntaxError):return {}
    symbols={}
    def module_values(module,level=0):
        parts=(module or '').split('.') if module else []
        bases=[source.parent] if level else [source.parent,*roots]
        if level>1:bases=[source.parents[level-1]]
        for base in bases:
            path=base.joinpath(*parts).with_suffix('.py') if parts else base/'__init__.py'
            if not path.is_file():path=base.joinpath(*parts)/'__init__.py'
            if path.is_file() and any(path.resolve().is_relative_to(root.resolve()) for root in roots):
                resolved=path.resolve()
                if resolved in cache:return cache[resolved]
                if resolved in stack+(source,):return {}
                try:
                    values=Scan(path.read_text(errors='replace'),environment,imported_symbols(path,roots,environment,stack+(source,),cache)).globals
                    cache[resolved]=values
                    return values
                except SyntaxError:return {}
        return {}
    for node in tree.body:
        if isinstance(node,ast.ImportFrom):
            values=module_values(node.module,node.level)
            for alias in node.names:
                if alias.name!='*' and alias.name in values:symbols[alias.asname or alias.name]=values[alias.name]
        elif isinstance(node,ast.Import):
            for alias in node.names:
                # Dotted imports without aliases need a package graph, not a
                # guessed flat mapping; keep those unresolved.
                if alias.asname or '.' not in alias.name:symbols[alias.asname or alias.name]=module_values(alias.name)
    return symbols

def ast_keys(code):
    try:s=scan_code(code);return sorted(s.writes),sorted(s.reads),True
    except SyntaxError:return [],[],False

def confirmed_write_keys(code):
    """Compatibility API. Never fall back to the unsafe window scanner."""
    return ast_keys(code)[0]

def build(root=ROOT):
    engines=[]
    for d in sorted((root/'aws/lambdas').iterdir()):
        if not d.is_dir() or d.name.startswith('_') or not (d/'source').is_dir():continue
        cfg={}
        try:cfg=json.loads((d/'config.json').read_text())
        except (OSError,ValueError):pass
        env=cfg.get('environment') or {}; env=env.get('Variables',env) if isinstance(env,dict) else {}
        keys,reads,proofs,unresolved,defaults=set(),set(),{},[],{};output_roles=[];other_writes=[]
        handler=cfg.get('handler') or cfg.get('Handler')
        runtime=cfg.get('runtime') or cfg.get('Runtime')
        module=str(handler or '').rsplit('.',1)[0].replace('.','/')
        entrypoint=next((module+ext for ext in ('.py','.js','.mjs','.cjs') if module and (d/'source'/str(module+ext)).is_file()),None)
        if handler and entrypoint is None:unresolved.append({'file':'config.json','reason':'configured handler module missing from source'})
        unsupported=[str(p.relative_to(d/'source')) for p in (d/'source').rglob('*') if p.suffix in ('.js','.mjs','.cjs','.ts') and 'node_modules' not in p.parts]
        for src in unsupported:unresolved.append({'file':src,'reason':'unsupported runtime analysis; API and dynamic outputs require explicit runtime contract'})
        constant_cache={}
        for src in sorted((d/'source').rglob('*.py')):
            if '__pycache__' in src.parts:continue
            rel=str(src.relative_to(d/'source'))
            try:
                symbols=imported_symbols(src,[d/'source',root/'aws/shared'],env,cache=constant_cache)
                s=scan_code(src.read_text(errors='replace'),env,str(handler).rsplit('.',1)[-1] if rel==entrypoint else None,symbols);keys.update(s.writes);reads.update(s.reads);defaults.update(s.defaults)
                for key,lines in s.proofs.items():proofs.setdefault(key,[]).extend({'file':rel,'line':line} for line in sorted(lines))
                for declaration in s.tree.body:
                    if not isinstance(declaration,ast.Assign) or not any(isinstance(t,ast.Name) and t.id=='OUTPUT_OWNERSHIP' for t in declaration.targets) or not isinstance(declaration.value,ast.Dict):continue
                    role={}
                    for key_node,value_node in zip(declaration.value.keys,declaration.value.values):
                        try:name=ast.literal_eval(key_node)
                        except (ValueError,TypeError):continue
                        value=s.globals.get(value_node.id) if isinstance(value_node,ast.Name) else ast.literal_eval(value_node)
                        role[name]=value
                    key=role.get('key');lines=s.proofs.get(key,set())
                    writes=[n for n in ast.walk(s.tree) if isinstance(n,ast.Call) and n.lineno in lines and isinstance(n.func,ast.Attribute) and n.func.attr=='put_object']
                    cas=bool(writes) and all(any(kw.arg=='IfMatch' for kw in n.keywords) for n in writes)
                    if key in s.writes and role.get('role')=='augmentation' and role.get('base_producer') and role.get('compare_and_swap') is True and cas:
                        output_roles.append({**role,'cas_write_verified':True,'source':rel,'declaration_line':declaration.lineno})
                    else:raise ValueError('Unproven output augmentation contract: '+d.name+' '+str(key))
                unresolved.extend(dict(x,file=rel) for x in s.unresolved);other_writes.extend(dict(x,file=rel) for x in s.other_writes)
            except SyntaxError as exc:unresolved.append({'file':rel,'line':exc.lineno,'reason':'parse failure'})
        # Resolve parameter-only reports only when that exact write site has a concrete or family binding.
        proven={(x['file'],x['line']) for values in proofs.values() for x in values}
        unresolved=[dict(t) for t in sorted({tuple(sorted(x.items())) for x in unresolved})]  # A resolved invocation must not hide another unresolved invocation at the same write site.
        exact=sorted(k for k in keys if '*' not in k);patterns=sorted(k for k in keys if '*' in k)
        engines.append({'engine':d.name,'keys':exact,'n_keys':len(exact),'key_patterns':patterns,
                        'reads':sorted(reads),'other_format_outputs':other_writes,'output_roles':output_roles,'method':'ast-call-binding-v3','write_evidence':proofs,
                        'unresolved_writes':unresolved,'environment_key_defaults':defaults,
                        'ownership_status':'incomplete' if unresolved else 'source_bound',
                        'deployment_overrides_verified':False,'configured_handler':handler,'runtime':runtime,'entrypoint_source':entrypoint,'entrypoint_verified':bool(entrypoint),'entrypoint_status':'CONFIGURED_SOURCE_PRESENT' if entrypoint else 'CONFIGURED_SOURCE_MISSING' if handler else 'DEPLOYMENT_CONFIG_NOT_RECORDED','analysis_scope':'Python source writes; API response bodies and unsupported runtimes require separate contracts','description':str(cfg.get('description') or '')[:140]})
    return {'schema_version':'engine-manifest.v3','generated_at':datetime.now(timezone.utc).isoformat(),
            'source':'scripts/gen_engine_manifest.py; source-bound outputs, separate families and unresolved writes; runtime not certified',
            'n_engines':len(engines),'engines':engines}

def main():
    doc=build();path=ROOT/'engine-manifest.json';tmp=path.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(doc,separators=(',',':')));os.replace(tmp,path)
    print(f"[manifest] {len(doc['engines'])} engines; {sum(bool(e['keys']) for e in doc['engines'])} with concrete outputs; unresolved explicitly reported")
if __name__=='__main__':main()
