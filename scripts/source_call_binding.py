"""AST-only Python call binding; no application import or execution.

Definitely invalid calls produce no body evidence. Unknown expansions preserve
uncertainty without lending caller keyword names to unrelated lexical globals.
"""
import ast


def duplicate_keyword(node, resolve_argument):
    """Return only a proven duplicate; unknown expansions are not inspected."""
    seen=set()
    for keyword in node.keywords:
        values={keyword.arg:None} if keyword.arg is not None else resolve_argument(keyword.value)
        if not isinstance(values,dict):continue
        for name in values:
            if name in seen:return name
            seen.add(name)
    return None


def bind_call(fn, node, resolve_argument, resolve_default, lexical, external=False):
    positional=list(fn.args.posonlyargs)+list(fn.args.args)
    keyword_only=list(fn.args.kwonlyargs)
    posonly={a.arg for a in fn.args.posonlyargs}
    names={a.arg for a in positional+keyword_only}
    bound=dict(lexical)
    for arg in positional+keyword_only:bound[arg.arg]=None
    if fn.args.vararg:bound[fn.args.vararg.arg]=None
    if fn.args.kwarg:bound[fn.args.kwarg.arg]={}
    defaults={a.arg for a in positional[len(positional)-len(fn.args.defaults):]}
    for arg,value in zip(positional[-len(fn.args.defaults):],fn.args.defaults) if fn.args.defaults else ():
        bound[arg.arg]=resolve_default(value)
    for arg,value in zip(keyword_only,fn.args.kw_defaults):
        if value is not None:bound[arg.arg]=resolve_default(value);defaults.add(arg.arg)
    if external:return bound,None

    starred=any(isinstance(arg,ast.Starred) for arg in node.args)
    if not starred and len(node.args)>len(positional) and fn.args.vararg is None:
        return None,'too many positional arguments'
    assigned=set()
    if not starred:
        for arg,value in zip(positional,node.args):bound[arg.arg]=resolve_argument(value);assigned.add(arg.arg)

    unknown_keywords=False;passed={};extras={}
    for keyword in node.keywords:
        if keyword.arg is None:
            values=resolve_argument(keyword.value)
            if not isinstance(values,dict):unknown_keywords=True;continue
        else:values={keyword.arg:resolve_argument(keyword.value)}
        for name,value in values.items():
            if not isinstance(name,str):return None,'keyword name is not a string'
            if name in passed:return None,'duplicate keyword argument: '+name
            passed[name]=value
            if name in posonly:
                if fn.args.kwarg is None:return None,'positional-only argument passed by keyword: '+name
                extras[name]=value
            elif name in names:
                if name in assigned:return None,'multiple values for argument: '+name
                bound[name]=value;assigned.add(name)
            elif fn.args.kwarg is not None:extras[name]=value
            else:return None,'unexpected keyword argument: '+name

    if not starred and not unknown_keywords:
        missing=sorted(names-assigned-defaults)
        if missing:return None,'missing required arguments: '+','.join(missing)
    if starred or unknown_keywords:
        # An unknown expansion could collide with a known argument; it cannot
        # certify a key or lend defaults to an unexamined invocation.
        for arg in positional+keyword_only:bound[arg.arg]=None
    if fn.args.kwarg:bound[fn.args.kwarg.arg]=None if unknown_keywords else extras
    return bound,None
