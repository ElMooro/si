"""Exact allowed OOS edit checks for older whole-function preservation tests."""
import ast


def dump(node):
    return ast.dump(node, include_attributes=False)


def expression(source):
    return ast.parse(source, mode='eval').body


def checked_dictionary(case, old, new, changes, additions):
    old_map = {k.value:v for k,v in zip(old.keys,old.values)}
    new_map = {k.value:v for k,v in zip(new.keys,new.values)}
    case.assertEqual(set(new_map), set(old_map) | set(additions))
    for key,value in old_map.items():
        case.assertEqual(dump(new_map[key]), dump(changes.get(key,value)), key)
    for key,value in additions.items():
        case.assertEqual(dump(new_map[key]),dump(value),key)


def assert_oos_only_change(case, old, new):
    """Preserve all statements/fields except the explicitly reviewed OOS slice."""
    if old.name == 'run_backtest':
        start = next(i for i,n in enumerate(old.body) if isinstance(n,ast.FunctionDef) and n.name=='predict')
        old_doc = next(i for i,n in enumerate(old.body) if isinstance(n,ast.Assign)
                       and any(isinstance(t,ast.Name) and t.id=='doc' for t in n.targets))
        new_doc = next(i for i,n in enumerate(new.body) if isinstance(n,ast.Assign)
                       and any(isinstance(t,ast.Name) and t.id=='doc' for t in n.targets))
        case.assertEqual([dump(n) for n in old.body[:start]], [dump(n) for n in new.body[:start]])
        expected = ast.parse('''feature_stats = {"%ds" % hz: fit(obs, hz) for hz in (63, 126, 252)}
split_i = date_idx[int(len(date_idx) * 0.6)] if len(date_idx) >= 5 else None
oos_validation = oos_boundary_evidence(obs, split_i, dates)
oos = {}
''').body
        case.assertEqual([dump(n) for n in new.body[start:new_doc]],[dump(n) for n in expected])
        old_value = old.body[old_doc].value
        old_note = next(v.value for k,v in zip(old_value.keys,old_value.values) if k.value=='note')
        ending = 'oos = the prior fitted on the first 60% of dates and scored on the last 40% (decile spread, correlation).'
        case.assertTrue(old_note.endswith(ending))
        new_note = old_note[:-len(ending)] + 'OOS statistics withheld: label availability and decision instants are not retained. Full-history priors are descriptive, not OOS evidence.'
        checked_dictionary(case,old_value,new.body[new_doc].value,{'note':ast.Constant(new_note)},
                           {'oos_validation':ast.Name(id='oos_validation',ctx=ast.Load())})
        case.assertEqual([dump(n) for n in old.body[old_doc+1:]], [dump(n) for n in new.body[new_doc+1:]])
        case.assertEqual(dump(old.args),dump(new.args))
    elif old.name == 'validation_summary':
        case.assertEqual([dump(n) for n in old.body[:-1]],[dump(n) for n in new.body[:-1]])
        checked_dictionary(case,old.body[-1].value,new.body[-1].value,
            {'oos':expression('{}'),
             'note':expression('(bt.get("note") or "") + " OOS validation unavailable: retained label availability and historical decision instants are required; legacy OOS metrics are withheld."')},
            {'oos_validation':expression('bt.get("oos_validation") or oos_boundary_evidence([], None, [])')})
        case.assertEqual(dump(old.args),dump(new.args))
    else:
        raise AssertionError('Unexpected exception to predecessor preservation')
