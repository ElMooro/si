from pathlib import Path
import ast
import importlib.util
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]


def test_each_options_package_uses_its_own_intended_receipt():
    path=ROOT/'aws/ops/staged/ops_6318_options_normal_publication.py'
    if not path.exists():path=ROOT/'aws/ops/STAGED/ops_6318_options_normal_publication.py'
    spec=importlib.util.spec_from_file_location('options_separate_releases',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    actual={fn:object() for fn in module.EXPECTED};baselines={fn:object() for fn in module.EXPECTED}
    with patch.object(module.acceptance,'check_runtime') as checker:
        module.check_packages(actual,baselines)
        assert checker.call_count==4
        for (fn,sha),call in zip(module.EXPECTED.items(),checker.call_args_list):
            assert call.args==(actual[fn],baselines[fn],sha,module.COUNTS[fn])
    bad=actual.copy();bad.pop(next(iter(bad)))
    try:module.check_packages(bad,baselines)
    except ValueError:pass
    else:raise AssertionError('Missing consumer silently accepted')
    source=path.read_text(encoding='utf-8');tree=ast.parse(source)
    calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls&{'invoke','put_object','get_secret_value','get_parameter','update_schedule','put_rule'}
    assert 'sys.exit(1)' in source
