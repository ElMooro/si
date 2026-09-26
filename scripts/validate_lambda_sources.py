"""Reject incomplete selected Python entrypoints before any AWS operation.

This is deliberately static: importing an engine can acquire data or mutate an
account. Syntax alone is insufficient because a bare PLACEHOLDER compiles.
"""
import ast
import json
import os
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from guard_stub_lambdas import MIN_BYTES, SHRINK_FRACTION, SHRINK_MIN_PREV, previous_size, shrink_override
from lambda_identity import resolve_identity


def callable_bindings(tree):
    """Resolve ordinary module-level function/import aliases without execution.

    Conditional definitions and dynamic getattr/exec exports require an explicit
    module-level wrapper. An imported binding is structurally allowed, not a
    claim that its dependency or runtime behavior has been tested.
    """
    names = set()
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            names.add(node.name)
        elif isinstance(node, ast.ImportFrom):
            names.update(alias.asname or alias.name for alias in node.names if alias.name != '*')
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            value = node.value
            valid = isinstance(value, ast.Lambda) or (isinstance(value, ast.Name) and value.id in names)
            for target in targets:
                if isinstance(target, ast.Name):
                    if valid: names.add(target.id)
                    else: names.discard(target.id)
        elif isinstance(node, (ast.AsyncFunctionDef, ast.ClassDef)):
            names.discard(node.name)
        elif isinstance(node, ast.Delete):
            for target in node.targets:
                if isinstance(target, ast.Name): names.discard(target.id)
    return names


def validate_sources(root, targets, base=None):
    root = Path(root).resolve()
    base = base or os.environ.get('GUARD_BASE_SHA') or 'HEAD~1'
    override = shrink_override(root, None if base == 'HEAD~1' else base)
    errors = []
    for target in targets:
        def fail(code, **details):
            errors.append({'function': target, 'error_code': code, **details})
        if not re.fullmatch(r'[A-Za-z0-9_-]+', target):
            fail('invalid_target'); continue
        folder = root/'aws/lambdas'/target
        try:
            config = json.loads((folder/'config.json').read_bytes()) if (folder/'config.json').is_file() else {}
            identity = resolve_identity(config)
            if not str(config.get('runtime', 'python3.12')).startswith('python'):
                fail('unsupported_runtime_requires_source_validator'); continue
            module, handler = identity['handler'].rsplit('.', 1)
            path = folder/'source'/Path(*module.split('.')).with_suffix('.py')
            if not path.is_file():
                fail('handler_source_missing'); continue
            raw = path.read_bytes().replace(b'\r\n', b'\n')
            if len(raw) < MIN_BYTES:
                fail('stub_body', bytes=len(raw), minimum_bytes=MIN_BYTES); continue
            tree = ast.parse(raw, filename=path.name)
            # compile catches errors ast.parse permits, such as return outside a function.
            compile(tree, path.name, 'exec')
            if handler not in callable_bindings(tree):
                fail('module_level_callable_handler_required', handler=handler); continue
            previous = previous_size(path.relative_to(root).as_posix(), base, root)
            if previous and previous >= SHRINK_MIN_PREV and len(raw) < previous*(1-SHRINK_FRACTION) and not override:
                fail('unreviewed_source_shrink', previous_bytes=previous, bytes=len(raw))
        except (ValueError, SyntaxError, TypeError, UnicodeError, OSError):
            # Never print source lines, configuration contents or secret values.
            fail('invalid_source_or_configuration')
    return errors


if __name__ == '__main__':
    errors = validate_sources(Path.cwd(), sys.argv[1:])
    print(json.dumps({'check': 'selected_lambda_sources', 'ok': not errors, 'errors': errors}, sort_keys=True))
    sys.exit(1 if errors else 0)
