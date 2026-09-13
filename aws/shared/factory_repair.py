"""A finite program-search problem tied to a real deployment defect.

Programs are typed descriptors, never caller-supplied Python. The interpreter
and renderer accept exactly the same closed vocabulary. General code work is
reported blocked until a compatible model and isolated executor are available.
"""
import re

from factory_core import Invalid, digest

PROBLEM = "deployment-create-identity.v1"
DEFAULT_ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
DEFAULT_HANDLER = "lambda_function.lambda_handler"


def validate_program(program):
    if not isinstance(program, dict) or set(program) != {"problem", "role", "handler", "validate"}:
        raise Invalid("invalid_repair_program")
    if program["problem"] != PROBLEM or program["role"] not in ("default", "configured") or program["handler"] not in ("default", "configured") or type(program["validate"]) is not bool:
        raise Invalid("repair_program_not_allowed")
    return program


def candidates():
    return [{"problem": PROBLEM, "role": role, "handler": handler, "validate": validate}
            for role in ("default", "configured") for handler in ("default", "configured") for validate in (False, True)]


def execute(program, configuration):
    validate_program(program)
    if not isinstance(configuration, dict):
        raise ValueError("lambda_identity_configuration_required")
    role = configuration.get("role", DEFAULT_ROLE) if program["role"] == "configured" else DEFAULT_ROLE
    handler = configuration.get("handler", DEFAULT_HANDLER) if program["handler"] == "configured" else DEFAULT_HANDLER
    if program["validate"]:
        if not isinstance(role, str) or not re.fullmatch(r"arn:aws:iam::857687956942:role/[A-Za-z0-9+=,.@_/-]{1,512}", role):
            raise ValueError("lambda_execution_role_invalid")
        if not isinstance(handler, str) or not re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+", handler) or len(handler) > 128:
            raise ValueError("lambda_handler_invalid")
    return {"role": role, "handler": handler}


def render(program):
    validate_program(program)
    role_expr = 'configuration.get("role", DEFAULT_ROLE)' if program["role"] == "configured" else "DEFAULT_ROLE"
    handler_expr = 'configuration.get("handler", DEFAULT_HANDLER)' if program["handler"] == "configured" else "DEFAULT_HANDLER"
    validation = '''    if not isinstance(role, str) or not re.fullmatch(r"arn:aws:iam::857687956942:role/[A-Za-z0-9+=,.@_/-]{1,512}", role):
        raise ValueError("lambda_execution_role_invalid")
    if not isinstance(handler, str) or not re.fullmatch(r"[A-Za-z_]\\w*(?:\\.[A-Za-z_]\\w*)+", handler) or len(handler) > 128:
        raise ValueError("lambda_handler_invalid")
''' if program["validate"] else ""
    return '''"""Resolve a configured Lambda identity before any deployment mutation."""
import re

DEFAULT_ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
DEFAULT_HANDLER = "lambda_function.lambda_handler"


def resolve_identity(configuration):
    if not isinstance(configuration, dict):
        raise ValueError("lambda_identity_configuration_required")
    role = ''' + role_expr + '''
    handler = ''' + handler_expr + '''
''' + validation + '''    return {"role": role, "handler": handler}


if __name__ == "__main__":
    import json
    import sys
    from pathlib import Path
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    configuration = json.loads(path.read_text()) if path and path.is_file() else {}
    print(json.dumps(resolve_identity(configuration), sort_keys=True))
'''


def public_development_cases():
    return [({}, {"role": DEFAULT_ROLE, "handler": DEFAULT_HANDLER}),
            ({"role": "arn:aws:iam::857687956942:role/restricted-student"}, {"role": "arn:aws:iam::857687956942:role/restricted-student", "handler": DEFAULT_HANDLER}),
            ({"handler": "student_lambda.lambda_handler"}, {"role": DEFAULT_ROLE, "handler": "student_lambda.lambda_handler"}),
            ({"role": "invalid"}, "reject"), ({"handler": None}, "reject")]


def score_cases(program, cases):
    passed, failures = 0, []
    for index, (inputs, expected) in enumerate(cases):
        try:
            answer = execute(program, inputs)
        except ValueError:
            answer = "reject"
        if answer == expected:
            passed += 1
        else:
            failures.append(index)
    return {"n": len(cases), "passed": passed, "score": passed / len(cases), "critical_failures": len(failures)}


def propose():
    ranked = sorted(((score_cases(program, public_development_cases())["score"], digest(program), program)
                     for program in candidates()), key=lambda item: (-item[0], item[1]))
    best = ranked[0][2]
    return {"problem": PROBLEM, "program": best, "program_hash": digest(best),
            "generator": "finite_program_search", "model_revision": None,
            "development": score_cases(best, public_development_cases()),
            "candidates_considered": len(ranked), "source": render(best),
            "scope": "configured-role-and-handler repair; not general autonomous coding"}
