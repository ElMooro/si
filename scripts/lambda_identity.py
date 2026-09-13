"""Resolve a configured Lambda identity before any deployment mutation."""
import re

DEFAULT_ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
DEFAULT_HANDLER = "lambda_function.lambda_handler"


def resolve_identity(configuration):
    if not isinstance(configuration, dict):
        raise ValueError("lambda_identity_configuration_required")
    role = configuration.get("role", DEFAULT_ROLE)
    handler = configuration.get("handler", DEFAULT_HANDLER)
    if not isinstance(role, str) or not re.fullmatch(r"arn:aws:iam::857687956942:role/[A-Za-z0-9+=,.@_/-]{1,512}", role):
        raise ValueError("lambda_execution_role_invalid")
    if not isinstance(handler, str) or not re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+", handler) or len(handler) > 128:
        raise ValueError("lambda_handler_invalid")
    return {"role": role, "handler": handler}


if __name__ == "__main__":
    import json
    import sys
    from pathlib import Path
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    configuration = json.loads(path.read_text()) if path and path.is_file() else {}
    print(json.dumps(resolve_identity(configuration), sort_keys=True))
