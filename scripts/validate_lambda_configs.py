#!/usr/bin/env python3
"""Reject invalid selected Lambda metadata before any release mutation."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from normalize_lambda_config import normalize_config
from lambda_architecture import architecture


def validate_configs(root, targets):
    errors = []
    for target in targets:
        path = Path(root) / "aws/lambdas" / target / "config.json"
        if not path.exists():
            continue
        config = json.loads(path.read_text())
        try:
            normalize_config(config)
        except ValueError as error:
            errors.append({"function": target, "field": "schedule", "error_code": str(error)})
        if config.get("update_runtime") is True and (not isinstance(config.get("runtime"), str) or not config["runtime"]):
            errors.append({"function": target, "field": "runtime", "error_code": "explicit_runtime_required"})
        try:
            architecture(config, 'update')
        except ValueError as error:
            errors.append({"function":target,"field":"architectures","error_code":str(error)})
        if "description" in config:
            description = config["description"]
            if not isinstance(description, str) or len(description) > 256:
                errors.append({"function": target, "field": "description", "maximum_characters": 256,
                               "actual_characters": len(description) if isinstance(description, str) else None})
    return errors


if __name__ == "__main__":
    errors = validate_configs(Path.cwd(), sys.argv[1:])
    print(json.dumps({"check": "selected_lambda_configuration", "ok": not errors, "errors": errors}, sort_keys=True))
    sys.exit(1 if errors else 0)
