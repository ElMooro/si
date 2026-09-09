"""Bounded source-owned contract overrides; never writes a remote registry."""
from copy import deepcopy
from datetime import datetime
import json
import math
from pathlib import Path


def load_overlay():
    document = json.loads(Path(__file__).with_name("reviewed_contracts.json").read_text())
    if document.get("version") != "namespace-audit-2026-09-09.v1" or not isinstance(document.get("entries"), dict):
        raise ValueError("reviewed contract overlay is invalid")
    return document


def _registry(registry, field):
    if not isinstance(registry, dict) or not isinstance(registry.get(field, {}), dict):
        raise ValueError("contract registry shape is invalid")
    result = deepcopy(registry)
    result.setdefault(field, {})
    return result


def apply_contracts(registry, overlay=None):
    overlay = load_overlay() if overlay is None else overlay
    result = _registry(registry, "contracts")
    for key, entry in overlay["entries"].items():
        if "contract" in entry:
            result["contracts"][key] = deepcopy(entry["contract"])
    for key in overlay.get("retired", {}):
        result["contracts"].pop(key, None)
    result["n_contracts"] = len(result["contracts"])
    result["source_overlay"] = {"version": overlay["version"], "mode": "IN_MEMORY_SOURCE_OVERLAY",
                                "reviewed_keys": sorted(overlay["entries"]),
                                "retired_keys": sorted(overlay.get("retired", {}))}
    return result


def apply_producers(registry, overlay=None):
    overlay = load_overlay() if overlay is None else overlay
    result = _registry(registry, "producers")
    for key, entry in overlay["entries"].items():
        old = result["producers"].get(key)
        record = deepcopy(old) if isinstance(old, dict) else {"readers": [], "mentions": []}
        record.update({"writers": list(entry["writers"]), "authoritative_writers": True,
                       "writer_source": overlay["version"]})
        if entry.get("writer_relationship"):
            record["writer_relationship"] = entry["writer_relationship"]
        if entry.get("visibility"):
            record["visibility"] = entry["visibility"]
        result["producers"][key] = record
    for key, entry in overlay.get("retired", {}).items():
        result["producers"][key] = {"writers": [], "readers": [], "mentions": [],
                                    "authoritative_writers": True, "retired": deepcopy(entry),
                                    "writer_source": overlay["version"]}
    result["n_mapped"] = len(result["producers"])
    return result


def artifact_function_name(arn):
    """Resolve function identity without mistaking :live or :17 for its name."""
    if not isinstance(arn, str):
        return None
    value = arn.split(":function:", 1)[-1]
    return value.split(":", 1)[0] if value and not value.startswith("arn:") else None


def _field(document, path):
    if path == "$":
        return document
    value = document
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


def validate_fields(document, contract):
    """Return field identifiers only; private/provider values never enter reports."""
    errors = []
    def timestamp(value):
        try:
            return isinstance(value, str) and datetime.fromisoformat(value.replace("Z", "+00:00")).tzinfo is not None
        except (ValueError, TypeError):
            return False
    tests = {"object": lambda v: isinstance(v, dict), "array": lambda v: isinstance(v, list),
             "string": lambda v: isinstance(v, str), "integer": lambda v: type(v) is int,
             "number": lambda v: type(v) in (int, float) and math.isfinite(v),
             "boolean": lambda v: type(v) is bool, "null": lambda v: v is None, "timestamp": timestamp}
    for field, expected in contract.get("required_types", {}).items():
        kinds = expected if isinstance(expected, list) else [expected]
        if not any(kind in tests and tests[kind](_field(document, field)) for kind in kinds):
            errors.append(("FIELD_TYPE", "source contract type mismatch: " + field))
    for field, expected in contract.get("required_values", {}).items():
        actual = _field(document, field)
        if type(actual) is not type(expected) or actual != expected:
            errors.append(("FIELD_VALUE", "source contract identity/status mismatch: " + field))
    return errors
