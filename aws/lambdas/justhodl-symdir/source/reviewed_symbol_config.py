"""Package-owned routing rules, with no fallback to a denied public object."""
from functools import lru_cache
import copy
import hashlib
import json
from pathlib import Path


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate resolver configuration member")
        result[key] = value
    return result


def validate(document):
    if not isinstance(document, dict) or document.get("schema") != "tv-resolver-1.0":
        raise ValueError("Reviewed resolver schema unavailable")
    for field in ("prefix", "exact", "computed_internals"):
        if not isinstance(document.get(field), dict):
            raise ValueError("Reviewed resolver table unavailable: " + field)
    skipped = document.get("licensed_econ_skip")
    if not isinstance(skipped, list) or not all(isinstance(x, str) and x for x in skipped):
        raise ValueError("Reviewed resolver hold list unavailable")
    for key, row in document["prefix"].items():
        if not isinstance(key, str) or not isinstance(row, dict):
            raise ValueError("Invalid resolver prefix")
        if row.get("engine") not in ("equity", "fred", "yahoo") or type(row.get("strip")) is not bool:
            raise ValueError("Invalid resolver prefix rule")
    for key, row in document["exact"].items():
        if not isinstance(key, str) or not isinstance(row, dict) or not isinstance(row.get("id"), str) or not row["id"]:
            raise ValueError("Invalid exact resolver rule")
        if row["id"] != "SKIP" and row.get("engine") not in ("equity", "fred", "yahoo", "computed"):
            raise ValueError("Invalid exact resolver engine")
    return document


@lru_cache(maxsize=1)
def _load():
    raw = Path(__file__).with_name("tv-symbol-resolver.json").read_bytes()
    if len(raw) > 128000:
        raise ValueError("Reviewed resolver configuration exceeds bound")
    document = validate(json.loads(raw, object_pairs_hook=unique_object))
    return document, hashlib.sha256(raw).hexdigest()


def reviewed_resolver():
    # Callers cannot mutate the immutable package's next request.
    return copy.deepcopy(_load()[0])


def evidence():
    return {"source": "release-package:tv-symbol-resolver.json",
            "sha256": _load()[1], "schema": "tv-resolver-1.0",
            "equivalence_verified": False, "history_verified": False}
