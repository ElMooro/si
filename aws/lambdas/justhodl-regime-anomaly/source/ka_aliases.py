"""
ka_aliases.py — Phase 2 of the Khalid → KA rebrand.

Recursively walks a JSON-serializable dict/list and adds `ka_*` aliases
alongside any existing `khalid_*` keys. Bottom of the recursion handles
nested dicts and lists of dicts.

Why this approach:
- Surgical: producer Lambdas wrap their output with one helper call
  instead of editing every dict-construction site.
- Symmetric: same logic everywhere, easy to audit and reverse.
- Idempotent: calling twice doesn't double-write; if `ka_*` already
  exists we don't overwrite it (consumer or upstream may have set it).
- Phase 6 ready: when we drop khalid_*, just stop calling this helper.

Usage in producer Lambdas:

    from ka_aliases import add_ka_aliases
    ...
    output = {"khalid_index": 42, "regime": "STABLE"}
    output = add_ka_aliases(output)
    # → {"khalid_index": 42, "ka_index": 42, "regime": "STABLE"}

Naming convention applied:
    khalid_index             → ka_index
    khalid_score             → ka_score
    khalid_strategy          → ka_strategy
    khalid_strategy_value_usd → ka_strategy_value_usd
    khalid_return_pct        → ka_return_pct
    khalid_raw               → ka_raw
    khalid_weight            → ka_weight
    khalid_adj               → ka_adj
    khalid_regime            → ka_regime
    khalid_score_at_log      → ka_score_at_log
    khalid_timeline          → ka_timeline
    khalid_component_weights → ka_component_weights
    khalid_new_weights       → ka_new_weights
    KHALID_*  (uppercase variants used in some places) → KA_* (rare, but supported)
"""
from typing import Any


def _alias_key(k: str) -> str:
    """Map khalid_* / KHALID_* key to ka_* / KA_* equivalent. Returns same
    key if no transform applies."""
    if not isinstance(k, str):
        return k
    if k.startswith("khalid_"):
        return "ka_" + k[len("khalid_"):]
    if k.startswith("KHALID_"):
        return "KA_" + k[len("KHALID_"):]
    if k == "khalid":
        return "ka"
    if k == "KHALID":
        return "KA"
    return k


def add_ka_aliases(obj: Any, *, _depth: int = 0, _max_depth: int = 12) -> Any:
    """Recursively add ka_* aliases for every khalid_* key in dicts.

    Walks through:
      - dicts: adds aliased keys at this level, recurses into values
      - lists/tuples: recurses into each element
      - scalars: returned as-is

    Idempotent: if `ka_foo` already exists, we don't overwrite it.

    Returns the same object reference for in-place modification of dicts.
    A copy of input lists is returned because tuple→list mutation isn't
    safe in-place.
    """
    if _depth > _max_depth:
        return obj

    if isinstance(obj, dict):
        # First, recurse into all existing values
        for k, v in list(obj.items()):
            obj[k] = add_ka_aliases(v, _depth=_depth + 1, _max_depth=_max_depth)
        # Then, add ka_ aliases for any khalid_ keys at this level
        for k in list(obj.keys()):
            new_k = _alias_key(k)
            if new_k != k and new_k not in obj:
                obj[new_k] = obj[k]
        return obj

    if isinstance(obj, list):
        return [add_ka_aliases(x, _depth=_depth + 1, _max_depth=_max_depth) for x in obj]

    if isinstance(obj, tuple):
        return tuple(add_ka_aliases(x, _depth=_depth + 1, _max_depth=_max_depth) for x in obj)

    # scalars: int, float, str, bool, None, etc.
    return obj


# Self-test — runs only when invoked directly via `python ka_aliases.py`
if __name__ == "__main__":
    # Test 1: flat dict
    assert add_ka_aliases({"khalid_index": 42}) == {"khalid_index": 42, "ka_index": 42}

    # Test 2: nested dict
    inp = {"scores": {"khalid_index": 50, "crisis_distance": 20}, "regime": "STABLE"}
    out = add_ka_aliases(inp)
    assert out["scores"]["khalid_index"] == 50
    assert out["scores"]["ka_index"] == 50
    assert out["scores"]["crisis_distance"] == 20
    assert out["regime"] == "STABLE"

    # Test 3: list of dicts
    inp = [{"khalid_score": 1}, {"khalid_score": 2}]
    out = add_ka_aliases(inp)
    assert out[0]["ka_score"] == 1
    assert out[1]["ka_score"] == 2

    # Test 4: idempotent (calling twice is safe)
    inp = {"khalid_index": 99, "ka_index": 88}  # ka_ pre-set, must not overwrite
    out = add_ka_aliases(inp)
    assert out["khalid_index"] == 99
    assert out["ka_index"] == 88  # preserved, not overwritten

    out2 = add_ka_aliases(out)
    assert out2 == out

    # Test 5: keys without 'khalid_' prefix are untouched
    inp = {"foo": 1, "bar_baz": 2, "khalid_x": 3}
    out = add_ka_aliases(inp)
    assert out == {"foo": 1, "bar_baz": 2, "khalid_x": 3, "ka_x": 3}

    # Test 6: bare "khalid" → "ka"
    out = add_ka_aliases({"khalid": "label"})
    assert out == {"khalid": "label", "ka": "label"}

    # Test 7: deeply nested
    inp = {"a": {"b": {"c": {"khalid_score": 7}}}}
    out = add_ka_aliases(inp)
    assert out["a"]["b"]["c"]["ka_score"] == 7

    # Test 8: scalars pass through
    assert add_ka_aliases(42) == 42
    assert add_ka_aliases("hello") == "hello"
    assert add_ka_aliases(None) is None

    print("ka_aliases.py — all 8 self-tests passed.")
