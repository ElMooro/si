"""A bundled policy file must be byte-identical to its config/ twin (2026-09-17).

Engines such as justhodl-engine-fusion, justhodl-jh-fusion and justhodl-jhsignal-bridge
read their registry / policy JSON from their own source/ dir, while config/<name> is the
reviewed copy every lane edits. Commit 002bbd1e edited both copies of
fusion-registry.v1.json in one commit and left them different (best_setups_view
evidence_level L2 in config/, L3 in the bundle) -- the deploy went green and AWS ran the
bundle, so the reviewed copy silently stopped describing what was live.

Rule: for every aws/lambdas/<fn>/source/<name>.json that also exists as config/<name>,
the two files are byte-identical. When DEPLOY_TARGETS (space-separated function names)
is set by the preflight step, only the functions being deployed are checked, so one
lane's mid-edit drift never blocks another lane's release; unset (local run) checks all.
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LAMBDAS = ROOT / "aws" / "lambdas"
CONFIG = ROOT / "config"


def bundled_pairs(targets: set[str] | None = None) -> list[tuple[str, str, Path, Path]]:
    pairs = []
    for src in sorted(LAMBDAS.glob("*/source/*.json")):
        fn = src.parts[-3]
        if targets is not None and fn not in targets:
            continue
        twin = CONFIG / src.name
        if twin.is_file():
            pairs.append((fn, src.name, src, twin))
    return pairs


def selected_targets() -> set[str] | None:
    raw = os.environ.get("DEPLOY_TARGETS", "").split()
    return set(raw) if raw else None


def test_bundled_policy_files_match_their_config_twins():
    drift = []
    for fn, name, src, twin in bundled_pairs(selected_targets()):
        if src.read_bytes() != twin.read_bytes():
            drift.append(f"{fn}/source/{name} != config/{name}")
    assert not drift, (
        "bundled copy differs from config/ twin -- copy the reviewed file over the bundle "
        "(or the bundle over config/) in the SAME commit, byte for byte: " + "; ".join(drift)
    )


def test_gate_covers_the_known_mirrored_engines():
    covered = {fn for fn, _, _, _ in bundled_pairs()}
    for fn in ("justhodl-engine-fusion", "justhodl-jh-fusion", "justhodl-jhsignal-bridge"):
        assert fn in covered, f"{fn} no longer has a config/ twin for any bundled JSON"


def test_target_scoping_only_narrows_never_widens():
    everything = {(fn, name) for fn, name, _, _ in bundled_pairs()}
    scoped = {(fn, name) for fn, name, _, _ in bundled_pairs({"justhodl-engine-fusion"})}
    assert scoped <= everything
    assert all(fn == "justhodl-engine-fusion" for fn, _ in scoped)
    assert bundled_pairs({"no-such-function"}) == []
