"""Route automated callers through validated releases, never staged $LATEST.

Keep this allowlist in sync with release_validation configuration. Explicit
numbered versions and named aliases are preserved; $LATEST is not a release.
"""
GOVERNED_FUNCTIONS = frozenset({
    "justhodl-backtest-engine", "justhodl-calibration-snapshotter",
    "justhodl-engine-fusion", "justhodl-katlin", "justhodl-khalid", "justhodl-khalid-risk",
    "justhodl-portfolio-snapshot", "justhodl-research-backtest",
    "justhodl-risk-gate", "justhodl-risk-sizer",
})


def function_identity(target):
    """Unqualified function name for catalog/feed lookup only."""
    return target.split(":function:")[-1].split(":")[0]


def governed_target(target):
    """Preserve explicitly pinned releases; qualify governed bare names/ARNs."""
    identity = target.split(":function:")[-1]
    name, separator, qualifier = identity.partition(":")
    if name not in GOVERNED_FUNCTIONS or (separator and qualifier != "$LATEST"):
        return target
    return (target[:-8] if target.endswith(":$LATEST") else target) + ":live"
