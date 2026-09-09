#!/usr/bin/env python3
"""Pin a recovery's exact source commit before credentials, tests or deployment."""
from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

WORKFLOW = ".github/workflows/deploy-lambdas.yml"


def pin_checkout(repo, expected, triggering, event_name="workflow_dispatch", base=""):
    repo = Path(repo)
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=repo, stderr=subprocess.DEVNULL, timeout=60)
    def sha(value, name):
        if not re.fullmatch(r"[a-fA-F0-9]{40}", value):
            raise ValueError(name + " must be full40hex")
        return value.lower()
    triggering = sha(triggering, "triggering_sha")
    if git("rev-parse", "HEAD").decode().strip() != triggering:
        raise RuntimeError("Initial checkout does not match the triggering workflow commit")
    if expected:
        expected = sha(expected, "expected_sha")
        if event_name != "workflow_dispatch":
            raise ValueError("Exact recovery checkout requires workflow_dispatch")
        if base:
            base = sha(base, "base_sha")
        # Fetch data arguments, never shell text. Refuse unreachable/divergent
        # commits; a later observer/report commit may advance the same branch.
        refs = list(dict.fromkeys([expected, triggering] + ([base] if base else [])))
        git("fetch", "--quiet", "--no-tags", "--depth=250", "origin", *refs)
        if git("cat-file", "-t", expected).decode().strip() != "commit":
            raise RuntimeError("Expected release object is not a commit")
        try:
            git("merge-base", "--is-ancestor", expected, triggering)
            if base:
                git("merge-base", "--is-ancestor", base, expected)
        except subprocess.CalledProcessError:
            raise RuntimeError("Recovery commits do not form the required ancestor chain") from None
        if git("show", triggering + ":" + WORKFLOW) != git("show", expected + ":" + WORKFLOW):
            raise RuntimeError("Workflow definition changed since the requested release; refusing checkout")
        git("checkout", "--quiet", "--detach", expected)
    release = expected or triggering
    if git("rev-parse", "HEAD").decode().strip() != release:
        raise RuntimeError("Exact release checkout failed")
    return {"release_sha": release, "triggering_sha": triggering, "exact_checkout": True,
            "workflow_definition_equal": True, "recovery": bool(expected)}


if __name__ == "__main__":
    result = pin_checkout(Path.cwd(), os.environ.get("EXPECTED_RELEASE_SHA", ""),
                          os.environ["TRIGGERING_RELEASE_SHA"], os.environ.get("RELEASE_EVENT_NAME", ""),
                          os.environ.get("RECOVERY_BASE_SHA", ""))
    print(json.dumps(result, sort_keys=True))
