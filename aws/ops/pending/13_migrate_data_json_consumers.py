#!/usr/bin/env python3
"""
Phase 3b fix — migrate 7 stale consumers from `data.json` to `data/report.json`.

The producer (justhodl-daily-report-v3) stopped writing the bucket-root
`data.json` file back in Feb 2026 when it was refactored to write
`data/report.json` instead. Seven consumers never got updated and have
been reading the orphan file ever since — seeing data that's 60+ days old.

This script:
  1. Edits the 7 Lambda source files in the repo
  2. Self-commits with a message that does NOT contain [skip-deploy]
  3. Pushes directly — triggers deploy-lambdas.yml which redeploys all 7

We can't use the normal run-ops auto-commit because it always stamps
[skip-deploy] on commits (to prevent double-deploys on imports).
"""

import os
import re
import subprocess
import sys
from pathlib import Path

from ops_report import report

REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

STALE_CONSUMERS = [
    "justhodl-ai-chat",
    "justhodl-bloomberg-v8",
    "justhodl-chat-api",
    "justhodl-crypto-intel",
    "justhodl-investor-agents",
    "justhodl-morning-intelligence",
    "justhodl-signal-logger",
]

SUBSTITUTIONS = [
    (re.compile(r"(Key\s*=\s*['\"])data\.json(['\"])"), r"\1data/report.json\2"),
    (re.compile(r"(['\"])data\.json(['\"])"), r"\1data/report.json\2"),
]


def sh(*args, check=True, capture=True):
    """Run a shell command, return (rc, stdout)."""
    result = subprocess.run(
        list(args), cwd=REPO_ROOT,
        capture_output=capture, text=True, check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed: {args}\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
    return result.returncode, (result.stdout or "") + (result.stderr or "")


with report("migrate_data_json_consumers") as r:
    r.heading("Migrate 7 stale consumers: data.json → data/report.json")

    modified_files = []
    summary_rows = []
    for fn_name in STALE_CONSUMERS:
        fn_dir = REPO_ROOT / "aws" / "lambdas" / fn_name / "source"
        if not fn_dir.exists():
            r.fail(f"{fn_name}: source dir missing")
            continue

        py_files = list(fn_dir.rglob("*.py"))
        r.section(f"{fn_name}")

        total_subs = 0
        for py in py_files:
            text = py.read_text(encoding="utf-8", errors="ignore")
            original = text

            # First pass: targeted Key='data.json' replacement
            text, n1 = SUBSTITUTIONS[0][0].subn(SUBSTITUTIONS[0][1], text)

            # Second pass: fallback on any remaining orphan 'data.json' references
            # but only if no 'data/report.json' is there yet (to avoid double-subbing)
            if "'data.json'" in text or '"data.json"' in text:
                text, n2 = SUBSTITUTIONS[1][0].subn(SUBSTITUTIONS[1][1], text)
            else:
                n2 = 0

            file_subs = n1 + n2
            if file_subs > 0:
                py.write_text(text, encoding="utf-8")
                total_subs += file_subs
                rel = py.relative_to(REPO_ROOT)
                modified_files.append(str(rel))
                r.log(f"  ✓ {rel}: {file_subs} substitution(s)")

        if total_subs > 0:
            r.ok(f"{fn_name}: {total_subs} substitution(s)")
            summary_rows.append((fn_name, total_subs, "modified"))
        else:
            r.warn(f"{fn_name}: no substitutions (already migrated?)")
            summary_rows.append((fn_name, 0, "no-op"))

        r.kv(lambda_name=fn_name, substitutions=total_subs,
             status="modified" if total_subs > 0 else "no-op")

    # Commit + push without [skip-deploy] — this triggers deploy-lambdas workflow
    r.section("Self-commit & push (no [skip-deploy] — deploys will trigger)")

    if not modified_files:
        r.warn("Nothing to commit. All consumers might already be migrated.")
        sys.exit(0)

    # Configure git identity in case it's not set yet
    sh("git", "config", "user.name", "github-actions[bot]")
    sh("git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")

    # Rebase to avoid race conditions with other jobs
    rc, out = sh("git", "pull", "--rebase", "origin", "main", check=False)
    r.log(f"  Rebase: rc={rc}")

    # Stage only the modified Lambda source files
    sh("git", "add", *modified_files)

    rc, out = sh("git", "diff", "--cached", "--stat")
    r.log("  Staged diff:")
    for line in out.strip().splitlines()[:15]:
        r.log(f"    {line}")

    # Commit with a descriptive message — NO [skip-deploy]
    modified_lambdas = sorted({f.split("/")[2] for f in modified_files})
    commit_msg = (
        f"fix(data): migrate 7 consumers from orphan data.json to data/report.json\n\n"
        f"justhodl-daily-report-v3 writes to data/report.json (has since ~Feb 2026).\n"
        f"The bucket-root data.json has been an abandoned orphan for 63 days.\n"
        f"These 7 Lambdas were still reading the orphan and showing stale data:\n\n"
        + "\n".join(f"  - {l}" for l in modified_lambdas) + "\n\n"
        "This commit deploys all 7 via deploy-lambdas.yml automatically."
    )
    sh("git", "commit", "-m", commit_msg)

    # Push
    rc, out = sh("git", "push", "origin", "main", check=False)
    if rc != 0:
        # Rebase and retry once
        sh("git", "pull", "--rebase", "origin", "main", check=False)
        sh("git", "push", "origin", "main")
    r.ok(f"  Pushed. deploy-lambdas.yml will redeploy {len(modified_lambdas)} Lambda(s)")

    r.log("")
    r.log("Next steps (automatic):")
    r.log("  - deploy-lambdas workflow detects changes in 7 source/ dirs")
    r.log("  - Each Lambda gets re-zipped and redeployed (~30s each)")
    r.log("  - After ~3 min: ai-chat and morning-intel stop showing [REGIME]/[DATA]")
    r.log("Done")

