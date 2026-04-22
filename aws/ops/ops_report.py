"""
Shared helper for ops scripts. Every script that imports this module gets:

  - A `report()` context manager that captures structured output
  - Automatic write-back to aws/ops/reports/latest/<script-name>.md
  - Automatic append to $GITHUB_STEP_SUMMARY

Use pattern:

    from ops_report import report
    with report("phase-4-dependency-tracer") as r:
        r.heading("Phase 4 — Dependency tracer")
        r.log("Starting scan…")
        r.row(fn="justhodl-morning-intel", status="traced", callers=3)
        r.log("Done")
"""

import json
import os
import sys
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


class Report:
    def __init__(self, name: str):
        self.name = name
        self.lines = []
        self.rows = []
        self.status = "running"
        self.error = None
        self._start = datetime.now(timezone.utc)

    def heading(self, text: str):
        self.lines.append(f"# {text}")
        self.lines.append("")
        print(text, flush=True)

    def section(self, text: str):
        self.lines.append(f"## {text}")
        self.lines.append("")
        print(f"── {text} ──", flush=True)

    def log(self, msg: str):
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        self.lines.append(f"- `{ts}` {msg}")
        print(f"[{ts}] {msg}", flush=True)

    def ok(self, msg: str):
        self.log(f"✅ {msg}")

    def warn(self, msg: str):
        self.log(f"⚠ {msg}")

    def fail(self, msg: str):
        self.log(f"✗ {msg}")

    def kv(self, **kwargs):
        """Record structured key/value pairs (will render as a table later)."""
        self.rows.append(kwargs)

    def render(self) -> str:
        end = datetime.now(timezone.utc)
        duration = (end - self._start).total_seconds()
        meta = [
            f"**Status:** {self.status}  ",
            f"**Duration:** {duration:.1f}s  ",
            f"**Finished:** {end.isoformat(timespec='seconds')}  ",
            "",
        ]

        if self.error:
            meta.extend(["## Error", "", "```", self.error, "```", ""])

        if self.rows:
            keys = sorted({k for row in self.rows for k in row.keys()})
            meta.append("## Data")
            meta.append("")
            meta.append("| " + " | ".join(keys) + " |")
            meta.append("|" + "|".join(["---"] * len(keys)) + "|")
            for row in self.rows:
                meta.append("| " + " | ".join(str(row.get(k, "")) for k in keys) + " |")
            meta.append("")

        meta.append("## Log")
        meta.append("")

        return "\n".join(self.lines[:2]) + "\n" + "\n".join(meta) + "\n".join(self.lines[2:]) + "\n"


@contextmanager
def report(name: str):
    r = Report(name)
    try:
        yield r
        r.status = "success"
    except SystemExit as e:
        # Preserve exit code semantics
        r.status = "failure" if e.code not in (0, None) else "success"
        r.error = f"SystemExit: {e}"
        raise
    except Exception:
        r.status = "failure"
        r.error = traceback.format_exc()
        raise
    finally:
        repo_root = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
        latest_dir = repo_root / "aws" / "ops" / "reports" / "latest"
        latest_dir.mkdir(parents=True, exist_ok=True)
        latest_file = latest_dir / f"{name}.md"
        latest_file.write_text(r.render(), encoding="utf-8")
        print(f"\n→ Report written to {latest_file.relative_to(repo_root)}", flush=True)

        # Also append to GitHub step summary
        gh = os.environ.get("GITHUB_STEP_SUMMARY")
        if gh:
            with open(gh, "a") as f:
                f.write(r.render())
