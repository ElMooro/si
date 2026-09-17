#!/usr/bin/env python3
"""Turn a red deploy into a report on main (2026-09-17).

Job logs live on a host most lanes cannot reach (Grok's connector, ChatGPT, Claude's sandbox), so a
failed preflight or deploy step used to be a bare red run: no test name, no traceback. This writes
the failing step's last lines -- redacted -- to

    aws/ops/reports/deploy-failures/<sha7>-<run_id>.md

and the deploy workflow commits it to main ([skip-deploy] [skip-ops]) in its failure step.

Usage (from .github/workflows/deploy-lambdas.yml):
  python3 scripts/deploy_failure_report.py --log "$RUNNER_TEMP/preflight.log" --step "preflight" \\
      --targets "$TARGETS" --commit "$SHA" --run-url "$URL" [--lines 150]

Redaction: the repo's own secret formats (scripts/check_secrets.py FORMATS), AWS secret-key-shaped
40-char runs, bearer tokens, and any key=value where the key smells like a credential. The report
refuses to be written (exit 2) if a known secret format still matches after redaction.
"""
from __future__ import annotations

import argparse
import re
import runpy
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "aws/ops/reports/deploy-failures"
FORMATS = runpy.run_path(str(ROOT / "scripts/check_secrets.py"))["FORMATS"]
EXTRA = [
    re.compile(r"(?<![A-Za-z0-9/+=])[A-Za-z0-9/+=]{40}(?![A-Za-z0-9/+=])"),         # AWS secret access key shape
    re.compile(r"(?i)bearer\s+[A-Za-z0-9._\-/+=]{16,}"),
    re.compile(r"(?i)\b(sk|xox[abp]|glpat|npm)[-_][A-Za-z0-9._\-]{16,}"),
]
KV = re.compile(r"(?i)([A-Za-z0-9_\-]*(?:key|secret|token|password|passwd|credential)[A-Za-z0-9_\-]*\s*[=:]\s*[\"']?)([A-Za-z0-9._\-/+=]{12,})")


def redact(text: str) -> str:
    for _, pat in FORMATS:
        text = pat.sub("[REDACTED]", text)
    for pat in EXTRA:
        text = pat.sub("[REDACTED]", text)
    text = KV.sub(lambda m: m.group(1) + "[REDACTED]", text)
    return text


def still_leaks(text: str) -> bool:
    return any(pat.search(text) for _, pat in FORMATS)


def build(log: str, step: str, targets: str, commit: str, run_url: str, lines: int) -> str:
    tail = log.splitlines()[-lines:]
    body = redact("\n".join(tail))
    failing = [ln for ln in tail if ("::error" in ln or "Error" in ln or "FAIL" in ln or "AssertionError" in ln or "Traceback" in ln)][-12:]
    head = [
        f"# deploy failure -- {commit[:10]} ({step})",
        "",
        f"- commit: `{commit}`",
        f"- run: {run_url}",
        f"- failed step: **{step}**",
        f"- targets: `{targets or '-'}`",
        f"- at: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}",
        "",
        "## Likely cause (lines mentioning an error)",
        "",
        "```",
        redact("\n".join(failing)) if failing else "(no error-shaped line in the tail; read the log below)",
        "```",
        "",
        f"## Last {len(tail)} lines of the {step} step",
        "",
        "```",
        body,
        "```",
        "",
        "Fix the cause and push again; the same commit range redeploys through deploy-lambdas.yml. "
        "Nothing on AWS changed for a preflight failure; a deploy-step failure is fail-hard per function "
        "(see the receipt for what did land).",
        "",
    ]
    return "\n".join(head)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    ap.add_argument("--step", required=True)
    ap.add_argument("--targets", default="")
    ap.add_argument("--commit", required=True)
    ap.add_argument("--run-url", default="")
    ap.add_argument("--run-id", default="")
    ap.add_argument("--lines", type=int, default=150)
    args = ap.parse_args(argv)
    log_path = Path(args.log)
    log = log_path.read_text(errors="replace") if log_path.is_file() else "(no log captured for this step)"
    report = build(log, args.step, args.targets, args.commit, args.run_url, args.lines)
    if still_leaks(report):
        print("::error::deploy_failure_report: a secret-shaped value survived redaction; report NOT written")
        return 2
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / f"{args.commit[:7]}-{args.run_id or 'run'}.md"
    out.write_text(report, encoding="utf-8")
    print(out.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    sys.exit(main())
