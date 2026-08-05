"""aws/shared/fabrication_guard.py — F2 detector + F3 runtime guard (4429).

F2 (static): scan_source() finds the patterns that manufacture fake numbers —
  `x or 0` / `.get("k") or 0.5`   silent literal substitution
  `random.*`                       invented values
  `except: return {}` / `pass`     swallowed failures that become zeros
  hardcoded output literals         placeholder constants shipped as data

F3 (runtime): guard_output() inspects a payload before it is written to S3.
Configurable severity:
  "warn"  — log and publish (default while the fleet migrates)
  "strip" — replace suspicious literals with explicit data_unavailable
  "block" — raise FabricationError, fail loud, emit a CloudWatch metric

The rule this enforces: a page must be able to tell "no data" from "measured
zero". A confident 0 with nothing behind it is worse than a blank.
"""
import json
import re

SUSPECT_LITERALS = {0, 0.0, 0.5, 1.0, -1.0, 100.0, 50.0}

FALLBACK_RE = re.compile(
    r'\.get\(\s*["\'][^"\']+["\']\s*\)\s*or\s+-?\d+(?:\.\d+)?'
    r'|\bor\s+0(?:\.\d+)?\b(?!\s*[,)\]}:])')
RANDOM_RE = re.compile(r'\brandom\.(?:choice|uniform|randint|random|gauss)\b')
SWALLOW_RE = re.compile(
    r'except\b[^\n]*:\s*\n?\s*(?:pass(?=\s|$)|return\s*(?:\{\s*\}|\[\s*\]|0(?=\s|$)|None(?=\s|$)))')
MOCK_RE = re.compile(
    r'\b(?:mock_|dummy_|fake_|placeholder|synthetic|TODO|FIXME)\b', re.I)


class FabricationError(Exception):
    """Raised when a payload would publish an unprovenanced/invented value."""


def scan_source(src, path="<source>"):
    """F2 static detector. Returns ranked findings with line evidence."""
    findings = []
    lines = src.split("\n")
    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        for rx, kind, sev in (
                (RANDOM_RE, "random_value", "critical"),
                (FALLBACK_RE, "silent_fallback", "high"),
                (MOCK_RE, "mock_marker", "medium"),
        ):
            if rx.search(line):
                findings.append({"path": path, "line": i, "kind": kind,
                                 "severity": sev, "code": stripped[:140]})
    for m in SWALLOW_RE.finditer(src):
        ln = src[:m.start()].count("\n") + 1
        findings.append({"path": path, "line": ln, "kind": "swallowed_error",
                         "severity": "high",
                         "code": m.group(0).replace("\n", " ")[:140]})
    score = sum({"critical": 5, "high": 2, "medium": 1}.get(f["severity"], 1)
                for f in findings)
    return {"path": path, "n_findings": len(findings), "risk_score": score,
            "findings": findings}


def _is_envelope(o):
    return isinstance(o, dict) and "value" in o and "source" in o


def guard_output(payload, mode="warn", engine=None, max_depth=8):
    """F3 runtime guard. Walk a payload about to be published and report (or
    block) bare numeric leaves that carry no provenance and match a suspect
    literal — the signature of `or 0` reaching the page as data."""
    suspects = []

    def walk(o, path="$", depth=0):
        if depth > max_depth:
            return o
        if _is_envelope(o):
            return o
        if isinstance(o, dict):
            out = {}
            for k, v in o.items():
                p = f"{path}.{k}"
                if isinstance(v, (int, float)) and not isinstance(v, bool) \
                        and v in SUSPECT_LITERALS:
                    suspects.append({"path": p, "value": v})
                    if mode == "strip":
                        out[k] = {"field": k, "value": None,
                                  "data_unavailable": True,
                                  "reason": "suspect literal without "
                                            "provenance (fabrication guard)",
                                  "source": {"kind": "unknown"}}
                        continue
                    out[k] = v
                else:
                    out[k] = walk(v, p, depth + 1)
            return out
        if isinstance(o, list):
            return [walk(v, f"{path}[{i}]", depth + 1)
                    for i, v in enumerate(o[:500])]
        return o

    guarded = walk(payload)
    report = {"engine": engine, "mode": mode, "n_suspects": len(suspects),
              "suspects": suspects[:50]}
    if suspects:
        print("[fabrication_guard] %s: %d suspect literals without "
              "provenance %s" % (engine or "?", len(suspects),
                                 json.dumps(suspects[:5])))
        try:  # CloudWatch EMF metric — alarmable
            print(json.dumps({"_aws": {"CloudWatchMetrics": [{
                "Namespace": "JustHodl/DataQuality",
                "Dimensions": [["engine"]],
                "Metrics": [{"Name": "FabricationSuspects", "Unit": "Count"}]}]},
                "engine": engine or "unknown",
                "FabricationSuspects": len(suspects)}))
        except Exception:
            pass
        if mode == "block":
            raise FabricationError(
                f"{engine}: {len(suspects)} unprovenanced suspect literals; "
                f"first: {suspects[:3]}")
    return guarded, report


def assert_no_fabrication(payload, engine=None):
    """Strict helper for engines that have completed F1 migration."""
    return guard_output(payload, mode="block", engine=engine)
