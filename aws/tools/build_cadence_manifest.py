"""
build_cadence_manifest.py — generate the fleet-monitor cadence manifest.

Scans every aws/lambdas/X/config.json for its EventBridge schedule, parses
the cron into an expected refresh interval, and maps it to the data outputs
that engine writes (detected from put_object / OUT_KEY lines in its source).

The fleet-monitor reads the result from _health/cadence-manifest.json and
judges each output against ITS OWN cadence — a weekly engine 5 days old is
healthy, a 3-hourly engine 13 hours old is not. Outputs whose engine has no
schedule are event-driven and exempt from staleness entirely.

Run standalone  -> writes ./cadence-manifest.json (the CI workflow uploads it)
import build_manifest() -> returns the dict (the ops seeder uploads it)
"""
import glob
import json
import os
import re


def cron_to_hours(expr):
    """An EventBridge schedule expression -> expected interval in hours."""
    if not expr:
        return None
    expr = expr.strip()
    try:
        if expr.startswith("rate("):
            n_s, unit = expr[5:-1].split()
            n = float(n_s)
            if "minute" in unit:
                return round(n / 60, 3)
            if "hour" in unit:
                return n
            if "day" in unit:
                return n * 24
            return 24.0
        if expr.startswith("cron("):
            f = expr[5:-1].split()
            if len(f) < 5:
                return 24.0
            minute, hour, dom, dow = f[0], f[1], f[2], f[4]
            if hour.startswith("*/"):
                return float(int(hour[2:]))
            if "/" in hour:                       # e.g. 0/6
                return float(int(hour.split("/")[1]))
            if hour == "*":                       # hourly / sub-hourly
                if minute.startswith("*/"):
                    return round(int(minute[2:]) / 60, 3)
                if "/" in minute:
                    return round(int(minute.split("/")[1]) / 60, 3)
                return 1.0
            # a specific hour -> daily, unless a weekday/month-day narrows it
            if dow not in ("*", "?", ""):
                # a range/list of weekdays (MON-FRI) runs most days — treat as
                # ~daily; a single weekday token is genuinely weekly
                if "-" in dow or "," in dow:
                    return 24.0
                return 24.0 * 7                   # weekly
            if dom not in ("*", "?", ""):
                return 24.0 * 30                  # monthly
            return 24.0                           # daily
    except Exception:
        return 24.0
    return 24.0


WRITE_HINTS = ("put_object", "OUT_KEY", "OUTPUT_KEY", "OUT_PATH",
               "Key=", "Key =")


def build_manifest(repo_root="."):
    """Scan all Lambda configs -> {output: {cadence_hours, cron, engine}}."""
    outputs, engines = {}, 0
    for cfg_path in sorted(glob.glob(
            os.path.join(repo_root, "aws/lambdas/*/config.json"))):
        try:
            cfg = json.load(open(cfg_path, encoding="utf-8"))
        except Exception:
            continue
        engines += 1
        fn = (cfg.get("function_name")
              or os.path.basename(os.path.dirname(cfg_path)))
        sched = cfg.get("schedule")
        if isinstance(sched, dict):
            cron = sched.get("cron")
        elif isinstance(sched, str) and sched.strip():
            cron = sched.strip()              # config stores the expr directly
        else:
            cron = None
        cadence = cron_to_hours(cron)             # None when no schedule
        produced = set()
        src_dir = os.path.join(os.path.dirname(cfg_path), "source")
        for src in glob.glob(os.path.join(src_dir, "*.py")):
            try:
                txt = open(src, encoding="utf-8", errors="ignore").read()
            except Exception:
                continue
            for line in txt.splitlines():
                hit = any(h in line for h in WRITE_HINTS)
                if not hit and re.search(
                        r"[A-Z][A-Z0-9_]{2,}\s*=\s*[\"']data/", line):
                    hit = True               # caps constant = an output key
                if hit:
                    for m in re.findall(r'data/([a-zA-Z0-9_-]+)\.json', line):
                        produced.add(m)
        for out in produced:
            prev = outputs.get(out)
            # on collision prefer a scheduled producer, then a name match
            better = (prev is None
                      or (prev.get("cadence_hours") is None
                          and cadence is not None)
                      or (out in fn))
            if better:
                outputs[out] = {"cadence_hours": cadence, "cron": cron,
                                "engine": fn}
    return {"schema_version": "1.0", "engines_scanned": engines,
            "n_outputs": len(outputs), "outputs": outputs}


if __name__ == "__main__":
    m = build_manifest(".")
    with open("cadence-manifest.json", "w", encoding="utf-8") as f:
        json.dump(m, f, indent=2)
    scheduled = sum(1 for v in m["outputs"].values()
                    if v.get("cadence_hours") is not None)
    print(f"[cadence-manifest] {m['n_outputs']} outputs from "
          f"{m['engines_scanned']} engines ({scheduled} scheduled, "
          f"{m['n_outputs'] - scheduled} event-driven) -> cadence-manifest.json")
