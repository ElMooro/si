"""Smoke suite v2 (Perplexity item 5): real contract tests —
(1) as_of within each feed's cadence, (2) required fields present,
(3) data_unavailable ratio under a per-feed ceiling, (4) canary flags
non-null whenever their inputs are non-null. exit 1 on any failure."""
import json
import sys
import urllib.request
from datetime import datetime, timezone

# key: (max_age_hours, required_fields, max_unavailable_ratio)
CHECKS = {
    "data/provider-catalog.json": (26, ["totals", "providers",
                                        "reconcile_ok"], 1.0),
    "data/canary-macro.json": (26, ["flags"], 0.20),
    "data/crisis-plumbing.json": (7, [], 0.35),
    "data/rotation-dashboard.json": (30, [], 0.35),
    "data/best-setups.json": (30, [], 0.50),
    "data/master-ranker.json": (30, [], 0.50),
    "data/families.json": (30, [], 0.50),
    "data/signal-board.json": (30, [], 0.50),
    "data/llm-cost.json": (30, [], 0.50),
}


def _unavail_ratio(obj):
    total = [0]
    bad = [0]

    def walk(o):
        if isinstance(o, dict):
            total[0] += 1
            if o.get("data_unavailable"):
                bad[0] += 1
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(obj)
    return (bad[0] / total[0]) if total[0] else 0.0


def _age_hours(d):
    for k in ("as_of", "generated_at", "updated_at", "swept_at"):
        v = d.get(k)
        if isinstance(v, str):
            try:
                ts = datetime.fromisoformat(v.replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                return (datetime.now(timezone.utc) - ts
                        ).total_seconds() / 3600
            except Exception:
                continue
    return None


def main():
    bad = []
    for key, (max_h, want, max_ur) in CHECKS.items():
        try:
            rq = urllib.request.Request(
                "https://justhodl.ai/" + key + "?smoke=2",
                headers={"User-Agent":
                         "JustHodl-smoke admin@justhodl.ai"})
            with urllib.request.urlopen(rq, timeout=25) as r:
                d = json.loads(r.read())
        except Exception as e:
            bad.append((key, f"{type(e).__name__}: {str(e)[:60]}"))
            continue
        missing = [x for x in want if x not in d]
        if missing:
            bad.append((key, f"missing {missing}"))
        age = _age_hours(d)
        if age is not None and age > max_h:
            bad.append((key, f"stale {age:.1f}h > {max_h}h"))
        ur = _unavail_ratio(d)
        if ur > max_ur:
            bad.append((key, f"data_unavailable {ur:.0%} > "
                             f"{max_ur:.0%}"))
        if key.endswith("provider-catalog.json"):
            for pr in (d.get("providers") or []):
                tg = pr.get("datasets_target")
                if tg and (pr.get("datasets") or 0) > tg:
                    bad.append((key, f"{pr.get('slug')}: datasets "
                                     f"{pr.get('datasets')} > target "
                                     f"{tg} (unit mix)"))
        if key.endswith("canary-macro.json"):
            fl = d.get("flags") or {}
            pairs = [("SAHMREALTIME", "sahm_triggered"),
                     ("T10Y3M", "curve_10y3m_inverted")]
            for sid, fk in pairs:
                val = (d.get(sid) or {}).get("value")
                if val is not None and fl.get(fk) is None:
                    bad.append((key, f"flag {fk} null while "
                                     f"{sid} present"))
    print(json.dumps({"checked": len(CHECKS), "failures": bad},
                     indent=1))
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
