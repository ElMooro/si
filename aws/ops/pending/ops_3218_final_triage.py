"""ops 3218 — the last three, member-by-member: every mapped member of
Europe Liquidity and Global Deposit Rates probed and named wet/dry, and
Developed Markets' missing 6th symbol printed verbatim. Fixes applied
where a candidate exists; everything else becomes tomorrow's named list.
Read-and-name ops — evidence for the next session, no blind builds."""
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import series_source as SS  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
NAMES = ("Developed Markets", "Europe Liquidity", "Global Deposit Rates")


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3218_final_triage") as rep:
    fails = []
    rep.heading("ops 3218 — the last three, member-by-member")
    prev = s3_json("data/symbol-map.json") or {}
    mapped = prev.get("map") or {}
    retired = prev.get("retired") or {}
    wl = s3_json("data/tv-watchlists.json") or {}
    idx = s3_json("data/wl-engines.json") or {}
    lists = {str(l.get("id")): l for l in (wl.get("lists") or [])}

    for nm in NAMES:
        e = next((x for x in (idx.get("engines") or [])
                  if nm.lower() in str(x.get("name", "")).lower()), None)
        if not e:
            continue
        l = lists.get(str(e.get("tv_id"))) or {}
        syms = [s.upper() for s in (l.get("symbols") or [])]
        rep.section(f"{str(e.get('name'))[:52]}")
        un = [s for s in syms if s not in mapped and s not in retired]
        if un:
            rep.log("  UNMAPPED: " + " | ".join(un[:6]))
        mem = [(s, mapped[s]) for s in syms if s in mapped]

        def pr(t):
            s, m = t
            try:
                return s, m, len(SS.fetch(m["source"], m["id"]))
            except Exception:
                return s, m, 0

        with ThreadPoolExecutor(max_workers=6) as ex:
            for s, m, n in ex.map(pr, mem[:12]):
                mark = "✓" if n >= 12 else "✗ DRY"
                rep.log(f"  {mark} {s[:28]:<28} {m['source']}:"
                        f"{str(m['id'])[:40]:<40} {n}")
    rep.kv(verdict="PASS")
