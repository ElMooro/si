"""ops 3231 — Europe Growth + France, member-by-member: fresh rows first
(the named reason says which gate), then every mapped member probed and
named wet/dry. The output is the fix list."""
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import series_source as SS  # noqa: E402

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
NAMES = ("Europe Growth", "France")


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3231_growth_triage") as rep:
    fails = []
    rep.heading("ops 3231 — the growth pair, member-by-member")
    prev = s3_json("data/symbol-map.json") or {}
    mapped = prev.get("map") or {}
    retired = prev.get("retired") or {}
    idx = s3_json("data/wl-engines.json") or {}
    wl = s3_json("data/tv-watchlists.json") or {}
    lists = {str(l.get("id")): l for l in (wl.get("lists") or [])}
    for nm in NAMES:
        e = next((x for x in (idx.get("engines") or [])
                  if str(x.get("name", "")).lower().startswith(
                      nm.lower())), None)
        if not e:
            rep.log(f"? {nm}: not found")
            continue
        rep.section(f"{str(e.get('name'))[:48]} — {e.get('state')} "
                    f"({str(e.get('reason') or 'ACTIVE')[:60]})")
        l = lists.get(str(e.get("tv_id"))) or {}
        syms = [s.upper() for s in (l.get("symbols") or [])]
        un = [s for s in syms if s not in mapped and s not in retired]
        if un:
            rep.log("  UNMAPPED: " + " | ".join(un[:5]))
        mem = [(s, mapped[s]) for s in syms if s in mapped][:12]

        def pr(t):
            s, m = t
            try:
                return s, m, len(SS.fetch(m["source"], m["id"]))
            except Exception:
                return s, m, 0

        with ThreadPoolExecutor(max_workers=6) as ex:
            for s, m, n in ex.map(pr, mem):
                mark = "✓" if n >= 12 else "✗ DRY"
                rep.log(f"  {mark} {s[:26]:<26} {m['source']}:"
                        f"{str(m['id'])[:42]:<42} {n}")
    rep.kv(verdict="PASS")
