"""ops_4033 — 491 landed: verify + rebuild the workbench on today's data."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300, retries={"max_attempts": 0}))
BUCKET = "justhodl-dashboard-live"


def main():
    with report("4033_491_verify_rebuild") as rep:
        rep.heading("ops 4033 — 491 watchlists verified + workbench rebuild")
        checks = []
        now = datetime.now(timezone.utc)

        rep.section("A. the tracker — all 491?")
        h = s3.head_object(Bucket=BUCKET, Key="data/tv-watchlists.json")
        age = (now - h["LastModified"]).total_seconds() / 60
        wl = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/tv-watchlists.json")["Body"].read())
        lists = wl.get("lists") or wl.get("watchlists") or []
        if isinstance(lists, dict):
            lists = list(lists.values())
        bs = next((l for l in lists
                   if "swan" in str(l.get("name", "")).lower()), None)
        rep.kv(modified_min_ago=round(age, 1), n_lists=len(lists),
               blackswan=(bs or {}).get("name"),
               bs_symbols=len((bs or {}).get("symbols") or []))
        checks += [("fresh (<45 min)", age < 45),
                   ("all 491 lists landed", len(lists) >= 485),
                   ("Black Swan present >=400", bool(bs) and
                    len(bs.get("symbols") or []) >= 400)]

        rep.section("B. sources — born yet? (harvest may still be counting)")
        try:
            sr = json.loads(s3.get_object(Bucket=BUCKET,
                                          Key="data/tv-sources.json")["Body"].read())
            rep.kv(sources_n=sr.get("n_symbols"),
                   sources_generated=sr.get("generated_at"))
            for k, v in list((sr.get("sources") or {}).items())[:6]:
                rep.log(f"    {k}: {str(v.get('source'))[:46]}")
        except Exception:
            rep.log("  not born yet — lands at harvest finish (expected)")

        rep.section("C. workbench rebuild on today's data")
        r = lam.invoke(FunctionName="justhodl-tv-workbench",
                       InvocationType="RequestResponse",
                       Payload=json.dumps({"source": "ops4033"}).encode())
        rep.kv(fnerr=r.get("FunctionError"))
        checks.append(("workbench invoke clean", not r.get("FunctionError")))
        doc = json.loads(s3.get_object(Bucket=BUCKET,
                                       Key="data/tv-workbench.json")["Body"].read())
        t = doc.get("totals") or {}
        rep.kv(**t)
        checks += [("workbench shows >=485 watchlists",
                    (t.get("watchlists") or 0) >= 485),
                   ("unique indicators >=9000",
                    (t.get("unique_symbols") or 0) >= 9000)]

        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {t.get('watchlists')} lists / "
               f"{t.get('unique_symbols')} indicators / "
               f"{t.get('symbols_with_notes')} noted / "
               f"{t.get('symbols_with_tv_source')} sourced live on the page")


if __name__ == "__main__":
    main()
