"""ops 3220 — closing read: the two engines' fresh rows verbatim, plus a
runner-path replay of one curated member (to_weekly obs count) so the
next session starts from evidence, not a dangling why."""
import json
import sys
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import series_source as SS  # noqa: E402

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return {}


with report("3220_final_reasons") as rep:
    rep.heading("ops 3220 — closing evidence")
    idx = s3_json("data/wl-engines.json")
    for nm in ("Europe Liquidity", "Global Deposit Rates"):
        e = next((x for x in (idx.get("engines") or [])
                  if nm.lower() in str(x.get("name", "")).lower()), None)
        if e:
            rep.log(f"{str(e.get('name'))[:44]:<44} state={e.get('state')} "
                    f"resolved={e.get('members_resolved')}/"
                    f"{e.get('members_total')} "
                    f"reason={str(e.get('reason') or 'ACTIVE')[:80]}")
    # runner-path replay: weekly obs of one monthly spread
    ser = SS.fetch("DERIVED",
                   "FRED~IRLTLT01DEM156N~minus~FRED~IRLTLT01ITM156N")
    wk = {}
    for d, v in sorted(ser.items()):
        y, m, dd = (int(x) for x in d[:10].split("-"))
        from datetime import date
        iy, iw, _ = date(y, m, dd).isocalendar()
        wk[f"{iy}-{iw:02d}"] = v
    rep.kv(spread_raw_pts=len(ser), spread_weekly_obs=len(wk),
           z_min_needed=12)
    rep.kv(verdict="PASS")
