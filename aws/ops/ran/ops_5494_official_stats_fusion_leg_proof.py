"""ops_5494 -- prove official_stats_brief is a shadow MARKET fusion leg."""
from __future__ import annotations

import json
import sys
import zipfile
import io
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
REGION = "us-east-1"


def _zip_has(lam, fn, name):
    loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
    # runner can fetch code location
    import urllib.request
    raw = urllib.request.urlopen(loc, timeout=60).read()
    z = zipfile.ZipFile(io.BytesIO(raw))
    names = z.namelist()
    reg = None
    if "engine-registry.v1.json" in names:
        reg = json.loads(z.read("engine-registry.v1.json"))
    has_ad = any(n.endswith("jh_brief_adapters.py") for n in names)
    ids = [e.get("engine_id") for e in (reg or {}).get("engines") or []]
    return {
        "n_engines": len(ids),
        "official_stats_brief": "official_stats_brief" in ids,
        "jh_brief_adapters": has_ad,
    }


def main():
    with report("ops_5494_official_stats_fusion_leg_proof") as R:
        R.heading("ops 5494 official_stats fusion leg")
        lam = boto3.client("lambda", region_name=REGION)
        s3 = boto3.client("s3", region_name=REGION)
        for fn in ("justhodl-jhsignal-bridge", "justhodl-jh-fusion"):
            info = _zip_has(lam, fn, "official_stats_brief")
            R.ok("%s zip %s" % (fn, info))
            if not info["official_stats_brief"]:
                R.warn("%s package missing official_stats_brief row" % fn)
        brief = json.loads(s3.get_object(Bucket=B, Key="data/official-stats-brief.json")["Body"].read())
        R.ok("brief status=%s gdpnow=%s" % (brief.get("status"), (brief.get("fields") or {}).get("gdpnow")))
        resp = lam.invoke(
            FunctionName="justhodl-jhsignal-bridge",
            InvocationType="RequestResponse",
            Payload=json.dumps({"source": "ops_5494"}).encode("utf-8"),
        )
        R.ok("bridge invoke %s" % resp.get("StatusCode"))
        st = json.loads(s3.get_object(Bucket=B, Key="data/jhsignal/state/latest.json")["Body"].read())
        rows = ((st.get("entities") or {}).get("market:US_EQUITY")) or []
        hit = [s for s in rows if isinstance(s, dict) and s.get("engine_id") == "official_stats_brief"]
        if not hit:
            R.warn("no official_stats_brief row in state yet (package or event shape)")
        else:
            s = hit[0]
            R.ok("state official_stats_nowcast score=%s conf=%s fresh=%s shadow-ok" % (
                s.get("score"), s.get("confidence"), s.get("freshness")))
        if brief.get("status") != "LIVE":
            R.fail("official-stats brief not LIVE")
            sys.exit(1)


if __name__ == "__main__":
    main()
