"""ops_5495 -- prove official_stats_brief in live bridge/fusion zips after 3216."""
from __future__ import annotations

import io
import json
import sys
import urllib.request
import zipfile
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"


def _zip_info(lam, fn):
    loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
    raw = urllib.request.urlopen(loc, timeout=60).read()
    z = zipfile.ZipFile(io.BytesIO(raw))
    names = z.namelist()
    reg = json.loads(z.read("engine-registry.v1.json")) if "engine-registry.v1.json" in names else {"engines": []}
    ids = [e.get("engine_id") for e in reg.get("engines") or []]
    return {"n_engines": len(ids), "official_stats_brief": "official_stats_brief" in ids,
            "jh_brief_adapters": any(n.endswith("jh_brief_adapters.py") for n in names)}


def main():
    with report("ops_5495_official_stats_fusion_leg_proof") as R:
        R.heading("ops 5495 after deploy 3216")
        lam = boto3.client("lambda", region_name="us-east-1")
        s3 = boto3.client("s3", region_name="us-east-1")
        miss = False
        for fn in ("justhodl-jhsignal-bridge", "justhodl-jh-fusion"):
            info = _zip_info(lam, fn)
            R.ok("%s %s" % (fn, info))
            if not info["official_stats_brief"]:
                miss = True
        brief = json.loads(s3.get_object(Bucket=B, Key="data/official-stats-brief.json")["Body"].read())
        R.ok("brief %s gdpnow=%s" % (brief.get("status"), (brief.get("fields") or {}).get("gdpnow")))
        resp = lam.invoke(FunctionName="justhodl-jhsignal-bridge",
                          InvocationType="RequestResponse",
                          Payload=json.dumps({"source": "ops_5495"}).encode("utf-8"))
        R.ok("bridge invoke %s" % resp.get("StatusCode"))
        st = json.loads(s3.get_object(Bucket=B, Key="data/jhsignal/state/latest.json")["Body"].read())
        ents = (st.get("entities") or {}).get("market:US_EQUITY") or []
        if isinstance(ents, dict):
            ents = list(ents.values()) if ents else []
        hit = [s for s in ents if isinstance(s, dict) and s.get("engine_id") == "official_stats_brief"]
        if hit:
            s = hit[0]
            R.ok("state score=%s conf=%s type=%s" % (s.get("score"), s.get("confidence"), s.get("signal_type")))
        else:
            R.warn("no official_stats_brief in state entity list n=%s" % (len(ents) if isinstance(ents, list) else type(ents)))
        if miss:
            R.fail("zip still 21 engines")
            sys.exit(1)


if __name__ == "__main__":
    main()
