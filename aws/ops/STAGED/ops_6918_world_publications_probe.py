"""ops 6918 - READ-ONLY: how far back the World tab's own daily figures can be rebuilt, per country.

ops 6917 found long yield histories for 33 of the World tab's 44 countries and a measured CDS history for 28; for the
rest the only history is the world document's own past publications (provider-reported 10-year yield and CDS for all
44).  The engine that writes data/global-sovereign.json keeps every publication it replaces under a private prefix
(6,346 objects, 482 MB - mostly source captures).  This probe finds the publications among them without reading the
captures: it asks for the first bytes of each object and reads in full only those that begin like the document does.
It also asks whether the bucket keeps earlier versions of the public key.  Nothing is written.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
BUCKET = "justhodl-dashboard-live"
RETAINED = "audit-private/20260909-originals/global-sovereign-research/"
HEAD = "data/global-sovereign.json"
OPENING = b'{"calls_eligible"'        # the engine writes its document with sorted keys
MONTHS = {m: i + 1 for i, m in enumerate("January February March April May June July August September October November December".split())}


def provider_day(text):
    """'21 November 2026' -> '2026-11-21'; anything else -> None."""
    parts = str(text or "").split()
    if len(parts) == 3 and parts[0].isdigit() and parts[1] in MONTHS and parts[2].isdigit():
        return "%04d-%02d-%02d" % (int(parts[2]), MONTHS[parts[1]], int(parts[0]))
    return None


def selftest():
    assert provider_day("21 November 2026") == "2026-11-21" and provider_day("3 May 2025") == "2025-05-03"
    assert provider_day("yesterday") is None and provider_day(None) is None


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(max_pool_connections=32))
    with report("ops_6918_world_publications_probe") as r:
        r.heading("ops 6918 - the world document's past publications (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. Earlier versions of the public key")
        try:
            status = s3.get_bucket_versioning(Bucket=BUCKET).get("Status")
            versions = []
            for page in s3.get_paginator("list_object_versions").paginate(Bucket=BUCKET, Prefix=HEAD):
                versions += [v for v in page.get("Versions") or [] if v["Key"] == HEAD]
            r.log("bucket versioning: %s | versions of %s: %d%s" % (status, HEAD, len(versions), (", %s .. %s" % (
                min(v["LastModified"] for v in versions).strftime("%Y-%m-%d"), max(v["LastModified"] for v in versions).strftime("%Y-%m-%d"))) if versions else ""))
        except Exception as e:  # noqa: BLE001
            r.log("versions: %s: %s" % (type(e).__name__, str(e)[:160]))

        r.section("2. Publications among the retained objects")
        keys = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=RETAINED):
            keys += page.get("Contents") or []
        sizes = sorted(k["Size"] for k in keys)
        r.log("%d objects, %.1f MB | size: smallest %d, median %d, largest %d" % (len(keys), sum(sizes) / 1e6, sizes[0], sizes[len(sizes) // 2], sizes[-1]))
        bands = {}
        for s in sizes:
            b = "<2k" if s < 2000 else "<8k" if s < 8000 else "<20k" if s < 20000 else "<60k" if s < 60000 else "<200k" if s < 200000 else "200k+"
            bands[b] = bands.get(b, 0) + 1
        r.log("by size: %s" % bands)

        def sniff(k):
            try:
                return k, s3.get_object(Bucket=BUCKET, Key=k["Key"], Range="bytes=0-23")["Body"].read()
            except Exception:  # noqa: BLE001
                return k, b""
        t0 = time.time()
        candidates = [k for k in keys if 4000 <= k["Size"] <= 400000]
        with ThreadPoolExecutor(24) as pool:
            sniffed = list(pool.map(sniff, candidates))
        openings = {}
        for _, first in sniffed:
            openings[first[:14]] = openings.get(first[:14], 0) + 1
        r.log("first bytes of %d objects between 4 KB and 400 KB, read in %.0fs: %s" % (len(candidates), time.time() - t0, sorted(((v, k.decode("latin-1")) for k, v in openings.items()), reverse=True)[:8]))
        heads = [k for k, first in sniffed if first.startswith(OPENING)]

        def whole(k):
            try:
                return json.loads(s3.get_object(Bucket=BUCKET, Key=k["Key"])["Body"].read())
            except Exception:  # noqa: BLE001
                return None
        t0 = time.time()
        with ThreadPoolExecutor(16) as pool:
            docs = [d for d in pool.map(whole, heads) if isinstance(d, dict) and isinstance(d.get("countries"), list) and d.get("generated_at")]
        by_time = {d["generated_at"]: d for d in docs}
        days = sorted({g[:10] for g in by_time})
        r.log("%d objects open like the document; %d read as publications in %.0fs (%.1f MB); %d distinct publication times on %d days, %s .. %s" % (
            len(heads), len(docs), time.time() - t0, sum(k["Size"] for k in heads) / 1e6, len(by_time), len(days), days[0] if days else "-", days[-1] if days else "-"))
        if by_time:
            first, last = by_time[min(by_time)], by_time[max(by_time)]
            r.log("earliest %s version %s, %d countries | latest %s version %s, %d countries" % (min(by_time)[:16], first.get("version"), len(first["countries"]), max(by_time)[:16], last.get("version"), len(last["countries"])))
            r.log("a row of the earliest: %s" % json.dumps(first["countries"][0])[:300])
            versions = {}
            for d in by_time.values():
                versions[str(d.get("version"))] = versions.get(str(d.get("version")), 0) + 1
            r.log("document versions: %s" % versions)
            gaps = [(days[i - 1], days[i]) for i in range(1, len(days)) if (datetime.fromisoformat(days[i]) - datetime.fromisoformat(days[i - 1])).days > 3]
            r.log("gaps longer than three days: %s" % (gaps or "none"))
            per, clock = {}, {"provider": 0, "none": 0}
            for g in sorted(by_time):
                for c in by_time[g]["countries"]:
                    if not isinstance(c, dict) or not c.get("country"):
                        continue
                    day = provider_day(c.get("as_of"))
                    clock["provider" if day else "none"] += 1
                    slot = per.setdefault(c["country"], {"y": {}, "c": {}})
                    if isinstance(c.get("yield_10y_pct"), (int, float)):
                        slot["y"][day or g[:10]] = c["yield_10y_pct"]
                    if isinstance(c.get("cds_bp"), (int, float)):
                        slot["c"][day or g[:10]] = c["cds_bp"]
            r.log("rows with the provider's own date: %s" % clock)
            r.log("days with a yield / with a CDS, per country: %s" % " | ".join("%s %d/%d" % (c, len(v["y"]), len(v["c"])) for c, v in sorted(per.items())))
            sample = per.get("Turkey") or next(iter(per.values()))
            ys = sorted(sample["y"].items())
            r.log("Turkey's reported 10-year yield: %s ... %s" % (ys[:3], ys[-3:]))
            r.kv(publications=len(by_time), days=len(days), first_day=days[0], last_day=days[-1], countries=len(per))
        r.ok("probe complete; nothing written")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
        print("selftest ok")
    else:
        main()
