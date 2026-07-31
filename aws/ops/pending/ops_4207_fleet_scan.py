"""ops_4207 — FLEET COVERAGE SCAN: mine the system's own artifacts for
series matching NFS TV-bares (ticker remap), write data/fleet-map.json."""
import json
import re
import sys
from collections import Counter
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
BARE_RX = re.compile(r"^[A-Z][A-Z0-9]{1,14}$")
VAL_KEYS = ("value", "latest", "close", "price", "last", "level", "px")


def latest_of(v):
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v), None
    if isinstance(v, dict):
        for kk in VAL_KEYS:
            x = v.get(kk)
            if isinstance(x, (int, float)) and not isinstance(x, bool):
                return float(x), str(v.get("asof")
                                     or v.get("date") or "")[:10]
        if isinstance(v.get("history"), list) and v["history"]:
            h = v["history"][-1]
            if isinstance(h, (list, tuple)) and len(h) >= 2 \
                    and isinstance(h[1], (int, float)):
                return float(h[1]), str(h[0])[:10]
            if isinstance(h, dict):
                return latest_of(h)
    if isinstance(v, list) and v and isinstance(v[-1], (int, float)):
        return float(v[-1]), None
    return None, None


def walk(obj, out, depth=0):
    if depth > 3 or len(out) > 4000:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            ks = str(k)
            if BARE_RX.match(ks):
                val, asof = latest_of(v)
                if val is not None and ks not in out:
                    out[ks] = (val, asof, depth)
            if isinstance(v, (dict, list)):
                walk(v, out, depth + 1)


def main():
    with report("4207_fleet_scan") as rep:
        rep.heading("ops 4207 — fleet coverage scan (ticker remap)")
        v = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        nfs = set()
        for r in v.get("symbols") or []:
            if r.get("status") == "NO_FREE_SOURCE":
                nfs.add(str(r.get("symbol")))
        rep.kv(nfs_pool=len(nfs))

        objs = []
        pag = s3.get_paginator("list_objects_v2")
        for pg in pag.paginate(Bucket=BUCKET, Prefix="data/"):
            for o in pg.get("Contents", []):
                k = o["Key"]
                if k.endswith(".json") and 3000 < o["Size"] < 2500000:
                    objs.append((k, o["Size"]))
        rep.kv(candidate_files=len(objs))

        SKIP = ("tradingview.json", "tv-watchlists", "te-feed",
                "symbol-feed", "cot-feed", "cq-feed", "families",
                "family-defs", "source-map", "tv-sources",
                "finviz-universe")
        fleet = {}
        perfile = []
        scanned = 0
        for k, sz in sorted(objs, key=lambda x: -x[1]):
            if any(s in k for s in SKIP) or scanned >= 60:
                continue
            scanned += 1
            try:
                d = json.loads(s3.get_object(
                    Bucket=BUCKET, Key=k)["Body"].read())
            except Exception:
                continue
            found = {}
            walk(d, found)
            hits = 0
            for bare, (val, asof, dep) in found.items():
                if bare in nfs and bare not in fleet:
                    fleet[bare] = {"value": val, "asof": asof,
                                   "src_key": k[:60]}
                    hits += 1
            if hits:
                perfile.append((hits, k, len(found)))
        perfile.sort(reverse=True)
        rep.section("per-artifact NFS hits (top 18)")
        for h, k, tot in perfile[:18]:
            rep.log(f"  {h:4d} hits  ({tot:5d} keys)  {k[:70]}")
        rep.kv(fleet_map_hits=len(fleet), files_scanned=scanned)
        sample = dict(list(fleet.items())[:6])
        rep.log("  sample: " + json.dumps(sample)[:500])

        s3.put_object(Bucket=BUCKET, Key="data/fleet-map.json",
                      Body=json.dumps(
                          {"marker": "fleet-map v1 ops4207",
                           "n": len(fleet),
                           "prices": fleet}).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=600")
        rep.ok(f"FLEET SCAN — {len(fleet)} remappable ticks banked")


if __name__ == "__main__":
    main()
