"""ops_5196 -- READ-ONLY: which of the bond war room's inputs does OUR warehouse (data.html) already serve?
Doctrine 2026-09-02: every series is pulled from our own AWS, never a third party. v1.2.2 pulls FRED, Bundesbank,
BoE, BoC, RBA, MOF, ECB and Yahoo directly. This resolves every input through the symdir /series resolver
(warehouse-first, tail-healed) and lists the official-yields + tv-bars banks so v1.3 can go warehouse-first and
bank what it must still fetch outside."""
import gzip
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"


def get(url, timeout=60):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops5196", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:200]
    except Exception as e:  # noqa: BLE001
        return -1, str(e).encode()


def ls(prefix, limit=2000):
    out, tok = [], None
    while True:
        kw = {"Bucket": B, "Prefix": prefix, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        out += [(o["Key"], o["Size"], o["LastModified"].isoformat()[:16]) for o in r.get("Contents") or []]
        tok = r.get("NextContinuationToken")
        if not tok or len(out) >= limit:
            return out


with report("ops_5196_warroom_warehouse_audit") as R:
    R.heading("ops 5196 -- bond war room inputs vs the warehouse")
    R.section("A. /series resolver (warehouse-first) for every war-room input")
    ids = ["fred:DGS3MO", "fred:DGS2", "fred:DGS10", "fred:DGS30", "fred:DFII10", "fred:T10YIE", "fred:SOFR", "fred:DTB3", "fred:DTWEXBGS", "fred:VIXCLS",
           "fred:BAMLH0A0HYM2", "fred:BAMLC0A0CM", "fred:BAMLC0A1CAAA", "fred:BAMLC0A4CBBB", "fred:BAMLH0A1HYBB", "fred:BAMLH0A2HYB", "fred:BAMLH0A3HYC",
           "fred:BAMLHE00EHYIOAS", "fred:BAMLEMCBPIOAS", "fred:BAMLEMHBHYCRPIOAS", "fred:BAMLEMPBPUBSICRPIOAS", "fred:BAMLEMIBHGCRPIOAS",
           "ustpar:2Y", "ustpar:10Y", "ustpar:30Y", "boe:IUDMNZC", "boe:IUDSNPY", "boe:IUDLNPY",
           "official-yields:de-10y-bbk", "official-yields:ea-aaa-10y-ecb",
           "ecb:YC:B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y", "ecb:YC:B.U2.EUR.4F.G_N_C.SV_C_YM.SR_10Y",
           "TLT", "IEF", "SHY", "HYG", "LQD", "EMB", "^MOVE", "^TNX", "^IRX", "^TYX", "MOVE"]
    for sid in ids:
        st, b = get("%s/series?id=%s" % (PROXY, urllib.parse.quote(sid)))
        try:
            j = json.loads(b)
            obs = j.get("observations") or j.get("obs") or []
            last = obs[-1] if obs else None
            R.log("   %-42s -> %s n=%-6d first=%s last=%s src=%s" % (sid, st, len(obs), (obs[0][0] if obs else None), last, str(j.get("source") or j.get("src"))[:60]))
        except Exception:  # noqa: BLE001
            R.log("   %-42s -> %s body=%s" % (sid, st, b[:120]))
    R.section("B. banks")
    oy = ls("data/warm/official-yields/")
    R.log("   official-yields: %d objects: %s" % (len(oy), [(k.split('/')[-1], s, m) for k, s, m in oy[:40]]))
    try:
        idx = json.loads(s3.get_object(Bucket=B, Key="data/warm/tv-bars/universe/_index.json")["Body"].read())
        keys = idx if isinstance(idx, list) else (idx.get("symbols") or idx.get("keys") or list(idx.keys()))
        R.log("   tv-bars universe index: %d symbols; type=%s sample=%s" % (len(keys), type(idx).__name__, list(keys)[:12]))
        want = ["TLT", "IEF", "SHY", "HYG", "LQD", "EMB", "^MOVE", "MOVE", "^TNX", "^IRX", "^FVX", "^TYX", "TVC:MOVE", "TVC:US10Y", "TVC:DE10Y", "TVC:IT10Y", "TVC:JP10Y"]
        ks = set(map(str, keys)) if not isinstance(idx, dict) else set(idx.keys())
        R.log("   present: %s" % {w: (w in ks) for w in want})
    except Exception as e:  # noqa: BLE001
        R.log("   tv-bars index read failed: %s" % str(e)[:120])
    for pref in ("data/warm/treasury/", "data/warm/boe-full/iadb/IUDMNZC", "data/warm/fred-scoped/", "data/warm/boj-full/api/"):
        objs = ls(pref, 1500)
        R.log("   %-32s %d objects; sample=%s" % (pref, len(objs), [k.split('/')[-1] for k, _, _ in objs[:6]]))
    fs = ls("data/warm/fred-scoped/", 20000)
    hit = [k for k, _, _ in fs if any(x in k for x in ("/DGS10.json", "/BAMLH0A0HYM2.json", "/VIXCLS.json", "/DTWEXBGS.json"))]
    R.log("   fred-scoped objects scanned=%d; war-room hits=%s" % (len(fs), hit[:8]))
    R.section("C. data.html provider hub")
    try:
        hub = json.loads(s3.get_object(Bucket=B, Key="data/providers/_hub.json")["Body"].read())
        rows = hub if isinstance(hub, list) else (hub.get("providers") or hub.get("rows") or [])
        R.log("   hub providers=%d: %s" % (len(rows), [(r.get("slug") or r.get("id") or r.get("name")) for r in rows if isinstance(r, dict)][:80]))
    except Exception as e:  # noqa: BLE001
        R.log("   hub read: %s; provider docs: %s" % (str(e)[:80], [k.split('/')[-1] for k, _, _ in ls("data/providers/")[:80]]))
    R.ok("audit complete")
    if False:
        sys.exit(1)
