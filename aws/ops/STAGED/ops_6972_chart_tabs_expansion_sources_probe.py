"""ops 6972 - read-only: what the warehouse already holds that the twelve engines could cover but do not.

Khalid 2026-11-26: "see if you can expand it to cover more data". An engine reads the warehouse, never a provider, so
an expansion is only possible where the data is already kept. This op reads the shape of the kept documents that the
catalyst calendar, the canary war room, the on-chain desk and the insider desk could draw on, lists the warehouse's
banks two levels down, and prints the insider purchases above $500 M (one of them drives the page's total).

Nothing is invoked and nothing is written. Safe to run again.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import importlib.util
import json
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
NAME = "ops_6972_chart_tabs_expansion_sources_probe"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
DOCS = [
    "data/eurodollar-plumbing.json", "data/econ-calendar.json", "data/macro-calendar.json", "data/fed-speak-state.json",
    "data/fed-speak.json", "data/auction-desk-view.json", "data/auction-grades.json", "data/auction-desk.json",
    "data/benzinga-earnings-calendar.json", "data/earnings-tracker.json", "data/finviz-earnings-calendar.json",
    "data/ipo-pipeline.json", "data/lockup-expiration.json", "data/rebalance-radar.json", "data/russell-recon-frontrun.json",
    "data/earnings-blackout.json", "data/insider-clusters-names.json", "data/breadth-divergence.json",
]
BIG = 3_000_000


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    spec = importlib.util.spec_from_file_location("audit6971", str(HERE / "ops_6971_chart_tabs_engine_audit.py"))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    A.CUT = 12
    s3 = boto3.client("s3", region_name=REGION, config=Config(retries={"max_attempts": 6, "mode": "standard"}, read_timeout=180))
    now = datetime.now(timezone.utc)
    pack = {"at": now.isoformat(timespec="seconds"), "docs": {}, "warm": {}, "big_buys": []}
    with report(NAME) as r:
        r.heading("6972 - kept data the twelve engines could cover (read-only), %s" % now.isoformat(timespec="minutes"))
        r.section("1. Kept documents")
        for key in DOCS:
            d = {}
            try:
                o = s3.get_object(Bucket=LIVE, Key=key)
                raw = o["Body"].read()
                d.update(bytes=len(raw), hours=round((now - o["LastModified"]).total_seconds() / 3600.0, 1))
                if raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)
                doc = json.loads(raw)
                paths = {}
                A.walk(doc, "", paths)
                d["paths"] = dict(sorted(paths.items())[:1500])
                d["cut"] = A.cut(doc) if d["bytes"] < BIG else {k: (A.cut(v) if not isinstance(v, (dict, list)) or len(json.dumps(v, default=str)) < 200000 else "large %s of %d" % (type(v).__name__, len(v))) for k, v in doc.items()} if isinstance(doc, dict) else None
                top = list(doc.keys())[:30] if isinstance(doc, dict) else ["list of %d" % len(doc)]
                r.log("%s | %s bytes, %s h old, %d field paths | top: %s" % (key, format(d["bytes"], ","), d["hours"], len(paths), ", ".join(map(str, top))))
            except Exception as exc:  # noqa: BLE001
                d["error"] = "%s: %s" % (type(exc).__name__, str(exc)[:160])
                r.log("%s | NOT READ: %s" % (key, d["error"]))
            pack["docs"][key] = d

        r.section("2. The warehouse's banks, two levels down")
        try:
            lvl1 = [p["Prefix"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix="data/warm/", Delimiter="/") for p in page.get("CommonPrefixes", [])]
            for p1 in lvl1:
                kids, files = [], 0
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix=p1, Delimiter="/", PaginationConfig={"MaxItems": 400}):
                    kids.extend(x["Prefix"][len(p1):] for x in page.get("CommonPrefixes", []))
                    files += len(page.get("Contents", []))
                pack["warm"][p1] = {"folders": kids[:200], "n_folders": len(kids), "files_here": files}
                r.log("%s | %d folders, %d files here | %s" % (p1, len(kids), files, ", ".join(kids[:60])))
        except Exception as exc:  # noqa: BLE001
            r.log("warehouse not listed: %s: %s" % (type(exc).__name__, str(exc)[:160]))

        r.section("3. Insider purchases above $500 M in the insider document")
        try:
            doc = json.loads(s3.get_object(Bucket=LIVE, Key="data/insider-trades.json")["Body"].read())
            for t in doc.get("transactions") or []:
                val = t.get("value") or t.get("value_usd") or t.get("total_value") or 0
                if isinstance(val, (int, float)) and val >= 5e8:
                    row = {k: t.get(k) for k in t if k not in ("insider_name", "name", "owner", "reporting_owner", "insider")}
                    pack["big_buys"].append(row)
                    r.log(json.dumps(row, default=str)[:600])
            r.log("purchases above $500 M: %d of %d" % (len(pack["big_buys"]), len(doc.get("transactions") or [])))
        except Exception as exc:  # noqa: BLE001
            r.log("insider document not read: %s: %s" % (type(exc).__name__, str(exc)[:160]))

        body = gzip.compress(json.dumps(pack, separators=(",", ":"), default=str).encode(), 9)
        b64 = base64.b64encode(body).decode("ascii")
        r.section("attachment")
        r.lines.append("<!-- FIXTURE sources %d bytes" % len(body))
        r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
        r.lines.append("FIXTURE-END -->")
        r.kv(documents=len(pack["docs"]), banks=len(pack["warm"]), attachment_bytes=len(body))
        r.ok("gathered; nothing invoked, nothing written")


if __name__ == "__main__":
    main()
