"""ops 6838 - READ-ONLY: the Leaders desk's feed, before a tearsheet is built on it (re-runnable).

Khalid 2026-11-18: "now the next one we are going to work on is leaders: apply the same concept to leaders" -
asked which concept, he chose "Tearsheet with the donut" (the one-page sheet 13F, Inflows & outflows and eight other
desks carry).  Audit before building: reads data/leaders.json as published (justhodl-leaders, daily 03:50 UTC),
reports its shape and the sums a sheet could stand on - above all which whole adds up for the ring (each company
once, one unit, one horizon) - and embeds the feed so the sheet is designed and tested on the real thing.
Writes nothing.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=60)


def shape(v, depth=0):
    if isinstance(v, dict):
        if depth >= 2:
            return "{%d keys}" % len(v)
        return {k: shape(x, depth + 1) for k, x in list(v.items())[:60]}
    if isinstance(v, list):
        return "[%d] of %s" % (len(v), shape(v[0], depth + 1) if v else "nothing")
    return type(v).__name__ if v is None or isinstance(v, (bool,)) else (repr(v)[:40] if depth else type(v).__name__)


def main():
    now = datetime.now(timezone.utc)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6838_leaders_feed_audit") as r:
        r.heading("ops 6838 - the Leaders feed as published %s (read-only)" % now.isoformat(timespec="minutes"))
        head = s3.head_object(Bucket=LIVE, Key="data/leaders.json")
        body = s3.get_object(Bucket=LIVE, Key="data/leaders.json")["Body"].read()
        raw = gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body
        doc = json.loads(raw)
        r.log("data/leaders.json: %d bytes stored, %d bytes of JSON, last modified %s, cache-control %s" % (len(body), len(raw), head["LastModified"].isoformat(timespec="seconds"), head.get("CacheControl")))
        r.log("engine %s %s, contract %s, generated %s, bars as of %s" % (doc.get("engine"), doc.get("version"), doc.get("contract"), doc.get("generated_at"), doc.get("as_of")))
        r.section("shape")
        r.log("top level: %s" % json.dumps({k: (shape(v, 1) if not isinstance(v, list) else "[%d]" % len(v)) for k, v in doc.items() if k not in ("rows", "industries", "etfs", "splits", "history")}, default=str)[:3500])
        rows, inds, etfs = doc.get("rows") or [], doc.get("industries") or [], doc.get("etfs") or []
        for name, coll in (("a leader row", rows), ("an industry", inds), ("a fund row", etfs)):
            if coll:
                r.log("%s: %s" % (name, json.dumps(coll[0], default=str)[:3800]))
        r.log("summary: %s" % json.dumps(doc.get("summary"), default=str))
        r.log("changes since the prior print: prev_as_of %s, %s new, %s gone; sample %s" % ((doc.get("changes") or {}).get("prev_as_of"), (doc.get("changes") or {}).get("n_new"), (doc.get("changes") or {}).get("n_gone"), json.dumps(((doc.get("changes") or {}).get("new") or [])[:6])))

        r.section("sums a ring could stand on")
        live = [x for x in rows if not x.get("stale")]
        r.log("leaders %d (live %d), tickers unique %s, industries %d, funds %d (live %d)" % (len(rows), len(live), len({x.get("ticker") for x in rows}) == len(rows), len(inds), len(etfs), len([x for x in etfs if not x.get("stale")])))

        def num(v):
            return v if isinstance(v, (int, float)) and v == v else None
        caps = [(x.get("ticker"), num(x.get("mcap_usd") or x.get("mcap"))) for x in live]
        have = [c for _, c in caps if c]
        r.log("market cap: %d of %d leaders carry one, total $%.2fT" % (len(have), len(live), sum(have) / 1e12))
        by_sector = {}
        for x in live:
            c = num(x.get("mcap_usd") or x.get("mcap")) or 0
            by_sector[x.get("sector")] = by_sector.get(x.get("sector"), 0) + c
        r.log("by sector ($B): %s" % json.dumps({k: round(v / 1e9, 1) for k, v in sorted(by_sector.items(), key=lambda kv: -kv[1])}))
        for win in ("w", "m", "q"):
            nets = [(x["ticker"], num(((x.get("mf") or {}).get(win) or {}).get("net"))) for x in live]
            vals = [v for _, v in nets if v is not None]
            pos, neg = sum(v for v in vals if v > 0), sum(v for v in vals if v < 0)
            top = sorted([n for n in nets if n[1] is not None], key=lambda kv: -kv[1])
            r.log("signed dollar volume, window %s: %d leaders with a figure, in +$%.1fB, out -$%.1fB, net %+.1fB; largest in %s; largest out %s" % (win, len(vals), pos / 1e9, -neg / 1e9, (pos + neg) / 1e9, [(t, round(v / 1e9, 2)) for t, v in top[:4]], [(t, round(v / 1e9, 2)) for t, v in top[-4:]]))
        labels = {}
        for x in live:
            lab = (x.get("ad") or {}).get("label")
            labels[lab] = labels.get(lab, 0) + 1
        r.log("tape labels (count of leaders): %s" % json.dumps(labels))
        smart = {}
        for x in live:
            v = (x.get("smart") or {}).get("verdict")
            smart[v] = smart.get(v, 0) + 1
        r.log("smart money verdicts: %s" % json.dumps(smart))
        f13 = [(x["ticker"], num((x.get("f13") or {}).get("net_usd"))) for x in live]
        f13v = [v for _, v in f13 if v is not None]
        r.log("13F cohort net dollars: %d leaders with a figure, bought +$%.1fB, sold -$%.1fB" % (len(f13v), sum(v for v in f13v if v > 0) / 1e9, -sum(v for v in f13v if v < 0) / 1e9))
        flow_keys = sorted({k for x in etfs for k in (x.get("flow") or x.get("flows") or {}).keys()})
        r.log("fund flow keys: %s" % flow_keys)
        for x in etfs[:2]:
            r.log("  a fund's flow block: %s" % json.dumps(x.get("flow") or x.get("flows"), default=str)[:700])
        sig = {}
        for x in live + etfs:
            for s in x.get("signals") or []:
                sig[s] = sig.get(s, 0) + 1
        r.log("signals in force: %s" % json.dumps(dict(sorted(sig.items(), key=lambda kv: -kv[1]))))
        boards = doc.get("boards") or {}
        r.log("boards: %s" % json.dumps({k: {b: (len(v) if isinstance(v, list) else v) for b, v in (boards.get(k) or {}).items()} for k in boards}, default=str)[:1500])

        blob = base64.b64encode(gzip.compress(json.dumps(doc, separators=(",", ":"), default=str).encode("utf-8"), 9)).decode("ascii")
        r.log("feed embedded: %d chars" % len(blob))
        r.lines.append("<!-- FEED-PACK-GZ-B64-BEGIN")
        r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
        r.lines.append("FEED-PACK-GZ-B64-END -->")
        r.kv(leaders=str(len(rows)), industries=str(len(inds)), funds=str(len(etfs)), as_of=str(doc.get("as_of")), version=str(doc.get("version")))
        r.ok("AUDIT: leaders feed %s read, %d leaders / %d industries / %d funds" % (doc.get("as_of"), len(rows), len(inds), len(etfs)))


if __name__ == "__main__":
    main()
    sys.exit(0)
